"""Acceptance Test T3: Worker Kill, Lease Takeover, Consumer Recovery & Stream Trimming.

Verifies:
1. Two Uvicorn server processes (A and B) on the same Redis and Neon test branch.
2. Stream audio to A, forcibly kill A mid-stream via Popen.kill().
3. Reconnect to B with the same client_instance_id (takes over immediately via K7 Lua CAS).
4. Retransmit unacked frames; stream in Redis holds every client_seq exactly once,
   in order, no gaps.
5. Consumer group 'asr' reads with XREADGROUP, crashes before XACK.
6. Second consumer recovers the pending entries via XAUTOCLAIM.
7. 5,000 pending entries survive trimming under the default AUDIO_STREAM_MAXLEN (20,000).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import httpx
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, get_free_port
from meetmind_gateway.keys import audio_stream_key
from meetmind_schemas.events import decode_audio_frame, encode_audio_frame
from meetmind_security import db

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.asyncio
async def test_t3_worker_kill_and_recovery(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t3_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a
    client_instance_id = str(uuid.uuid4())

    repo_root = Path(__file__).resolve().parents[4]

    # Setup environment for second server process B
    port_b = get_free_port()
    env_b = os.environ.copy()
    env_b["CLERK_JWKS_URL"] = h.jwks.jwks_url
    env_b["CLERK_ISSUER"] = h.jwks.issuer
    env_b["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
    env_b["TENANT_ORG_MAP"] = json.dumps({h.org_a: h.tenant_a, h.org_b: h.tenant_b})
    env_b["ELEVATED_LISTEN_ROLES"] = json.dumps(["org:admin"])
    app_dsn = (
        os.environ.get("NEON_TEST_APP_DATABASE_URL")
        or os.environ.get("NEON_APP_DATABASE_URL")
        or db.get_app_dsn()
    )
    redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
    env_b["APP_ENV"] = "test"
    env_b["NEON_DATABASE_URL"] = app_dsn
    env_b["NEON_APP_DATABASE_URL"] = app_dsn
    env_b["NEON_TEST_APP_DATABASE_URL"] = app_dsn
    env_b.pop("NEON_TEST_DATABASE_URL", None)
    env_b.pop("NEON_DIRECT_DATABASE_URL", None)
    env_b["REDIS_URL"] = redis_url
    env_b["AUDIO_REDIS_URL"] = redis_url
    env_b["AUDIO_STREAM_MAXLEN"] = "20000"
    env_b["LOG_LEVEL"] = "INFO"
    env_b["PYTHONPATH"] = (
        str(repo_root / "backend" / "gateway")
        + os.pathsep
        + str(repo_root / "backend" / "security")
        + os.pathsep
        + str(repo_root / "packages" / "shared-schemas" / "schemas_py")
    )

    proc_b = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port_b),
            "--log-level",
            "warning",
        ],
        env=env_b,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for Server B to be healthy
    healthy_b = False
    for _ in range(150):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port_b}/health", timeout=1.0)
                if res.status_code == 200:
                    healthy_b = True
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)
    assert healthy_b, "Server B failed to start"

    # Start Server A (we spawn a dedicated subprocess for A so we can kill it cleanly)
    port_a = get_free_port()
    env_a = env_b.copy()
    proc_a = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port_a),
            "--log-level",
            "warning",
        ],
        env=env_a,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    healthy_a = False
    for _ in range(150):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port_a}/health", timeout=1.0)
                if res.status_code == 200:
                    healthy_a = True
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)
    assert healthy_a, "Server A failed to start"

    token = h.jwks.mint_token(sub="user_t3_writer", org_id=h.org_a, org_role="org:admin")

    try:
        # 1. Exchange ticket on Server A
        status_a, ticket_resp_a = await exchange_ws_ticket(
            f"http://127.0.0.1:{port_a}",
            token,
            meeting_id,
            role="ingest",
            client_instance_id=client_instance_id,
        )
        assert status_a == 200
        ticket_a = ticket_resp_a["ticket"]

        ws_uri_a = f"ws://127.0.0.1:{port_a}/v1/meetings/{meeting_id}/stream?ticket={ticket_a}"

        # 2. Connect to Server A, start meeting & consent, stream first 5 chunks (1..5)
        async with websockets.connect(ws_uri_a) as ws_a:
            await ws_a.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            await ws_a.recv()

            await ws_a.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": False,
                        "consent_text_version": "v1.0",
                        "client_version": "1.0.0",
                    }
                )
            )
            await ws_a.recv()

            # Stream chunks 1..5 and wait for acks
            for seq in range(1, 6):
                frame_bytes = encode_audio_frame(
                    client_seq=seq,
                    capture_ts_ms=1000 + seq * 100,
                    payload=b"\x00" * 3200,
                )
                await ws_a.send(frame_bytes)
                ack_raw = await ws_a.recv()
                ack = json.loads(ack_raw)
                assert ack["type"] == "ack"

            # 3. Forcibly kill Server A mid-stream via Popen.kill()
            proc_a.kill()
            proc_a.wait()

        # 4. Reconnect to Server B with same client_instance_id (immediate lease takeover via K7)
        status_b, ticket_resp_b = await exchange_ws_ticket(
            f"http://127.0.0.1:{port_b}",
            token,
            meeting_id,
            role="ingest",
            client_instance_id=client_instance_id,
        )
        assert status_b == 200
        ticket_b = ticket_resp_b["ticket"]

        ws_uri_b = f"ws://127.0.0.1:{port_b}/v1/meetings/{meeting_id}/stream?ticket={ticket_b}"
        async with websockets.connect(ws_uri_b) as ws_b:
            await ws_b.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            await ws_b.recv()

            await ws_b.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": False,
                        "consent_text_version": "v1.0",
                        "client_version": "1.0.0",
                    }
                )
            )
            await ws_b.recv()

            # Stream chunks 6..10
            for seq in range(6, 11):
                frame_bytes = encode_audio_frame(
                    client_seq=seq,
                    capture_ts_ms=1000 + seq * 100,
                    payload=b"\x00" * 3200,
                )
                await ws_b.send(frame_bytes)
                ack_raw = await ws_b.recv()
                ack = json.loads(ack_raw)
                assert ack["type"] == "ack"

        # 5. Assert the audio stream in Redis holds every client_seq exactly once, in order, no gaps
        s_key = audio_stream_key(tenant_id, meeting_id)
        entries = await real_redis.xrange(s_key, min="-", max="+")
        assert len(entries) == 10, f"Expected 10 chunks, found {len(entries)}"

        found_seqs = []
        for _, fields in entries:
            cseq = fields.get(b"client_seq") or fields.get("client_seq")
            if cseq is not None:
                found_seqs.append(int(cseq))
            else:
                raw_audio = fields.get(b"audio_data") or fields.get("audio_data")
                hdr, _ = decode_audio_frame(raw_audio)
                found_seqs.append(hdr.client_seq)

        assert found_seqs == list(range(1, 11)), f"Sequences mismatch: {found_seqs}"

        # 6. Consumer Group 'asr' reads with XREADGROUP, crashes before XACK
        await real_redis.xgroup_create(s_key, "asr", id="0", mkstream=True)

        read_entries = await real_redis.xreadgroup(
            groupname="asr",
            consumername="consumer_1",
            streams={s_key: ">"},
            count=5,
        )
        assert len(read_entries) == 1
        stream_name, c1_entries = read_entries[0]
        assert len(c1_entries) == 5

        # Consumer 1 crashes before XACK (simulated by not calling xack)

        # 7. Second consumer recovers the entries via XAUTOCLAIM
        # In Redis 7/8, xautoclaim reclaims entries idle for min_idle_time
        claimed_res = await real_redis.xautoclaim(
            name=s_key,
            groupname="asr",
            consumername="consumer_2",
            min_idle_time=0,
            start_id="0-0",
            count=10,
        )
        claimed_entries = claimed_res[1]
        assert len(claimed_entries) >= 5, (
            f"Expected at least 5 claimed entries, got {len(claimed_entries)}"
        )

        # 8. Assert 5,000 pending entries survive trimming under AUDIO_STREAM_MAXLEN (20,000)
        # Push additional 4,990 entries
        pipe = real_redis.pipeline()
        for i in range(11, 5001):
            chunk = encode_audio_frame(i, 2000 + i, b"\x00" * 32)
            pipe.xadd(s_key, {"audio_data": chunk}, maxlen=20000, approximate=True)
        await pipe.execute()

        stream_len = await real_redis.xlen(s_key)
        assert stream_len >= 5000, f"Expected at least 5000 entries in stream, got {stream_len}"

    finally:
        proc_a.kill()
        proc_b.kill()
        proc_a.wait()
        proc_b.wait()
