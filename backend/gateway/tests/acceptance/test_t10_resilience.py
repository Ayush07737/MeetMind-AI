"""Acceptance Test T10: Resilience — Subscriber Recovery via last_event_id.

Verifies:
1. Listener's Redis subscription interrupted / reconnects via last_event_id:
   delivery resumes with zero loss.
2. Ingest stream during brief Redis interruption:
   Client receives close/error 1011, reconnects, retransmits, and all chunks
   are recovered without gaps.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key, out_stream_key
from meetmind_schemas.events import encode_audio_frame

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.asyncio
async def test_t10_resilience_subscriber_recovery_and_retransmit(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t10_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a
    token = h.jwks.mint_token(sub="user_t10", org_id=h.org_a, org_role="org:admin")

    client_instance_id = str(uuid.uuid4())

    # 1. Start Ingest connection, meeting_start, consent_confirmed
    st_w, ticket_w_resp = await exchange_ws_ticket(
        h.http_url, token, meeting_id, role="ingest", client_instance_id=client_instance_id
    )
    assert st_w == 200

    consent_payload = {
        "type": "control",
        "action": "consent_confirmed",
        "consent_type": "audio_capture",
        "external_participants": True,
        "jurisdiction_hint": "US",
        "consent_text_version": "v1.0",
        "client_version": "1.0.0",
    }

    ws_uri_w = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_w_resp['ticket']}"
    async with websockets.connect(ws_uri_w) as ws_w:
        await ws_w.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        ack_start = json.loads(await ws_w.recv())
        assert ack_start.get("type") == "ack"
        await ws_w.send(json.dumps(consent_payload))
        ack_consent = json.loads(await ws_w.recv())
        assert ack_consent.get("type") == "ack"

        # Part (a): Listener connects, gets first event, drops, reconnects via last_event_id
        st_l, ticket_l_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="listen")
        assert st_l == 200

        out_key = out_stream_key(tenant_id, meeting_id)

        ticket_l = ticket_l_resp["ticket"]
        ws_uri_l = f"{h.ws_url}/v1/meetings/{meeting_id}/listen?ticket={ticket_l}&last_event_id=0-0"
        async with websockets.connect(ws_uri_l) as ws_l:
            # Publish event 1 while listener is connected
            ev1_payload = {
                "event_type": "intelligence_event",
                "payload": {"summary": "part 1"},
            }
            entry1_id = await real_redis.xadd(
                out_key,
                {
                    "event_type": "intelligence_event",
                    "payload": json.dumps(ev1_payload),
                    "meeting_id": meeting_id,
                },
            )
            entry1_id_str = (
                entry1_id.decode("utf-8") if isinstance(entry1_id, bytes) else str(entry1_id)
            )

            msg1_raw = await asyncio.wait_for(ws_l.recv(), timeout=5.0)
            msg1 = json.loads(msg1_raw)
            assert msg1["payload"]["summary"] == "part 1"

            # Execute CLIENT KILL on the redis listener connection
            clients = await real_redis.client_list()
            for c in clients:
                c_addr = c.get("addr") or c.get(b"addr")
                if c_addr:
                    addr_str = c_addr.decode("utf-8") if isinstance(c_addr, bytes) else str(c_addr)
                    with contextlib.suppress(Exception):
                        await real_redis.execute_command("CLIENT", "KILL", "ADDR", addr_str)

        # Publish event 2 while listener was disconnected
        ev2_payload = {
            "event_type": "intelligence_event",
            "payload": {"summary": "part 2 resumed"},
        }
        await real_redis.xadd(
            out_key,
            {
                "event_type": "intelligence_event",
                "payload": json.dumps(ev2_payload),
                "meeting_id": meeting_id,
            },
        )

        # Reconnect listener with last_event_id=entry1_id_str
        st_l2, ticket_l2_resp = await exchange_ws_ticket(
            h.http_url, token, meeting_id, role="listen"
        )
        assert st_l2 == 200

        ticket_l2 = ticket_l2_resp["ticket"]
        ws_uri_l2 = (
            f"{h.ws_url}/v1/meetings/{meeting_id}/listen"
            f"?ticket={ticket_l2}&last_event_id={entry1_id_str}"
        )
        async with websockets.connect(ws_uri_l2) as ws_l2:
            msg2_raw = await asyncio.wait_for(ws_l2.recv(), timeout=5.0)
            msg2 = json.loads(msg2_raw)
            assert msg2["payload"]["summary"] == "part 2 resumed"

        # Drain any outbound events that arrived on ws_w before sending audio
        while True:
            try:
                await asyncio.wait_for(ws_w.recv(), timeout=0.1)
            except TimeoutError:
                break

        # Part (b): Ingest retransmit recovery across interruption
        # If running against compose Redis, trigger interruption via docker stop/start
        is_docker_redis = False
        import shutil
        import subprocess

        if shutil.which("docker"):
            try:
                check_proc = subprocess.run(
                    ["docker", "ps", "--filter", "name=meetmind-redis", "-q"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if check_proc.returncode == 0 and check_proc.stdout.strip():
                    subprocess.run(["docker", "stop", "meetmind-redis"], check=True, timeout=10)
                    is_docker_redis = True
            except Exception:
                pass

        try:
            # Sending audio frame during Redis interruption triggers INTERNAL_ERROR / 1011
            chunk_drop = encode_audio_frame(
                client_seq=1, capture_ts_ms=1100, payload=b"\x00" * 3200
            )
            await ws_w.send(chunk_drop)
            err = {}
            for _ in range(10):
                try:
                    err_raw = await asyncio.wait_for(ws_w.recv(), timeout=10.0)
                    msg = json.loads(err_raw)
                    if msg.get("type") == "error":
                        err = msg
                        break
                except Exception:
                    break
            assert err.get("type") == "error"
            assert err.get("code") == "INTERNAL_ERROR"
        finally:
            if is_docker_redis:
                subprocess.run(["docker", "start", "meetmind-redis"], check=True, timeout=10)
                for _ in range(30):
                    try:
                        await real_redis.ping()
                        break
                    except Exception:
                        await asyncio.sleep(0.2)

    # Client reconnects to continue streaming and retransmit chunks 1..10
    st_w2, ticket_w2_resp = await exchange_ws_ticket(
        h.http_url, token, meeting_id, role="ingest", client_instance_id=client_instance_id
    )
    assert st_w2 == 200

    ws_uri_w2 = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_w2_resp['ticket']}"
    async with websockets.connect(ws_uri_w2) as ws_w2:
        await ws_w2.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        m_start_ack = {}
        received_msgs = []
        for _ in range(10):
            try:
                raw_m = await asyncio.wait_for(ws_w2.recv(), timeout=5.0)
                msg = json.loads(raw_m)
                received_msgs.append(msg)
                if msg.get("type") == "ack":
                    m_start_ack = msg
                    break
            except Exception as e:
                received_msgs.append(f"exception: {type(e).__name__}: {e}")
                break
        assert m_start_ack.get("type") == "ack", (
            f"Unexpected msgs on ws_w2 meeting_start: {received_msgs}"
        )
        await ws_w2.send(json.dumps(consent_payload))
        consent_ack = {}
        for _ in range(10):
            try:
                raw_c = await asyncio.wait_for(ws_w2.recv(), timeout=10.0)
                msg = json.loads(raw_c)
                if msg.get("type") == "ack":
                    consent_ack = msg
                    break
            except Exception:
                break
        assert consent_ack.get("type") == "ack"

        # Retransmit chunks 1..10
        for seq in range(1, 11):
            chunk = encode_audio_frame(
                client_seq=seq, capture_ts_ms=1000 + seq * 100, payload=b"\x00" * 3200
            )
            await ws_w2.send(chunk)
            ack_raw2 = await ws_w2.recv()
            ack2 = json.loads(ack_raw2)
            assert ack2.get("type") == "ack", f"Expected ack for chunk {seq}, got: {ack_raw2}"

    # Verify continuity: chunks 1..10 present in stream with zero gaps
    a_key = audio_stream_key(tenant_id, meeting_id)
    entries = await real_redis.xrange(a_key, min="-", max="+")
    assert len(entries) == 10, f"Expected 10 entries in {a_key}, got {len(entries)}"
    seqs = [
        int(fields[b"client_seq" if b"client_seq" in fields else "client_seq"])
        for _, fields in entries
    ]
    assert seqs == list(range(1, 11))
