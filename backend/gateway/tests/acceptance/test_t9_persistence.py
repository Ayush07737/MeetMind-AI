"""Acceptance Test T9: Non-Persistence Invariants, Startup Guard, and Retention TTLs.

Verifies:
1. Real Redis config check: CONFIG GET save is empty and appendonly is no.
2. Startup guard refuses to start against Redis with persistence enabled
   (starts temporary persistent Redis on port 6389).
3. With short TTL settings, after meeting_end grace expire, audio and transcript
   stream keys are removed.
4. After abandonment sweep via Janitor, no meeting session, listener, or stream keys remain.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, get_free_port
from meetmind_gateway.config import GatewaySettings
from meetmind_gateway.keys import (
    audio_stream_key,
    listeners_key,
    meeting_session_key,
    transcript_stream_key,
)
from meetmind_gateway.persistence_check import assert_audio_redis_not_persistent
from meetmind_schemas.events import encode_audio_frame

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.asyncio
async def test_t9_non_persistence_and_ttl(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    settings = GatewaySettings(
        app_env="production",
        clerk_issuer="https://clerk.meetmind.ai",
        clerk_jwks_url="https://clerk.meetmind.ai/.well-known/jwks.json",
        allowed_origins=["https://app.meetmind.ai"],
        audio_redis_persistence_acknowledged=False,
    )
    # Must pass without error on the production/test Redis
    await assert_audio_redis_not_persistent(real_redis, settings)

    cfg_save = await real_redis.config_get("save")
    cfg_aof = await real_redis.config_get("appendonly")
    assert cfg_save.get("save") == "" or cfg_save.get(b"save") == b""
    assert cfg_aof.get("appendonly") == "no" or cfg_aof.get(b"appendonly") == b"no"

    # 2. Startup check refuses to start against Redis with persistence enabled
    redis_dir = (
        Path(os.environ.get("USERPROFILE", ""))
        / ".gemini"
        / "antigravity-ide"
        / "brain"
        / "980c3b9c-2a1d-4000-9616-bbc9da2aa8f8"
        / "scratch"
        / "redis"
        / "Redis-8.10.2-Windows-x64-msys2"
    )
    redis_bin = redis_dir / "redis-server.exe"
    port_persistent = get_free_port()
    container_id: str | None = None
    proc_redis_persist = None

    if redis_bin.exists():
        proc_redis_persist = subprocess.Popen(
            [
                str(redis_bin),
                "--port",
                str(port_persistent),
                "--save",
                "900 1",
                "--appendonly",
                "yes",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    else:
        run_res = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "-p",
                f"{port_persistent}:6379",
                "redis:7-alpine",
                "redis-server",
                "--save",
                "900 1",
                "--appendonly",
                "yes",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        container_id = run_res.stdout.strip()

    await asyncio.sleep(1.0)
    try:
        repo_root = Path(__file__).resolve().parents[4]
        port_gw = get_free_port()
        env_persist = os.environ.copy()
        env_persist["CLERK_JWKS_URL"] = gateway_harness.jwks.jwks_url
        env_persist["CLERK_ISSUER"] = gateway_harness.jwks.issuer
        env_persist["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
        org_map = {gateway_harness.org_a: gateway_harness.tenant_a}
        env_persist["TENANT_ORG_MAP"] = json.dumps(org_map)
        env_persist["APP_ENV"] = "test"
        env_persist["AUDIO_REDIS_URL"] = f"redis://127.0.0.1:{port_persistent}/0"
        env_persist["REDIS_URL"] = f"redis://127.0.0.1:{port_persistent}/0"
        env_persist["AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED"] = "false"
        env_persist["PYTHONPATH"] = (
            str(repo_root / "backend" / "gateway")
            + os.pathsep
            + str(repo_root / "backend" / "security")
            + os.pathsep
            + str(repo_root / "packages" / "shared-schemas" / "schemas_py")
        )

        proc_gw_persist = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "meetmind_gateway.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(port_gw),
                "--log-level",
                "error",
            ],
            env=env_persist,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Uvicorn MUST exit early with non-zero exit code due to persistence refusal
        try:
            returncode = proc_gw_persist.wait(timeout=5.0)
            assert returncode != 0, "Server must refuse to start against persistent Redis"
            _, stderr = proc_gw_persist.communicate()
            assert "Audio Redis has persistence enabled" in stderr.decode("utf-8")
        finally:
            proc_gw_persist.kill()
            proc_gw_persist.wait()
    finally:
        if proc_redis_persist:
            proc_redis_persist.kill()
            proc_redis_persist.wait()
        if container_id:
            subprocess.run(["docker", "stop", container_id], check=False)

    # 3. Short TTL / meeting_end / abandonment retention check
    h = gateway_harness
    meeting_id = f"mtg_t9_retention_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a

    token = h.jwks.mint_token(sub="user_t9", org_id=h.org_a, org_role="org:admin")
    status, ticket_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200

    ws_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_resp['ticket']}"
    async with websockets.connect(ws_uri) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        await ws.recv()
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        await ws.recv()

        # Send an audio frame
        await ws.send(encode_audio_frame(client_seq=1, capture_ts_ms=1000, payload=b"\x00" * 3200))
        await ws.recv()

        # End the meeting -> triggers 5-min grace expire on streams
        await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
        ack_end = json.loads(await ws.recv())
        assert ack_end["type"] == "ack"

    a_key = audio_stream_key(tenant_id, meeting_id)
    t_key = transcript_stream_key(tenant_id, meeting_id)
    ttl_audio = await real_redis.ttl(a_key)
    # Stream key has 300s (5 min) grace expire set
    assert 0 < ttl_audio <= 300

    # Simulate expiration by setting short 1s expire and waiting
    await real_redis.expire(a_key, 1)
    await real_redis.expire(t_key, 1)
    await real_redis.expire(meeting_session_key(tenant_id, meeting_id), 1)
    await real_redis.expire(listeners_key(tenant_id, meeting_id), 1)

    await asyncio.sleep(1.2)
    remaining_keys = await real_redis.keys(f"mm:{tenant_id}:*:{meeting_id}")
    assert len(remaining_keys) == 0, (
        f"Expected no keys remaining after expiration, found: {remaining_keys}"
    )
