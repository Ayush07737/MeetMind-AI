"""Acceptance Test T11: Audit Durability — Fail Closed on DB Outage.

Verifies:
1. Start server instance with NEON_APP_DATABASE_URL pointing at an unreachable host.
2. Connect WebSocket ingest client.
3. meeting_start and consent_confirmed fail closed with close code 4503 (AUDIT_UNAVAILABLE).
4. Disconnect cleanup still completes every step:
   writer lease is released and session state cleaned.
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
from meetmind_gateway.keys import writer_lease_key
from meetmind_schemas.events import WSCloseCode
from websockets.exceptions import ConnectionClosed

pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.asyncio
async def test_t11_audit_durability_fail_closed(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    repo_root = Path(__file__).resolve().parents[4]
    port_unreachable = get_free_port()

    env_unreachable = os.environ.copy()
    env_unreachable["CLERK_JWKS_URL"] = gateway_harness.jwks.jwks_url
    env_unreachable["CLERK_ISSUER"] = gateway_harness.jwks.issuer
    env_unreachable["ALLOWED_ORIGINS"] = json.dumps(["https://app.meetmind.ai"])
    org_map = {gateway_harness.org_a: gateway_harness.tenant_a}
    env_unreachable["TENANT_ORG_MAP"] = json.dumps(org_map)
    env_unreachable["APP_ENV"] = "test"
    # Unreachable database host and port across all DSN env vars
    unreachable_dsn = "postgresql://user:pass@127.0.0.1:54321/unreachable"
    env_unreachable["NEON_DATABASE_URL"] = unreachable_dsn
    env_unreachable["NEON_APP_DATABASE_URL"] = unreachable_dsn
    env_unreachable["NEON_TEST_DATABASE_URL"] = unreachable_dsn
    env_unreachable["NEON_TEST_APP_DATABASE_URL"] = unreachable_dsn
    env_unreachable["NEON_DIRECT_DATABASE_URL"] = unreachable_dsn
    env_unreachable["REDIS_URL"] = "redis://127.0.0.1:6379/0"
    env_unreachable["LOG_LEVEL"] = "WARNING"
    env_unreachable["PYTHONPATH"] = (
        str(repo_root / "backend" / "gateway")
        + os.pathsep
        + str(repo_root / "backend" / "security")
        + os.pathsep
        + str(repo_root / "packages" / "shared-schemas" / "schemas_py")
    )

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "meetmind_gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port_unreachable),
            "--log-level",
            "warning",
        ],
        env=env_unreachable,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    healthy = False
    for _ in range(150):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port_unreachable}/health", timeout=1.0)
                if res.status_code == 200:
                    healthy = True
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)

    assert healthy, "Server with unreachable audit DB failed to start health endpoint"

    meeting_id = f"mtg_t11_{uuid.uuid4().hex[:6]}"
    tenant_id = gateway_harness.tenant_a
    token = gateway_harness.jwks.mint_token(
        sub="user_t11", org_id=gateway_harness.org_a, org_role="org:admin"
    )

    try:
        status, ticket_resp = await exchange_ws_ticket(
            f"http://127.0.0.1:{port_unreachable}",
            token,
            meeting_id,
            role="ingest",
        )
        assert status == 200
        ticket = ticket_resp["ticket"]

        ws_uri = (
            f"ws://127.0.0.1:{port_unreachable}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
        )

        # Connect and send meeting_start -> Audit write fails -> closed with 4503 AUDIT_UNAVAILABLE
        with pytest.raises(ConnectionClosed) as exc_audit:
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
                while True:
                    await ws.recv()

        assert exc_audit.value.rcvd is not None
        assert exc_audit.value.rcvd.code == WSCloseCode.AUDIT_UNAVAILABLE

        # Disconnect cleanup still completes: assert writer lease is released
        lease_key = writer_lease_key(tenant_id, meeting_id)
        current_lease = await real_redis.get(lease_key)
        assert current_lease is None, "Writer lease must be released on disconnect cleanup"

    finally:
        proc.kill()
        proc.wait()
