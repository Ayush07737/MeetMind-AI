"""Acceptance Test T8: Limits & Idle Watchdog (Real Uvicorn, Redis, Neon).

Verifies:
1. Oversize frame (> 64KB) rejected with 4400 (PROTOCOL_VIOLATION).
2. Idle watchdog triggers before consent, closing with 4408 (IDLE_TIMEOUT).
3. Pre-consent flood of invalid frames rate-limits audit log writes (max 5 rows).
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_schemas.events import WSCloseCode
from websockets.exceptions import ConnectionClosed

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t8_oversize_frame_rejected(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t8_oversize_{uuid.uuid4().hex[:6]}"

    token = h.jwks.mint_token(sub="user_oversize", org_id=h.org_a, org_role="org:admin")
    status, resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200
    ticket = resp["ticket"]

    uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"

    async with websockets.connect(uri, max_size=100 * 1024) as ws:
        # Send 70 KB binary frame (> 64 KB limit)
        oversize_payload = b"X" * (70 * 1024)
        await ws.send(oversize_payload)

        # Receive ErrorFrame first
        resp_raw = await ws.recv()
        resp_data = json.loads(resp_raw)
        assert resp_data["type"] == "error"
        assert "64KB" in resp_data["message"]

        # Then connection closed with 4400 (PROTOCOL_VIOLATION)
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.rcvd is not None
        assert exc.value.rcvd.code == WSCloseCode.PROTOCOL_VIOLATION


@pytest.mark.asyncio
async def test_t8_idle_watchdog_timeout(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t8_idle_{uuid.uuid4().hex[:6]}"

    token = h.jwks.mint_token(sub="user_idle", org_id=h.org_a, org_role="org:admin")
    status, resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200
    ticket = resp["ticket"]

    # Request short idle watchdog timeout for acceptance test
    uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}&idle_timeout=3.0"

    async with websockets.connect(uri) as ws:
        # Send meeting_start (enters PENDING_CONSENT)
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        ack_start = json.loads(await ws.recv())
        assert ack_start["status"] == "accepted"

        # Remain idle without confirming consent. Expect watchdog to fire after ~1.5s
        with pytest.raises(ConnectionClosed) as exc:
            while True:
                await asyncio.wait_for(ws.recv(), timeout=5.0)

        assert exc.value.rcvd is not None
        assert exc.value.rcvd.code == WSCloseCode.IDLE_TIMEOUT


@pytest.mark.asyncio
async def test_t8_pre_consent_flood_audit_rate_limiting(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t8_flood_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a

    token = h.jwks.mint_token(sub="user_flood", org_id=h.org_a, org_role="org:admin")
    status, resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200
    ticket = resp["ticket"]

    uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"

    async with websockets.connect(uri) as ws:
        # Rapid burst of 25 invalid audio chunks before consent
        for _ in range(25):
            await ws.send(b"\x00\x01\x02\x03\x04\x05")

        # Read back error responses
        for _ in range(25):
            err_raw = await ws.recv()
            err_data = json.loads(err_raw)
            assert err_data["type"] == "error"
            assert err_data["code"] == "CONSENT_REQUIRED"

    # Verify Neon audit log: only first 5 rejections recorded in DB
    async with h.tenant_conn(tenant_id) as conn:
        count = await conn.fetchval(
            """
            SELECT count(*)
            FROM audit_events
            WHERE tenant_id = $1
              AND event_type = 'frame_rejected_no_consent'
              AND (payload->>'meeting_id') = $2
            """,
            tenant_id,
            meeting_id,
        )
        assert count == 5, f"Expected exactly 5 rate-limited audit rows, got {count}"
