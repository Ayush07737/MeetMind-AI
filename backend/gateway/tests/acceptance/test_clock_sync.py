"""Acceptance Test: Gateway Clock Synchronization & Correction (Q1 Spec §3, §4).

Verifies against real uvicorn, real Redis, and Neon:
1. SNTP handshake: client sends clock_sync (t1), gateway replies with clock_sync_reply (t1, t2, t3).
2. Clock report: client reports offset_ms and rtt_ms -> gateway writes to Redis meeting hash
   and logs 'clock_sync_established' in Neon audit log.
3. Clock correction: client clock lies by 2.5s -> gateway translates capture_ts_ms to
   capture_server_ms in Redis audio stream within 100ms of true server time.
4. Skew detection: subsequent clock_report with > 500ms shift logs 'clock_skew_detected' in Neon.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key, meeting_session_key
from meetmind_schemas.events import encode_audio_frame

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.mark.asyncio
async def test_gateway_clock_synchronization_and_skew_correction(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_clk_{uuid.uuid4().hex[:8]}"
    tenant_id = h.tenant_a
    user_id = "user_clk_tester"

    # 1. Mint token and exchange for WS ticket
    token = h.jwks.mint_token(sub=user_id, org_id=h.org_a, org_role="org:admin")
    status, ticket_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200, f"Ticket exchange failed: {ticket_resp}"
    ticket = ticket_resp["ticket"]

    ws_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    stream_key = audio_stream_key(tenant_id, meeting_id)
    session_key = meeting_session_key(tenant_id, meeting_id)

    async with websockets.connect(ws_uri) as ws:
        # Step 1: meeting_start
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        resp1 = json.loads(await ws.recv())
        assert resp1.get("status") == "accepted"

        # Step 2: clock_sync probe (SNTP handshake)
        client_t1 = int(time.time() * 1000) + 2500  # Client clock is 2.5s ahead
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "clock_sync",
                    "t1": client_t1,
                }
            )
        )
        sync_reply = json.loads(await ws.recv())
        assert sync_reply.get("action") == "clock_sync_reply"
        assert sync_reply.get("t1") == client_t1
        assert "t2" in sync_reply and "t3" in sync_reply
        assert sync_reply["t2"] > 0 and sync_reply["t3"] >= sync_reply["t2"]

        # Step 3: client computes and reports clock offset (-2500ms to correct client to server)
        skew_offset = -2500
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "clock_report",
                    "offset_ms": skew_offset,
                    "rtt_ms": 36,
                    "samples": 3,
                }
            )
        )
        report_ack = json.loads(await ws.recv())
        assert report_ack.get("status") == "accepted"

        # Verify Redis session hash carries clock_offset_ms
        redis_offset = await real_redis.hget(session_key, "clock_offset_ms")
        assert redis_offset is not None
        assert int(redis_offset.decode() if isinstance(redis_offset, bytes) else redis_offset) == skew_offset

        # Verify initial audit record in Neon
        async with h.tenant_conn(tenant_id) as conn:
            rows = await conn.fetch(
                f'SELECT event_type, payload FROM "{h.schema_a}".audit_events '
                f"WHERE event_type = 'clock_sync_established' ORDER BY id DESC LIMIT 1"
            )
            assert len(rows) == 1
            payload = json.loads(rows[0]["payload"]) if isinstance(rows[0]["payload"], str) else rows[0]["payload"]
            assert payload["meeting_id"] == meeting_id
            assert payload["clock_offset_ms"] == skew_offset
            assert payload["rtt_ms"] == 36

        # Step 4: Confirm consent
        await ws.send(
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
        consent_ack = json.loads(await ws.recv())
        assert consent_ack.get("status") == "accepted"

        # Step 5: Send audio frame with client timestamp skewed by +2500ms
        server_now_ms = int(datetime.now(UTC).timestamp() * 1000)
        client_capture_ts = server_now_ms + 2500
        frame_bytes = encode_audio_frame(
            client_seq=1,
            capture_ts_ms=client_capture_ts,
            payload=b"\x00" * 3200,
        )
        await ws.send(frame_bytes)
        audio_ack = json.loads(await ws.recv())
        assert audio_ack.get("status") == "acked"
        assert audio_ack.get("seq") == 1

        # Inspect Redis audio stream: capture_server_ms must be corrected to server truth
        entries = await real_redis.xrange(stream_key, "-", "+")
        assert len(entries) == 1
        entry_fields = entries[0][1]

        raw_cap_ts = entry_fields.get(b"capture_ts_ms") or entry_fields.get("capture_ts_ms")
        raw_cap_srv = entry_fields.get(b"capture_server_ms") or entry_fields.get("capture_server_ms")
        cap_ts = int(raw_cap_ts.decode() if isinstance(raw_cap_ts, bytes) else raw_cap_ts)
        cap_srv = int(raw_cap_srv.decode() if isinstance(raw_cap_srv, bytes) else raw_cap_srv)

        assert cap_ts == client_capture_ts
        # capture_server_ms = capture_ts_ms + clock_offset_ms = (server_now + 2500) - 2500 = server_now
        assert abs(cap_srv - server_now_ms) <= 100

        # Step 6: Trigger skew detection (> 500ms shift from -2500ms to -1800ms)
        new_offset = -1800
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "clock_report",
                    "offset_ms": new_offset,
                    "rtt_ms": 42,
                    "samples": 3,
                }
            )
        )
        skew_ack = json.loads(await ws.recv())
        assert skew_ack.get("status") == "accepted"

        # Verify clock_skew_detected in Neon
        async with h.tenant_conn(tenant_id) as conn:
            rows = await conn.fetch(
                f'SELECT event_type, payload FROM "{h.schema_a}".audit_events '
                f"WHERE event_type = 'clock_skew_detected' ORDER BY id DESC LIMIT 1"
            )
            assert len(rows) == 1
            payload = json.loads(rows[0]["payload"]) if isinstance(rows[0]["payload"], str) else rows[0]["payload"]
            assert payload["meeting_id"] == meeting_id
            assert payload["previous_offset_ms"] == -2500
            assert payload["new_offset_ms"] == -1800
            assert payload["delta_ms"] == 700

