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

import asyncio
import json
import time
import uuid
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key, meeting_session_key
from meetmind_schemas.events import encode_audio_frame

# 31.63s observed (setup+call+teardown); ~2x=63s
pytestmark = [pytest.mark.integration, pytest.mark.timeout(65)]


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
        offset_val = redis_offset.decode() if isinstance(redis_offset, bytes) else redis_offset
        assert int(offset_val) == skew_offset

        # Verify initial audit record in Neon
        async with h.tenant_conn(tenant_id) as conn:
            rows = await conn.fetch(
                f'SELECT event_type, payload FROM "{h.schema_a}".audit_events '
                f"WHERE event_type = 'clock_sync_established' ORDER BY id DESC LIMIT 1"
            )
            assert len(rows) == 1
            raw_payload = rows[0]["payload"]
            payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
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
        raw_cap_srv = entry_fields.get(b"capture_server_ms") or entry_fields.get(
            "capture_server_ms"
        )
        cap_ts = int(raw_cap_ts.decode() if isinstance(raw_cap_ts, bytes) else raw_cap_ts)
        cap_srv = int(raw_cap_srv.decode() if isinstance(raw_cap_srv, bytes) else raw_cap_srv)

        assert cap_ts == client_capture_ts
        # capture_server_ms = capture_ts_ms + clock_offset_ms
        # = (server_now + 2500) - 2500 = server_now
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
            raw_payload = rows[0]["payload"]
            payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
            assert payload["meeting_id"] == meeting_id
            assert payload["previous_offset_ms"] == -2500
            assert payload["new_offset_ms"] == -1800
            assert payload["delta_ms"] == 700

        # Step 7: Send audio frame with extreme offset (+60s into future, R6)
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "clock_report",
                    "offset_ms": 60_000,
                    "rtt_ms": 40,
                    "samples": 3,
                }
            )
        )
        await ws.recv()

        t_send_future = int(datetime.now(UTC).timestamp() * 1000)
        frame_future = encode_audio_frame(
            client_seq=2,
            capture_ts_ms=t_send_future,
            payload=b"\x00" * 3200,
        )
        await ws.send(frame_future)
        await ws.recv()

        # Verify Redis stream entry 2 has capture_server_ms clamped to <= gw_recv
        entries = await real_redis.xrange(stream_key, "-", "+")
        assert len(entries) == 2
        entry_2_fields = entries[1][1]
        raw_cap_srv_2 = entry_2_fields.get(b"capture_server_ms") or entry_2_fields.get(
            "capture_server_ms"
        )
        cap_srv_2 = int(
            raw_cap_srv_2.decode() if isinstance(raw_cap_srv_2, bytes) else raw_cap_srv_2
        )
        assert cap_srv_2 < t_send_future + 50_000
        assert abs(cap_srv_2 - t_send_future) <= 2000

        # Step 8: Send audio frame with extreme offset (-60s into past, R6)
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "clock_report",
                    "offset_ms": -60_000,
                    "rtt_ms": 40,
                    "samples": 3,
                }
            )
        )
        await ws.recv()

        t_send_past = int(datetime.now(UTC).timestamp() * 1000)
        frame_past = encode_audio_frame(
            client_seq=3,
            capture_ts_ms=t_send_past,
            payload=b"\x00" * 3200,
        )
        await ws.send(frame_past)
        await ws.recv()

        entries = await real_redis.xrange(stream_key, "-", "+")
        assert len(entries) == 3
        entry_3_fields = entries[2][1]
        raw_cap_srv_3 = entry_3_fields.get(b"capture_server_ms") or entry_3_fields.get(
            "capture_server_ms"
        )
        cap_srv_3 = int(
            raw_cap_srv_3.decode() if isinstance(raw_cap_srv_3, bytes) else raw_cap_srv_3
        )
        assert cap_srv_3 > t_send_past - 30_000
        assert abs(cap_srv_3 - (t_send_past - 15_000)) <= 2000

        # Verify clock_clamped audit event in Neon (dispatched non-blocking from audio handler)
        async with h.tenant_conn(tenant_id) as conn:
            rows = []
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                rows = await conn.fetch(
                    f'SELECT event_type, payload FROM "{h.schema_a}".audit_events '
                    f"WHERE event_type = 'clock_clamped' ORDER BY id DESC"
                )
                if len(rows) >= 1:
                    break
                await asyncio.sleep(0.1)
            assert len(rows) >= 1
