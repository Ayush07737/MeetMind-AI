"""Acceptance Test T7: Protocol State Machine & Deduplication (Real Uvicorn, Redis, Neon).

Verifies:
1. Pre-start consent rejected with 4400 (PROTOCOL_VIOLATION).
2. Retransmit deduplication: client_seq <= last_acked is acked but NOT re-published.
3. Gap tracking: missing sequences calculated and persisted in gap_size stream field.
4. Frames after meeting_end rejected with ErrorFrame(MEETING_ENDED).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key
from meetmind_schemas.events import WSCloseCode, encode_audio_frame
from websockets.exceptions import ConnectionClosed

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t7_pre_start_consent_violation(
    gateway_harness: ServerHarness,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t7_pre_{uuid.uuid4().hex[:6]}"

    token = h.jwks.mint_token(sub="user_sm_tester", org_id=h.org_a, org_role="org:admin")
    status, resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200
    ticket = resp["ticket"]

    uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"

    # Connect and immediately send consent_confirmed without meeting_start
    async with websockets.connect(uri) as ws:
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
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()

        assert exc.value.rcvd is not None
        assert exc.value.rcvd.code == WSCloseCode.PROTOCOL_VIOLATION


@pytest.mark.asyncio
async def test_t7_deduplication_gap_tracking_and_meeting_end(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t7_flow_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a
    stream_key = audio_stream_key(tenant_id, meeting_id)

    token = h.jwks.mint_token(sub="user_sm_flow", org_id=h.org_a, org_role="org:admin")
    status, resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200
    ticket = resp["ticket"]

    uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"

    async with websockets.connect(uri) as ws:
        # 1. meeting_start
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

        # 2. consent_confirmed
        consent_frame_data = {
            "type": "control",
            "action": "consent_confirmed",
            "consent_type": "audio_capture",
            "external_participants": False,
            "consent_text_version": "v1.0",
            "client_version": "1.0.0",
        }
        await ws.send(json.dumps(consent_frame_data))
        ack_consent = json.loads(await ws.recv())
        assert ack_consent["status"] == "accepted"

        # Duplicate consent_confirmed -> idempotent, acked without re-insertion
        await ws.send(json.dumps(consent_frame_data))
        ack_dup_consent = json.loads(await ws.recv())
        assert ack_dup_consent["status"] == "accepted"

        async with h.tenant_conn(tenant_id) as conn:
            consent_count = await conn.fetchval(
                "SELECT COUNT(*) FROM consent_events WHERE tenant_id = $1 AND meeting_id = $2",
                tenant_id,
                meeting_id,
            )
            assert consent_count == 1

        # 3. Send Frame client_seq=1
        ts1 = int(datetime.now(UTC).timestamp() * 1000)
        pcm1 = b"PCM_AUDIO_SEQ_1"
        frame1 = encode_audio_frame(client_seq=1, capture_ts_ms=ts1, payload=pcm1)
        await ws.send(frame1)

        ack1 = json.loads(await ws.recv())
        assert ack1["type"] == "ack"
        assert ack1["seq"] == 1

        # Check Redis stream has 1 item
        assert await real_redis.xlen(stream_key) == 1

        # 4. Retransmit Frame client_seq=1 (Deduplication test)
        await ws.send(frame1)
        ack_dup = json.loads(await ws.recv())
        assert ack_dup["type"] == "ack"
        assert ack_dup["seq"] == 1

        # Check Redis stream STILL has exactly 1 item (not duplicated!)
        assert await real_redis.xlen(stream_key) == 1

        # 5. Gap tracking: send client_seq=4 (missing seq 2 and 3 -> gap_size=2)
        ts4 = int(datetime.now(UTC).timestamp() * 1000)
        pcm4 = b"PCM_AUDIO_SEQ_4_AFTER_GAP"
        frame4 = encode_audio_frame(client_seq=4, capture_ts_ms=ts4, payload=pcm4)
        await ws.send(frame4)

        ack4 = json.loads(await ws.recv())
        assert ack4["type"] == "ack"
        assert ack4["seq"] == 4

        # Stream now has 2 entries; entry 2 records gap_size=2
        assert await real_redis.xlen(stream_key) == 2
        entries = await real_redis.xrange(stream_key)
        entry4_fields = entries[1][1]
        raw_gap = entry4_fields.get(b"gap_size") or entry4_fields.get("gap_size")
        assert raw_gap in (b"2", "2")

        # 6. meeting_end
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_end",
                }
            )
        )
        ack_end = json.loads(await ws.recv())
        assert ack_end["type"] == "ack"
        assert ack_end["status"] == "accepted"

        # 7. Audio after meeting_end rejected with ErrorFrame(MEETING_ENDED)
        ts_post = int(datetime.now(UTC).timestamp() * 1000)
        post_end_frame = encode_audio_frame(
            client_seq=99,
            capture_ts_ms=ts_post,
            payload=b"POST_END_PCM",
        )
        await ws.send(post_end_frame)

        resp_post = json.loads(await ws.recv())
        assert resp_post["type"] == "error"
        assert resp_post["code"] == "MEETING_ENDED"

        # 8. Connection gracefully closed with 1000 (NORMAL_CLOSURE)
        with pytest.raises(ConnectionClosed) as exc_closed:
            await ws.recv()
        assert exc_closed.value.rcvd is not None
        assert exc_closed.value.rcvd.code == 1000

    # 8. Verify meeting_end audit event written to Neon
    async with h.tenant_conn(tenant_id) as conn:
        audit_row = await conn.fetchrow(
            """
            SELECT event_type, payload
            FROM audit_events
            WHERE tenant_id = $1 AND event_type = 'meeting_ended'
            ORDER BY created_at DESC LIMIT 1
            """,
            tenant_id,
        )
        assert audit_row is not None
        payload = json.loads(audit_row["payload"])
        assert payload["meeting_id"] == meeting_id
