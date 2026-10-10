"""Acceptance Test T1: Consent Gate Invariants (Real Uvicorn, Redis, Neon).

Verifies:
1. Audio before consent -> rejected with CONSENT_REQUIRED, audit row written, 0 frames in stream.
2. meeting_start + consent_confirmed with extended fields -> persisted to consent_events in Neon.
3. Audio chunk after consent -> accepted, ack received, chunk in Redis audio stream.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key
from meetmind_schemas.events import encode_audio_frame

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
async def test_t1_consent_gate_enforcement(
    gateway_harness: ServerHarness,
    real_redis,
) -> None:
    h = gateway_harness
    import uuid

    meeting_id = f"mtg_t1_{uuid.uuid4().hex[:8]}"
    tenant_id = h.tenant_a
    user_id = "user_t1_tester"

    # 1. Mint token and exchange for WS ticket
    token = h.jwks.mint_token(sub=user_id, org_id=h.org_a, org_role="org:admin")
    status, ticket_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200, f"Ticket exchange failed: {ticket_resp}"
    ticket = ticket_resp["ticket"]

    ws_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    stream_key = audio_stream_key(tenant_id, meeting_id)

    async with websockets.connect(ws_uri) as ws:
        # Step 1: Send raw audio chunk before consent
        raw_audio = b"\x00\x01\x02\x03\x04\x05"
        await ws.send(raw_audio)

        # Receive server response
        resp_raw = await ws.recv()
        resp = json.loads(resp_raw)
        assert resp["type"] == "error"
        assert resp["code"] == "CONSENT_REQUIRED"

        # Verify 0 items in Redis audio stream
        stream_len = await real_redis.xlen(stream_key)
        assert stream_len == 0

        # Verify row written to audit_events in Neon
        async with h.tenant_conn(tenant_id) as conn:
            audit_row = await conn.fetchrow(
                """
                SELECT event_type, payload
                FROM audit_events
                WHERE tenant_id = $1 AND event_type = 'frame_rejected_no_consent'
                ORDER BY created_at DESC LIMIT 1
                """,
                tenant_id,
            )
            assert audit_row is not None
            payload = json.loads(audit_row["payload"])
            assert payload["meeting_id"] == meeting_id
            assert payload["frame_type"] == "audio_chunk"
            assert payload["reason"] == "consent_not_confirmed"

        # Step 2: Send meeting_start control frame
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
        assert ack_start["type"] == "ack"
        assert ack_start["status"] == "accepted"

        # Step 3: Send consent_confirmed with extended fields
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": True,
                    "jurisdiction_hint": "GDPR",
                    "consent_text_version": "v2.0",
                    "client_version": "2.1.0",
                }
            )
        )
        ack_consent = json.loads(await ws.recv())
        assert ack_consent["type"] == "ack"
        assert ack_consent["status"] == "accepted"

        # Verify extended fields persisted to consent_events in Neon
        async with h.tenant_conn(tenant_id) as conn:
            consent_row = await conn.fetchrow(
                """
                SELECT action, external_participants, jurisdiction_hint,
                       consent_text_version, client_version
                FROM consent_events
                WHERE tenant_id = $1 AND meeting_id = $2 AND action = 'granted'
                ORDER BY created_at DESC LIMIT 1
                """,
                tenant_id,
                meeting_id,
            )
            assert consent_row is not None
            assert consent_row["external_participants"] is True
            assert consent_row["jurisdiction_hint"] == "GDPR"
            assert consent_row["consent_text_version"] == "v2.0"
            assert consent_row["client_version"] == "2.1.0"

        # Step 4: Send audio chunk with 18-byte big-endian header and 3,200 bytes REAL PCM payload
        pcm_payload = b"\x00\x01" * 1600  # Exactly 3,200 bytes of PCM audio
        assert len(pcm_payload) == 3200
        ts_ms = int(datetime.now(UTC).timestamp() * 1000)
        framed_audio = encode_audio_frame(
            client_seq=1,
            capture_ts_ms=ts_ms,
            payload=pcm_payload,
        )
        await ws.send(framed_audio)

        ack_audio = json.loads(await ws.recv())
        assert ack_audio["type"] == "ack"
        assert ack_audio["seq"] == 1

        # Verify audio chunk published to Redis stream
        stream_len = await real_redis.xlen(stream_key)
        assert stream_len == 1

        entries = await real_redis.xrange(stream_key)
        assert len(entries) == 1
        fields = entries[0][1]
        assert fields[b"client_seq"] == b"1" or fields.get("client_seq") == "1"
        assert fields[b"audio_data"] == pcm_payload or fields.get("audio_data") == pcm_payload
        stored_bytes = fields.get(b"audio_data") or fields.get("audio_data")
        assert len(stored_bytes) == 3200
