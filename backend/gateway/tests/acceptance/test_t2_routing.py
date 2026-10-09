"""Acceptance Test T2: Frame Shape Routing (Real Uvicorn, Redis, Neon).

Verifies:
1. Binary frame -> 18-byte header parsed -> published to Redis audio stream.
2. JSON frame (transcript_chunk) -> published to Redis transcript stream.
3. Proven across all 3 adapter types: chrome_extension, desktop_app, meeting_bot.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_gateway.keys import audio_stream_key, transcript_stream_key
from meetmind_schemas.events import encode_audio_frame

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adapter_type",
    [
        "chrome_extension",
        "desktop_app",
        "meeting_bot",
    ],
)
async def test_t2_frame_routing_all_adapters(
    gateway_harness: ServerHarness,
    real_redis,
    adapter_type: str,
) -> None:
    h = gateway_harness
    meeting_id = f"mtg_t2_{adapter_type}_{uuid.uuid4().hex[:6]}"
    tenant_id = h.tenant_a
    user_id = f"user_t2_{adapter_type}"

    # 1. Exchange WS ticket
    token = h.jwks.mint_token(sub=user_id, org_id=h.org_a, org_role="org:admin")
    status, ticket_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
    assert status == 200, f"Ticket exchange failed: {ticket_resp}"
    ticket = ticket_resp["ticket"]

    ws_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket}"
    audio_key = audio_stream_key(tenant_id, meeting_id)
    transcript_key = transcript_stream_key(tenant_id, meeting_id)

    async with websockets.connect(ws_uri) as ws:
        # 2. meeting_start with adapter_type
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": adapter_type,
                }
            )
        )
        ack_start = json.loads(await ws.recv())
        assert ack_start["type"] == "ack"
        assert ack_start["status"] == "accepted"

        # 3. consent_confirmed
        await ws.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "consent_type": "audio_capture",
                    "external_participants": False,
                    "jurisdiction_hint": "CCPA",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        ack_consent = json.loads(await ws.recv())
        assert ack_consent["type"] == "ack"
        assert ack_consent["status"] == "accepted"

        # 4. Binary audio frame -> audio stream
        pcm_bytes = f"AUDIO_DATA_{adapter_type}".encode("ascii")
        ts_ms = int(datetime.now(UTC).timestamp() * 1000)
        audio_frame = encode_audio_frame(client_seq=1, capture_ts_ms=ts_ms, payload=pcm_bytes)
        await ws.send(audio_frame)

        ack_audio = json.loads(await ws.recv())
        assert ack_audio["type"] == "ack"
        assert ack_audio["seq"] == 1

        # Verify entry in Redis audio stream
        audio_len = await real_redis.xlen(audio_key)
        assert audio_len == 1
        audio_entries = await real_redis.xrange(audio_key)
        audio_fields = audio_entries[0][1]
        raw_seq = audio_fields.get(b"client_seq") or audio_fields.get("client_seq")
        raw_audio = audio_fields.get(b"audio_data") or audio_fields.get("audio_data")
        assert raw_seq in (b"1", "1")
        assert raw_audio == pcm_bytes

        # 5. JSON transcript chunk -> transcript stream
        transcript_text = f"Live transcript chunk from {adapter_type}"
        await ws.send(
            json.dumps(
                {
                    "type": "transcript_chunk",
                    "text": transcript_text,
                    "client_seq": 2,
                }
            )
        )

        ack_tx = json.loads(await ws.recv())
        assert ack_tx["type"] == "ack"
        assert ack_tx["seq"] == 2

        # Verify entry in Redis transcript stream
        tx_len = await real_redis.xlen(transcript_key)
        assert tx_len == 1
        tx_entries = await real_redis.xrange(transcript_key)
        tx_fields = tx_entries[0][1]
        raw_tx_text = tx_fields.get(b"text") or tx_fields.get("text")
        raw_tx_adapter = tx_fields.get(b"adapter_type") or tx_fields.get("adapter_type")
        assert raw_tx_text in (transcript_text.encode(), transcript_text)
        assert raw_tx_adapter in (adapter_type.encode(), adapter_type)
