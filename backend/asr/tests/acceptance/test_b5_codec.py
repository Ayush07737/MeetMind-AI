"""Acceptance Test B5: Unsupported Codec Handling (Opus codec=2).

Verifies:
1. Audio frames with codec=2 (Opus) are acked by the gateway.
2. Dropped by ASR service.
3. Exactly ONE transcript_service_degraded event (reason="unsupported_codec", state="degraded")
   is emitted per meeting, not one per frame.
4. No transcript_final events are generated.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket
from meetmind_schemas import encode_audio_frame
from meetmind_schemas.keys import events_stream_key, out_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


def make_opus_chunk(seq: int, ts_ms: int) -> bytes:
    """Make synthetic frame with codec=2 (Opus)."""
    fake_opus_payload = b"\x4f\x70\x75\x73\x00\x01\x02\x03" * 20  # 160 bytes
    return encode_audio_frame(
        client_seq=seq,
        capture_ts_ms=ts_ms,
        payload=fake_opus_payload,
        sample_rate=16000,
        channels=1,
        codec=2,
    )


@pytest.mark.asyncio
async def test_b5_opus_codec_dropped_and_degraded_emitted_once(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Send Opus frames; assert acked by gateway, dropped by ASR, single degraded event."""
    meeting_id = f"mtg_b5_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a

    _worker = await asr_worker_factory(provider="stub")

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b5", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    async with websockets.connect(ingest_uri) as ws_ingest:

        async def recv_ack():
            while True:
                msg = json.loads(await ws_ingest.recv())
                if msg.get("type") == "ack":
                    return msg

        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        await ws_ingest.send(
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
        assert (await recv_ack())["status"] == "accepted"

        # Send 10 Opus frames
        t_base = 5_000_000
        for seq in range(1, 11):
            frame = make_opus_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            # Gateway must accept and ack the frame
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.02)

        # Allow pipeline to process frames
        await asyncio.sleep(2.0)

    # Inspect events stream
    ev_key = events_stream_key(tenant_id, meeting_id)
    out_key = out_stream_key(tenant_id, meeting_id)

    raw_events = await real_redis.xrange(ev_key)
    raw_out = await real_redis.xrange(out_key)

    events = []
    for _, fields in raw_events:
        p_raw = fields.get(b"payload") or fields.get("payload")
        if p_raw:
            events.append(json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw))

    # Assert: Exactly one unsupported_codec degraded event
    codec_events = [
        e
        for e in events
        if e.get("event_type") == "transcript_service_degraded"
        and e["payload"].get("reason") == "unsupported_codec"
    ]
    assert len(codec_events) == 1, (
        f"Expected exactly 1 unsupported_codec degraded event, "
        f"got {len(codec_events)}: {codec_events}"
    )
    assert codec_events[0]["payload"]["state"] == "degraded"

    # Assert: No finals were generated
    finals = [e for e in events if e.get("event_type") == "transcript_final"]
    assert len(finals) == 0, f"Expected 0 finals for Opus stream, got {len(finals)}"

    # Assert: Degraded event was also published to client-facing out stream
    out_codec = []
    for _, fields in raw_out:
        p_raw = fields.get(b"payload") or fields.get("payload")
        if p_raw:
            parsed = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
            if (
                parsed.get("event_type") == "transcript_service_degraded"
                and parsed["payload"].get("reason") == "unsupported_codec"
            ):
                out_codec.append(parsed)
    assert len(out_codec) == 1
