"""Acceptance Test B1: End-to-End ASR Pipeline Integration.

Verifies:
- Ingest PCM audio through the real gateway after consent
- ASR worker processes stream and produces:
  - transcript_final on the internal bus (mm:{tenant}:events:{meeting})
  - transcript_interim on the client out stream (mm:{tenant}:out:{meeting})
- Events are schema-valid, ordered, and have unique deterministic utterance_ids
- A real /listen client receives interim transcripts and NEVER final transcripts
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload, TranscriptInterimPayload
from meetmind_schemas.keys import events_stream_key, out_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]


@pytest.mark.asyncio
async def test_b1_end_to_end_streaming(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    h = gateway_harness
    tenant_id = h.tenant_a
    meeting_id = f"mtg_b1_{uuid.uuid4().hex[:8]}"

    # 1. Spawn real ASR worker subprocess running StubProvider
    _worker = await asr_worker_factory(provider="stub")

    # 2. Tickets for ingest and listener
    admin_token = h.jwks.mint_token(sub="admin_b1", org_id=h.org_a, org_role="org:admin")
    listen_token = h.jwks.mint_token(sub="listener_b1", org_id=h.org_a, org_role="org:admin")

    _, ingest_ticket_resp = await exchange_ws_ticket(
        h.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]

    _, listen_ticket_resp = await exchange_ws_ticket(
        h.http_url, listen_token, meeting_id, role="listen"
    )
    listen_ticket = listen_ticket_resp["ticket"]

    ingest_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"
    listen_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/listen?ticket={listen_ticket}"

    received_listener_events: list[dict] = []
    listener_stop = asyncio.Event()

    async def _listener_loop():
        async with websockets.connect(listen_uri) as ws:
            while not listener_stop.is_set():
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    data = json.loads(msg)
                    received_listener_events.append(data)
                except TimeoutError:
                    continue
                except Exception:
                    break

    listener_task = asyncio.create_task(_listener_loop())

    try:
        async with websockets.connect(ingest_uri) as ws_ingest:
            # 3. Control handshake: meeting_start + consent_confirmed
            await ws_ingest.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            ack_start = json.loads(await ws_ingest.recv())
            assert ack_start["status"] == "accepted"

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
            raw_consent_ack = await ws_ingest.recv()
            ack_consent = json.loads(raw_consent_ack)
            assert ack_consent.get("status") == "accepted", f"Unexpected ack_consent: {ack_consent}"

            async def recv_ack(ws):
                while True:
                    m = json.loads(await ws.recv())
                    if m.get("type") == "ack":
                        return m

            # 4. Stream 12 chunks of PCM audio (1200ms total, triggers finals at chunk 5 and 10)
            t_base = 1_000_000
            for seq in range(1, 13):
                frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
                await ws_ingest.send(frame)
                ack = await recv_ack(ws_ingest)
                assert ack.get("status") in ("accepted", "acked"), f"Unexpected frame ack: {ack}"
                await asyncio.sleep(0.02)

            # Give pipeline time to process
            await asyncio.sleep(1.5)

    finally:
        listener_stop.set()
        await asyncio.sleep(0.2)
        listener_task.cancel()

    # 5. Assertions on internal events stream (transcript_final)
    ev_key = events_stream_key(tenant_id, meeting_id)
    raw_events = await real_redis.xrange(ev_key)
    assert len(raw_events) > 0, "No events found on internal bus"

    finals: list[dict] = []
    for _, fields in raw_events:
        payload_raw = fields.get(b"payload") or fields.get("payload")
        assert payload_raw is not None
        env_dict = json.loads(
            payload_raw.decode() if isinstance(payload_raw, bytes) else payload_raw
        )
        if env_dict.get("event_type") == "transcript_final":
            finals.append(env_dict)

    assert len(finals) >= 2, f"Expected at least 2 finals, got {len(finals)}"

    # Validate final schema, ordering, and unique utterance_id
    seen_uids = set()
    prev_start_ms = -1
    for f_item in finals:
        env = EventEnvelope.model_validate(f_item)
        payload = TranscriptFinalPayload.model_validate(env.payload)

        assert len(payload.utterance_id) == 24
        assert payload.utterance_id not in seen_uids
        seen_uids.add(payload.utterance_id)

        assert payload.start_ms >= prev_start_ms
        prev_start_ms = payload.start_ms
        assert payload.end_ms > payload.start_ms
        assert len(payload.text) > 0

    # 6. Assertions on out stream (transcript_interim)
    out_key = out_stream_key(tenant_id, meeting_id)
    raw_out = await real_redis.xrange(out_key)
    assert len(raw_out) > 0, "No interims found on out stream"

    interim_count = 0
    for _, fields in raw_out:
        p_raw = fields.get(b"payload") or fields.get("payload")
        assert p_raw is not None
        env_dict = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
        if env_dict.get("event_type") == "transcript_interim":
            interim_count += 1
            env = EventEnvelope.model_validate(env_dict)
            TranscriptInterimPayload.model_validate(env.payload)

    assert interim_count > 0, "Expected interim events in out stream"

    # 7. Assertions on /listen client received events
    assert len(received_listener_events) > 0, "Listener received no events"
    for client_ev in received_listener_events:
        ev_type = client_ev.get("event_type")
        # Invariant: /listen client receives interims and NEVER finals
        assert ev_type != "transcript_final", "Listener MUST NEVER receive transcript_final"
        assert ev_type in ("transcript_interim", "transcript_service_degraded")
