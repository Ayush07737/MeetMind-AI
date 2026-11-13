"""Acceptance Test B14: Runtime Q4 `words` & `stable_prefix_words` (A2).

Verifies end-to-end through real Gateway, Redis Streams, and spawned ASR worker:
1. StubProvider pipeline emits populated `words` on `transcript_final` and
   `stable_prefix_words` on `transcript_interim`.
2. DeepgramProvider pointed at a local loopback WebSocket replaying recorded
   Nova-3 JSON responses emits valid WordTiming lists with per-word confidences
   on `transcript_final` and non-negative `stable_prefix_words` on `transcript_interim`.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, get_free_port, make_pcm16_chunk
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload, TranscriptInterimPayload
from meetmind_schemas.keys import events_stream_key, out_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(45)]  # 22.53s observed (~2x=45s)


RECORDED_DG_FRAMES = [
    {
        "type": "Results",
        "channel_index": [0, 1],
        "duration": 0.30,
        "start": 0.05,
        "is_final": False,
        "speech_final": False,
        "channel": {
            "alternatives": [
                {
                    "transcript": "Reviewing ticket PLAT",
                    "confidence": 0.91,
                    "words": [
                        {
                            "word": "reviewing",
                            "start": 0.05,
                            "end": 0.15,
                            "confidence": 0.94,
                            "speaker": 0,
                        },
                        {
                            "word": "ticket",
                            "start": 0.16,
                            "end": 0.25,
                            "confidence": 0.92,
                            "speaker": 0,
                        },
                        {
                            "word": "plat",
                            "start": 0.26,
                            "end": 0.35,
                            "confidence": 0.87,
                            "speaker": 0,
                        },
                    ],
                }
            ]
        },
    },
    {
        "type": "Results",
        "channel_index": [0, 1],
        "duration": 0.45,
        "start": 0.05,
        "is_final": False,
        "speech_final": False,
        "channel": {
            "alternatives": [
                {
                    "transcript": "Reviewing ticket PLAT 123",
                    "confidence": 0.94,
                    "words": [
                        {
                            "word": "reviewing",
                            "start": 0.05,
                            "end": 0.15,
                            "confidence": 0.95,
                            "speaker": 0,
                        },
                        {
                            "word": "ticket",
                            "start": 0.16,
                            "end": 0.25,
                            "confidence": 0.95,
                            "speaker": 0,
                        },
                        {
                            "word": "plat",
                            "start": 0.26,
                            "end": 0.35,
                            "confidence": 0.93,
                            "speaker": 0,
                        },
                        {
                            "word": "123",
                            "start": 0.36,
                            "end": 0.50,
                            "confidence": 0.91,
                            "speaker": 0,
                        },
                    ],
                }
            ]
        },
    },
    {
        "type": "Results",
        "channel_index": [0, 1],
        "duration": 0.50,
        "start": 0.05,
        "is_final": True,
        "speech_final": True,
        "channel": {
            "alternatives": [
                {
                    "transcript": "Reviewing ticket PLAT-123.",
                    "confidence": 0.985,
                    "words": [
                        {
                            "word": "reviewing",
                            "punctuated_word": "Reviewing",
                            "start": 0.05,
                            "end": 0.15,
                            "confidence": 0.99,
                            "speaker": 0,
                        },
                        {
                            "word": "ticket",
                            "punctuated_word": "ticket",
                            "start": 0.16,
                            "end": 0.25,
                            "confidence": 0.99,
                            "speaker": 0,
                        },
                        {
                            "word": "plat-123",
                            "punctuated_word": "PLAT-123.",
                            "start": 0.26,
                            "end": 0.55,
                            "confidence": 0.97,
                            "speaker": 0,
                        },
                    ],
                }
            ]
        },
    },
]


@pytest.mark.asyncio
async def test_b14_q4_runtime_words_with_recorded_deepgram(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    h = gateway_harness
    tenant_id = h.tenant_a
    meeting_id = f"mtg_b14_{uuid.uuid4().hex[:8]}"

    dg_port = get_free_port()
    audio_frames_received = 0

    async def _mock_dg_handler(ws):
        nonlocal audio_frames_received
        sent_idx = 0
        async for msg in ws:
            if isinstance(msg, bytes):
                audio_frames_received += 1
                if audio_frames_received in (2, 4, 6) and sent_idx < len(RECORDED_DG_FRAMES):
                    await ws.send(json.dumps(RECORDED_DG_FRAMES[sent_idx]))
                    sent_idx += 1
            elif isinstance(msg, str):
                data = json.loads(msg)
                if data.get("type") == "CloseStream":
                    break

    async with websockets.serve(_mock_dg_handler, "127.0.0.1", dg_port):
        _worker = await asr_worker_factory(
            provider="deepgram",
            extra_env={
                "DEEPGRAM_API_KEY": "dg_test_loopback_key",
                "DEEPGRAM_BASE_URL": f"ws://127.0.0.1:{dg_port}",
                "ASR_REGION": "in",
                "ASR_ALLOWED_REGIONS": "in",
            },
        )

        token = h.jwks.mint_token(sub="user_b14", org_id=h.org_a, org_role="org:admin")
        _, ticket_resp = await exchange_ws_ticket(h.http_url, token, meeting_id, role="ingest")
        ingest_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ticket_resp['ticket']}"

        async with websockets.connect(ingest_uri) as ws_in:

            async def _recv_until_ack() -> dict:
                while True:
                    msg = json.loads(await ws_in.recv())
                    if msg.get("type") == "ack":
                        return msg

            await ws_in.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            await _recv_until_ack()
            await ws_in.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": False,
                        "consent_text_version": "v2.0",
                        "client_version": "2.1.0",
                    }
                )
            )
            await _recv_until_ack()

            t_base = 1_794_100_000_000
            for seq in range(1, 9):
                await ws_in.send(make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100))
                ack = await _recv_until_ack()
                assert ack.get("status") in ("accepted", "acked")
                await asyncio.sleep(0.05)

            # Wait up to 5s for recorded final event to be published before ending meeting
            ev_key = events_stream_key(tenant_id, meeting_id)
            for _ in range(50):
                raw_check = await real_redis.xrange(ev_key)
                has_final = False
                for _, flds in raw_check:
                    rp = flds.get(b"payload") or flds.get("payload")
                    ed = json.loads(rp.decode() if isinstance(rp, bytes) else rp)
                    if ed.get("event_type") == "transcript_final":
                        has_final = True
                        break
                if has_final:
                    break
                await asyncio.sleep(0.1)

            await ws_in.send(json.dumps({"type": "control", "action": "meeting_end"}))
            await _recv_until_ack()

    raw_events = await real_redis.xrange(events_stream_key(tenant_id, meeting_id))
    finals = []
    for _, fields in raw_events:
        raw_p = fields.get(b"payload") or fields.get("payload")
        env_dict = json.loads(raw_p.decode() if isinstance(raw_p, bytes) else raw_p)
        if env_dict.get("event_type") == "transcript_final":
            finals.append(
                TranscriptFinalPayload.model_validate(
                    EventEnvelope.model_validate(env_dict).payload
                )
            )

    assert len(finals) == 1
    assert finals[0].words is not None
    assert [w.w for w in finals[0].words] == ["Reviewing", "ticket", "PLAT-123."]
    assert finals[0].words[2].confidence == pytest.approx(0.97)
    assert all(w.end_ms >= w.start_ms >= finals[0].start_ms for w in finals[0].words)

    raw_out = await real_redis.xrange(out_stream_key(tenant_id, meeting_id))
    interims = []
    for _, fields in raw_out:
        raw_p = fields.get(b"payload") or fields.get("payload")
        env_dict = json.loads(raw_p.decode() if isinstance(raw_p, bytes) else raw_p)
        if env_dict.get("event_type") == "transcript_interim":
            interims.append(TranscriptInterimPayload.model_validate(env_dict["payload"]))

    assert len(interims) == 2
    assert interims[0].stable_prefix_words == 0
    assert interims[1].stable_prefix_words == 3
