"""Unit tests for Q4 Runtime Word Timings & Stable Prefix Words (A2).

Verifies:
- Recorded Deepgram Nova-3 JSON interim & final parsing preserves word timings,
  per-word confidences, punctuated_word fallback, and speaker assignment.
- MeetingPipeline populates `words` on `transcript_final` and computes
  `stable_prefix_words` across evolving `transcript_interim` messages.
- MeetingPipeline.stop() drains `_reconnect_buffer` cleanly (no `_buffer` AttributeError).
- Reconnect origin wall preserves non-negative E2E timestamps across replay bursts.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_asr.config import AsrSettings
from meetmind_asr.pipeline import MeetingPipeline
from meetmind_asr.providers.base import AudioChunk, ProviderEvent, ProviderSession
from meetmind_asr.providers.deepgram import DeepgramProvider
from meetmind_asr.providers.stub import StubProvider
from meetmind_asr.tenant_config import AsrTenantConfig
from meetmind_schemas.events import TranscriptFinalPayload, TranscriptInterimPayload
from pydantic import SecretStr

# Exact Nova-3 JSON shape (recorded response structure from /v1/listen)
RECORDED_DG_INTERIM_1: dict[str, Any] = {
    "type": "Results",
    "channel_index": [0, 1],
    "duration": 0.64,
    "start": 0.20,
    "is_final": False,
    "speech_final": False,
    "channel": {
        "alternatives": [
            {
                "transcript": "Let us ship PR",
                "confidence": 0.912,
                "words": [
                    {"word": "let", "start": 0.20, "end": 0.32, "confidence": 0.96, "speaker": 0},
                    {"word": "us", "start": 0.33, "end": 0.44, "confidence": 0.94, "speaker": 0},
                    {"word": "ship", "start": 0.45, "end": 0.62, "confidence": 0.89, "speaker": 0},
                    {"word": "p", "start": 0.64, "end": 0.80, "confidence": 0.72, "speaker": 0},
                ],
            }
        ]
    },
}

RECORDED_DG_INTERIM_2: dict[str, Any] = {
    "type": "Results",
    "channel_index": [0, 1],
    "duration": 1.05,
    "start": 0.20,
    "is_final": False,
    "speech_final": False,
    "channel": {
        "alternatives": [
            {
                "transcript": "Let us ship today",
                "confidence": 0.935,
                "words": [
                    {"word": "let", "start": 0.20, "end": 0.32, "confidence": 0.96, "speaker": 0},
                    {"word": "us", "start": 0.33, "end": 0.44, "confidence": 0.95, "speaker": 0},
                    {"word": "ship", "start": 0.45, "end": 0.62, "confidence": 0.93, "speaker": 0},
                    {
                        "word": "today",
                        "start": 0.82,
                        "end": 1.25,
                        "confidence": 0.90,
                        "speaker": 0,
                    },
                ],
            }
        ]
    },
}

RECORDED_DG_FINAL: dict[str, Any] = {
    "type": "Results",
    "channel_index": [0, 1],
    "duration": 1.32,
    "start": 0.20,
    "is_final": True,
    "speech_final": True,
    "channel": {
        "alternatives": [
            {
                "transcript": "Let us ship PR-482 today.",
                "confidence": 0.981,
                "words": [
                    {
                        "word": "let",
                        "punctuated_word": "Let",
                        "start": 0.20,
                        "end": 0.32,
                        "confidence": 0.99,
                        "speaker": 0,
                    },
                    {
                        "word": "us",
                        "punctuated_word": "us",
                        "start": 0.33,
                        "end": 0.44,
                        "confidence": 0.99,
                        "speaker": 0,
                    },
                    {
                        "word": "ship",
                        "punctuated_word": "ship",
                        "start": 0.45,
                        "end": 0.62,
                        "confidence": 0.98,
                        "speaker": 0,
                    },
                    {
                        "word": "pr482",
                        "punctuated_word": "PR-482",
                        "start": 0.64,
                        "end": 0.95,
                        "confidence": 0.97,
                        "speaker": 0,
                    },
                    {
                        "word": "today",
                        "punctuated_word": "today.",
                        "start": 0.98,
                        "end": 1.48,
                        "confidence": 0.96,
                        "speaker": None,
                    },
                ],
            }
        ]
    },
}


def _make_fake_redis(captured_xadds: list[tuple[str, dict[str, Any]]]) -> AsyncMock:
    redis = AsyncMock()
    redis.hsetnx = AsyncMock(return_value=1)
    redis.set = AsyncMock(return_value=True)
    redis.xack = AsyncMock(return_value=1)
    redis.xdel = AsyncMock(return_value=1)
    redis.xrevrange = AsyncMock(return_value=[])
    redis.xreadgroup = AsyncMock(return_value=[])

    class _FakePipe:
        def xadd(self, key: Any, fields: dict[str, Any], **kwargs: Any) -> _FakePipe:
            captured_xadds.append((str(key), fields))
            return self

        def expire(self, *args: Any, **kwargs: Any) -> _FakePipe:
            return self

        async def execute(self) -> list[Any]:
            return [b"1-0", True]

    redis.pipeline = MagicMock(return_value=_FakePipe())
    return redis


@pytest.mark.asyncio
async def test_deepgram_parser_populates_normalized_words_and_handles_null_speaker() -> None:
    provider = DeepgramProvider(api_key=SecretStr("mock_key"))
    await provider._handle_message(RECORDED_DG_FINAL, recv_perf=time.perf_counter())
    ev = await provider._event_queue.get()

    assert ev.event_type == "final"
    assert ev.is_final is True
    assert ev.is_stable is True
    assert ev.speaker == 0
    assert len(ev.words) == 5
    assert ev.words[0] == {
        "w": "Let",
        "start_ms": 200,
        "end_ms": 320,
        "confidence": 0.99,
        "speaker": 0,
    }
    assert ev.words[3]["w"] == "PR-482"
    assert ev.words[3]["confidence"] == 0.97
    assert ev.words[4]["speaker"] is None


@pytest.mark.asyncio
async def test_pipeline_populates_stable_prefix_words_and_final_words() -> None:
    captured: list[tuple[str, dict[str, Any]]] = []
    redis = _make_fake_redis(captured)
    dg = DeepgramProvider(api_key=SecretStr("mock_key"))
    cfg = AsrTenantConfig(tenant_id="t_q4", language="en-IN", keyterms=[])
    settings = AsrSettings(_env_file=None, asr_provider="deepgram", deepgram_api_key="mock_key")

    pipeline = MeetingPipeline(
        tenant_id="t_q4",
        meeting_id="mtg_q4_unit",
        worker_id="w_q4",
        redis_client=redis,
        provider=dg,
        tenant_config=cfg,
        settings=settings,
        metrics=MagicMock(),
        auditor=MagicMock(),
    )
    origin_ts = 1_794_000_000_000
    for seq in range(25):
        pipeline._timeline.register_chunk(origin_ts + seq * 100, duration_ms=100)
    pipeline._provider_session_origin_wall = time.time() - 2.5

    # Feed recorded interims and final through Deepgram parser into pipeline handler
    for msg in (RECORDED_DG_INTERIM_1, RECORDED_DG_INTERIM_2, RECORDED_DG_FINAL):
        await dg._handle_message(msg, recv_perf=time.perf_counter())
        ev = await dg._event_queue.get()
        await pipeline._handle_transcript_event(ev)

    assert len(captured) == 3

    env1 = json.loads(captured[0][1]["payload"])
    p1 = TranscriptInterimPayload.model_validate(env1["payload"])
    assert p1.stable_prefix_words == 0  # first hypothesis

    env2 = json.loads(captured[1][1]["payload"])
    p2 = TranscriptInterimPayload.model_validate(env2["payload"])
    assert p2.stable_prefix_words == 3  # "let", "us", "ship" match prior interim

    env3 = json.loads(captured[2][1]["payload"])
    p3 = TranscriptFinalPayload.model_validate(env3["payload"])
    assert p3.words is not None
    assert [w.w for w in p3.words] == ["Let", "us", "ship", "PR-482", "today."]
    assert p3.words[3].confidence == pytest.approx(0.97)
    assert p3.words[4].speaker == "spk_0"
    # Interim prefix buffer resets after final
    assert 0 not in pipeline._prev_interim_tokens

    # Subsequent stable interim must report full token count
    stable_interim_msg = dict(RECORDED_DG_INTERIM_1)
    stable_interim_msg["speech_final"] = True
    await dg._handle_message(stable_interim_msg, recv_perf=time.perf_counter())
    ev_stable = await dg._event_queue.get()
    await pipeline._handle_transcript_event(ev_stable)

    env4 = json.loads(captured[3][1]["payload"])
    p4 = TranscriptInterimPayload.model_validate(env4["payload"])
    assert p4.is_stable is True
    assert p4.stable_prefix_words == 4


@pytest.mark.asyncio
async def test_stop_drains_reconnect_buffer_and_reconnect_sets_non_negative_e2e() -> None:
    captured: list[tuple[str, dict[str, Any]]] = []
    redis = _make_fake_redis(captured)
    stub = StubProvider()
    cfg = AsrTenantConfig(tenant_id="t_stop", language="en", keyterms=[])
    settings = AsrSettings(_env_file=None, asr_provider="stub")
    auditor = MagicMock()
    auditor.record_session_summary_sync = AsyncMock()

    pipeline = MeetingPipeline(
        tenant_id="t_stop",
        meeting_id="mtg_stop_buf",
        worker_id="w_stop",
        redis_client=redis,
        provider=stub,
        tenant_config=cfg,
        settings=settings,
        metrics=MagicMock(),
        auditor=auditor,
    )
    await stub.open(
        ProviderSession(
            tenant_id="t_stop",
            meeting_id="mtg_stop_buf",
            sample_rate=16000,
            channels=1,
        )
    )

    buffered_chunk = AudioChunk(
        data=b"\x00" * 3200,
        client_seq=42,
        capture_ts_ms=1_794_000_004_200,
    )
    pipeline._reconnect_buffer.append(("999-0", buffered_chunk))
    pipeline._audio_read_task = asyncio.create_task(asyncio.sleep(0))
    pipeline._tasks = [pipeline._audio_read_task]

    await pipeline.stop(is_ended=True)
    assert 42 in stub.received_client_seqs
    assert len(pipeline._reconnect_buffer) == 0

    # Verify reconnect origin adjustment produces non-clamped e2e when replay burst occurs
    ev = ProviderEvent(
        event_type="final",
        text="replayed phrase",
        start_ms=0,
        end_ms=2000,
        confidence=0.95,
        speaker=0,
        channel=0,
        is_final=True,
        is_stable=True,
        words=[{"w": "replayed", "start_ms": 0, "end_ms": 900, "confidence": 0.95, "speaker": 0}],
        recv_perf=time.perf_counter(),
    )
    # 20 chunks * 3200 bytes = 2.0s audio replayed before t_pub
    replayed_bytes = 20 * 3200
    pipeline._provider_session_origin_wall = max(0.0, time.time() - (replayed_bytes / 32000.0))
    # Simulate 250ms provider turnaround after replay
    await asyncio.sleep(0.25)
    await pipeline._handle_transcript_event(ev)

    fin_env = json.loads(captured[-1][1]["payload"])
    assert fin_env["payload"]["e2e_ms"] >= 200.0
