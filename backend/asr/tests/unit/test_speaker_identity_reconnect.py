"""MeetMind AI — Speaker Identity Reconnect Alignment Test (Session D2d Item 6).

Verifies that:
1. speaker_epoch is tracked and included in published transcript_final payloads.
2. When the provider renumbers speakers after a reconnect (e.g. speaker 0 becomes speaker 1),
   the pipeline aligns the new session speaker to the previous identity using the replayed window
   (overlapping time and text).
3. The published speaker_id never flips silently.
4. Any new speaker in a subsequent epoch that cannot be aligned with confidence
   is marked 'spk_unknown'.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_asr.config import AsrSettings
from meetmind_asr.health import AsrMetrics
from meetmind_asr.pipeline import MeetingPipeline
from meetmind_asr.providers.base import ProviderEvent
from meetmind_asr.tenant_config import AsrTenantConfig


@pytest.mark.asyncio
async def test_speaker_identity_alignment_across_reconnect() -> None:
    settings = AsrSettings()
    tenant_cfg = AsrTenantConfig(tenant_id="test_spk_tenant", language="en-US", asr_region="global")
    metrics = AsrMetrics()
    auditor = MagicMock()
    redis_client = AsyncMock()

    published_events: list[dict] = []

    def mock_xadd(key, fields, **kwargs):
        payload_str = fields.get("payload")
        if payload_str:
            published_events.append(json.loads(payload_str))
        return "1-0"

    pipe_mock = MagicMock()
    pipe_mock.xadd = mock_xadd
    pipe_mock.expire = MagicMock()
    pipe_mock.execute = AsyncMock(return_value=[True, True])
    redis_client.pipeline = MagicMock(return_value=pipe_mock)

    provider = MagicMock()
    provider.base_url = "wss://api.deepgram.com"
    provider.open = AsyncMock()
    provider.close = AsyncMock()

    pipeline = MeetingPipeline(
        tenant_id="test_spk_tenant",
        meeting_id="mtg_spk_123",
        worker_id="worker_test",
        redis_client=redis_client,
        provider=provider,
        tenant_config=tenant_cfg,
        settings=settings,
        metrics=metrics,
        auditor=auditor,
    )
    pipeline._channels = 1
    # Anchoring origin
    pipeline._timeline.meeting_origin_capture_ts = 1_000_000

    # 1. Epoch 0: Speaker 0 speaks "Welcome everyone to our meeting"
    pipeline._timeline.register_chunk(capture_ts_ms=1_000_000, duration_ms=2000)
    ev1 = ProviderEvent(
        event_type="final",
        text="Welcome everyone to our meeting",
        start_ms=0,
        end_ms=2000,
        confidence=0.98,
        speaker=0,
        channel=0,
        is_final=True,
        is_stable=True,
    )
    await pipeline._handle_transcript_event(ev1)

    assert len(published_events) == 1
    p1 = published_events[0]["payload"]
    assert p1["speaker_id"] == "spk_0"
    assert p1["speaker_epoch"] == 0
    assert p1["text"] == "Welcome everyone to our meeting"

    # 2. Simulate reconnect (Session drops, epoch increments, session resets)
    pipeline._speaker_epoch += 1
    pipeline._speaker_mapping.clear()
    pipeline._timeline.reset_session()
    assert pipeline._speaker_epoch == 1

    # In Session 1, Deepgram restarts speaker numbering:
    # Speaker 0 from session 0 is now emitted as Speaker 1 on the replayed/overlapping audio!
    # Replayed audio registered in new session
    pipeline._timeline.register_chunk(capture_ts_ms=1_000_000, duration_ms=2000)

    # Utterance arrives from new session with speaker=1, text="Welcome everyone to our meeting"
    # Even if dedupe drops the exact duplicate, let's verify speaker alignment mapping:
    ev_replay = ProviderEvent(
        event_type="final",
        text="Welcome everyone to our meeting",
        start_ms=0,
        end_ms=2000,
        confidence=0.98,
        speaker=1,  # Renumbered by provider from 0 -> 1!
        channel=0,
        is_final=True,
        is_stable=True,
    )
    await pipeline._handle_transcript_event(ev_replay)

    # Alignment should now have learned: raw speaker 1 -> spk_0!
    assert pipeline._speaker_mapping.get(1) == "spk_0"

    # 3. New utterance from the same person in session 1 (raw speaker=1)
    pipeline._timeline.register_chunk(capture_ts_ms=1_002_000, duration_ms=2000)
    ev_new = ProviderEvent(
        event_type="final",
        text="Today we review the term sheet",
        start_ms=2000,
        end_ms=4000,
        confidence=0.95,
        speaker=1,  # Still raw speaker 1 in session 1
        channel=0,
        is_final=True,
        is_stable=True,
    )
    await pipeline._handle_transcript_event(ev_new)

    # Must be published as spk_0, NOT spk_1!
    assert len(published_events) == 2
    p2 = published_events[1]["payload"]
    assert p2["speaker_id"] == "spk_0", (
        f"Expected spk_0 but got {p2['speaker_id']} (flipped silently!)"
    )
    assert p2["speaker_epoch"] == 1
    assert p2["text"] == "Today we review the term sheet"

    # 4. Unknown new speaker in session 1 without prior overlap
    pipeline._timeline.register_chunk(capture_ts_ms=1_004_000, duration_ms=2000)
    ev_unknown = ProviderEvent(
        event_type="final",
        text="A completely new attendee has joined the call",
        start_ms=4000,
        end_ms=6000,
        confidence=0.90,
        speaker=9,  # Unseen raw speaker
        channel=0,
        is_final=True,
        is_stable=True,
    )
    await pipeline._handle_transcript_event(ev_unknown)

    assert len(published_events) == 3
    p3 = published_events[2]["payload"]
    assert p3["speaker_id"] == "spk_unknown"
    assert p3["speaker_epoch"] == 1
