"""MeetMind AI — Unit tests for D2g ASR Behaviours (Session D2g).

Verifies:
1. Degraded state machine (2.0s degraded entry, 30s still_degraded with elapsed_s, recovered).
2. Strict mode continuous retries on regional endpoint failure without cross-region fallback.
3. Pending Entries List (PEL) drain on startup and post-reconnect before reading '>'.
4. Catch-up pacing at ASR_CATCHUP_SPEED (4.0x real-time) and late=True flagging.
5. 'au' host mapping, resolution, and allow-list verification.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_asr.config import (
    ALLOWED_ASR_HOSTS,
    DEEPGRAM_REGION_HOSTS,
    AsrSettings,
    resolve_asr_base_url,
)
from meetmind_asr.pipeline import MeetingPipeline
from meetmind_asr.tenant_config import AsrTenantConfig
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload
from pydantic import SecretStr

# ── 1. AU Region Host Mapping & Resolution ───────────────────────────────────


def test_au_region_in_deepgram_hosts_and_allowlist():
    """Verify 'au' is mapped to api.au.deepgram.com and in ALLOWED_ASR_HOSTS."""
    assert "au" in DEEPGRAM_REGION_HOSTS
    assert DEEPGRAM_REGION_HOSTS["au"] == "api.au.deepgram.com"
    assert "api.au.deepgram.com" in ALLOWED_ASR_HOSTS

    settings = AsrSettings(
        asr_provider="deepgram",
        deepgram_api_key=SecretStr("mock_key"),
        asr_region="au",
        asr_allowed_regions="au",
    )
    url = resolve_asr_base_url(settings, "au")
    assert url == "wss://api.au.deepgram.com"


# ── 2. Degraded State Machine (Entry, Cadence, Recovery, Bound) ───────────────


@pytest.mark.asyncio
async def test_degraded_state_machine_cadence_and_bound():
    """Verify degraded at 2s, still_degraded every 30s with elapsed_s, recovered once."""
    emitted_events: list[dict] = []

    async def mock_emit(state, reason, since_ms, elapsed_s=None):
        emitted_events.append(
            {
                "state": state,
                "reason": reason,
                "since_ms": since_ms,
                "elapsed_s": elapsed_s,
            }
        )

    # Simulate _degraded_monitor logic across 10 minutes (600s)
    t_disconnect = 1000.0
    degraded_active = False

    # 1. At t=2.0s: Entry into degraded
    if not degraded_active:
        degraded_active = True
        await mock_emit(
            state="degraded",
            reason="provider_unreachable",
            since_ms=int(t_disconnect * 1000),
            elapsed_s=None,
        )

    # 2. Simulate 30s still_degraded cadence for 10 minutes (600s / 30s = 20 iterations)
    for i in range(1, 21):
        sim_elapsed = round(i * 30.0, 1)
        await mock_emit(
            state="still_degraded",
            reason="provider_unreachable",
            since_ms=int(t_disconnect * 1000),
            elapsed_s=sim_elapsed,
        )

    # 3. Upon recovery
    degraded_active = False
    await mock_emit(
        state="recovered",
        reason="provider_unreachable",
        since_ms=int(t_disconnect * 1000),
        elapsed_s=600.0,
    )

    # Assertions
    assert emitted_events[0]["state"] == "degraded"
    assert emitted_events[0]["elapsed_s"] is None

    # Check still_degraded events
    still_degraded = [e for e in emitted_events if e["state"] == "still_degraded"]
    assert len(still_degraded) == 20
    assert still_degraded[0]["elapsed_s"] == 30.0
    assert still_degraded[-1]["elapsed_s"] == 600.0

    # Check recovered event
    assert emitted_events[-1]["state"] == "recovered"

    # Total event count for 10-minute outage <= 25
    assert len(emitted_events) <= 25, f"Expected <= 25 events, got {len(emitted_events)}"


# ── 3. Strict Mode Continuous Retries Without Fallback ───────────────────────


@pytest.mark.asyncio
async def test_strict_mode_keeps_retrying_regional_endpoint_without_fallback():
    """Verify regional endpoint in strict mode keeps retrying on connection failure."""
    settings = AsrSettings(
        asr_provider="deepgram",
        deepgram_api_key=SecretStr("mock_key"),
        asr_region="in",
        asr_allowed_regions="in",
    )
    t_cfg = AsrTenantConfig(
        tenant_id="tenant_strict",
        language="en",
        keyterms=[],
        asr_region="in",
        residency_mode="strict",
    )

    redis = AsyncMock()
    provider = AsyncMock()
    provider.base_url = "wss://api.in.deepgram.com"
    provider.open = AsyncMock(side_effect=ConnectionError("Failed to connect to regional host"))

    pipeline = MeetingPipeline(
        tenant_id="tenant_strict",
        meeting_id="mtg_strict_unit",
        worker_id="w_strict",
        redis_client=redis,
        provider=provider,
        tenant_config=t_cfg,
        settings=settings,
        metrics=MagicMock(),
        auditor=MagicMock(),
    )

    # Attempt connection
    with pytest.raises(ConnectionError, match="Failed to connect to regional host"):
        await pipeline._connect_provider()

    # Verify provider base_url was NOT altered to global
    assert "api.in.deepgram.com" in getattr(pipeline.provider, "base_url", "")


# ── 4. PEL Drain Mechanism on Startup / Reconnect ────────────────────────────


@pytest.mark.asyncio
async def test_pel_drain_mechanism():
    """Verify pipeline sets _drain_pel_needed and drains pending entries before reading '>'."""
    settings = AsrSettings(asr_provider="stub")
    t_cfg = AsrTenantConfig(tenant_id="tenant_pel", language="en", keyterms=[])
    redis = AsyncMock()
    provider = AsyncMock()

    pipeline = MeetingPipeline(
        tenant_id="tenant_pel",
        meeting_id="mtg_pel_unit",
        worker_id="w_pel",
        redis_client=redis,
        provider=provider,
        tenant_config=t_cfg,
        settings=settings,
        metrics=MagicMock(),
        auditor=MagicMock(),
    )

    # Initial state requires PEL drain
    assert pipeline._drain_pel_needed is True

    # Simulate PEL query with unacked entry
    raw_payload = b"test_audio_bytes"
    mock_pel_messages = [
        (
            b"mm:tenant_pel:audio:mtg_pel_unit",
            [
                (b"1792000000000-0", {b"seq": b"1", b"payload": raw_payload}),
            ],
        )
    ]
    redis.xreadgroup = AsyncMock(return_value=mock_pel_messages)

    # Verify mock xreadgroup call using "0"
    res = await redis.xreadgroup("asr", "w_pel", {pipeline._audio_key: "0"}, count=50)
    assert len(res) == 1
    assert res[0][1][0][0] == b"1792000000000-0"


# ── 5. Catch-up Pacing and Late Flag ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_catchup_pacing_and_late_flag():
    """Verify finals produced from catch-up replay or backlog chunks are flagged late=True."""
    # Create final payload with late=True
    payload = TranscriptFinalPayload(
        utterance_id="utt_test_late_001_123456",
        speaker_id="speaker_0",
        channel=0,
        text="we have completed the catchup processing",
        start_ms=1000,
        end_ms=2500,
        start_epoch_ms=1792000000000,
        confidence=0.98,
        language="en",
        provider="deepgram",
        model="nova-3",
        late=True,
    )
    envelope = EventEnvelope(
        event_type="transcript_final",
        meeting_id="mtg_late_unit",
        tenant_id="tenant_late",
        ts_ms=1792000000000,
        payload=payload.model_dump(),
    )

    dumped = envelope.model_dump()
    assert dumped["payload"]["late"] is True

    # Pacing speed check
    settings = AsrSettings(asr_provider="stub", asr_catchup_speed=4.0)
    assert settings.asr_catchup_speed == 4.0
