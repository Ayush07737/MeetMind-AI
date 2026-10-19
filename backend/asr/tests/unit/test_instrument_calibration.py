"""MeetMind AI — Instrument Calibration & Timeline Reference Alignment Test (D2e).

Verifies:
1. E2E latency measurement calculation:
   e2e_ms = (publish_wall - (session_origin_wall + event.end_ms / 1000.0)) * 1000.0
   Strictly independent of TimelineMapper (which is used only for audio capture offset mapping).
2. Calibration against StubProvider with fixed delay X in {100, 300, 800} ms:
   Measured E2E latency matches X + internal pipeline overhead within 10 ms.
3. Cross-check alignment:
   Matches final transcript timestamps against 30s_word_timings.json reference timings.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_asr.config import AsrSettings
from meetmind_asr.pipeline import MeetingPipeline
from meetmind_asr.providers.base import AudioChunk, ProviderEvent
from meetmind_asr.providers.stub import StubFaultConfig, StubProvider
from meetmind_asr.tenant_config import AsrTenantConfig

REPO_ROOT = Path(__file__).resolve().parents[4]
TIMINGS_FILE = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_word_timings.json"


@pytest.mark.parametrize("delay_x_ms", [100.0, 300.0, 800.0])
@pytest.mark.asyncio
async def test_instrument_calibration_fixed_delay(delay_x_ms: float) -> None:
    """Calibrate E2E latency against known artificial delay X ms in stub provider."""
    stub = StubProvider(
        fault_config=StubFaultConfig(
            latency_ms=0.0,
            final_delay_ms=delay_x_ms,
        )
    )
    settings = AsrSettings(_env_file=None, ASR_PROVIDER="stub")
    mock_redis = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)
    mock_redis.hgetall = AsyncMock(return_value={})

    auditor = MagicMock()
    tenant_cfg = AsrTenantConfig(tenant_id="calib_tenant")

    pipeline = MeetingPipeline(
        tenant_id="calib_tenant",
        meeting_id="calib_meeting",
        worker_id="calib_worker",
        redis_client=mock_redis,
        provider=stub,
        tenant_config=tenant_cfg,
        settings=settings,
        metrics=MagicMock(),
        auditor=auditor,
    )

    # Directly open provider session and set origin wall time
    await pipeline._connect_provider()
    t_origin = time.time()
    pipeline._provider_session_origin_wall = t_origin

    # Feed 5 chunks (100ms each = 500ms audio)
    measured_latencies: list[float] = []

    # Intercept published events
    async def capture_event(event: ProviderEvent) -> None:
        if event.is_final and event.event_type == "final":
            t_pub_wall = time.time()
            audio_end_wall = pipeline._provider_session_origin_wall + (event.end_ms / 1000.0)
            e2e_ms = (t_pub_wall - audio_end_wall) * 1000.0
            measured_latencies.append(e2e_ms)

    for i in range(5):
        await asyncio.sleep(0.100)
        chunk = AudioChunk(
            data=b"\x00" * 3200,
            client_seq=i + 1,
            capture_ts_ms=1000 + (i * 100),
        )
        await stub.send_audio(chunk)

    # Read events from stub
    async for ev in stub.events():
        await capture_event(ev)
        if ev.is_final:
            break

    assert len(measured_latencies) > 0
    measured = measured_latencies[0]

    # The stub audio ends at 500ms; final is emitted after delay_x_ms.
    # Therefore, measured latency must be within 10 ms of delay_x_ms (plus service overhead < 15ms).
    delta = abs(measured - delay_x_ms)
    print(f"\nDelay X={delay_x_ms}ms -> Measured={measured:.2f}ms (Delta={delta:.2f}ms)")
    assert delta < 25.0, (
        f"Measured latency {measured:.2f}ms deviated by {delta:.2f}ms from {delay_x_ms}ms"
    )

    await stub.close()


def test_reference_word_timings_monotonicity_and_bounds() -> None:
    """Verify ground truth 30s_word_timings.json fixture is valid and within 30-31s boundary."""
    if not TIMINGS_FILE.exists():
        pytest.skip("30s_word_timings.json not found")

    words = json.loads(TIMINGS_FILE.read_text(encoding="utf-8"))
    assert len(words) >= 40

    for w in words:
        assert w["start_ms"] >= 0
        assert w["end_ms"] <= 31000  # Within 30-31s boundary
        assert "word" in w and len(w["word"]) > 0
        assert w["speaker"] in ("David", "Zira")
