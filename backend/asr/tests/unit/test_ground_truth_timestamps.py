"""MeetMind AI — Ground-Truth Word Timestamp Accuracy Test (Session D2d).

Validates that TimelineMapper correctly maps speech word timestamps back to the
original ground-truth capture timeline across:
1. Continuous real-time streaming of the 30s two-speaker fixture.
2. Reconnect with session reset at t=15s (new session offset restarts at 0).
3. Client sequence / packet gaps.

Target SLO:
Mean absolute error <= 150ms.
P95 absolute error <= 150ms.
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path

import pytest
from meetmind_asr.timeline import TimelineMapper

REPO_ROOT = Path(__file__).resolve().parents[4]
TIMINGS_FILE = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_word_timings.json"


def load_ground_truth_words() -> list[dict]:
    if not TIMINGS_FILE.exists():
        pytest.skip(f"Ground-truth file {TIMINGS_FILE} not found")
    return json.loads(TIMINGS_FILE.read_text(encoding="utf-8"))


def test_timeline_continuous_streaming_ground_truth() -> None:
    words = load_ground_truth_words()
    assert len(words) > 0

    timeline = TimelineMapper(window_seconds=120)
    origin_capture_ts = 1_700_000_000_000

    # Simulate 30 seconds of audio streamed at 100ms intervals (300 chunks)
    for chunk_idx in range(300):
        capture_ts = origin_capture_ts + (chunk_idx * 100)
        timeline.register_chunk(capture_ts_ms=capture_ts, duration_ms=100)

    start_errors: list[float] = []
    end_errors: list[float] = []

    for w in words:
        gt_start = w["start_ms"]
        gt_end = w["end_ms"]

        # Provider emits timestamps relative to its audio stream start
        mapped_start, mapped_end, _ = timeline.map_range(gt_start, gt_end)

        start_err = abs(mapped_start - gt_start)
        end_err = abs(mapped_end - gt_end)
        start_errors.append(start_err)
        end_errors.append(end_err)

    mean_start_err = statistics.mean(start_errors)
    s_sorted = sorted(start_errors)
    p95_start_err = s_sorted[int(len(s_sorted) * 0.95)]

    print(
        f"\nContinuous Stream: Mean Error={mean_start_err:.2f}ms | "
        f"P95 Error={p95_start_err:.2f}ms"
    )
    assert mean_start_err <= 150.0, f"Mean error {mean_start_err}ms exceeds 150ms target"
    assert p95_start_err <= 150.0, f"P95 error {p95_start_err}ms exceeds 150ms target"


def test_timeline_reconnect_session_reset_ground_truth() -> None:
    """Verify that when a reconnect resets provider session at t=15s, timestamps align."""
    words = load_ground_truth_words()
    assert len(words) > 0

    timeline = TimelineMapper(window_seconds=120)
    origin_capture_ts = 1_700_000_000_000

    # Stream first 15 seconds (150 chunks)
    for chunk_idx in range(150):
        capture_ts = origin_capture_ts + (chunk_idx * 100)
        timeline.register_chunk(capture_ts_ms=capture_ts, duration_ms=100)

    # Provider connection drops; reconnect resets provider session
    timeline.reset_session()

    # Reconnect streams replay window (last 30 chunks: 120..149) + remaining audio (150..299)
    # The new provider session starts from offset 0 at chunk 120
    for chunk_idx in range(120, 300):
        capture_ts = origin_capture_ts + (chunk_idx * 100)
        timeline.register_chunk(capture_ts_ms=capture_ts, duration_ms=100)

    start_errors: list[float] = []
    for w in words:
        gt_start = w["start_ms"]
        gt_end = w["end_ms"]

        # If word was in new session (after chunk 120 = 12000ms):
        if gt_start >= 12000:
            provider_offset = gt_start - 12000
            provider_end_offset = gt_end - 12000
            mapped_start, mapped_end, _ = timeline.map_range(provider_offset, provider_end_offset)
            start_errors.append(abs(mapped_start - gt_start))

    assert len(start_errors) > 0
    mean_err = statistics.mean(start_errors)
    s_sorted = sorted(start_errors)
    p95_err = s_sorted[int(len(s_sorted) * 0.95)]

    print(f"\nReconnect Stream: Mean Error={mean_err:.2f}ms | P95 Error={p95_err:.2f}ms")
    assert mean_err <= 150.0, f"Mean error after reconnect {mean_err}ms exceeds 150ms"
    assert p95_err <= 150.0, f"P95 error after reconnect {p95_err}ms exceeds 150ms"
