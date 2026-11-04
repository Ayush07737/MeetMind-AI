"""MeetMind AI — Clock Synchronization Unit Tests (Q1 Spec §3, §5).

Tests clock offset estimation under simulated network conditions:
- Offsets: +/- 3.0 seconds
- Jitter: 20-150ms
- Asymmetric network delays: 20ms uplink / 120ms downlink
- Monotonic timing validation and stage latency calculation
"""

from __future__ import annotations

import random

import pytest
from meetmind_schemas.events import EventEnvelope, EventTiming
from meetmind_schemas.timing import (
    ClockEstimator,
    compute_stage_latencies,
    stamp,
    validate_monotonic,
)


def test_clock_estimator_symmetric_delay():
    """Under symmetric delays, estimated offset matches true offset within integer rounding."""
    t_server_base = 1_730_000_000_000
    true_offset = 2500  # Client is 2.5s ahead: t_client = t_server + true_offset

    # Symmetric delay: 30ms up, 30ms down
    t1 = t_server_base + true_offset
    t2 = t_server_base + 30
    t3 = t2 + 2  # 2ms server processing time
    t4 = t3 + true_offset + 30

    probe = ClockEstimator.compute_probe(t1, t2, t3, t4)
    assert probe["rtt_ms"] == 60
    assert abs(probe["offset_ms"] - (-true_offset)) <= 1
    assert probe["max_error_ms"] == 30


def test_clock_estimator_asymmetric_delay_bounded():
    """Under asymmetric delay (20ms up / 120ms down), error is bounded by RTT / 2."""
    t_server_base = 1_730_000_000_000
    true_offset = -1800  # Client is 1.8s behind

    # Asymmetric delay: 20ms up, 120ms down
    d_up = 20
    d_down = 120
    t1 = t_server_base + true_offset
    t2 = t_server_base + d_up
    t3 = t2 + 5  # 5ms server processing
    t4 = t3 + true_offset + d_down

    probe = ClockEstimator.compute_probe(t1, t2, t3, t4)
    rtt = probe["rtt_ms"]
    assert rtt == d_up + d_down

    # Computed offset in probe is (t_server - t_client), which estimates -true_offset
    est_offset = probe["offset_ms"]
    actual_error = abs(est_offset - (-true_offset))
    assert actual_error <= probe["max_error_ms"]
    assert actual_error <= rtt // 2


@pytest.mark.parametrize("true_offset", [-3000, -1500, 0, 1500, 3000])
def test_clock_estimator_best_probe_selection(true_offset: int):
    """Best probe selector selects probe with minimum RTT across simulated jitter."""
    rng = random.Random(42 + true_offset)
    t_server_base = 1_730_000_000_000

    probes = []
    for _ in range(5):
        d_up = rng.randint(20, 150)
        d_down = rng.randint(20, 150)
        t_proc = rng.randint(1, 10)

        t1 = t_server_base + true_offset
        t2 = t_server_base + d_up
        t3 = t2 + t_proc
        t4 = t3 + true_offset + d_down

        p = ClockEstimator.compute_probe(t1, t2, t3, t4)
        probes.append(p)

    best = ClockEstimator.select_best_probe(probes)
    min_rtt = min(p["rtt_ms"] for p in probes)
    assert best["rtt_ms"] == min_rtt
    assert abs(best["offset_ms"] - (-true_offset)) <= best["max_error_ms"]


def test_validate_monotonic_clean():
    """Monotonically increasing timestamps report zero violations."""
    timing = EventTiming(
        t0_capture=1000,
        gw_recv=1025,
        asr_recv=1030,
        provider_msg=1400,
        asr_pub=1410,
        classify_pub=1550,
        retrieval_done=1800,
        card_pub=1950,
        ui_render=2000,
    )
    violations = validate_monotonic(timing)
    assert violations == []


def test_validate_monotonic_detects_inversion():
    """Inversion between stages is detected with descriptive error message."""
    timing = EventTiming(
        t0_capture=2000,
        gw_recv=1950,  # 50ms before t0_capture!
        asr_recv=2010,
    )
    violations = validate_monotonic(timing)
    assert len(violations) == 1
    assert (
        "Monotonic violation: stage 'gw_recv' (1950ms) preceded by 't0_capture' (2000ms)"
        in violations[0]
    )


def test_stamp_and_compute_stage_latencies():
    """Enriching envelope with stamp and computing stage latencies."""
    env = EventEnvelope(
        event_type="transcript_final",
        meeting_id="mtg_test",
        tenant_id="tenant_test",
        ts_ms=1000,
        payload={"text": "hello"},
    )
    stamp(env, "t0_capture", 1000)
    stamp(env, "gw_recv", 1025)
    stamp(env, "asr_recv", 1030)
    stamp(env, "asr_pub", 1100)

    assert env.timing is not None
    assert env.timing.t0_capture == 1000
    assert env.timing.gw_recv == 1025
    assert env.timing.asr_recv == 1030
    assert env.timing.asr_pub == 1100

    latencies = compute_stage_latencies(env.timing)
    assert latencies["t0_capture_to_gw_recv"] == 25
    assert latencies["gw_recv_to_asr_recv"] == 5
    assert latencies["asr_recv_to_asr_pub"] == 70
    assert latencies["e2e_total"] == 100
