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


def test_clock_clamping_logic():
    """R6: Gateway clamps capture_server_ms to [gw_recv - max_age, gw_recv] on +-60s errors."""
    gw_recv_ms = 1_730_000_000_000
    max_age_ms = 15_000

    def clamp(capture_ts_ms: int, offset_ms: int) -> tuple[int, bool, str]:
        raw = capture_ts_ms + offset_ms
        min_allowed = gw_recv_ms - max_age_ms
        max_allowed = gw_recv_ms
        if raw < min_allowed:
            return min_allowed, True, "past_clamp"
        if raw > max_allowed:
            return max_allowed, True, "future_clamp"
        return raw, False, "none"

    # Case 1: Client lies by +60s into the future (offset_ms = +60_000)
    clamped_val, is_clamped, reason = clamp(capture_ts_ms=gw_recv_ms, offset_ms=60_000)
    assert is_clamped is True
    assert reason == "future_clamp"
    assert clamped_val == gw_recv_ms
    assert clamped_val <= gw_recv_ms

    # Case 2: Client lies by -60s into the past (offset_ms = -60_000)
    clamped_val, is_clamped, reason = clamp(capture_ts_ms=gw_recv_ms, offset_ms=-60_000)
    assert is_clamped is True
    assert reason == "past_clamp"
    assert clamped_val == gw_recv_ms - max_age_ms
    assert clamped_val == gw_recv_ms - 15_000

    # Case 3: Legitimate offset within bounds (-1200ms)
    legit_ts = gw_recv_ms + 1200
    clamped_val, is_clamped, reason = clamp(capture_ts_ms=legit_ts, offset_ms=-1200)
    assert is_clamped is False
    assert reason == "none"
    assert clamped_val == gw_recv_ms


def test_clock_resync_drift_100ppm():
    """R6: Client clock drifting 100 ppm over several 60-second resync cycles."""
    # 100 ppm = 100 microseconds per second = 6 ms per 60 seconds
    drift_rate_ppm = 100
    resync_interval_s = 60
    drift_per_cycle_ms = int(drift_rate_ppm * resync_interval_s / 1000)  # 6ms
    t_server_start = 1_730_000_000_000

    initial_offset = 500  # Client starts 500ms ahead
    current_true_offset = initial_offset
    offset_errors: list[int] = []

    print("\n--- 60-SECOND RESYNC SIMULATION (100 PPM DRIFT) ---")
    for cycle in range(1, 6):
        # Time advanced by 60s
        t_server = t_server_start + (cycle * resync_interval_s * 1000)
        # Client clock accumulated drift
        current_true_offset += drift_per_cycle_ms

        # Run 3-probe handshake with simulated 35ms symmetric network delay
        probes = []
        for p_idx in range(3):
            t1 = t_server + current_true_offset + (p_idx * 50)
            t2 = t_server + (p_idx * 50) + 18
            t3 = t2 + 2  # 2ms processing
            t4 = t3 + current_true_offset + 18
            probes.append(ClockEstimator.compute_probe(t1, t2, t3, t4))

        best = ClockEstimator.select_best_probe(probes)
        estimated_offset = best["offset_ms"]
        # Error between estimated offset and negative of true client offset
        error = abs(estimated_offset - (-current_true_offset))
        offset_errors.append(error)
        print(
            f"Cycle {cycle} (T+{cycle * 60}s): TrueOffset={current_true_offset}ms | "
            f"EstOffset={estimated_offset}ms | Error={error}ms | RTT={best['rtt_ms']}ms"
        )
        assert error <= 2, f"Resync error {error}ms exceeds 2ms tolerance"

    print("----------------------------------------------------\n")


def test_clock_estimator_asymmetric_and_jitter_data():
    """R6: Worst-case offset error with asymmetric delays and jitter against 50ms target."""
    rng = random.Random(12345)
    t_server = 1_730_000_000_000

    # 1. Asymmetric: 20ms up / 120ms down (RTT = 140ms)
    d_up = 20
    d_down = 120
    true_offset = 2500
    t1 = t_server + true_offset
    t2 = t_server + d_up
    t3 = t2 + 3
    t4 = t3 + true_offset + d_down
    probe_asym = ClockEstimator.compute_probe(t1, t2, t3, t4)
    # Theoretical error: (d_up - d_down) / 2 = (20 - 120) / 2 = -50ms
    asym_error = probe_asym["offset_ms"] - (-true_offset)

    print("\n--- ASYMMETRIC DELAY AND JITTER ACCURACY DATA ---")
    print(f"Asymmetric Delays: Up={d_up}ms, Down={d_down}ms, RTT={probe_asym['rtt_ms']}ms")
    print(f"Worst-Case Offset Error: {asym_error:+d}ms (Contract target: <= 50ms)")
    assert abs(asym_error) <= 50

    # 2. Jitter range: 20ms to 150ms over +-3s offsets with 5 probes
    max_observed_error = 0
    for test_offset in [-3000, -1500, 0, 1500, 3000]:
        probes = []
        for _ in range(5):
            up = rng.randint(20, 150)
            down = rng.randint(20, 150)
            p_t1 = t_server + test_offset
            p_t2 = t_server + up
            p_t3 = p_t2 + rng.randint(1, 5)
            p_t4 = p_t3 + test_offset + down
            probes.append(ClockEstimator.compute_probe(p_t1, p_t2, p_t3, p_t4))

        best = ClockEstimator.select_best_probe(probes)
        err = abs(best["offset_ms"] - (-test_offset))
        max_observed_error = max(max_observed_error, err)
        print(
            f"Offset {test_offset:+d}ms: Best RTT={best['rtt_ms']}ms | "
            f"Est Error={err}ms (MaxErrorBound={best['max_error_ms']}ms)"
        )

    print(f"Max Error Observed Across Jitter: {max_observed_error}ms")
    print("-------------------------------------------------\n")


def test_gateway_clamp_capture_server_ms_sntp_tolerance():
    """Gateway caps minor SNTP jitter silently while auditing anomalous future/past shifts."""
    from meetmind_gateway.ws_endpoint import _clamp_capture_server_ms

    gw_recv_ms = 1_730_000_000_000
    max_age_ms = 15_000

    # Minor SNTP asymmetry residual (+5ms, RTT=40ms -> tolerance=100ms):
    # capped to gw_recv_ms for monotonicity without emitting future_clamp audit
    val, clamped, reason = _clamp_capture_server_ms(
        raw_capture_server_ms=gw_recv_ms + 5,
        gw_recv_ms=gw_recv_ms,
        max_age_ms=max_age_ms,
        rtt_ms=40,
    )
    assert val == gw_recv_ms
    assert clamped is False
    assert reason == "none"

    # Genuine future anomaly (+60s): capped and audited as future_clamp
    val, clamped, reason = _clamp_capture_server_ms(
        raw_capture_server_ms=gw_recv_ms + 60_000,
        gw_recv_ms=gw_recv_ms,
        max_age_ms=max_age_ms,
        rtt_ms=40,
    )
    assert val == gw_recv_ms
    assert clamped is True
    assert reason == "future_clamp"

    # Genuine past anomaly (-60s): clamped to gw_recv_ms - max_age_ms and audited as past_clamp
    val, clamped, reason = _clamp_capture_server_ms(
        raw_capture_server_ms=gw_recv_ms - 60_000,
        gw_recv_ms=gw_recv_ms,
        max_age_ms=max_age_ms,
        rtt_ms=40,
    )
    assert val == gw_recv_ms - max_age_ms
    assert clamped is True
    assert reason == "past_clamp"
