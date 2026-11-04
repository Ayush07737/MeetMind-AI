"""MeetMind AI — Pipeline Timing, Monotonic Validation & Clock Offset Estimation (Q1).

Defines stage milestones across the end-to-end pipeline, stamp enrichment,
monotonic timing validation, and clock offset calculation with error bounds.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .events import EventEnvelope, EventTiming

TIMING_STAGES: tuple[str, ...] = (
    "t0_capture",
    "gw_recv",
    "asr_recv",
    "provider_msg",
    "asr_pub",
    "classify_pub",
    "retrieval_done",
    "card_pub",
    "ui_render",
)


def now_epoch_ms() -> int:
    """Return current UTC epoch time in milliseconds as an integer."""
    return int(datetime.now(UTC).timestamp() * 1000)


def stamp(
    envelope: EventEnvelope | dict[str, Any],
    stage: str,
    timestamp_ms: int | None = None,
) -> EventEnvelope | dict[str, Any]:
    """Stamp an envelope with a monotonic epoch millisecond timestamp at a named stage.

    Args:
        envelope: EventEnvelope instance or raw dict.
        stage: Pipeline stage name (must be one of TIMING_STAGES).
        timestamp_ms: Epoch ms timestamp (defaults to current UTC epoch ms).

    Returns:
        The mutated or updated envelope.
    """
    if stage not in TIMING_STAGES:
        raise ValueError(f"Unknown timing stage '{stage}'. Must be one of {TIMING_STAGES}")

    t_ms = timestamp_ms if timestamp_ms is not None else now_epoch_ms()

    if isinstance(envelope, EventEnvelope):
        if envelope.timing is None:
            envelope.timing = EventTiming()
        setattr(envelope.timing, stage, t_ms)
    elif isinstance(envelope, dict):
        if "timing" not in envelope or envelope["timing"] is None:
            envelope["timing"] = {}
        envelope["timing"][stage] = t_ms
    else:
        raise TypeError(f"Unsupported envelope type: {type(envelope)}")

    return envelope


def validate_monotonic(timing: EventTiming | dict[str, Any]) -> list[str]:
    """Validate that populated pipeline stage timestamps are non-decreasing.

    Returns:
        List of validation error messages describing any monotonic inversions.
        Returns empty list if timestamps are strictly non-decreasing.
    """
    data = timing.model_dump() if isinstance(timing, EventTiming) else timing
    violations: list[str] = []

    last_stage: str | None = None
    last_val: int | None = None

    for stage in TIMING_STAGES:
        val = data.get(stage)
        if val is None:
            continue
        if last_val is not None and val < last_val:
            violations.append(
                f"Monotonic violation: stage '{stage}' ({val}ms) preceded by "
                f"'{last_stage}' ({last_val}ms) with delta {val - last_val}ms"
            )
        last_stage = stage
        last_val = val

    return violations


def compute_stage_latencies(timing: EventTiming | dict[str, Any]) -> dict[str, int]:
    """Compute elapsed milliseconds between consecutive populated stages."""
    data = timing.model_dump() if isinstance(timing, EventTiming) else timing
    latencies: dict[str, int] = {}

    prev_stage: str | None = None
    prev_val: int | None = None

    for stage in TIMING_STAGES:
        val = data.get(stage)
        if val is None:
            continue
        if prev_stage is not None and prev_val is not None:
            latencies[f"{prev_stage}_to_{stage}"] = val - prev_val
        prev_stage = stage
        prev_val = val

    if "t0_capture" in data and prev_stage and prev_stage != "t0_capture":
        t0 = data["t0_capture"]
        if t0 is not None and prev_val is not None:
            latencies["e2e_total"] = prev_val - t0

    return latencies


class ClockEstimator:
    """SNTP-style clock offset and round-trip time estimator (Q1 Spec §3)."""

    @staticmethod
    def compute_probe(t1: int, t2: int, t3: int, t4: int) -> dict[str, int]:
        """Compute RTT and offset for a single 4-timestamp probe.

        t1: Client send timestamp
        t2: Gateway receive timestamp
        t3: Gateway reply send timestamp
        t4: Client reply receive timestamp
        """
        rtt = (t4 - t1) - (t3 - t2)
        offset = ((t2 - t1) + (t3 - t4)) // 2
        return {
            "rtt_ms": max(0, rtt),
            "offset_ms": offset,
            "max_error_ms": max(1, rtt // 2),
        }

    @staticmethod
    def select_best_probe(probes: list[dict[str, int]]) -> dict[str, int]:
        """Select probe with minimum RTT to minimize network asymmetry error."""
        if not probes:
            raise ValueError("No probes provided")
        return min(probes, key=lambda p: p["rtt_ms"])
