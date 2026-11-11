#!/usr/bin/env python3
"""MeetMind AI — Interim Transcript Stability & Speculative Trigger Analysis (Q5).

Analyzes recorded event streams (or JSONL event logs) to evaluate:
1. Retraction rate of speculative trigger candidates when final transcripts arrive.
2. Time saved (latency lead) gained by pre-fetching on stable prefix vs waiting for final.

Usage:
    python scripts/analyze_stability.py [--input-log path/to/events.jsonl] [--summary]
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any


def analyze_event_log(log_path: Path) -> dict[str, Any]:
    """Parse JSONL log containing EventEnvelopes and compute stability metrics."""
    if not log_path.exists():
        return {"error": f"Log file not found: {log_path}"}

    interim_count = 0
    candidate_triggers: dict[str, dict[str, Any]] = {}  # trigger_id -> info
    confirmed_triggers: list[dict[str, Any]] = []
    retracted_triggers: list[dict[str, Any]] = []
    lead_times_ms: list[float] = []

    with log_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                raw_obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            event = raw_obj.get("event", raw_obj) if isinstance(raw_obj, dict) else {}
            if not isinstance(event, dict):
                continue
            event_type = event.get("event_type")
            payload = event.get("payload", {})
            ts_ms = event.get("ts_ms") or raw_obj.get("wall_ms") or 0

            if event_type == "transcript_interim":
                interim_count += 1

            elif event_type == "trigger_candidate":
                cid = payload.get("candidate_id") or payload.get("trigger_id")
                if cid:
                    candidate_triggers[cid] = {
                        "candidate_id": cid,
                        "timestamp_ms": ts_ms,
                        "category": payload.get("category"),
                        "matched_phrase": payload.get("matched_phrase"),
                    }

            elif event_type == "trigger_confirmed":
                cid = payload.get("candidate_id")
                if cid and cid in candidate_triggers:
                    cand = candidate_triggers.pop(cid)
                    lead_time = ts_ms - cand["timestamp_ms"]
                    lead_times_ms.append(max(0.0, float(lead_time)))
                    confirmed_triggers.append(
                        {
                            "candidate_id": cid,
                            "confirmed_ts_ms": ts_ms,
                            "lead_time_ms": lead_time,
                        }
                    )

            elif event_type == "trigger_retracted":
                cid = payload.get("candidate_id")
                if cid and cid in candidate_triggers:
                    cand = candidate_triggers.pop(cid)
                    retracted_triggers.append(cand)

    # Any remaining candidates unconfirmed at end of meeting are considered retracted
    for cand in candidate_triggers.values():
        retracted_triggers.append(cand)

    total_candidates = len(confirmed_triggers) + len(retracted_triggers)
    retraction_rate = (
        (len(retracted_triggers) / total_candidates * 100.0) if total_candidates > 0 else 0.0
    )

    lead_stats = {
        "p50_ms": (round(statistics.median(lead_times_ms), 1) if lead_times_ms else 0.0),
        "mean_ms": (round(statistics.mean(lead_times_ms), 1) if lead_times_ms else 0.0),
        "p90_ms": (
            round(
                statistics.quantiles(lead_times_ms, n=10)[8],
                1,
            )
            if len(lead_times_ms) >= 10
            else (round(max(lead_times_ms), 1) if lead_times_ms else 0.0)
        ),
    }

    return {
        "total_interims_evaluated": interim_count,
        "total_candidate_triggers": total_candidates,
        "confirmed_triggers_count": len(confirmed_triggers),
        "retracted_triggers_count": len(retracted_triggers),
        "retraction_rate_pct": round(retraction_rate, 2),
        "lead_time_stats": lead_stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze interim transcript stability")
    parser.add_argument("--input-log", type=Path, required=True, help="Path to event JSONL log")
    args = parser.parse_args()

    metrics = analyze_event_log(args.input_log)
    print(json.dumps(metrics, indent=2))
    return 0 if "error" not in metrics else 1


if __name__ == "__main__":
    sys.exit(main())
