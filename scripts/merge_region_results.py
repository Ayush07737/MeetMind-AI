"""MeetMind AI — Multi-Region Bake-Off Results Merger (Q-0e Section E2).

Reads regional bake-off JSON output files (e.g. from Mumbai, Frankfurt, N. Virginia,
Singapore/Sydney), merges them, and prints the consolidated Region x Provider matrix:
RTT, first-partial lag (TTFB), time-to-stable-text, end-of-turn latency (EOT),
retraction rate, and the RTT share of the Lane-0 budget (SLO <= 300ms).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
LANE_0_BUDGET_MS = 300.0  # P95 target for Lane-0 classification & cards


def merge_regional_results(input_files: list[Path]) -> list[dict[str, Any]]:
    merged_rows: list[dict[str, Any]] = []

    for file_path in input_files:
        if not file_path.exists():
            continue
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        region = data.get("region", file_path.stem.replace("bakeoff_", ""))
        summaries = data.get("summaries", {})

        for cand_key, summary in summaries.items():
            provider = cand_key.split(":")[0]
            config_mode = summary.get("config", "default")
            rtt_p50 = summary.get("network_rtt_p50_ms", 0.0)
            ttfb_p50 = summary.get("ttfb_interim_p50_ms", 0.0)
            stable_p50 = summary.get("stable_text_p50_ms", 0.0)
            eot_p50 = summary.get("eot_p50_ms", 0.0)
            retr_pct = summary.get("retraction_rate_pct", 0.0)
            rtt_budget_share = (rtt_p50 / LANE_0_BUDGET_MS) * 100.0 if LANE_0_BUDGET_MS > 0 else 0.0

            merged_rows.append(
                {
                    "region": region,
                    "provider": provider,
                    "config": config_mode,
                    "rtt_p50_ms": round(rtt_p50, 1),
                    "ttfb_p50_ms": round(ttfb_p50, 1),
                    "stable_text_p50_ms": round(stable_p50, 1),
                    "eot_p50_ms": round(eot_p50, 1),
                    "retraction_rate_pct": round(retr_pct, 2),
                    "rtt_budget_share_pct": round(rtt_budget_share, 1),
                }
            )

    return merged_rows


def print_matrix_table(rows: list[dict[str, Any]]) -> None:
    print("=" * 105)
    print("MULTI-REGION ASR BENCHMARK MATRIX (Region x Provider)")
    print(f"Lane-0 Budget: {LANE_0_BUDGET_MS:.0f}ms P95 ceiling")
    print("=" * 105)
    cols = [
        f"{'REGION':<12}",
        f"{'PROVIDER':<18}",
        f"{'CONFIG':<11}",
        f"{'RTT_P50':>8}",
        f"{'TTFB_50':>8}",
        f"{'STABLE50':>9}",
        f"{'EOT_P50':>8}",
        f"{'RETR%':>6}",
        f"{'RTT_BUD%':>9}",
    ]
    print(" ".join(cols))
    print("-" * 105)
    for r in sorted(rows, key=lambda x: (x["region"], x["provider"], x["config"])):
        print(
            f"{r['region']:<15} {r['provider']:<22} {r['config']:<11} "
            f"{r['rtt_p50_ms']:>7.1f}m {r['ttfb_p50_ms']:>7.1f}m "
            f"{r['stable_text_p50_ms']:>8.1f}m {r['eot_p50_ms']:>7.1f}m "
            f"{r['retraction_rate_pct']:>6.2f}% {r['rtt_budget_share_pct']:>11.1f}%"
        )
    print("=" * 105)


def main():
    parser = argparse.ArgumentParser(description="Merge regional bake-off benchmark outputs.")
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="List of regional bake-off JSON files (e.g. logs/bakeoff_mumbai.json ...)",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=REPO_ROOT / "logs" / "multi_region_matrix.json",
        help="Destination path for merged matrix JSON",
    )
    args = parser.parse_args()

    files = args.files
    if not files:
        # Default search in logs/
        files = list((REPO_ROOT / "logs").glob("bakeoff_region_*.json"))
        if not files:
            files = [REPO_ROOT / "logs" / "bakeoff_streaming_q0d.json"]

    rows = merge_regional_results(files)
    if not rows:
        print("No valid regional result files found to merge.")
        return

    print_matrix_table(rows)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"Merged output saved to: {args.output_json}")


if __name__ == "__main__":
    main()
