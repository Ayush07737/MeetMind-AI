"""MeetMind AI — Golden Quality Evaluation CLI (Q6 / Addendum Section D).

DATA ONLY output. Runs AbstainAlways, RetrievalOnly, or NaiveFabricator pipelines
over the deterministic seed set (NON-AUTHORITATIVE) and optional external EVAL_HELDOUT_PATH.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from meetmind_quality.eval import (
    DEFAULT_MIN_CASES_FOR_CLAIM,
    DEFAULT_SEED_PATH,
    AbstainAlways,
    EvalGatesConfig,
    NaiveFabricator,
    RateMetric,
    RetrievalOnly,
    run_evaluation,
)

PIPELINES = {
    "AbstainAlways": AbstainAlways,
    "RetrievalOnly": RetrievalOnly,
    "NaiveFabricator": NaiveFabricator,
}


def _fmt_rate(rate_dict: dict[str, Any] | None) -> str:
    if not rate_dict:
        return "N=0"
    n = rate_dict["denominator"]
    num = rate_dict["numerator"]
    est = rate_dict["point_estimate"]
    lo = rate_dict["cp_95_lower"]
    hi = rate_dict["cp_95_upper"]
    flag = rate_dict["flag"]
    if est is None:
        return f"n={num}/0 (N=0) [{flag}]"
    return f"{est:.4f} ({num}/{n}, cp95_lo={lo:.4f}, cp95_hi={hi:.4f}) [{flag}]"


def _print_split_table(split: dict[str, Any]) -> None:
    label = split["split_label"]
    print(f"SPLIT: {label}")
    if not split["provided"]:
        print("  status: NOT PROVIDED")
        return

    print(
        f"  seed={split['seed']} total_cases={split['total_cases']} "
        f"audio_ref_cases={split['audio_ref_cases']}"
    )
    for key in (
        "trigger_precision",
        "trigger_recall",
        "supported_claim_precision",
        "displayed_unsupported_claim_rate",
        "abstention_correctness",
    ):
        print(f"  {key}: {_fmt_rate(split.get(key))}")

    for band in ("high", "medium", "low"):
        cal = split.get("band_calibration", {}).get(band)
        print(f"  calibration_{band}: {_fmt_rate(cal)}")

    print(
        f"  latency_overhead_ms: n={split['latency_overhead_n']} "
        f"p50={split['latency_overhead_p50_ms']} p95={split['latency_overhead_p95_ms']}"
    )
    print(
        f"  provisional_gates_met={split['provisional_gates_met']} "
        f"gate_failures={split['gate_failures']}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate MeetMind InsightPipeline baselines")
    parser.add_argument(
        "--pipeline",
        choices=list(PIPELINES.keys()),
        default="RetrievalOnly",
        help="Single baseline pipeline to evaluate",
    )
    parser.add_argument(
        "--all-baselines",
        action="store_true",
        help="Run AbstainAlways, RetrievalOnly, and NaiveFabricator calibration baselines",
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="Path to deterministic seed JSONL",
    )
    parser.add_argument(
        "--heldout-env",
        default="EVAL_HELDOUT_PATH",
        help="Environment variable pointing to external held-out JSONL",
    )
    parser.add_argument(
        "--min-cases-for-claim",
        type=int,
        default=DEFAULT_MIN_CASES_FOR_CLAIM,
        help="Sample size threshold below which rates emit INSUFFICIENT N (default 300)",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Optional output path to dump structured JSON report",
    )
    args = parser.parse_args(argv)

    gates = EvalGatesConfig(min_cases_for_claim=args.min_cases_for_claim)
    selected = list(PIPELINES.keys()) if args.all_baselines else [args.pipeline]

    results: list[dict[str, Any]] = []
    exit_code = 0

    for name in selected:
        pipe = PIPELINES[name]()
        res = run_evaluation(
            pipeline=pipe,
            seed_path=args.seed_path,
            heldout_env_var=args.heldout_env,
            gates=gates,
        )
        results.append(res)
        print(f"PIPELINE: {res['pipeline']}")
        print(f"SEED_PATH: {res['seed_path']}")
        print(f"HELDOUT_ENV: {res['heldout_env_var']}={res['heldout_status']}")
        _print_split_table(res["seed"])
        _print_split_table(res["heldout"])
        print("---")

        # Check calibration sanity when running a single pipeline directly
        if not args.all_baselines:
            target_split = res["heldout"] if res["heldout"]["provided"] else res["seed"]
            if not target_split["provisional_gates_met"]:
                exit_code = 1

    # Under --all-baselines calibration mode, verify RetrievalOnly passes and NaiveFabricator fails
    if args.all_baselines:
        by_name = {r["pipeline"]: r for r in results}
        abstain_halluc = RateMetric.model_validate(
            by_name["AbstainAlways"]["seed"]["displayed_unsupported_claim_rate"]
        )
        retrieval_ok = by_name["RetrievalOnly"]["seed"]["provisional_gates_met"]
        fabricator_ok = by_name["NaiveFabricator"]["seed"]["provisional_gates_met"]
        print(
            f"CALIBRATION_SUMMARY: "
            f"abstain_always_unsupported={abstain_halluc.numerator} "
            f"retrieval_only_gates_met={retrieval_ok} "
            f"naive_fabricator_gates_met={fabricator_ok}"
        )
        if abstain_halluc.numerator != 0 or not retrieval_ok or fabricator_ok:
            exit_code = 1

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
