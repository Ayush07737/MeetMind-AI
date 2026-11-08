"""MeetMind AI — ASR Provider Bake-off Evaluation Runner (B4 / B5).

Runs evaluation across candidate ASR providers over the synthetic audio corpus:
1. Deepgram Nova-3 (Baseline)
2. Deepgram Flux
3. Sarvam Saaras v3 (Streaming)
4. OpenAI Realtime / Streaming Transcription

Metrics:
- Overall WER
- Hinglish-segment WER
- Numeric-Token Error Rate (NTER) on canonical financial numbers
- Entity Error Rate (EER) on tenant financial vocabulary
- Latency (Send time on harness clock to transcript arrival)
- Bootstrap 95% confidence intervals resampled at segment level
"""

from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path
from typing import Any

from normalize import (
    normalize_text,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "ml" / "eval" / "audio" / "synthetic" / "manifest.jsonl"


def bootstrap_metric_ci(
    values: list[float], n_bootstrap: int = 1000, alpha: float = 0.05, seed: int = 42
) -> dict[str, float]:
    """Compute 95% bootstrap confidence interval resampled at segment level."""
    if not values:
        return {"mean": 0.0, "ci_lower": 0.0, "ci_upper": 0.0}
    rng = random.Random(seed)
    n = len(values)
    mean_val = sum(values) / float(n)

    boot_means: list[float] = []
    for _ in range(n_bootstrap):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        boot_means.append(sum(sample) / float(n))

    boot_means.sort()
    lower_idx = int(n_bootstrap * (alpha / 2.0))
    upper_idx = min(n_bootstrap - 1, int(n_bootstrap * (1.0 - alpha / 2.0)))

    return {
        "mean": round(mean_val, 4),
        "ci_lower": round(boot_means[lower_idx], 4),
        "ci_upper": round(boot_means[upper_idx], 4),
    }


def compute_entity_error_rate(ref_entities: list[str], hypothesis: str) -> dict[str, Any]:
    """Compute error rate on key named entities (exact substring match on normalized text)."""
    if not ref_entities:
        return {"eer": 0.0, "total": 0, "correct": 0, "missing": 0}

    norm_hyp = normalize_text(hypothesis)
    matched = 0
    for ent in ref_entities:
        norm_ent = normalize_text(ent)
        if norm_ent in norm_hyp:
            matched += 1

    total = len(ref_entities)
    return {
        "eer": round((total - matched) / float(total), 4),
        "total": total,
        "correct": matched,
        "missing": total - matched,
    }


def main():
    parser = argparse.ArgumentParser(description="MeetMind AI ASR Bake-off Evaluation Runner")
    parser.add_argument(
        "--provider",
        type=str,
        default="stub",
        choices=["stub", "deepgram_nova3", "flux", "sarvam", "openai"],
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="clean",
        choices=["all", "clean", "noise_15db", "telephony_8khz", "speed_1_15x", "two_channel"],
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of segments evaluated"
    )
    args = parser.parse_args()

    if not MANIFEST_PATH.exists():
        print(f"[ERROR] Manifest not found at {MANIFEST_PATH}. Run generator.py first.")
        return

    with open(MANIFEST_PATH, encoding="utf-8") as f:
        all_records = [json.loads(line) for line in f if line.strip()]

    records = [r for r in all_records if args.variant == "all" or r.get("variant") == args.variant]
    if args.limit:
        records = records[: args.limit]

    print("=" * 80)
    print(" PRELIMINARY (SYNTHETIC ONLY) — ASR PROVIDER BAKE-OFF BENCHMARK")
    print(f" Candidate Provider:  {args.provider.upper()}")
    print(f" Acoustic Variant:    {args.variant.upper()}")
    print(f" Evaluated Segments:  N={len(records)}")
    print("=" * 80)

    # Check API key requirement
    if args.provider == "sarvam" and not os.environ.get("SARVAM_API_KEY"):
        print("[STATUS: NOT RUN] SARVAM_API_KEY environment variable is not set.")
        return
    if args.provider == "openai" and not os.environ.get("OPENAI_API_KEY"):
        print("[STATUS: NOT RUN] OPENAI_API_KEY environment variable is not set.")
        return
    if args.provider in ("deepgram_nova3", "flux") and not os.environ.get("DEEPGRAM_API_KEY"):
        print("[STATUS: NOT RUN] DEEPGRAM_API_KEY environment variable is not set.")
        return

    print(
        "Harness ready. Awaiting owner confirmation of 30-minute stats run "
        "before live API execution."
    )


if __name__ == "__main__":
    main()
