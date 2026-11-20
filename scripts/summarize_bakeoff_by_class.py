#!/usr/bin/env python3
"""MeetMind AI — ASR Bake-off Class & Mix-Weighted WER Summary.

Reads existing bake-off JSON artifacts (no network/API calls).
Per candidate, computes:
  - English-only WER with segment-level 95% bootstrap CI
  - Hinglish transliterated WER with segment-level 95% bootstrap CI
  - Mix-weighted composites at:
      - 100/0 (100% English / 0% Hinglish)
      - 85/15 (85% English / 15% Hinglish — default for Indian engineering teams)
      - 70/30 (70% English / 30% Hinglish)
  - Effective N (total trials, unique segments: English / Hinglish)
  - Source artifact file path

Ground Rules: DATA ONLY. Prints structured markdown table and detailed metrics.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any


def compute_segment_bootstrap_ci(
    en_means: list[float],
    hi_means: list[float],
    w_en: float,
    w_hi: float,
    n_boot: int = 2000,
    seed: int = 42,
) -> tuple[float, float, float]:
    """Compute point estimate and 95% bootstrap confidence interval over segments."""
    m_en = sum(en_means) / len(en_means) if en_means else 0.0
    m_hi = sum(hi_means) / len(hi_means) if hi_means else 0.0
    point = w_en * m_en + w_hi * m_hi

    if not en_means and not hi_means:
        return point, 0.0, 0.0

    rng = random.Random(seed)
    boot_vals = []
    n_en = len(en_means)
    n_hi = len(hi_means)

    for _ in range(n_boot):
        b_en_m = sum(rng.choice(en_means) for _ in range(n_en)) / n_en if n_en else 0.0
        b_hi_m = sum(rng.choice(hi_means) for _ in range(n_hi)) / n_hi if n_hi else 0.0
        boot_vals.append(w_en * b_en_m + w_hi * b_hi_m)

    boot_vals.sort()
    ci_low = boot_vals[int(0.025 * n_boot)]
    ci_high = boot_vals[int(0.975 * n_boot)]
    return point, ci_low, ci_high


def analyze_candidate_trials(
    cand_id: str,
    trials: list[dict[str, Any]],
    source_file: str,
) -> dict[str, Any]:
    """Process trials for a candidate and compute class-specific and composite WERs."""
    seg_en: dict[str, list[float]] = {}
    seg_hi: dict[str, list[float]] = {}
    total_valid_trials = 0

    for t in trials:
        scores = t.get("scores")
        if not scores or "wer" not in scores or scores["wer"] is None:
            continue
        wer = float(scores["wer"])
        sid = str(t.get("segment_id", t.get("scenario", "unknown")))
        lang = str(t.get("language_tag", "")).lower()

        if lang == "en":
            seg_en.setdefault(sid, []).append(wer)
            total_valid_trials += 1
        elif lang in ("hinglish", "hi"):
            seg_hi.setdefault(sid, []).append(wer)
            total_valid_trials += 1

    en_means = [sum(v) / len(v) for v in seg_en.values()]
    hi_means = [sum(v) / len(v) for v in seg_hi.values()]

    en_point, en_low, en_high = compute_segment_bootstrap_ci(
        en_means, hi_means, 1.0, 0.0, seed=hash(f"{cand_id}_en") % 1000000
    )
    hi_point, hi_low, hi_high = compute_segment_bootstrap_ci(
        en_means, hi_means, 0.0, 1.0, seed=hash(f"{cand_id}_hi") % 1000000
    )
    c100_pt, c100_low, c100_high = en_point, en_low, en_high
    c85_pt, c85_low, c85_high = compute_segment_bootstrap_ci(
        en_means, hi_means, 0.85, 0.15, seed=hash(f"{cand_id}_85") % 1000000
    )
    c70_pt, c70_low, c70_high = compute_segment_bootstrap_ci(
        en_means, hi_means, 0.70, 0.30, seed=hash(f"{cand_id}_70") % 1000000
    )

    return {
        "candidate": cand_id,
        "source_file": source_file,
        "n_total_trials": len(trials),
        "n_valid_trials": total_valid_trials,
        "n_en_segments": len(en_means),
        "n_hi_segments": len(hi_means),
        "en_wer": en_point,
        "en_ci": (en_low, en_high),
        "hi_wer": hi_point,
        "hi_ci": (hi_low, hi_high),
        "composite_100_0": c100_pt,
        "composite_100_0_ci": (c100_low, c100_high),
        "composite_85_15": c85_pt,
        "composite_85_15_ci": (c85_low, c85_high),
        "composite_70_30": c70_pt,
        "composite_70_30_ci": (c70_low, c70_high),
    }


def find_bakeoff_jsons(explicit_paths: list[Path] | None = None) -> list[Path]:
    """Find valid bake-off JSON artifacts containing raw_trials."""
    if explicit_paths:
        return [p for p in explicit_paths if p.is_file()]

    root = Path(__file__).resolve().parents[1]
    logs_dir = root / "logs"
    candidates = list(logs_dir.glob("*bakeoff*.json"))
    candidates += list(logs_dir.glob("bakeoff*.json"))

    valid_files = []
    seen = set()
    for p in sorted(candidates):
        if p.resolve() in seen:
            continue
        seen.add(p.resolve())
        try:
            with p.open("r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and d.get("raw_trials"):
                valid_files.append(p)
        except Exception:
            continue
    return valid_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize bake-off accuracy by language class and mix-weighted composites."
    )
    parser.add_argument(
        "--files",
        type=Path,
        nargs="*",
        default=None,
        help="Specific bake-off JSON artifact paths to analyze (default: scans logs/)",
    )
    args = parser.parse_args()

    files = find_bakeoff_jsons(args.files)
    if not files:
        print("ERROR: No valid bake-off JSON artifacts with raw_trials found.", file=sys.stderr)
        sys.exit(1)

    all_results = []

    for fpath in files:
        rel_path = fpath.name
        try:
            with fpath.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            print(f"Skipping {fpath}: {exc}", file=sys.stderr)
            continue

        raw_trials = data.get("raw_trials", {})
        if isinstance(raw_trials, dict):
            for cand_id, trials in raw_trials.items():
                if isinstance(trials, list) and trials:
                    res = analyze_candidate_trials(cand_id, trials, rel_path)
                    all_results.append(res)
        elif isinstance(raw_trials, list) and raw_trials:
            res = analyze_candidate_trials(rel_path.replace(".json", ""), raw_trials, rel_path)
            all_results.append(res)

    if not all_results:
        print("ERROR: No valid candidate trials found across scanned JSON files.", file=sys.stderr)
        sys.exit(1)

    print("=" * 145)
    print("MEETMIND AI — ASR BAKE-OFF ACCURACY BY CLASS & MIX-WEIGHTED COMPOSITES (DATA ONLY)")
    print(
        "Segment-level 95% bootstrap CIs (N_boot=2000). "
        "Hinglish scored with indic-transliteration (MIT)."
    )
    print("=" * 145)

    headers = [
        "Candidate [Mode]",
        "Source Artifact",
        "N (Tr/Seg)",
        "English WER [95% CI]",
        "Hinglish Tr WER [95% CI]",
        "Mix 100/0 [95% CI]",
        "Mix 85/15 [95% CI]",
        "Mix 70/30 [95% CI]",
    ]

    col_fmt = "{:<32} {:<24} {:<11} {:<22} {:<24} {:<22} {:<22} {:<22}"
    print(col_fmt.format(*headers))
    print("-" * 185)

    for r in all_results:
        n_str = f"{r['n_valid_trials']}/{r['n_en_segments']}+{r['n_hi_segments']}"
        en_str = f"{r['en_wer']:.3f} [{r['en_ci'][0]:.3f}, {r['en_ci'][1]:.3f}]"
        hi_str = f"{r['hi_wer']:.3f} [{r['hi_ci'][0]:.3f}, {r['hi_ci'][1]:.3f}]"
        c100_ci = f"[{r['composite_100_0_ci'][0]:.3f}, {r['composite_100_0_ci'][1]:.3f}]"
        c85_ci = f"[{r['composite_85_15_ci'][0]:.3f}, {r['composite_85_15_ci'][1]:.3f}]"
        c70_ci = f"[{r['composite_70_30_ci'][0]:.3f}, {r['composite_70_30_ci'][1]:.3f}]"
        c100_str = f"{r['composite_100_0']:.3f} {c100_ci}"
        c85_str = f"{r['composite_85_15']:.3f} {c85_ci}"
        c70_str = f"{r['composite_70_30']:.3f} {c70_ci}"

        print(
            col_fmt.format(
                r["candidate"],
                r["source_file"],
                n_str,
                en_str,
                hi_str,
                c100_str,
                c85_str,
                c70_str,
            )
        )

    print("=" * 185)


if __name__ == "__main__":
    main()
