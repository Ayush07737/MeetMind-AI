"""MeetMind AI — Identifier Recovery Offline Evaluation (Q-0e Section D).

Evaluates the IdentifierRecoveryEngine on the bake-off corpus hypotheses across
all candidate providers and configurations against the simulated sprint board.
Reports before/after match rates, wrong-substitution rates, and per-provider tables.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ml.eval.asr_bakeoff.normalize import extract_identifiers  # noqa: E402
from ml.eval.identifier_recovery.board import build_simulated_sprint_board  # noqa: E402
from ml.eval.identifier_recovery.matcher import IdentifierRecoveryEngine  # noqa: E402


def evaluate_identifier_recovery(bakeoff_json_path: Path, manifest_path: Path) -> dict[str, Any]:
    # 1. Load manifest ground truth spoken texts
    manifest_map: dict[str, str] = {}
    with open(manifest_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            d = json.loads(line)
            manifest_map[d["segment_id"]] = d.get("spoken_text", "")

    # 2. Load bakeoff trials
    with open(bakeoff_json_path, encoding="utf-8") as f:
        bakeoff_data = json.load(f)

    raw_trials_dict = bakeoff_data.get("raw_trials", {})
    engine = IdentifierRecoveryEngine(
        board=build_simulated_sprint_board(),
        min_confidence_margin=1,
        max_allowed_distance=1,
    )

    per_provider_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "total_ref_ids": 0,
            "matched_before": 0,
            "matched_after": 0,
            "substitutions_made": 0,
            "wrong_substitutions": 0,
            "abstentions": 0,
        }
    )

    total_ref_ids = 0
    total_matched_before = 0
    total_matched_after = 0
    total_substitutions = 0
    total_wrong_substitutions = 0
    total_abstentions = 0

    for cand_key, trial_list in raw_trials_dict.items():
        provider = cand_key.split(":")[0]
        stats = per_provider_stats[provider]

        for tr in trial_list:
            seg_id = tr.get("segment_id")
            ref_text = manifest_map.get(seg_id, "")
            ref_ids = extract_identifiers(ref_text)
            if not ref_ids:
                continue

            hyp_raw = tr.get("api", {}).get("transcript", "")
            hyp_ids_before = set(extract_identifiers(hyp_raw))

            # Apply candidate-set recovery
            rec_result = engine.recover(hyp_raw)
            hyp_ids_after = set(extract_identifiers(rec_result.recovered_text))

            stats["total_ref_ids"] += len(ref_ids)
            total_ref_ids += len(ref_ids)

            for r in ref_ids:
                if r in hyp_ids_before:
                    stats["matched_before"] += 1
                    total_matched_before += 1
                if r in hyp_ids_after:
                    stats["matched_after"] += 1
                    total_matched_after += 1

            # Check substitutions made for accuracy
            stats["substitutions_made"] += len(rec_result.substitutions)
            total_substitutions += len(rec_result.substitutions)
            stats["abstentions"] += len(rec_result.abstentions)
            total_abstentions += len(rec_result.abstentions)

            for sub in rec_result.substitutions:
                if sub.matched_id not in ref_ids:
                    stats["wrong_substitutions"] += 1
                    total_wrong_substitutions += 1

    provider_table: list[dict[str, Any]] = []
    for prov, s in sorted(per_provider_stats.items()):
        ref_n = s["total_ref_ids"]
        b_rate = (s["matched_before"] / ref_n) if ref_n > 0 else 0.0
        a_rate = (s["matched_after"] / ref_n) if ref_n > 0 else 0.0
        subs = s["substitutions_made"]
        w_rate = (s["wrong_substitutions"] / subs) if subs > 0 else 0.0
        provider_table.append(
            {
                "provider": prov,
                "total_ref_ids": ref_n,
                "match_rate_before": round(b_rate, 4),
                "match_rate_after": round(a_rate, 4),
                "delta": round(a_rate - b_rate, 4),
                "substitutions": subs,
                "wrong_substitutions": s["wrong_substitutions"],
                "wrong_substitution_rate": round(w_rate, 4),
                "abstentions": s["abstentions"],
            }
        )

    overall_before = (total_matched_before / total_ref_ids) if total_ref_ids > 0 else 0.0
    overall_after = (total_matched_after / total_ref_ids) if total_ref_ids > 0 else 0.0
    overall_wrong_rate = (
        (total_wrong_substitutions / total_substitutions) if total_substitutions > 0 else 0.0
    )

    return {
        "overall": {
            "total_ref_ids": total_ref_ids,
            "match_rate_before": round(overall_before, 4),
            "match_rate_after": round(overall_after, 4),
            "delta": round(overall_after - overall_before, 4),
            "substitutions": total_substitutions,
            "wrong_substitutions": total_wrong_substitutions,
            "wrong_substitution_rate": round(overall_wrong_rate, 4),
            "abstentions": total_abstentions,
        },
        "per_provider": provider_table,
    }


def main():
    bakeoff_json = REPO_ROOT / "logs" / "bakeoff_streaming_q0d.json"
    manifest = REPO_ROOT / "ml" / "eval" / "audio" / "synthetic_eng" / "manifest.jsonl"
    if not bakeoff_json.exists():
        print(f"Bakeoff log not found: {bakeoff_json}")
        return

    res = evaluate_identifier_recovery(bakeoff_json, manifest)
    ov = res["overall"]
    print("=" * 80)
    print("IDENTIFIER RECOVERY EVALUATION REPORT (Q-0e Section D)")
    print("=" * 80)
    print(f"Total reference identifier instances: {ov['total_ref_ids']}")
    print(f"Match rate before recovery:           {ov['match_rate_before'] * 100:.2f}%")
    print(
        f"Match rate after recovery:            {ov['match_rate_after'] * 100:.2f}% "
        f"(delta: +{ov['delta'] * 100:.2f}%)"
    )
    print(
        f"Total candidate substitutions:        {ov['substitutions']} "
        f"(wrong: {ov['wrong_substitutions']})"
    )
    print(
        f"Wrong-substitution rate:              {ov['wrong_substitution_rate'] * 100:.2f}% "
        f"(target <= 1.0%)"
    )
    print(f"Total abstentions:                    {ov['abstentions']}")
    print("-" * 80)
    print(f"{'PROVIDER':<25} {'REF_IDS':>8} {'BEFORE':>8} {'AFTER':>8} {'DELTA':>8} {'WRONG%':>8}")
    print("-" * 80)
    for p in res["per_provider"]:
        print(
            f"{p['provider']:<25} {p['total_ref_ids']:>8} "
            f"{p['match_rate_before'] * 100:>7.2f}% {p['match_rate_after'] * 100:>7.2f}% "
            f"+{p['delta'] * 100:>6.2f}% {p['wrong_substitution_rate'] * 100:>7.2f}%"
        )
    print("=" * 80)


if __name__ == "__main__":
    main()
