"""MeetMind AI — Synthetic Corpus Sample Spot Check (B1 / Corpus v2).

Prints 20 random segments from ml/eval/audio/synthetic_eng/manifest.jsonl
displaying audio path, speaker, variant, entities, decisions, and reference text.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "ml" / "eval" / "audio" / "synthetic_eng" / "manifest.jsonl"
LEGACY_MANIFEST = REPO_ROOT / "ml" / "eval" / "audio" / "synthetic" / "manifest.jsonl"


def main():
    parser = argparse.ArgumentParser(description="Synthetic Corpus Spot Check")
    parser.add_argument(
        "--corpus",
        choices=["eng", "finance"],
        default="eng",
        help="Corpus domain to inspect (eng|finance, default: eng)",
    )
    args = parser.parse_args()

    manifest_path = DEFAULT_MANIFEST if args.corpus == "eng" else LEGACY_MANIFEST
    if not manifest_path.exists():
        manifest_path = (
            LEGACY_MANIFEST if args.corpus == "eng" and LEGACY_MANIFEST.exists() else manifest_path
        )

    if not manifest_path.exists():
        print(f"[ERROR] Manifest not found at {manifest_path}. Run generator.py first.")
        return

    with open(manifest_path, encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    print("=" * 80)
    print(f" MEETMIND AI — SYNTHETIC CORPUS 20-SEGMENT SPOT CHECK [DOMAIN: {args.corpus.upper()}]")
    print(f" Manifest Path:             {manifest_path.relative_to(REPO_ROOT)}")
    print(f" Total Segments in Manifest: {len(records)}")
    unique_count = sum(1 for r in records if r.get("variant") == "clean") or (len(records) // 5)
    print(f" Unique Utterances:         {unique_count}")
    owner_verified_count = sum(r.get("owner_verified_count", 0) for r in records)
    print(f" Owner Verified Count:      {owner_verified_count}")
    print("=" * 80)

    rng = random.Random(42)
    sample_size = min(20, len(records))
    sampled = rng.sample(records, sample_size)

    for i, r in enumerate(sampled, 1):
        spk_str = f"Speaker: {r.get('speaker')} ({r.get('voice')})"
        print(f"\n[{i:02d}/20] Segment: {r.get('segment_id', 'N/A')} | {spk_str}")
        print(
            f"      Variant: {r.get('variant')} | Scenario: {r.get('scenario')} "
            f"| Turn #{r.get('turn_index')}"
        )
        print(f"      Audio Path: {r.get('audio_path')}")
        print(
            f"      Timing: [{r.get('start_ms')}ms -> {r.get('end_ms')}ms] "
            f"| Lang: {r.get('language_tag')}"
        )
        print(f'      Text: "{r.get("spoken_text")}"')
        if r.get("entities"):
            print(f"      Entities: {r.get('entities')}")
        if r.get("decisions"):
            print(f"      Decisions: {r.get('decisions')}")
        if r.get("action_items"):
            print(f"      Action Items: {r.get('action_items')}")

    print("\n" + "=" * 80)
    print(" SPOT CHECK COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
