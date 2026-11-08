"""MeetMind AI — Synthetic Corpus Sample Spot Check (B1).

Prints 20 random segments from ml/eval/audio/synthetic/manifest.jsonl
displaying audio path, speaker, variant, and reference text.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "ml" / "eval" / "audio" / "synthetic" / "manifest.jsonl"


def main():
    if not MANIFEST_PATH.exists():
        print(f"[ERROR] Manifest not found at {MANIFEST_PATH}. Run generator.py first.")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    print("=" * 80)
    print(" MEETMIND AI — SYNTHETIC CORPUS 20-SEGMENT SPOT CHECK (B1)")
    print(f" Total Segments in Manifest: {len(records)}")
    print("=" * 80)

    rng = random.Random(42)
    sample_size = min(20, len(records))
    sampled = rng.sample(records, sample_size)

    for i, r in enumerate(sampled, 1):
        print(f"\n[{i:02d}/20] Variant: {r.get('variant')} | Speaker: {r.get('speaker')} ({r.get('voice')})")
        print(f"      Scenario: {r.get('scenario')} | Turn #{r.get('turn_index')}")
        print(f"      Audio Path: {r.get('audio_path')}")
        print(f"      Timing: [{r.get('start_ms')}ms -> {r.get('end_ms')}ms]")
        print(f"      Text: \"{r.get('spoken_text')}\"")
        if r.get("canonical_numbers"):
            print(f"      Canonical Numbers: {r.get('canonical_numbers')}")
        if r.get("entities"):
            print(f"      Entities: {r.get('entities')}")

    print("\n" + "=" * 80)
    print(" SPOT CHECK COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()

