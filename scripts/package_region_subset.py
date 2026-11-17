"""MeetMind AI — Package Stratified Audio Subset for Remote VM Benchmarks.

Selects N stratified segments from the synthetic engineering corpus,
bundles the WAV audio files along with a self-contained sliced manifest.jsonl,
and packages them into a ZIP archive for deployment to cloud VMs (e.g. AWS Mumbai, GCP Delhi).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ml.eval.asr_bakeoff.run_bakeoff import select_stratified_corpus  # noqa: E402


def compute_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def package_subset(
    manifest_path: Path,
    sample_size: int = 30,
    seed: int = 42,
    output_zip: Path | None = None,
) -> dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path, encoding="utf-8") as f:
        all_records = [json.loads(line) for line in f if line.strip()]

    selected = select_stratified_corpus(all_records, sample_size=sample_size, seed=seed)

    if output_zip is None:
        output_zip = REPO_ROOT / "dist" / f"region_subset_{sample_size}.zip"

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    bundled_manifest_records: list[dict[str, Any]] = []
    total_audio_s = 0.0

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rec in selected:
            orig_audio_rel = rec["audio_path"]
            orig_audio_abs = REPO_ROOT / orig_audio_rel
            if not orig_audio_abs.exists():
                raise FileNotFoundError(f"Audio file missing from repo: {orig_audio_abs}")

            # Store inside zip preserving scenario subfolder to avoid turn name collisions
            sc_dir = rec.get("scenario", "default")
            arcname = f"audio/{sc_dir}/{orig_audio_abs.name}"
            zf.write(orig_audio_abs, arcname=arcname)

            dur_s = (float(rec.get("end_ms", 0)) - float(rec.get("start_ms", 0))) / 1000.0
            total_audio_s += max(0.0, dur_s)

            mod_rec = dict(rec)
            mod_rec["audio_path"] = arcname
            bundled_manifest_records.append(mod_rec)

        # Write sliced manifest.jsonl into zip root
        manifest_str = "\n".join(json.dumps(r) for r in bundled_manifest_records) + "\n"
        zf.writestr("manifest.jsonl", manifest_str.encode("utf-8"))

        # Write README.md with VM execution instructions
        readme_content = f"""# MeetMind AI — Region Subset Bundle ({sample_size} Segments)

This bundle contains {len(selected)} stratified segments from the synthetic engineering corpus.

## Contents
- `manifest.jsonl`: Sliced manifest for the {len(selected)} segments
- `audio/`: 16kHz PCM16 audio WAV files

## Execution on Remote VM (e.g. AWS Mumbai ap-south-1 / GCP Delhi)
1. Extract archive:
   ```bash
   unzip region_subset_{sample_size}.zip -d region_eval
   cd region_eval
   ```
2. Run bake-off / evaluation against bundled manifest:
   ```bash
   uv run python ml/eval/asr_bakeoff/run_bakeoff.py \\
       --manifest manifest.jsonl \\
       --sample-size {sample_size} \\
       --reps 3 \\
       --configs default,aggressive \\
       --json-out results_{sample_size}.json
   ```
"""
        zf.writestr("README.md", readme_content.encode("utf-8"))

    zip_bytes = output_zip.stat().st_size
    zip_sha256 = compute_file_sha256(output_zip)

    summary = {
        "output_zip": str(output_zip),
        "sample_size": len(selected),
        "seed": seed,
        "total_audio_seconds": round(total_audio_s, 2),
        "zip_size_bytes": zip_bytes,
        "zip_size_mb": round(zip_bytes / (1024 * 1024), 2),
        "sha256": zip_sha256,
        "scenarios": sorted({r["scenario"] for r in selected}),
        "variants": dict(Counter(r["variant"] for r in selected)),
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Package stratified audio subset and manifest into a ZIP for VM evaluation."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=30,
        help="Number of stratified segments to bundle (default: 30)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Sampling seed for deterministic stratification (default: 42)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=REPO_ROOT / "ml/eval/audio/synthetic_eng/manifest.jsonl",
        help="Path to source manifest.jsonl",
    )
    parser.add_argument(
        "--output-zip",
        type=Path,
        default=None,
        help="Target ZIP archive path (default: dist/region_subset_{sample_size}.zip)",
    )
    args = parser.parse_args()

    print(f"Packaging {args.sample_size} stratified segments from {args.manifest}...")
    summary = package_subset(
        manifest_path=args.manifest,
        sample_size=args.sample_size,
        seed=args.seed,
        output_zip=args.output_zip,
    )

    print("\n" + "=" * 90)
    print("REGION SUBSET BUNDLE CREATED SUCCESSFULLY")
    print("-" * 90)
    print(f"ZIP Path:     {summary['output_zip']}")
    print(f"File Size:    {summary['zip_size_mb']} MB ({summary['zip_size_bytes']} bytes)")
    print(f"SHA-256:      {summary['sha256']}")
    print(
        f"Segments:     {summary['sample_size']} segments ({summary['total_audio_seconds']}s audio)"
    )
    print(f"Variants:     {summary['variants']}")
    print("=" * 90)
    return 0


if __name__ == "__main__":
    sys.exit(main())
