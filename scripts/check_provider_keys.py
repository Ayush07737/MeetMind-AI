"""MeetMind AI — Provider API Key Presence & Env Var Inventory (DATA ONLY, A1.9).

Reads `.env` (without overriding explicit process environment variables) and
reports which primary env var and accepted aliases the ASR bake-off and runtime
read, plus PRESENT / ABSENT status for each provider. Never prints key values.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import dotenv_values, load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# Canonical mapping shared with ml/eval/asr_bakeoff/run_bakeoff.py.
PROVIDER_KEY_SPECS: list[dict[str, Any]] = [
    {
        "provider": "deepgram",
        "candidates": [
            "deepgram_nova3",
            "deepgram_nova3_mono",
            "deepgram_nova3_multi",
            "deepgram_flux",
            "deepgram_async",
        ],
        "primary_env": "DEEPGRAM_API_KEY",
        "aliases": ["DG_API_KEY"],
        "extra_env": ["ASR_REGION", "ASR_ALLOWED_REGIONS", "DEEPGRAM_BASE_URL"],
    },
    {
        "provider": "gladia",
        "candidates": ["gladia_solaria1", "gladia_async"],
        "primary_env": "GLADIA_API_KEY",
        "aliases": ["GLADIA_KEY"],
        "extra_env": [],
    },
    {
        "provider": "assemblyai",
        "candidates": ["assemblyai_universal", "assemblyai_async"],
        "primary_env": "ASSEMBLYAI_API_KEY",
        "aliases": ["AAI_API_KEY"],
        "extra_env": [],
    },
    {
        "provider": "sarvam",
        "candidates": ["sarvam", "sarvam_async"],
        "primary_env": "SARVAM_API_KEY",
        "aliases": ["SARVAM_API_SUBSCRIPTION_KEY"],
        "extra_env": [],
    },
    {
        "provider": "openai",
        "candidates": ["openai", "openai_async"],
        "primary_env": "OPENAI_API_KEY",
        "aliases": ["OPENAI_KEY"],
        "extra_env": [],
    },
]


def resolve_key_status(dotenv_path: Path | None = None) -> list[dict[str, Any]]:
    """Inspect presence/absence of each provider key across process env and `.env`."""
    env_file = dotenv_path or (REPO_ROOT / ".env")
    process_keys_before = {k for k, v in os.environ.items() if v and v.strip()}
    dotenv_map = (
        {k: v for k, v in dotenv_values(env_file).items() if v and str(v).strip()}
        if env_file.exists()
        else {}
    )
    if env_file.exists():
        load_dotenv(env_file, override=False)

    rows: list[dict[str, Any]] = []
    for spec in PROVIDER_KEY_SPECS:
        all_names = [spec["primary_env"], *spec["aliases"]]
        matched_var: str | None = None
        for name in all_names:
            val = os.environ.get(name, "").strip()
            if val:
                matched_var = name
                break
        in_process = [n for n in all_names if n in process_keys_before]
        in_dotenv = [n for n in all_names if n in dotenv_map]
        rows.append(
            {
                "provider": spec["provider"],
                "candidates": spec["candidates"],
                "primary_env": spec["primary_env"],
                "aliases": spec["aliases"],
                "status": "PRESENT" if matched_var else "ABSENT",
                "matched_env_var": matched_var,
                "in_process_env": in_process,
                "in_dotenv": in_dotenv,
                "extra_env_present": {
                    ek: ("PRESENT" if os.environ.get(ek, "").strip() else "ABSENT")
                    for ek in spec["extra_env"]
                },
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="MeetMind AI — Check provider API key presence without revealing values."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=REPO_ROOT / ".env",
        help="Path to .env file to inspect (default: .env at repo root)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of table output",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any provider key is ABSENT",
    )
    args = parser.parse_args(argv)

    rows = resolve_key_status(args.env_file)
    if args.json:
        print(json.dumps({"env_file": str(args.env_file), "providers": rows}, indent=2))
    else:
        print(f"ENV_FILE={args.env_file} exists={args.env_file.exists()}")
        print(
            f"{'PROVIDER':<12} {'PRIMARY_ENV':<20} {'ALIASES':<30} {'STATUS':<8} "
            f"{'RESOLVED_VAR':<28} {'IN_DOTENV':<20}"
        )
        print("-" * 124)
        for r in rows:
            aliases_s = ",".join(r["aliases"]) if r["aliases"] else "-"
            matched_s = r["matched_env_var"] or "NONE"
            dotenv_s = ",".join(r["in_dotenv"]) if r["in_dotenv"] else "NONE"
            print(
                f"{r['provider']:<12} {r['primary_env']:<20} {aliases_s:<30} "
                f"{r['status']:<8} {matched_s:<28} {dotenv_s:<20}"
            )
            if r["extra_env_present"]:
                extras = " ".join(f"{k}={v}" for k, v in r["extra_env_present"].items())
                print(f"  EXTRA_ENV: {extras}")

    if args.strict and any(r["status"] != "PRESENT" for r in rows):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
