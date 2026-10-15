"""MeetMind AI — L7 Empirical Matrix Runner (Session D2b).

Runs the 4 empirical reconnect conditions against live Deepgram Nova-3:
1. Baseline (no-drop)
2. 1 drop (t=10s)
3. 2 drops (t=8s, 16s)
4. 5s outage (outage at t=10s for 5.0s)

For each condition:
- drop-to-reaccept ms
- set of client_seq handed to provider sessions vs sent
- duplicate/overlapping finals (target = 0)
- WER vs no-drop baseline (delta <= 0.10 absolute)

Security:
- Never prints DEEPGRAM_API_KEY.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


def main() -> None:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] DEEPGRAM_API_KEY is not set.")
        sys.exit(1)

    print("=" * 80)
    print(" MeetMind AI — L7 Empirical Reconnect & Outage Matrix")
    print("=" * 80)
    print("Conditions:")
    print("  1. Baseline (no-drop)")
    print("  2. Single Drop (t=10s)")
    print("  3. Double Drops (t=8s, 16s)")
    print("  4. 5.0s Outage (t=10s, 5.0s)")
    print("-" * 80)

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/asr/tests/live/test_l7_real_reconnect.py",
        "-v",
        "-s",
    ]

    env = os.environ.copy()
    env["RUN_DEEPGRAM_TESTS"] = "1"
    env["PYTHONPATH"] = (
        str(REPO_ROOT)
        + os.pathsep
        + str(REPO_ROOT / "backend" / "asr")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "gateway")
        + os.pathsep
        + str(REPO_ROOT / "backend" / "security")
        + os.pathsep
        + str(REPO_ROOT / "packages" / "shared-schemas" / "schemas_py")
        + os.pathsep
        + str(REPO_ROOT / "ml")
    )

    result = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
