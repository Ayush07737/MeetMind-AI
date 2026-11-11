"""Acceptance Test: End-to-End Latency Budget Quality Contract (Q2).

Verifies the budget check contract:
1. Live simulated pipeline against real Gateway and Stub ASR satisfies stage overheads (<= 25ms)
   and monotonic timing stamps.
2. Official benchmark log (logs/bench30.log) passes budget checks.
3. Artificially inflated benchmark log fails budget checks (non-zero exit code).
4. CLI execution via subprocess validates process exit codes.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.budget_check import check_benchmark_log, run_budget_check

REPO_ROOT = Path(__file__).resolve().parents[4]
pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


@pytest.mark.asyncio
@pytest.mark.timeout(180)
async def test_budget_check_simulated_pipeline() -> None:
    """Run live simulated pipeline check through real Gateway and Stub ASR (N=200 utterances)."""
    redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
    passed = await run_budget_check(
        audio_chunks=1000,
        target_utterances=200,
        redis_url=redis_url,
        verbose=True,
    )
    assert passed is True, "Live simulated pipeline budget check failed"


def test_budget_check_bench30_log() -> None:
    """Verify owner's 30-minute benchmark log against latency budget."""
    bench30_path = REPO_ROOT / "logs" / "bench30.log"
    assert bench30_path.exists(), f"Benchmark log not found at {bench30_path}"
    passed = check_benchmark_log(log_path=bench30_path, verbose=False)
    assert passed is True, "bench30.log failed latency budget check"


def test_budget_check_inflated_log_fails(tmp_path: Path) -> None:
    """Verify that an artificially inflated benchmark log fails the budget check."""
    inflated_content = (
        "LATENCY BENCHMARK RESULTS\n"
        "End-to-End Latency      : P50=2500.00ms | P95=4200.00ms | Max=5000.00ms\n"
        "Service-Added Latency   : P50=  45.00ms | P95= 110.00ms | Max= 150.00ms\n"
    )
    inflated_file = tmp_path / "inflated_bench.log"
    inflated_file.write_text(inflated_content, encoding="utf-8")

    passed = check_benchmark_log(log_path=inflated_file, verbose=False)
    assert passed is False, "Inflated benchmark log should have failed budget check"


def test_budget_check_cli_invocation(tmp_path: Path) -> None:
    """Verify CLI exit codes: 0 for bench30.log, non-zero for inflated log."""
    bench30_path = REPO_ROOT / "logs" / "bench30.log"
    cmd_pass = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "budget_check.py"),
        "--log",
        str(bench30_path),
    ]
    res_pass = subprocess.run(cmd_pass, capture_output=True, text=True)
    assert res_pass.returncode == 0, f"CLI expected 0, got {res_pass.returncode}: {res_pass.stderr}"

    inflated_content = (
        "LATENCY BENCHMARK RESULTS\n"
        "End-to-End Latency      : P50=2500.00ms | P95=4200.00ms | Max=5000.00ms\n"
        "Service-Added Latency   : P50=  45.00ms | P95= 110.00ms | Max= 150.00ms\n"
    )
    inflated_file = tmp_path / "inflated_bench.log"
    inflated_file.write_text(inflated_content, encoding="utf-8")

    cmd_fail = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "budget_check.py"),
        "--log",
        str(inflated_file),
    ]
    res_fail = subprocess.run(cmd_fail, capture_output=True, text=True)
    assert res_fail.returncode != 0, f"CLI expected non-zero, got {res_fail.returncode}"
