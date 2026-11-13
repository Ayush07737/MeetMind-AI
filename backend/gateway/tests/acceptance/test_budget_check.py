"""Acceptance Test: End-to-End Latency Budget Quality Contract (Q2 / A3).

Verifies the budget check contract:
1. Live simulated pipeline against real Gateway and Stub ASR satisfies stage overheads (<= 25ms),
   Lane-0 t0_to_stable_text (<= 2000ms), and monotonic timing stamps.
2. Benchmark logs MUST include Per-Unique-Utterance Medians and 95% Bootstrap CI upper bound
   <= 2000ms (both logs/bench30.log and logs/q0a_stats30_india.log pass point-estimate P95
   and service P95, and fail the CI upper bound > 2000ms; missing CI fails).
3. Explicit resolved log path, LastWriteTime, and ASR-receipt vs T0 clock notice are printed.
4. Lane-0 time_to_stable_text from A1 bakeoff JSON is gated against lane_0_instant target_p95_ms.
5. CLI execution via subprocess validates process exit codes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.budget_check import check_benchmark_log, run_budget_check

REPO_ROOT = Path(__file__).resolve().parents[4]
# fast log/CLI checks use 30s default; simulated below uses 200s
pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio
@pytest.mark.timeout(200)
async def test_budget_check_simulated_pipeline() -> None:
    """Run live simulated pipeline through real Gateway and Stub ASR (~100s observed; 2x=200s)."""
    redis_url = os.environ.get("ACCEPTANCE_REDIS_URL", "redis://127.0.0.1:6379/0")
    passed = await run_budget_check(
        audio_chunks=1000,
        target_utterances=200,
        redis_url=redis_url,
        verbose=True,
    )
    assert passed is True, "Live simulated pipeline budget check failed"


def _compliant_log_text(ci_upper_ms: float = 1890.0) -> str:
    return (
        "LATENCY BENCHMARK RESULTS\n"
        "End-to-End Latency      : P50=426.89ms | P95=1859.91ms | Max=2291.51ms\n"
        "Interim Latency         : P50=582.15ms | P95=1587.82ms | Max=3094.70ms\n"
        "Service-Added Latency   : P50=  3.08ms | P95=  5.20ms | Max= 16.30ms\n"
        "Per-Unique-Utterance Medians (N=85 independent utterances):\n"
        f"   E2E Latency: P50=423.975ms | P95=1750.000ms "
        f"[95% Bootstrap CI: 1610.00ms, {ci_upper_ms:.2f}ms] | Max=1950.000ms\n"
    )


def test_budget_check_real_logs_enforce_ci_upper_bound() -> None:
    """Valid 30m logs with CI upper > 2000ms fail strict CI gate without weakening ceiling."""
    for rel in ("logs/bench30.log", "logs/q0a_stats30_india.log"):
        log_path = REPO_ROOT / rel
        if not log_path.exists():
            pytest.skip(f"Log not found at {log_path}")
        passed = check_benchmark_log(log_path=log_path, verbose=False)
        assert passed is False, f"{rel} has bootstrap CI upper > 2000ms and must fail CI gate"


def test_budget_check_compliant_log_and_lane0_bakeoff(tmp_path: Path) -> None:
    """Compliant CI <= 2000ms passes; missing CI fails; Lane-0 bakeoff P95 is gated."""
    ok_log = tmp_path / "ok_bench.log"
    ok_log.write_text(_compliant_log_text(ci_upper_ms=1890.0), encoding="utf-8")

    ok_bakeoff = tmp_path / "bakeoff_ok.json"
    ok_bakeoff.write_text(
        json.dumps(
            {
                "summaries": {
                    "deepgram_nova3:default": {
                        "name": "Deepgram Nova-3 (default)",
                        "stable_text_p50_ms": 510.0,
                        "stable_text_p95_ms": 1450.0,
                        "stable_text_n": 450,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    assert check_benchmark_log(log_path=ok_log, bakeoff_json=ok_bakeoff, verbose=False) is True

    slow_bakeoff = tmp_path / "bakeoff_slow.json"
    slow_bakeoff.write_text(
        json.dumps(
            {
                "summaries": {
                    "deepgram_nova3:default": {
                        "name": "Deepgram Nova-3 (default)",
                        "stable_text_p50_ms": 950.0,
                        "stable_text_p95_ms": 2350.0,
                        "stable_text_n": 450,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    assert check_benchmark_log(log_path=ok_log, bakeoff_json=slow_bakeoff, verbose=False) is False

    missing_ci_log = tmp_path / "missing_ci.log"
    missing_ci_log.write_text(
        "LATENCY BENCHMARK RESULTS\n"
        "End-to-End Latency      : P50=420.00ms | P95=1850.00ms | Max=1900.00ms\n"
        "Service-Added Latency   : P50=  3.00ms | P95=  5.00ms | Max=  9.00ms\n",
        encoding="utf-8",
    )
    assert check_benchmark_log(log_path=missing_ci_log, verbose=False) is False


def test_budget_check_inflated_log_fails(tmp_path: Path) -> None:
    """Verify that an artificially inflated benchmark log fails the budget check."""
    inflated_content = (
        "LATENCY BENCHMARK RESULTS\n"
        "End-to-End Latency      : P50=2500.00ms | P95=4200.00ms | Max=5000.00ms\n"
        "Service-Added Latency   : P50=  45.00ms | P95= 110.00ms | Max= 150.00ms\n"
        "Per-Unique-Utterance Medians (N=85 independent utterances):\n"
        "   E2E Latency: P50=2400.000ms | P95=4100.000ms "
        "[95% Bootstrap CI: 3900.00ms, 4300.00ms] | Max=4500.000ms\n"
    )
    inflated_file = tmp_path / "inflated_bench.log"
    inflated_file.write_text(inflated_content, encoding="utf-8")

    passed = check_benchmark_log(log_path=inflated_file, verbose=False)
    assert passed is False, "Inflated benchmark log should have failed budget check"


def test_budget_check_cli_invocation(tmp_path: Path) -> None:
    """Verify CLI prints resolved path, LastWriteTime, clock note, and enforces exit codes."""
    ok_file = tmp_path / "ok_bench.log"
    ok_file.write_text(_compliant_log_text(ci_upper_ms=1890.0), encoding="utf-8")
    ok_bakeoff = tmp_path / "bakeoff_ok.json"
    ok_bakeoff.write_text(
        json.dumps(
            {
                "summaries": {
                    "deepgram_nova3:default": {
                        "name": "Deepgram Nova-3 (default)",
                        "stable_text_p50_ms": 500.0,
                        "stable_text_p95_ms": 1400.0,
                        "stable_text_n": 150,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    cmd_pass = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "budget_check.py"),
        "--log",
        str(ok_file),
        "--bakeoff-json",
        str(ok_bakeoff),
    ]
    res_pass = subprocess.run(cmd_pass, capture_output=True, text=True)
    assert res_pass.returncode == 0, f"CLI expected 0, got {res_pass.returncode}: {res_pass.stdout}"
    assert "Log LastWriteTime" in res_pass.stdout
    assert "NOT T0-clock" in res_pass.stdout
    assert "Lane-0 Time-to-Stable-Text" in res_pass.stdout

    inflated_file = tmp_path / "inflated_bench.log"
    inflated_file.write_text(_compliant_log_text(ci_upper_ms=2217.01), encoding="utf-8")

    cmd_fail = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "budget_check.py"),
        "--log",
        str(inflated_file),
    ]
    res_fail = subprocess.run(cmd_fail, capture_output=True, text=True)
    assert res_fail.returncode != 0, f"CLI expected non-zero, got {res_fail.returncode}"
