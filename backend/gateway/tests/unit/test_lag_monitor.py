"""Unit tests for Event-Loop Lag Monitor and percentile calculation (P0-4)."""

from __future__ import annotations

import asyncio

import pytest
from meetmind_gateway.lag_monitor import EventLoopLagMonitor, compute_percentiles


def test_compute_percentiles_empty() -> None:
    res = compute_percentiles([])
    assert res == {"min": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


def test_compute_percentiles_single_value() -> None:
    res = compute_percentiles([42.5])
    assert res["min"] == 42.5
    assert res["p50"] == 42.5
    assert res["p95"] == 42.5
    assert res["p99"] == 42.5
    assert res["max"] == 42.5


def test_compute_percentiles_known_distribution() -> None:
    # 1 to 100
    values = [float(x) for x in range(1, 101)]
    res = compute_percentiles(values)
    assert res["min"] == 1.0
    assert res["max"] == 100.0
    assert 49.0 <= res["p50"] <= 51.0
    assert 94.0 <= res["p95"] <= 96.0
    assert 98.0 <= res["p99"] <= 100.0


@pytest.mark.asyncio
async def test_lag_monitor_setup_and_steady_phases() -> None:
    monitor = EventLoopLagMonitor(interval_s=0.005)
    assert not monitor.is_steady

    monitor.start()
    await asyncio.sleep(0.030)  # should collect some setup lags
    assert len(monitor.setup_lags_ms) >= 1
    assert len(monitor.steady_lags_ms) == 0

    monitor.mark_steady()
    assert monitor.is_steady
    await asyncio.sleep(0.030)  # should collect some steady lags
    assert len(monitor.steady_lags_ms) >= 1

    monitor.stop()

    stats = monitor.get_stats()
    assert stats["setup"]["samples"] >= 1
    assert stats["steady"]["samples"] >= 1
    assert stats["setup"]["p50"] >= 0.0
    assert stats["steady"]["p50"] >= 0.0
