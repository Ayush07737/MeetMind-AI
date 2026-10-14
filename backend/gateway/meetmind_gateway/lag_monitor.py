"""MeetMind AI - Event-Loop Lag Monitor.

Measures event-loop scheduling latency (actual vs scheduled wake-up)
distinguishing between setup phase and steady-state streaming phase.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


def compute_percentiles(values: list[float]) -> dict[str, float]:
    """Compute min, p50, p95, p99, max percentiles from a list of floats."""
    if not values:
        return {"min": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    s = sorted(values)
    n = len(s)

    def _perc(p: float) -> float:
        idx = int(round((p / 100.0) * (n - 1)))
        return round(s[idx], 2)

    return {
        "min": round(s[0], 2),
        "p50": _perc(50),
        "p95": _perc(95),
        "p99": _perc(99),
        "max": round(s[-1], 2),
    }


class EventLoopLagMonitor:
    """Monitors asyncio event loop scheduling latency every 20ms."""

    def __init__(self, interval_s: float = 0.020) -> None:
        self.interval_s = interval_s
        self.setup_lags_ms: list[float] = []
        self.steady_lags_ms: list[float] = []
        self._is_steady = False
        self._running = False
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        # Enable 50ms slow-callback logging if debug is active or requested
        if (
            os.environ.get("PYTHONASYNCIODEBUG") in ("1", "true", "True")
            or os.environ.get("APP_ENV") == "test"
        ):
            loop.slow_callback_duration = 0.050
            logger.info("Event loop slow callback duration set to 50ms")
        self._task = asyncio.create_task(self._monitor_loop())

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    def mark_steady(self) -> None:
        self._is_steady = True

    @property
    def is_steady(self) -> bool:
        return self._is_steady

    async def _monitor_loop(self) -> None:
        while self._running:
            target = time.perf_counter() + self.interval_s
            try:
                await asyncio.sleep(self.interval_s)
            except asyncio.CancelledError:
                break
            except Exception:
                break
            actual = time.perf_counter()
            lag_ms = max(0.0, (actual - target) * 1000.0)
            if self._is_steady:
                self.steady_lags_ms.append(lag_ms)
            else:
                self.setup_lags_ms.append(lag_ms)

    def get_stats(self) -> dict[str, Any]:
        return {
            "setup": {
                "samples": len(self.setup_lags_ms),
                **compute_percentiles(self.setup_lags_ms),
            },
            "steady": {
                "samples": len(self.steady_lags_ms),
                **compute_percentiles(self.steady_lags_ms),
            },
        }
