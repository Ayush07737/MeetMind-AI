"""MeetMind AI — ASR Health & Metrics Endpoint (WP-D, WP-E).

Exposes lightweight HTTP /healthz endpoint on configurable port.
Supports /healthz?verbose=1 for real-time observability metrics.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class AsrMetrics:
    """In-process observability metrics."""

    chunks_in: int = 0
    chunks_acked: int = 0
    replayed_chunks: int = 0
    dropped_chunks: int = 0
    reconnects: int = 0
    degraded_seconds: float = 0.0
    publish_latencies_ms: list[float] = field(default_factory=list)
    service_added_latencies_ms: list[float] = field(default_factory=list)
    loop_lag_ms: float = 0.0
    max_loop_lag_ms: float = 0.0
    gc_pauses_ms: list[float] = field(default_factory=list)
    reconnect_durations_ms: list[float] = field(default_factory=list)
    started_via: str = ""
    started_via_counts: dict[str, int] = field(
        default_factory=lambda: {"lifecycle": 0, "reconciler": 0}
    )

    def record_reconnect_duration(self, duration_ms: float) -> None:
        self.reconnect_durations_ms.append(duration_ms)
        if len(self.reconnect_durations_ms) > 1000:
            self.reconnect_durations_ms.pop(0)

    def record_started_via(self, meeting_id: str, source: str) -> None:
        self.started_via = source
        self.started_via_counts[source] = self.started_via_counts.get(source, 0) + 1

    def record_publish_latency(self, latency_ms: float) -> None:
        self.publish_latencies_ms.append(latency_ms)
        if len(self.publish_latencies_ms) > 1000:
            self.publish_latencies_ms.pop(0)

    def record_service_added_latency(self, latency_ms: float) -> None:
        self.service_added_latencies_ms.append(latency_ms)
        if len(self.service_added_latencies_ms) > 1000:
            self.service_added_latencies_ms.pop(0)

    def record_gc_pause(self, pause_ms: float) -> None:
        self.gc_pauses_ms.append(pause_ms)
        if len(self.gc_pauses_ms) > 1000:
            self.gc_pauses_ms.pop(0)

    def _calc_percentiles(self, vals: list[float]) -> dict[str, float]:
        if not vals:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        s = sorted(vals)
        n = len(s)

        def p(pct: float) -> float:
            idx = int(n * (pct / 100.0))
            return s[min(idx, n - 1)]

        return {
            "p50": round(p(50), 2),
            "p95": round(p(95), 2),
            "p99": round(p(99), 2),
            "max": round(max(s), 2),
        }

    def get_latency_percentiles(self) -> dict[str, float]:
        return self._calc_percentiles(self.publish_latencies_ms)

    def get_service_added_percentiles(self) -> dict[str, float]:
        return self._calc_percentiles(self.service_added_latencies_ms)


def init_gc_monitor(metrics: AsrMetrics) -> None:
    """Instrument Python garbage collector callbacks to measure stop-the-world pauses."""
    _gc_start = 0.0

    def _cb(phase: str, info: dict[str, Any]) -> None:
        nonlocal _gc_start
        if phase == "start":
            _gc_start = time.perf_counter()
        elif phase == "stop":
            pause_ms = (time.perf_counter() - _gc_start) * 1000.0
            metrics.record_gc_pause(pause_ms)

    gc.callbacks.append(_cb)


class HealthServer:
    """Asyncio HTTP health server."""

    def __init__(
        self,
        port: int,
        metrics: AsrMetrics,
        redis_client: Any,
        host: str = "127.0.0.1",
        service: Any = None,
    ) -> None:
        self.port = port
        self.host = host
        self.metrics = metrics
        self._redis = redis_client
        self.service = service
        self._server: asyncio.Server | None = None
        self._lag_task: asyncio.Task[None] | None = None

    async def _lag_monitor_loop(self) -> None:
        interval = 0.05
        while True:
            try:
                t0 = time.perf_counter()
                await asyncio.sleep(interval)
                lag = max(0.0, (time.perf_counter() - t0 - interval) * 1000.0)
                self.metrics.loop_lag_ms = round(lag, 2)
                if lag > self.metrics.max_loop_lag_ms:
                    self.metrics.max_loop_lag_ms = round(lag, 2)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._lag_task = asyncio.create_task(self._lag_monitor_loop())
        logger.info("ASR Health endpoint listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self._lag_task:
            self._lag_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                return

            req_line = line.decode("utf-8").strip()
            parts = req_line.split()
            if len(parts) >= 2 and parts[0] == "GET":
                path = parts[1]
                if path.startswith("/healthz"):
                    redis_ok = False
                    try:
                        await self._redis.ping()
                        redis_ok = True
                    except Exception:
                        pass

                    status_code = 200 if redis_ok else 503
                    resp_data: dict[str, Any] = {
                        "status": "healthy" if redis_ok else "unhealthy",
                        "redis": "connected" if redis_ok else "disconnected",
                    }

                    if "verbose=1" in path or "verbose=true" in path:
                        gc_pauses = self.metrics.gc_pauses_ms
                        resp_data["metrics"] = {
                            "chunks_in": self.metrics.chunks_in,
                            "chunks_acked": self.metrics.chunks_acked,
                            "replayed_chunks": self.metrics.replayed_chunks,
                            "dropped_chunks": self.metrics.dropped_chunks,
                            "reconnects": self.metrics.reconnects,
                            "reconnect_durations_ms": [
                                round(x, 2) for x in self.metrics.reconnect_durations_ms
                            ],
                            "degraded_seconds": round(self.metrics.degraded_seconds, 2),
                            "publish_latencies": self.metrics.get_latency_percentiles(),
                            "service_added_latencies": self.metrics.get_service_added_percentiles(),
                            "loop_lag_ms": self.metrics.loop_lag_ms,
                            "max_loop_lag_ms": self.metrics.max_loop_lag_ms,
                            "gc": {
                                "count": len(gc_pauses),
                                "max_ms": round(max(gc_pauses), 2) if gc_pauses else 0.0,
                                "total_ms": round(sum(gc_pauses), 2),
                            },
                            "started_via": self.metrics.started_via,
                            "started_via_counts": self.metrics.started_via_counts,
                        }

                        if self.service and hasattr(self.service, "_active_pipelines"):
                            p_info = {}
                            for m_id, pipe in self.service._active_pipelines.items():
                                p_info[m_id] = {
                                    "reconnect_buffer_depth": len(pipe._reconnect_buffer),
                                    "pending_acks_depth": len(pipe._pending_acks),
                                    "dedupe_lru_size": len(pipe._dedupe._seen_utterance_ids),
                                }
                            resp_data["metrics"]["pipelines"] = p_info

                    body = json.dumps(resp_data).encode("utf-8")
                    status_text = "OK" if status_code == 200 else "Service Unavailable"
                    headers = (
                        f"HTTP/1.1 {status_code} {status_text}\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        f"Connection: close\r\n\r\n"
                    )
                    writer.write(headers.encode("utf-8") + body)
                    await writer.drain()
                else:
                    writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                    await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
