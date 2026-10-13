"""MeetMind AI — ASR Health & Metrics Endpoint (WP-D, WP-E).

Exposes lightweight HTTP /healthz endpoint on configurable port.
Supports /healthz?verbose=1 for real-time observability metrics.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
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

    def record_publish_latency(self, latency_ms: float) -> None:
        self.publish_latencies_ms.append(latency_ms)
        if len(self.publish_latencies_ms) > 1000:
            self.publish_latencies_ms.pop(0)

    def get_latency_percentiles(self) -> dict[str, float]:
        if not self.publish_latencies_ms:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        s = sorted(self.publish_latencies_ms)
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


class HealthServer:
    """Asyncio HTTP health server."""

    def __init__(
        self,
        port: int,
        metrics: AsrMetrics,
        redis_client: Any,
        host: str = "127.0.0.1",
    ) -> None:
        self.port = port
        self.host = host
        self.metrics = metrics
        self._redis = redis_client
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info("ASR Health endpoint listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
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
                        resp_data["metrics"] = {
                            "chunks_in": self.metrics.chunks_in,
                            "chunks_acked": self.metrics.chunks_acked,
                            "replayed_chunks": self.metrics.replayed_chunks,
                            "dropped_chunks": self.metrics.dropped_chunks,
                            "reconnects": self.metrics.reconnects,
                            "degraded_seconds": round(self.metrics.degraded_seconds, 2),
                            "publish_latencies": self.metrics.get_latency_percentiles(),
                        }

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
