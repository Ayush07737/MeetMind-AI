"""MeetMind AI — ASR Audit Logger (WP-E).

Emits `asr_session_started` and `asr_session_summary` events to Neon audit_events
via meetmind_security using the app role.

Security Invariants:
- Asynchronous worker with bounded queue and retries.
- NOT on the hot path: ASR transcription continues even if Neon/Postgres is unreachable.
- NEVER includes raw audio bytes or transcript text in audit payloads.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any

from meetmind_security import audit_log as audit

logger = logging.getLogger(__name__)


@dataclass
class AuditRecord:
    """Queued audit item for background submission."""

    tenant_id: str
    user_id: str
    event_type: str
    payload: dict[str, Any]
    retries: int = 0


class AsrAuditor:
    """Non-blocking background audit queue and dispatcher."""

    def __init__(self, max_queue_size: int = 500) -> None:
        self._queue: asyncio.Queue[AuditRecord] = asyncio.Queue(maxsize=max_queue_size)
        self._worker_task: asyncio.Task[None] | None = None
        self._is_running = False
        self.failed_audit_count = 0
        self.successful_audit_count = 0

    def start(self) -> None:
        """Start the background audit worker task."""
        if not self._is_running:
            self._is_running = True
            self._worker_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        """Drain queue and stop background worker task."""
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            with asyncio.suppress(asyncio.CancelledError):
                await self._worker_task

    def record_session_started(
        self,
        tenant_id: str,
        user_id: str,
        meeting_id: str,
        provider: str,
        endpoint_host: str,
        language: str,
        mip_opt_out: bool,
    ) -> None:
        """Enqueue asr_session_started audit event (non-blocking)."""
        payload = {
            "meeting_id": meeting_id,
            "provider": provider,
            "endpoint_host": endpoint_host,
            "language": language,
            "mip_opt_out": mip_opt_out,
        }
        self._enqueue(tenant_id, user_id, "asr_session_started", payload)

    def record_session_summary(
        self,
        tenant_id: str,
        user_id: str,
        meeting_id: str,
        provider: str,
        chunks_sent: int,
        bytes_sent: int,
        reconnects: int,
        dropped_chunks: int,
        degraded_seconds: float,
    ) -> None:
        """Enqueue asr_session_summary audit event (non-blocking)."""
        payload = {
            "meeting_id": meeting_id,
            "provider": provider,
            "chunks_sent": chunks_sent,
            "bytes_sent": bytes_sent,
            "reconnects": reconnects,
            "dropped_chunks": dropped_chunks,
            "degraded_seconds": round(degraded_seconds, 2),
        }
        self._enqueue(tenant_id, user_id, "asr_session_summary", payload)

    def _enqueue(
        self,
        tenant_id: str,
        user_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        rec = AuditRecord(
            tenant_id=tenant_id,
            user_id=user_id,
            event_type=event_type,
            payload=payload,
        )
        try:
            self._queue.put_nowait(rec)
        except asyncio.QueueFull:
            self.failed_audit_count += 1
            logger.warning("Audit queue full; dropped audit event %s", event_type)

    async def _process_queue(self) -> None:
        while self._is_running:
            try:
                rec = await self._queue.get()
                try:
                    await audit.write_audit_event(
                        tenant_id=rec.tenant_id,
                        user_id=rec.user_id,
                        event_type=rec.event_type,
                        payload=rec.payload,
                    )
                    self.successful_audit_count += 1
                except Exception as exc:
                    self.failed_audit_count += 1
                    logger.warning("Failed writing audit event %s: %s", rec.event_type, exc)
                    if rec.retries < 2:
                        rec.retries += 1
                        with contextlib.suppress(asyncio.QueueFull):
                            self._queue.put_nowait(rec)
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Unexpected error in audit processor: %s", e)
