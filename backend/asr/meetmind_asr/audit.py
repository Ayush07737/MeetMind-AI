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

    async def stop(self, timeout: float = 15.0) -> None:
        """Drain queue within bounded timeout and stop background worker task."""
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
        except TimeoutError:
            logger.warning(
                "Audit queue drain timed out after %.1fs; %d items remaining",
                timeout,
                self._queue.qsize(),
            )

        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
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

    async def record_session_summary_sync(
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
        gaps: list[dict[str, Any]] | None = None,
        timeout: float = 15.0,
        retries: int = 3,
    ) -> bool:
        """Write asr_session_summary audit event synchronously with timeout and retries."""
        payload: dict[str, Any] = {
            "meeting_id": meeting_id,
            "provider": provider,
            "chunks_sent": chunks_sent,
            "bytes_sent": bytes_sent,
            "reconnects": reconnects,
            "dropped_chunks": dropped_chunks,
            "degraded_seconds": round(degraded_seconds, 2),
            "gaps": gaps or [],
        }

        async def _write() -> bool:
            last_exc: Exception | None = None
            delay = 0.5
            for attempt in range(1, retries + 1):
                try:
                    await audit.write_audit_event(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        event_type="asr_session_summary",
                        payload=payload,
                    )
                    self.successful_audit_count += 1
                    logger.info(
                        "Synchronously recorded asr_session_summary for meeting %s (attempt %d/%d)",
                        meeting_id,
                        attempt,
                        retries,
                    )
                    return True
                except Exception as exc:
                    last_exc = exc
                    self.failed_audit_count += 1
                    logger.warning(
                        "Attempt %d/%d failed writing asr_session_summary for %s: %s",
                        attempt,
                        retries,
                        meeting_id,
                        exc,
                    )
                    if attempt < retries:
                        await asyncio.sleep(delay)
                        delay *= 2
            logger.error(
                "All %d attempts failed writing asr_session_summary for %s: %s",
                retries,
                meeting_id,
                last_exc,
            )
            return False

        try:
            return await asyncio.wait_for(_write(), timeout=timeout)
        except TimeoutError:
            self.failed_audit_count += 1
            logger.error(
                "Timed out after %.1fs writing synchronous asr_session_summary for %s",
                timeout,
                meeting_id,
            )
            return False

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
        gaps: list[dict[str, Any]] | None = None,
    ) -> None:
        """Enqueue asr_session_summary audit event (non-blocking fallback)."""
        payload: dict[str, Any] = {
            "meeting_id": meeting_id,
            "provider": provider,
            "chunks_sent": chunks_sent,
            "bytes_sent": bytes_sent,
            "reconnects": reconnects,
            "dropped_chunks": dropped_chunks,
            "degraded_seconds": round(degraded_seconds, 2),
            "gaps": gaps or [],
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
