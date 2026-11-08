"""MeetMind AI - Gateway audit event helpers.

Thin wrappers around the §15 audit log for gateway-specific events.
Every control-frame event (connect, consent, disconnect) is written to
the append-only audit log.

Policy:
- Connection and meeting_start audit writes are security-critical and MUST fail-closed:
  if the audit write fails, the error propagates and terminates the connection.
- Disconnection and meeting_end audit writes are retried up to 3 times. If they still
  fail, the error is logged, but will NEVER crash the cleanup routine.
"""

from __future__ import annotations

import asyncio
import logging

from meetmind_security.audit_log import write_audit_event

logger = logging.getLogger(__name__)


async def _retry_audit_write(
    *,
    tenant_id: str,
    user_id: str,
    event_type: str,
    payload: dict,
    max_retries: int = 3,
) -> None:
    """Retry audit write with exponential backoff; log error on final failure."""
    backoff = 0.1
    for attempt in range(1, max_retries + 1):
        try:
            await write_audit_event(
                tenant_id=tenant_id,
                user_id=user_id,
                event_type=event_type,
                payload=payload,
            )
            return
        except Exception as exc:
            if attempt == max_retries:
                logger.error(
                    "Failed to write %s audit event after %d attempts for user %s: %s",
                    event_type,
                    max_retries,
                    user_id,
                    exc,
                )
                return
            logger.warning(
                "Audit write %s failed (attempt %d/%d): %s. Retrying in %.2fs...",
                event_type,
                attempt,
                max_retries,
                exc,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff *= 2.0


async def audit_connection(
    *, tenant_id: str, user_id: str, meeting_id: str, adapter_type: str
) -> None:
    """Log a WebSocket connection event (fail-closed)."""
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="ws_connected",
        payload={
            "meeting_id": meeting_id,
            "adapter_type": adapter_type,
        },
    )


async def audit_disconnection(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    reason: str = "normal",
    frames_processed: int = 0,
) -> None:
    """Log a WebSocket disconnection event (retried, safe for cleanup)."""
    await _retry_audit_write(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="ws_disconnected",
        payload={
            "meeting_id": meeting_id,
            "reason": reason,
            "frames_processed": frames_processed,
        },
    )


async def audit_meeting_start(
    *, tenant_id: str, user_id: str, meeting_id: str, adapter_type: str
) -> None:
    """Log a meeting_start control event (fail-closed)."""
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="meeting_started",
        payload={
            "meeting_id": meeting_id,
            "adapter_type": adapter_type,
        },
    )


async def audit_meeting_end(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    total_chunks: int = 0,
) -> None:
    """Log a meeting_end control event (retried, safe for cleanup)."""
    await _retry_audit_write(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="meeting_ended",
        payload={
            "meeting_id": meeting_id,
            "total_chunks": total_chunks,
        },
    )


async def audit_clock_sync_established(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    clock_offset_ms: int,
    rtt_ms: int,
    samples: int = 3,
) -> None:
    """Log initial clock synchronization establishment (fail-closed, Q1)."""
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="clock_sync_established",
        payload={
            "meeting_id": meeting_id,
            "clock_offset_ms": clock_offset_ms,
            "rtt_ms": rtt_ms,
            "samples": samples,
        },
    )


async def audit_clock_skew_detected(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    previous_offset_ms: int,
    new_offset_ms: int,
    delta_ms: int,
    rtt_ms: int,
) -> None:
    """Log clock skew shift > 500ms warning (retried, Q1)."""
    await _retry_audit_write(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="clock_skew_detected",
        payload={
            "meeting_id": meeting_id,
            "previous_offset_ms": previous_offset_ms,
            "new_offset_ms": new_offset_ms,
            "delta_ms": delta_ms,
            "rtt_ms": rtt_ms,
        },
    )


async def audit_clock_clamped(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    raw_capture_server_ms: int,
    clamped_capture_server_ms: int,
    gw_recv_ms: int,
    max_age_ms: int,
    reason: str,
) -> None:
    """Log clock clamping event (retried, R6)."""
    await _retry_audit_write(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="clock_clamped",
        payload={
            "meeting_id": meeting_id,
            "raw_capture_server_ms": raw_capture_server_ms,
            "clamped_capture_server_ms": clamped_capture_server_ms,
            "gw_recv_ms": gw_recv_ms,
            "max_age_ms": max_age_ms,
            "reason": reason,
        },
    )
