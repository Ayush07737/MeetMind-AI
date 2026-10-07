"""MeetMind AI - Gateway audit event helpers.

Thin wrappers around the §15 audit log for gateway-specific events.
Every control-frame event (connect, consent, disconnect) is written to
the append-only audit log.
"""

from __future__ import annotations

from meetmind_security.audit_log import write_audit_event


async def audit_connection(
    *, tenant_id: str, user_id: str, meeting_id: str, adapter_type: str
) -> None:
    """Log a WebSocket connection event."""
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
    """Log a WebSocket disconnection event."""
    await write_audit_event(
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
    """Log a meeting_start control event."""
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
    """Log a meeting_end control event."""
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="meeting_ended",
        payload={
            "meeting_id": meeting_id,
            "total_chunks": total_chunks,
        },
    )
