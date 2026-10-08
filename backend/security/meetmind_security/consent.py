"""MeetMind AI - Append-Only Consent Record Service.

Hard invariant: NO audio frame may be processed without a prior logged
`consent_confirmed` event. This module enforces that invariant via an
APPEND-ONLY `consent_events` table:

1. `record_consent` - grants consent by inserting an event with action='granted'
2. `check_consent`  - checks if the latest event for (user, meeting, type) is 'granted'
3. `revoke_consent` - revokes consent by inserting an event with action='revoked'

Every consent state change produces an audit trail via the audit_log module.
In-database triggers forbid UPDATE and DELETE operations.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field, computed_field

from . import db
from .audit_log import write_audit_event
from .migrations import apply_migrations
from .tenant import TenantRouter

_router = TenantRouter()

ConsentAction = Literal["granted", "revoked"]


class ConsentEvent(BaseModel):
    """An immutable consent event record (granted or revoked)."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: str
    user_id: str
    meeting_id: str
    consent_type: str = "audio_capture"
    action: ConsentAction = "granted"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @computed_field
    @property
    def granted_at(self) -> datetime:
        return self.created_at

    @computed_field
    @property
    def revoked_at(self) -> datetime | None:
        return self.created_at if self.action == "revoked" else None


# Backward-compatible alias
ConsentRecord = ConsentEvent


async def init_consent_table(tenant_id: str = "default") -> None:
    """Ensure consent tables are initialized via the migration runner."""
    schema = _router.pg_schema(tenant_id)
    pool = await db.get_pool()
    await apply_migrations(pool, schema)


_INSERT_SQL = """
INSERT INTO consent_events (id, tenant_id, user_id, meeting_id, consent_type, action, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, created_at;
"""


async def record_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> ConsentEvent:
    """Grant consent by appending a new 'granted' event and writing an audit trail.

    Returns:
        The created ConsentEvent.
    """
    event = ConsentEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        meeting_id=meeting_id,
        consent_type=consent_type,
        action="granted",
    )

    row = await db.fetch_one(
        _INSERT_SQL,
        event.id,
        event.tenant_id,
        event.user_id,
        event.meeting_id,
        event.consent_type,
        event.action,
        event.created_at,
        tenant_id=tenant_id,
    )

    if row:
        event.id = row["id"]
        event.created_at = row["created_at"]

    # Audit trail
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="consent_confirmed",
        payload={
            "meeting_id": meeting_id,
            "consent_type": consent_type,
            "consent_event_id": str(event.id),
        },
    )

    return event


_CHECK_LATEST_SQL = """
SELECT action
FROM consent_events
WHERE tenant_id = $1
  AND user_id = $2
  AND meeting_id = $3
  AND consent_type = $4
ORDER BY created_at DESC, id DESC
LIMIT 1;
"""


async def check_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> bool:
    """Check whether active consent exists based on the latest event.

    Returns True only if the latest event has action='granted'.
    """
    row = await db.fetch_one(
        _CHECK_LATEST_SQL,
        tenant_id,
        user_id,
        meeting_id,
        consent_type,
        tenant_id=tenant_id,
    )
    return bool(row and row["action"] == "granted")


async def revoke_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> list[uuid.UUID]:
    """Revoke consent by appending a new 'revoked' event and writing an audit trail.

    Returns:
        List containing the ID of the newly created revocation event.
    """
    event = ConsentEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        meeting_id=meeting_id,
        consent_type=consent_type,
        action="revoked",
    )

    row = await db.fetch_one(
        _INSERT_SQL,
        event.id,
        event.tenant_id,
        event.user_id,
        event.meeting_id,
        event.consent_type,
        event.action,
        event.created_at,
        tenant_id=tenant_id,
    )

    revoked_id = row["id"] if row else event.id

    # Audit trail
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="consent_revoked",
        payload={
            "meeting_id": meeting_id,
            "consent_type": consent_type,
            "consent_event_id": str(revoked_id),
        },
    )

    return [revoked_id]
