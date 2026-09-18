"""MeetMind AI — Consent Record Service.

Hard invariant: NO audio frame may be processed without a prior logged
`consent_confirmed` event. This module enforces that invariant by providing:

1. `record_consent` — grants consent and writes an audit event
2. `check_consent`  — returns whether active (un-revoked) consent exists
3. `revoke_consent` — revokes consent and writes an audit event

Every consent state change produces an audit trail via the audit_log module.

Table schema::

    CREATE TABLE IF NOT EXISTS consent_records (
        id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id     TEXT NOT NULL,
        user_id       TEXT NOT NULL,
        meeting_id    TEXT NOT NULL,
        consent_type  TEXT NOT NULL,
        granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        revoked_at    TIMESTAMPTZ
    );
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from . import db
from .audit_log import write_audit_event


# ── Pydantic model ──────────────────────────────────────────────────────────


class ConsentRecord(BaseModel):
    """A consent grant/revocation record."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: str
    user_id: str
    meeting_id: str
    consent_type: str  # e.g. "audio_capture", "transcript_storage", "ai_analysis"
    granted_at: datetime = Field(default_factory=datetime.utcnow)
    revoked_at: datetime | None = None


# ── Table initialization ───────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS consent_records (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    meeting_id    TEXT NOT NULL,
    consent_type  TEXT NOT NULL,
    granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_consent_records_lookup
    ON consent_records (tenant_id, user_id, meeting_id, consent_type)
    WHERE revoked_at IS NULL;
"""


async def init_consent_table(tenant_id: str = "default") -> None:
    """Create the consent_records table if it does not exist.

    Called once during service startup — idempotent.
    """
    await db.execute_raw(_CREATE_TABLE_SQL, tenant_id=tenant_id)


# ── Grant consent ──────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO consent_records (id, tenant_id, user_id, meeting_id, consent_type, granted_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, granted_at;
"""


async def record_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> ConsentRecord:
    """Grant consent and log an audit event.

    Returns:
        The created ConsentRecord.
    """
    record = ConsentRecord(
        tenant_id=tenant_id,
        user_id=user_id,
        meeting_id=meeting_id,
        consent_type=consent_type,
    )

    row = await db.fetch_one(
        _INSERT_SQL,
        record.id,
        record.tenant_id,
        record.user_id,
        record.meeting_id,
        record.consent_type,
        record.granted_at,
        tenant_id=tenant_id,
    )

    if row:
        record.id = row["id"]
        record.granted_at = row["granted_at"]

    # Audit trail
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="consent_confirmed",
        payload={
            "meeting_id": meeting_id,
            "consent_type": consent_type,
            "consent_record_id": str(record.id),
        },
    )

    return record


# ── Check consent ──────────────────────────────────────────────────────────

_CHECK_SQL = """
SELECT EXISTS (
    SELECT 1 FROM consent_records
    WHERE tenant_id = $1
      AND user_id = $2
      AND meeting_id = $3
      AND consent_type = $4
      AND revoked_at IS NULL
) AS has_consent;
"""


async def check_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> bool:
    """Check whether active (un-revoked) consent exists.

    This is the gate that §1's gateway checks before processing any audio frame.
    """
    row = await db.fetch_one(
        _CHECK_SQL,
        tenant_id,
        user_id,
        meeting_id,
        consent_type,
        tenant_id=tenant_id,
    )
    return bool(row and row["has_consent"])


# ── Revoke consent ─────────────────────────────────────────────────────────

_REVOKE_SQL = """
UPDATE consent_records
SET revoked_at = NOW()
WHERE tenant_id = $1
  AND user_id = $2
  AND meeting_id = $3
  AND consent_type = $4
  AND revoked_at IS NULL
RETURNING id;
"""


async def revoke_consent(
    *,
    tenant_id: str,
    user_id: str,
    meeting_id: str,
    consent_type: str = "audio_capture",
) -> list[uuid.UUID]:
    """Revoke all active consent records matching the criteria.

    Returns:
        List of revoked consent record IDs.
    """
    rows = await db.fetch_all(
        _REVOKE_SQL,
        tenant_id,
        user_id,
        meeting_id,
        consent_type,
        tenant_id=tenant_id,
    )

    revoked_ids = [row["id"] for row in rows]

    # Audit trail
    await write_audit_event(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="consent_revoked",
        payload={
            "meeting_id": meeting_id,
            "consent_type": consent_type,
            "revoked_record_ids": [str(rid) for rid in revoked_ids],
        },
    )

    return revoked_ids
