"""MeetMind AI — Append-Only Audit Log.

The audit_events table is APPEND-ONLY by design:
- Only INSERT is exposed via `write_audit_event`.
- No UPDATE or DELETE function exists in this module.
- The table is created with no foreign keys to avoid coupling.

This is a hard security invariant: every state-changing operation in MeetMind AI
must produce an audit event, and no audit event can ever be modified or deleted
through the application layer.

Table schema::

    CREATE TABLE IF NOT EXISTS audit_events (
        id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id     TEXT NOT NULL,
        user_id       TEXT NOT NULL,
        event_type    TEXT NOT NULL,
        payload       JSONB NOT NULL DEFAULT '{}',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from . import db

# ── Pydantic model ──────────────────────────────────────────────────────────


class AuditEvent(BaseModel):
    """Immutable audit event record."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: str
    user_id: str
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ── Table initialization ───────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant
    ON audit_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_user
    ON audit_events (tenant_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_type
    ON audit_events (event_type, created_at DESC);
"""


async def init_audit_table(tenant_id: str = "default") -> None:
    """Create the audit_events table if it does not exist.

    Called once during service startup — idempotent.
    """
    await db.execute_raw(_CREATE_TABLE_SQL, tenant_id=tenant_id)


# ── Write (INSERT only — no UPDATE, no DELETE) ─────────────────────────────

_INSERT_SQL = """
INSERT INTO audit_events (id, tenant_id, user_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
RETURNING id, created_at;
"""


async def write_audit_event(
    *,
    tenant_id: str,
    user_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> AuditEvent:
    """Insert an audit event. This is the ONLY write operation exposed.

    Returns:
        The created AuditEvent with server-assigned id and timestamp.
    """
    event = AuditEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type=event_type,
        payload=payload or {},
    )

    row = await db.fetch_one(
        _INSERT_SQL,
        event.id,
        event.tenant_id,
        event.user_id,
        event.event_type,
        json.dumps(event.payload),
        event.created_at,
        tenant_id=tenant_id,
    )

    if row:
        event.id = row["id"]
        event.created_at = row["created_at"]

    return event


# ── Read (for verification / compliance queries) ───────────────────────────

_QUERY_BY_TYPE_SQL = """
SELECT id, tenant_id, user_id, event_type, payload, created_at
FROM audit_events
WHERE tenant_id = $1 AND event_type = $2
ORDER BY created_at DESC
LIMIT $3;
"""


async def query_events_by_type(
    tenant_id: str,
    event_type: str,
    limit: int = 100,
) -> list[AuditEvent]:
    """Query audit events by type (for compliance dashboards)."""
    rows = await db.fetch_all(
        _QUERY_BY_TYPE_SQL,
        tenant_id,
        event_type,
        limit,
        tenant_id=tenant_id,
    )
    return [
        AuditEvent(
            id=row["id"],
            tenant_id=row["tenant_id"],
            user_id=row["user_id"],
            event_type=row["event_type"],
            payload=(
                row["payload"]
                if isinstance(row["payload"], dict)
                else json.loads(row["payload"])
            ),
            created_at=row["created_at"],
        )
        for row in rows
    ]
