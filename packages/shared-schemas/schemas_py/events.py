"""MeetMind AI — Shared Event Schemas (Pydantic).

These Pydantic models mirror the Zod schemas defined in
``packages/shared-schemas/src/index.ts``. Any field additions or
type changes MUST be applied to both files simultaneously.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

# -- Tenant --------------------------------------------------------------------

TenantId = str
"""Unique tenant identifier. Non-empty string."""


# -- Audit Event ---------------------------------------------------------------


class AuditEventSchema(BaseModel):
    """Audit event contract shared across services.

    Mirrors ``AuditEventSchema`` in the TypeScript shared-schemas package.
    """

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# -- Consent Record -----------------------------------------------------------


class ConsentRecordSchema(BaseModel):
    """Consent record contract shared across services.

    Mirrors ``ConsentRecordSchema`` in the TypeScript shared-schemas package.
    """

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    meeting_id: str = Field(..., min_length=1)
    consent_type: str = Field(
        ..., description="e.g. audio_capture, transcript_storage, ai_analysis"
    )
    granted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    revoked_at: datetime | None = None
