"""MeetMind AI — Shared Event Schemas (Pydantic).

These Pydantic models mirror the Zod schemas defined in
``packages/shared-schemas/src/index.ts``. Any field additions or
type changes MUST be applied to both files simultaneously.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

# -- Tenant --------------------------------------------------------------------

TenantId = str
"""Unique tenant identifier. Non-empty string."""

ConsentType = Literal["audio_capture", "transcript_storage", "ai_analysis"]


# -- Audit Event ---------------------------------------------------------------


class AuditEventSchema(BaseModel):
    """Audit event contract shared across services.

    Mirrors ``AuditEventSchema`` in the TypeScript shared-schemas package.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

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

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    meeting_id: str = Field(..., min_length=1)
    consent_type: ConsentType = Field(
        ..., description="e.g. audio_capture, transcript_storage, ai_analysis"
    )
    granted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    revoked_at: datetime | None = None
