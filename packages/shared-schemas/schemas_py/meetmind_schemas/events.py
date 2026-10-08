"""MeetMind AI - Pydantic models mirroring shared Zod schemas.

These models MUST stay in sync with ``packages/shared-schemas/src/index.ts``. Any
type changes MUST be applied to both files simultaneously.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import IntEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

# -- Tenant --------------------------------------------------------------------

TenantId = str
"""Unique tenant identifier. Non-empty string."""

ConsentType = Literal["audio_capture", "transcript_storage", "ai_analysis"]
ConsentAction = Literal["granted", "revoked"]


# -- Audit Event ---------------------------------------------------------------


class AuditEventSchema(BaseModel):
    """Immutable audit event contract.

    Mirrors ``AuditEventSchema`` in the TypeScript shared-schemas package.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# -- Consent Event (Append-Only) -----------------------------------------------


class ConsentEventSchema(BaseModel):
    """Append-only consent event contract.

    Mirrors ``ConsentEventSchema`` in the TypeScript shared-schemas package.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    meeting_id: str = Field(..., min_length=1)
    consent_type: ConsentType = Field(
        ..., description="e.g. audio_capture, transcript_storage, ai_analysis"
    )
    action: ConsentAction = Field(default="granted")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ConsentRecordSchema(BaseModel):
    """Backward-compatible consent record schema.

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


class WSCloseCode(IntEnum):
    NORMAL_CLOSURE = 1000
    INTERNAL_ERROR = 1011
    PROTOCOL_VIOLATION = 4400
    UNAUTHORIZED = 4401
    FORBIDDEN = 4403
    IDLE_TIMEOUT = 4408
    WRITER_CONFLICT = 4409
    RATE_LIMITED = 4429
