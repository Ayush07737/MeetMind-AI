"""MeetMind AI - Pydantic models mirroring shared Zod schemas.

These models MUST stay in sync with ``packages/shared-schemas/src/index.ts``. Any
type changes MUST be applied to both files simultaneously.
"""

from __future__ import annotations

import struct
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel

# -- Tenant --------------------------------------------------------------------

TenantId = str
"""Unique tenant identifier. Non-empty string."""

ConsentType = Literal["audio_capture", "transcript_storage", "ai_analysis"]
ConsentAction = Literal["granted", "revoked"]


# -- Enums & Constants ---------------------------------------------------------


class WSCloseCode(IntEnum):
    """RFC 6455 and custom application WebSocket close codes."""

    NORMAL_CLOSURE = 1000
    INTERNAL_ERROR = 1011
    PROTOCOL_VIOLATION = 4400
    UNAUTHORIZED = 4401
    FORBIDDEN = 4403
    IDLE_TIMEOUT = 4408
    WRITER_CONFLICT = 4409
    RATE_LIMITED = 4429


class ErrorCode(StrEnum):
    """Error codes sent to clients in ErrorFrame."""

    CONSENT_REQUIRED = "CONSENT_REQUIRED"
    AUTH_FAILED = "AUTH_FAILED"
    INVALID_FRAME = "INVALID_FRAME"
    UNKNOWN_FRAME_TYPE = "UNKNOWN_FRAME_TYPE"
    MEETING_ENDED = "MEETING_ENDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    EVENTS_UNAVAILABLE = "EVENTS_UNAVAILABLE"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"


class AdapterType(StrEnum):
    """Source adapter that originated the connection."""

    CHROME_EXTENSION = "chrome_extension"
    DESKTOP_APP = "desktop_app"
    MEETING_BOT = "meeting_bot"


class ControlAction(StrEnum):
    """Actions supported by control frames."""

    MEETING_START = "meeting_start"
    MEETING_END = "meeting_end"
    CONSENT_CONFIRMED = "consent_confirmed"


class MeetingStatus(StrEnum):
    """Meeting lifecycle states."""

    PENDING_CONSENT = "pending_consent"
    ACTIVE = "active"
    ENDED = "ended"


OUTBOUND_EVENT_TYPES: tuple[str, ...] = (
    "transcript_chunk",
    "intelligence_event",
    "action_required",
    "summary_update",
)


# -- Audit Event ---------------------------------------------------------------


class AuditEventSchema(BaseModel):
    """Immutable audit event contract."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# -- Consent Event (Append-Only) -----------------------------------------------


class ConsentEventSchema(BaseModel):
    """Append-only consent event contract."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    meeting_id: str = Field(..., min_length=1)
    consent_type: ConsentType = Field(
        ..., description="e.g. audio_capture, transcript_storage, ai_analysis"
    )
    action: ConsentAction = Field(default="granted")
    external_participants: bool = False
    jurisdiction_hint: str | None = None
    consent_text_version: str = "v1.0"
    client_version: str = "1.0.0"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ConsentRecordSchema(BaseModel):
    """Backward-compatible consent record schema."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    tenant_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    meeting_id: str = Field(..., min_length=1)
    consent_type: ConsentType = Field(
        ..., description="e.g. audio_capture, transcript_storage, ai_analysis"
    )
    granted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    revoked_at: datetime | None = None


# -- Client -> Server Frames --------------------------------------------------


class ControlFrame(BaseModel):
    """Control frame for meeting lifecycle events with strict field validation."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["control"] = "control"
    action: ControlAction
    adapter_type: AdapterType | None = None
    consent_type: str | None = None

    # Fields required specifically when action == consent_confirmed
    external_participants: bool | None = None
    jurisdiction_hint: str | None = None
    consent_text_version: str | None = None
    client_version: str | None = None

    @model_validator(mode="after")
    def validate_action_fields(self) -> Self:
        if self.action == ControlAction.CONSENT_CONFIRMED:
            if self.external_participants is None:
                raise ValueError("external_participants is required for consent_confirmed")
            if not self.consent_text_version or not self.consent_text_version.strip():
                raise ValueError("consent_text_version is required for consent_confirmed")
            if not self.client_version or not self.client_version.strip():
                raise ValueError("client_version is required for consent_confirmed")
        else:
            # For meeting_start and meeting_end, consent fields should not be populated
            if (
                self.external_participants is not None
                or self.consent_text_version is not None
                or self.client_version is not None
            ):
                raise ValueError(f"Consent fields not allowed for action '{self.action}'")
        return self


class TranscriptChunkFrame(BaseModel):
    """Pre-transcribed text from an adapter doing local ASR."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["transcript_chunk"] = "transcript_chunk"
    text: str = Field(..., min_length=1)
    client_seq: int | None = None
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


# Discriminated union for client JSON frames
ClientFrame = Annotated[
    ControlFrame | TranscriptChunkFrame,
    Field(discriminator="type"),
]


# -- Server -> Client Frames --------------------------------------------------


class IntelligenceEventFrame(BaseModel):
    """Intelligence event from downstream services (§6/§7)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["intelligence_event"] = "intelligence_event"
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    meeting_id: str = ""
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


class AckFrame(BaseModel):
    """Acknowledgement of a received frame."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["ack"] = "ack"
    seq: int
    frame_type: str | None = None
    status: str = "acked"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ErrorFrame(BaseModel):
    """Error frame sent to the client."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["error"] = "error"
    code: ErrorCode
    message: str


# -- Binary Audio Frame Codec (WP5) -------------------------------------------

BINARY_HEADER_FORMAT = ">BBQQ"
BINARY_HEADER_SIZE = struct.calcsize(BINARY_HEADER_FORMAT)  # 18 bytes


@dataclass(frozen=True)
class AudioHeader:
    """Header prepended to all binary audio chunks."""

    version: int
    flags: int
    client_seq: int
    capture_ts_ms: int


def encode_audio_frame(
    client_seq: int,
    capture_ts_ms: int,
    payload: bytes,
    flags: int = 0,
    version: int = 1,
) -> bytes:
    """Encode an 18-byte big-endian header followed by binary audio payload."""
    header = struct.pack(BINARY_HEADER_FORMAT, version, flags, client_seq, capture_ts_ms)
    return header + payload


def decode_audio_frame(data: bytes) -> tuple[AudioHeader, bytes]:
    """Decode an 18-byte big-endian header and extract the payload."""
    if len(data) < BINARY_HEADER_SIZE:
        raise ValueError(
            f"Binary audio frame too short: {len(data)} bytes, expected {BINARY_HEADER_SIZE}"
        )
    version, flags, client_seq, capture_ts_ms = struct.unpack(
        BINARY_HEADER_FORMAT, data[:BINARY_HEADER_SIZE]
    )
    if version != 1:
        raise ValueError(f"Unsupported audio frame version: {version}, expected 1")
    return AudioHeader(version, flags, client_seq, capture_ts_ms), data[BINARY_HEADER_SIZE:]
