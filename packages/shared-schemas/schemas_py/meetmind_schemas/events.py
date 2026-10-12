"""MeetMind AI - Pydantic models mirroring shared Zod schemas.

These models MUST stay in sync with ``packages/shared-schemas/src/index.ts``. Any
type changes MUST be applied to both files simultaneously.
"""

from __future__ import annotations

import hashlib
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
    AUDIT_UNAVAILABLE = 4503


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
    "intelligence_event",
    "pre_meeting_brief",
    "transcript_interim",
    "transcript_service_degraded",
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


class PreMeetingBriefFrame(BaseModel):
    """Pre-meeting brief event frame sent to listener client."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["intelligence_event"] = "intelligence_event"
    event_type: Literal["pre_meeting_brief"] = "pre_meeting_brief"
    payload: dict[str, Any] = Field(default_factory=dict)
    meeting_id: str = ""
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


class TranscriptInterimFrame(BaseModel):
    """Client-facing interim transcript chunk frame."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["intelligence_event"] = "intelligence_event"
    event_type: Literal["transcript_interim"] = "transcript_interim"
    payload: dict[str, Any] = Field(default_factory=dict)
    meeting_id: str = ""
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


class TranscriptServiceDegradedFrame(BaseModel):
    """Client-facing service degradation notice."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["intelligence_event"] = "intelligence_event"
    event_type: Literal["transcript_service_degraded"] = "transcript_service_degraded"
    payload: dict[str, Any] = Field(default_factory=dict)
    meeting_id: str = ""
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


OutboundEventFrame = (
    IntelligenceEventFrame
    | PreMeetingBriefFrame
    | TranscriptInterimFrame
    | TranscriptServiceDegradedFrame
)


def build_outbound_frame(
    event_type: str, payload: dict[str, Any], meeting_id: str = ""
) -> OutboundEventFrame:
    """Build typed outbound frame matching OUTBOUND_EVENT_TYPES."""
    if event_type == "pre_meeting_brief":
        return PreMeetingBriefFrame(payload=payload, meeting_id=meeting_id)
    if event_type == "transcript_interim":
        return TranscriptInterimFrame(payload=payload, meeting_id=meeting_id)
    if event_type == "transcript_service_degraded":
        return TranscriptServiceDegradedFrame(payload=payload, meeting_id=meeting_id)
    if event_type == "intelligence_event":
        return IntelligenceEventFrame(event_type=event_type, payload=payload, meeting_id=meeting_id)
    raise ValueError(f"Unsupported outbound event_type: '{event_type}'")


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


# -- Binary Audio Frame Codec (WP5 & §2.3 Canonical) -------------------------

CANONICAL_BINARY_HEADER_FORMAT = (
    ">2sIQHBB"  # magic(2), client_seq(4), capture_ts_ms(8), sample_rate(2), channels(1), codec(1)
)
LEGACY_BINARY_HEADER_FORMAT = ">BBQQ"  # version(1), flags(1), client_seq(8), capture_ts_ms(8)
BINARY_HEADER_FORMAT = CANONICAL_BINARY_HEADER_FORMAT
BINARY_HEADER_SIZE = 18


@dataclass(frozen=True)
class AudioHeader:
    """Header prepended to all binary audio chunks (18 bytes)."""

    client_seq: int
    capture_ts_ms: int
    sample_rate: int = 16000
    channels: int = 1
    codec: int = 1
    magic: bytes = b"MM"
    # Backwards-compatibility fields for legacy WP5 fixtures/callers
    version: int = 1
    flags: int = 0


def encode_audio_frame(
    client_seq: int,
    capture_ts_ms: int,
    payload: bytes,
    *,
    sample_rate: int = 16000,
    channels: int = 1,
    codec: int = 1,
    flags: int | None = None,
    version: int | None = None,
    magic: bytes = b"MM",
) -> bytes:
    """Encode an 18-byte big-endian header followed by binary audio payload.

    Canonical format (>2sIQHBB):
        magic: 0x4D4D ('MM') (2 bytes)
        client_seq: uint32 (4 bytes)
        capture_ts_ms: uint64 (8 bytes)
        sample_rate: uint16 (2 bytes, default 16000)
        channels: uint8 (1 byte, default 1)
        codec: uint8 (1 byte, 1 = PCM16 LE, 2 = Opus)
    Legacy WP5 format (>BBQQ):
        version: uint8 (1 byte)
        flags: uint8 (1 byte)
        client_seq: uint64 (8 bytes)
        capture_ts_ms: uint64 (8 bytes)
    """
    if flags is not None or (version is not None and version != 1) or magic != b"MM":
        v = version if version is not None else 1
        fl = flags if flags is not None else 0
        header = struct.pack(LEGACY_BINARY_HEADER_FORMAT, v, fl, client_seq, capture_ts_ms)
        return header + payload

    header = struct.pack(
        CANONICAL_BINARY_HEADER_FORMAT,
        magic,
        client_seq,
        capture_ts_ms,
        sample_rate,
        channels,
        codec,
    )
    return header + payload


def decode_audio_frame(data: bytes) -> tuple[AudioHeader, bytes]:
    """Decode an 18-byte big-endian header and extract the payload."""
    if len(data) < BINARY_HEADER_SIZE:
        raise ValueError(
            f"Binary audio frame too short: {len(data)} bytes, expected {BINARY_HEADER_SIZE}"
        )
    if data[:2] == b"MM":
        magic, client_seq, capture_ts_ms, sample_rate, channels, codec = struct.unpack(
            CANONICAL_BINARY_HEADER_FORMAT, data[:BINARY_HEADER_SIZE]
        )
        return (
            AudioHeader(
                client_seq=client_seq,
                capture_ts_ms=capture_ts_ms,
                sample_rate=sample_rate,
                channels=channels,
                codec=codec,
                magic=magic,
                version=1,
                flags=0,
            ),
            data[BINARY_HEADER_SIZE:],
        )
    if data[0] == 1:
        version, flags, client_seq, capture_ts_ms = struct.unpack(
            LEGACY_BINARY_HEADER_FORMAT, data[:BINARY_HEADER_SIZE]
        )
        return (
            AudioHeader(
                client_seq=client_seq,
                capture_ts_ms=capture_ts_ms,
                sample_rate=16000,
                channels=1,
                codec=1,
                magic=b"",
                version=version,
                flags=flags,
            ),
            data[BINARY_HEADER_SIZE:],
        )
    raise ValueError(
        f"Unsupported audio frame format: magic {data[:2]!r}, first byte 0x{data[0]:02x}"
    )


# ── Event Envelope & Payloads (WP-A) ─────────────────────────────────────────

DegradedState = Literal["degraded", "recovered"]
DegradedReason = Literal[
    "provider_unreachable",
    "provider_error",
    "unsupported_codec",
    "buffer_overflow",
    "audio_gap",
]


class TranscriptFinalPayload(BaseModel):
    """Payload for transcript_final event (internal bus only)."""

    model_config = ConfigDict(extra="forbid")

    utterance_id: str = Field(..., min_length=24, max_length=24)
    speaker_id: str
    speaker_name: str | None = None
    channel: int | None = None
    text: str
    start_ms: int
    end_ms: int
    start_epoch_ms: int
    confidence: float
    language: str
    provider: str
    model: str


class TranscriptInterimPayload(BaseModel):
    """Payload for transcript_interim event (out stream only)."""

    model_config = ConfigDict(extra="forbid")

    speaker_id: str
    speaker_name: str | None = None
    channel: int | None = None
    text: str
    start_ms: int
    end_ms: int
    start_epoch_ms: int
    confidence: float
    language: str
    provider: str
    model: str
    is_stable: bool = False


class TranscriptServiceDegradedPayload(BaseModel):
    """Payload for transcript_service_degraded event (both streams)."""

    model_config = ConfigDict(extra="forbid")

    state: DegradedState
    reason: DegradedReason
    since_ms: int


class EventEnvelope(BaseModel):
    """Standardized event envelope across internal events and client out streams."""

    model_config = ConfigDict(extra="allow")

    event_type: str
    meeting_id: str
    tenant_id: str
    ts_ms: int
    payload: dict[str, Any]


def compute_utterance_id(
    meeting_id: str,
    channel: int | None,
    speaker_id: str,
    start_ms: int,
) -> str:
    """Deterministic utterance_id:
    First 24 hex chars of sha256 of meeting|channel|speaker|start_ms.
    """
    ch_str = str(channel) if channel is not None else "0"
    spk_str = str(speaker_id) if speaker_id else "spk_0"
    key = f"{meeting_id}|{ch_str}|{spk_str}|{start_ms}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
