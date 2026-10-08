"""MeetMind AI - WebSocket frame models.

All frames exchanged over the WebSocket connection are defined here as
Pydantic v2 models. The protocol uses a discriminated union on the ``type``
field for JSON frames. Binary frames (raw audio) are handled separately.

Client -> Server:
    - ControlFrame (type="control")
    - TranscriptChunkFrame (type="transcript_chunk")
    - Binary bytes (audio_chunk -- not a JSON model)

Server -> Client:
    - IntelligenceEventFrame (type="intelligence_event")
    - AckFrame (type="ack")
    - ErrorFrame (type="error")
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ── Enums ────────────────────────────────────────────────────────────────────


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


class ErrorCode(StrEnum):
    """Error codes sent to clients."""

    CONSENT_REQUIRED = "CONSENT_REQUIRED"
    AUTH_FAILED = "AUTH_FAILED"
    INVALID_FRAME = "INVALID_FRAME"
    UNKNOWN_FRAME_TYPE = "UNKNOWN_FRAME_TYPE"
    MEETING_ENDED = "MEETING_ENDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


# ── Client -> Server frames ─────────────────────────────────────────────────


class ControlFrame(BaseModel):
    """Control frame for meeting lifecycle events."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["control"] = "control"
    action: ControlAction
    adapter_type: AdapterType | None = None
    consent_type: str | None = None  # e.g. "audio_capture"


class TranscriptChunkFrame(BaseModel):
    """Pre-transcribed text from an adapter doing local ASR."""

    type: Literal["transcript_chunk"] = "transcript_chunk"
    text: str = Field(..., min_length=1)
    client_seq: int | None = None  # client-side sequence, informational only
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


# Discriminated union for all client JSON frames
ClientFrame = Annotated[
    ControlFrame | TranscriptChunkFrame,
    Field(discriminator="type"),
]


# ── Server -> Client frames ─────────────────────────────────────────────────


class IntelligenceEventFrame(BaseModel):
    """Intelligence event from downstream services (§6/§7)."""

    type: Literal["intelligence_event"] = "intelligence_event"
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    meeting_id: str = ""
    timestamp_ms: int = Field(default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000))


class AckFrame(BaseModel):
    """Acknowledgement of a received frame."""

    type: Literal["ack"] = "ack"
    seq: int
    frame_type: str | None = None
    status: str = "acked"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ErrorFrame(BaseModel):
    """Error frame sent to the client."""

    type: Literal["error"] = "error"
    code: ErrorCode
    message: str
