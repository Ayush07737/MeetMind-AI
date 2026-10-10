"""MeetMind AI - Shared Python Schemas.

Pydantic models that mirror the Zod schemas in src/index.ts.
These are the canonical Python-side contracts shared across backend services.
"""

from .events import (
    BINARY_HEADER_FORMAT,
    BINARY_HEADER_SIZE,
    OUTBOUND_EVENT_TYPES,
    AckFrame,
    AdapterType,
    AudioHeader,
    AuditEventSchema,
    ClientFrame,
    ConsentAction,
    ConsentEventSchema,
    ConsentRecordSchema,
    ConsentType,
    ControlAction,
    ControlFrame,
    ErrorCode,
    ErrorFrame,
    IntelligenceEventFrame,
    MeetingStatus,
    TenantId,
    TranscriptChunkFrame,
    WSCloseCode,
    decode_audio_frame,
    encode_audio_frame,
)

__all__ = [
    "BINARY_HEADER_FORMAT",
    "BINARY_HEADER_SIZE",
    "OUTBOUND_EVENT_TYPES",
    "AckFrame",
    "AdapterType",
    "AuditEventSchema",
    "AudioHeader",
    "ClientFrame",
    "ConsentAction",
    "ConsentEventSchema",
    "ConsentRecordSchema",
    "ConsentType",
    "ControlAction",
    "ControlFrame",
    "ErrorCode",
    "ErrorFrame",
    "IntelligenceEventFrame",
    "MeetingStatus",
    "TenantId",
    "TranscriptChunkFrame",
    "WSCloseCode",
    "decode_audio_frame",
    "encode_audio_frame",
]
