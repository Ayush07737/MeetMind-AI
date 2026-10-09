"""MeetMind AI - Gateway frame models re-exported from meetmind_schemas.

CA-5 canonical compliance: The gateway imports all schemas, close codes,
error codes, and frame contracts from meetmind_schemas.events and does not
define its own separate duplicate copies.
"""

from __future__ import annotations

from meetmind_schemas.events import (
    BINARY_HEADER_FORMAT,
    BINARY_HEADER_SIZE,
    OUTBOUND_EVENT_TYPES,
    AckFrame,
    AdapterType,
    AudioHeader,
    ClientFrame,
    ControlAction,
    ControlFrame,
    ErrorCode,
    ErrorFrame,
    IntelligenceEventFrame,
    MeetingStatus,
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
    "AudioHeader",
    "ClientFrame",
    "ControlAction",
    "ControlFrame",
    "ErrorCode",
    "ErrorFrame",
    "IntelligenceEventFrame",
    "MeetingStatus",
    "TranscriptChunkFrame",
    "WSCloseCode",
    "decode_audio_frame",
    "encode_audio_frame",
]
