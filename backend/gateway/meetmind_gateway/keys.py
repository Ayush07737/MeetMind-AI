"""MeetMind AI - Gateway key re-exports from meetmind_schemas.keys.

Thin re-export preserving backwards compatibility across the gateway and tests.
"""

from __future__ import annotations

from meetmind_schemas.keys import (
    ACTIVE_MEETINGS_SET_KEY,
    JANITOR_LEASE_KEY,
    KEY_SEGMENT_PATTERN,
    LIFECYCLE_STREAM_KEY,
    TENANT_SEGMENT_PATTERN,
    active_meetings_set_key,
    asr_owner_lease_key,
    audio_stream_key,
    events_stream_key,
    janitor_lock_key,
    last_seq_key,
    lifecycle_stream_key,
    listeners_key,
    meeting_session_key,
    metrics_key,
    out_stream_key,
    tenant_connections_key,
    transcript_stream_key,
    user_connections_key,
    validate_key_segment,
    validate_tenant_segment,
    writer_lease_key,
    ws_ticket_key,
)

__all__ = [
    "ACTIVE_MEETINGS_SET_KEY",
    "JANITOR_LEASE_KEY",
    "KEY_SEGMENT_PATTERN",
    "LIFECYCLE_STREAM_KEY",
    "TENANT_SEGMENT_PATTERN",
    "active_meetings_set_key",
    "asr_owner_lease_key",
    "audio_stream_key",
    "events_stream_key",
    "janitor_lock_key",
    "last_seq_key",
    "lifecycle_stream_key",
    "listeners_key",
    "meeting_session_key",
    "metrics_key",
    "out_stream_key",
    "tenant_connections_key",
    "transcript_stream_key",
    "user_connections_key",
    "validate_key_segment",
    "validate_tenant_segment",
    "writer_lease_key",
    "ws_ticket_key",
]
