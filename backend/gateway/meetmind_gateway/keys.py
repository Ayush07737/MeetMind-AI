"""MeetMind AI - Centralized Redis Key Schemes for Ingestion Gateway (WP4).

This module is the EXCLUSIVE place where Redis keys are formatted for the gateway.
All keys follow the pattern:
    mm:{tenant}:{resource}:{meeting}

All tenant segments are strictly normalised lowercase and validated against:
    ^[a-z0-9_]{1,40}$ (K3)
Meeting and other resource segments are validated against:
    ^[A-Za-z0-9_-]{1,64}$
"""

from __future__ import annotations

import re

TENANT_SEGMENT_PATTERN = re.compile(r"^[a-z0-9_]{1,40}$")
KEY_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def validate_tenant_segment(tenant: str) -> str:
    """Normalize and validate tenant key segment against ^[a-z0-9_]{1,40}$ (K3)."""
    if not isinstance(tenant, str) or not tenant.strip():
        raise ValueError("Invalid tenant: must not be empty")
    norm = tenant.strip().lower()
    if not TENANT_SEGMENT_PATTERN.match(norm):
        raise ValueError(
            f"Invalid tenant: '{tenant}'. Normalized '{norm}' must match ^[a-z0-9_]{{1,40}}$"
        )
    return norm


def validate_key_segment(val: str, name: str = "identifier") -> str:
    """Validate key segment against allowed characters and length."""
    if not val or not KEY_SEGMENT_PATTERN.match(val):
        raise ValueError(f"Invalid {name}: '{val}'. Must match ^[A-Za-z0-9_-]{{1,64}}$")
    return val


def audio_stream_key(tenant: str, meeting_id: str) -> str:
    """Stream of raw audio chunks: mm:{tenant}:audio:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:audio:{m}"


def transcript_stream_key(tenant: str, meeting_id: str) -> str:
    """Stream of transcript chunks: mm:{tenant}:transcript:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:transcript:{m}"


def events_stream_key(tenant: str, meeting_id: str) -> str:
    """Internal event bus: mm:{tenant}:events:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:events:{m}"


def out_stream_key(tenant: str, meeting_id: str) -> str:
    """Client-facing allow-listed outbound stream: mm:{tenant}:out:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:out:{m}"


def meeting_session_key(tenant: str, meeting_id: str) -> str:
    """Meeting session metadata hash: mm:{tenant}:meeting:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:meeting:{m}"


def writer_lease_key(tenant: str, meeting_id: str) -> str:
    """Single-writer lease string: mm:{tenant}:writer:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:writer:{m}"


def last_seq_key(tenant: str, meeting_id: str) -> str:
    """Highest consecutive seq counter string: mm:{tenant}:seq:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:seq:{m}"


def listeners_key(tenant: str, meeting_id: str) -> str:
    """Active listeners set: mm:{tenant}:listeners:{meeting}."""
    t = validate_tenant_segment(tenant)
    m = validate_key_segment(meeting_id, "meeting_id")
    return f"mm:{t}:listeners:{m}"


def ws_ticket_key(ticket: str) -> str:
    """One-time WebSocket ticket key: mm:ticket:{ticket}."""
    t = validate_key_segment(ticket, "ticket")
    return f"mm:ticket:{t}"


def user_connections_key(tenant: str, user_id: str) -> str:
    """Active connection count for a user within a tenant."""
    t = validate_tenant_segment(tenant)
    u = validate_key_segment(user_id, "user_id")
    return f"mm:{t}:userconns:{u}"


def tenant_connections_key(tenant: str) -> str:
    """Active connection count for a tenant."""
    t = validate_tenant_segment(tenant)
    return f"mm:{t}:conns"
