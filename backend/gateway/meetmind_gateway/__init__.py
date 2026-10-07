"""MeetMind AI — Ingestion Gateway & WebSocket Core (§1).

This package provides the real-time audio/text ingestion gateway.
It is domain-agnostic: it has no knowledge of which vertical the
audio came from. Domain behavior belongs to §19 (Domain Pack Framework).

Key modules:
    - app: FastAPI application factory
    - ws_endpoint: WebSocket /ws/ingest/{meeting_id} handler
    - auth: Clerk JWT authentication
    - frames: Pydantic models for all WS frame types
    - consent_gate: Consent verification (§15 integration)
    - redis_streams: Redis stream producer + event subscriber
    - sequence: Redis-backed per-meeting sequence counter
    - session_manager: Meeting session lifecycle
    - audit: Gateway audit event helpers
"""
