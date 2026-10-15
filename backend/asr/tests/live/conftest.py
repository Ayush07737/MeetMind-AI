"""Pytest configuration for live Deepgram verification suite."""

from __future__ import annotations

from backend.asr.tests.acceptance.conftest import (  # noqa: F401
    AsrProcessHandle,
    LocalJWKSServer,
    ServerHarness,
    asr_worker_factory,
    create_meeting,
    enforce_linux_redis_without_persistence,
    exchange_ws_ticket,
    gateway_harness,
    get_free_port,
    jwks_server,
    make_pcm16_chunk,
    pytest_runtest_makereport,
    real_db_tenants,
    real_redis,
)
