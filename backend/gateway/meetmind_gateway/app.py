"""MeetMind AI - FastAPI application factory.

Creates the gateway FastAPI app with lifespan management for Redis
connections, sequence counters, stream producers, and session managers.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket

from .auth import ClerkAuthenticator
from .config import GatewaySettings, get_settings
from .redis_streams import StreamProducer
from .sequence import SequenceCounter
from .session_manager import SessionManager
from .ws_endpoint import websocket_ingest

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifecycle: start/stop Redis connections."""
    settings: GatewaySettings = app.state.settings

    # -- Start Redis connection ------------------------------------------------
    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        max_connections=50,
    )
    # Separate client for binary data (audio chunks have bytes)
    redis_binary_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=False,
        max_connections=50,
    )

    app.state.redis = redis_client
    app.state.redis_binary = redis_binary_client
    app.state.authenticator = ClerkAuthenticator(
        clerk_secret_key=settings.clerk_secret_key,
        default_tenant_id=settings.default_tenant_id,
    )
    app.state.session_manager = SessionManager(redis_client)
    app.state.seq_counter = SequenceCounter(redis_client)
    app.state.stream_producer = StreamProducer(
        redis_binary_client, maxlen=settings.redis_stream_maxlen
    )

    logger.info("Gateway started (env=%s)", settings.app_env)

    yield

    # -- Shutdown --------------------------------------------------------------
    await redis_client.aclose()
    await redis_binary_client.aclose()
    logger.info("Gateway shut down")


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if settings is None:
        settings = get_settings()

    app = FastAPI(
        title="MeetMind AI Gateway",
        description="Ingestion Gateway & WebSocket Core (§1)",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = settings

    # ── Health check ─────────────────────────────────────────────────────
    @app.get("/health")
    async def health_check() -> dict[str, str]:
        return {"status": "ok", "service": "gateway"}

    # ── WebSocket route ──────────────────────────────────────────────────
    @app.websocket("/ws/ingest/{meeting_id}")
    async def ws_ingest(websocket: WebSocket, meeting_id: str) -> None:
        await websocket_ingest(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            seq_counter=app.state.seq_counter,
            stream_producer=app.state.stream_producer,
            redis_client=app.state.redis,
        )

    return app
