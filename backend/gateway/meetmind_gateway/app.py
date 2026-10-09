"""MeetMind AI - FastAPI application factory.

Creates the gateway FastAPI app with lifespan management for Redis
connections, sequence counters, stream producers, session managers,
and the one-time WebSocket ticket issuer.
"""

from __future__ import annotations

import contextlib
import json
import logging
import secrets
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, WebSocket, status

from .auth import (
    AuthForbiddenError,
    AuthUnauthorizedError,
    ClerkAuthenticator,
    can_listen,
)
from .config import GatewaySettings, get_settings
from .keys import ws_ticket_key
from .redis_streams import StreamProducer
from .sequence import SequenceCounter
from .session_manager import SessionManager
from .ws_endpoint import websocket_ingest, websocket_listen

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
        jwks_url=settings.clerk_jwks_url,
        allowed_origins=settings.allowed_origins,
        expected_issuer=settings.clerk_issuer or None,
        tenant_org_map=settings.tenant_org_map,
        default_tenant_id=settings.default_tenant_id,
        app_env=settings.app_env,
        clerk_secret_key=settings.clerk_secret_key,
    )
    app.state.session_manager = SessionManager(redis_client)
    app.state.seq_counter = SequenceCounter(redis_client)
    app.state.stream_producer = StreamProducer(
        redis_binary_client, maxlen=settings.redis_stream_maxlen
    )

    # -- Warm DB connection pool via app role (CA-1) ---------------------------
    try:
        from meetmind_security.db import get_app_dsn, init_db

        pool = await init_db(get_app_dsn())
        warmed = [await pool.acquire() for _ in range(pool.get_min_size())]
        for conn in warmed:
            await pool.release(conn)
        logger.info("Database pool warmed (%d connections)", pool.get_min_size())
    except Exception as exc:
        logger.warning("Database pool warming skipped or failed: %s", exc)

    logger.info("Gateway started (env=%s)", settings.app_env)

    yield

    # -- Shutdown --------------------------------------------------------------
    await redis_client.aclose()
    await redis_binary_client.aclose()
    try:
        from meetmind_security.db import close_db

        await close_db()
    except Exception:
        pass
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

    # ── One-Time WebSocket Ticket Exchange (WP4) ─────────────────────────
    @app.post("/v1/ws-ticket")
    async def create_ws_ticket(request: Request) -> dict[str, Any]:
        """Issue a single-use 128-bit ticket bound to meeting_id and role (ingest|listen)."""
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid Authorization header",
            )
        token = auth_header[7:].strip()
        try:
            user = await app.state.authenticator.authenticate(token)
        except AuthForbiddenError as exc:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(exc.message),
            ) from exc
        except AuthUnauthorizedError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc.message),
            ) from exc

        # Parse request body or query params for meeting_id and role
        body: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            body = await request.json()

        meeting_id = str(body.get("meeting_id") or request.query_params.get("meeting_id") or "")
        role = str(body.get("role") or request.query_params.get("role") or "ingest").lower()

        if role not in ("ingest", "listen"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Role must be 'ingest' or 'listen'",
            )

        if meeting_id:
            # Enforce cross-tenant isolation at ticket issue time
            bound_tenant = await app.state.session_manager.get_meeting_tenant(meeting_id)
            if bound_tenant and bound_tenant != user.tenant_id:
                try:
                    from meetmind_security.audit_log import write_audit_event

                    await write_audit_event(
                        tenant_id=user.tenant_id,
                        user_id=user.user_id,
                        event_type="cross_tenant_access_denied",
                        payload={
                            "meeting_id": meeting_id,
                            "target_tenant": bound_tenant,
                            "attempted_tenant": user.tenant_id,
                            "source": "ticket_exchange",
                        },
                    )
                except Exception:
                    pass
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="cross-tenant access denied",
                )

            # Enforce can_listen role permissions if role is listen
            if role == "listen":
                session_data = await app.state.session_manager.get_session_data(
                    meeting_id, user.tenant_id
                )
                if session_data:
                    owner_id = session_data.get("user_id", "")
                    if not can_listen(
                        user,
                        user.tenant_id,
                        owner_id,
                        settings.elevated_listen_roles,
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="forbidden: cannot listen to meeting",
                        )

        # 128-bit cryptographically secure ticket
        ticket = secrets.token_urlsafe(16)
        ticket_payload = json.dumps(
            {
                "user_id": user.user_id,
                "tenant_id": user.tenant_id,
                "session_id": user.session_id,
                "role": role,
                "meeting_id": meeting_id,
                "claims": user.claims,
                "role_claim": user.role,
                "created_at": time.time(),
            }
        )
        ticket_key = ws_ticket_key(ticket)
        stored = await app.state.redis.set(ticket_key, ticket_payload, ex=30, nx=True)
        if not stored:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store ticket",
            )

        return {"ticket": ticket, "expires_in": 30}

    # ── WebSocket routes (WP4) ───────────────────────────────────────────
    @app.websocket("/v1/meetings/{meeting_id}/stream")
    async def ws_meetings_stream(websocket: WebSocket, meeting_id: str) -> None:
        """Primary ingest/write WebSocket endpoint."""
        await websocket_ingest(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            seq_counter=app.state.seq_counter,
            stream_producer=app.state.stream_producer,
            redis_client=app.state.redis,
        )

    @app.websocket("/v1/meetings/{meeting_id}/listen")
    async def ws_meetings_listen(websocket: WebSocket, meeting_id: str) -> None:
        """Read-only listener WebSocket endpoint."""
        await websocket_listen(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            redis_client=app.state.redis,
        )

    # Legacy & alias routes for backward compatibility
    @app.websocket("/v1/ws/ingest/{meeting_id}")
    async def ws_v1_ingest(websocket: WebSocket, meeting_id: str) -> None:
        await websocket_ingest(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            seq_counter=app.state.seq_counter,
            stream_producer=app.state.stream_producer,
            redis_client=app.state.redis,
        )

    @app.websocket("/ws/ingest/{meeting_id}")
    async def ws_legacy_ingest(websocket: WebSocket, meeting_id: str) -> None:
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
