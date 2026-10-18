"""MeetMind AI - FastAPI application factory.

Creates the gateway FastAPI app with lifespan management for Redis
connections, sequence counters, stream producers, session managers,
and the one-time WebSocket ticket issuer.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import secrets
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import JSONResponse

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

    from redis.asyncio.retry import Retry
    from redis.backoff import ExponentialBackoff
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    retry_policy = Retry(ExponentialBackoff(cap=0.5, base=0.05), retries=3)

    # -- Start Redis connection ------------------------------------------------
    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        max_connections=50,
        retry=retry_policy,
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )
    # Separate client for binary data (audio chunks have bytes) — WP6: audio_redis_url
    audio_redis_url = settings.audio_redis_url or settings.redis_url
    redis_binary_client = aioredis.from_url(
        audio_redis_url,
        decode_responses=False,
        max_connections=50,
        retry=retry_policy,
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )

    # WP6: Startup non-persistence check
    from .persistence_check import assert_audio_redis_not_persistent

    await assert_audio_redis_not_persistent(redis_binary_client, settings)

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
        redis_binary_client, maxlen=settings.audio_stream_maxlen
    )

    # WP6: Background Janitor task
    from .janitor import run_janitor_loop

    janitor_task = asyncio.create_task(run_janitor_loop(redis_client, app.state.session_manager))

    # Event-loop lag monitor
    from .lag_monitor import EventLoopLagMonitor

    lag_monitor = EventLoopLagMonitor()
    lag_monitor.start()
    app.state.lag_monitor = lag_monitor

    # Pre-warm JWKS keys cache at startup to avoid runtime request queueing
    await app.state.authenticator.warm_cache()

    # Background worker metrics flush
    async def _flush_worker_metrics() -> None:
        while True:
            await asyncio.sleep(1.0)
            try:
                pid = os.getpid()
                data = {
                    "pid": pid,
                    "lag": app.state.lag_monitor.get_stats(),
                    "auth": app.state.authenticator.get_stats(),
                    "session": app.state.session_manager.get_stats(),
                    "lag_setup_samples": app.state.lag_monitor.setup_lags_ms[-1000:],
                    "lag_steady_samples": app.state.lag_monitor.steady_lags_ms[-1000:],
                    "warm_jwt_samples": app.state.authenticator.warm_jwt_times_ms[-1000:],
                    "redis_samples": app.state.session_manager.create_meeting_times_ms[-1000:],
                }
                await app.state.redis.set(
                    f"mm:system:worker_metrics:{pid}", json.dumps(data), ex=120
                )
                if not app.state.lag_monitor.is_steady:
                    val = await app.state.redis.get("mm:system:mark_steady")
                    if val in ("1", b"1"):
                        app.state.lag_monitor.mark_steady()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    metrics_flush_task = asyncio.create_task(_flush_worker_metrics())

    # -- Warm DB connection pool via app role (CA-1) ---------------------------
    try:
        from meetmind_security.db import get_app_dsn, init_db

        pool = await init_db(get_app_dsn(), verify_privileges=True)
        warmed = [await pool.acquire() for _ in range(pool.get_min_size())]
        current_user = await warmed[0].fetchval("SELECT current_user")
        for conn in warmed:
            await pool.release(conn)
        msg = (
            f"INFO: Database pool warmed ({pool.get_min_size()} connections, "
            f"current_user={current_user})\n"
        )
        sys.stderr.write(msg)
        sys.stderr.flush()
        logger.info(
            "Database pool warmed (%d connections, current_user=%s)",
            pool.get_min_size(),
            current_user,
        )
        app.state.db_pool = pool

        # Verify registered tenant schemas exist in database (Item 2)
        verified_schemas: set[str] = set()
        try:
            from meetmind_security.tenant import get_tenant_router

            router = get_tenant_router()
            tenant_ids_to_check = set(settings.tenant_org_map.values())
            if settings.default_tenant_id:
                tenant_ids_to_check.add(settings.default_tenant_id)
            for tid in tenant_ids_to_check:
                try:
                    cfg = router.resolve(tid)
                    check_sql = (
                        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata "
                        "WHERE schema_name = $1)"
                    )
                    exists = await pool.fetchval(check_sql, cfg.pg_schema)
                    if exists:
                        verified_schemas.add(cfg.pg_schema)
                        logger.info("Verified tenant schema '%s' exists", cfg.pg_schema)
                    else:
                        logger.warning("Tenant schema '%s' is NOT provisioned", cfg.pg_schema)
                except Exception as e:
                    logger.warning("Error checking tenant '%s': %s", tid, e)
        except Exception as exc:
            logger.warning("Could not verify registered tenant schemas at startup: %s", exc)
        app.state.verified_schemas = verified_schemas
        app.state.negative_schema_cache = {}
    except RuntimeError as exc:
        if "Security violation" in str(exc) or "holds illegal privilege" in str(exc):
            logger.critical("Startup refused due to database privilege violation: %s", exc)
            raise
        logger.warning("Database pool initialization skipped: %s", exc)
    except Exception as exc:
        logger.warning("Database pool warming skipped or failed: %s", exc)

    logger.info("Gateway started (env=%s)", settings.app_env)

    yield

    # -- Shutdown --------------------------------------------------------------
    metrics_flush_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await metrics_flush_task
    lag_monitor.stop()
    janitor_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await janitor_task
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

    # ── Multi-Tenancy Router Injection (K2) ──────────────────────────────
    from meetmind_security.tenant import configure_tenant_router

    configure_tenant_router(
        app_env=settings.app_env,
        tenant_org_map=settings.tenant_org_map,
        default_tenant_id=settings.default_tenant_id,
    )

    # ── CORS Middleware (CA-3 & Security Hardening) ──────────────────────
    from fastapi.middleware.cors import CORSMiddleware

    origins = settings.allowed_origins if settings.allowed_origins else ["https://app.meetmind.ai"]
    has_wildcard = "*" in origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=not has_wildcard,  # Must NOT be '*' with credentials
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # ── Health check ─────────────────────────────────────────────────────
    @app.get("/health")
    async def health_check() -> dict[str, str]:
        return {"status": "ok", "service": "gateway"}

    # ── System Metrics & Phase Transitions (P0-1) ────────────────────────
    async def _check_bench_access(req: Request) -> None:
        """P0-1: Enforce security on system benchmark endpoints.

        Disabled unless GATEWAY_BENCH_ENDPOINTS=true AND environment is not production.
        Requires admin credentials (shared secret header or Clerk admin role).
        """
        is_prod = settings.app_env.lower() in ("prod", "production")
        if not settings.gateway_bench_endpoints or is_prod:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

        # 1. Check shared-secret header X-Admin-Key
        admin_header = req.headers.get("x-admin-key")
        if (
            settings.gateway_admin_key
            and admin_header
            and secrets.compare_digest(admin_header, settings.gateway_admin_key)
        ):
            return

        # 2. Check Clerk Bearer token with admin role
        auth_header = req.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()
            try:
                user = await app.state.authenticator.authenticate(token)
                admin_roles = set(settings.elevated_listen_roles) | {"admin", "org:admin"}
                user_role = (
                    getattr(user, "role", "")
                    or getattr(user, "org_role", "")
                    or user.claims.get("o", {}).get("rol", "")
                )
                if user_role and user_role in admin_roles:
                    return
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Admin role required for system benchmark endpoints",
                )
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Invalid admin credentials: {exc}",
                ) from exc

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin credentials required for system benchmark endpoints",
        )

    @app.post("/v1/system/mark-steady")
    async def mark_steady(request: Request) -> dict[str, str]:
        """Mark steady-state streaming phase across all gateway workers."""
        await _check_bench_access(request)
        if hasattr(app.state, "lag_monitor"):
            app.state.lag_monitor.mark_steady()
        if hasattr(app.state, "redis"):
            with contextlib.suppress(Exception):
                await app.state.redis.set("mm:system:mark_steady", "1", ex=3600)
        return {"status": "ok", "state": "steady"}

    @app.get("/v1/system/metrics")
    async def get_system_metrics(request: Request) -> dict[str, Any]:
        """Aggregate event-loop lag, JWKS hit rate, JWT verify, and Redis times."""
        await _check_bench_access(request)
        from .lag_monitor import compute_percentiles

        pid = os.getpid()
        lag_stats = app.state.lag_monitor.get_stats() if hasattr(app.state, "lag_monitor") else {}
        import inspect

        auth_stats = (
            app.state.authenticator.get_stats() if hasattr(app.state, "authenticator") else {}
        )
        if inspect.isawaitable(auth_stats):
            auth_stats = await auth_stats
        session_stats = (
            app.state.session_manager.get_stats() if hasattr(app.state, "session_manager") else {}
        )
        if inspect.isawaitable(session_stats):
            session_stats = await session_stats

        cur_data = {
            "pid": pid,
            "lag": lag_stats,
            "auth": auth_stats,
            "session": session_stats,
            "lag_setup_samples": (
                app.state.lag_monitor.setup_lags_ms[-1000:]
                if hasattr(app.state, "lag_monitor")
                else []
            ),
            "lag_steady_samples": (
                app.state.lag_monitor.steady_lags_ms[-1000:]
                if hasattr(app.state, "lag_monitor")
                else []
            ),
            "warm_jwt_samples": (
                app.state.authenticator.warm_jwt_times_ms[-1000:]
                if hasattr(app.state, "authenticator")
                else []
            ),
            "redis_samples": (
                app.state.session_manager.create_meeting_times_ms[-1000:]
                if hasattr(app.state, "session_manager")
                else []
            ),
        }
        if hasattr(app.state, "redis"):
            with contextlib.suppress(Exception):
                await app.state.redis.set(
                    f"mm:system:worker_metrics:{pid}", json.dumps(cur_data), ex=120
                )

        all_workers: list[dict[str, Any]] = []
        if hasattr(app.state, "redis"):
            try:
                async for k in app.state.redis.scan_iter("mm:system:worker_metrics:*", count=50):
                    raw = await app.state.redis.get(k)
                    if raw:
                        all_workers.append(json.loads(raw))
            except Exception as e:
                logger.warning("Error fetching worker metrics from Redis: %s", e)

        if not all_workers:
            all_workers = [cur_data]

        all_setup_lags: list[float] = []
        all_steady_lags: list[float] = []
        all_warm_jwt: list[float] = []
        all_redis_times: list[float] = []
        total_hits = 0
        total_misses = 0

        for w in all_workers:
            all_setup_lags.extend(w.get("lag_setup_samples", []))
            all_steady_lags.extend(w.get("lag_steady_samples", []))
            all_warm_jwt.extend(w.get("warm_jwt_samples", []))
            all_redis_times.extend(w.get("redis_samples", []))
            auth_st = w.get("auth", {})
            total_hits += auth_st.get("cache_hits", 0)
            total_misses += auth_st.get("cache_misses", 0)

        total_reqs = total_hits + total_misses
        hit_rate = round((total_hits / total_reqs) * 100.0, 2) if total_reqs > 0 else 100.0

        return {
            "workers_reporting": len(all_workers),
            "lag": {
                "setup": compute_percentiles(all_setup_lags),
                "steady": compute_percentiles(all_steady_lags),
            },
            "jwks": {
                "hits": total_hits,
                "misses": total_misses,
                "hit_rate_pct": hit_rate,
            },
            "warm_jwt_ms": compute_percentiles(all_warm_jwt),
            "redis_create_meeting_ms": compute_percentiles(all_redis_times),
        }

    # ── Server-Issued Meeting Lifecycle (K6) ─────────────────────────────
    @app.post("/v1/meetings", status_code=status.HTTP_201_CREATED)
    async def create_meeting(request: Request) -> dict[str, Any]:
        """Issue a random unguessable meeting_id bound to tenant and owner (K6)."""
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid Authorization header",
            )
        token = auth_header[7:].strip()
        t_req_start = time.perf_counter()
        t0 = time.perf_counter()
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
        t_jwt = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        tenant_id = user.tenant_id

        # Verify tenant schema is provisioned (Item 2 & P0-2)
        verified = getattr(app.state, "verified_schemas", set())
        negative_cache = getattr(app.state, "negative_schema_cache", {})
        pool = getattr(app.state, "db_pool", None)
        if pool:
            from meetmind_security.tenant import get_tenant_router

            try:
                cfg = get_tenant_router().resolve(tenant_id)
                schema_name = cfg.pg_schema
            except Exception:
                schema_name = f"tenant_{tenant_id}"

            if schema_name not in verified:
                now_mono = time.monotonic()
                last_failed = negative_cache.get(schema_name, 0.0)
                if now_mono - last_failed < 10.0:
                    err_msg = (
                        f"Tenant schema '{schema_name}' is not provisioned. "
                        "Provision tenant schema before creating meetings."
                    )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=err_msg,
                    )

                check_sql = (
                    "SELECT EXISTS (SELECT 1 FROM information_schema.schemata "
                    "WHERE schema_name = $1)"
                )
                exists = await pool.fetchval(check_sql, schema_name)
                if exists:
                    verified.add(schema_name)
                    negative_cache.pop(schema_name, None)
                else:
                    negative_cache[schema_name] = now_mono
                    err_msg = (
                        f"Tenant schema '{schema_name}' is not provisioned. "
                        "Provision tenant schema before creating meetings."
                    )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=err_msg,
                    )
        t_tenant = (time.perf_counter() - t0) * 1000

        body: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            parsed = await request.json()
            if isinstance(parsed, dict):
                body = parsed
            elif parsed is not None:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="Request body must be a JSON object",
                )

        raw_mid = str(body.get("meeting_id") or "").strip()
        meeting_id = raw_mid or uuid.uuid4().hex  # 32 characters (>= 22)
        trace_id = str(body.get("trace_id") or "").strip() or None

        local_user_name = body.get("local_user_name")
        if local_user_name is not None:
            if not isinstance(local_user_name, str):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="local_user_name must be a string",
                )
            if len(local_user_name) > 100:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="local_user_name must be at most 100 characters",
                )

        attendees = body.get("attendees")
        if attendees is not None:
            if not isinstance(attendees, list):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="attendees must be a list of strings",
                )
            if len(attendees) > 50:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="attendees must contain at most 50 items",
                )
            for att in attendees:
                if not isinstance(att, str):
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail="All attendees items must be strings",
                    )
                if len(att) > 100:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail="Attendee item must be at most 100 characters",
                    )

        t0 = time.perf_counter()
        record = await app.state.session_manager.create_meeting(
            tenant_id=tenant_id,
            owner_id=user.user_id,
            meeting_id=meeting_id,
            local_user_name=local_user_name,
            attendees=attendees,
            trace_id=trace_id,
        )
        t_redis = (time.perf_counter() - t0) * 1000
        t_total = (time.perf_counter() - t_req_start) * 1000

        return JSONResponse(
            content=record,
            status_code=status.HTTP_201_CREATED,
            headers={
                "Server-Timing": (
                    f"jwt;dur={t_jwt:.2f}, tenant;dur={t_tenant:.2f}, "
                    f"redis;dur={t_redis:.2f}, total;dur={t_total:.2f}"
                )
            },
        )

    # ── One-Time WebSocket Ticket Exchange (WP4, K6, K7) ──────────────────
    @app.post("/v1/ws-ticket")
    async def create_ws_ticket(request: Request) -> dict[str, Any]:
        """Issue a single-use 128-bit ticket bound to meeting_id, role, and client_instance_id."""
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

        # Parse request body or query params for meeting_id, role, client_instance_id
        body: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            body = await request.json()

        mid_source = body.get("meeting_id") or request.query_params.get("meeting_id") or ""
        meeting_id = str(mid_source).strip()
        role = str(body.get("role") or request.query_params.get("role") or "ingest").lower()

        if role not in ("ingest", "listen"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Role must be 'ingest' or 'listen'",
            )

        if not meeting_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="meeting_id is required",
            )

        # K6: Server-issued meeting ids. Verify meeting exists in tenant.
        # "Not found" and "not yours" return the SAME 404 response externally
        # (audit still distinguishes them).
        existing_meeting = await app.state.session_manager.get_meeting(user.tenant_id, meeting_id)
        if not existing_meeting:
            other_tenant = await app.state.session_manager.find_meeting_tenant(meeting_id)
            if other_tenant and other_tenant != user.tenant_id:
                try:
                    from meetmind_security.audit_log import write_audit_event

                    await write_audit_event(
                        tenant_id=user.tenant_id,
                        user_id=user.user_id,
                        event_type="cross_tenant_access_denied",
                        payload={
                            "meeting_id": meeting_id,
                            "target_tenant": other_tenant,
                            "attempted_tenant": user.tenant_id,
                            "source": "ticket_exchange",
                        },
                    )
                except Exception:
                    pass
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="meeting not found",
            )

        # Enforce can_listen role permissions if role is listen
        if role == "listen":
            owner_id = existing_meeting.get("owner_id") or existing_meeting.get("user_id", "")
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

        # K7: client_instance_id is required for capture session
        client_instance_id = str(
            body.get("client_instance_id") or request.query_params.get("client_instance_id") or ""
        ).strip()
        if not client_instance_id:
            if role == "ingest":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="client_instance_id is required for ingest role",
                )
            client_instance_id = str(uuid.uuid4())
        else:
            try:
                uuid.UUID(client_instance_id)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="client_instance_id must be a valid UUID",
                ) from exc

        # 128-bit cryptographically secure ticket
        ticket = secrets.token_urlsafe(16)
        ticket_payload = json.dumps(
            {
                "user_id": user.user_id,
                "tenant_id": user.tenant_id,
                "session_id": user.session_id,
                "role": role,
                "meeting_id": meeting_id,
                "client_instance_id": client_instance_id,
                "claims": user.claims,
                "role_claim": user.role,
                "created_at": time.time(),
            }
        )
        ticket_key = ws_ticket_key(ticket)
        ticket_ttl = settings.ticket_ttl_seconds
        stored = await app.state.redis.set(ticket_key, ticket_payload, ex=ticket_ttl, nx=True)
        if not stored:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store ticket",
            )

        return {"ticket": ticket, "expires_in": ticket_ttl}

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
