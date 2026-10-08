"""MeetMind AI - Async PostgreSQL Connection Pool Management.

Connects to Neon (or managed PostgreSQL) via asyncpg with a single shared
connection pool and per-transaction tenant isolation.

Tenant isolation is achieved by setting ``SET LOCAL search_path = "<schema>"``
within each query transaction, preventing connection pool search_path leaks.
Statement caching is disabled (``statement_cache_size=0``) for safe operation
with PgBouncer and Neon connection pooling.
"""

from __future__ import annotations

import asyncio
import logging
import os
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from .tenant import TenantRouter

logger = logging.getLogger(__name__)

# Shared singleton router and pool
_router = TenantRouter()
_shared_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

# Deprecated module variable kept for test patching backward-compatibility
_UNSET_SENTINEL = object()
NEON_DATABASE_URL: Any = _UNSET_SENTINEL


def get_neon_database_url() -> str:
    """Return the runtime PostgreSQL/Neon connection URL.

    Runtime defaults to the app role DSN (NEON_APP_DATABASE_URL, SELECT+INSERT only),
    falling back to NEON_DATABASE_URL if not configured.
    """
    if NEON_DATABASE_URL is not _UNSET_SENTINEL:
        url = NEON_DATABASE_URL
    else:
        url = os.environ.get("NEON_APP_DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "NEON_DATABASE_URL is not set. "
            "Set it in .env to your Neon connection string, e.g.:\n"
            "  NEON_DATABASE_URL=postgresql://meetmind:<password>@<endpoint>.neon.tech/meetmind_ai?sslmode=require"
        )
    return url


def get_neon_owner_database_url() -> str:
    """Return the owner DSN for migrations and DDL operations."""
    url = os.environ.get("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL owner DSN is not set.")
    return url


_require_neon_url = get_neon_database_url


def _create_ssl_context(
    dsn: str = "postgresql://ep.neon.tech/db?sslmode=require",
) -> ssl.SSLContext | None:
    """Create an SSL context for connections (TLS required for Neon)."""
    if ("sslmode=disable" in dsn or "localhost" in dsn or "127.0.0.1" in dsn) and (
        "sslmode=require" not in dsn
    ):
        return None

    ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def _create_pool_with_retry(dsn: str, max_retries: int = 3) -> asyncpg.Pool:
    """Create connection pool with exponential backoff retry for Neon cold starts."""
    ssl_ctx = _create_ssl_context(dsn)
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Initializing asyncpg connection pool (attempt %d/%d)...", attempt, max_retries
            )
            return await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=10,
                ssl=ssl_ctx,
                timeout=10.0,
                command_timeout=30.0,
                statement_cache_size=0,
            )
        except (
            asyncpg.CannotConnectNowError,
            asyncpg.PostgresConnectionError,
            OSError,
            TimeoutError,
        ) as exc:
            if attempt == max_retries:
                logger.error(
                    "Failed to connect to database after %d attempts: %s", max_retries, exc
                )
                raise
            logger.warning(
                "Database connection failed (attempt %d/%d): %s. Retrying in %.1fs...",
                attempt,
                max_retries,
                exc,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff *= 2.0


async def init_db(dsn: str | None = None) -> asyncpg.Pool:
    """Initialize the global shared connection pool."""
    global _shared_pool
    async with _pool_lock:
        if _shared_pool is not None and not _shared_pool._closed:
            return _shared_pool

        connection_url = dsn or get_neon_database_url()
        _shared_pool = await _create_pool_with_retry(connection_url)
        return _shared_pool


async def get_pool(tenant_id: str = "default") -> asyncpg.Pool:
    """Return the shared connection pool.

    Tenant isolation is handled at the transaction / query level.
    The ``tenant_id`` parameter is accepted for backward compatibility.
    """
    global _shared_pool
    if _shared_pool is not None and not _shared_pool._closed:
        return _shared_pool

    return await init_db()


async def close_db() -> None:
    """Gracefully close the shared connection pool."""
    global _shared_pool
    async with _pool_lock:
        if _shared_pool is not None:
            await _shared_pool.close()
            _shared_pool = None


async def close_all_pools() -> None:
    """Backward-compatible alias for close_db."""
    await close_db()


@asynccontextmanager
async def tenant_conn(tenant_id: str = "default") -> AsyncIterator[asyncpg.Connection]:
    """Acquire a connection scoped to the tenant's schema within a transaction.

    Ensures the search_path does not leak back to other connections.
    """
    schema = _router.pg_schema(tenant_id)
    pool = await get_pool()
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(f'SET LOCAL search_path = "{schema}"')
        yield conn


async def execute_raw(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> str:
    """Execute a raw SQL statement against a tenant's schema."""
    async with tenant_conn(tenant_id) as conn:
        return await conn.execute(sql, *args)


async def fetch_all(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> list[asyncpg.Record]:
    """Fetch all rows from a query against a tenant's schema."""
    async with tenant_conn(tenant_id) as conn:
        return await conn.fetch(sql, *args)


async def fetch_one(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> asyncpg.Record | None:
    """Fetch a single row from a query against a tenant's schema."""
    async with tenant_conn(tenant_id) as conn:
        return await conn.fetchrow(sql, *args)
