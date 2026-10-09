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


def get_owner_dsn() -> str:
    """Return the owner DSN for migrations and DDL operations.

    Checks NEON_TEST_DATABASE_URL (for test runs), NEON_DIRECT_DATABASE_URL,
    and NEON_DATABASE_URL.
    """
    if NEON_DATABASE_URL is not _UNSET_SENTINEL and NEON_DATABASE_URL:
        return NEON_DATABASE_URL
    url = (
        os.environ.get("NEON_TEST_DATABASE_URL")
        or os.environ.get("NEON_DIRECT_DATABASE_URL")
        or os.environ.get("NEON_DATABASE_URL")
    )
    if not url:
        raise RuntimeError(
            "Owner database DSN is not set. "
            "Set NEON_DATABASE_URL, NEON_DIRECT_DATABASE_URL, or NEON_TEST_DATABASE_URL in .env"
        )
    return url


def get_app_dsn() -> str:
    """Return the runtime PostgreSQL connection URL for application services.

    Defaults to the restricted app role DSN (SELECT + INSERT only).
    Checks NEON_TEST_APP_DATABASE_URL (for test runs), NEON_APP_DATABASE_URL,
    falling back to NEON_DATABASE_URL.
    """
    if NEON_DATABASE_URL is not _UNSET_SENTINEL and NEON_DATABASE_URL:
        return NEON_DATABASE_URL
    url = (
        os.environ.get("NEON_TEST_APP_DATABASE_URL")
        or os.environ.get("NEON_APP_DATABASE_URL")
        or os.environ.get("NEON_DATABASE_URL")
    )
    if not url:
        raise RuntimeError(
            "NEON_DATABASE_URL is not set. "
            "Set it in .env to your Neon connection string, e.g.:\n"
            "  NEON_DATABASE_URL=postgresql://meetmind:<password>@<endpoint>.neon.tech/meetmind_ai?sslmode=require"
        )
    return url


# Backward-compatible aliases
get_neon_database_url = get_app_dsn
get_neon_owner_database_url = get_owner_dsn
_require_neon_url = get_app_dsn


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


async def verify_runtime_role_privileges(
    conn: asyncpg.Connection,
    schema: str = "tenant_default",
) -> str:
    """Verify that runtime role holds least-privilege (SELECT/INSERT only).

    Refuses startup if the runtime role holds UPDATE, DELETE, or TRUNCATE
    on append-only audit_events or consent_events tables.
    Returns the current user string.
    """
    current_user: str = await conn.fetchval("SELECT current_user")
    logger.info("Database runtime connected as user: %s", current_user)

    for table in ["audit_events", "consent_events"]:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            schema,
            table,
        )
        if not exists:
            continue

        for priv in ["UPDATE", "DELETE", "TRUNCATE"]:
            target_table = f'"{schema}"."{table}"'
            query = f"SELECT has_table_privilege(current_user, '{target_table}', '{priv}')"
            has_priv = await conn.fetchval(query)
            if has_priv:
                msg = (
                    f"Security violation: role '{current_user}' holds illegal privilege "
                    f"'{priv}' on table '{schema}.{table}'."
                )
                logger.critical(msg)
                raise RuntimeError(msg)

    return current_user


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


async def init_db(
    dsn: str | None = None,
    verify_privileges: bool = True,
    schema: str = "tenant_default",
) -> asyncpg.Pool:
    """Initialize the global shared connection pool using the app role DSN."""
    global _shared_pool
    async with _pool_lock:
        if _shared_pool is not None and not _shared_pool._closed:
            return _shared_pool

        connection_url = dsn or get_app_dsn()
        _shared_pool = await _create_pool_with_retry(connection_url)

        if verify_privileges:
            try:
                async with _shared_pool.acquire() as conn:
                    await verify_runtime_role_privileges(conn, schema=schema)
            except Exception:
                await _shared_pool.close()
                _shared_pool = None
                raise

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
