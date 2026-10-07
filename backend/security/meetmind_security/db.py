"""MeetMind AI - Async PostgreSQL Connection Pool Management.

Connects to Neon (managed serverless Postgres) via asyncpg with tenant-aware
connection pools. Each tenant resolves to a PostgreSQL schema through the
TenantRouter.

DEV/PROD: Both environments use Neon. The dev environment connects to a "dev"
branch, production to a "prod" branch -- both within the same Neon project, so
schema migrations are traceable across both.

Usage::

    pool = await get_pool("default")
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")
"""

from __future__ import annotations

import os
import ssl
from typing import Any

import asyncpg

from .tenant import TenantRouter

# -- Module-level state -------------------------------------------------------

_router = TenantRouter()
_pools: dict[str, asyncpg.Pool] = {}

# Neon requires TLS. There is no localhost fallback.
# Fail fast if the env var is missing so misconfigurations surface immediately.
NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")


def _require_neon_url() -> str:
    """Return the Neon connection URL or raise a clear error."""
    if not NEON_DATABASE_URL:
        raise RuntimeError(
            "NEON_DATABASE_URL is not set. "
            "Set it in .env (see .env.example) to your Neon connection string, e.g.:\n"
            "  NEON_DATABASE_URL=postgresql://meetmind:<password>@<endpoint>.neon.tech/meetmind_ai?sslmode=require"
        )
    return NEON_DATABASE_URL


def _create_ssl_context() -> ssl.SSLContext:
    """Create an SSL context for Neon connections (TLS required)."""
    ctx = ssl.create_default_context()
    # Neon uses valid certificates -- verify them.
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def get_pool(tenant_id: str = "default") -> asyncpg.Pool:
    """Return (or create) the asyncpg connection pool for a tenant.

    On first call for a given tenant, creates the pool and ensures the
    tenant's schema exists.
    """
    if tenant_id in _pools:
        return _pools[tenant_id]

    schema = _router.pg_schema(tenant_id)
    dsn = _require_neon_url()

    pool = await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=10,
        ssl=_create_ssl_context(),
        server_settings={"search_path": schema},
    )

    # Ensure the tenant schema exists
    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    _pools[tenant_id] = pool
    return pool


async def close_all_pools() -> None:
    """Gracefully close all connection pools. Call on shutdown."""
    for pool in _pools.values():
        await pool.close()
    _pools.clear()


async def execute_raw(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> str:
    """Execute a raw SQL statement against a tenant's schema.

    Returns the command status string (e.g., 'CREATE TABLE').
    """
    pool = await get_pool(tenant_id)
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


async def fetch_all(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> list[asyncpg.Record]:
    """Fetch all rows from a query against a tenant's schema."""
    pool = await get_pool(tenant_id)
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetch_one(
    sql: str,
    *args: Any,
    tenant_id: str = "default",
) -> asyncpg.Record | None:
    """Fetch a single row from a query against a tenant's schema."""
    pool = await get_pool(tenant_id)
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)
