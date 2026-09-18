"""MeetMind AI — Async PostgreSQL Connection Pool Management.

Provides tenant-aware connection pools via asyncpg. Each tenant resolves to a
PostgreSQL schema through the TenantRouter.

Usage::

    pool = await get_pool("default")
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")
"""

from __future__ import annotations

import os
from typing import Any

import asyncpg

from .tenant import TenantRouter

# ── Module-level state ──────────────────────────────────────────────────────

_router = TenantRouter()
_pools: dict[str, asyncpg.Pool] = {}

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://meetmind:meetmind_dev@localhost:5432/meetmind_ai",
)


async def get_pool(tenant_id: str = "default") -> asyncpg.Pool:
    """Return (or create) the asyncpg connection pool for a tenant.

    On first call for a given tenant, creates the pool and ensures the
    tenant's schema exists.
    """
    if tenant_id in _pools:
        return _pools[tenant_id]

    schema = _router.pg_schema(tenant_id)

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
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
