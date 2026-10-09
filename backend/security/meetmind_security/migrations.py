"""MeetMind AI - Lightweight Database Migration Runner.

Manages versioned plain-SQL migrations per tenant schema.
Tracks applied versions in ``<schema>._schema_migrations`` with SHA-256 checksums.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)

MIGRATION_FILE_PATTERN = re.compile(r"^(\d+)_(.+)\.sql$")
DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


class MigrationError(Exception):
    """Base exception for database migrations."""


class MigrationChecksumMismatchError(MigrationError):
    """Raised when an already applied migration has a modified checksum."""


class MigrationPendingError(MigrationError):
    """Raised when database schema is not at the expected migration version."""


@dataclass(frozen=True)
class MigrationFile:
    """A versioned migration file."""

    version: int
    name: str
    path: Path
    checksum: str
    sql: str


def load_migration_files(migrations_dir: Path | None = None) -> list[MigrationFile]:
    """Load and validate all SQL migration files sorted by version."""
    dir_path = migrations_dir or DEFAULT_MIGRATIONS_DIR
    if not dir_path.is_dir():
        return []

    migrations: list[MigrationFile] = []
    versions_seen: set[int] = set()

    for file_path in sorted(dir_path.glob("*.sql")):
        match = MIGRATION_FILE_PATTERN.match(file_path.name)
        if not match:
            continue

        version = int(match.group(1))
        name = match.group(2)

        if version in versions_seen:
            raise MigrationError(f"Duplicate migration version {version}: {file_path.name}")
        versions_seen.add(version)

        raw_sql = file_path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(raw_sql.encode("utf-8")).hexdigest()

        migrations.append(
            MigrationFile(
                version=version,
                name=name,
                path=file_path,
                checksum=checksum,
                sql=raw_sql,
            )
        )

    migrations.sort(key=lambda m: m.version)
    return migrations


async def ensure_schema_and_tracking_table(
    conn: asyncpg.Connection,
    schema: str,
) -> None:
    """Ensure the target schema and tracking table exist."""
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."_schema_migrations" (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


async def get_applied_migrations(
    conn: asyncpg.Connection,
    schema: str,
) -> dict[int, str]:
    """Return map of {version: checksum} for migrations already applied."""
    await ensure_schema_and_tracking_table(conn, schema)
    rows = await conn.fetch(
        f'SELECT version, checksum FROM "{schema}"."_schema_migrations" ORDER BY version ASC'
    )
    return {row["version"]: row["checksum"] for row in rows}


def get_migration_dsn() -> str:
    """Return the direct owner DSN for running migrations.

    Ensures that a direct endpoint (no '-pooler') is used as required by CA-2.
    """
    import os

    from meetmind_security.db import get_owner_dsn

    dsn = get_owner_dsn()
    if "-pooler" in dsn:
        direct = os.environ.get("NEON_DIRECT_DATABASE_URL")
        if direct and "-pooler" not in direct:
            return direct
        direct_candidate = dsn.replace("-pooler", "")
        logger.warning(
            "Found '-pooler' in migration DSN. Migrations require direct endpoint; "
            "converting to direct endpoint host."
        )
        return direct_candidate
    return dsn


async def apply_migrations_conn(
    conn: asyncpg.Connection,
    schema: str,
    migrations_dir: Path | None = None,
) -> list[int]:
    """Apply all pending migrations to the schema on an existing connection.

    Runs within a transaction. Verifies checksums of previously applied migrations.
    Uses transaction-scoped advisory lock (pg_advisory_xact_lock) for safe concurrency.
    """
    applied = await get_applied_migrations(conn, schema)
    all_migrations = load_migration_files(migrations_dir)
    newly_applied: list[int] = []

    # 1. Verify existing migrations have not been tampered with
    for mig in all_migrations:
        if mig.version in applied:
            recorded_checksum = applied[mig.version]
            if recorded_checksum != mig.checksum:
                raise MigrationChecksumMismatchError(
                    f"Checksum mismatch for migration {mig.version}_{mig.name} "
                    f"in schema '{schema}': "
                    f"expected {recorded_checksum}, found {mig.checksum}"
                )

    # 2. Apply pending migrations sequentially within transaction and advisory lock
    tx = conn.transaction()
    if hasattr(tx, "__aenter__"):
        async with tx:
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext('migration_' || $1))", schema)
            for mig in all_migrations:
                if mig.version not in applied:
                    logger.info(
                        "Applying migration %s_%s to schema %s",
                        mig.version,
                        mig.name,
                        schema,
                    )
                    await conn.execute(f'SET LOCAL search_path = "{schema}"')
                    await conn.execute(mig.sql)
                    await conn.execute(
                        f"""
                        INSERT INTO "{schema}"."_schema_migrations" (version, name, checksum)
                        VALUES ($1, $2, $3)
                        """,
                        mig.version,
                        mig.name,
                        mig.checksum,
                    )
                    newly_applied.append(mig.version)
    else:
        for mig in all_migrations:
            if mig.version not in applied:
                logger.info("Applying migration %s_%s to schema %s", mig.version, mig.name, schema)
                await conn.execute(f'SET LOCAL search_path = "{schema}"')
                await conn.execute(mig.sql)
                await conn.execute(
                    f"""
                    INSERT INTO "{schema}"."_schema_migrations" (version, name, checksum)
                    VALUES ($1, $2, $3)
                    """,
                    mig.version,
                    mig.name,
                    mig.checksum,
                )
                newly_applied.append(mig.version)

    # Grant app role (SELECT + INSERT only) access to the tenant schema tables
    with contextlib.suppress(Exception):
        await conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO meetmind_app')
        await conn.execute(
            f'GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA "{schema}" TO meetmind_app'
        )
        await conn.execute(
            f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "{schema}" TO meetmind_app'
        )

    return newly_applied


async def apply_migrations(
    pool: asyncpg.Pool,
    schema: str,
    migrations_dir: Path | None = None,
) -> list[int]:
    """Acquire connection from pool and apply pending migrations."""
    async with pool.acquire() as conn:
        return await apply_migrations_conn(conn, schema, migrations_dir)


async def get_latest_applied_version(
    conn: asyncpg.Connection,
    schema: str,
) -> int:
    """Return highest applied version number, or 0 if none."""
    applied = await get_applied_migrations(conn, schema)
    return max(applied.keys(), default=0)


async def verify_schema_version(
    conn: asyncpg.Connection,
    schema: str,
    expected_version: int,
) -> bool:
    """Verify that the schema is at the expected migration version."""
    latest = await get_latest_applied_version(conn, schema)
    return latest == expected_version


async def ensure_schema_current(
    pool: asyncpg.Pool,
    schema: str,
    auto_migrate: bool = False,
    migrations_dir: Path | None = None,
) -> int:
    """Ensure the schema is current with available migration files.

    If auto_migrate is True, pending migrations will be applied.
    Otherwise, raises MigrationPendingError if schema is out of date.
    """
    migrations = load_migration_files(migrations_dir)
    target_version = migrations[-1].version if migrations else 0

    async with pool.acquire() as conn:
        current_version = await get_latest_applied_version(conn, schema)

    if current_version < target_version:
        if auto_migrate:
            applied = await apply_migrations(pool, schema, migrations_dir)
            logger.info("Auto-applied migrations %s to schema %s", applied, schema)
            return target_version
        raise MigrationPendingError(
            f"Schema '{schema}' is at version {current_version}, but expected {target_version}. "
            "Run migrations or set RUN_MIGRATIONS_ON_STARTUP=true."
        )

    return current_version
