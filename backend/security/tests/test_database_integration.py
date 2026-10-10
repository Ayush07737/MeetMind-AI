"""Integration tests for MeetMind AI Database Layer & Audit Immutability.

Tests run against a live PostgreSQL / Neon instance and verify:
1. Migration execution and idempotence
2. PostgreSQL trigger immutability (UPDATE/DELETE forbidden on audit_events and consent_events)
3. Append-only consent lifecycle and latest-event-wins resolution
4. Concurrent query execution across tenants on the single shared pool
"""

import asyncio
import json
import uuid
from pathlib import Path

import asyncpg
import pytest
from dotenv import load_dotenv
from meetmind_security import db
from meetmind_security.audit_log import query_events_by_type, write_audit_event
from meetmind_security.consent import check_consent, record_consent, revoke_consent
from meetmind_security.migrations import apply_migrations, verify_schema_version
from meetmind_security.tenant import TenantConfig

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env")
pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]


@pytest.fixture
async def shared_db_pool():
    try:
        dsn = db.get_owner_dsn()
    except RuntimeError:
        pytest.skip("Owner DSN is not set")
    pool = await db.init_db(dsn, verify_privileges=False)
    yield pool
    await db.close_db()


@pytest.fixture
async def app_db_pool():
    try:
        dsn = db.get_app_dsn()
    except RuntimeError:
        pytest.skip("App DSN is not set")
    pool = await db._create_pool_with_retry(dsn)
    yield pool
    await pool.close()
    await pool.close()


@pytest.fixture
async def isolated_tenant(shared_db_pool):
    """Create an isolated test tenant and clean up schema after test."""
    unique_suffix = uuid.uuid4().hex[:8]
    tenant_id = f"test_{unique_suffix}"
    pg_schema = f"tenant_{tenant_id}"

    config = TenantConfig(
        tenant_id=tenant_id,
        pg_schema=pg_schema,
        neo4j_namespace=tenant_id,
        qdrant_prefix=tenant_id,
        redis_prefix=tenant_id,
    )
    # Register with the singleton router
    db._router.register(config)

    # Apply initial schema migration
    await apply_migrations(shared_db_pool, pg_schema)

    yield config

    # Teardown: drop test schema
    async with shared_db_pool.acquire() as conn:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{pg_schema}" CASCADE')


class TestDatabaseLayerIntegration:
    @pytest.mark.asyncio
    async def test_ca2_pooled_dsn_200_interleaved_operations_no_schema_leak(self, shared_db_pool):
        """CA-2: 200 interleaved operations across two tenants through the pooled DSN

        Proves that SET LOCAL search_path inside transactions never leaks across
        connections in the connection pool.
        """
        id_a = f"ca2_a_{uuid.uuid4().hex[:6]}"
        id_b = f"ca2_b_{uuid.uuid4().hex[:6]}"
        cfg_a = TenantConfig(
            tenant_id=id_a,
            pg_schema=f"tenant_{id_a}",
            neo4j_namespace=id_a,
            qdrant_prefix=id_a,
            redis_prefix=id_a,
        )
        cfg_b = TenantConfig(
            tenant_id=id_b,
            pg_schema=f"tenant_{id_b}",
            neo4j_namespace=id_b,
            qdrant_prefix=id_b,
            redis_prefix=id_b,
        )
        db._router.register(cfg_a)
        db._router.register(cfg_b)

        await apply_migrations(shared_db_pool, cfg_a.pg_schema)
        await apply_migrations(shared_db_pool, cfg_b.pg_schema)

        try:
            sem = asyncio.Semaphore(10)

            async def execute_tenant_op(tenant_cfg: TenantConfig, op_num: int):
                async with sem, db.tenant_conn(tenant_cfg.tenant_id) as conn:
                    current_schema = await conn.fetchval("SELECT current_schema()")
                    assert current_schema == tenant_cfg.pg_schema, (
                        f"Schema leak! Expected {tenant_cfg.pg_schema}, got {current_schema}"
                    )
                    await conn.execute(
                        """
                        INSERT INTO audit_events (tenant_id, user_id, event_type, payload)
                        VALUES ($1, $2, $3, $4::jsonb)
                        """,
                        tenant_cfg.tenant_id,
                        f"user_{op_num}",
                        "ca2_concurrency_op",
                        json.dumps({"op": op_num}),
                    )

            # 200 interleaved operations: 100 for A, 100 for B
            tasks = []
            for i in range(200):
                cfg = cfg_a if (i % 2 == 0) else cfg_b
                tasks.append(execute_tenant_op(cfg, i))

            await asyncio.gather(*tasks)

            # Assert each schema received exactly 100 events
            async with db.tenant_conn(cfg_a.tenant_id) as conn:
                count_a = await conn.fetchval("SELECT COUNT(*) FROM audit_events")
                assert count_a == 100

            async with db.tenant_conn(cfg_b.tenant_id) as conn:
                count_b = await conn.fetchval("SELECT COUNT(*) FROM audit_events")
                assert count_b == 100

        finally:
            async with shared_db_pool.acquire() as conn:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{cfg_a.pg_schema}" CASCADE')
                await conn.execute(f'DROP SCHEMA IF EXISTS "{cfg_b.pg_schema}" CASCADE')

    @pytest.mark.asyncio
    async def test_migration_runner_is_idempotent(self, shared_db_pool, isolated_tenant):
        schema = isolated_tenant.pg_schema
        # Verify schema is at version 1
        async with shared_db_pool.acquire() as conn:
            is_valid = await verify_schema_version(conn, schema, expected_version=2)
            assert is_valid is True

        # Re-running migrations should apply nothing
        newly_applied = await apply_migrations(shared_db_pool, schema)
        assert newly_applied == []

    @pytest.mark.asyncio
    async def test_audit_immutability_trigger(self, isolated_tenant):
        tenant_id = isolated_tenant.tenant_id
        # 1. Insert an audit event
        event = await write_audit_event(
            tenant_id=tenant_id,
            user_id="user_sec_1",
            event_type="test_immutable",
            payload={"key": "original"},
        )
        assert event.id is not None

        # 2. Attempt to UPDATE the row - must be rejected by trigger
        with pytest.raises(asyncpg.PostgresError, match="strictly append-only"):
            await db.execute_raw(
                "UPDATE audit_events SET payload = $1::jsonb WHERE id = $2",
                '{"hacked": true}',
                event.id,
                tenant_id=tenant_id,
            )

        # 3. Attempt to DELETE the row - must be rejected by trigger
        with pytest.raises(asyncpg.PostgresError, match="strictly append-only"):
            await db.execute_raw(
                "DELETE FROM audit_events WHERE id = $1",
                event.id,
                tenant_id=tenant_id,
            )

        # 4. Verify data was NOT modified
        events = await query_events_by_type(tenant_id, "test_immutable")
        assert len(events) == 1
        assert events[0].payload == {"key": "original"}

    @pytest.mark.asyncio
    async def test_consent_immutability_trigger(self, isolated_tenant):
        tenant_id = isolated_tenant.tenant_id
        # 1. Grant consent
        c_event = await record_consent(
            tenant_id=tenant_id,
            user_id="user_sec_2",
            meeting_id="mtg_sec_2",
            consent_type="audio_capture",
        )

        # 2. Attempt UPDATE - rejected by trigger
        with pytest.raises(asyncpg.PostgresError, match="strictly append-only"):
            await db.execute_raw(
                "UPDATE consent_events SET action = 'revoked' WHERE id = $1",
                c_event.id,
                tenant_id=tenant_id,
            )

        # 3. Attempt DELETE - rejected by trigger
        with pytest.raises(asyncpg.PostgresError, match="strictly append-only"):
            await db.execute_raw(
                "DELETE FROM consent_events WHERE id = $1",
                c_event.id,
                tenant_id=tenant_id,
            )

    @pytest.mark.asyncio
    async def test_append_only_consent_lifecycle(self, isolated_tenant):
        tenant_id = isolated_tenant.tenant_id
        user_id = "user_lifecycle_1"
        meeting_id = "mtg_lifecycle_1"

        # Initially no consent
        has_consent = await check_consent(
            tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id
        )
        assert has_consent is False

        # Grant consent
        await record_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id)
        assert (
            await check_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id) is True
        )

        # Revoke consent
        await revoke_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id)
        assert (
            await check_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id)
            is False
        )

        # Grant consent again
        await record_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id)
        assert (
            await check_consent(tenant_id=tenant_id, user_id=user_id, meeting_id=meeting_id) is True
        )

        # Verify all 3 events still exist in consent_events table
        lookup_sql = (
            "SELECT action FROM consent_events "
            "WHERE tenant_id = $1 AND user_id = $2 AND meeting_id = $3 "
            "ORDER BY created_at ASC"
        )
        rows = await db.fetch_all(
            lookup_sql,
            tenant_id,
            user_id,
            meeting_id,
            tenant_id=tenant_id,
        )
        assert len(rows) == 3
        assert [r["action"] for r in rows] == ["granted", "revoked", "granted"]

    @pytest.mark.asyncio
    async def test_shared_pool_concurrent_multi_tenancy(self, shared_db_pool):
        """Execute concurrent operations across 5 tenants on the shared pool."""
        tenants = []
        for _ in range(5):
            t_id = f"concurr_{uuid.uuid4().hex[:6]}"
            schema = f"tenant_{t_id}"
            config = TenantConfig(
                tenant_id=t_id,
                pg_schema=schema,
                neo4j_namespace=t_id,
                qdrant_prefix=t_id,
                redis_prefix=t_id,
            )
            db._router.register(config)
            await apply_migrations(shared_db_pool, schema)
            tenants.append(config)

        try:
            sem = asyncio.Semaphore(5)

            async def run_tenant_ops(config: TenantConfig, op_idx: int):
                async with sem:
                    await write_audit_event(
                        tenant_id=config.tenant_id,
                        user_id=f"user_{op_idx}",
                        event_type=f"evt_{op_idx}",
                        payload={"tenant": config.tenant_id, "op": op_idx},
                    )

            # Concurrent operations across the 5 tenants bounded by connection pool
            tasks = []
            for op in range(20):
                t_cfg = tenants[op % len(tenants)]
                tasks.append(run_tenant_ops(t_cfg, op))

            await asyncio.gather(*tasks)

            # Verify tenant data isolation
            for t_cfg in tenants:
                events = await query_events_by_type(t_cfg.tenant_id, f"evt_{tenants.index(t_cfg)}")
                for ev in events:
                    assert ev.tenant_id == t_cfg.tenant_id

        finally:
            async with shared_db_pool.acquire() as conn:
                for t_cfg in tenants:
                    await conn.execute(f'DROP SCHEMA IF EXISTS "{t_cfg.pg_schema}" CASCADE')

    @pytest.mark.asyncio
    async def test_app_role_insufficient_privileges_on_audit_and_consent_tables(
        self, app_db_pool, isolated_tenant
    ):
        """CO-4: Verify app role gets InsufficientPrivilegeError on UPDATE/DELETE/TRUNCATE."""
        tenant_id = isolated_tenant.tenant_id
        schema = isolated_tenant.pg_schema

        async with app_db_pool.acquire() as conn:
            # Verify connected as app role
            current_user = await conn.fetchval("SELECT current_user")
            assert current_user == "meetmind_app"

            await conn.execute(f'SET search_path = "{schema}"')

            # 1. INSERT into audit_events works
            await conn.execute(
                """
                INSERT INTO audit_events (tenant_id, user_id, event_type, payload)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                tenant_id,
                "user_app_test",
                "app_test_evt",
                '{"test": true}',
            )

            # 2. SELECT from audit_events works
            rows = await conn.fetch("SELECT * FROM audit_events WHERE tenant_id = $1", tenant_id)
            assert len(rows) >= 1
            evt_id = rows[0]["id"]

            # 3. UPDATE audit_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute(
                    """UPDATE audit_events SET payload = '{"hacked": true}'::jsonb WHERE id = $1""",
                    evt_id,
                )

            # 4. DELETE FROM audit_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute("DELETE FROM audit_events WHERE id = $1", evt_id)

            # 5. TRUNCATE audit_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute("TRUNCATE audit_events")

            # 6. INSERT into consent_events works
            await conn.execute(
                """
                INSERT INTO consent_events (tenant_id, user_id, meeting_id, consent_type, action)
                VALUES ($1, $2, $3, $4, $5)
                """,
                tenant_id,
                "user_app_test",
                "mtg_app_test",
                "audio_capture",
                "granted",
            )

            # 7. UPDATE consent_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute("UPDATE consent_events SET action = 'revoked'")

            # 8. DELETE FROM consent_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute("DELETE FROM consent_events")

            # 9. TRUNCATE consent_events -> raises InsufficientPrivilegeError
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute("TRUNCATE consent_events")

    @pytest.mark.asyncio
    async def test_schema_name_injection_rejected(self):
        """CO-4: Schema-name injection attempts must be rejected at validation boundary."""
        malicious_names = [
            "public; DROP TABLE audit_events; --",
            "tenant_1'; SELECT 1; --",
            "schema with spaces",
            "schema/with/slashes",
            "schema--comment",
            'schema"quote',
            "schema'quote",
            "a" * 65,
        ]
        for bad_name in malicious_names:
            with pytest.raises(ValueError):
                TenantConfig(
                    tenant_id="valid_id",
                    pg_schema=bad_name,
                    neo4j_namespace="ns",
                    qdrant_prefix="qd",
                    redis_prefix="rd",
                )

    @pytest.mark.asyncio
    async def test_tenant_a_cannot_read_tenant_b_rows(self, shared_db_pool):
        """CO-4: Tenant A cannot read Tenant B rows due to strict schema isolation."""
        t_a_id = f"tenanta_{uuid.uuid4().hex[:6]}"
        schema_a = f"tenant_{t_a_id}"
        cfg_a = TenantConfig(
            tenant_id=t_a_id,
            pg_schema=schema_a,
            neo4j_namespace=t_a_id,
            qdrant_prefix=t_a_id,
            redis_prefix=t_a_id,
        )
        db._router.register(cfg_a)
        await apply_migrations(shared_db_pool, schema_a)

        t_b_id = f"tenantb_{uuid.uuid4().hex[:6]}"
        schema_b = f"tenant_{t_b_id}"
        cfg_b = TenantConfig(
            tenant_id=t_b_id,
            pg_schema=schema_b,
            neo4j_namespace=t_b_id,
            qdrant_prefix=t_b_id,
            redis_prefix=t_b_id,
        )
        db._router.register(cfg_b)
        await apply_migrations(shared_db_pool, schema_b)

        try:
            # Write private audit event to Tenant B
            b_evt = await write_audit_event(
                tenant_id=t_b_id,
                user_id="user_b",
                event_type="secret_b_event",
                payload={"confidential": "tenant_b_secret_data"},
            )

            # Query from Tenant A -> 0 events
            a_events = await query_events_by_type(t_a_id, "secret_b_event")
            assert len(a_events) == 0

            # Direct query scoped to Tenant A's search_path for Tenant B's event ID returns None
            row = await db.fetch_one(
                "SELECT * FROM audit_events WHERE id = $1",
                b_evt.id,
                tenant_id=t_a_id,
            )
            assert row is None
        finally:
            async with shared_db_pool.acquire() as conn:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_a}" CASCADE')
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
