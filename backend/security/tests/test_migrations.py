"""Tests for MeetMind AI - Database Migration Runner."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_security.migrations import (
    MigrationChecksumMismatchError,
    MigrationError,
    MigrationPendingError,
    apply_migrations_conn,
    ensure_schema_and_tracking_table,
    ensure_schema_current,
    load_migration_files,
)


class TestMigrationFileLoading:
    def test_load_migration_files_finds_initial_schema(self):
        migrations = load_migration_files()
        assert len(migrations) >= 1
        mig1 = migrations[0]
        assert mig1.version == 1
        assert mig1.name == "initial_schema"
        assert len(mig1.checksum) == 64
        assert "CREATE TABLE IF NOT EXISTS audit_events" in mig1.sql
        assert "CREATE TABLE IF NOT EXISTS consent_events" in mig1.sql
        assert "prevent_audit_modification" in mig1.sql

    def test_duplicate_version_raises_migration_error(self, tmp_path):
        f1 = tmp_path / "001_first.sql"
        f1.write_text("SELECT 1;", encoding="utf-8")
        f2 = tmp_path / "001_second.sql"
        f2.write_text("SELECT 2;", encoding="utf-8")

        with pytest.raises(MigrationError, match="Duplicate migration version 1"):
            load_migration_files(tmp_path)


class TestMigrationExecutionLogic:
    @pytest.mark.asyncio
    async def test_ensure_schema_and_tracking_table(self):
        mock_conn = AsyncMock()
        await ensure_schema_and_tracking_table(mock_conn, "tenant_test")
        assert mock_conn.execute.call_count == 2
        calls = [c[0][0] for c in mock_conn.execute.call_args_list]
        assert 'CREATE SCHEMA IF NOT EXISTS "tenant_test"' in calls[0]
        assert "_schema_migrations" in calls[1]

    @pytest.mark.asyncio
    async def test_apply_migrations_applies_unapplied(self, tmp_path):
        mig_sql = "CREATE TABLE test_table (id INT);"
        mig_file = tmp_path / "001_test.sql"
        mig_file.write_text(mig_sql, encoding="utf-8")

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []  # No migrations applied yet

        tx_context = MagicMock()
        tx_context.__aenter__ = AsyncMock(return_value=tx_context)
        tx_context.__aexit__ = AsyncMock(return_value=None)
        mock_conn.transaction = MagicMock(return_value=tx_context)

        applied = await apply_migrations_conn(mock_conn, "tenant_acme", migrations_dir=tmp_path)
        assert applied == [1]

        # Verify SQL statements executed
        exec_calls = [c[0][0] for c in mock_conn.execute.call_args_list]
        assert 'SET LOCAL search_path = "tenant_acme"' in exec_calls
        assert mig_sql in exec_calls

    @pytest.mark.asyncio
    async def test_detects_checksum_mismatch(self, tmp_path):
        mig_sql = "CREATE TABLE test_table (id INT);"
        mig_file = tmp_path / "001_test.sql"
        mig_file.write_text(mig_sql, encoding="utf-8")

        mock_conn = AsyncMock()
        # Simulated previously recorded checksum with DIFFERENT hash
        mock_conn.fetch.return_value = [{"version": 1, "checksum": "tampered_checksum_hash"}]

        with pytest.raises(MigrationChecksumMismatchError, match="Checksum mismatch"):
            await apply_migrations_conn(mock_conn, "tenant_acme", migrations_dir=tmp_path)

    @pytest.mark.asyncio
    async def test_ensure_schema_current_raises_when_pending(self, tmp_path):
        mig_file = tmp_path / "001_test.sql"
        mig_file.write_text("SELECT 1;", encoding="utf-8")

        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []  # Version 0

        conn_context = MagicMock()
        conn_context.__aenter__ = AsyncMock(return_value=mock_conn)
        conn_context.__aexit__ = AsyncMock()
        mock_pool.acquire.return_value = conn_context

        with pytest.raises(MigrationPendingError, match="expected 1"):
            await ensure_schema_current(
                mock_pool, "tenant_acme", auto_migrate=False, migrations_dir=tmp_path
            )
