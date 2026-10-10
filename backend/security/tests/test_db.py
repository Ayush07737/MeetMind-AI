"""Tests for MeetMind AI - Neon Database Connection Pool (CA-1 & CA-2).

Verifies:
1. SSL context enforcement (TLS required for Neon)
2. Fail-fast error when owner / app DSNs are missing
3. Correct resolution of owner vs app DSNs
4. Privilege verification fails fast if runtime role holds UPDATE/DELETE/TRUNCATE
5. Privilege verification passes when role holds SELECT/INSERT only
"""

import ssl
from unittest.mock import AsyncMock, patch

import pytest
from meetmind_security.db import (
    _create_ssl_context,
    get_app_dsn,
    get_owner_dsn,
    verify_runtime_role_privileges,
)


class TestNeonDatabaseConfig:
    def test_create_ssl_context_enforces_tls(self):
        ctx = _create_ssl_context()
        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is True
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_get_app_dsn_raises_when_missing(self):
        with (
            patch("meetmind_security.db.NEON_DATABASE_URL", None),
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(RuntimeError, match="NEON_DATABASE_URL is not set"),
        ):
            get_app_dsn()

    def test_get_owner_dsn_raises_when_missing(self):
        with (
            patch("meetmind_security.db.NEON_DATABASE_URL", None),
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(RuntimeError, match="Owner database DSN is not set"),
        ):
            get_owner_dsn()

    def test_dsn_helpers_return_configured_urls(self):
        owner_url = "postgresql://owner:pwd@ep-direct.neon.tech/meetmind_ai?sslmode=require"
        app_url = "postgresql://app:pwd@ep-pooler.neon.tech/meetmind_ai?sslmode=require"
        with patch.dict(
            "os.environ",
            {"NEON_TEST_DATABASE_URL": owner_url, "NEON_TEST_APP_DATABASE_URL": app_url},
        ):
            assert get_owner_dsn() == owner_url
            assert get_app_dsn() == app_url


class TestRuntimePrivilegeVerification:
    @pytest.mark.asyncio
    async def test_refuses_startup_if_role_has_delete_privilege(self):
        conn = AsyncMock()
        conn.fetchval.side_effect = [
            "neondb_owner",  # SELECT current_user
            True,  # table audit_events exists
            False,  # UPDATE
            True,  # DELETE -> illegal!
        ]

        with pytest.raises(RuntimeError, match="holds illegal privilege 'DELETE'"):
            await verify_runtime_role_privileges(conn, schema="test_schema")

    @pytest.mark.asyncio
    async def test_refuses_startup_if_role_has_truncate_privilege(self):
        conn = AsyncMock()
        conn.fetchval.side_effect = [
            "admin_role",  # SELECT current_user
            True,  # table audit_events exists
            False,  # UPDATE
            False,  # DELETE
            True,  # TRUNCATE -> illegal!
        ]

        with pytest.raises(RuntimeError, match="holds illegal privilege 'TRUNCATE'"):
            await verify_runtime_role_privileges(conn, schema="test_schema")

    @pytest.mark.asyncio
    async def test_passes_when_role_holds_no_destructive_privileges(self):
        conn = AsyncMock()
        # Returns current_user, table1 exists, 3 privs False, table2 exists, 3 privs False
        conn.fetchval.side_effect = [
            "meetmind_app",
            True,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
        ]

        verified = await verify_runtime_role_privileges(conn, schema="test_schema")
        assert verified == "meetmind_app"
