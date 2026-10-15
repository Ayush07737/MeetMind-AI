"""Unit tests for lazy tenant provisioning and 10s negative cache (P0-2).

Verifies:
1. Unknown tenant checks information_schema.schemata on miss.
2. If not yet provisioned, returns 503 and enters negative cache.
3. Second request within 10s fails fast with 503 without DB round-trip.
4. After 10s cooldown, re-checks information_schema.schemata.
5. Once schema is created in DB, request succeeds (201 Created) and enters
   verified_schemas fast path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from meetmind_gateway.app import create_app
from meetmind_gateway.auth import AuthenticatedUser
from meetmind_gateway.config import GatewaySettings


def test_lazy_tenant_provisioning_and_negative_cache() -> None:
    settings = GatewaySettings(
        app_env="development",
        default_tenant_id="default",
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)

        # Mock authenticated user with a dynamically provisioned tenant
        tenant_new = "tenant_dyn_999"
        user = AuthenticatedUser(
            user_id="user_dyn_1",
            tenant_id=tenant_new,
            session_id="sess_dyn_1",
            claims={"o": {"id": "org_dyn", "rol": "org:admin"}},
            role="org:admin",
        )
        app.state.authenticator = AsyncMock()
        app.state.authenticator.authenticate.return_value = user

        # Mock session manager
        app.state.session_manager = AsyncMock()
        app.state.session_manager.create_meeting.return_value = {
            "meeting_id": "mtg_dyn_001",
            "tenant_id": tenant_new,
            "status": "pending_consent",
        }

        # Mock db_pool
        mock_pool = AsyncMock()
        app.state.db_pool = mock_pool

        # Initially, verified_schemas only has default
        app.state.verified_schemas = {"tenant_default"}
        app.state.negative_schema_cache = {}

        client = TestClient(app)

        # Step 1: Schema does not exist in DB yet
        mock_pool.fetchval.return_value = False

        mono_time = 1000.0
        with patch("time.monotonic", return_value=mono_time):
            res1 = client.post(
                "/v1/meetings",
                json={"meeting_id": "mtg_dyn_001"},
                headers={"Authorization": "Bearer token"},
            )
            assert res1.status_code == 503
            assert "is not provisioned" in res1.json()["detail"]
            assert mock_pool.fetchval.call_count == 1
            assert f"tenant_{tenant_new}" in app.state.negative_schema_cache

        # Step 2: Request 2 seconds later (within 10s cooldown)
        # Even if schema is now in DB, negative cache should block DB query
        mock_pool.fetchval.return_value = True
        with patch("time.monotonic", return_value=mono_time + 2.0):
            res2 = client.post(
                "/v1/meetings",
                json={"meeting_id": "mtg_dyn_001"},
                headers={"Authorization": "Bearer token"},
            )
            assert res2.status_code == 503
            # mock_pool.fetchval should NOT have been called again!
            assert mock_pool.fetchval.call_count == 1

        # Step 3: 11 seconds later (cooldown expired), schema is now provisioned in DB
        with patch("time.monotonic", return_value=mono_time + 11.0):
            res3 = client.post(
                "/v1/meetings",
                json={"meeting_id": "mtg_dyn_001"},
                headers={"Authorization": "Bearer token"},
            )
            assert res3.status_code == 201
            assert mock_pool.fetchval.call_count == 2
            assert f"tenant_{tenant_new}" in app.state.verified_schemas
            assert f"tenant_{tenant_new}" not in app.state.negative_schema_cache

        # Step 4: Subsequent request hits fast path (no DB query)
        with patch("time.monotonic", return_value=mono_time + 12.0):
            res4 = client.post(
                "/v1/meetings",
                json={"meeting_id": "mtg_dyn_002"},
                headers={"Authorization": "Bearer token"},
            )
            assert res4.status_code == 201
            # Still 2 calls; fast path used
            assert mock_pool.fetchval.call_count == 2
