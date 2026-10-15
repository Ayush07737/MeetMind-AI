"""Unit tests for Gateway System Benchmark Endpoints security (P0-1).

Tests:
1. Disabled by default: returns 404 Not Found.
2. Production environment forces them off: returns 404 even if GATEWAY_BENCH_ENDPOINTS=true.
3. Enabled path without admin credentials: returns 401 Unauthorized.
4. Enabled path with non-admin Clerk token: returns 403 Forbidden.
5. Enabled path with valid X-Admin-Key header: returns 200 OK.
6. Enabled path with valid Clerk admin token: returns 200 OK.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from meetmind_gateway.app import create_app
from meetmind_gateway.auth import AuthenticatedUser
from meetmind_gateway.config import GatewaySettings


def test_bench_endpoints_disabled_by_default() -> None:
    settings = GatewaySettings(
        app_env="development",
        gateway_bench_endpoints=False,
        gateway_admin_key="supersecret",
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)
        client = TestClient(app)

        res_post = client.post(
            "/v1/system/mark-steady",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_post.status_code == 404

        res_get = client.get(
            "/v1/system/metrics",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_get.status_code == 404


def test_bench_endpoints_production_forces_off() -> None:
    settings = GatewaySettings(
        app_env="production",
        clerk_jwks_url="https://clerk.meetmind.ai/.well-known/jwks.json",
        clerk_issuer="https://clerk.meetmind.ai",
        allowed_origins=["https://meetmind.ai"],
        gateway_bench_endpoints=True,
        gateway_admin_key="supersecret",
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)
        client = TestClient(app)

        res_post = client.post(
            "/v1/system/mark-steady",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_post.status_code == 404

        res_get = client.get(
            "/v1/system/metrics",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_get.status_code == 404


def test_bench_endpoints_unauthenticated_or_invalid_key() -> None:
    settings = GatewaySettings(
        app_env="development",
        gateway_bench_endpoints=True,
        gateway_admin_key="supersecret",
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)
        client = TestClient(app)

        # No auth
        assert client.post("/v1/system/mark-steady").status_code == 401
        assert client.get("/v1/system/metrics").status_code == 401

        # Wrong secret
        assert (
            client.post(
                "/v1/system/mark-steady",
                headers={"X-Admin-Key": "wrong"},
            ).status_code
            == 401
        )


def test_bench_endpoints_authenticated_with_admin_key() -> None:
    settings = GatewaySettings(
        app_env="development",
        gateway_bench_endpoints=True,
        gateway_admin_key="supersecret",
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)
        client = TestClient(app)

        res_post = client.post(
            "/v1/system/mark-steady",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_post.status_code == 200
        assert res_post.json() == {"status": "ok", "state": "steady"}

        res_get = client.get(
            "/v1/system/metrics",
            headers={"X-Admin-Key": "supersecret"},
        )
        assert res_get.status_code == 200
        assert "lag" in res_get.json()


def test_bench_endpoints_clerk_admin_role() -> None:
    settings = GatewaySettings(
        app_env="development",
        gateway_bench_endpoints=True,
        elevated_listen_roles=["org:admin"],
    )
    with patch("meetmind_gateway.app.get_settings", return_value=settings):
        app = create_app(settings)
        # Mock authenticator
        admin_user = AuthenticatedUser(
            user_id="user_admin",
            tenant_id="default",
            session_id="sess_admin",
            claims={"o": {"id": "org_1", "rol": "org:admin"}},
            role="org:admin",
        )
        member_user = AuthenticatedUser(
            user_id="user_member",
            tenant_id="default",
            session_id="sess_member",
            claims={"o": {"id": "org_1", "rol": "org:member"}},
            role="org:member",
        )

        app.state.authenticator = MagicMock()
        app.state.authenticator.authenticate = AsyncMock()
        app.state.authenticator.get_stats.return_value = {"cache_hits": 0, "cache_misses": 0}
        app.state.authenticator.warm_jwt_times_ms = []

        client = TestClient(app)

        # Non-admin role -> 403 Forbidden
        app.state.authenticator.authenticate.return_value = member_user
        res_forbidden = client.get(
            "/v1/system/metrics",
            headers={"Authorization": "Bearer member_token"},
        )
        assert res_forbidden.status_code == 403

        # Admin role -> 200 OK
        app.state.authenticator.authenticate.return_value = admin_user
        res_ok = client.get(
            "/v1/system/metrics",
            headers={"Authorization": "Bearer admin_token"},
        )
        assert res_ok.status_code == 200
