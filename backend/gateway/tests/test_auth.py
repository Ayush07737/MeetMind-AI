"""Tests for MeetMind AI Gateway - Clerk RS256 JWT Authentication & Tenant Mapping.

Covers:
- RS256 asymmetric verification via JWKS HTTP server
- Expiration, nbf with 5s leeway, issuer, and azp origin validation
- Unknown kid rejection & key rotation
- Strict rejection of HS256 and 'none' algorithms
- Tenant mapping and enforcement across dev vs production modes
"""

import http.server
import json
import threading
import time
from typing import Any

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from meetmind_gateway.auth import (
    AuthenticatedUser,
    AuthForbiddenError,
    AuthUnauthorizedError,
    ClerkAuthenticator,
)


class MockJWKSServer:
    """Threaded local HTTP server serving JWKS json."""

    def __init__(self) -> None:
        self.keys: list[dict[str, Any]] = []
        handler = self._make_handler()
        self.server = http.server.HTTPServer(("127.0.0.1", 0), handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def _make_handler(outer):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                payload = json.dumps({"keys": outer.keys})
                self.wfile.write(payload.encode("utf-8"))

            def log_message(self, format, *args):
                pass

        return Handler

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture(scope="module")
def rsa_keys():
    """Generate two RS256 keypairs for signing and rotation testing."""
    priv1 = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk1 = RSAAlgorithm.to_jwk(priv1.public_key(), as_dict=True)
    jwk1.update({"kid": "key-1", "use": "sig", "alg": "RS256"})

    priv2 = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk2 = RSAAlgorithm.to_jwk(priv2.public_key(), as_dict=True)
    jwk2.update({"kid": "key-2", "use": "sig", "alg": "RS256"})

    return {"priv1": priv1, "jwk1": jwk1, "priv2": priv2, "jwk2": jwk2}


@pytest.fixture(scope="module")
def jwks_server(rsa_keys):
    server = MockJWKSServer()
    server.keys = [rsa_keys["jwk1"]]
    yield server
    server.shutdown()


@pytest.fixture
def auth(jwks_server):
    return ClerkAuthenticator(
        jwks_url=f"http://127.0.0.1:{jwks_server.port}/.well-known/jwks.json",
        default_tenant_id="test_tenant",
        app_env="development",
    )


class TestClerkAuthenticatorRS256:
    async def test_empty_token_raises(self, auth):
        with pytest.raises(AuthUnauthorizedError, match="No authentication token"):
            await auth.authenticate("")

    async def test_none_token_raises(self, auth):
        with pytest.raises(AuthUnauthorizedError, match="No authentication token"):
            await auth.authenticate(None)  # type: ignore[arg-type]

    async def test_valid_rs256_token(self, auth, rsa_keys):
        """Valid RS256 token authenticated via JWKS."""
        payload = {
            "sub": "user_123",
            "sid": "sess_456",
            "org_id": "org_acme",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        user = await auth.authenticate(token)
        assert user.user_id == "user_123"
        assert user.session_id == "sess_456"
        assert user.tenant_id == "org_acme"

    async def test_default_tenant_in_dev_when_no_org_id(self, auth, rsa_keys):
        """When no org_id claim, development mode allows default_tenant_id."""
        payload = {
            "sub": "user_solo",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        user = await auth.authenticate(token)
        assert user.tenant_id == "test_tenant"

    async def test_expired_token_raises(self, auth, rsa_keys):
        """Expired JWT raises AuthUnauthorizedError."""
        payload = {
            "sub": "user_123",
            "exp": int(time.time()) - 3600,
            "iat": int(time.time()) - 7200,
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthUnauthorizedError, match="expired"):
            await auth.authenticate(token)

    async def test_future_nbf_token_raises(self, auth, rsa_keys):
        """Token with nbf > 5s in future raises AuthUnauthorizedError."""
        payload = {
            "sub": "user_123",
            "nbf": int(time.time()) + 100,
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthUnauthorizedError, match="not yet valid"):
            await auth.authenticate(token)

    async def test_wrong_issuer_raises(self, jwks_server, rsa_keys):
        """Token with mismatching issuer raises AuthUnauthorizedError."""
        auth_with_iss = ClerkAuthenticator(
            jwks_url=f"http://127.0.0.1:{jwks_server.port}/.well-known/jwks.json",
            expected_issuer="https://clerk.meetmind.ai",
        )
        payload = {
            "sub": "user_123",
            "iss": "https://attacker.clerk.com",
            "exp": int(time.time()) + 3600,
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthUnauthorizedError, match="Invalid issuer"):
            await auth_with_iss.authenticate(token)

    async def test_wrong_azp_origin_raises(self, jwks_server, rsa_keys):
        """Token with azp not in allowed_origins raises AuthUnauthorizedError."""
        auth_with_origins = ClerkAuthenticator(
            jwks_url=f"http://127.0.0.1:{jwks_server.port}/.well-known/jwks.json",
            allowed_origins=["https://app.meetmind.ai"],
        )
        payload = {
            "sub": "user_123",
            "azp": "https://evil-site.com",
            "exp": int(time.time()) + 3600,
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthUnauthorizedError, match="Invalid authorized party"):
            await auth_with_origins.authenticate(token)

    async def test_unknown_kid_raises(self, auth, rsa_keys):
        """Token signed with kid not present in JWKS raises AuthUnauthorizedError."""
        payload = {
            "sub": "user_123",
            "exp": int(time.time()) + 3600,
        }
        token = pyjwt.encode(
            payload,
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "unknown-kid-999"},
        )
        with pytest.raises(AuthUnauthorizedError):
            await auth.authenticate(token)

    async def test_key_rotation_refreshes_and_validates(self, jwks_server, rsa_keys):
        """When a new key is added to JWKS, authenticator verifies token with rotated key."""
        # Add key 2 to JWKS server
        jwks_server.keys = [rsa_keys["jwk1"], rsa_keys["jwk2"]]

        auth_rot = ClerkAuthenticator(
            jwks_url=f"http://127.0.0.1:{jwks_server.port}/.well-known/jwks.json",
        )
        token2 = pyjwt.encode(
            {"sub": "user_rot", "exp": int(time.time()) + 3600},
            rsa_keys["priv2"],
            algorithm="RS256",
            headers={"kid": "key-2"},
        )
        user = await auth_rot.authenticate(token2)
        assert user.user_id == "user_rot"

    async def test_hs256_token_strictly_rejected(self, auth):
        """Tokens signed with HS256 must be rejected."""
        payload = {"sub": "user_123", "exp": int(time.time()) + 3600}
        token = pyjwt.encode(
            payload, "a_secure_hmac_secret_key_that_is_at_least_32_bytes_long!", algorithm="HS256"
        )
        with pytest.raises(AuthUnauthorizedError):
            await auth.authenticate(token)

    async def test_alg_none_token_strictly_rejected(self, auth):
        """Tokens with alg 'none' must be rejected."""
        token = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiIxMjMiLCJleHAiOjIwMDAwMDAwMDB9."
        with pytest.raises(AuthUnauthorizedError):
            await auth.authenticate(token)

    async def test_tenant_mapping_production_enforcement(self, jwks_server, rsa_keys):
        """In production, unmapped org_id and missing org_id raise AuthForbiddenError (4403)."""
        auth_prod = ClerkAuthenticator(
            jwks_url=f"http://127.0.0.1:{jwks_server.port}/.well-known/jwks.json",
            tenant_org_map={"org_valid": "tenant_prod_1"},
            app_env="production",
        )

        # 1. Valid mapped org
        token_valid = pyjwt.encode(
            {"sub": "user_1", "org_id": "org_valid", "exp": int(time.time()) + 3600},
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        user = await auth_prod.authenticate(token_valid)
        assert user.tenant_id == "tenant_prod_1"

        # 2. Unmapped org in prod
        token_unmapped = pyjwt.encode(
            {"sub": "user_2", "org_id": "org_unmapped", "exp": int(time.time()) + 3600},
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthForbiddenError, match="not mapped"):
            await auth_prod.authenticate(token_unmapped)

        # 3. Missing org in prod
        token_no_org = pyjwt.encode(
            {"sub": "user_3", "exp": int(time.time()) + 3600},
            rsa_keys["priv1"],
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(AuthForbiddenError, match="missing required organization"):
            await auth_prod.authenticate(token_no_org)

    def test_authenticated_user_frozen(self):
        """AuthenticatedUser should be immutable."""
        user = AuthenticatedUser(user_id="u", tenant_id="t", session_id="s", claims={})
        with pytest.raises(AttributeError):
            user.user_id = "tampered"  # type: ignore[misc]
