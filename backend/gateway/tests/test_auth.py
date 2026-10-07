"""Tests for MeetMind AI Gateway — Clerk JWT authentication."""

import time

import jwt as pyjwt
import pytest
from meetmind_gateway.auth import AuthenticatedUser, AuthError, ClerkAuthenticator


class TestClerkAuthenticator:
    @pytest.fixture
    def auth(self):
        return ClerkAuthenticator(
            clerk_secret_key="sk_test_secret_for_testing",
            default_tenant_id="test_tenant",
        )

    async def test_empty_token_raises(self, auth):
        with pytest.raises(AuthError, match="No authentication token"):
            await auth.authenticate("")

    async def test_none_token_raises(self, auth):
        with pytest.raises(AuthError, match="No authentication token"):
            await auth.authenticate("")

    async def test_valid_hs256_token(self, auth):
        """Test authentication with a valid HS256 JWT."""
        payload = {
            "sub": "user_123",
            "sid": "sess_456",
            "org_id": "org_acme",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = pyjwt.encode(payload, "sk_test_secret_for_testing", algorithm="HS256")
        user = await auth.authenticate(token)
        assert user.user_id == "user_123"
        assert user.session_id == "sess_456"
        assert user.tenant_id == "org_acme"

    async def test_default_tenant_when_no_org_id(self, auth):
        """When no org_id claim, use the default tenant."""
        payload = {
            "sub": "user_solo",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = pyjwt.encode(payload, "sk_test_secret_for_testing", algorithm="HS256")
        user = await auth.authenticate(token)
        assert user.tenant_id == "test_tenant"

    async def test_expired_token_raises(self, auth):
        """Expired JWTs should raise AuthError."""
        payload = {
            "sub": "user_123",
            "exp": int(time.time()) - 3600,
            "iat": int(time.time()) - 7200,
        }
        token = pyjwt.encode(payload, "sk_test_secret_for_testing", algorithm="HS256")
        with pytest.raises(AuthError, match="expired"):
            await auth.authenticate(token)

    async def test_wrong_secret_raises(self, auth):
        """Token signed with wrong key should raise AuthError."""
        payload = {
            "sub": "user_123",
            "exp": int(time.time()) + 3600,
        }
        token = pyjwt.encode(payload, "wrong_secret", algorithm="HS256")
        with pytest.raises(AuthError):
            await auth.authenticate(token)

    async def test_malformed_token_raises(self, auth):
        """Garbage string should raise AuthError."""
        with pytest.raises(AuthError):
            await auth.authenticate("not.a.valid.jwt")

    async def test_missing_sub_claim_raises(self, auth):
        """Token without 'sub' claim should raise AuthError."""
        payload = {
            "email": "test@example.com",
            "exp": int(time.time()) + 3600,
        }
        token = pyjwt.encode(payload, "sk_test_secret_for_testing", algorithm="HS256")
        with pytest.raises(AuthError, match="sub"):
            await auth.authenticate(token)

    def test_authenticated_user_frozen(self):
        """AuthenticatedUser should be immutable."""
        user = AuthenticatedUser(user_id="u", tenant_id="t", session_id="s", claims={})
        with pytest.raises(AttributeError):
            user.user_id = "tampered"
