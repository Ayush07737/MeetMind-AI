"""MeetMind AI - Clerk JWT authentication for WebSocket connections.

Validates Clerk-issued session JWTs using Clerk's JWKS endpoint. The JWT is
expected as a query parameter ``token`` on the WebSocket upgrade request.

Per §0: authentication is via Clerk -- no separate JWT_SECRET. The backend
fetches Clerk's public JWKS to verify token signatures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import jwt as pyjwt

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthenticatedUser:
    """Represents a validated Clerk user."""

    user_id: str
    tenant_id: str
    session_id: str
    claims: dict[str, Any]


class AuthError(Exception):
    """Raised when JWT validation fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ClerkAuthenticator:
    """Validates Clerk JWTs using JWKS.

    Usage::

        auth = ClerkAuthenticator(clerk_secret_key="sk_test_...")
        user = await auth.authenticate(token)
    """

    def __init__(
        self,
        clerk_secret_key: str,
        default_tenant_id: str = "default",
    ) -> None:
        self._secret_key = clerk_secret_key
        self._default_tenant_id = default_tenant_id
        self._jwks_client: pyjwt.PyJWKClient | None = None

    def _get_jwks_client(self) -> pyjwt.PyJWKClient:
        """Lazily create JWKS client pointing at Clerk's endpoint."""
        if self._jwks_client is None:
            # Clerk's JWKS endpoint is derived from the secret key's instance
            # For Clerk, the JWKS URL is: https://<clerk-frontend-api>/.well-known/jwks.json
            # But we can also use the secret key to verify directly.
            # We'll use PyJWT with the secret key for HMAC verification
            # since Clerk supports both RS256 and HS256 depending on config.
            self._jwks_client = pyjwt.PyJWKClient(
                uri=self._clerk_jwks_url(),
                cache_keys=True,
                lifespan=3600,
            )
        return self._jwks_client

    def _clerk_jwks_url(self) -> str:
        """Derive Clerk JWKS URL from secret key."""
        # Clerk frontend API domain is embedded in the publishable key,
        # but the standard JWKS endpoint for any Clerk instance is:
        # https://api.clerk.com/v1/jwks
        return "https://api.clerk.com/v1/jwks"

    async def authenticate(self, token: str) -> AuthenticatedUser:
        """Validate a Clerk JWT and return the authenticated user.

        Args:
            token: The raw JWT string from the WebSocket query parameter.

        Returns:
            AuthenticatedUser with user_id, tenant_id, session_id.

        Raises:
            AuthError: If the token is invalid, expired, or malformed.
        """
        if not token:
            raise AuthError("No authentication token provided")

        try:
            # Try JWKS-based verification first (RS256)
            try:
                jwks_client = self._get_jwks_client()
                signing_key = jwks_client.get_signing_key_from_jwt(token)
                claims = pyjwt.decode(
                    token,
                    signing_key.key,
                    algorithms=["RS256"],
                    options={"verify_aud": False},
                )
            except Exception:
                # Fallback: verify with secret key (HS256 dev mode)
                claims = pyjwt.decode(
                    token,
                    self._secret_key,
                    algorithms=["HS256"],
                    options={"verify_aud": False},
                )

            user_id = claims.get("sub", "")
            if not user_id:
                raise AuthError("Token missing 'sub' claim")

            session_id = claims.get("sid", claims.get("session_id", ""))
            tenant_id = claims.get(
                "org_id",
                claims.get("tenant_id", self._default_tenant_id),
            )

            return AuthenticatedUser(
                user_id=user_id,
                tenant_id=tenant_id,
                session_id=session_id,
                claims=claims,
            )

        except AuthError:
            raise
        except pyjwt.ExpiredSignatureError as err:
            raise AuthError("Token has expired") from err
        except pyjwt.InvalidTokenError as err:
            raise AuthError(f"Invalid token: {err}") from err
        except Exception as err:
            logger.exception("Unexpected authentication error")
            raise AuthError(f"Authentication failed: {err}") from err
