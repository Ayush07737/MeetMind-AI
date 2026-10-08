"""MeetMind AI - Clerk RS256 JWT Authentication & Tenant Mapping.

Validates Clerk-issued session JWTs using Clerk's public JWKS endpoint.
Only asymmetric RS256 signatures are accepted. HS256 and 'none' algorithms
are strictly forbidden.

Validates claims:
- exp: token expiration (with 5s leeway)
- nbf: not before (with 5s leeway)
- iss: issuer (validated against configured expected issuer)
- azp: authorized party (validated against allowed origins)
- org: mapped to tenant_id via TENANT_ORG_MAP (unmapped orgs rejected with 4403)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import jwt as pyjwt
from meetmind_schemas.events import WSCloseCode

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthenticatedUser:
    """Represents a validated Clerk user with tenant mapping."""

    user_id: str
    tenant_id: str
    session_id: str
    claims: dict[str, Any]


class AuthError(Exception):
    """Base exception for authentication and authorization failures."""

    close_code: WSCloseCode = WSCloseCode.UNAUTHORIZED

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class AuthUnauthorizedError(AuthError):
    """Authentication failed (invalid signature, expired, bad algorithm) -> 4401."""

    close_code: WSCloseCode = WSCloseCode.UNAUTHORIZED


class AuthForbiddenError(AuthError):
    """Authorization failed (unknown organization, tenant mismatch) -> 4403."""

    close_code: WSCloseCode = WSCloseCode.FORBIDDEN


class ClerkAuthenticator:
    """Validates Clerk JWTs using RS256 public keys fetched from JWKS."""

    def __init__(
        self,
        jwks_url: str = "https://api.clerk.com/v1/jwks",
        allowed_origins: list[str] | None = None,
        expected_issuer: str | None = None,
        tenant_org_map: dict[str, str] | None = None,
        default_tenant_id: str = "default",
        app_env: str = "development",
        jwks_client: pyjwt.PyJWKClient | None = None,
        clerk_secret_key: str = "",
    ) -> None:
        self.jwks_url = jwks_url
        self.allowed_origins = allowed_origins or ["*"]
        self.expected_issuer = expected_issuer
        self.tenant_org_map = tenant_org_map or {}
        self.default_tenant_id = default_tenant_id
        self.app_env = app_env.lower()
        self._custom_jwks_client = jwks_client
        self._default_jwks_client: pyjwt.PyJWKClient | None = None

    def _get_jwks_client(self) -> pyjwt.PyJWKClient:
        if self._custom_jwks_client is not None:
            return self._custom_jwks_client
        if self._default_jwks_client is None:
            self._default_jwks_client = pyjwt.PyJWKClient(
                uri=self.jwks_url,
                cache_keys=True,
                lifespan=3600,
            )
        return self._default_jwks_client

    async def authenticate(self, token: str) -> AuthenticatedUser:
        """Validate a Clerk JWT and return the authenticated user.

        Raises:
            AuthUnauthorizedError: If token is missing, expired, invalid RS256, or bad issuer/azp.
            AuthForbiddenError: If org is unmapped or missing in production.
        """
        if not token or not token.strip():
            raise AuthUnauthorizedError("No authentication token provided")

        try:
            jwks_client = self._get_jwks_client()
            signing_key = jwks_client.get_signing_key_from_jwt(token)

            # Strictly enforce RS256 - no HS256 or 'none'
            claims = pyjwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                leeway=5,
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                    "require": ["sub", "exp"],
                },
            )

        except pyjwt.ExpiredSignatureError as err:
            raise AuthUnauthorizedError("Token has expired") from err
        except pyjwt.ImmatureSignatureError as err:
            raise AuthUnauthorizedError("Token not yet valid (nbf claim in future)") from err
        except pyjwt.InvalidAlgorithmError as err:
            raise AuthUnauthorizedError("Invalid algorithm: only RS256 is accepted") from err
        except (pyjwt.PyJWKClientError, pyjwt.InvalidTokenError) as err:
            raise AuthUnauthorizedError(f"Invalid token: {err}") from err
        except AuthError:
            raise
        except Exception as err:
            logger.exception("Unexpected error validating Clerk token")
            raise AuthUnauthorizedError(f"Authentication failed: {err}") from err

        # Validate issuer
        if self.expected_issuer and claims.get("iss") != self.expected_issuer:
            raise AuthUnauthorizedError(
                f"Invalid issuer: expected '{self.expected_issuer}', got '{claims.get('iss')}'"
            )

        # Validate authorized party (azp)
        if self.allowed_origins and "*" not in self.allowed_origins:
            azp = claims.get("azp")
            if azp and azp not in self.allowed_origins:
                raise AuthUnauthorizedError(
                    f"Invalid authorized party: '{azp}' is not in allowed origins"
                )

        # Validate sub claim
        user_id = claims.get("sub", "")
        if not user_id:
            raise AuthUnauthorizedError("Token missing 'sub' claim")

        # Tenant mapping from organization claim
        org_id = claims.get("org_id") or claims.get("o") or claims.get("tenant_id")
        if org_id:
            if org_id in self.tenant_org_map:
                tenant_id = self.tenant_org_map[org_id]
            elif self.app_env in ("development", "dev"):
                tenant_id = org_id
            else:
                raise AuthForbiddenError(
                    f"Organization '{org_id}' is not mapped to an authorized tenant"
                )
        else:
            if self.app_env in ("development", "dev"):
                tenant_id = self.default_tenant_id
            else:
                raise AuthForbiddenError("Token missing required organization claim")

        session_id = claims.get("sid", claims.get("session_id", ""))

        return AuthenticatedUser(
            user_id=user_id,
            tenant_id=tenant_id,
            session_id=session_id,
            claims=claims,
        )
