"""MeetMind AI - Clerk RS256 JWT Authentication & Tenant Mapping.

Validates Clerk session tokens passed as:
- One-time ticket claims exchanged at POST /v1/ws-ticket
- Direct WebSocket query param /v1/meetings/{meeting_id}/stream?token=... (dev-only)

Security invariants:
- RS256 ONLY. Rejects HS256, 'none', and all other algorithms.
- Validates expiration (exp), not-before (nbf, with 5s clock skew leeway),
  issuer (iss), and authorized party (azp).
- org: mapped to tenant_id via TENANT_ORG_MAP (unmapped orgs rejected with 4403).
- In dev mode (APP_ENV=development), tokens without an org fall back to
  DEFAULT_TENANT_ID, or self-assert tenant_id.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import jwt as pyjwt
from jwt import PyJWKClient
from meetmind_schemas.events import WSCloseCode

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthenticatedUser:
    """Represents a validated Clerk user with tenant mapping."""

    user_id: str
    tenant_id: str
    session_id: str
    claims: dict[str, Any]
    role: str = ""


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
    """Validates Clerk JWTs using RS256 via Clerk's JWKS endpoint."""

    def __init__(
        self,
        jwks_url: str,
        allowed_origins: list[str] | None = None,
        expected_issuer: str | None = None,
        tenant_org_map: dict[str, str] | None = None,
        default_tenant_id: str = "default",
        app_env: str = "development",
        clerk_secret_key: str = "",
    ) -> None:
        self.jwks_url = jwks_url
        self.allowed_origins = allowed_origins or []
        self.expected_issuer = expected_issuer
        self.tenant_org_map = tenant_org_map or {}
        self.default_tenant_id = default_tenant_id
        self.app_env = app_env
        self.clerk_secret_key = clerk_secret_key
        self._jwks_client: PyJWKClient | None = None

    def _get_jwks_client(self) -> PyJWKClient:
        """Lazily create and cache the PyJWKClient (handles caching & rotation)."""
        if self._jwks_client is None:
            headers: dict[str, str] = {}
            if self.clerk_secret_key and not self.jwks_url.endswith("/.well-known/jwks.json"):
                headers["Authorization"] = f"Bearer {self.clerk_secret_key}"

            self._jwks_client = PyJWKClient(
                self.jwks_url,
                cache_keys=True,
                max_cached_keys=16,
                cache_jwk_set=True,
                lifespan=3600,
                headers=headers,
            )
        return self._jwks_client

    def extract_user_from_claims(self, claims: dict[str, Any]) -> AuthenticatedUser:
        """Extract and validate claims, tenant mapping, and role.

        Raises:
            AuthUnauthorizedError: If issuer, azp, or sub claims are invalid.
            AuthForbiddenError: If org is unmapped or malformed.
        """
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

        # Extract org_id and role from claims (safe org claim extraction)
        org_id: str | None = None
        role: str = ""

        if "org_id" in claims and isinstance(claims["org_id"], str):
            org_id = claims["org_id"].strip()
            role = str(claims.get("org_role", claims.get("role", "")))
        elif "o" in claims:
            o_claim = claims["o"]
            if isinstance(o_claim, dict):
                o_id = o_claim.get("id")
                if isinstance(o_id, str) and o_id.strip():
                    org_id = o_id.strip()
                    role = str(o_claim.get("rol", o_claim.get("role", "")))
                else:
                    raise AuthForbiddenError(
                        "Invalid organization claim format in 'o': missing string 'id'"
                    )
            elif isinstance(o_claim, str) and o_claim.strip():
                org_id = o_claim.strip()
            else:
                raise AuthForbiddenError(
                    f"Invalid organization claim format in 'o': {type(o_claim).__name__}"
                )
        elif "tenant_id" in claims and isinstance(claims["tenant_id"], str):
            org_id = claims["tenant_id"].strip()
            role = str(claims.get("role", ""))

        if org_id:
            # org_id is guaranteed to be a string here; never use a dict as mapping key
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
            role=role,
        )

    async def authenticate(self, token: str) -> AuthenticatedUser:
        """Authenticate a raw JWT string against Clerk JWKS.

        Validates RS256 signature, expiry, leeway, and extracts tenant & role.
        """
        if not token or not token.strip():
            raise AuthUnauthorizedError("No authentication token provided")

        try:
            jwks_client = self._get_jwks_client()
            try:
                signing_key = jwks_client.get_signing_key_from_jwt(token)
            except pyjwt.PyJWKClientError:
                # Key rotation: refresh JWKS and retry lookup
                jwks_client.get_jwk_set(refresh=True)
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

        return self.extract_user_from_claims(claims)


def can_listen(
    user: AuthenticatedUser,
    session_tenant_id: str,
    session_owner_id: str,
    elevated_roles: list[str] | set[str] | None = None,
) -> bool:
    """Check if user has permission to listen to meeting events.

    Requires:
    1. Same tenant (user.tenant_id == session_tenant_id).
    2. Either the meeting owner (user.user_id == session_owner_id)
       OR holds an elevated role (e.g. 'org:admin') in elevated_roles.
    """
    if user.tenant_id != session_tenant_id:
        return False

    if user.user_id == session_owner_id:
        return True

    elevated = set(elevated_roles or ["org:admin"])
    user_role = user.role.strip()
    if user_role in elevated:
        return True

    # Account for optional "org:" prefix matching (e.g. "admin" <=> "org:admin")
    if user_role.startswith("org:") and user_role[4:] in elevated:
        return True
    return f"org:{user_role}" in elevated
