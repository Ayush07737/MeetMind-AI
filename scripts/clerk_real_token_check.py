#!/usr/bin/env python
"""MeetMind AI — Validate real Clerk JWT against production Clerk JWKS.

Validates a REAL token through the production ClerkAuthenticator path:
- Uses configured CLERK_JWKS_URL and CLERK_ISSUER from settings (no local keypair, no mock JWKS).
- Prints ONLY claim NAMES present (never values, NEVER prints the token).
- Prints org claim shape ('org_id' vs 'o' vs 'tenant_id').
- Prints resolved tenant ID via TENANT_ORG_MAP.
"""

from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# Ensure gateway package is importable
sys.path.insert(0, os.path.abspath("backend/gateway"))

from meetmind_gateway.auth import AuthError, ClerkAuthenticator  # noqa: E402
from meetmind_gateway.config import get_settings  # noqa: E402


async def validate_real_token(token: str) -> int:
    if not token or not token.strip():
        print("ERROR: Token argument is required.", file=sys.stderr)
        print("Usage: python scripts/clerk_real_token_check.py <token>", file=sys.stderr)
        return 1

    settings = get_settings()

    print("========================================================")
    print(" MeetMind AI — Real Clerk Token Validation Check")
    print("========================================================")
    print(f"JWKS Endpoint:   {settings.clerk_jwks_url}")
    print(f"Expected Issuer: {settings.clerk_issuer or '(none configured)'}")
    print(f"Environment:     {settings.app_env}")
    print(f"Allowed Origins: {settings.allowed_origins}")
    print("--------------------------------------------------------")

    authenticator = ClerkAuthenticator(
        jwks_url=settings.clerk_jwks_url,
        allowed_origins=settings.allowed_origins,
        expected_issuer=settings.clerk_issuer or None,
        tenant_org_map=settings.tenant_org_map,
        default_tenant_id=settings.default_tenant_id,
        app_env=settings.app_env,
        clerk_secret_key=settings.clerk_secret_key,
    )

    try:
        user = await authenticator.authenticate(token.strip())
    except AuthError as exc:
        print(f"STATUS: FAILED (Code: {exc.close_code})")
        print(f"ERROR:  {exc.message}")
        return 2
    except Exception as exc:
        print("STATUS: ERROR")
        print(f"ERROR:  {type(exc).__name__}: {exc}")
        return 3

    print("STATUS: SUCCESS")
    print(f"Claim Names Present: {sorted(list(user.claims.keys()))}")

    # Determine org claim shape
    if "org_id" in user.claims:
        org_shape = "org_id"
    elif "o" in user.claims:
        org_shape = "o"
    elif "tenant_id" in user.claims:
        org_shape = "tenant_id"
    else:
        org_shape = "none (fallback to default/dev)"

    print(f"Org Claim Shape:     {org_shape}")
    print(f"Resolved Tenant:     {user.tenant_id}")
    print("Token Secret:        [REDACTED - NEVER LOGGED/PRINTED]")
    print("--------------------------------------------------------")
    return 0


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/clerk_real_token_check.py <token>", file=sys.stderr)
        sys.exit(1)

    token = sys.argv[1]
    code = asyncio.run(validate_real_token(token))
    sys.exit(code)


if __name__ == "__main__":
    main()
