#!/usr/bin/env python
"""MeetMind AI - Validate real Clerk JWT against production Clerk JWKS.

Validates a REAL token through the production ClerkAuthenticator path:
- Uses configured CLERK_JWKS_URL and CLERK_ISSUER from settings.
- Prints the EFFECTIVE values used by the authenticator.
- Prints ONLY claim NAMES present (never values, NEVER prints the token).
- Prints org claim shape ('o' compact object vs 'org_id' vs 'tenant_id').
- Prints resolved tenant ID and role.
- On failure prints exception type, message, and token-redacted traceback.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

# Ensure .env is loaded from repository root regardless of current working directory
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

# Ensure gateway package is importable
sys.path.insert(0, str(REPO_ROOT / "backend" / "gateway"))

from meetmind_gateway.auth import AuthError, ClerkAuthenticator  # noqa: E402
from meetmind_gateway.config import get_settings  # noqa: E402


async def validate_real_token(token: str) -> int:
    token_clean = token.strip()
    if not token_clean:
        print("ERROR: Token argument is required.", file=sys.stderr)
        print("Usage: uv run python scripts/clerk_real_token_check.py <token>", file=sys.stderr)
        return 1

    settings = get_settings()

    # Build the authenticator
    authenticator = ClerkAuthenticator(
        jwks_url=settings.clerk_jwks_url,
        allowed_origins=settings.allowed_origins,
        expected_issuer=settings.clerk_issuer or None,
        tenant_org_map=settings.tenant_org_map,
        default_tenant_id=settings.default_tenant_id,
        app_env=settings.app_env,
        clerk_secret_key=settings.clerk_secret_key,
    )

    print("========================================================")
    print(" MeetMind AI - Real Clerk Token Validation Check")
    print("========================================================")
    # Print EFFECTIVE values used by the authenticator
    print(f"Effective JWKS Endpoint:   {authenticator.jwks_url}")
    print(f"Effective Expected Issuer: {authenticator.expected_issuer or '(none configured)'}")
    print(f"Effective Allowed Origins: {authenticator.allowed_origins}")
    print(f"Effective Environment:     {authenticator.app_env}")
    print(f"Configured Org Map Keys:   {sorted(list(authenticator.tenant_org_map.keys()))}")
    print("--------------------------------------------------------")

    try:
        user = await authenticator.authenticate(token_clean)
    except AuthError as exc:
        print(f"STATUS:    FAILED (Code: {exc.close_code})")
        print(f"EXCEPTION: {type(exc).__name__}")
        print(f"MESSAGE:   {exc.message}")
        print("--------------------------------------------------------")
        return 2
    except Exception as exc:
        print("STATUS:    ERROR")
        print(f"EXCEPTION: {type(exc).__name__}")
        print(f"MESSAGE:   {exc}")
        print("TRACEBACK (token redacted):")
        tb_lines = traceback.format_exc()
        # Redact token from traceback if it appears anywhere
        redacted_tb = tb_lines.replace(token_clean, "[REDACTED_TOKEN]")
        print(redacted_tb)
        print("--------------------------------------------------------")
        return 3

    print("STATUS: SUCCESS")
    print(f"Claim Names Present: {sorted(list(user.claims.keys()))}")

    # Determine org claim shape
    if "o" in user.claims:
        o_val = user.claims["o"]
        if isinstance(o_val, dict):
            org_shape = f"o (compact object with keys: {sorted(list(o_val.keys()))})"
        else:
            org_shape = f"o ({type(o_val).__name__})"
    elif "org_id" in user.claims:
        org_shape = "org_id (string)"
    elif "tenant_id" in user.claims:
        org_shape = "tenant_id (string)"
    else:
        org_shape = "none (fallback to default/dev)"

    print(f"Org Claim Shape:     {org_shape}")
    print(f"Resolved Tenant:     {user.tenant_id}")
    print(f"Resolved Role:       {user.role or '(none)'}")
    print("Token Secret:        [REDACTED - NEVER LOGGED/PRINTED]")
    print("--------------------------------------------------------")
    return 0


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: uv run python scripts/clerk_real_token_check.py <token>", file=sys.stderr)
        sys.exit(1)

    token = sys.argv[1]
    code = asyncio.run(validate_real_token(token))
    sys.exit(code)


if __name__ == "__main__":
    main()
