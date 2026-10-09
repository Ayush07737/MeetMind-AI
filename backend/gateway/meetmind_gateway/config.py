"""MeetMind AI - Gateway configuration via environment variables.

All settings are loaded from environment variables (or a .env file via
pydantic-settings). No defaults for secrets -- the service fails fast
if required credentials are missing.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Self

from pydantic import model_validator
from pydantic_settings import BaseSettings

JWKS_URL_PATTERN = re.compile(r"^https://[a-zA-Z0-9.-]+/\.well-known/jwks\.json$")
REPO_ROOT = Path(__file__).resolve().parents[3]


class GatewaySettings(BaseSettings):
    """Configuration for the Ingestion Gateway."""

    model_config = {
        "env_file": str(REPO_ROOT / ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    # -- Neon (Postgres) -------------------------------------------------------
    neon_database_url: str

    # -- Redis -----------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # -- Clerk Authentication (CA-3: no insecure defaults) --------------------
    clerk_secret_key: str = ""
    clerk_publishable_key: str = ""
    clerk_jwks_url: str = ""
    clerk_issuer: str = ""
    allowed_origins: list[str] = []
    tenant_org_map: dict[str, str] = {}
    elevated_listen_roles: list[str] = ["org:admin"]

    # -- Multi-Tenancy ---------------------------------------------------------
    default_tenant_id: str = "default"

    # -- Application -----------------------------------------------------------
    app_env: str = "development"
    log_level: str = "DEBUG"

    # -- Gateway-specific ------------------------------------------------------
    ws_max_frame_bytes: int = 64 * 1024  # 64 KB max per WS frame
    redis_stream_maxlen: int = 10_000  # cap stream length per meeting
    allow_token_in_dev: bool = True
    idle_timeout_seconds: float = 30.0
    max_connections_per_user: int = 10
    max_connections_per_tenant: int = 100
    frame_rate_limit: int = 120  # frames per sec
    byte_rate_limit: int = 256 * 1024  # bytes per sec

    @model_validator(mode="before")
    @classmethod
    def parse_complex_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Parse tenant_org_map if JSON string
            tom = data.get("tenant_org_map")
            if isinstance(tom, str):
                try:
                    data["tenant_org_map"] = json.loads(tom)
                except Exception:
                    data["tenant_org_map"] = {}

            # Parse allowed_origins if JSON list string or comma-separated
            origins = data.get("allowed_origins")
            if isinstance(origins, str):
                origins = origins.strip()
                if origins.startswith("[") and origins.endswith("]"):
                    try:
                        data["allowed_origins"] = json.loads(origins)
                    except Exception:
                        data["allowed_origins"] = []
                else:
                    data["allowed_origins"] = [
                        orig.strip() for orig in origins.split(",") if orig.strip()
                    ]

            # Parse elevated_listen_roles if JSON list string or comma-separated
            el_roles = data.get("elevated_listen_roles")
            if isinstance(el_roles, str):
                el_roles = el_roles.strip()
                if el_roles.startswith("[") and el_roles.endswith("]"):
                    try:
                        data["elevated_listen_roles"] = json.loads(el_roles)
                    except Exception:
                        data["elevated_listen_roles"] = ["org:admin"]
                else:
                    data["elevated_listen_roles"] = [
                        r.strip() for r in el_roles.split(",") if r.strip()
                    ]
        return data

    @model_validator(mode="after")
    def validate_clerk_config(self) -> Self:
        """CA-3 Clerk config fail-fast validation."""
        is_dev = self.app_env.lower() in ("dev", "development")
        is_test = self.app_env.lower() in ("test", "testing")

        if not is_dev:
            # 1. Non-empty required configurations
            if not self.clerk_jwks_url or not self.clerk_jwks_url.strip():
                raise ValueError("CLERK_JWKS_URL is required outside dev environment")
            if not self.clerk_issuer or not self.clerk_issuer.strip():
                raise ValueError("CLERK_ISSUER is required outside dev environment")
            if not self.allowed_origins:
                raise ValueError("ALLOWED_ORIGINS is required outside dev environment")

            # 2. Reject wildcard origin outside dev
            if "*" in self.allowed_origins:
                raise ValueError("Wildcard origin '*' is forbidden outside dev environment")

            # 3. JWKS URL structure check
            jwks = self.clerk_jwks_url.strip()
            if is_test:
                is_local = jwks.startswith("http://127.0.0.1") or jwks.startswith(
                    "http://localhost"
                )
                if not (is_local or JWKS_URL_PATTERN.match(jwks)):
                    raise ValueError(
                        f"CLERK_JWKS_URL '{jwks}' must match https://<host>/.well-known/jwks.json "
                        "or local test server"
                    )
            else:
                if not JWKS_URL_PATTERN.match(jwks):
                    raise ValueError(
                        f"CLERK_JWKS_URL '{jwks}' must match https://<host>/.well-known/jwks.json"
                    )

        return self


def get_settings() -> GatewaySettings:
    """Return cached settings instance."""
    return GatewaySettings()
