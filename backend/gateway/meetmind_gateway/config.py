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
    neon_database_url: str = ""

    # -- Redis -----------------------------------------------------------------
    redis_url: str = "redis://127.0.0.1:6379/0"
    audio_redis_url: str = ""
    audio_redis_persistence_acknowledged: bool = False
    audio_stream_maxlen: int = 6_000

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
    redis_stream_maxlen: int = 6_000  # cap stream length per meeting (10m @ 10 chunk/s, ~19.2 MB)
    allow_token_in_dev: bool = True
    idle_timeout_seconds: float = 30.0
    max_connections_per_user: int = 10
    max_connections_per_tenant: int = 100
    frame_rate_limit: int = 120  # frames per sec (sustained)
    burst_frame_limit: int = 150  # K8: burst capacity for 5s reconnect retransmits
    byte_rate_limit: int = 512 * 1024  # 512 KB per sec (sustained)
    burst_byte_limit: int = 1024 * 1024  # 1 MB burst capacity
    ticket_ttl_seconds: int = 30  # K9: ticket expiration TTL
    gateway_bench_endpoints: bool = False  # P0-1: system benchmark endpoints disabled by default
    gateway_admin_key: str = ""  # P0-1: shared secret admin key for benchmark endpoints

    @model_validator(mode="before")
    @classmethod
    def parse_complex_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Parse tenant_org_map if JSON string - K2: malformed map must fail startup
            tom = data.get("tenant_org_map")
            if isinstance(tom, str):
                tom = tom.strip()
                if not tom:
                    data["tenant_org_map"] = {}
                else:
                    try:
                        parsed = json.loads(tom)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"Invalid TENANT_ORG_MAP JSON format: {exc}") from exc
                    if not isinstance(parsed, dict):
                        raise ValueError(
                            "Invalid TENANT_ORG_MAP: "
                            "must be a JSON object mapping org_id to tenant_id"
                        )
                    data["tenant_org_map"] = parsed
            elif tom is not None and not isinstance(tom, dict):
                raise ValueError(
                    "Invalid TENANT_ORG_MAP: must be a dictionary or JSON object string"
                )

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

        if not self.audio_redis_url:
            self.audio_redis_url = self.redis_url

        if self.ticket_ttl_seconds > 30:
            raise ValueError(
                f"ticket_ttl_seconds must be <= 30 per K9 security invariant, "
                f"got {self.ticket_ttl_seconds}"
            )

        return self


def get_settings() -> GatewaySettings:
    """Return cached settings instance."""
    return GatewaySettings()
