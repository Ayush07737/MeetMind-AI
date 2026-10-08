"""MeetMind AI - Gateway configuration via environment variables.

All settings are loaded from environment variables (or a .env file via
pydantic-settings). No defaults for secrets -- the service fails fast
if required credentials are missing.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings


class GatewaySettings(BaseSettings):
    """Configuration for the Ingestion Gateway."""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    # -- Neon (Postgres) -------------------------------------------------------
    neon_database_url: str

    # -- Redis -----------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # -- Clerk Authentication --------------------------------------------------
    clerk_secret_key: str = ""
    clerk_publishable_key: str = ""
    clerk_jwks_url: str = "https://api.clerk.com/v1/jwks"
    clerk_issuer: str = ""
    allowed_origins: list[str] = ["*"]
    tenant_org_map: dict[str, str] = {}

    # -- Multi-Tenancy ---------------------------------------------------------
    default_tenant_id: str = "default"

    # -- Application -----------------------------------------------------------
    app_env: str = "development"
    log_level: str = "DEBUG"

    # -- Gateway-specific ------------------------------------------------------
    ws_max_frame_bytes: int = 64 * 1024  # 64 KB max per WS frame
    redis_stream_maxlen: int = 10_000  # cap stream length per meeting
    allow_token_in_dev: bool = True

    @field_validator("tenant_org_map", mode="before")
    @classmethod
    def parse_tenant_org_map(cls, v: Any) -> dict[str, str]:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return {}
        return v or {}

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def parse_allowed_origins(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return [orig.strip() for orig in v.split(",") if orig.strip()]
        return v or ["*"]


def get_settings() -> GatewaySettings:
    """Return cached settings instance."""
    return GatewaySettings()
