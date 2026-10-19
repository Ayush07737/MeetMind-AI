"""MeetMind AI — ASR Service Configuration.

Loads environment variables with fail-fast validation.
Provider selected solely via ASR_PROVIDER.
Secrets (like DEEPGRAM_API_KEY) are strictly wrapped in SecretStr.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class AsrSettings(BaseSettings):
    """Runtime configuration for MeetMind ASR Pipeline Service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="development", alias="APP_ENV")
    asr_provider: Literal["deepgram", "stub"] = Field(default="stub", alias="ASR_PROVIDER")
    deepgram_api_key: SecretStr | None = Field(default=None, alias="DEEPGRAM_API_KEY")
    deepgram_base_url: str | None = Field(default=None, alias="DEEPGRAM_BASE_URL")
    redis_url: str = Field(default="redis://127.0.0.1:6379/0", alias="REDIS_URL")
    asr_max_meetings_per_worker: int = Field(default=10, alias="ASR_MAX_MEETINGS_PER_WORKER")
    asr_replay_window_ms: int = Field(default=5000, alias="ASR_REPLAY_WINDOW_MS")
    asr_buffer_max_seconds: int = Field(default=30, alias="ASR_BUFFER_MAX_SECONDS")
    asr_tenant_config_path: str | None = Field(default=None, alias="ASR_TENANT_CONFIG_PATH")
    asr_health_port: int = Field(default=8080, alias="ASR_HEALTH_PORT")
    asr_worker_id: str | None = Field(default=None, alias="ASR_WORKER_ID")
    asr_reconcile_interval_ms: int = Field(default=2000, alias="ASR_RECONCILE_INTERVAL_MS")
    asr_owner_lease_ttl_ms: int = Field(default=15000, alias="ASR_OWNER_LEASE_TTL_MS")
    asr_audio_idle_ms: int = Field(default=5000, alias="ASR_AUDIO_IDLE_MS")
    asr_stub_replay_jitter_ms: int = Field(default=0, alias="ASR_STUB_REPLAY_JITTER_MS")
    asr_test_hooks: bool = Field(default=False, alias="ASR_TEST_HOOKS")
    asr_warm_standby: bool = Field(default=False, alias="ASR_WARM_STANDBY")
    asr_region: Literal["global", "in", "eu"] = Field(default="global", alias="ASR_REGION")
    asr_allowed_regions: str = Field(default="global,in,eu", alias="ASR_ALLOWED_REGIONS")

    @property
    def allowed_regions_set(self) -> set[str]:
        return {r.strip().lower() for r in self.asr_allowed_regions.split(",") if r.strip()}

    @model_validator(mode="after")
    def validate_provider_requirements(self) -> AsrSettings:
        is_prod = self.app_env.lower() in ("prod", "production")
        if is_prod and self.asr_test_hooks:
            raise ValueError("ASR_TEST_HOOKS is forbidden in production environments")
        if self.asr_provider == "deepgram" and (
            not self.deepgram_api_key or not self.deepgram_api_key.get_secret_value().strip()
        ):
            raise ValueError("DEEPGRAM_API_KEY is required when ASR_PROVIDER is 'deepgram'")
        if self.asr_max_meetings_per_worker <= 0:
            raise ValueError("ASR_MAX_MEETINGS_PER_WORKER must be greater than 0")
        if self.asr_replay_window_ms <= 0:
            raise ValueError("ASR_REPLAY_WINDOW_MS must be greater than 0")
        if self.asr_buffer_max_seconds <= 0:
            raise ValueError("ASR_BUFFER_MAX_SECONDS must be greater than 0")

        # Deployment guardrail: ASR_ALLOWED_REGIONS
        allowed = self.allowed_regions_set
        valid_regions = set(DEEPGRAM_REGION_HOSTS.keys())
        unknown = allowed - valid_regions
        if unknown:
            raise ValueError(
                f"Invalid regions in ASR_ALLOWED_REGIONS: {sorted(unknown)}. "
                f"Allowed: {sorted(valid_regions)}"
            )
        if self.asr_region not in allowed:
            raise ValueError(
                f"Configured ASR_REGION '{self.asr_region}' contradicts deployment guardrail "
                f"ASR_ALLOWED_REGIONS: {sorted(allowed)}"
            )

        # Loopback-only guardrail for DEEPGRAM_BASE_URL
        if self.deepgram_base_url and self.deepgram_base_url.strip():
            import urllib.parse

            parsed = urllib.parse.urlparse(self.deepgram_base_url)
            host = (parsed.hostname or "").lower()
            if host not in ("127.0.0.1", "localhost"):
                raise ValueError(
                    f"DEEPGRAM_BASE_URL '{self.deepgram_base_url}' is not a loopback test server. "
                    "Hosts must be derived from region via allow-list; "
                    "DEEPGRAM_BASE_URL is accepted only for loopback test servers "
                    "(127.0.0.1, localhost)."
                )

        return self


DEEPGRAM_REGION_HOSTS: dict[str, str] = {
    "global": "api.deepgram.com",
    "in": "api.in.deepgram.com",
    "eu": "api.eu.deepgram.com",
}

ALLOWED_ASR_HOSTS: set[str] = {
    "api.deepgram.com",
    "api.in.deepgram.com",
    "api.eu.deepgram.com",
    "127.0.0.1",
    "localhost",
}


def resolve_asr_base_url(settings: AsrSettings, region: str = "global") -> str:
    """Resolve Deepgram WebSocket base URL based on region and settings with host allow-list."""
    import urllib.parse

    norm_region = region.lower().strip()
    if norm_region not in DEEPGRAM_REGION_HOSTS:
        raise ValueError(
            f"Invalid asr_region '{region}'. Allowed: {sorted(DEEPGRAM_REGION_HOSTS.keys())}"
        )

    # If settings.deepgram_base_url is explicitly set to a loopback test host, permit it
    if settings.deepgram_base_url and settings.deepgram_base_url.strip():
        parsed = urllib.parse.urlparse(settings.deepgram_base_url)
        host = (parsed.hostname or "").lower()
        if host in ("127.0.0.1", "localhost"):
            return settings.deepgram_base_url
        raise ValueError(
            f"ASR host '{host}' is not a loopback test server. "
            "DEEPGRAM_BASE_URL is accepted only for loopback test servers."
        )

    target_host = DEEPGRAM_REGION_HOSTS[norm_region]
    if target_host not in ALLOWED_ASR_HOSTS:
        raise ValueError(f"Target ASR host '{target_host}' violates ALLOWED_ASR_HOSTS")

    return f"wss://{target_host}"


def get_settings() -> AsrSettings:
    """Load settings from environment."""
    return AsrSettings()
