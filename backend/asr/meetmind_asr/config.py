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
    deepgram_base_url: str = Field(default="wss://api.deepgram.com", alias="DEEPGRAM_BASE_URL")
    redis_url: str = Field(default="redis://127.0.0.1:6379/0", alias="REDIS_URL")
    asr_max_meetings_per_worker: int = Field(default=10, alias="ASR_MAX_MEETINGS_PER_WORKER")
    asr_replay_window_ms: int = Field(default=5000, alias="ASR_REPLAY_WINDOW_MS")
    asr_buffer_max_seconds: int = Field(default=30, alias="ASR_BUFFER_MAX_SECONDS")
    asr_tenant_config_path: str | None = Field(default=None, alias="ASR_TENANT_CONFIG_PATH")
    asr_health_port: int = Field(default=8080, alias="ASR_HEALTH_PORT")
    asr_lifecycle_idle_ms: int = Field(default=10000, alias="ASR_LIFECYCLE_IDLE_MS")
    asr_audio_idle_ms: int = Field(default=5000, alias="ASR_AUDIO_IDLE_MS")

    @model_validator(mode="after")
    def validate_provider_requirements(self) -> AsrSettings:
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
        return self


def get_settings() -> AsrSettings:
    """Load settings from environment."""
    return AsrSettings()
