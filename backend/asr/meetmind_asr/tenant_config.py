"""MeetMind AI — Tenant ASR Configuration Provider (WP-D).

Provides tenant-specific ASR settings (language, custom keyterms).
Keyterms are strictly per tenant / per domain pack, never hardcoded.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path

from pydantic import BaseModel, Field


class AsrTenantConfig(BaseModel):
    """Tenant ASR configuration."""

    tenant_id: str
    language: str = "en"
    keyterms: list[str] = Field(default_factory=list)


class TenantAsrConfigProvider(ABC):
    """Abstract provider for tenant ASR configurations."""

    @abstractmethod
    async def get_config(self, tenant_id: str) -> AsrTenantConfig:
        """Fetch ASR configuration for a specific tenant."""
        ...


class FileTenantAsrConfigProvider(TenantAsrConfigProvider):
    """Loads tenant configurations from a JSON file with memory caching."""

    def __init__(self, config_path: str | None = None) -> None:
        self.config_path = config_path
        self._configs: dict[str, AsrTenantConfig] = {}
        self._load_file()

    def _load_file(self) -> None:
        if not self.config_path:
            return
        p = Path(self.config_path)
        if not p.is_file():
            return

        try:
            raw_text = p.read_text(encoding="utf-8")
            data = json.loads(raw_text)
            tenants_data = data.get("tenants", {})
            for tid, tcfg in tenants_data.items():
                self._configs[tid] = AsrTenantConfig(
                    tenant_id=tid,
                    language=tcfg.get("language", "en"),
                    keyterms=tcfg.get("keyterms", []),
                )
        except Exception:
            pass

    async def get_config(self, tenant_id: str) -> AsrTenantConfig:
        if tenant_id in self._configs:
            return self._configs[tenant_id]
        return AsrTenantConfig(tenant_id=tenant_id, language="en", keyterms=[])
