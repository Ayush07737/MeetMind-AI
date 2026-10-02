"""MeetMind AI — Multi-Tenancy Routing Layer.

Every service that needs tenant isolation MUST resolve infrastructure connections
through this router — never via hardcoded connection strings.

Design decisions:
- Neo4j CE doesn't support multiple databases, so we use namespace-prefixed
  labels/properties within a single database. The routing function returns the
  prefix, not a separate connection.
- PostgreSQL uses schema-based isolation (one schema per tenant).
- Qdrant uses collection-name prefixes.
- Redis uses key prefixes.

Adding tenant #2 is a config change (add an entry to TENANT_CONFIGS), not a rewrite.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TenantConfig:
    """Infrastructure connection configuration for a single tenant."""

    tenant_id: str

    # PostgreSQL
    pg_schema: str

    # Neo4j — namespace prefix for labels (CE doesn't support multi-database)
    neo4j_namespace: str

    # Qdrant — collection name prefix
    qdrant_prefix: str

    # Redis — key prefix
    redis_prefix: str


# ── Default tenant registry ────────────────────────────────────────────────
# Currently a single entry. Adding tenant #2 = adding one more entry here
# (or loading from a database/config service).

_DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "default")

TENANT_CONFIGS: dict[str, TenantConfig] = {
    _DEFAULT_TENANT_ID: TenantConfig(
        tenant_id=_DEFAULT_TENANT_ID,
        pg_schema=f"tenant_{_DEFAULT_TENANT_ID}",
        neo4j_namespace=f"{_DEFAULT_TENANT_ID}",
        qdrant_prefix=f"{_DEFAULT_TENANT_ID}",
        redis_prefix=f"{_DEFAULT_TENANT_ID}",
    ),
}


class TenantNotFoundError(Exception):
    """Raised when a tenant_id has no matching configuration."""

    def __init__(self, tenant_id: str) -> None:
        super().__init__(f"No configuration found for tenant '{tenant_id}'")
        self.tenant_id = tenant_id


class TenantRouter:
    """Resolves tenant IDs to infrastructure connection configs.

    Usage::

        router = TenantRouter()
        config = router.resolve("default")
        pg_schema = config.pg_schema          # "tenant_default"
        neo4j_ns  = config.neo4j_namespace    # "default"
        qdrant_pfx = config.qdrant_prefix     # "default"
        redis_pfx  = config.redis_prefix      # "default"
    """

    def __init__(self, configs: dict[str, TenantConfig] | None = None) -> None:
        self._configs = configs if configs is not None else TENANT_CONFIGS

    def resolve(self, tenant_id: str) -> TenantConfig:
        """Return the TenantConfig for the given tenant_id.

        Raises:
            TenantNotFoundError: If no config exists for the tenant.
        """
        config = self._configs.get(tenant_id)
        if config is None:
            raise TenantNotFoundError(tenant_id)
        return config

    def list_tenants(self) -> list[str]:
        """Return all registered tenant IDs."""
        return list(self._configs.keys())

    def register(self, config: TenantConfig) -> None:
        """Register a new tenant configuration at runtime.

        This allows dynamic tenant onboarding without restarting the service.
        """
        self._configs[config.tenant_id] = config

    def pg_schema(self, tenant_id: str) -> str:
        """Shortcut: resolve tenant → PostgreSQL schema name."""
        return self.resolve(tenant_id).pg_schema

    def neo4j_namespace(self, tenant_id: str) -> str:
        """Shortcut: resolve tenant → Neo4j label namespace prefix."""
        return self.resolve(tenant_id).neo4j_namespace

    def qdrant_collection(self, tenant_id: str, base_name: str) -> str:
        """Shortcut: resolve tenant → full Qdrant collection name."""
        prefix = self.resolve(tenant_id).qdrant_prefix
        return f"{prefix}_{base_name}"

    def redis_key(self, tenant_id: str, key: str) -> str:
        """Shortcut: resolve tenant → prefixed Redis key."""
        prefix = self.resolve(tenant_id).redis_prefix
        return f"{prefix}:{key}"
