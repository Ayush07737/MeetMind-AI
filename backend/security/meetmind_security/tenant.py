"""MeetMind AI - Multi-Tenancy Routing Layer.

Every service that needs tenant isolation MUST resolve infrastructure connections
through this router - never via hardcoded connection strings.

Design decisions:
- Neo4j CE doesn't support multiple databases, so we use namespace-prefixed
  labels/properties within a single database. The routing function returns the
  prefix, not a separate connection.
- PostgreSQL uses schema-based isolation (one schema per tenant).
  Schema names are strictly validated against ^[a-z0-9_]{1,48}$ to prevent SQL injection.
- Qdrant uses collection-name prefixes.
- Redis uses key prefixes.

Adding tenant #2 is a config change (add an entry to TENANT_CONFIGS), not a rewrite.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

SCHEMA_NAME_REGEX = re.compile(r"^[a-z0-9_]{1,48}$")
TENANT_ID_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


@dataclass(frozen=True)
class TenantConfig:
    """Infrastructure connection configuration for a single tenant."""

    tenant_id: str

    # PostgreSQL
    pg_schema: str

    # Neo4j - namespace prefix for labels (CE doesn't support multi-database)
    neo4j_namespace: str

    # Qdrant - collection name prefix
    qdrant_prefix: str

    # Redis - key prefix
    redis_prefix: str

    def __post_init__(self) -> None:
        if not SCHEMA_NAME_REGEX.match(self.pg_schema):
            raise ValueError(
                f"Invalid pg_schema '{self.pg_schema}': must match ^[a-z0-9_]{{1,48}}$"
            )
        if not TENANT_ID_REGEX.match(self.tenant_id):
            raise ValueError(
                f"Invalid tenant_id '{self.tenant_id}': must match ^[a-zA-Z0-9_-]{{1,64}}$"
            )


# Default tenant registry
_DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "default")

TENANT_CONFIGS: dict[str, TenantConfig] = {
    _DEFAULT_TENANT_ID: TenantConfig(
        tenant_id=_DEFAULT_TENANT_ID,
        pg_schema=f"tenant_{_DEFAULT_TENANT_ID}",
        neo4j_namespace=f"{_DEFAULT_TENANT_ID}",
        qdrant_prefix=f"{_DEFAULT_TENANT_ID}",
        redis_prefix=f"{_DEFAULT_TENANT_ID}",
    ),
    "test_tenant": TenantConfig(
        tenant_id="test_tenant",
        pg_schema="tenant_test_tenant",
        neo4j_namespace="test_tenant",
        qdrant_prefix="test_tenant",
        redis_prefix="test_tenant",
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

    def __init__(
        self,
        configs: dict[str, TenantConfig] | None = None,
        allow_dynamic: bool | None = None,
    ) -> None:
        if configs is not None:
            self._configs = dict(configs)
        else:
            self._configs = dict(TENANT_CONFIGS)
            # Register mapped tenants from environment if present
            org_map_str = os.getenv("TENANT_ORG_MAP", "")
            if org_map_str.strip():
                try:
                    import json

                    org_map = json.loads(org_map_str)
                    if isinstance(org_map, dict):
                        for _org_id, tid in org_map.items():
                            if (
                                isinstance(tid, str)
                                and tid not in self._configs
                                and TENANT_ID_REGEX.match(tid)
                            ):
                                schema = f"tenant_{tid.lower()}"[:48]
                                if SCHEMA_NAME_REGEX.match(schema):
                                    self._configs[tid] = TenantConfig(
                                        tenant_id=tid,
                                        pg_schema=schema,
                                        neo4j_namespace=tid,
                                        qdrant_prefix=tid,
                                        redis_prefix=tid,
                                    )
                except Exception:
                    pass

        app_env = os.getenv("APP_ENV", "production").lower()
        is_dev = app_env in ("development", "dev")
        self._allow_dynamic = (
            allow_dynamic if allow_dynamic is not None else (is_dev and configs is None)
        )

    def resolve(self, tenant_id: str) -> TenantConfig:
        """Return the TenantConfig for the given tenant_id.

        Raises:
            ValueError: If tenant_id format is invalid or attempts injection.
            TenantNotFoundError: If no config exists for the tenant.
        """
        if not isinstance(tenant_id, str) or not TENANT_ID_REGEX.match(tenant_id):
            raise ValueError(
                f"Invalid tenant identifier '{tenant_id}': must match ^[a-zA-Z0-9_-]{{1,64}}$"
            )

        config = self._configs.get(tenant_id)
        if config is None:
            if self._allow_dynamic:
                schema = f"tenant_{tenant_id.lower()}"[:48]
                if SCHEMA_NAME_REGEX.match(schema):
                    return TenantConfig(
                        tenant_id=tenant_id,
                        pg_schema=schema,
                        neo4j_namespace=tenant_id,
                        qdrant_prefix=tenant_id,
                        redis_prefix=tenant_id,
                    )
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
        """Shortcut: resolve tenant -> PostgreSQL schema name."""
        return self.resolve(tenant_id).pg_schema

    def neo4j_namespace(self, tenant_id: str) -> str:
        """Shortcut: resolve tenant -> Neo4j label namespace prefix."""
        return self.resolve(tenant_id).neo4j_namespace

    def qdrant_collection(self, tenant_id: str, base_name: str) -> str:
        """Shortcut: resolve tenant -> full Qdrant collection name."""
        prefix = self.resolve(tenant_id).qdrant_prefix
        return f"{prefix}_{base_name}"

    def redis_key(self, tenant_id: str, key: str) -> str:
        """Shortcut: resolve tenant -> prefixed Redis key."""
        prefix = self.resolve(tenant_id).redis_prefix
        return f"{prefix}:{key}"
