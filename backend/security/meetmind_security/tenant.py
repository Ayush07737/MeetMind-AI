"""MeetMind AI - Multi-Tenancy Routing Layer.

Every service that needs tenant isolation MUST resolve infrastructure connections
through this router - never via hardcoded connection strings.

Design decisions:
- Neo4j CE doesn't support multiple databases, so we use namespace-prefixed
  labels/properties within a single database. The routing function returns the
  prefix, not a separate connection.
- PostgreSQL uses schema-based isolation (one schema per tenant).
  Schema names are strictly validated against ^[a-z0-9_]{1,48}$ to prevent SQL injection.
- Tenant IDs are strictly normalised lowercase and match ^[a-z0-9_]{1,40}$
  so that "tenant_{tid}" fits within 48 chars without truncation or collision.
- Qdrant uses collection-name prefixes.
- Redis uses key prefixes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

SCHEMA_NAME_REGEX = re.compile(r"^[a-z0-9_]{1,48}$")
TENANT_ID_REGEX = re.compile(r"^[a-z0-9_]{1,40}$")


def normalize_tenant_id(tenant_id: str) -> str:
    """Normalize tenant_id to lowercase and validate format ^[a-z0-9_]{1,40}$."""
    if not isinstance(tenant_id, str):
        raise ValueError(
            f"Invalid tenant identifier: expected string, got {type(tenant_id).__name__}"
        )
    normalized = tenant_id.strip().lower()
    if not TENANT_ID_REGEX.match(normalized):
        raise ValueError(
            f"Invalid tenant_id (Invalid tenant identifier) '{tenant_id}': "
            f"normalized value '{normalized}' must match ^[a-z0-9_]{{1,40}}$"
        )
    return normalized


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
        norm_id = normalize_tenant_id(self.tenant_id)
        if self.tenant_id != norm_id:
            object.__setattr__(self, "tenant_id", norm_id)
        if not SCHEMA_NAME_REGEX.match(self.pg_schema):
            raise ValueError(
                f"Invalid pg_schema '{self.pg_schema}': must match ^[a-z0-9_]{{1,48}}$"
            )


class TenantNotFoundError(Exception):
    """Raised when a tenant_id has no matching configuration."""

    def __init__(self, tenant_id: str) -> None:
        super().__init__(f"No configuration found for tenant '{tenant_id}'")
        self.tenant_id = tenant_id


class TenantRouter:
    """Resolves tenant IDs to infrastructure connection configs.

    No runtime os.getenv calls: environment name and org map are explicitly injected.
    """

    def __init__(
        self,
        configs: dict[str, TenantConfig] | None = None,
        allow_dynamic: bool | None = None,
        app_env: str = "production",
        tenant_org_map: dict[str, str] | None = None,
        default_tenant_id: str = "default",
    ) -> None:
        self.app_env = app_env.strip().lower()
        is_dev = self.app_env in ("development", "dev")
        self._allow_dynamic = (
            allow_dynamic if allow_dynamic is not None else (is_dev and configs is None)
        )
        self._configs: dict[str, TenantConfig] = {}

        if configs is not None:
            for _k, cfg in configs.items():
                self.register(cfg)
        else:
            # Register default tenant
            norm_def = normalize_tenant_id(default_tenant_id)
            self.register(
                TenantConfig(
                    tenant_id=norm_def,
                    pg_schema=f"tenant_{norm_def}",
                    neo4j_namespace=norm_def,
                    qdrant_prefix=norm_def,
                    redis_prefix=norm_def,
                )
            )
            if norm_def != "test_tenant":
                self.register(
                    TenantConfig(
                        tenant_id="test_tenant",
                        pg_schema="tenant_test_tenant",
                        neo4j_namespace="test_tenant",
                        qdrant_prefix="test_tenant",
                        redis_prefix="test_tenant",
                    )
                )

            # Register mapped tenants from injected tenant_org_map
            if tenant_org_map:
                if not isinstance(tenant_org_map, dict):
                    raise ValueError(
                        "Invalid tenant_org_map: expected dict, "
                        f"got {type(tenant_org_map).__name__}"
                    )
                for org_id, tid in tenant_org_map.items():
                    if not isinstance(org_id, str) or not isinstance(tid, str):
                        raise ValueError(
                            f"Invalid tenant_org_map entry ({org_id!r}: {tid!r}): "
                            "keys and values must be strings"
                        )
                    norm_tid = normalize_tenant_id(tid)
                    schema = f"tenant_{norm_tid}"
                    self.register(
                        TenantConfig(
                            tenant_id=norm_tid,
                            pg_schema=schema,
                            neo4j_namespace=norm_tid,
                            qdrant_prefix=norm_tid,
                            redis_prefix=norm_tid,
                        )
                    )

    def register(self, config: TenantConfig) -> None:
        """Register a new tenant configuration at runtime.

        Rejects duplicates case-insensitively and fails startup on collision.
        """
        norm_id = normalize_tenant_id(config.tenant_id)
        if norm_id in self._configs:
            existing = self._configs[norm_id]
            if (
                existing.pg_schema != config.pg_schema
                or existing.neo4j_namespace != config.neo4j_namespace
                or existing.qdrant_prefix != config.qdrant_prefix
                or existing.redis_prefix != config.redis_prefix
            ):
                raise ValueError(
                    f"Tenant collision for '{norm_id}': existing schema '{existing.pg_schema}' "
                    f"conflicts with new schema '{config.pg_schema}'"
                )
        self._configs[norm_id] = config

    def resolve(self, tenant_id: str) -> TenantConfig:
        """Return the TenantConfig for the given tenant_id.

        Raises:
            ValueError: If tenant_id format is invalid or attempts injection.
            TenantNotFoundError: If no config exists for the tenant.
        """
        norm_id = normalize_tenant_id(tenant_id)
        config = self._configs.get(norm_id)
        if config is None:
            if self._allow_dynamic:
                schema = f"tenant_{norm_id}"
                cfg = TenantConfig(
                    tenant_id=norm_id,
                    pg_schema=schema,
                    neo4j_namespace=norm_id,
                    qdrant_prefix=norm_id,
                    redis_prefix=norm_id,
                )
                self._configs[norm_id] = cfg
                return cfg
            raise TenantNotFoundError(tenant_id)
        return config

    def list_tenants(self) -> list[str]:
        """Return all registered tenant IDs."""
        return list(self._configs.keys())

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
        """Shortcut: resolve tenant -> prefixed Redis key using normalised tenant ID."""
        prefix = self.resolve(tenant_id).redis_prefix
        return f"{prefix}:{key}"


# Module-level singleton router and configuration hook
_global_router: TenantRouter = TenantRouter()


def get_tenant_router() -> TenantRouter:
    """Return the global active TenantRouter singleton."""
    global _global_router
    return _global_router


def configure_tenant_router(
    app_env: str = "production",
    tenant_org_map: dict[str, str] | None = None,
    allow_dynamic: bool | None = None,
    default_tenant_id: str = "default",
) -> TenantRouter:
    """Configure and set the global TenantRouter instance."""
    global _global_router
    _global_router = TenantRouter(
        app_env=app_env,
        tenant_org_map=tenant_org_map,
        allow_dynamic=allow_dynamic,
        default_tenant_id=default_tenant_id,
    )
    try:
        from . import audit_log, consent, db

        db._router = _global_router
        audit_log._router = _global_router
        consent._router = _global_router
    except ImportError:
        pass
    return _global_router
