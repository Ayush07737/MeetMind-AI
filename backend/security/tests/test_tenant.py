"""Tests for MeetMind AI — Multi-Tenancy Routing Layer."""

import pytest
from meetmind_security.tenant import (
    TenantConfig,
    TenantNotFoundError,
    TenantRouter,
)


class TestTenantRouter:
    """Verify the TenantRouter resolves configs correctly."""

    def setup_method(self):
        """Create a fresh router with test configs."""
        self.configs = {
            "acme": TenantConfig(
                tenant_id="acme",
                pg_schema="tenant_acme",
                neo4j_namespace="acme",
                qdrant_prefix="acme",
                redis_prefix="acme",
            ),
            "globex": TenantConfig(
                tenant_id="globex",
                pg_schema="tenant_globex",
                neo4j_namespace="globex",
                qdrant_prefix="globex",
                redis_prefix="globex",
            ),
        }
        self.router = TenantRouter(configs=self.configs)

    def test_resolve_existing_tenant(self):
        config = self.router.resolve("acme")
        assert config.tenant_id == "acme"
        assert config.pg_schema == "tenant_acme"
        assert config.neo4j_namespace == "acme"

    def test_resolve_unknown_tenant_raises(self):
        with pytest.raises(TenantNotFoundError):
            self.router.resolve("unknown_corp")

    def test_list_tenants(self):
        tenants = self.router.list_tenants()
        assert set(tenants) == {"acme", "globex"}

    def test_register_new_tenant(self):
        new_config = TenantConfig(
            tenant_id="initech",
            pg_schema="tenant_initech",
            neo4j_namespace="initech",
            qdrant_prefix="initech",
            redis_prefix="initech",
        )
        self.router.register(new_config)
        resolved = self.router.resolve("initech")
        assert resolved.tenant_id == "initech"

    def test_pg_schema_shortcut(self):
        assert self.router.pg_schema("acme") == "tenant_acme"

    def test_neo4j_namespace_shortcut(self):
        assert self.router.neo4j_namespace("globex") == "globex"

    def test_qdrant_collection_shortcut(self):
        result = self.router.qdrant_collection("acme", "meetings")
        assert result == "acme_meetings"

    def test_redis_key_shortcut(self):
        result = self.router.redis_key("globex", "session:123")
        assert result == "globex:session:123"

    def test_tenant_config_is_frozen(self):
        config = self.router.resolve("acme")
        with pytest.raises(AttributeError):
            config.tenant_id = "tampered"  # type: ignore[misc]
