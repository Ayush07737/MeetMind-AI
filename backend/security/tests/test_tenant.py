"""Tests for MeetMind AI - Multi-Tenancy Routing Layer."""

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

    def test_schema_name_injection_rejected(self):
        """Verify that SQL injection attempts in pg_schema raise ValueError."""
        with pytest.raises(ValueError, match="Invalid pg_schema"):
            TenantConfig(
                tenant_id="bad",
                pg_schema="public; DROP TABLE audit_events; --",
                neo4j_namespace="bad",
                qdrant_prefix="bad",
                redis_prefix="bad",
            )

        with pytest.raises(ValueError, match="Invalid pg_schema"):
            TenantConfig(
                tenant_id="bad2",
                pg_schema="invalid--schema",
                neo4j_namespace="bad",
                qdrant_prefix="bad",
                redis_prefix="bad",
            )

    def test_tenant_id_injection_rejected(self):
        """Verify that invalid tenant_ids raise ValueError in resolve and config."""
        with pytest.raises(ValueError, match="Invalid tenant_id"):
            TenantConfig(
                tenant_id="bad' OR 1=1; --",
                pg_schema="tenant_bad",
                neo4j_namespace="bad",
                qdrant_prefix="bad",
                redis_prefix="bad",
            )

        with pytest.raises(ValueError, match="Invalid tenant identifier"):
            self.router.resolve("bad' OR '1'='1")

    def test_dev_mode_permits_dynamic_tenant_fallback(self):
        from unittest.mock import patch

        with patch.dict("os.environ", {"APP_ENV": "development"}, clear=False):
            router = TenantRouter()
            resolved = router.resolve("dynamic_dev_tenant")
            assert resolved.tenant_id == "dynamic_dev_tenant"
            assert resolved.pg_schema == "tenant_dynamic_dev_tenant"

    def test_prod_mode_rejects_unknown_tenant(self):
        from unittest.mock import patch

        with patch.dict("os.environ", {"APP_ENV": "production"}, clear=False):
            router = TenantRouter()
            with pytest.raises(TenantNotFoundError):
                router.resolve("unregistered_prod_tenant")

    def test_tenant_org_map_env_registers_tenants(self):
        import json
        from unittest.mock import patch

        mapping = {"org_enterprise_1": "tenant_ent_1", "org_enterprise_2": "tenant_ent_2"}
        with patch.dict("os.environ", {"TENANT_ORG_MAP": json.dumps(mapping)}, clear=False):
            router = TenantRouter()
            t1 = router.resolve("tenant_ent_1")
            assert t1.tenant_id == "tenant_ent_1"
            assert t1.pg_schema == "tenant_tenant_ent_1"
            t2 = router.resolve("tenant_ent_2")
            assert t2.tenant_id == "tenant_ent_2"
