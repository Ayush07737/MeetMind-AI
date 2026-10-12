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
        """K2: Injected app_env='development' enables dynamic fallback for dev."""
        router = TenantRouter(app_env="development")
        resolved = router.resolve("dynamic_dev_tenant")
        assert resolved.tenant_id == "dynamic_dev_tenant"
        assert resolved.pg_schema == "tenant_dynamic_dev_tenant"

    def test_prod_mode_rejects_unknown_tenant(self):
        """K2: Injected app_env='production' rejects unknown tenants."""
        router = TenantRouter(app_env="production")
        with pytest.raises(TenantNotFoundError):
            router.resolve("unregistered_prod_tenant")

    def test_tenant_org_map_injected_registers_tenants(self):
        """K2: Injected tenant_org_map registers configured tenants."""
        mapping = {"org_enterprise_1": "tenant_ent_1", "org_enterprise_2": "tenant_ent_2"}
        router = TenantRouter(app_env="production", tenant_org_map=mapping)
        t1 = router.resolve("tenant_ent_1")
        assert t1.tenant_id == "tenant_ent_1"
        assert t1.pg_schema == "tenant_tenant_ent_1"
        t2 = router.resolve("tenant_ent_2")
        assert t2.tenant_id == "tenant_ent_2"

    def test_malformed_tenant_org_map_fails(self):
        """K2: Malformed tenant_org_map entries fail with clear ValueError."""
        with pytest.raises(ValueError, match="Invalid tenant_org_map"):
            TenantRouter(tenant_org_map={"org_bad": 12345})  # type: ignore[dict-item]

    def test_k3_tenant_id_normalised_lowercase(self):
        """K3: Tenant IDs are normalised lowercase and must match ^[a-z0-9_]{1,40}$."""
        router = TenantRouter(app_env="development")
        # Mixed-case resolve should normalise to lowercase
        resolved = router.resolve("Acme_Corp")
        assert resolved.tenant_id == "acme_corp"
        assert resolved.pg_schema == "tenant_acme_corp"
        assert resolved.redis_prefix == "acme_corp"

    def test_k3_tenant_id_length_and_characters_enforced(self):
        """K3: Tenant IDs longer than 40 chars or with invalid chars are rejected."""
        router = TenantRouter(app_env="development")
        # 41 chars -> rejected
        too_long = "a" * 41
        with pytest.raises(ValueError, match=r"\^\[a-z0-9_\]\{1,40\}\$"):
            router.resolve(too_long)

        # Invalid characters (hyphen, space, punctuation) -> rejected
        with pytest.raises(ValueError, match=r"\^\[a-z0-9_\]\{1,40\}\$"):
            router.resolve("tenant-with-hyphen")

    def test_k3_registration_duplicate_case_insensitive_and_collision(self):
        """K3: Registration rejects conflicting schema collision case-insensitively."""
        router = TenantRouter(app_env="production")
        # First registration
        cfg1 = TenantConfig(
            tenant_id="corp_x",
            pg_schema="tenant_corp_x",
            neo4j_namespace="corp_x",
            qdrant_prefix="corp_x",
            redis_prefix="corp_x",
        )
        router.register(cfg1)

        # Same config registered again with different case is accepted idempotently
        cfg1_upper = TenantConfig(
            tenant_id="CORP_X",
            pg_schema="tenant_corp_x",
            neo4j_namespace="corp_x",
            qdrant_prefix="corp_x",
            redis_prefix="corp_x",
        )
        router.register(cfg1_upper)

        # Conflicting schema for same normalised tenant ID fails with collision ValueError
        cfg_conflict = TenantConfig(
            tenant_id="CORP_X",
            pg_schema="tenant_conflicting_schema",
            neo4j_namespace="corp_x",
            qdrant_prefix="corp_x",
            redis_prefix="corp_x",
        )
        with pytest.raises(ValueError, match="Tenant collision"):
            router.register(cfg_conflict)

    def test_k3_redis_keys_use_normalised_id(self):
        """K3: Redis key shortcuts use normalized lowercase tenant ID."""
        router = TenantRouter(app_env="development")
        key = router.redis_key("ACME_CORP", "session:abc")
        assert key == "acme_corp:session:abc"
