-- Migration 001: Initial Schema (Audit & Consent Append-Only)
-- Executed per tenant schema with search_path set to the tenant's schema.

-- 1. Schema Migrations Tracking Table
CREATE TABLE IF NOT EXISTS _schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Audit Events Table (Strictly Append-Only)
CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant
    ON audit_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_user
    ON audit_events (tenant_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_type
    ON audit_events (event_type, created_at DESC);

-- 3. Consent Events Table (Strictly Append-Only)
CREATE TABLE IF NOT EXISTS consent_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    meeting_id TEXT NOT NULL,
    consent_type TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('granted', 'revoked')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consent_events_lookup
    ON consent_events (tenant_id, user_id, meeting_id, consent_type, created_at DESC);

-- 4. PostgreSQL Immutability Trigger Function & Triggers
CREATE OR REPLACE FUNCTION prevent_audit_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Table % is strictly append-only: updates and deletes are forbidden', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_events_immutable ON audit_events;
CREATE TRIGGER trg_audit_events_immutable
BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION prevent_audit_modification();

DROP TRIGGER IF EXISTS trg_consent_events_immutable ON consent_events;
CREATE TRIGGER trg_consent_events_immutable
BEFORE UPDATE OR DELETE ON consent_events
FOR EACH ROW EXECUTE FUNCTION prevent_audit_modification();

-- 5. Role separation for app user (if role exists)
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'meetmind_app') THEN
        REVOKE UPDATE, DELETE, TRUNCATE ON audit_events, consent_events FROM meetmind_app;
        GRANT SELECT, INSERT ON audit_events, consent_events TO meetmind_app;
    END IF;
END $$;
