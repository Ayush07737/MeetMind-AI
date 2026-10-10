-- Migration 002: Add extended consent fields to consent_events (WP5)
-- Executed per tenant schema with search_path set to the tenant's schema.

ALTER TABLE consent_events
    ADD COLUMN IF NOT EXISTS external_participants BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS jurisdiction_hint TEXT,
    ADD COLUMN IF NOT EXISTS consent_text_version TEXT NOT NULL DEFAULT 'v1.0',
    ADD COLUMN IF NOT EXISTS client_version TEXT NOT NULL DEFAULT '1.0.0';
