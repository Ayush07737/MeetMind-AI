"""Tests for MeetMind AI — Shared Python Schemas.

Verifies that schemas_py models match the expected contract:
- AuditEventSchema validates and defaults correctly
- ConsentRecordSchema validates and defaults correctly
- Empty tenant_id/user_id are rejected
"""

import pytest
from meetmind_schemas.events import AuditEventSchema, ConsentRecordSchema
from pydantic import ValidationError


class TestSharedSchemas:
    def test_audit_event_schema_valid(self):
        event = AuditEventSchema(
            tenant_id="default",
            user_id="user_123",
            event_type="consent_confirmed",
            payload={"meeting_id": "mtg_1"},
        )
        assert event.tenant_id == "default"
        assert event.user_id == "user_123"
        assert event.event_type == "consent_confirmed"
        assert event.payload == {"meeting_id": "mtg_1"}
        assert event.created_at is not None

    def test_audit_event_schema_rejects_empty_fields(self):
        with pytest.raises(ValidationError):
            AuditEventSchema(tenant_id="", user_id="user_1", event_type="type")

    def test_consent_record_schema_valid(self):
        record = ConsentRecordSchema(
            tenant_id="default",
            user_id="user_123",
            meeting_id="mtg_456",
            consent_type="audio_capture",
        )
        assert record.tenant_id == "default"
        assert record.user_id == "user_123"
        assert record.meeting_id == "mtg_456"
        assert record.consent_type == "audio_capture"
        assert record.granted_at is not None
        assert record.revoked_at is None

    def test_consent_record_schema_rejects_empty_fields(self):
        with pytest.raises(ValidationError):
            ConsentRecordSchema(tenant_id="t", user_id="u", meeting_id="", consent_type="audio")
