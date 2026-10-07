from __future__ import annotations

import json
from pathlib import Path

import pytest
from meetmind_schemas.events import AuditEventSchema, ConsentRecordSchema
from pydantic import ValidationError

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
VALID_DIR = FIXTURES_DIR / "valid"
INVALID_DIR = FIXTURES_DIR / "invalid"


class TestSchemaContracts:
    def test_validates_valid_fixtures(self):
        # 1. Audit event
        raw_audit = json.loads((VALID_DIR / "audit_event_1.json").read_text(encoding="utf-8"))
        audit_event = AuditEventSchema.model_validate(raw_audit)
        assert audit_event.tenant_id == "tenant_123"
        assert audit_event.event_type == "meeting_start"

        # 2. Consent record
        raw_consent = json.loads((VALID_DIR / "consent_record_1.json").read_text(encoding="utf-8"))
        consent_record = ConsentRecordSchema.model_validate(raw_consent)
        assert consent_record.tenant_id == "tenant_123"
        assert consent_record.consent_type == "audio_capture"

    def test_rejects_invalid_fixtures(self):
        # 1. Invalid audit event
        invalid_audit_file = INVALID_DIR / "audit_event_empty_tenant.json"
        raw_audit = json.loads(invalid_audit_file.read_text(encoding="utf-8"))
        with pytest.raises(ValidationError):
            AuditEventSchema.model_validate(raw_audit)

        # 2. Invalid consent record
        invalid_consent_file = INVALID_DIR / "consent_record_invalid_type.json"
        raw_consent = json.loads(invalid_consent_file.read_text(encoding="utf-8"))
        with pytest.raises(ValidationError):
            ConsentRecordSchema.model_validate(raw_consent)
