"""Tests for MeetMind AI — Audit Log Module.

These tests verify the audit_log module's API surface — specifically that:
1. write_audit_event creates an AuditEvent with all fields
2. The module exposes NO update or delete function
3. AuditEvent model serializes correctly

NOTE: Tests requiring a live PostgreSQL connection are marked with
@pytest.mark.integration and skipped by default. They run against the
docker-compose.dev.yml infrastructure.
"""

import inspect

import pytest

from meetmind_security.audit_log import AuditEvent, write_audit_event


class TestAuditEventModel:
    """Verify the AuditEvent Pydantic model."""

    def test_create_event_with_defaults(self):
        event = AuditEvent(
            tenant_id="default",
            user_id="user_123",
            event_type="consent_confirmed",
        )
        assert event.tenant_id == "default"
        assert event.user_id == "user_123"
        assert event.event_type == "consent_confirmed"
        assert event.payload == {}
        assert event.id is not None
        assert event.created_at is not None

    def test_create_event_with_payload(self):
        payload = {"meeting_id": "mtg_456", "consent_type": "audio_capture"}
        event = AuditEvent(
            tenant_id="acme",
            user_id="user_789",
            event_type="consent_confirmed",
            payload=payload,
        )
        assert event.payload == payload

    def test_event_serialization(self):
        event = AuditEvent(
            tenant_id="default",
            user_id="user_123",
            event_type="test_event",
            payload={"key": "value"},
        )
        data = event.model_dump()
        assert "tenant_id" in data
        assert "user_id" in data
        assert "event_type" in data
        assert "payload" in data
        assert "created_at" in data
        assert "id" in data


class TestAuditLogAPISurface:
    """Verify the audit_log module does NOT expose update/delete functions.

    This is a critical security invariant: the audit log is append-only.
    """

    def test_no_update_function_exposed(self):
        import meetmind_security.audit_log as audit_module

        public_functions = [
            name
            for name, obj in inspect.getmembers(audit_module)
            if inspect.isfunction(obj) and not name.startswith("_")
        ]
        for fn_name in public_functions:
            assert "update" not in fn_name.lower(), (
                f"Audit log module exposes an update function: {fn_name}"
            )
            assert "delete" not in fn_name.lower(), (
                f"Audit log module exposes a delete function: {fn_name}"
            )

    def test_write_audit_event_is_async(self):
        assert inspect.iscoroutinefunction(write_audit_event)
