"""Tests for MeetMind AI — Consent Record Service.

These tests verify the consent module's API surface and Pydantic model.

NOTE: Integration tests requiring a live PostgreSQL connection are marked with
@pytest.mark.integration and skipped by default.
"""

import inspect

from meetmind_security.consent import (
    ConsentRecord,
    check_consent,
    record_consent,
    revoke_consent,
)


class TestConsentRecordModel:
    """Verify the ConsentRecord Pydantic model."""

    def test_create_consent_record(self):
        record = ConsentRecord(
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
        assert record.id is not None

    def test_consent_record_serialization(self):
        record = ConsentRecord(
            tenant_id="default",
            user_id="user_123",
            meeting_id="mtg_456",
            consent_type="transcript_storage",
        )
        data = record.model_dump()
        assert "tenant_id" in data
        assert "user_id" in data
        assert "meeting_id" in data
        assert "consent_type" in data
        assert "granted_at" in data
        assert "revoked_at" in data
        assert data["revoked_at"] is None


class TestConsentAPISurface:
    """Verify the consent module exposes the expected async functions."""

    def test_record_consent_is_async(self):
        assert inspect.iscoroutinefunction(record_consent)

    def test_check_consent_is_async(self):
        assert inspect.iscoroutinefunction(check_consent)

    def test_revoke_consent_is_async(self):
        assert inspect.iscoroutinefunction(revoke_consent)
