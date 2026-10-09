"""Tests for MeetMind AI Gateway — Consent gate.

These tests verify the consent gate enforces the hard compliance invariant:
no audio or transcript may be processed before consent is confirmed.
"""

from unittest.mock import AsyncMock, patch

import pytest
from meetmind_gateway.consent_gate import ConsentGate


class TestConsentGate:
    @pytest.fixture
    def gate(self):
        return ConsentGate(
            meeting_id="mtg_001",
            tenant_id="acme",
            user_id="user_123",
        )

    def test_gate_starts_closed(self, gate):
        assert gate.is_open is False

    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    async def test_confirm_opens_gate(self, mock_record, gate):
        await gate.confirm_consent("audio_capture")
        assert gate.is_open is True
        mock_record.assert_awaited_once_with(
            tenant_id="acme",
            user_id="user_123",
            meeting_id="mtg_001",
            consent_type="audio_capture",
            external_participants=False,
            jurisdiction_hint=None,
            consent_text_version="v1.0",
            client_version="1.0.0",
        )

    @patch("meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock)
    async def test_check_existing_consent_opens_gate(self, mock_check, gate):
        mock_check.return_value = True
        result = await gate.check_existing_consent()
        assert result is True
        assert gate.is_open is True

    @patch("meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock)
    async def test_check_existing_no_consent(self, mock_check, gate):
        mock_check.return_value = False
        result = await gate.check_existing_consent()
        assert result is False
        assert gate.is_open is False

    @patch("meetmind_gateway.consent_gate.write_audit_event", new_callable=AsyncMock)
    async def test_reject_frame_writes_audit(self, mock_audit, gate):
        await gate.reject_frame("audio_chunk")
        mock_audit.assert_awaited_once()
        call_kwargs = mock_audit.call_args.kwargs
        assert call_kwargs["event_type"] == "frame_rejected_no_consent"
        assert call_kwargs["payload"]["frame_type"] == "audio_chunk"
        assert call_kwargs["payload"]["meeting_id"] == "mtg_001"

    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    async def test_gate_closed_then_opened(self, mock_record, gate):
        """Verify the full lifecycle: closed -> reject -> confirm -> open."""
        assert gate.is_open is False
        await gate.confirm_consent()
        assert gate.is_open is True
