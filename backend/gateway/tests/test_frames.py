"""Tests for MeetMind AI Gateway — Frame models."""

import json

import pytest
from meetmind_gateway.frames import (
    AckFrame,
    AdapterType,
    ControlAction,
    ControlFrame,
    ErrorCode,
    ErrorFrame,
    IntelligenceEventFrame,
    TranscriptChunkFrame,
)
from pydantic import ValidationError


class TestControlFrame:
    def test_create_meeting_start(self):
        frame = ControlFrame(
            action=ControlAction.MEETING_START,
            adapter_type=AdapterType.CHROME_EXTENSION,
        )
        assert frame.type == "control"
        assert frame.action == ControlAction.MEETING_START
        assert frame.adapter_type == AdapterType.CHROME_EXTENSION

    def test_create_consent_confirmed(self):
        frame = ControlFrame(
            action=ControlAction.CONSENT_CONFIRMED,
            consent_type="audio_capture",
        )
        assert frame.action == ControlAction.CONSENT_CONFIRMED
        assert frame.consent_type == "audio_capture"

    def test_create_meeting_end(self):
        frame = ControlFrame(action=ControlAction.MEETING_END)
        assert frame.action == ControlAction.MEETING_END

    def test_serialization_roundtrip(self):
        frame = ControlFrame(
            action=ControlAction.MEETING_START,
            adapter_type=AdapterType.DESKTOP_APP,
        )
        data = json.loads(frame.model_dump_json())
        assert data["type"] == "control"
        assert data["action"] == "meeting_start"
        assert data["adapter_type"] == "desktop_app"

    def test_invalid_action_rejected(self):
        with pytest.raises(ValidationError):
            ControlFrame(action="invalid_action")

    def test_malformed_shape_with_payload_rejected_by_extra_forbid(self):
        """CO-3: Legacy or fallback payload dict must be rejected with ValidationError."""
        with pytest.raises(ValidationError) as exc:
            ControlFrame.model_validate(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "payload": {"consent_type": "audio_capture"},
                }
            )
        assert "Extra inputs are not permitted" in str(exc.value)

    def test_malformed_shape_with_metadata_rejected_by_extra_forbid(self):
        """CO-3: Metadata dict on ControlFrame must be rejected."""
        with pytest.raises(ValidationError) as exc:
            ControlFrame.model_validate(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "metadata": {"consent_type": "audio_capture"},
                }
            )
        assert "Extra inputs are not permitted" in str(exc.value)

    def test_malformed_shape_with_extra_field_rejected(self):
        """CO-3: Arbitrary extra keys must be rejected."""
        with pytest.raises(ValidationError) as exc:
            ControlFrame.model_validate(
                {
                    "type": "control",
                    "action": "consent_confirmed",
                    "unknown_field": "disallowed",
                }
            )
        assert "Extra inputs are not permitted" in str(exc.value)

    def test_malformed_shape_missing_action_rejected(self):
        """CO-3: Control frame without action field must be rejected."""
        with pytest.raises(ValidationError) as exc:
            ControlFrame.model_validate(
                {
                    "type": "control",
                    "consent_type": "audio_capture",
                }
            )
        assert "Field required" in str(exc.value)

    def test_canonical_consent_confirmed_accepted(self):
        """CO-3: Canonical shape with action and optional consent_type is accepted."""
        frame = ControlFrame.model_validate(
            {
                "type": "control",
                "action": "consent_confirmed",
                "consent_type": "audio_capture",
            }
        )
        assert frame.action == ControlAction.CONSENT_CONFIRMED
        assert frame.consent_type == "audio_capture"


class TestTranscriptChunkFrame:
    def test_create_transcript(self):
        frame = TranscriptChunkFrame(text="Hello, world")
        assert frame.type == "transcript_chunk"
        assert frame.text == "Hello, world"
        assert frame.timestamp_ms > 0

    def test_empty_text_rejected(self):
        with pytest.raises(ValidationError):
            TranscriptChunkFrame(text="")

    def test_client_seq_optional(self):
        frame = TranscriptChunkFrame(text="test", client_seq=42)
        assert frame.client_seq == 42

        frame2 = TranscriptChunkFrame(text="test")
        assert frame2.client_seq is None


class TestServerFrames:
    def test_ack_frame(self):
        frame = AckFrame(seq=42)
        data = json.loads(frame.model_dump_json())
        assert data["type"] == "ack"
        assert data["seq"] == 42

    def test_error_frame(self):
        frame = ErrorFrame(
            code=ErrorCode.CONSENT_REQUIRED,
            message="Consent required",
        )
        data = json.loads(frame.model_dump_json())
        assert data["type"] == "error"
        assert data["code"] == "CONSENT_REQUIRED"

    def test_intelligence_event_frame(self):
        frame = IntelligenceEventFrame(
            event_type="trigger_detected",
            payload={"confidence": 0.95},
            meeting_id="mtg_123",
        )
        data = json.loads(frame.model_dump_json())
        assert data["type"] == "intelligence_event"
        assert data["event_type"] == "trigger_detected"
        assert data["payload"]["confidence"] == 0.95
