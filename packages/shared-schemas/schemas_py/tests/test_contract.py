from __future__ import annotations

import json
from pathlib import Path

import pytest
from meetmind_schemas.events import (
    AckFrame,
    AuditEventSchema,
    ConsentRecordSchema,
    ControlFrame,
    ErrorFrame,
    IntelligenceEventFrame,
    TranscriptChunkFrame,
    decode_audio_frame,
    encode_audio_frame,
)
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

    def test_gateway_frames_fixtures(self):
        gf = json.loads((FIXTURES_DIR / "gateway_frame_fixtures.json").read_text(encoding="utf-8"))

        for frame_dict in gf["valid_control"]:
            cf = ControlFrame.model_validate(frame_dict)
            assert cf.type == "control"
            if cf.action == "consent_confirmed":
                assert cf.external_participants is True
                assert cf.consent_text_version == "v1.0"
                assert cf.client_version == "1.2.3"

        for invalid_case in gf["invalid_control"]:
            with pytest.raises(ValidationError):
                ControlFrame.model_validate(invalid_case["frame"])

        for tf in gf["valid_transcript"]:
            tcf = TranscriptChunkFrame.model_validate(tf)
            assert tcf.type == "transcript_chunk"
            assert tcf.text == tf["text"]

        for af in gf["valid_ack"]:
            ack = AckFrame.model_validate(af)
            assert ack.type == "ack"
            assert ack.seq == af["seq"]

        for ef in gf["valid_error"]:
            err = ErrorFrame.model_validate(ef)
            assert err.type == "error"
            assert err.code == ef["code"]

        for inf in gf["valid_intelligence"]:
            intel = IntelligenceEventFrame.model_validate(inf)
            assert intel.type == "intelligence_event"
            assert intel.event_type == inf["event_type"]

    def test_binary_audio_header_codec_fixtures(self):
        af = json.loads((FIXTURES_DIR / "audio_frame_fixtures.json").read_text(encoding="utf-8"))

        for case in af["valid"]:
            raw_bytes = bytes.fromhex(case["hex"])
            header, payload = decode_audio_frame(raw_bytes)
            assert header.version == case["header"]["version"]
            assert header.flags == case["header"]["flags"]
            assert header.client_seq == case["header"]["client_seq"]
            assert header.capture_ts_ms == case["header"]["capture_ts_ms"]
            assert payload.hex() == case["payload_hex"]

            # Round trip encode
            re_encoded = encode_audio_frame(
                client_seq=header.client_seq,
                capture_ts_ms=header.capture_ts_ms,
                payload=payload,
                flags=header.flags,
                version=header.version,
            )
            assert re_encoded == raw_bytes

        for invalid_case in af["invalid"]:
            raw_bytes = bytes.fromhex(invalid_case["hex"])
            with pytest.raises(ValueError):
                decode_audio_frame(raw_bytes)
