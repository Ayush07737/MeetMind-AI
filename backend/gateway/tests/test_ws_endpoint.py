"""Tests for MeetMind AI Gateway - WebSocket endpoint integration.

Covers:
- One-time WebSocket ticket exchange and consumption
- Replay and expired ticket rejection (close code 4401)
- WSCloseCode enforcement on auth failures and writer conflicts (4409)
- Consent enforcement and frame routing
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from conftest import TEST_JWKS_URL, make_test_jwt
from fastapi.testclient import TestClient
from meetmind_gateway.app import create_app
from meetmind_gateway.config import GatewaySettings
from meetmind_schemas.events import WSCloseCode
from starlette.websockets import WebSocketDisconnect


@pytest.fixture
def test_settings() -> GatewaySettings:
    return GatewaySettings(
        neon_database_url="postgresql://test:test@localhost/test",
        redis_url="redis://localhost:6379/0",
        clerk_jwks_url=TEST_JWKS_URL,
        clerk_secret_key="sk_test_secret_32_chars_long_val!",
        clerk_publishable_key="pk_test_key",
        default_tenant_id="test_tenant",
        app_env="development",
    )


@pytest.fixture
def app_with_mocks(
    test_settings, fake_redis, authenticator, seq_counter, stream_producer, session_manager
):
    """Create gateway FastAPI app with FakeRedis and test authenticator."""
    app = create_app(test_settings)
    app.state.redis = fake_redis
    app.state.redis_binary = fake_redis
    app.state.authenticator = authenticator
    app.state.seq_counter = seq_counter
    app.state.stream_producer = stream_producer
    app.state.session_manager = session_manager
    return app


@pytest.fixture(autouse=True)
def mock_audit_write():
    with patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock) as m:
        yield m


class TestWebSocketAuthAndTickets:
    """Verify authentication and one-time ticket lifecycle on WebSocket connect."""

    def test_no_ticket_or_token_rejected(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        with client.websocket_connect("/ws/ingest/mtg_001") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_text()
            assert exc_info.value.code == WSCloseCode.UNAUTHORIZED

    def test_invalid_token_rejected(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        with client.websocket_connect("/ws/ingest/mtg_001?token=garbage_token") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_text()
            assert exc_info.value.code == WSCloseCode.UNAUTHORIZED

    def test_ticket_exchange_and_connect(self, app_with_mocks):
        """Full ticket flow: POST /v1/ws-ticket -> connect with ticket -> success."""
        token = make_test_jwt()
        client = TestClient(app_with_mocks)

        # 1. Exchange JWT for ticket
        resp = client.post("/v1/ws-ticket", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200
        ticket_data = resp.json()
        assert "ticket" in ticket_data
        ticket = ticket_data["ticket"]

        # 2. Connect with ticket
        with (
            patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock),
            client.websocket_connect(f"/v1/meetings/mtg_001/stream?ticket={ticket}") as ws,
        ):
            ws.send_text(json.dumps({"type": "control", "action": "meeting_end"}))
            ack = json.loads(ws.receive_text())
            assert ack["status"] == "accepted"

    def test_replayed_ticket_rejected(self, app_with_mocks):
        """A ticket consumed once cannot be reused (atomic GETDEL)."""
        token = make_test_jwt()
        client = TestClient(app_with_mocks)

        resp = client.post("/v1/ws-ticket", headers={"Authorization": f"Bearer {token}"})
        ticket = resp.json()["ticket"]

        # First connection consumes ticket
        with (
            patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock),
            client.websocket_connect(f"/v1/meetings/mtg_001/stream?ticket={ticket}") as ws,
        ):
            ws.send_text(json.dumps({"type": "control", "action": "meeting_end"}))

        # Replay ticket -> rejected with 4401
        with client.websocket_connect(f"/v1/meetings/mtg_001/stream?ticket={ticket}") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_text()
            assert exc_info.value.code == WSCloseCode.UNAUTHORIZED

    def test_expired_or_unknown_ticket_rejected(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        with client.websocket_connect("/v1/meetings/mtg_001/stream?ticket=non_existent") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_text()
            assert exc_info.value.code == WSCloseCode.UNAUTHORIZED

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    def test_valid_token_accepted_in_dev(self, mock_check, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with (
            patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock),
            client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws,
        ):
            ws.send_text(json.dumps({"type": "control", "action": "meeting_end"}))
            ack = json.loads(ws.receive_text())
            assert ack["status"] == "accepted"


class TestConsentEnforcement:
    """Verify the hard compliance invariant: no data without consent."""

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.write_audit_event", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_audio_before_consent_rejected(
        self, mock_audit_gw, mock_audit_gate, mock_check, app_with_mocks
    ):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_bytes(b"mock_audio_data")
            response = json.loads(ws.receive_text())
            assert response["type"] == "error"
            assert response["code"] == "CONSENT_REQUIRED"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.write_audit_event", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_transcript_before_consent_rejected(
        self, mock_audit_gw, mock_audit_gate, mock_check, app_with_mocks
    ):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "transcript_chunk",
                        "text": "Hello world",
                        "speaker": "speaker_0",
                        "is_final": True,
                        "start_ms": 0,
                        "end_ms": 1000,
                    }
                )
            )
            response = json.loads(ws.receive_text())
            assert response["type"] == "error"
            assert response["code"] == "CONSENT_REQUIRED"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_audio_after_consent_accepted(
        self, mock_audit, mock_record, mock_check, app_with_mocks
    ):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                    }
                )
            )
            ack = json.loads(ws.receive_text())
            assert ack["status"] == "accepted"

            ws.send_bytes(b"mock_audio_data")
            ack = json.loads(ws.receive_text())
            assert ack["status"] == "acked"


class TestFrameProcessing:
    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_transcript_chunk_acked(self, mock_audit, mock_record, mock_check, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                    }
                )
            )
            ws.receive_text()  # consume control ack

            ws.send_text(
                json.dumps(
                    {
                        "type": "transcript_chunk",
                        "text": "Meeting opened.",
                        "speaker": "chair",
                        "is_final": True,
                        "start_ms": 0,
                        "end_ms": 2000,
                    }
                )
            )
            ack = json.loads(ws.receive_text())
            assert ack["status"] == "acked"
            assert ack["frame_type"] == "transcript_chunk"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_sequence_numbers_increment(self, mock_audit, mock_record, mock_check, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                    }
                )
            )
            ws.receive_text()

            ws.send_bytes(b"mock_audio_data")
            ack1 = json.loads(ws.receive_text())
            ws.send_bytes(b"mock_audio_data")
            ack2 = json.loads(ws.receive_text())

            assert ack1["seq"] == 1
            assert ack2["seq"] == 2

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_invalid_json_returns_error(self, mock_audit, mock_check, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text("not valid json {{{")
            response = json.loads(ws.receive_text())
            assert response["type"] == "error"
            assert response["code"] == "INVALID_FRAME"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_unknown_frame_type_returns_error(self, mock_audit, mock_check, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(json.dumps({"type": "unknown_type"}))
            response = json.loads(ws.receive_text())
            assert response["type"] == "error"
            assert response["code"] == "UNKNOWN_FRAME_TYPE"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_audio_published_to_redis_stream(
        self, mock_audit, mock_record, mock_check, app_with_mocks, fake_redis
    ):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                    }
                )
            )
            ws.receive_text()
            ws.send_bytes(b"mock_audio_data")
            ws.receive_text()  # consume ack

        entries = fake_redis._streams.get("audio:mtg_001", [])
        assert len(entries) == 1


class TestWriterConflict:
    """Verify single-writer exclusivity enforcement."""

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_second_audio_producer_rejected_with_4409(
        self, mock_audit, mock_record, mock_check, app_with_mocks
    ):
        token1 = make_test_jwt(sub="user_1")
        token2 = make_test_jwt(sub="user_2")
        client = TestClient(app_with_mocks)

        with client.websocket_connect(f"/ws/ingest/mtg_conflict?token={token1}") as ws1:
            ws1.send_text(json.dumps({"type": "control", "action": "consent_confirmed"}))
            ws1.receive_text()
            ws1.send_bytes(b"audio_from_writer_1")
            ws1.receive_text()  # writer lock acquired by ws1

            with (
                pytest.raises(WebSocketDisconnect) as exc_info,
                client.websocket_connect(f"/ws/ingest/mtg_conflict?token={token2}") as ws2,
            ):
                ws2.send_text(json.dumps({"type": "control", "action": "consent_confirmed"}))
                ws2.receive_text()
                ws2.send_bytes(b"audio_from_writer_2")  # conflict!
                ws2.receive_text()

            assert exc_info.value.code == WSCloseCode.WRITER_CONFLICT


class TestControlFrameMalformedShapesRejection:
    """CO-3: Revert permissive fallback mapping. Other shapes get INVALID_FRAME."""

    def test_malformed_control_shape_payload_gets_invalid_frame(self, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_ctrl_test?token={token}") as ws:
            # Send permissive legacy payload shape
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "payload": {"consent_type": "audio_capture"},
                    }
                )
            )
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "error"
            assert resp["code"] == "INVALID_FRAME"
            assert "Invalid control frame" in resp["message"]

    def test_malformed_control_shape_metadata_gets_invalid_frame(self, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_ctrl_test?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "metadata": {"consent_type": "audio_capture"},
                    }
                )
            )
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "error"
            assert resp["code"] == "INVALID_FRAME"

    def test_malformed_control_extra_key_gets_invalid_frame(self, app_with_mocks):
        token = make_test_jwt()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_ctrl_test?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {"type": "control", "action": "consent_confirmed", "extra_key": "not_allowed"}
                )
            )
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "error"
            assert resp["code"] == "INVALID_FRAME"
