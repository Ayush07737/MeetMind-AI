"""Tests for MeetMind AI Gateway — WebSocket endpoint integration.

These tests use FastAPI's TestClient to simulate full WebSocket lifecycle.
All external dependencies (Redis, Clerk, consent service) are mocked.
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, patch

import jwt as pyjwt
import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from meetmind_gateway.auth import ClerkAuthenticator
from meetmind_gateway.config import GatewaySettings
from starlette.websockets import WebSocketDisconnect


def _make_token(
    sub: str = "user_test",
    secret: str = "sk_test_secret_32_chars_long_val!",
    exp_offset: int = 3600,
    **extra_claims: object,
) -> str:
    """Create a valid HS256 JWT for testing."""
    payload = {
        "sub": sub,
        "exp": int(time.time()) + exp_offset,
        "iat": int(time.time()),
        **extra_claims,
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def test_settings() -> GatewaySettings:
    return GatewaySettings(
        neon_database_url="postgresql://test:test@localhost/test",
        redis_url="redis://localhost:6379/0",
        clerk_secret_key="sk_test_secret_32_chars_long_val!",
        clerk_publishable_key="pk_test_key",
        default_tenant_id="test_tenant",
    )


@pytest.fixture
def app_with_mocks(test_settings, fake_redis):
    """Create a FastAPI app with mocked Redis and auth."""
    app = FastAPI()
    app.state.settings = test_settings
    app.state.redis = fake_redis
    app.state.redis_binary = fake_redis

    from meetmind_gateway.redis_streams import StreamProducer
    from meetmind_gateway.sequence import SequenceCounter
    from meetmind_gateway.session_manager import SessionManager
    from meetmind_gateway.ws_endpoint import websocket_ingest

    app.state.authenticator = ClerkAuthenticator(
        clerk_secret_key=test_settings.clerk_secret_key,
        default_tenant_id=test_settings.default_tenant_id,
    )
    app.state.session_manager = SessionManager(fake_redis)
    app.state.seq_counter = SequenceCounter(fake_redis)
    app.state.stream_producer = StreamProducer(fake_redis, maxlen=1000)

    @app.websocket("/ws/ingest/{meeting_id}")
    async def ws_ingest(websocket: WebSocket, meeting_id: str):
        await websocket_ingest(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            seq_counter=app.state.seq_counter,
            stream_producer=app.state.stream_producer,
            redis_client=app.state.redis,
        )

    return app


class TestWebSocketAuth:
    """Verify authentication on WebSocket connect."""

    def test_no_token_rejected(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        with pytest.raises(WebSocketDisconnect), client.websocket_connect("/ws/ingest/mtg_001"):
            pass  # Should not reach here

    def test_invalid_token_rejected(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        with (
            pytest.raises(WebSocketDisconnect),
            client.websocket_connect("/ws/ingest/mtg_001?token=garbage"),
        ):
            pass

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    def test_valid_token_accepted(self, mock_check, app_with_mocks):
        token = _make_token()
        client = TestClient(app_with_mocks)
        with (
            patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock),
            client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws,
        ):
            # Connection accepted -- send meeting_end to close cleanly
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_end",
                    }
                )
            )


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
        token = _make_token()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            # Send audio before consent
            ws.send_bytes(b"\x00\x01\x02\x03")
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
        token = _make_token()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "transcript_chunk",
                        "text": "Hello world",
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
        token = _make_token()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            # Confirm consent first
            ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                    }
                )
            )

            # Now send audio -- should get ack
            ws.send_bytes(b"\x00\x01\x02\x03")
            response = json.loads(ws.receive_text())
            assert response["type"] == "ack"
            assert response["seq"] == 1


class TestFrameProcessing:
    """Verify correct frame handling after consent is confirmed."""

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_transcript_chunk_acked(self, mock_audit, mock_record, mock_check, app_with_mocks):
        token = _make_token()
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
            ws.send_text(
                json.dumps(
                    {
                        "type": "transcript_chunk",
                        "text": "The quarterly results show growth",
                    }
                )
            )
            response = json.loads(ws.receive_text())
            assert response["type"] == "ack"
            assert response["seq"] == 1

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_sequence_numbers_increment(self, mock_audit, mock_record, mock_check, app_with_mocks):
        token = _make_token()
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
            # Send 3 audio chunks
            for i in range(3):
                ws.send_bytes(b"\x00" * 100)
                response = json.loads(ws.receive_text())
                assert response["seq"] == i + 1

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_invalid_json_returns_error(self, mock_audit, mock_check, app_with_mocks):
        token = _make_token()
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
        token = _make_token()
        client = TestClient(app_with_mocks)
        with client.websocket_connect(f"/ws/ingest/mtg_001?token={token}") as ws:
            ws.send_text(json.dumps({"type": "unknown_type"}))
            response = json.loads(ws.receive_text())
            assert response["type"] == "error"
            assert response["code"] == "INVALID_FRAME"

    @patch(
        "meetmind_gateway.consent_gate.check_consent", new_callable=AsyncMock, return_value=False
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_audio_published_to_redis_stream(
        self, mock_audit, mock_record, mock_check, app_with_mocks, fake_redis
    ):
        token = _make_token()
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
            ws.send_bytes(b"\xde\xad\xbe\xef")
            ws.receive_text()  # consume ack

        # Verify the audio landed in the Redis stream
        assert "audio:mtg_001" in fake_redis._streams
        entries = fake_redis._streams["audio:mtg_001"]
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields["audio_data"] == b"\xde\xad\xbe\xef"
        assert fields["seq"] == "1"
