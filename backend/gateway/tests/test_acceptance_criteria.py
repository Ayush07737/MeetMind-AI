"""Acceptance tests for Component #1 — Backend Ingestion Gateway.

Verifies the 6 key requirements:
1. Audio chunk before consent_confirmed rejected and logged to audit table
2. Routing by frame shape (binary vs. JSON), not by adapter_type
3. Kill one backend worker mid-stream — does another worker pick up without losing buffered audio
4. Open two simultaneous listeners on one meeting_id — both get events without
   duplicated upstream processing
5. Send an expired/invalid Clerk JWT — rejected before any frame is processed
6. Sign-off: grep this service for any domain-specific word — returns nothing
"""

from __future__ import annotations

import ast
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from meetmind_gateway.auth import ClerkAuthenticator
from meetmind_gateway.config import GatewaySettings
from meetmind_gateway.frames import AdapterType
from meetmind_gateway.keys import (
    audio_stream_key,
    listeners_key,
    out_stream_key,
    transcript_stream_key,
)
from meetmind_gateway.redis_streams import EventSubscriber, StreamProducer
from meetmind_gateway.sequence import SequenceCounter
from meetmind_gateway.session_manager import SessionManager
from meetmind_gateway.ws_endpoint import websocket_ingest, websocket_listen
from meetmind_schemas.events import WSCloseCode
from starlette.websockets import WebSocketDisconnect
from tests.jwt_test_utils import TEST_JWKS_URL, make_test_jwt

SECRET = "sk_test_secret_32_chars_long_val!"


def _make_token(
    sub: str = "user_test",
    exp_offset: int = 3600,
    **extra_claims: object,
) -> str:
    return make_test_jwt(sub=sub, exp_offset=exp_offset, **extra_claims)


def _build_test_app(fake_redis, settings: GatewaySettings | None = None) -> FastAPI:
    if settings is None:
        settings = GatewaySettings(
            neon_database_url="postgresql://test:test@localhost/test",
            redis_url="redis://localhost:6379/0",
            clerk_jwks_url=TEST_JWKS_URL,
            clerk_secret_key=SECRET,
            clerk_publishable_key="pk_test_key",
            default_tenant_id="test_tenant",
            app_env="development",
        )
    app = FastAPI()
    app.state.settings = settings
    app.state.redis = fake_redis
    app.state.redis_binary = fake_redis
    app.state.authenticator = ClerkAuthenticator(
        jwks_url=settings.clerk_jwks_url,
        default_tenant_id=settings.default_tenant_id,
        app_env="development",
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
            redis_client=fake_redis,
        )

    @app.websocket("/v1/meetings/{meeting_id}/listen")
    async def ws_listen(websocket: WebSocket, meeting_id: str):
        await websocket_listen(
            ws=websocket,
            meeting_id=meeting_id,
            authenticator=app.state.authenticator,
            session_manager=app.state.session_manager,
            redis_client=fake_redis,
        )

    return app


# ── 1. Mock audio_chunk before consent_confirmed rejected AND audited ────────
class TestConsentRejectionAudit:
    @patch(
        "meetmind_gateway.consent_gate.check_consent",
        new_callable=AsyncMock,
        return_value=False,
    )
    @patch("meetmind_gateway.consent_gate.write_audit_event", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_audio_before_consent_rejected_and_audited(
        self, mock_audit_gw, mock_audit_gate, mock_check, fake_redis
    ):
        """Audio before consent must return CONSENT_REQUIRED and log an audit event."""
        app = _build_test_app(fake_redis)
        token = _make_token()
        client = TestClient(app)

        with client.websocket_connect(f"/ws/ingest/mtg_consent_test?token={token}") as ws:
            # Send raw audio bytes before consent
            ws.send_bytes(b"\x00\x01\x02\x03")
            response = json.loads(ws.receive_text())

            # 1. Verify rejected with error response (not processed)
            assert response["type"] == "error"
            assert response["code"] == "CONSENT_REQUIRED"

            # 2. Verify rejection is written to the audit log (not just a 4xx / WS error)
            all_calls = mock_audit_gate.call_args_list + mock_audit_gw.call_args_list
            rejection_calls = [
                c for c in all_calls if c.kwargs.get("event_type") == "frame_rejected_no_consent"
            ]
            assert len(rejection_calls) >= 1
            call_kwargs = rejection_calls[0].kwargs
            assert call_kwargs["payload"]["frame_type"] == "audio_chunk"
            assert call_kwargs["payload"]["meeting_id"] == "mtg_consent_test"
            assert call_kwargs["payload"]["reason"] == "consent_not_confirmed"


# ── 2. Routing by frame shape, not by adapter_type ───────────────────────────
class TestRoutingByFrameShape:
    @patch(
        "meetmind_gateway.consent_gate.check_consent",
        new_callable=AsyncMock,
        return_value=False,
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_routing_by_shape_regardless_of_adapter(
        self, mock_audit, mock_record, mock_check, fake_redis
    ):
        """Binary is routed to audio stream; JSON text is routed to transcript/control."""
        app = _build_test_app(fake_redis)
        token = _make_token()
        client = TestClient(app)

        valid_adapters = [
            AdapterType.CHROME_EXTENSION.value,
            AdapterType.DESKTOP_APP.value,
            AdapterType.MEETING_BOT.value,
        ]

        for adapter in valid_adapters:
            meeting_id = f"mtg_shape_{adapter}"
            with client.websocket_connect(f"/ws/ingest/{meeting_id}?token={token}") as ws:
                # Send control frame (JSON text)
                ws.send_text(
                    json.dumps(
                        {
                            "type": "control",
                            "action": "meeting_start",
                            "adapter_type": adapter,
                        }
                    )
                )
                ws.receive_text()  # consume meeting_start ack
                ws.send_text(
                    json.dumps(
                        {
                            "type": "control",
                            "action": "consent_confirmed",
                            "external_participants": False,
                            "consent_text_version": "v1.0",
                            "client_version": "1.0.0",
                        }
                    )
                )
                ws.receive_text()  # consume consent_confirmed ack

                # Send binary frame (audio) -> goes to audio stream
                ws.send_bytes(b"PCM_AUDIO_BYTES_TEST")
                ack1 = json.loads(ws.receive_text())
                assert ack1["type"] == "ack"

                # Send JSON text frame (transcript) -> goes to transcript stream
                ws.send_text(
                    json.dumps(
                        {
                            "type": "transcript_chunk",
                            "text": f"Transcript chunk from {adapter}",
                            "timestamp_ms": 1000,
                        }
                    )
                )
                ack2 = json.loads(ws.receive_text())
                assert ack2["type"] == "ack"

                # Check Redis streams: audio stream got binary, transcript stream got text
                a_key = audio_stream_key("test_tenant", meeting_id)
                t_key = transcript_stream_key("test_tenant", meeting_id)
                audio_stream = fake_redis._streams.get(a_key, [])
                transcript_stream = fake_redis._streams.get(t_key, [])

                assert len(audio_stream) == 1
                assert audio_stream[0][1]["audio_data"] == b"PCM_AUDIO_BYTES_TEST"
                assert len(transcript_stream) == 1
                assert transcript_stream[0][1]["text"] == f"Transcript chunk from {adapter}"

    def test_ast_check_no_adapter_type_branching_in_ws_endpoint(self):
        """Static analysis: AST check verifying no conditional branch on adapter_type."""
        endpoint_path = Path(__file__).resolve().parents[1] / "meetmind_gateway" / "ws_endpoint.py"
        tree = ast.parse(endpoint_path.read_text(encoding="utf-8"))

        class AdapterBranchVisitor(ast.NodeVisitor):
            def __init__(self):
                self.branches = []

            def visit_If(self, node: ast.If):
                cond_src = ast.unparse(node.test)
                if "adapter_type" in cond_src and (
                    "==" in cond_src or "!=" in cond_src or "in" in cond_src
                ):
                    self.branches.append((node.lineno, cond_src))
                self.generic_visit(node)

        visitor = AdapterBranchVisitor()
        visitor.visit(tree)
        assert len(visitor.branches) == 0, f"Found branching on adapter_type: {visitor.branches}"


# ── 3. Kill one backend worker mid-stream failover ───────────────────────────
class TestWorkerFailover:
    @patch(
        "meetmind_gateway.consent_gate.check_consent",
        new_callable=AsyncMock,
        return_value=False,
    )
    @patch("meetmind_gateway.consent_gate.record_consent", new_callable=AsyncMock)
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_worker_kill_mid_stream_audio_retained(
        self, mock_audit, mock_record, mock_check, fake_redis
    ):
        """When Worker 1 is killed, Worker 2 picks up meeting without losing buffered audio."""
        meeting_id = "mtg_worker_failover"
        token = _make_token()

        # Worker 1 serves initial connection
        app_worker_1 = _build_test_app(fake_redis)
        client_1 = TestClient(app_worker_1)

        with client_1.websocket_connect(f"/ws/ingest/{meeting_id}?token={token}") as ws1:
            ws1.send_text(json.dumps({"type": "control", "action": "meeting_start"}))
            ws1.receive_text()
            ws1.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "external_participants": False,
                        "consent_text_version": "v1.0",
                        "client_version": "1.0.0",
                    }
                )
            )
            ws1.receive_text()
            ws1.send_bytes(b"worker_1_audio_chunk_1")
            ack1 = json.loads(ws1.receive_text())
            assert ack1["seq"] == 1

            ws1.send_bytes(b"worker_1_audio_chunk_2")
            ack2 = json.loads(ws1.receive_text())
            assert ack2["seq"] == 2

        # Worker 1 is killed / crashes mid-stream
        del app_worker_1
        del client_1

        # Consent is confirmed in database for reconnection
        mock_check.return_value = True

        # Worker 2 spins up connected to the same Redis cluster
        app_worker_2 = _build_test_app(fake_redis)
        client_2 = TestClient(app_worker_2)

        with client_2.websocket_connect(f"/ws/ingest/{meeting_id}?token={token}") as ws2:
            # Worker 2 accepts chunk 3 — sequence counter continues monotonically (seq=3)
            ws2.send_bytes(b"worker_2_audio_chunk_3")
            ack3 = json.loads(ws2.receive_text())
            assert ack3["seq"] == 3

        # Verify stream content: all 3 chunks retained in order with correct seq numbers
        audio_stream = fake_redis._streams.get(audio_stream_key("test_tenant", meeting_id), [])
        assert len(audio_stream) == 3
        assert audio_stream[0][1]["audio_data"] == b"worker_1_audio_chunk_1"
        assert audio_stream[0][1]["seq"] == "1"
        assert audio_stream[1][1]["audio_data"] == b"worker_1_audio_chunk_2"
        assert audio_stream[1][1]["seq"] == "2"
        assert audio_stream[2][1]["audio_data"] == b"worker_2_audio_chunk_3"
        assert audio_stream[2][1]["seq"] == "3"


# ── 4. Open two simultaneous listeners on one meeting_id ─────────────────────
class TestSimultaneousListeners:
    @patch(
        "meetmind_gateway.consent_gate.check_consent",
        new_callable=AsyncMock,
        return_value=True,
    )
    @patch("meetmind_gateway.audit.write_audit_event", new_callable=AsyncMock)
    def test_two_listeners_no_duplicate_upstream(self, mock_audit, mock_check, fake_redis):
        """Two listeners connect via /listen and receive outbound events; writer ingests audio."""
        meeting_id = "mtg_dual_listeners"
        app = _build_test_app(fake_redis)
        client = TestClient(app)

        token1 = _make_token(sub="user_1")
        token2 = _make_token(sub="user_2", role="org:admin")
        token_writer = _make_token(sub="user_writer")

        with (
            client.websocket_connect(f"/v1/meetings/{meeting_id}/listen?token={token1}") as _ws1,
            client.websocket_connect(f"/v1/meetings/{meeting_id}/listen?token={token2}") as _ws2,
        ):
            # Both listeners are registered in Redis
            listeners = fake_redis._data.get(listeners_key("test_tenant", meeting_id), set())
            assert len(listeners) == 2

            writer_url = f"/ws/ingest/{meeting_id}?token={token_writer}"
            with client.websocket_connect(writer_url) as ws_writer:
                # Send audio from writer
                ws_writer.send_bytes(b"single_audio_input")
                ack = json.loads(ws_writer.receive_text())
                assert ack["type"] == "ack"

                # Verify audio stream was written exactly ONCE upstream (no duplication)
                a_key = audio_stream_key("test_tenant", meeting_id)
                audio_stream = fake_redis._streams.get(a_key, [])
                assert len(audio_stream) == 1
                assert audio_stream[0][1]["audio_data"] == b"single_audio_input"

    async def test_event_subscriber_fanout(self, fake_redis):
        """Direct subscriber test: events published to out_stream
        fan out to subscribers.
        """
        received_events_1 = []
        received_events_2 = []

        sub1 = EventSubscriber(fake_redis)
        sub2 = EventSubscriber(fake_redis)

        async def callback1(data):
            received_events_1.append(data)

        async def callback2(data):
            received_events_2.append(data)

        meeting_id = "mtg_fanout_test"
        await sub1.subscribe(meeting_id, callback1, tenant_id="test_tenant")
        await sub2.subscribe(meeting_id, callback2, tenant_id="test_tenant")

        # Single upstream publication
        event_data = {"event_type": "intelligence_event", "payload": {"topic": "pricing"}}
        out_key = out_stream_key("test_tenant", meeting_id)
        await fake_redis.xadd(out_key, {"payload": json.dumps(event_data)})

        # Yield control so async listen loops process the message
        import asyncio

        await asyncio.sleep(0.1)

        # Both subscribers received the event from a single publish
        assert len(received_events_1) == 1
        assert received_events_1[0]["event_type"] == "intelligence_event"
        assert len(received_events_2) == 1
        assert received_events_2[0]["event_type"] == "intelligence_event"

        await sub1.unsubscribe()
        await sub2.unsubscribe()


# ── 5. Expired/invalid Clerk JWT rejected before any frame processed ─────────
class TestAuthRejectionBeforeProcessing:
    def test_expired_jwt_rejected_immediately(self, fake_redis):
        """Expired JWT must be rejected with WSCloseCode.UNAUTHORIZED (4401)."""
        app = _build_test_app(fake_redis)
        client = TestClient(app)
        expired_token = _make_token(exp_offset=-100)

        with (
            client.websocket_connect(f"/ws/ingest/mtg_auth_test?token={expired_token}") as ws,
            pytest.raises(WebSocketDisconnect) as exc_info,
        ):
            ws.receive_text()

        assert exc_info.value.code == WSCloseCode.UNAUTHORIZED
        # Ensure session was never created in Redis
        assert "session:mtg_auth_test" not in fake_redis._data

    def test_invalid_signature_jwt_rejected_immediately(self, fake_redis):
        """JWT with invalid signature must be rejected immediately with 4401."""
        app = _build_test_app(fake_redis)
        client = TestClient(app)
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        payload = {
            "sub": "user_test",
            "org_id": "test_tenant",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        bad_token = pyjwt.encode(
            payload,
            other_key,
            algorithm="RS256",
            headers={"kid": "unknown-key"},
        )

        with (
            client.websocket_connect(f"/ws/ingest/mtg_auth_test?token={bad_token}") as ws,
            pytest.raises(WebSocketDisconnect) as exc_info,
        ):
            ws.receive_text()

        assert exc_info.value.code == WSCloseCode.UNAUTHORIZED

    def test_missing_token_rejected_immediately(self, fake_redis):
        """Connection without token must be rejected immediately with 4401."""
        app = _build_test_app(fake_redis)
        client = TestClient(app)

        with (
            client.websocket_connect("/ws/ingest/mtg_auth_test") as ws,
            pytest.raises(WebSocketDisconnect) as exc_info,
        ):
            ws.receive_text()

        assert exc_info.value.code == WSCloseCode.UNAUTHORIZED


# ── 6. Sign-off: Zero domain-specific words in service ────────────────────────
class TestDomainAgnosticSignOff:
    def test_zero_domain_specific_words_in_gateway_service(self):
        """Gateway must not encode any domain assumptions (Finance, Sales, HR, etc.)."""
        import re

        domain_keywords = [
            # Finance / PE / VC
            "carried_interest",
            "waterfall",
            "ebitda",
            "term_sheet",
            "due_diligence",
            "ic_memo",
            "investment_banking",
            "private_equity",
            "portfolio_company",
            # Sales
            "prospect",
            "pipeline",
            "quota",
            "lead_score",
            "close_date",
            # HR
            "offer_letter",
            "headcount",
            "resume",
            # Engineering
            "sprint",
            "pull_request",
            "backlog",
            # Healthcare / Clinical
            "patient",
            "prescription",
            "clinical_trial",
        ]

        src_dir = Path(__file__).resolve().parents[1] / "meetmind_gateway"
        matched = []

        for py_file in src_dir.glob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            for kw in domain_keywords:
                pattern = r"\b" + re.escape(kw) + r"\b"
                if re.search(pattern, text, re.IGNORECASE):
                    matched.append((py_file.name, kw))

        assert len(matched) == 0, f"Found domain-specific terms in gateway: {matched}"
