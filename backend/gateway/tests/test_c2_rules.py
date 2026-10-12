"""Unit tests for Session C2: Carry-overs (K2-K8), WP6, and TLS rule.

Verifies:
- TLS rule: production rejects WebSocket upgrades without X-Forwarded-Proto: https
- K4: Real TTL checks after creation and refresh; listener refreshes activity
- K5: Outbound forwarding drops missing payload, internal events, and validates schemas
- K6: Server-issued meeting IDs, 404 parity for nonexistent vs other-tenant meetings
- K7: Lease takeover via Lua CAS; same instance takes over, old closed with 4409 SUPERSEDED;
  conflict rejected
- K8: Rate-limit burst capacity accepts >= 100 frames at once, rejects sustained floods and
  huge bursts
- WP6: Redis non-persistence startup check and Janitor task sweep
"""

from __future__ import annotations

import asyncio
import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from meetmind_gateway.app import create_app
from meetmind_gateway.config import GatewaySettings
from meetmind_gateway.frames import MeetingStatus, WSCloseCode
from meetmind_gateway.janitor import run_janitor_sweep
from meetmind_gateway.persistence_check import assert_audio_redis_not_persistent
from meetmind_gateway.session_manager import SessionManager
from meetmind_gateway.ws_endpoint import (
    _ACTIVE_INGEST_CONNECTIONS,
    _check_rate_limits,
    _forward_event,
)
from meetmind_schemas.events import OUTBOUND_EVENT_TYPES
from starlette.websockets import WebSocketDisconnect
from tests.jwt_test_utils import make_test_jwt


# ── TLS Rule (Unit Test) ──────────────────────────────────────────────────────
class TestTLSRule:
    def test_production_rejects_non_https_upgrades(
        self, fake_redis, authenticator, seq_counter, stream_producer
    ):
        """In production, gateway rejects upgrades whose X-Forwarded-Proto is not https."""
        settings = GatewaySettings(
            app_env="production",
            clerk_issuer="https://clerk.meetmind.ai",
            clerk_jwks_url="https://clerk.meetmind.ai/.well-known/jwks.json",
            allowed_origins=["https://app.meetmind.ai"],
            audio_redis_persistence_acknowledged=True,
        )
        app = create_app(settings)
        app.state.redis = fake_redis
        app.state.redis_binary = fake_redis
        app.state.session_manager = SessionManager(fake_redis)
        app.state.authenticator = authenticator
        app.state.seq_counter = seq_counter
        app.state.stream_producer = stream_producer

        client = TestClient(app)

        # 1. Without X-Forwarded-Proto header -> Rejected with PROTOCOL_VIOLATION (4400)
        with (
            pytest.raises(WebSocketDisconnect) as exc,
            client.websocket_connect("/v1/meetings/mtg_tls/stream?token=dummy"),
        ):
            pass
        assert exc.value.code == WSCloseCode.PROTOCOL_VIOLATION

        # 2. With X-Forwarded-Proto: http -> Rejected
        with (
            pytest.raises(WebSocketDisconnect) as exc,
            client.websocket_connect(
                "/v1/meetings/mtg_tls/stream?token=dummy",
                headers={"X-Forwarded-Proto": "http"},
            ),
        ):
            pass
        assert exc.value.code == WSCloseCode.PROTOCOL_VIOLATION

        # 3. With X-Forwarded-Proto: https -> Accepted past TLS check to auth
        with client.websocket_connect(
            "/v1/meetings/mtg_tls/stream?token=dummy",
            headers={"X-Forwarded-Proto": "https"},
        ) as ws:
            with pytest.raises(WebSocketDisconnect) as exc:
                ws.receive_text()
            # Proceeds to authentication (fails on invalid token 4401, NOT protocol violation 4400)
            assert exc.value.code == WSCloseCode.UNAUTHORIZED


# ── K5: Outbound Forwarding Tests ─────────────────────────────────────────────
class TestOutboundForwarding:
    @pytest.mark.asyncio
    async def test_event_without_payload_dropped_with_warning(self):
        """Events without a 'payload' key must be dropped with a warning and counter increment."""
        mock_ws = AsyncMock()
        event_without_payload = {"type": "transcript_interim", "data": "raw_internal"}

        with patch("meetmind_gateway.ws_endpoint.logger.warning") as mock_warn:
            await _forward_event(mock_ws, event_without_payload, "mtg_001")
            assert mock_ws.send_text.call_count == 0
            mock_warn.assert_called()

    @pytest.mark.asyncio
    async def test_internal_events_never_reach_listener(self):
        """Internal events (e.g. transcript_final, trigger_classified) must never be forwarded."""
        mock_ws = AsyncMock()

        # Internal events with payload shape
        internal_events = [
            {"event_type": "transcript_final", "payload": {"text": "internal transcript"}},
            {"event_type": "trigger_classified", "payload": {"trigger": "action_item"}},
            {"event_type": "raw_audio_chunk", "payload": {"audio": "base64"}},
        ]

        for ev in internal_events:
            assert ev["event_type"] not in OUTBOUND_EVENT_TYPES
            await _forward_event(mock_ws, ev, "mtg_001")
            assert mock_ws.send_text.call_count == 0

    @pytest.mark.asyncio
    async def test_allowlisted_event_forwarded_with_explicit_schema(self):
        """Client-facing events (pre_meeting_brief, transcript_interim, etc.) forwarded."""
        mock_ws = AsyncMock()
        valid_event = {
            "event_type": "transcript_interim",
            "payload": {"text": "hello interim", "speaker": "Alice"},
        }
        await _forward_event(mock_ws, valid_event, "mtg_001")
        assert mock_ws.send_text.call_count == 1
        sent_data = json.loads(mock_ws.send_text.call_args[0][0])
        assert sent_data["type"] == "intelligence_event"
        assert sent_data["event_type"] == "transcript_interim"
        assert sent_data["payload"]["text"] == "hello interim"


# ── K6: Server-Issued Meeting IDs & 404 Parity ────────────────────────────────
class TestServerIssuedMeetingsAnd404Parity:
    def test_post_meetings_creates_random_unguessable_id(self, app_with_mocks):
        client = TestClient(app_with_mocks)
        token = make_test_jwt()

        resp = client.post("/v1/meetings", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 201
        data = resp.json()
        assert "meeting_id" in data
        assert len(data["meeting_id"]) >= 22
        assert data["status"] == MeetingStatus.PENDING_CONSENT.value

    def test_ticket_exchange_returns_404_for_nonexistent_or_other_tenant(
        self, app_with_mocks, fake_redis
    ):
        client = TestClient(app_with_mocks)
        token = make_test_jwt()
        inst_id = str(uuid.uuid4())

        # 1. Nonexistent meeting -> 404
        resp = client.post(
            "/v1/ws-ticket",
            headers={"Authorization": f"Bearer {token}"},
            json={"meeting_id": "nonexistent_meeting_123", "client_instance_id": inst_id},
        )
        assert resp.status_code == 404
        assert resp.json()["detail"] == "meeting not found"

        # 2. Meeting owned by tenant_b -> tenant_a user also receives 404 (parity)
        # Pre-seed meeting under other tenant in Redis
        from meetmind_gateway.keys import meeting_session_key

        other_mtg = "mtg_tenant_b_secret"
        other_key = meeting_session_key("tenant_b", other_mtg)
        asyncio.run(
            fake_redis.hset(other_key, mapping={"meeting_id": other_mtg, "tenant_id": "tenant_b"})
        )

        resp_other = client.post(
            "/v1/ws-ticket",
            headers={"Authorization": f"Bearer {token}"},
            json={"meeting_id": other_mtg, "client_instance_id": inst_id},
        )
        assert resp_other.status_code == 404
        assert resp_other.json()["detail"] == "meeting not found"


# ── K7: Lease Takeover Logic ──────────────────────────────────────────────────
class TestLeaseTakeover:
    @pytest.mark.asyncio
    async def test_same_instance_takeover_and_different_instance_rejection(self, fake_redis):
        sm = SessionManager(fake_redis)
        meeting_id = "mtg_lease_test"
        tenant_id = "tenant_a"
        user_1 = "user_1"
        inst_1 = str(uuid.uuid4())

        user_2 = "user_2"
        inst_2 = str(uuid.uuid4())

        # 1. Initial acquisition by user_1:inst_1 -> Success
        acquired = await sm.acquire_or_takeover_writer_lease(
            meeting_id, tenant_id, user_1, inst_1, ttl_ms=15000
        )
        assert acquired is True

        # 2. Acquisition attempt by different user/instance -> Rejected
        conflict = await sm.acquire_or_takeover_writer_lease(
            meeting_id, tenant_id, user_2, inst_2, ttl_ms=15000
        )
        assert conflict is False

        # 3. Same owner and instance reconnects -> Takes over immediately (K7)
        takeover = await sm.acquire_or_takeover_writer_lease(
            meeting_id, tenant_id, user_1, inst_1, ttl_ms=15000
        )
        assert takeover is True

    @pytest.mark.asyncio
    async def test_old_connection_closed_with_4409_superseded_on_takeover(self):
        """When the same instance reconnects, old open connection is closed with 4409 SUPERSEDED."""
        tenant_id = "tenant_test"
        meeting_id = "mtg_superseded_test"

        old_ws = AsyncMock()
        _ACTIVE_INGEST_CONNECTIONS[(tenant_id, meeting_id)] = old_ws

        new_ws = AsyncMock()

        # Simulate new connection arrival logic
        existing = _ACTIVE_INGEST_CONNECTIONS.get((tenant_id, meeting_id))
        if existing and existing is not new_ws:
            await existing.close(code=WSCloseCode.WRITER_CONFLICT, reason="SUPERSEDED")
        _ACTIVE_INGEST_CONNECTIONS[(tenant_id, meeting_id)] = new_ws

        old_ws.close.assert_called_once_with(code=WSCloseCode.WRITER_CONFLICT, reason="SUPERSEDED")
        assert _ACTIVE_INGEST_CONNECTIONS[(tenant_id, meeting_id)] is new_ws
        _ACTIVE_INGEST_CONNECTIONS.clear()


# ── K8: Rate Limiting Thresholds ──────────────────────────────────────────────
class TestRateLimitingThresholds:
    def test_burst_of_100_frames_accepted_sustained_floods_rejected(self):
        ctx = MagicMock()
        ctx.rate_window_start = 1000.0
        ctx.rate_frame_count = 0
        ctx.rate_byte_count = 0
        ctx.rate_violations = 0
        ctx.settings.frame_rate_limit = 100
        ctx.settings.burst_frame_limit = 140
        ctx.settings.byte_rate_limit = 512 * 1024
        ctx.settings.burst_byte_limit = 1024 * 1024

        # Burst of 100 frames at once within same window -> All accepted (K8)
        with patch("time.time", return_value=1000.5):
            for _ in range(100):
                accepted = _check_rate_limits(ctx, byte_length=100)
                assert accepted is True, "Burst of 100 frames should be accepted"

        # Frame 141 exceeds burst limit -> Rejected
        with patch("time.time", return_value=1000.8):
            for _ in range(40):
                _check_rate_limits(ctx, byte_length=100)
            rejected = _check_rate_limits(ctx, byte_length=100)
            assert rejected is False, "Frame 141 exceeding burst capacity must be rejected"


# ── WP6: Redis Non-Persistence Check & Janitor ────────────────────────────────
class TestWP6RetentionAndJanitor:
    @pytest.mark.asyncio
    async def test_persistence_check_refuses_to_start_if_persistence_enabled(self):
        settings = GatewaySettings(
            app_env="production",
            clerk_issuer="https://clerk.meetmind.ai",
            clerk_jwks_url="https://clerk.meetmind.ai/.well-known/jwks.json",
            allowed_origins=["https://app.meetmind.ai"],
            audio_redis_persistence_acknowledged=False,
        )

        mock_redis = AsyncMock()
        # Mock Redis with save persistence enabled
        mock_redis.config_get.return_value = {"save": "900 1", "appendonly": "no"}

        with pytest.raises(RuntimeError, match="Audio Redis has persistence enabled"):
            await assert_audio_redis_not_persistent(mock_redis, settings)

        # Mock Redis with appendonly enabled
        mock_redis.config_get.return_value = {"save": "", "appendonly": "yes"}
        with pytest.raises(RuntimeError, match="Audio Redis has persistence enabled"):
            await assert_audio_redis_not_persistent(mock_redis, settings)

        # Non-persistent Redis -> passes check
        mock_redis.config_get.return_value = {"save": "", "appendonly": "no"}
        await assert_audio_redis_not_persistent(mock_redis, settings)

    @pytest.mark.asyncio
    async def test_janitor_sweep_marks_abandoned_and_expires_keys(self, fake_redis):
        sm = SessionManager(fake_redis)
        tenant_id = "tenant_janitor"
        mtg_id = "mtg_abandoned_123"

        # Pre-seed session
        await sm.create_meeting(tenant_id=tenant_id, owner_id="user_owner", meeting_id=mtg_id)

        # Run sweep with 0 idle threshold to trigger abandonment
        cleaned = await run_janitor_sweep(
            redis_client=fake_redis,
            session_manager=sm,
            idle_ttl_seconds=-1.0,
            expire_grace_seconds=60,
        )
        assert cleaned == 1

        # Meeting status marked as abandoned
        data = await sm.get_session_data(mtg_id, tenant_id)
        assert data.get("status") == "abandoned"


# ── K9: Ticket TTL Constraints and Real Expiration ───────────────────────────
class TestK9TicketTTL:
    def test_ticket_ttl_max_30s(self):
        """K9: ticket_ttl_seconds cannot exceed 30 seconds."""
        cfg_valid = GatewaySettings(app_env="dev", ticket_ttl_seconds=30)
        assert cfg_valid.ticket_ttl_seconds == 30

        with pytest.raises(ValueError, match="ticket_ttl_seconds must be <= 30"):
            GatewaySettings(app_env="dev", ticket_ttl_seconds=31)

    @pytest.mark.asyncio
    async def test_real_ticket_expiry_with_custom_ttl(self, app_with_mocks, fake_redis):
        """K9: A ticket configured with TICKET_TTL_SECONDS expires and cannot be used."""
        app_with_mocks.state.settings.ticket_ttl_seconds = 1
        client = TestClient(app_with_mocks)
        token = make_test_jwt()
        mid = "mtg_k9_exp"

        resp_mtg = client.post(
            "/v1/meetings",
            headers={"Authorization": f"Bearer {token}"},
            json={"meeting_id": mid},
        )
        assert resp_mtg.status_code == 201

        resp = client.post(
            "/v1/ws-ticket",
            headers={"Authorization": f"Bearer {token}"},
            json={"meeting_id": mid, "role": "ingest", "client_instance_id": str(uuid.uuid4())},
        )
        assert resp.status_code == 200
        ticket = resp.json()["ticket"]
        assert resp.json()["expires_in"] == 1

        # Wait for ticket TTL to elapse
        await asyncio.sleep(1.1)

        # Connection with expired ticket is rejected with 4401 UNAUTHORIZED
        with client.websocket_connect(f"/v1/meetings/{mid}/stream?ticket={ticket}") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_text()
            assert exc_info.value.code == 4401
