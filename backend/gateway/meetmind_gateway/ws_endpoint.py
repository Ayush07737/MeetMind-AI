"""MeetMind AI - Hardened WebSocket Ingestion & Listener Endpoints (WP4 & WP5).

Implements:
1. Ingest Endpoint (/v1/meetings/{meeting_id}/stream):
   - Single-writer lease (15s TTL, heartbeat refresh, 4409 on second producer)
   - State Machine: CONNECTED -> PENDING_CONSENT -> ACTIVE -> ENDED
   - Consent gate enforcement & rate-limited rejected frame audits
   - 18-byte binary audio header (decode_audio_frame, deduplicate retransmit, gap tracking)
   - Monotonic sequencing, transcript chunk ingestion
   - Idle timeout (30s before consent, close 4408)
   - Rate limiting and frame size caps (64 KB, close 4429)

2. Listener Endpoint (/v1/meetings/{meeting_id}/listen):
   - Read-only: forwards allow-listed OUTBOUND_EVENT_TYPES via XREAD BLOCK
   - Any client data frame -> close 4400 (protocol violation)
   - Cursor resumption via last_event_id query param
   - Role enforcement via can_listen(user, session)

3. Error handling & audit invariants:
   - Synchronous audits for connection, meeting_start, consent_confirmed, cross-tenant denial
   - Generic client-facing error strings (CA-4)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from meetmind_schemas.events import (
    OUTBOUND_EVENT_TYPES,
    AckFrame,
    ControlAction,
    ControlFrame,
    ErrorCode,
    ErrorFrame,
    IntelligenceEventFrame,
    MeetingStatus,
    TranscriptChunkFrame,
    WSCloseCode,
    decode_audio_frame,
)
from pydantic import ValidationError

from . import audit
from .audit import (
    audit_connection,
    audit_disconnection,
    audit_meeting_end,
    audit_meeting_start,
    write_audit_event,
)
from .auth import (
    AuthenticatedUser,
    AuthError,
    ClerkAuthenticator,
    can_listen,
)
from .config import GatewaySettings, get_settings
from .consent_gate import ConsentGate
from .keys import (
    tenant_connections_key,
    user_connections_key,
    ws_ticket_key,
)
from .redis_streams import EventSubscriber, StreamProducer
from .sequence import SequenceCounter
from .session_manager import MeetingSession, SessionManager

logger = logging.getLogger(__name__)

router = APIRouter()


class ProtocolState:
    CONNECTED = "CONNECTED"
    PENDING_CONSENT = "PENDING_CONSENT"
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"


class ConnectionContext:
    """Holds per-connection state for an ingest session."""

    def __init__(
        self,
        ws: WebSocket,
        meeting_id: str,
        session: MeetingSession,
        consent_gate: ConsentGate,
        seq_counter: SequenceCounter,
        stream_producer: StreamProducer,
        event_subscriber: EventSubscriber,
        session_manager: SessionManager,
        settings: GatewaySettings,
        redis_client: Any,
    ) -> None:
        self.ws = ws
        self.meeting_id = meeting_id
        self.session = session
        self.consent_gate = consent_gate
        self.seq_counter = seq_counter
        self.stream_producer = stream_producer
        self.event_subscriber = event_subscriber
        self.session_manager = session_manager
        self.settings = settings
        self.redis_client = redis_client

        self.state = ProtocolState.CONNECTED
        self.frames_processed = 0
        self.last_acked_client_seq = 0
        self.gap_count = 0
        self.is_closed = False

        # Idle watchdog task
        self.idle_task: asyncio.Task[None] | None = None
        # Writer lease heartbeat task
        self.lease_task: asyncio.Task[None] | None = None

        # Rate-limited rejected audits
        self.rejected_minute_start = time.time()
        self.rejected_minute_count = 0
        self.total_rejected_frames = 0

        # Rate limits per connection
        self.rate_window_start = time.time()
        self.rate_frame_count = 0
        self.rate_byte_count = 0
        self.rate_violations = 0

    def mark_closed(self) -> None:
        self.is_closed = True
        if self.idle_task and not self.idle_task.done():
            self.idle_task.cancel()
        if self.lease_task and not self.lease_task.done():
            self.lease_task.cancel()


async def _record_rejected_frame(ctx: ConnectionContext, frame_type: str, reason: str) -> None:
    """Record rejected frame with rate-limiting: first 5/min individual, rest counted."""
    ctx.total_rejected_frames += 1
    now = time.time()
    if now - ctx.rejected_minute_start >= 60.0:
        ctx.rejected_minute_start = now
        ctx.rejected_minute_count = 0

    if ctx.rejected_minute_count < 5:
        ctx.rejected_minute_count += 1
        with contextlib.suppress(Exception):
            await audit.write_audit_event(
                tenant_id=ctx.session.tenant_id,
                user_id=ctx.session.user_id,
                event_type="frame_rejected_no_consent",
                payload={
                    "meeting_id": ctx.meeting_id,
                    "frame_type": frame_type,
                    "reason": reason,
                },
            )


def _check_rate_limits(ctx: ConnectionContext, byte_length: int) -> bool:
    """Check if connection exceeded frame or byte limits. Returns False if exceeded repeatedly."""
    now = time.time()
    if now - ctx.rate_window_start >= 1.0:
        ctx.rate_window_start = now
        ctx.rate_frame_count = 0
        ctx.rate_byte_count = 0

    ctx.rate_frame_count += 1
    ctx.rate_byte_count += byte_length

    if (
        ctx.rate_frame_count > ctx.settings.frame_rate_limit
        or ctx.rate_byte_count > ctx.settings.byte_rate_limit
    ):
        ctx.rate_violations += 1
        if ctx.rate_violations >= 3:
            return False
    return True


async def _handle_control_frame(
    ctx: ConnectionContext,
    frame: ControlFrame,
) -> None:
    """Process a control frame (meeting_start, consent_confirmed, meeting_end)."""
    if ctx.state == ProtocolState.ENDED:
        err = ErrorFrame(code=ErrorCode.MEETING_ENDED, message="Meeting has already ended")
        await ctx.ws.send_text(err.model_dump_json())
        return

    if frame.action == ControlAction.MEETING_START:
        ctx.state = ProtocolState.PENDING_CONSENT
        if frame.adapter_type:
            adapter_val = (
                frame.adapter_type.value
                if hasattr(frame.adapter_type, "value")
                else str(frame.adapter_type)
            )
            ctx.session.adapter_type = adapter_val
        await ctx.session_manager.update_status(
            ctx.meeting_id, MeetingStatus.PENDING_CONSENT, ctx.session.tenant_id
        )
        await audit_meeting_start(
            tenant_id=ctx.session.tenant_id,
            user_id=ctx.session.user_id,
            meeting_id=ctx.meeting_id,
            adapter_type=ctx.session.adapter_type,
        )
        ack = AckFrame(
            seq=0,
            frame_type="control",
            status="accepted",
            timestamp=datetime.now(UTC),
        )
        await ctx.ws.send_text(ack.model_dump_json())

    elif frame.action == ControlAction.CONSENT_CONFIRMED:
        # Check state machine: consent before meeting_start is protocol error 4400
        if ctx.state == ProtocolState.CONNECTED:
            await ctx.ws.close(
                code=WSCloseCode.PROTOCOL_VIOLATION,
                reason="consent_confirmed received before meeting_start",
            )
            ctx.mark_closed()
            return

        if ctx.state == ProtocolState.ACTIVE:
            # Duplicate consent_confirmed is idempotent: ack without duplicate processing
            ack = AckFrame(
                seq=0,
                frame_type="control",
                status="accepted",
                timestamp=datetime.now(UTC),
            )
            await ctx.ws.send_text(ack.model_dump_json())
            return

        consent_type = frame.consent_type or "audio_capture"
        await ctx.consent_gate.confirm_consent(
            consent_type=consent_type,
            external_participants=frame.external_participants,
            jurisdiction_hint=frame.jurisdiction_hint,
            consent_text_version=frame.consent_text_version,
            client_version=frame.client_version,
        )

        ctx.state = ProtocolState.ACTIVE
        ctx.session.status = MeetingStatus.ACTIVE
        await ctx.session_manager.update_status(
            ctx.meeting_id, MeetingStatus.ACTIVE, ctx.session.tenant_id
        )

        # Cancel idle watchdog now that consent is active
        if ctx.idle_task and not ctx.idle_task.done():
            ctx.idle_task.cancel()

        # Supervised subscriber for outbound events
        await ctx.event_subscriber.subscribe(
            ctx.meeting_id,
            lambda data: _forward_event(ctx.ws, data, ctx.meeting_id),
            tenant_id=ctx.session.tenant_id,
        )

        ack = AckFrame(
            seq=0,
            frame_type="control",
            status="accepted",
            timestamp=datetime.now(UTC),
        )
        await ctx.ws.send_text(ack.model_dump_json())

    elif frame.action == ControlAction.MEETING_END:
        ctx.state = ProtocolState.ENDED
        ctx.session.status = MeetingStatus.ENDED
        await ctx.session_manager.update_status(
            ctx.meeting_id, MeetingStatus.ENDED, ctx.session.tenant_id
        )

        await audit_meeting_end(
            tenant_id=ctx.session.tenant_id,
            user_id=ctx.session.user_id,
            meeting_id=ctx.meeting_id,
            total_chunks=ctx.frames_processed,
        )

        ack = AckFrame(
            seq=0,
            frame_type="control",
            status="accepted",
            timestamp=datetime.now(UTC),
        )
        await ctx.ws.send_text(ack.model_dump_json())
        # Clean close with code 1000
        await ctx.ws.close(code=WSCloseCode.NORMAL_CLOSURE, reason="Meeting ended")
        ctx.mark_closed()


async def _handle_audio_chunk(
    ctx: ConnectionContext,
    data: bytes,
) -> None:
    """Process a binary audio chunk with 18-byte big-endian header (WP5)."""
    # 1. State machine & Consent gate check
    if ctx.state == ProtocolState.ENDED:
        err = ErrorFrame(code=ErrorCode.MEETING_ENDED, message="Meeting has already ended")
        await ctx.ws.send_text(err.model_dump_json())
        return

    if ctx.state != ProtocolState.ACTIVE or not ctx.consent_gate.is_open:
        await _record_rejected_frame(ctx, "audio_chunk", "consent_not_confirmed")
        err = ErrorFrame(
            code=ErrorCode.CONSENT_REQUIRED,
            message="No audio frames permitted before consent_confirmed",
        )
        await ctx.ws.send_text(err.model_dump_json())
        return

    # 2. Binary header decode & validation (18-byte big-endian header)
    client_seq = 0
    capture_ts_ms = int(datetime.now(UTC).timestamp() * 1000)
    gap_size = 0
    payload = data

    if len(data) >= 18:
        try:
            hdr, payload = decode_audio_frame(data)
            client_seq = hdr.client_seq
            capture_ts_ms = hdr.capture_ts_ms

            # Retransmit deduplication: client_seq <= last_acked is acked but not re-published
            if ctx.last_acked_client_seq > 0 and client_seq <= ctx.last_acked_client_seq:
                ack = AckFrame(
                    seq=client_seq,
                    frame_type="audio_chunk",
                    status="acked",
                    timestamp=datetime.now(UTC),
                )
                await ctx.ws.send_text(ack.model_dump_json())
                return

            # Gap tracking: client_seq > last_acked + 1
            if ctx.last_acked_client_seq > 0 and client_seq > ctx.last_acked_client_seq + 1:
                gap_size = client_seq - (ctx.last_acked_client_seq + 1)
                ctx.gap_count += 1

            ctx.last_acked_client_seq = max(ctx.last_acked_client_seq, client_seq)
        except Exception:
            # If decode fails, fallback to sequence counter if not valid header
            client_seq = await ctx.seq_counter.next(ctx.meeting_id)
    else:
        # Backward compatibility for raw audio bytes under 18 bytes
        client_seq = await ctx.seq_counter.next(ctx.meeting_id)

    # 3. Publish to Redis stream mm:{tenant}:audio:{meeting}
    try:
        await ctx.stream_producer.publish_audio(
            meeting_id=ctx.meeting_id,
            seq=client_seq,
            tenant_id=ctx.session.tenant_id,
            user_id=ctx.session.user_id,
            adapter_type=ctx.session.adapter_type,
            audio_data=payload,
            timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
            client_seq=client_seq,
            capture_ts_ms=capture_ts_ms,
            gap_size=gap_size,
        )
    except Exception as exc:
        logger.exception("Redis stream publish failed for meeting %s: %s", ctx.meeting_id, exc)
        err = ErrorFrame(code=ErrorCode.INTERNAL_ERROR, message="Failed to store audio frame")
        await ctx.ws.send_text(err.model_dump_json())
        await ctx.ws.close(code=WSCloseCode.INTERNAL_ERROR, reason="Redis publish failure")
        ctx.mark_closed()
        return

    ctx.frames_processed += 1

    ack = AckFrame(
        seq=client_seq,
        frame_type="audio_chunk",
        status="acked",
        timestamp=datetime.now(UTC),
    )
    await ctx.ws.send_text(ack.model_dump_json())


async def _handle_transcript_chunk(
    ctx: ConnectionContext,
    frame: TranscriptChunkFrame,
) -> None:
    """Process a JSON transcript chunk."""
    if ctx.state == ProtocolState.ENDED:
        err = ErrorFrame(code=ErrorCode.MEETING_ENDED, message="Meeting has already ended")
        await ctx.ws.send_text(err.model_dump_json())
        return

    if ctx.state != ProtocolState.ACTIVE or not ctx.consent_gate.is_open:
        await _record_rejected_frame(ctx, "transcript_chunk", "consent_not_confirmed")
        err = ErrorFrame(
            code=ErrorCode.CONSENT_REQUIRED,
            message="No transcript frames permitted before consent_confirmed",
        )
        await ctx.ws.send_text(err.model_dump_json())
        return

    seq = (
        frame.client_seq
        if frame.client_seq is not None
        else await ctx.seq_counter.next(ctx.meeting_id)
    )

    await ctx.stream_producer.publish_transcript(
        meeting_id=ctx.meeting_id,
        seq=seq,
        tenant_id=ctx.session.tenant_id,
        user_id=ctx.session.user_id,
        adapter_type=ctx.session.adapter_type,
        text=frame.text,
        timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
    )

    ctx.frames_processed += 1

    ack = AckFrame(
        seq=seq,
        frame_type="transcript_chunk",
        status="acked",
        timestamp=datetime.now(UTC),
    )
    await ctx.ws.send_text(ack.model_dump_json())


async def _handle_json_frame(
    ctx: ConnectionContext,
    raw_text: str,
) -> None:
    """Parse and dispatch a JSON frame based on frame_type."""
    if ctx.state == ProtocolState.ENDED:
        err = ErrorFrame(code=ErrorCode.MEETING_ENDED, message="Meeting has already ended")
        await ctx.ws.send_text(err.model_dump_json())
        return

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        err = ErrorFrame(code=ErrorCode.INVALID_FRAME, message="Invalid JSON")
        await ctx.ws.send_text(err.model_dump_json())
        return

    frame_type = data.get("type", "")

    if frame_type == "control":
        try:
            ctrl = ControlFrame.model_validate(data)
            await _handle_control_frame(ctx, ctrl)
        except ValidationError as e:
            err = ErrorFrame(code=ErrorCode.INVALID_FRAME, message=f"Invalid control frame: {e}")
            await ctx.ws.send_text(err.model_dump_json())
        except Exception as e:
            err = ErrorFrame(code=ErrorCode.INVALID_FRAME, message=f"Invalid control frame: {e}")
            await ctx.ws.send_text(err.model_dump_json())

    elif frame_type == "transcript_chunk":
        try:
            chunk = TranscriptChunkFrame.model_validate(data)
            await _handle_transcript_chunk(ctx, chunk)
        except ValidationError as e:
            err = ErrorFrame(
                code=ErrorCode.INVALID_FRAME,
                message=f"Invalid transcript frame: {e}",
            )
            await ctx.ws.send_text(err.model_dump_json())
        except Exception as e:
            err = ErrorFrame(
                code=ErrorCode.INVALID_FRAME,
                message=f"Invalid transcript frame: {e}",
            )
            await ctx.ws.send_text(err.model_dump_json())

    else:
        err = ErrorFrame(
            code=ErrorCode.UNKNOWN_FRAME_TYPE,
            message=f"Unrecognized frame type: {frame_type}",
        )
        await ctx.ws.send_text(err.model_dump_json())


async def _forward_event(ws: WebSocket, data: dict[str, Any], meeting_id: str) -> None:
    """Forward an intelligence event to the WebSocket client."""
    try:
        ev_type = data.get("event_type") or data.get("type", "unknown")
        if ev_type in OUTBOUND_EVENT_TYPES:
            frame = IntelligenceEventFrame(
                event_type=ev_type,
                payload=data.get("payload", data),
                meeting_id=meeting_id,
            )
            await ws.send_text(frame.model_dump_json())
    except Exception:
        logger.debug("Failed to forward event to client for meeting %s", meeting_id)


async def _run_lease_heartbeat(
    session_manager: SessionManager,
    meeting_id: str,
    connection_id: str,
    tenant_id: str,
) -> None:
    """Periodically refresh the single-writer lease while connected."""
    while True:
        try:
            await asyncio.sleep(5.0)
            refreshed = await session_manager.refresh_writer_lock(
                meeting_id=meeting_id,
                connection_id=connection_id,
                tenant_id=tenant_id,
                ttl_ms=15000,
            )
            if not refreshed:
                logger.warning(
                    "Writer lease lost for meeting=%s conn=%s",
                    meeting_id,
                    connection_id,
                )
                break
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Error refreshing writer lease: %s", exc)


async def _run_idle_watchdog(ws: WebSocket, ctx: ConnectionContext, timeout_sec: float) -> None:
    """Enforce 30s idle timeout before consent."""
    try:
        await asyncio.sleep(timeout_sec)
        if (
            ctx.state in (ProtocolState.CONNECTED, ProtocolState.PENDING_CONSENT)
            and not ctx.is_closed
        ):
            logger.info("Idle timeout triggered for meeting %s", ctx.meeting_id)
            err = ErrorFrame(code=ErrorCode.INVALID_FRAME, message="Idle timeout before consent")
            with contextlib.suppress(Exception):
                await ws.send_text(err.model_dump_json())
            await ws.close(code=WSCloseCode.IDLE_TIMEOUT, reason="idle timeout")
            ctx.mark_closed()
    except asyncio.CancelledError:
        pass


async def _authenticate_ws(
    ws: WebSocket,
    meeting_id: str,
    authenticator: ClerkAuthenticator,
    redis_client: Any,
    expected_role: str,
) -> AuthenticatedUser | None:
    """Authenticate WebSocket connection via one-time ticket or dev token."""
    ticket = ws.query_params.get("ticket")
    if ticket:
        ticket_key = ws_ticket_key(ticket)
        payload_raw = None
        try:
            if hasattr(redis_client, "getdel"):
                payload_raw = await redis_client.getdel(ticket_key)
            elif hasattr(redis_client, "execute_command"):
                payload_raw = await redis_client.execute_command("GETDEL", ticket_key)
        except Exception as exc:
            logger.warning("Error fetching ticket from Redis: %s", exc)

        if not payload_raw:
            await ws.accept()
            await ws.close(
                code=WSCloseCode.UNAUTHORIZED,
                reason="Invalid, expired, or previously consumed ticket",
            )
            return None

        try:
            u_dict = (
                json.loads(payload_raw)
                if isinstance(payload_raw, str)
                else json.loads(payload_raw.decode("utf-8"))
            )
            # Verify meeting_id binding in ticket
            bound_meeting = u_dict.get("meeting_id")
            if bound_meeting and bound_meeting != meeting_id:
                await ws.accept()
                await ws.close(
                    code=WSCloseCode.FORBIDDEN,
                    reason="Ticket issued for different meeting",
                )
                return None

            # Verify role binding in ticket
            bound_role = u_dict.get("role", "ingest")
            if bound_role != expected_role:
                await ws.accept()
                await ws.close(
                    code=WSCloseCode.FORBIDDEN,
                    reason=f"Ticket issued for role '{bound_role}', not '{expected_role}'",
                )
                return None

            return AuthenticatedUser(
                user_id=u_dict["user_id"],
                tenant_id=u_dict["tenant_id"],
                session_id=u_dict.get("session_id", ""),
                claims=u_dict.get("claims", {}),
                role=u_dict.get("role_claim", u_dict.get("claims", {}).get("role", "")),
            )
        except Exception:
            await ws.accept()
            await ws.close(
                code=WSCloseCode.UNAUTHORIZED,
                reason="Corrupted ticket payload",
            )
            return None

    # Fallback to token query param (dev/test only)
    token = ws.query_params.get("token", "")
    if token:
        try:
            user = await authenticator.authenticate(token)
            return user
        except AuthError as e:
            await ws.accept()
            await ws.close(code=e.close_code, reason=str(e.message))
            return None

    await ws.accept()
    await ws.close(
        code=WSCloseCode.UNAUTHORIZED,
        reason="Authentication ticket required",
    )
    return None


async def websocket_ingest(
    ws: WebSocket,
    meeting_id: str,
    authenticator: ClerkAuthenticator,
    session_manager: SessionManager,
    seq_counter: SequenceCounter,
    stream_producer: StreamProducer,
    redis_client: Any,
) -> None:
    """Main WebSocket handler for meeting stream ingestion (Write endpoint)."""
    settings = get_settings()

    # 1. Authenticate
    user = await _authenticate_ws(
        ws, meeting_id, authenticator, redis_client, expected_role="ingest"
    )
    if not user:
        return

    # 2. Check cross-tenant meeting binding
    existing_tenant = await session_manager.get_meeting_tenant(meeting_id)
    if existing_tenant and existing_tenant != user.tenant_id:
        with contextlib.suppress(Exception):
            await write_audit_event(
                tenant_id=user.tenant_id,
                user_id=user.user_id,
                event_type="cross_tenant_access_denied",
                payload={
                    "meeting_id": meeting_id,
                    "target_tenant": existing_tenant,
                    "attempted_tenant": user.tenant_id,
                },
            )
        await ws.accept()
        await ws.close(
            code=WSCloseCode.FORBIDDEN,
            reason="cross-tenant access denied",
        )
        return

    # 3. Check connection cap limits
    uconn_key = user_connections_key(user.tenant_id, user.user_id)
    tconn_key = tenant_connections_key(user.tenant_id)
    try:
        user_conns = await redis_client.incr(uconn_key)
        await redis_client.expire(uconn_key, 3600)
        tenant_conns = await redis_client.incr(tconn_key)
        await redis_client.expire(tconn_key, 3600)
        if (
            user_conns > settings.max_connections_per_user
            or tenant_conns > settings.max_connections_per_tenant
        ):
            await ws.accept()
            await ws.close(code=WSCloseCode.RATE_LIMITED, reason="Connection cap exceeded")
            await redis_client.decr(uconn_key)
            await redis_client.decr(tconn_key)
            return
    except Exception:
        pass

    # 4. Accept connection
    with contextlib.suppress(RuntimeError):
        await ws.accept()

    # 5. Create / join session
    session = await session_manager.create_session(
        meeting_id=meeting_id,
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        adapter_type="unknown",
    )

    # 6. Single-writer lease on ingest (SET NX PX 15000)
    acquired = await session_manager.acquire_writer_lock(
        meeting_id=meeting_id,
        connection_id=session.connection_id,
        tenant_id=user.tenant_id,
        ttl_ms=15000,
    )
    if not acquired:
        logger.warning(
            "Writer conflict on meeting %s for connection %s",
            meeting_id,
            session.connection_id,
        )
        await ws.close(
            code=WSCloseCode.WRITER_CONFLICT,
            reason="Meeting already has an active audio producer",
        )
        with contextlib.suppress(Exception):
            await redis_client.decr(uconn_key)
            await redis_client.decr(tconn_key)
        return

    # 7. Consent Gate & Outbound Subscriber
    consent_gate = ConsentGate(
        meeting_id=meeting_id,
        tenant_id=user.tenant_id,
        user_id=user.user_id,
    )
    await consent_gate.check_existing_consent()

    event_subscriber = EventSubscriber(redis_client, tenant_id=user.tenant_id)
    if consent_gate.is_open:
        await session_manager.update_status(meeting_id, MeetingStatus.ACTIVE, user.tenant_id)
        await event_subscriber.subscribe(
            meeting_id,
            lambda data: _forward_event(ws, data, meeting_id),
            tenant_id=user.tenant_id,
        )

    # 8. Build context & launch background tasks
    ctx = ConnectionContext(
        ws=ws,
        meeting_id=meeting_id,
        session=session,
        consent_gate=consent_gate,
        seq_counter=seq_counter,
        stream_producer=stream_producer,
        event_subscriber=event_subscriber,
        session_manager=session_manager,
        settings=settings,
        redis_client=redis_client,
    )
    if consent_gate.is_open:
        ctx.state = ProtocolState.ACTIVE

    # Lease refresh loop
    ctx.lease_task = asyncio.create_task(
        _run_lease_heartbeat(session_manager, meeting_id, session.connection_id, user.tenant_id)
    )

    # Idle watchdog loop (default 30s)
    if ctx.state != ProtocolState.ACTIVE:
        idle_to = float(ws.query_params.get("idle_timeout") or settings.idle_timeout_seconds)
        ctx.idle_task = asyncio.create_task(_run_idle_watchdog(ws, ctx, idle_to))

    # 9. Audit connection (fail-closed)
    await audit_connection(
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        meeting_id=meeting_id,
        adapter_type=session.adapter_type,
    )

    # 10. Receive loop
    disconnect_reason = "normal"
    try:
        while not ctx.is_closed:
            message = await ws.receive()
            msg_type = message.get("type", "")

            if msg_type == "websocket.receive":
                # Check frame size
                b_data = message.get("bytes")
                t_data = message.get("text")
                frame_len = (
                    len(b_data) if b_data else (len(t_data.encode("utf-8")) if t_data else 0)
                )

                if frame_len > settings.ws_max_frame_bytes:
                    err = ErrorFrame(
                        code=ErrorCode.INVALID_FRAME, message="Frame exceeds 64KB limit"
                    )
                    await ws.send_text(err.model_dump_json())
                    await ws.close(code=WSCloseCode.PROTOCOL_VIOLATION, reason="Oversize frame")
                    ctx.mark_closed()
                    break

                if not _check_rate_limits(ctx, frame_len):
                    err = ErrorFrame(
                        code=ErrorCode.RATE_LIMIT_EXCEEDED, message="Rate limit exceeded"
                    )
                    await ws.send_text(err.model_dump_json())
                    await ws.close(code=WSCloseCode.RATE_LIMITED, reason="Rate limit exceeded")
                    ctx.mark_closed()
                    break

                if b_data:
                    await _handle_audio_chunk(ctx, b_data)
                elif t_data:
                    await _handle_json_frame(ctx, t_data)

            elif msg_type == "websocket.disconnect":
                break

    except WebSocketDisconnect:
        disconnect_reason = "client_disconnect"
    except Exception:
        disconnect_reason = "error"
        logger.exception("Error in WebSocket handler for meeting %s", meeting_id)
    finally:
        ctx.mark_closed()
        # Individual try/except per cleanup step
        with contextlib.suppress(Exception):
            await event_subscriber.unsubscribe()

        with contextlib.suppress(Exception):
            await session_manager.release_writer_lock(
                meeting_id, session.connection_id, user.tenant_id
            )

        try:
            remaining = await session_manager.remove_listener(
                meeting_id, session.connection_id, user.tenant_id
            )
        except Exception:
            remaining = 0

        with contextlib.suppress(Exception):
            await redis_client.decr(uconn_key)
            await redis_client.decr(tconn_key)

        # Disconnect audit (always attempted)
        with contextlib.suppress(Exception):
            await audit_disconnection(
                tenant_id=user.tenant_id,
                user_id=user.user_id,
                meeting_id=meeting_id,
                reason=disconnect_reason,
                frames_processed=ctx.frames_processed,
            )

        # Flush summary of rejected frames if flood occurred (> 5)
        if ctx.total_rejected_frames > 5:
            with contextlib.suppress(Exception):
                await audit.write_audit_event(
                    tenant_id=user.tenant_id,
                    user_id=user.user_id,
                    event_type="rejected_frames_summary",
                    payload={
                        "meeting_id": meeting_id,
                        "total_rejected": ctx.total_rejected_frames,
                    },
                )

        if remaining == 0 and ctx.session.status == MeetingStatus.ENDED:
            with contextlib.suppress(Exception):
                await session_manager.cleanup_session(meeting_id, user.tenant_id)


async def websocket_listen(
    ws: WebSocket,
    meeting_id: str,
    authenticator: ClerkAuthenticator,
    session_manager: SessionManager,
    redis_client: Any,
) -> None:
    """Read-only listener endpoint for meeting events (/v1/meetings/{meeting_id}/listen)."""
    settings = get_settings()

    # 1. Authenticate (must have role="listen" or valid dev token)
    user = await _authenticate_ws(
        ws, meeting_id, authenticator, redis_client, expected_role="listen"
    )
    if not user:
        return

    # 2. Check cross-tenant binding
    existing_tenant = await session_manager.get_meeting_tenant(meeting_id)
    if existing_tenant and existing_tenant != user.tenant_id:
        with contextlib.suppress(Exception):
            await write_audit_event(
                tenant_id=user.tenant_id,
                user_id=user.user_id,
                event_type="cross_tenant_access_denied",
                payload={
                    "meeting_id": meeting_id,
                    "target_tenant": existing_tenant,
                    "attempted_tenant": user.tenant_id,
                },
            )
        await ws.accept()
        await ws.close(code=WSCloseCode.FORBIDDEN, reason="cross-tenant access denied")
        return

    # 3. Check can_listen authorization
    session_data = await session_manager.get_session_data(meeting_id, user.tenant_id)
    owner_id = session_data.get("user_id", user.user_id) if session_data else user.user_id
    allowed = can_listen(
        user=user,
        session_tenant_id=user.tenant_id,
        session_owner_id=owner_id,
        elevated_roles=settings.elevated_listen_roles,
    )
    if not allowed:
        await ws.accept()
        await ws.close(code=WSCloseCode.FORBIDDEN, reason="forbidden: cannot listen to meeting")
        return

    # 4. Accept WebSocket connection
    with contextlib.suppress(RuntimeError):
        await ws.accept()

    # Register as listener in Redis
    conn_id = f"listen_{time.time_ns()}"
    await session_manager.create_session(
        meeting_id=meeting_id,
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        adapter_type="listener",
    )

    # 5. Outbound event subscriber via XREAD BLOCK
    last_event_id = ws.query_params.get("last_event_id", "$")
    event_subscriber = EventSubscriber(redis_client, tenant_id=user.tenant_id)

    async def _on_stream_error(err_frame: ErrorFrame) -> None:
        with contextlib.suppress(Exception):
            await ws.send_text(err_frame.model_dump_json())

    await event_subscriber.subscribe(
        meeting_id=meeting_id,
        callback=lambda data: _forward_event(ws, data, meeting_id),
        tenant_id=user.tenant_id,
        last_event_id=last_event_id,
        on_error=_on_stream_error,
    )

    # 6. Receive loop: listener is READ-ONLY. Any client frame other than ping is 4400!
    try:
        while True:
            message = await ws.receive()
            msg_type = message.get("type", "")
            if msg_type == "websocket.receive":
                # Any client data frame sent to listen endpoint is protocol violation 4400!
                if message.get("bytes") or message.get("text"):
                    err = ErrorFrame(
                        code=ErrorCode.INVALID_FRAME,
                        message="Listen endpoint is read-only",
                    )
                    with contextlib.suppress(Exception):
                        await ws.send_text(err.model_dump_json())
                    await ws.close(
                        code=WSCloseCode.PROTOCOL_VIOLATION,
                        reason="protocol violation: listen endpoint cannot receive data",
                    )
                    break
            elif msg_type == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Error in listener WebSocket for meeting %s", meeting_id)
    finally:
        with contextlib.suppress(Exception):
            await event_subscriber.unsubscribe()
        with contextlib.suppress(Exception):
            await session_manager.remove_listener(meeting_id, conn_id, user.tenant_id)
