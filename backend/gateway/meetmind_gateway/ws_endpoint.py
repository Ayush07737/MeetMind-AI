"""MeetMind AI - WebSocket ingestion endpoint.

The endpoint handles the full lifecycle:
1. Authenticate via one-time ticket (or query param token in dev mode)
2. Accept connection then close with structured WSCloseCode on error
3. Create/join meeting session and track listeners/writer exclusivity
4. Wait for consent_confirmed control frame (gate is closed until then)
5. Accept audio_chunk (binary) and transcript_chunk (JSON) frames
6. Assign monotonic sequence numbers and publish to Redis streams
7. Clean up on disconnect
"""

from __future__ import annotations

import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from meetmind_schemas.events import WSCloseCode

from .audit import (
    audit_connection,
    audit_disconnection,
    audit_meeting_end,
    audit_meeting_start,
)
from .auth import (
    AuthenticatedUser,
    AuthError,
    ClerkAuthenticator,
)
from .consent_gate import ConsentGate
from .frames import (
    AckFrame,
    ControlAction,
    ControlFrame,
    ErrorCode,
    ErrorFrame,
    IntelligenceEventFrame,
    MeetingStatus,
    TranscriptChunkFrame,
)
from .redis_streams import EventSubscriber, StreamProducer
from .sequence import SequenceCounter
from .session_manager import MeetingSession, SessionManager

logger = logging.getLogger(__name__)

router = APIRouter()


class ConnectionContext:
    """Holds all per-connection state for a WebSocket session."""

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
    ) -> None:
        self.ws = ws
        self.meeting_id = meeting_id
        self.session = session
        self.consent_gate = consent_gate
        self.seq_counter = seq_counter
        self.stream_producer = stream_producer
        self.event_subscriber = event_subscriber
        self.session_manager = session_manager
        self.frames_processed = 0
        self.is_closed = False

    def mark_closed(self) -> None:
        self.is_closed = True


async def _handle_control_frame(
    ctx: ConnectionContext,
    frame: ControlFrame,
) -> None:
    """Process a control frame (meeting_start, consent_confirmed, meeting_end)."""
    if frame.action == ControlAction.MEETING_START:
        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.PENDING_CONSENT)
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
        consent_type = frame.consent_type or "audio_capture"
        await ctx.consent_gate.confirm_consent(consent_type)

        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.ACTIVE)
        ctx.session.status = MeetingStatus.ACTIVE

        # Subscribe to intelligence events
        await ctx.event_subscriber.subscribe(
            ctx.meeting_id,
            lambda data: _forward_event(ctx.ws, data, ctx.meeting_id),
        )

        ack = AckFrame(
            seq=0,
            frame_type="control",
            status="accepted",
            timestamp=datetime.now(UTC),
        )
        await ctx.ws.send_text(ack.model_dump_json())

    elif frame.action == ControlAction.MEETING_END:
        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.ENDED)
        ctx.session.status = MeetingStatus.ENDED

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


async def _handle_audio_chunk(
    ctx: ConnectionContext,
    data: bytes,
) -> None:
    """Process a binary audio chunk."""
    # Consent gate check
    if not ctx.consent_gate.is_open:
        await ctx.consent_gate.reject_frame("audio_chunk")
        err = ErrorFrame(
            code=ErrorCode.CONSENT_REQUIRED,
            message="No audio frames permitted before consent_confirmed",
        )
        await ctx.ws.send_text(err.model_dump_json())
        return

    # Check writer lock (exclusive audio stream producer)
    acquired = await ctx.session_manager.acquire_writer_lock(
        ctx.meeting_id, ctx.session.connection_id
    )
    if not acquired:
        logger.warning(
            "Writer conflict on meeting %s for connection %s",
            ctx.meeting_id,
            ctx.session.connection_id,
        )
        await ctx.ws.close(
            code=WSCloseCode.WRITER_CONFLICT,
            reason="Meeting already has an active audio producer",
        )
        ctx.mark_closed()
        return

    seq = await ctx.seq_counter.next(ctx.meeting_id)

    # Publish to Redis streams
    await ctx.stream_producer.publish_audio(
        meeting_id=ctx.meeting_id,
        seq=seq,
        tenant_id=ctx.session.tenant_id,
        user_id=ctx.session.user_id,
        adapter_type=ctx.session.adapter_type,
        audio_data=data,
        timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
    )

    ctx.frames_processed += 1

    ack = AckFrame(
        seq=seq,
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
    if not ctx.consent_gate.is_open:
        await ctx.consent_gate.reject_frame("transcript_chunk")
        err = ErrorFrame(
            code=ErrorCode.CONSENT_REQUIRED,
            message="No transcript frames permitted before consent_confirmed",
        )
        await ctx.ws.send_text(err.model_dump_json())
        return

    seq = await ctx.seq_counter.next(ctx.meeting_id)

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
        except Exception as e:
            err = ErrorFrame(code=ErrorCode.INVALID_FRAME, message=f"Invalid control frame: {e}")
            await ctx.ws.send_text(err.model_dump_json())

    elif frame_type == "transcript_chunk":
        try:
            chunk = TranscriptChunkFrame.model_validate(data)
            await _handle_transcript_chunk(ctx, chunk)
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


async def websocket_ingest(
    ws: WebSocket,
    meeting_id: str,
    authenticator: ClerkAuthenticator,
    session_manager: SessionManager,
    seq_counter: SequenceCounter,
    stream_producer: StreamProducer,
    redis_client: Any,
) -> None:
    """Main WebSocket handler for meeting stream ingestion."""
    # ── 1. Authenticate ──────────────────────────────────────────────────
    ticket = ws.query_params.get("ticket")
    user: AuthenticatedUser | None = None

    if ticket:
        ticket_key = f"ws_ticket:{ticket}"
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
            return

        try:
            u_dict = (
                json.loads(payload_raw)
                if isinstance(payload_raw, str)
                else json.loads(payload_raw.decode("utf-8"))
            )
            user = AuthenticatedUser(
                user_id=u_dict["user_id"],
                tenant_id=u_dict["tenant_id"],
                session_id=u_dict.get("session_id", ""),
                claims=u_dict.get("claims", {}),
            )
        except Exception:
            await ws.accept()
            await ws.close(
                code=WSCloseCode.UNAUTHORIZED,
                reason="Corrupted ticket payload",
            )
            return
    else:
        # Fallback to query param token in dev / testing
        token = ws.query_params.get("token", "")
        if token:
            try:
                user = await authenticator.authenticate(token)
            except AuthError as e:
                await ws.accept()
                await ws.close(code=e.close_code, reason=str(e.message))
                return
        else:
            await ws.accept()
            await ws.close(
                code=WSCloseCode.UNAUTHORIZED,
                reason="Authentication ticket required",
            )
            return

    # ── 2. Accept the WebSocket connection ───────────────────────────────
    with contextlib.suppress(RuntimeError):
        await ws.accept()

    # ── 3. Create / join meeting session ─────────────────────────────────
    session = await session_manager.create_session(
        meeting_id=meeting_id,
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        adapter_type="unknown",
    )

    # ── 4. Set up consent gate ───────────────────────────────────────────
    consent_gate = ConsentGate(
        meeting_id=meeting_id,
        tenant_id=user.tenant_id,
        user_id=user.user_id,
    )
    await consent_gate.check_existing_consent()

    # ── 5. Set up event subscriber ───────────────────────────────────────
    event_subscriber = EventSubscriber(redis_client)
    if consent_gate.is_open:
        await session_manager.update_status(meeting_id, MeetingStatus.ACTIVE)
        await event_subscriber.subscribe(
            meeting_id, lambda data: _forward_event(ws, data, meeting_id)
        )

    # ── 6. Build connection context ──────────────────────────────────────
    ctx = ConnectionContext(
        ws=ws,
        meeting_id=meeting_id,
        session=session,
        consent_gate=consent_gate,
        seq_counter=seq_counter,
        stream_producer=stream_producer,
        event_subscriber=event_subscriber,
        session_manager=session_manager,
    )

    # ── 7. Audit the connection (fail-closed) ────────────────────────────
    await audit_connection(
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        meeting_id=meeting_id,
        adapter_type=session.adapter_type,
    )

    # ── 8. Main receive loop ─────────────────────────────────────────────
    disconnect_reason = "normal"
    try:
        while not ctx.is_closed:
            message = await ws.receive()
            msg_type = message.get("type", "")

            if msg_type == "websocket.receive":
                if "bytes" in message and message["bytes"]:
                    await _handle_audio_chunk(ctx, message["bytes"])
                elif "text" in message and message["text"]:
                    await _handle_json_frame(ctx, message["text"])
            elif msg_type == "websocket.disconnect":
                break

    except WebSocketDisconnect:
        disconnect_reason = "client_disconnect"
    except Exception:
        disconnect_reason = "error"
        logger.exception("Error in WebSocket handler for meeting %s", meeting_id)
    finally:
        ctx.mark_closed()
        await event_subscriber.unsubscribe()
        remaining = await session_manager.remove_listener(meeting_id, session.connection_id)

        await audit_disconnection(
            tenant_id=user.tenant_id,
            user_id=user.user_id,
            meeting_id=meeting_id,
            reason=disconnect_reason,
            frames_processed=ctx.frames_processed,
        )

        if remaining == 0 and ctx.session.status == MeetingStatus.ENDED:
            await session_manager.cleanup_session(meeting_id)

        logger.info(
            "Disconnected from meeting %s (reason=%s, frames=%d, remaining_listeners=%d)",
            meeting_id,
            disconnect_reason,
            ctx.frames_processed,
            remaining,
        )


async def _forward_event(ws: WebSocket, data: dict[str, Any], meeting_id: str) -> None:
    """Forward an intelligence event to the WebSocket client."""
    try:
        frame = IntelligenceEventFrame(
            event_type=data.get("event_type", "unknown"),
            payload=data.get("payload", {}),
            meeting_id=meeting_id,
        )
        await ws.send_text(frame.model_dump_json())
    except Exception:
        logger.debug("Failed to forward event to client for meeting %s", meeting_id)
