"""MeetMind AI - WebSocket ingestion endpoint.

The single endpoint ``/ws/ingest/{meeting_id}`` handles the full lifecycle:

1. Authenticate via Clerk JWT (query param ``token``)
2. Create/join meeting session
3. Wait for ``consent_confirmed`` control frame (gate is closed until then)
4. Accept audio_chunk (binary) and transcript_chunk (JSON) frames
5. Assign sequence numbers and publish to Redis streams
6. Subscribe to intelligence events and multiplex back to client
7. Clean up on disconnect
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from .audit import (
    audit_connection,
    audit_disconnection,
    audit_meeting_end,
    audit_meeting_start,
)
from .auth import AuthError, ClerkAuthenticator
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
        self._closed = False

    @property
    def tenant_id(self) -> str:
        return self.session.tenant_id

    @property
    def user_id(self) -> str:
        return self.session.user_id

    @property
    def adapter_type(self) -> str:
        return self.session.adapter_type

    async def send_error(self, code: ErrorCode, message: str) -> None:
        """Send an error frame to the client."""
        if not self._closed:
            frame = ErrorFrame(code=code, message=message)
            await self.ws.send_text(frame.model_dump_json())

    async def send_ack(self, seq: int) -> None:
        """Send an acknowledgement frame."""
        if not self._closed:
            frame = AckFrame(seq=seq)
            await self.ws.send_text(frame.model_dump_json())

    async def send_intelligence_event(self, data: dict[str, Any]) -> None:
        """Forward an intelligence event from downstream services."""
        if not self._closed:
            frame = IntelligenceEventFrame(
                event_type=data.get("event_type", "unknown"),
                payload=data.get("payload", {}),
                meeting_id=self.meeting_id,
            )
            await self.ws.send_text(frame.model_dump_json())

    def mark_closed(self) -> None:
        self._closed = True


async def _handle_control(ctx: ConnectionContext, frame: ControlFrame) -> None:
    """Process a control frame."""
    if frame.action == ControlAction.MEETING_START:
        adapter_type = frame.adapter_type
        if adapter_type:
            ctx.session.adapter_type = adapter_type.value

        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.PENDING_CONSENT)
        await audit_meeting_start(
            tenant_id=ctx.tenant_id,
            user_id=ctx.user_id,
            meeting_id=ctx.meeting_id,
            adapter_type=ctx.adapter_type,
        )
        logger.info("Meeting started: %s", ctx.meeting_id)

    elif frame.action == ControlAction.CONSENT_CONFIRMED:
        consent_type = frame.consent_type or "audio_capture"
        await ctx.consent_gate.confirm_consent(consent_type)
        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.ACTIVE)

        # Start subscribing to intelligence events now that meeting is active
        await ctx.event_subscriber.subscribe(ctx.meeting_id, ctx.send_intelligence_event)
        logger.info("Consent confirmed for meeting %s, now ACTIVE", ctx.meeting_id)

    elif frame.action == ControlAction.MEETING_END:
        await ctx.session_manager.update_status(ctx.meeting_id, MeetingStatus.ENDED)
        await audit_meeting_end(
            tenant_id=ctx.tenant_id,
            user_id=ctx.user_id,
            meeting_id=ctx.meeting_id,
            total_chunks=ctx.frames_processed,
        )
        logger.info("Meeting ended: %s (%d chunks)", ctx.meeting_id, ctx.frames_processed)


async def _handle_audio_chunk(ctx: ConnectionContext, data: bytes) -> None:
    """Process a binary audio chunk."""
    if not ctx.consent_gate.is_open:
        await ctx.consent_gate.reject_frame("audio_chunk")
        await ctx.send_error(
            ErrorCode.CONSENT_REQUIRED,
            "Consent must be confirmed before sending audio",
        )
        return

    seq = await ctx.seq_counter.next(ctx.meeting_id)
    timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

    await ctx.stream_producer.publish_audio(
        ctx.meeting_id,
        seq=seq,
        tenant_id=ctx.tenant_id,
        user_id=ctx.user_id,
        adapter_type=ctx.adapter_type,
        audio_data=data,
        timestamp_ms=timestamp_ms,
    )

    ctx.frames_processed += 1
    await ctx.send_ack(seq)


async def _handle_transcript_chunk(ctx: ConnectionContext, frame: TranscriptChunkFrame) -> None:
    """Process a pre-transcribed text chunk."""
    if not ctx.consent_gate.is_open:
        await ctx.consent_gate.reject_frame("transcript_chunk")
        await ctx.send_error(
            ErrorCode.CONSENT_REQUIRED,
            "Consent must be confirmed before sending transcripts",
        )
        return

    seq = await ctx.seq_counter.next(ctx.meeting_id)

    await ctx.stream_producer.publish_transcript(
        ctx.meeting_id,
        seq=seq,
        tenant_id=ctx.tenant_id,
        user_id=ctx.user_id,
        adapter_type=ctx.adapter_type,
        text=frame.text,
        timestamp_ms=frame.timestamp_ms,
    )

    ctx.frames_processed += 1
    await ctx.send_ack(seq)


async def _handle_json_frame(ctx: ConnectionContext, text: str) -> None:
    """Parse and dispatch a JSON text frame."""
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        await ctx.send_error(ErrorCode.INVALID_FRAME, "Invalid JSON")
        return

    frame_type = raw.get("type")
    if frame_type == "control":
        try:
            frame = ControlFrame.model_validate(raw)
        except Exception as e:
            await ctx.send_error(ErrorCode.INVALID_FRAME, f"Invalid control frame: {e}")
            return
        await _handle_control(ctx, frame)
    elif frame_type == "transcript_chunk":
        try:
            frame = TranscriptChunkFrame.model_validate(raw)
        except Exception as e:
            await ctx.send_error(ErrorCode.INVALID_FRAME, f"Invalid transcript chunk: {e}")
            return
        await _handle_transcript_chunk(ctx, frame)
    else:
        await ctx.send_error(
            ErrorCode.INVALID_FRAME,
            f"Unknown frame type: {frame_type}",
        )


async def websocket_ingest(
    ws: WebSocket,
    meeting_id: str,
    authenticator: ClerkAuthenticator,
    session_manager: SessionManager,
    seq_counter: SequenceCounter,
    stream_producer: StreamProducer,
    redis_client: Any,
) -> None:
    """Main WebSocket handler for /ws/ingest/{meeting_id}.

    This function is called by the route handler with all dependencies
    injected, making it testable.
    """
    # ── 1. Authenticate ──────────────────────────────────────────────────
    token = ws.query_params.get("token", "")
    try:
        user = await authenticator.authenticate(token)
    except AuthError as e:
        await ws.close(code=status.WS_1008_POLICY_VIOLATION, reason=str(e.message))
        return

    # ── 2. Accept the WebSocket connection ───────────────────────────────
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

    # Check if consent was already granted (reconnect scenario)
    await consent_gate.check_existing_consent()

    # ── 5. Set up event subscriber ───────────────────────────────────────
    event_subscriber = EventSubscriber(redis_client)

    # If consent was already confirmed (reconnect), start event subscription
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

    # ── 7. Audit the connection ──────────────────────────────────────────
    await audit_connection(
        tenant_id=user.tenant_id,
        user_id=user.user_id,
        meeting_id=meeting_id,
        adapter_type=session.adapter_type,
    )

    # ── 8. Main receive loop ─────────────────────────────────────────────
    disconnect_reason = "normal"
    try:
        while True:
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

        # Clean up event subscriber
        await event_subscriber.unsubscribe()

        # Remove this listener
        remaining = await session_manager.remove_listener(meeting_id, session.connection_id)

        # Audit disconnection
        await audit_disconnection(
            tenant_id=user.tenant_id,
            user_id=user.user_id,
            meeting_id=meeting_id,
            reason=disconnect_reason,
            frames_processed=ctx.frames_processed,
        )

        # If no listeners remain and meeting was explicitly ended, clean up session
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
