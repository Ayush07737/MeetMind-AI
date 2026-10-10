"""MeetMind AI - Redis stream producer and outbound event subscriber (WP4).

Downstream interface:
    - Audio chunks published via keys.audio_stream_key
    - Transcript chunks published via keys.transcript_stream_key

Upstream interface:
    - Internal event bus via keys.events_stream_key (not read by gateway)
    - Client outbound stream via keys.out_stream_key (forwarded to clients via XREAD BLOCK)
    - Forward allow-listed events (OUTBOUND_EVENT_TYPES) to connected WebSocket clients.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

import redis.asyncio as redis
from meetmind_schemas.events import OUTBOUND_EVENT_TYPES, ErrorCode, ErrorFrame

from .keys import (
    audio_stream_key,
    events_stream_key,
    out_stream_key,
    transcript_stream_key,
)

logger = logging.getLogger(__name__)

# Type alias for event callback
EventCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
ErrorCallback = Callable[[ErrorFrame], Coroutine[Any, Any, None]]


class StreamProducer:
    """Publishes audio/transcript chunks and events to Redis streams."""

    def __init__(self, redis_client: redis.Redis, maxlen: int = 10_000) -> None:
        self._redis = redis_client
        self._maxlen = maxlen

    async def publish_audio(
        self,
        meeting_id: str,
        *,
        seq: int,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
        audio_data: bytes,
        timestamp_ms: int,
        client_seq: int | None = None,
        capture_ts_ms: int | None = None,
        recv_ns: int | None = None,
        gap_size: int = 0,
    ) -> str:
        """Publish an audio chunk to the meeting's audio stream.

        Carries client_seq, capture_ts_ms, recv_ns, published_ns, and gap_size.
        """
        stream_key = audio_stream_key(tenant_id, meeting_id)
        now_ns = time.time_ns()

        fields: dict[str, Any] = {
            "seq": str(seq),
            "client_seq": str(client_seq if client_seq is not None else seq),
            "tenant_id": tenant_id,
            "user_id": user_id,
            "adapter_type": adapter_type,
            "audio_data": audio_data,
            "timestamp_ms": str(timestamp_ms),
            "capture_ts_ms": str(capture_ts_ms if capture_ts_ms is not None else timestamp_ms),
            "recv_ns": str(recv_ns if recv_ns is not None else now_ns),
            "published_ns": str(now_ns),
        }
        if gap_size > 0:
            fields["gap_size"] = str(gap_size)

        entry_id = await self._redis.xadd(
            stream_key,
            fields,
            maxlen=self._maxlen,
        )
        return entry_id

    async def publish_transcript(
        self,
        meeting_id: str,
        *,
        seq: int,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
        text: str,
        timestamp_ms: int,
    ) -> str:
        """Publish a transcript chunk to the meeting's transcript stream."""
        stream_key = transcript_stream_key(tenant_id, meeting_id)
        entry_id = await self._redis.xadd(
            stream_key,
            {
                "seq": str(seq),
                "tenant_id": tenant_id,
                "user_id": user_id,
                "adapter_type": adapter_type,
                "text": text,
                "timestamp_ms": str(timestamp_ms),
            },
            maxlen=self._maxlen,
        )
        return entry_id

    async def publish_internal_event(
        self,
        meeting_id: str,
        tenant_id: str,
        event_payload: dict[str, Any],
    ) -> str:
        """Publish to the internal events stream (never forwarded to clients)."""
        stream_key = events_stream_key(tenant_id, meeting_id)
        entry_id = await self._redis.xadd(
            stream_key,
            {"payload": json.dumps(event_payload)},
            maxlen=self._maxlen,
        )
        return entry_id

    async def publish_outbound_event(
        self,
        meeting_id: str,
        tenant_id: str,
        event_payload: dict[str, Any],
    ) -> str:
        """Publish allow-listed outbound event for clients to consume."""
        ev_type = event_payload.get("event_type") or event_payload.get("type")
        if ev_type not in OUTBOUND_EVENT_TYPES:
            raise ValueError(f"Event type '{ev_type}' is not allow-listed in OUTBOUND_EVENT_TYPES")

        stream_key = out_stream_key(tenant_id, meeting_id)
        entry_id = await self._redis.xadd(
            stream_key,
            {
                "event_type": str(ev_type),
                "payload": json.dumps(event_payload),
            },
            maxlen=self._maxlen,
        )
        # Bounded stream TTL
        if hasattr(self._redis, "expire"):
            await self._redis.expire(stream_key, 86400)
        return entry_id

    async def ensure_consumer_group(self, stream_key: str, group_name: str) -> None:
        """Create a consumer group if it doesn't exist."""
        try:
            await self._redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


class EventSubscriber:
    """Supervised outbound event stream subscriber using XREAD BLOCK (WP4).

    Reads allow-listed events from mm:{tenant}:out:{meeting}.
    Maintains a per-client cursor (last_event_id), handles reconnection
    with backoff, sends EVENTS_UNAVAILABLE on repeated Redis failure,
    and is strictly idempotent.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        tenant_id: str = "default",
        max_retries: int = 5,
    ) -> None:
        self._redis = redis_client
        self.tenant_id = tenant_id
        self._max_retries = max_retries
        self._task: asyncio.Task[None] | None = None
        self._is_running = False
        self._last_event_id = "$"

    @property
    def last_event_id(self) -> str:
        return self._last_event_id

    async def subscribe(
        self,
        meeting_id: str,
        callback: EventCallback,
        tenant_id: str | None = None,
        last_event_id: str = "$",
        on_error: ErrorCallback | None = None,
    ) -> None:
        """Start listening for outbound events via Redis Streams XREAD BLOCK."""
        # Idempotent: don't start a second loop if already running
        if self._is_running and self._task and not self._task.done():
            logger.debug("EventSubscriber already subscribed for meeting %s", meeting_id)
            return

        eff_tenant = tenant_id or self.tenant_id or "default"
        self._last_event_id = last_event_id or "$"
        stream_key = out_stream_key(eff_tenant, meeting_id)

        if self._last_event_id == "$":
            try:
                latest = await self._redis.xrevrange(stream_key, count=1)
                if latest:
                    lid = latest[0][0]
                    lid_str = lid.decode("utf-8") if isinstance(lid, bytes) else str(lid)
                    self._last_event_id = lid_str
                else:
                    self._last_event_id = "0-0"
            except Exception:
                self._last_event_id = "0-0"

        self._is_running = True

        async def _read_loop() -> None:
            failure_count = 0
            while self._is_running:
                try:
                    # Block for up to 1000ms waiting for new items
                    response = await self._redis.xread(
                        {stream_key: self._last_event_id},
                        count=50,
                        block=1000,
                    )
                    failure_count = 0  # reset on successful read
                    if not response:
                        await asyncio.sleep(0.05)
                        continue
                    if response:
                        for _, messages in response:
                            for msg_id, fields in messages:
                                msg_id_str = (
                                    msg_id.decode("utf-8")
                                    if isinstance(msg_id, bytes)
                                    else str(msg_id)
                                )
                                self._last_event_id = msg_id_str
                                payload_raw = fields.get("payload") or fields.get(b"payload")
                                if payload_raw:
                                    payload_str = (
                                        payload_raw.decode("utf-8")
                                        if isinstance(payload_raw, bytes)
                                        else str(payload_raw)
                                    )
                                    try:
                                        data = json.loads(payload_str)
                                        # Only deliver allow-listed outbound event types
                                        ev_type = data.get("event_type") or data.get("type")
                                        if ev_type in OUTBOUND_EVENT_TYPES:
                                            await callback(data)
                                    except Exception:
                                        logger.exception(
                                            "Error dispatching outbound event from %s",
                                            stream_key,
                                        )
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    failure_count += 1
                    logger.warning(
                        "Redis stream read error on %s (attempt %d/%d): %s",
                        stream_key,
                        failure_count,
                        self._max_retries,
                        exc,
                    )
                    if failure_count >= self._max_retries:
                        logger.error("Outbound event stream unavailable on %s", stream_key)
                        if on_error:
                            with contextlib.suppress(Exception):
                                await on_error(
                                    ErrorFrame(
                                        code=ErrorCode.EVENTS_UNAVAILABLE,
                                        message="Event stream temporarily unavailable",
                                    )
                                )
                        break
                    # Exponential backoff
                    await asyncio.sleep(min(0.2 * (2**failure_count), 2.0))

        self._task = asyncio.create_task(_read_loop())

    async def unsubscribe(self) -> None:
        """Stop reading and clean up listener task."""
        self._is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
