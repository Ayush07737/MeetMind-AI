"""MeetMind AI - Redis stream producer and intelligence event subscriber.

Downstream interface:
    - Audio chunks -> Redis stream ``audio:{meeting_id}``
    - Transcript chunks -> Redis stream ``transcript:{meeting_id}``

Upstream interface:
    - Subscribe to ``events:{meeting_id}`` channel (Redis pub-sub)
    - Forward intelligence events to connected WebSocket clients
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Coroutine
from typing import Any

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class StreamProducer:
    """Publishes audio/transcript chunks to Redis streams."""

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
    ) -> str:
        """Publish an audio chunk to the meeting's audio stream.

        Returns:
            The Redis stream entry ID.
        """
        stream_key = f"audio:{meeting_id}"
        entry_id = await self._redis.xadd(
            stream_key,
            {
                "seq": str(seq),
                "tenant_id": tenant_id,
                "user_id": user_id,
                "adapter_type": adapter_type,
                "audio_data": audio_data,
                "timestamp_ms": str(timestamp_ms),
            },
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
        """Publish a transcript chunk to the meeting's transcript stream.

        Returns:
            The Redis stream entry ID.
        """
        stream_key = f"transcript:{meeting_id}"
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

    async def ensure_consumer_group(self, stream_key: str, group_name: str) -> None:
        """Create a consumer group if it doesn't exist."""
        try:
            await self._redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


# Type alias for event callback
EventCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class EventSubscriber:
    """Subscribes to intelligence events for a meeting via Redis pub-sub.

    Downstream services (§6/§7) publish events to the channel
    ``events:{meeting_id}``. This subscriber forwards them to the
    WebSocket event multiplexer.
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client
        self._pubsub: redis.client.PubSub | None = None
        self._task: asyncio.Task[None] | None = None

    async def subscribe(self, meeting_id: str, callback: EventCallback) -> None:
        """Start listening for events on the meeting's channel."""
        channel = f"events:{meeting_id}"
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(channel)

        async def _listen() -> None:
            assert self._pubsub is not None
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        await callback(data)
                    except Exception:
                        logger.exception(
                            "Error processing event for meeting %s",
                            meeting_id,
                        )

        self._task = asyncio.create_task(_listen())

    async def unsubscribe(self) -> None:
        """Stop listening and clean up."""
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
            self._pubsub = None
