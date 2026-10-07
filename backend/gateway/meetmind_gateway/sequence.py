"""MeetMind AI - Redis-backed per-meeting sequence counter.

Provides globally ordered, atomic sequence numbers for audio and transcript
chunks within a meeting. This ensures correct ordering even when multiple
backend workers handle frames for the same meeting.
"""

from __future__ import annotations

import redis.asyncio as redis


class SequenceCounter:
    """Atomic per-meeting sequence counter backed by Redis.

    Usage::

        counter = SequenceCounter(redis_client)
        seq = await counter.next("mtg_123")  # 1
        seq = await counter.next("mtg_123")  # 2
    """

    KEY_PREFIX = "seq"

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    def _key(self, meeting_id: str) -> str:
        return f"{self.KEY_PREFIX}:{meeting_id}"

    async def next(self, meeting_id: str) -> int:
        """Return the next sequence number for the given meeting."""
        return await self._redis.incr(self._key(meeting_id))

    async def current(self, meeting_id: str) -> int:
        """Return the current sequence number (0 if no frames yet)."""
        val = await self._redis.get(self._key(meeting_id))
        return int(val) if val else 0

    async def reset(self, meeting_id: str) -> None:
        """Reset the sequence counter (used on meeting end)."""
        await self._redis.delete(self._key(meeting_id))
