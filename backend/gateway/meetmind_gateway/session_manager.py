"""MeetMind AI - Meeting session lifecycle manager.

Tracks active meetings, connected listeners, and meeting state in Redis.
Supports multiple simultaneous listeners per meeting without duplicating
upstream processing.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import redis.asyncio as redis

from .frames import MeetingStatus

logger = logging.getLogger(__name__)


@staticmethod
def _generate_connection_id() -> str:
    return uuid.uuid4().hex[:12]


class MeetingSession:
    """In-memory representation of a meeting session."""

    def __init__(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
    ) -> None:
        self.meeting_id = meeting_id
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.adapter_type = adapter_type
        self.status = MeetingStatus.PENDING_CONSENT
        self.connection_id = uuid.uuid4().hex[:12]
        self.started_at = datetime.now(UTC)


class SessionManager:
    """Manages meeting sessions and listener tracking in Redis.

    Keys used in Redis:
        - ``session:{meeting_id}`` -- hash with meeting metadata + status
        - ``listeners:{meeting_id}`` -- set of connection IDs
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    async def create_session(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
    ) -> MeetingSession:
        """Create or join a meeting session."""
        session = MeetingSession(
            meeting_id=meeting_id,
            tenant_id=tenant_id,
            user_id=user_id,
            adapter_type=adapter_type,
        )

        # Store session metadata (HSETNX = don't overwrite if exists)
        session_key = f"session:{meeting_id}"
        existing = await self._redis.exists(session_key)

        if not existing:
            await self._redis.hset(
                session_key,
                mapping={
                    "meeting_id": meeting_id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "adapter_type": adapter_type,
                    "status": MeetingStatus.PENDING_CONSENT.value,
                    "started_at": session.started_at.isoformat(),
                },
            )
        else:
            # Load existing session state
            data = await self._redis.hgetall(session_key)
            if data:
                session.status = MeetingStatus(
                    data.get("status", MeetingStatus.PENDING_CONSENT.value)
                )
                session.tenant_id = data.get("tenant_id", tenant_id)

        # Register this connection as a listener
        await self._redis.sadd(f"listeners:{meeting_id}", session.connection_id)

        return session

    async def update_status(self, meeting_id: str, status: MeetingStatus) -> None:
        """Update the meeting status in Redis."""
        await self._redis.hset(f"session:{meeting_id}", "status", status.value)

    async def get_status(self, meeting_id: str) -> MeetingStatus | None:
        """Get the current meeting status."""
        status_val = await self._redis.hget(f"session:{meeting_id}", "status")
        if status_val is None:
            return None
        return MeetingStatus(status_val)

    async def get_session_data(self, meeting_id: str) -> dict[str, str] | None:
        """Get all session metadata."""
        data = await self._redis.hgetall(f"session:{meeting_id}")
        return data if data else None

    async def remove_listener(self, meeting_id: str, connection_id: str) -> int:
        """Remove a listener and return remaining listener count."""
        await self._redis.srem(f"listeners:{meeting_id}", connection_id)
        return await self._redis.scard(f"listeners:{meeting_id}")

    async def get_listener_count(self, meeting_id: str) -> int:
        """Return the number of active listeners for a meeting."""
        return await self._redis.scard(f"listeners:{meeting_id}")

    async def cleanup_session(self, meeting_id: str) -> None:
        """Remove all Redis state for a meeting."""
        await self._redis.delete(
            f"session:{meeting_id}",
            f"listeners:{meeting_id}",
            f"seq:{meeting_id}",
        )
