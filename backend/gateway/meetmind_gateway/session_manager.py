"""MeetMind AI - Meeting session lifecycle manager.

Tracks active meetings, connected listeners, and meeting state in Redis.
Supports multiple simultaneous listeners per meeting without duplicating
upstream processing, and enforces single-writer exclusivity.

All Redis keys are strictly formatted via meetmind_gateway.keys.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import redis.asyncio as redis

from .frames import MeetingStatus
from .keys import (
    last_seq_key,
    listeners_key,
    meeting_session_key,
    meeting_tenant_binding_key,
    writer_lease_key,
)

logger = logging.getLogger(__name__)


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


class CrossTenantAccessError(Exception):
    """Raised when a meeting is accessed from a mismatched tenant."""


class SessionManager:
    """Manages meeting sessions and listener tracking in Redis."""

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    async def get_meeting_tenant(self, meeting_id: str) -> str | None:
        """Lookup which tenant owns a meeting from the global binding key."""
        bkey = meeting_tenant_binding_key(meeting_id)
        val = await self._redis.get(bkey)
        if val is None:
            return None
        return val.decode("utf-8") if isinstance(val, bytes) else str(val)

    async def _resolve_tenant(self, meeting_id: str, tenant_id: str | None = None) -> str:
        """Resolve effective tenant for a meeting."""
        if tenant_id and tenant_id != "default":
            return tenant_id
        bound = await self.get_meeting_tenant(meeting_id)
        if bound:
            return bound
        return tenant_id or "default"

    async def create_session(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
    ) -> MeetingSession:
        """Create or join a meeting session under tenant isolation."""
        # 1. Enforce single-tenant binding per meeting
        bkey = meeting_tenant_binding_key(meeting_id)
        existing_tenant = await self._redis.get(bkey)
        if existing_tenant is not None:
            existing_tenant_str = (
                existing_tenant.decode("utf-8")
                if isinstance(existing_tenant, bytes)
                else str(existing_tenant)
            )
            if existing_tenant_str != tenant_id:
                raise CrossTenantAccessError(
                    f"Meeting {meeting_id} belongs to tenant '{existing_tenant_str}', "
                    f"access denied for tenant '{tenant_id}'"
                )
        else:
            await self._redis.set(bkey, tenant_id, ex=86400, nx=True)

        session = MeetingSession(
            meeting_id=meeting_id,
            tenant_id=tenant_id,
            user_id=user_id,
            adapter_type=adapter_type,
        )

        session_key = meeting_session_key(tenant_id, meeting_id)
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
            await self._redis.expire(session_key, 86400)
        else:
            data = await self._redis.hgetall(session_key)
            if data:
                # Handle bytes vs str from redis
                clean_data = {
                    (k.decode("utf-8") if isinstance(k, bytes) else k): (
                        v.decode("utf-8") if isinstance(v, bytes) else v
                    )
                    for k, v in data.items()
                }
                session.status = MeetingStatus(
                    clean_data.get("status", MeetingStatus.PENDING_CONSENT.value)
                )
                session.tenant_id = clean_data.get("tenant_id", tenant_id)

        # Register connection in listener set with TTL
        lkey = listeners_key(tenant_id, meeting_id)
        await self._redis.sadd(lkey, session.connection_id)
        await self._redis.expire(lkey, 86400)

        return session

    async def acquire_writer_lock(
        self,
        meeting_id: str,
        connection_id: str,
        tenant_id: str = "default",
        ttl_ms: int = 15000,
    ) -> bool:
        """Acquire single-writer lease for audio ingestion.

        Returns:
            True if acquired or already held by connection_id; False if conflict.
        """
        t = await self._resolve_tenant(meeting_id, tenant_id)
        key = writer_lease_key(t, meeting_id)
        current = await self._redis.get(key)
        if current is None:
            acquired = await self._redis.set(key, connection_id, nx=True, px=ttl_ms)
            return bool(acquired)
        curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
        if curr_str == connection_id:
            await self._redis.pexpire(key, ttl_ms)
            return True
        return False

    async def refresh_writer_lock(
        self,
        meeting_id: str,
        connection_id: str,
        tenant_id: str = "default",
        ttl_ms: int = 15000,
    ) -> bool:
        """Refresh single-writer lease if held by this connection."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        key = writer_lease_key(t, meeting_id)
        current = await self._redis.get(key)
        if current is not None:
            curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
            if curr_str == connection_id:
                await self._redis.pexpire(key, ttl_ms)
                return True
        return False

    async def release_writer_lock(
        self,
        meeting_id: str,
        connection_id: str,
        tenant_id: str = "default",
    ) -> None:
        """Release writer lease if held by this connection."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        key = writer_lease_key(t, meeting_id)
        current = await self._redis.get(key)
        if current is not None:
            curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
            if curr_str == connection_id:
                await self._redis.delete(key)

    async def update_status(
        self,
        meeting_id: str,
        status: MeetingStatus,
        tenant_id: str = "default",
    ) -> None:
        """Update the meeting status in Redis."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        skey = meeting_session_key(t, meeting_id)
        await self._redis.hset(skey, "status", status.value)
        await self._redis.expire(skey, 86400)

    async def get_status(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> MeetingStatus | None:
        """Get the current meeting status."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        skey = meeting_session_key(t, meeting_id)
        status_val = await self._redis.hget(skey, "status")
        if status_val is None:
            return None
        val_str = status_val.decode("utf-8") if isinstance(status_val, bytes) else str(status_val)
        return MeetingStatus(val_str)

    async def get_session_data(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> dict[str, str] | None:
        """Get all session metadata."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        skey = meeting_session_key(t, meeting_id)
        data = await self._redis.hgetall(skey)
        if not data:
            return None
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in data.items()
        }

    async def remove_listener(
        self,
        meeting_id: str,
        connection_id: str,
        tenant_id: str = "default",
    ) -> int:
        """Remove a listener and release writer lock."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        await self.release_writer_lock(meeting_id, connection_id, t)
        lkey = listeners_key(t, meeting_id)
        await self._redis.srem(lkey, connection_id)
        return await self._redis.scard(lkey)

    async def get_listener_count(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> int:
        """Return the number of active listeners for a meeting."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        lkey = listeners_key(t, meeting_id)
        return await self._redis.scard(lkey)

    async def cleanup_session(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> None:
        """Remove all Redis state for a meeting."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        await self._redis.delete(
            meeting_session_key(t, meeting_id),
            listeners_key(t, meeting_id),
            writer_lease_key(t, meeting_id),
            last_seq_key(t, meeting_id),
            meeting_tenant_binding_key(meeting_id),
        )
