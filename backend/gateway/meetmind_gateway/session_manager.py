"""MeetMind AI - Meeting session lifecycle manager.

Tracks active meetings, connected listeners, and meeting state in Redis.
Supports multiple simultaneous listeners per meeting without duplicating
upstream processing, and enforces single-writer exclusivity with K7 lease takeover.

All Redis keys are strictly formatted via meetmind_gateway.keys.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis

from .frames import MeetingStatus
from .keys import (
    last_seq_key,
    listeners_key,
    meeting_session_key,
    writer_lease_key,
)

logger = logging.getLogger(__name__)

# K7: Atomic compare-and-set Lua script for writer lease acquisition and takeover
LEASE_TAKEOVER_LUA = """
local cur = redis.call('GET', KEYS[1])
if not cur or cur == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
else
    return 0
end
"""

LEASE_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""


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
    """Manages meeting sessions, server-issued meetings, and leases in Redis."""

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client
        self.create_meeting_times_ms: list[float] = []

    async def create_meeting(
        self,
        tenant_id: str,
        owner_id: str,
        meeting_id: str | None = None,
        local_user_name: str | None = None,
        attendees: list[str] | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a server-issued meeting bound to tenant and owner (K6)."""
        mid = meeting_id or uuid.uuid4().hex
        tid = trace_id or secrets.token_hex(16)
        skey = meeting_session_key(tenant_id, mid)
        now_iso = datetime.now(UTC).isoformat()
        mapping: dict[str, Any] = {
            "meeting_id": mid,
            "tenant_id": tenant_id,
            "owner_id": owner_id,
            "user_id": owner_id,
            "trace_id": tid,
            "status": MeetingStatus.PENDING_CONSENT.value,
            "created_at": now_iso,
        }
        if local_user_name is not None:
            mapping["local_user_name"] = local_user_name
        if attendees is not None:
            mapping["attendees"] = json.dumps(attendees)

        t0 = time.perf_counter()
        await self._redis.hset(
            skey,
            mapping=mapping,
        )
        await self._redis.expire(skey, 86400)
        self.create_meeting_times_ms.append((time.perf_counter() - t0) * 1000)

        res: dict[str, Any] = {
            "meeting_id": mid,
            "tenant_id": tenant_id,
            "owner_id": owner_id,
            "trace_id": tid,
            "status": MeetingStatus.PENDING_CONSENT.value,
            "created_at": now_iso,
        }
        if local_user_name is not None:
            res["local_user_name"] = local_user_name
        if attendees is not None:
            res["attendees"] = attendees
        return res

    def get_stats(self) -> dict[str, Any]:
        """Return Redis create_meeting timing statistics."""
        from .lag_monitor import compute_percentiles

        return {
            "create_meeting_ms": compute_percentiles(self.create_meeting_times_ms),
        }

    async def get_meeting(self, tenant_id: str, meeting_id: str) -> dict[str, str] | None:
        """Lookup an existing meeting within tenant scope (K6)."""
        skey = meeting_session_key(tenant_id, meeting_id)
        data = await self._redis.hgetall(skey)
        if not data:
            return None
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
                v.decode("utf-8") if isinstance(v, bytes) else str(v)
            )
            for k, v in data.items()
        }

    async def find_meeting_tenant(self, meeting_id: str) -> str | None:
        """Probe for which tenant holds this meeting (for audit logging only)."""
        try:
            async for k in self._redis.scan_iter(f"mm:*:meeting:{meeting_id}", count=50):
                k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                parts = k_str.split(":")
                if len(parts) >= 2:
                    return parts[1]
        except Exception as exc:
            logger.warning("Error finding meeting tenant for %s: %s", meeting_id, exc)
        return None

    async def get_meeting_tenant(self, meeting_id: str) -> str | None:
        """Backward-compatible helper: probe which tenant holds this meeting."""
        return await self.find_meeting_tenant(meeting_id)

    async def create_session(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        adapter_type: str,
    ) -> MeetingSession:
        """Create or join a meeting session under tenant isolation."""
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
                    "owner_id": user_id,
                    "adapter_type": adapter_type,
                    "status": MeetingStatus.PENDING_CONSENT.value,
                    "started_at": session.started_at.isoformat(),
                },
            )
            await self._redis.expire(session_key, 86400)
        else:
            data = await self._redis.hgetall(session_key)
            if data:
                clean_data = {
                    (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
                        v.decode("utf-8") if isinstance(v, bytes) else str(v)
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

    async def acquire_or_takeover_writer_lease(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        client_instance_id: str,
        ttl_ms: int = 15000,
    ) -> bool:
        """Acquire or take over writer lease using atomic compare-and-set Lua script (K7).

        Lease value is 'user_id:client_instance_id'.
        Same owner and instance takes over immediately.
        Different owner or instance is rejected.
        """
        lease_val = f"{user_id}:{client_instance_id}"
        key = writer_lease_key(tenant_id, meeting_id)
        try:
            res = await self._redis.eval(LEASE_TAKEOVER_LUA, 1, key, lease_val, str(ttl_ms))
            return bool(res == 1 or res == b"1")
        except Exception:
            # Fallback if eval unsupported (e.g. basic mock)
            current = await self._redis.get(key)
            if current is None:
                return bool(await self._redis.set(key, lease_val, px=ttl_ms, nx=True))
            curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
            if curr_str == lease_val:
                await self._redis.pexpire(key, ttl_ms)
                return True
            return False

    async def refresh_writer_lease(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        client_instance_id: str,
        ttl_ms: int = 15000,
    ) -> bool:
        """Refresh writer lease if held by this user and client instance (K7)."""
        lease_val = f"{user_id}:{client_instance_id}"
        key = writer_lease_key(tenant_id, meeting_id)
        try:
            res = await self._redis.eval(LEASE_TAKEOVER_LUA, 1, key, lease_val, str(ttl_ms))
            return bool(res == 1 or res == b"1")
        except Exception:
            current = await self._redis.get(key)
            if current is not None:
                curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
                if curr_str == lease_val:
                    await self._redis.pexpire(key, ttl_ms)
                    return True
            return False

    async def release_writer_lease(
        self,
        meeting_id: str,
        tenant_id: str,
        user_id: str,
        client_instance_id: str,
    ) -> None:
        """Release writer lease if held by this user and instance (K7)."""
        lease_val = f"{user_id}:{client_instance_id}"
        key = writer_lease_key(tenant_id, meeting_id)
        try:
            await self._redis.eval(LEASE_RELEASE_LUA, 1, key, lease_val)
        except Exception:
            current = await self._redis.get(key)
            if current is not None:
                curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
                if curr_str == lease_val:
                    await self._redis.delete(key)

    # ── Backward-compatible lease helpers ─────────────────────────────────
    async def acquire_writer_lock(
        self,
        meeting_id: str,
        connection_id: str,
        tenant_id: str = "default",
        ttl_ms: int = 15000,
    ) -> bool:
        """Backward-compatible lease acquisition."""
        parts = connection_id.split(":", 1)
        if len(parts) == 2:
            return await self.acquire_or_takeover_writer_lease(
                meeting_id, tenant_id, parts[0], parts[1], ttl_ms=ttl_ms
            )
        key = writer_lease_key(tenant_id, meeting_id)
        current = await self._redis.get(key)
        if current is None:
            return bool(await self._redis.set(key, connection_id, nx=True, px=ttl_ms))
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
        """Backward-compatible lease refresh."""
        parts = connection_id.split(":", 1)
        if len(parts) == 2:
            return await self.refresh_writer_lease(
                meeting_id, tenant_id, parts[0], parts[1], ttl_ms=ttl_ms
            )
        key = writer_lease_key(tenant_id, meeting_id)
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
        """Backward-compatible lease release."""
        parts = connection_id.split(":", 1)
        if len(parts) == 2:
            await self.release_writer_lease(meeting_id, tenant_id, parts[0], parts[1])
            return
        key = writer_lease_key(tenant_id, meeting_id)
        current = await self._redis.get(key)
        if current is not None:
            curr_str = current.decode("utf-8") if isinstance(current, bytes) else str(current)
            if curr_str == connection_id:
                await self._redis.delete(key)

    async def _resolve_tenant(self, meeting_id: str, tenant_id: str = "default") -> str:
        if tenant_id and tenant_id != "default":
            return tenant_id
        found = await self.find_meeting_tenant(meeting_id)
        return found or tenant_id

    async def update_status(
        self,
        meeting_id: str,
        status: MeetingStatus,
        tenant_id: str = "default",
    ) -> None:
        """Update meeting status."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        key = meeting_session_key(t, meeting_id)
        await self._redis.hset(key, "status", status.value)

    async def get_status(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> MeetingStatus | None:
        """Get the current meeting status."""
        data = await self.get_session_data(meeting_id, tenant_id)
        if not data:
            return None
        val_str = data.get("status")
        if not val_str:
            return None
        return MeetingStatus(val_str)

    async def get_session_data(
        self,
        meeting_id: str,
        tenant_id: str = "default",
    ) -> dict[str, str]:
        """Fetch all metadata fields for a meeting session."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        key = meeting_session_key(t, meeting_id)
        data = await self._redis.hgetall(key)
        if not data:
            return {}
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
                v.decode("utf-8") if isinstance(v, bytes) else str(v)
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

    async def refresh_activity(
        self,
        meeting_id: str,
        tenant_id: str = "default",
        ttl: int = 86400,
    ) -> None:
        """Refresh TTLs on meeting session and listener keys on activity (K4)."""
        t = await self._resolve_tenant(meeting_id, tenant_id)
        skey = meeting_session_key(t, meeting_id)
        lkey = listeners_key(t, meeting_id)
        try:
            await self._redis.expire(skey, ttl)
            await self._redis.expire(lkey, ttl)
        except Exception as exc:
            logger.warning(
                "Failed to refresh activity TTL for meeting %s (tenant %s): %s",
                meeting_id,
                t,
                exc,
            )

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
        )
