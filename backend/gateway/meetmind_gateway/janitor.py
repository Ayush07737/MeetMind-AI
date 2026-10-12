"""MeetMind AI - Janitor task for abandoned meetings and audio retention (WP6).

Finds meetings with no activity beyond the TTL, marks them ABANDONED,
writes an audit event, and expires their streams and session keys.
Safe to run on multiple workers using a distributed Redis lease.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import datetime
from typing import Any

from meetmind_schemas.events import MeetingStatus

from .keys import (
    audio_stream_key,
    listeners_key,
    transcript_stream_key,
    writer_lease_key,
)
from .session_manager import SessionManager

logger = logging.getLogger(__name__)

JANITOR_LEASE_KEY = "mm:global:janitor:lease"


async def run_janitor_sweep(
    redis_client: Any,
    session_manager: SessionManager,
    idle_ttl_seconds: float = 3600.0,
    expire_grace_seconds: int = 60,
) -> int:
    """Execute a single janitor sweep with distributed lease (WP6).

    Returns:
        Number of abandoned meetings cleaned up.
    """
    lease_acquired = await redis_client.set(JANITOR_LEASE_KEY, "locked", ex=30, nx=True)
    if not lease_acquired:
        logger.debug("Janitor sweep skipped: another worker holds the lease")
        return 0

    abandoned_count = 0
    try:
        keys = await redis_client.keys("mm:*:meeting:*")
        now = time.time()
        for k in keys:
            k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            parts = k_str.split(":")
            if len(parts) != 4:
                continue
            tenant_id, meeting_id = parts[1], parts[3]

            data = await session_manager.get_session_data(meeting_id, tenant_id)
            if not data:
                continue

            status = data.get("status", "")
            if status in (MeetingStatus.ENDED.value, "abandoned"):
                continue

            listeners_count = await session_manager.get_listener_count(meeting_id, tenant_id)
            created_at_str = data.get("created_at", "")
            is_idle = False
            if created_at_str:
                try:
                    dt = datetime.fromisoformat(created_at_str)
                    age = now - dt.timestamp()
                    if age > idle_ttl_seconds and listeners_count == 0:
                        is_idle = True
                except Exception:
                    pass

            k_ttl = await redis_client.ttl(k_str)
            if is_idle or (k_ttl is not None and 0 < k_ttl <= expire_grace_seconds):
                # Mark abandoned in Redis
                await redis_client.hset(k_str, "status", "abandoned")
                abandoned_count += 1

                # Audit event
                try:
                    from meetmind_security.audit_log import write_audit_event

                    await write_audit_event(
                        tenant_id=tenant_id,
                        user_id=data.get("user_id") or data.get("owner_id", "system"),
                        event_type="meeting_abandoned",
                        payload={
                            "meeting_id": meeting_id,
                            "reason": "inactivity_timeout",
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to write audit event for abandoned meeting %s: %s",
                        meeting_id,
                        exc,
                    )

                # Lifecycle event (P0-2)
                with contextlib.suppress(Exception):
                    from .redis_streams import publish_lifecycle_event

                    await publish_lifecycle_event(
                        redis_client, "meeting_abandoned", tenant_id, meeting_id
                    )

                # Expire streams and keys
                from .keys import metrics_key

                for key_to_expire in (
                    audio_stream_key(tenant_id, meeting_id),
                    transcript_stream_key(tenant_id, meeting_id),
                    k_str,
                    writer_lease_key(tenant_id, meeting_id),
                    listeners_key(tenant_id, meeting_id),
                    metrics_key(tenant_id, meeting_id),
                ):
                    with contextlib.suppress(Exception):
                        await redis_client.expire(key_to_expire, expire_grace_seconds)
    finally:
        with contextlib.suppress(Exception):
            await redis_client.delete(JANITOR_LEASE_KEY)

    return abandoned_count


async def run_janitor_loop(
    redis_client: Any,
    session_manager: SessionManager,
    interval_seconds: float = 60.0,
    idle_ttl_seconds: float = 3600.0,
) -> None:
    """Continuous background loop for janitor sweeps."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await run_janitor_sweep(
                redis_client=redis_client,
                session_manager=session_manager,
                idle_ttl_seconds=idle_ttl_seconds,
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Error during janitor loop: %s", exc)
