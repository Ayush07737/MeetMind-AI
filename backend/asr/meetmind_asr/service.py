"""MeetMind AI — ASR Pipeline Orchestrator Service (WP-D).

Discovers active meetings via consumer group 'asr' on mm:system:lifecycle,
manages distributed worker ownership leases via Lua CAS, and lifecycle shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import uuid
from typing import Any

import redis.asyncio as aioredis
from meetmind_schemas.keys import ACTIVE_MEETINGS_SET_KEY, asr_owner_key
from meetmind_security.tenant import configure_tenant_router

from .audit import AsrAuditor
from .config import AsrSettings
from .health import AsrMetrics, HealthServer
from .pipeline import MeetingPipeline
from .providers import get_provider
from .tenant_config import FileTenantAsrConfigProvider

logger = logging.getLogger(__name__)

LIFECYCLE_STREAM = "mm:system:lifecycle"

LEASE_LUA_SCRIPT = """
local key = KEYS[1]
local worker_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local current = redis.call('GET', key)
if current == false or current == worker_id then
    redis.call('SET', key, worker_id, 'PX', ttl_ms)
    return 1
else
    return 0
end
"""


class AsrService:
    """ASR worker process managing multiple concurrent meeting pipelines."""

    def __init__(
        self,
        settings: AsrSettings,
        worker_id: str | None = None,
    ) -> None:
        self.settings = settings
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"

        self.redis: aioredis.Redis | None = None
        self.metrics = AsrMetrics()
        self.auditor = AsrAuditor()
        self.tenant_config_provider = FileTenantAsrConfigProvider(settings.asr_tenant_config_path)
        self.health_server: HealthServer | None = None

        self._active_pipelines: dict[str, MeetingPipeline] = {}
        self._lease_tasks: dict[str, asyncio.Task[None]] = {}
        self._is_running = False
        self._discovery_task: asyncio.Task[None] | None = None
        self._reconcile_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start ASR worker daemon."""
        self.redis = aioredis.from_url(
            self.settings.redis_url,
            decode_responses=False,
        )
        self._is_running = True
        app_env = os.environ.get("APP_ENV", "development").strip().lower()
        configure_tenant_router(
            app_env=app_env,
            allow_dynamic=(app_env in ("development", "dev", "test")),
        )

        self.auditor.start()

        self.health_server = HealthServer(
            port=self.settings.asr_health_port,
            metrics=self.metrics,
            redis_client=self.redis,
        )
        await self.health_server.start()

        await self._ensure_lifecycle_group()

        self._discovery_task = asyncio.create_task(self._lifecycle_discovery_loop())
        self._reconcile_task = asyncio.create_task(self._reconcile_loop())
        logger.info("MeetMind ASR Service started with worker_id=%s", self.worker_id)

    async def _ensure_lifecycle_group(self) -> None:
        try:
            await self.redis.xgroup_create(LIFECYCLE_STREAM, "asr", id="0", mkstream=True)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                logger.warning("Error creating asr group on %s: %s", LIFECYCLE_STREAM, exc)

    async def _acquire_lease(self, tenant_id: str, meeting_id: str) -> bool:
        """Acquire or verify ownership lease in Redis using configured TTL (ms)."""
        l_key = asr_owner_key(tenant_id, meeting_id)
        res = await self.redis.eval(
            LEASE_LUA_SCRIPT,
            1,
            l_key,
            self.worker_id,
            self.settings.asr_owner_lease_ttl_ms,
        )
        return bool(res == 1)

    async def _release_lease(self, tenant_id: str, meeting_id: str) -> None:
        l_key = asr_owner_key(tenant_id, meeting_id)
        with contextlib.suppress(Exception):
            current = await self.redis.get(l_key)
            if (
                current
                and (current.decode() if isinstance(current, bytes) else current) == self.worker_id
            ):
                await self.redis.delete(l_key)

    async def _run_lease_heartbeat(self, tenant_id: str, meeting_id: str) -> None:
        """Refresh lease periodically; abort pipeline if lease lost."""
        interval = max(0.2, (self.settings.asr_owner_lease_ttl_ms / 3000.0))
        while self._is_running and meeting_id in self._active_pipelines:
            await asyncio.sleep(interval)
            if not self._is_running or meeting_id not in self._active_pipelines:
                break
            acquired = await self._acquire_lease(tenant_id, meeting_id)
            if not acquired:
                logger.warning(
                    "Worker %s lost ownership lease for meeting %s; stopping immediately",
                    self.worker_id,
                    meeting_id,
                )
                pipeline = self._active_pipelines.pop(meeting_id, None)
                if pipeline:
                    asyncio.create_task(pipeline.stop(is_ended=False))
                break

    async def _start_pipeline(
        self,
        tenant: str,
        meeting_id: str,
        lifecycle_entry_id: str | None = None,
    ) -> None:
        """Instantiate and run MeetingPipeline."""
        if meeting_id in self._active_pipelines:
            return

        t_cfg = await self.tenant_config_provider.get_config(tenant)
        provider_kwargs: dict[str, Any] = {
            "redis_client": self.redis,
        }
        if self.settings.asr_provider == "deepgram" and self.settings.deepgram_api_key:
            provider_kwargs.update(
                {
                    "api_key": self.settings.deepgram_api_key,
                    "base_url": self.settings.deepgram_base_url,
                    "mip_opt_out": True,
                }
            )

        prov = get_provider(self.settings.asr_provider, **provider_kwargs)
        pipeline = MeetingPipeline(
            tenant_id=tenant,
            meeting_id=meeting_id,
            worker_id=self.worker_id,
            redis_client=self.redis,
            provider=prov,
            tenant_config=t_cfg,
            settings=self.settings,
            metrics=self.metrics,
            auditor=self.auditor,
        )
        if lifecycle_entry_id:
            pipeline._lifecycle_entry_id = lifecycle_entry_id

        self._active_pipelines[meeting_id] = pipeline
        self._lease_tasks[meeting_id] = asyncio.create_task(
            self._run_lease_heartbeat(tenant, meeting_id)
        )

        await pipeline.start()
        logger.info(
            "Worker %s successfully started pipeline for %s/%s",
            self.worker_id,
            tenant,
            meeting_id,
        )

    async def _reconcile_loop(self) -> None:
        """Periodic reconciler checking mm:system:active_meetings for unowned meetings."""
        interval = max(0.2, self.settings.asr_reconcile_interval_ms / 1000.0)
        while self._is_running:
            try:
                await asyncio.sleep(interval)
                if not self._is_running:
                    break

                if len(self._active_pipelines) >= self.settings.asr_max_meetings_per_worker:
                    continue

                members = await self.redis.smembers(ACTIVE_MEETINGS_SET_KEY)
                if not members:
                    continue

                for m_raw in members:
                    if len(self._active_pipelines) >= self.settings.asr_max_meetings_per_worker:
                        break

                    m_str = m_raw.decode("utf-8") if isinstance(m_raw, bytes) else str(m_raw)
                    if "|" not in m_str:
                        continue
                    tenant, meeting_id = m_str.split("|", 1)

                    if meeting_id in self._active_pipelines:
                        continue

                    # Check if live lease exists in Redis
                    l_key = asr_owner_key(tenant, meeting_id)
                    current_owner = await self.redis.get(l_key)
                    if current_owner is not None:
                        continue

                    # No live lease found -> attempt claim via Lua CAS
                    acquired = await self._acquire_lease(tenant, meeting_id)
                    if acquired:
                        logger.info(
                            "Reconciler: worker %s acquired unowned meeting %s/%s",
                            self.worker_id,
                            tenant,
                            meeting_id,
                        )
                        await self._start_pipeline(tenant, meeting_id)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Reconciler loop error: %s", exc)
                    await asyncio.sleep(0.5)

    async def _lifecycle_discovery_loop(self) -> None:
        """Consume mm:system:lifecycle to discover active/ended meetings (fast path)."""
        while self._is_running:
            try:
                entries = await self.redis.xreadgroup(
                    "asr",
                    self.worker_id,
                    {LIFECYCLE_STREAM: ">"},
                    count=10,
                    block=1000,
                )
                if entries:
                    for _, messages in entries:
                        for entry_id, fields in messages:
                            await self._handle_lifecycle_entry(entry_id, fields)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Lifecycle discovery loop error: %s", exc)
                    await asyncio.sleep(0.5)

    async def _handle_lifecycle_entry(self, entry_id: Any, fields: dict[Any, Any]) -> None:
        eid_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

        def dec(v: Any) -> str:
            return v.decode() if isinstance(v, bytes) else str(v)

        event = dec(fields.get(b"event") or fields.get("event") or "")
        tenant = dec(fields.get(b"tenant") or fields.get("tenant") or "")
        meeting_id = dec(fields.get(b"meeting_id") or fields.get("meeting_id") or "")

        if not meeting_id or not tenant:
            await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)
            return

        if event == "meeting_active":
            # Idempotency: if already running on this worker, ack without duplicate pipeline
            if meeting_id in self._active_pipelines:
                await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)
                return

            # Capacity check
            if len(self._active_pipelines) >= self.settings.asr_max_meetings_per_worker:
                # Do not ack; leave for other competing consumers in group
                return

            # Attempt ownership lease acquisition
            acquired = await self._acquire_lease(tenant, meeting_id)
            if not acquired:
                # Another worker secured the lease; ack event as handled
                await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)
                return

            # Ownership secured -> ack lifecycle entry and start pipeline
            await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)
            await self._start_pipeline(tenant, meeting_id, lifecycle_entry_id=eid_str)

        elif event in ("meeting_ended", "meeting_abandoned"):
            await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)

            if meeting_id in self._active_pipelines:
                lease_task = self._lease_tasks.pop(meeting_id, None)
                if lease_task:
                    lease_task.cancel()

                pipeline = self._active_pipelines.pop(meeting_id)
                await pipeline.stop(is_ended=True)
                await self._release_lease(tenant, meeting_id)

    async def stop(self) -> None:
        """Gracefully stop worker daemon on SIGTERM."""
        self._is_running = False
        if self._discovery_task:
            self._discovery_task.cancel()
        if self._reconcile_task:
            self._reconcile_task.cancel()

        for lt in self._lease_tasks.values():
            lt.cancel()

        # Stop active pipelines cleanly
        stop_tasks = []
        for mid, pipeline in list(self._active_pipelines.items()):
            stop_tasks.append(pipeline.stop(is_ended=False))
            stop_tasks.append(self._release_lease(pipeline.tenant_id, mid))
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._active_pipelines.clear()

        if self.health_server:
            await self.health_server.stop()

        await self.auditor.stop()

        if self.redis:
            await self.redis.aclose()
        logger.info("ASR Service worker %s stopped", self.worker_id)
