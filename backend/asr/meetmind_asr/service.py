"""MeetMind AI — ASR Pipeline Orchestrator Service (WP-D).

Discovers active meetings via consumer group 'asr' on mm:system:lifecycle,
manages distributed worker ownership leases via Lua CAS, and lifecycle shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import uuid
from typing import Any

import redis.asyncio as aioredis
from meetmind_schemas.keys import ACTIVE_MEETINGS_SET_KEY, asr_owner_key
from meetmind_security.tenant import configure_tenant_router

from .audit import AsrAuditor
from .config import AsrSettings, resolve_asr_base_url
from .health import AsrMetrics, HealthServer, init_gc_monitor
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
        self._control_task: asyncio.Task[None] | None = None
        self._stopping_tasks: set[asyncio.Task[Any]] = set()

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

        # Performance, loop monitoring & GC optimizations (Requirement 4)
        loop = asyncio.get_running_loop()
        loop.slow_callback_duration = 0.050  # 50 ms slow callback threshold
        import gc

        gc.collect()
        if hasattr(gc, "freeze"):
            gc.freeze()
        gc.set_threshold(70000, 10, 10)

        self.auditor.start()
        init_gc_monitor(self.metrics)

        self.health_server = HealthServer(
            port=self.settings.asr_health_port,
            metrics=self.metrics,
            redis_client=self.redis,
            service=self,
        )
        await self.health_server.start()

        await self._ensure_lifecycle_group()

        self._discovery_task = asyncio.create_task(self._lifecycle_discovery_loop())
        self._reconcile_task = asyncio.create_task(self._reconcile_loop())
        if self.settings.asr_test_hooks:
            self._control_task = asyncio.create_task(self._control_loop())
        logger.info(
            "MeetMind ASR Service started with worker_id=%s, default_region=%s, "
            "allowed_regions=%s, test_hooks=%s",
            self.worker_id,
            self.settings.asr_region,
            sorted(self.settings.allowed_regions_set),
            self.settings.asr_test_hooks,
        )

    async def _ensure_lifecycle_group(self) -> None:
        try:
            await self.redis.xgroup_create(LIFECYCLE_STREAM, "asr", id="0", mkstream=True)
            logger.info("Created consumer group 'asr' on %s with MKSTREAM", LIFECYCLE_STREAM)
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

    async def _recycle_meeting(self, tenant_id: str, meeting_id: str) -> None:
        """Gracefully recycle pipeline when max duration is exceeded without dropping audio."""
        logger.info(
            "Worker %s initiating graceful recycle of pipeline for %s/%s",
            self.worker_id,
            tenant_id,
            meeting_id,
        )
        if meeting_id in self._lease_tasks:
            self._lease_tasks.pop(meeting_id).cancel()
        if meeting_id in self._active_pipelines:
            pipeline = self._active_pipelines.pop(meeting_id)
            await pipeline.stop(is_ended=False)
            await self._release_lease(tenant_id, meeting_id)

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
        started_via: str = "reconciler",
    ) -> None:
        """Instantiate and run MeetingPipeline."""
        if meeting_id in self._active_pipelines:
            return

        self.metrics.record_started_via(meeting_id, started_via)

        t_cfg = await self.tenant_config_provider.get_config(tenant)
        provider_kwargs: dict[str, Any] = {
            "redis_client": self.redis,
        }

        # Resolve effective region: None = inherit from settings.asr_region
        # ("global" is NOT sentinel)
        requested_region = (
            t_cfg.asr_region if t_cfg.asr_region is not None else self.settings.asr_region
        )
        effective_region = requested_region

        if requested_region not in self.settings.allowed_regions_set:
            if t_cfg.residency_mode == "strict":
                logger.error(
                    "Tenant %s requested region '%s' not in ASR_ALLOWED_REGIONS %s. "
                    "Residency mode is 'strict': refusing session and emitting degraded event.",
                    tenant,
                    requested_region,
                    sorted(self.settings.allowed_regions_set),
                )
                pipeline_stub = MeetingPipeline(
                    tenant_id=tenant,
                    meeting_id=meeting_id,
                    worker_id=self.worker_id,
                    redis_client=self.redis,
                    provider=get_provider("stub", redis_client=self.redis),
                    tenant_config=t_cfg,
                    settings=self.settings,
                    metrics=self.metrics,
                    auditor=self.auditor,
                )
                await pipeline_stub._emit_degraded_event(
                    state="degraded",
                    reason="residency_violation",
                )
                return
            else:
                # Preferred mode: fall back to global if global allowed, audited
                if "global" in self.settings.allowed_regions_set:
                    effective_region = "global"
                    logger.warning(
                        "Tenant %s requested region '%s' not in ASR_ALLOWED_REGIONS %s. "
                        "Residency mode is 'preferred': falling back to global (audited).",
                        tenant,
                        requested_region,
                        sorted(self.settings.allowed_regions_set),
                    )
                    self.auditor._enqueue(
                        tenant,
                        "system_asr",
                        "asr_region_fallback",
                        {
                            "meeting_id": meeting_id,
                            "requested_region": requested_region,
                            "effective_region": effective_region,
                        },
                    )
                else:
                    logger.error(
                        "Tenant %s requested region '%s' not allowed and "
                        "global fallback unavailable.",
                        tenant,
                        requested_region,
                    )
                    return

        # Policy Invariant: Any non-global region or strict tenant forces mip_opt_out=true
        mip_opt_out = True

        base_url = resolve_asr_base_url(self.settings, effective_region)
        if self.settings.asr_provider == "deepgram" and self.settings.deepgram_api_key:
            provider_kwargs.update(
                {
                    "api_key": self.settings.deepgram_api_key,
                    "base_url": base_url,
                    "mip_opt_out": mip_opt_out,
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
            on_recycle=self._recycle_meeting,
        )
        pipeline.started_via = started_via
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
        interval = max(0.005, self.settings.asr_reconcile_interval_ms / 1000.0)
        while self._is_running:
            try:
                await asyncio.sleep(interval)
                if not self._is_running:
                    break

                if len(self._active_pipelines) >= self.settings.asr_max_meetings_per_worker:
                    continue

                cursor = "0"
                while self._is_running:
                    res = await self.redis.sscan(ACTIVE_MEETINGS_SET_KEY, cursor=cursor, count=100)
                    if isinstance(res, (tuple, list)) and len(res) == 2:
                        cursor_val, members = res
                        cursor = (
                            cursor_val.decode()
                            if isinstance(cursor_val, bytes)
                            else str(cursor_val)
                        )
                    else:
                        members = res
                        cursor = "0"

                    if not members:
                        if cursor == "0":
                            break
                        continue

                    candidates: list[tuple[str, str, str]] = []
                    for m_raw in members:
                        m_str = m_raw.decode("utf-8") if isinstance(m_raw, bytes) else str(m_raw)
                        if "|" not in m_str:
                            continue
                        tenant, meeting_id = m_str.split("|", 1)
                        if meeting_id in self._active_pipelines:
                            continue
                        l_key = asr_owner_key(tenant, meeting_id)
                        candidates.append((tenant, meeting_id, l_key))

                    if candidates:
                        # Pipeline the lease GETs
                        pipe = self.redis.pipeline(transaction=False)
                        for _, _, l_key in candidates:
                            pipe.get(l_key)
                        owners = await pipe.execute()

                        for (tenant, meeting_id, _), owner in zip(candidates, owners, strict=False):
                            if (
                                len(self._active_pipelines)
                                >= self.settings.asr_max_meetings_per_worker
                            ):
                                break
                            if owner is not None:
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
                                await self._start_pipeline(
                                    tenant, meeting_id, started_via="reconciler"
                                )

                    if cursor == "0":
                        break

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
            except aioredis.ResponseError as exc:
                if "NOGROUP" in str(exc):
                    logger.warning(
                        "NOGROUP on %s; recreating consumer group 'asr' (id '0') with retry",
                        LIFECYCLE_STREAM,
                    )
                    await self._ensure_lifecycle_group()
                    await asyncio.sleep(0.05)
                else:
                    if self._is_running:
                        logger.warning("Lifecycle ResponseError: %s", exc)
                        await asyncio.sleep(0.5)
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
            await self._start_pipeline(
                tenant, meeting_id, lifecycle_entry_id=eid_str, started_via="lifecycle"
            )

        elif event in ("meeting_ended", "meeting_abandoned"):
            await self.redis.xack(LIFECYCLE_STREAM, "asr", eid_str)

            if meeting_id in self._active_pipelines:
                lease_task = self._lease_tasks.pop(meeting_id, None)
                if lease_task:
                    lease_task.cancel()

                pipeline = self._active_pipelines.pop(meeting_id)
                t = asyncio.create_task(self._stop_pipeline_lifecycle(tenant, meeting_id, pipeline))
                self._stopping_tasks.add(t)
                t.add_done_callback(self._stopping_tasks.discard)
                await asyncio.shield(t)

    async def _stop_pipeline_lifecycle(
        self, tenant: str, meeting_id: str, pipeline: MeetingPipeline
    ) -> None:
        try:
            await pipeline.stop(is_ended=True)
        finally:
            await self._release_lease(tenant, meeting_id)

    async def stop(self) -> None:
        """Gracefully stop worker daemon on SIGTERM."""
        self._is_running = False
        if self._control_task:
            self._control_task.cancel()
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

        # Wait for any in-flight stopping tasks (e.g. from lifecycle meeting_ended)
        if self._stopping_tasks:
            await asyncio.gather(*list(self._stopping_tasks), return_exceptions=True)

        if self.health_server:
            await self.health_server.stop()

        await self.auditor.stop()

        if self.redis:
            await self.redis.aclose()
        logger.info("ASR Service worker %s stopped", self.worker_id)

    async def _control_loop(self) -> None:
        """Test hook: listen on mm:system:asr_control for fault injection commands."""
        control_stream = "mm:system:asr_control"
        last_id = "$"
        logger.info("ASR test hook control loop active on stream %s", control_stream)
        while self._is_running:
            try:
                res = await self.redis.xread({control_stream: last_id}, count=10, block=500)
                if not res:
                    continue
                for _, messages in res:
                    for entry_id, fields in messages:
                        last_id = (
                            entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                        )

                        def dec(v: Any) -> str:
                            return v.decode() if isinstance(v, bytes) else str(v)

                        action = dec(fields.get(b"action") or fields.get("action") or "")
                        cmd = dec(fields.get(b"command") or fields.get("command") or "")
                        mid = dec(fields.get(b"meeting_id") or fields.get("meeting_id") or "")

                        if not mid and cmd.startswith("drop_provider "):
                            mid = cmd.split(" ", 1)[1].strip()
                            action = "drop_provider"

                        if action == "drop_provider" and mid:
                            pipeline = self._active_pipelines.get(mid)
                            if pipeline and hasattr(pipeline.provider, "_debug_drop_connection"):
                                logger.warning(
                                    "[TEST_HOOK] Dropping connection for meeting %s", mid
                                )
                                pipeline.provider._debug_drop_connection()
                        elif action == "provider_outage" and mid:
                            pipeline = self._active_pipelines.get(mid)
                            if pipeline:
                                raw_dur = fields.get(b"duration_s") or fields.get("duration_s")
                                dur = float(dec(raw_dur or "5.0"))
                                logger.warning(
                                    "[TEST_HOOK] Simulating %.1fs provider outage for meeting %s",
                                    dur,
                                    mid,
                                )
                                pipeline._debug_block_connect_until = time.time() + dur
                                if hasattr(pipeline.provider, "_debug_drop_connection"):
                                    pipeline.provider._debug_drop_connection()
                                else:
                                    asyncio.create_task(pipeline.provider.close())
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Error in _control_loop: %s", exc)
                    await asyncio.sleep(0.5)
