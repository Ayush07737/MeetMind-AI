"""MeetMind AI — Meeting ASR Pipeline Service (WP-D).

Coordinates per-meeting streaming speech-to-text pipeline:
- Consumer group 'asr' on mm:{tenant}:audio:{meeting}.
- In-memory bounded reconnect buffer (30s) and delayed ack window (5s).
- TimelineMapper for stable timestamps across reconnects and client-seq gaps.
- Deduplication by deterministic utterance_id and text-time overlap.
- Publishing transcript_final to internal bus, transcript_interim to out stream,
  and degraded events to both streams.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import random
import time
from typing import Any

import redis.asyncio as aioredis
from meetmind_schemas.events import (
    compute_utterance_id,
    decode_audio_frame,
)
from meetmind_schemas.keys import (
    audio_stream_key,
    events_stream_key,
    meeting_session_key,
    out_stream_key,
)

from .audit import AsrAuditor
from .config import AsrSettings
from .dedupe import Deduplicator
from .health import AsrMetrics
from .providers.base import (
    ASRProvider,
    AudioChunk,
    ProviderEvent,
    ProviderSession,
)
from .speaker import SpeakerNameResolver
from .tenant_config import AsrTenantConfig
from .timeline import TimelineMapper

logger = logging.getLogger(__name__)


class MeetingPipeline:
    """Manages audio ingestion, transcription, and publishing for a single meeting."""

    def __init__(
        self,
        tenant_id: str,
        meeting_id: str,
        worker_id: str,
        redis_client: aioredis.Redis,
        provider: ASRProvider,
        tenant_config: AsrTenantConfig,
        settings: AsrSettings,
        metrics: AsrMetrics,
        auditor: AsrAuditor,
        on_recycle: Any = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.meeting_id = meeting_id
        self.worker_id = worker_id
        self.redis = redis_client
        self.provider = provider
        self.tenant_config = tenant_config
        self.settings = settings
        self.metrics = metrics
        self.auditor = auditor
        self._on_recycle = on_recycle
        self._start_wall_time = time.time()

        self._audio_key = audio_stream_key(tenant_id, meeting_id)
        self._events_key = events_stream_key(tenant_id, meeting_id)
        self._out_key = out_stream_key(tenant_id, meeting_id)
        self._session_key = meeting_session_key(tenant_id, meeting_id)

        self._timeline = TimelineMapper()
        self._dedupe = Deduplicator()
        self._speaker_resolver: SpeakerNameResolver | None = None
        self._highest_published_start_ms: dict[tuple[int, str], int] = collections.defaultdict(
            lambda: -1
        )

        # State & lifecycle
        self.started_via: str = "reconciler"
        self._is_running = True
        self._meeting_ended = False
        self._unsupported_codec_reported = False
        self._last_gap_degraded_time = 0.0
        self._degraded_active = False
        self._degraded_since_ms: int = 0
        self._total_degraded_s: float = 0.0

        # Sequence tracking
        self._last_seen_seq: int | None = None
        self._channels = 1
        self._sample_rate = 16000

        # Buffering & Acking
        # Pending ack queue: deque of (send_time_perf, entry_id)
        self._pending_acks: collections.deque[tuple[float, str]] = collections.deque()
        # In-memory audio buffer for provider downtime: deque of AudioChunk
        self._reconnect_buffer: collections.deque[tuple[str, AudioChunk]] = collections.deque()
        # Replay window: keep last sent chunks within replay window for resending on crash/reconnect
        self._sent_replay_history: collections.deque[tuple[float, AudioChunk]] = collections.deque()

        # Speaker alignment & session tracking
        self._speaker_epoch: int = 0
        self._speaker_mapping: dict[int, str] = {}
        self._published_utterances_for_alignment: collections.deque[tuple[int, int, str, str]] = (
            collections.deque(maxlen=200)
        )

        # Chunk history: [client_seq, start_ms, end_ms, t_recv_wall, t_recv_perf, t_send_perf]
        self._chunk_history: collections.deque[list[Any]] = collections.deque(maxlen=600)
        self._seen_interim_words_window: collections.deque[tuple[str, int]] = collections.deque(
            maxlen=300
        )
        self._seen_interim_words: set[tuple[str, int]] = set()
        self._prev_interim_tokens: dict[int, list[str]] = {}
        self._standby_provider: ASRProvider | None = None

        # Provider session timeline tracking (Requirement 2: no TimelineMapper for latency)
        self._provider_session_origin_wall: float | None = None

        # Bounded sequence dispatch deduplication (Requirement 4 & 5)
        self._dispatched_seqs_window: collections.deque[int] = collections.deque(maxlen=5000)
        self._dispatched_seqs_set: set[int] = set()
        self._processed_entry_ids_window: collections.deque[str] = collections.deque(maxlen=5000)
        self._processed_entry_ids: set[str] = set()

        # Audit stats
        self._chunks_sent = 0
        self._bytes_sent = 0
        self._reconnect_count = 0
        self._dropped_chunks_count = 0
        self._all_dispatched_client_seqs: collections.deque[int] = collections.deque(maxlen=5000)
        self._detected_gaps: list[dict[str, Any]] = []
        self._reconnect_attempts: list[dict[str, Any]] = []

        # Cumulative stats for worker handoff / recycle (Requirement A1.2)
        self._cumulative_chunks_sent: int = 0
        self._cumulative_bytes_sent: int = 0
        self._cumulative_reconnect_count: int = 0
        self._cumulative_dropped_chunks_count: int = 0
        self._cumulative_degraded_s: float = 0.0
        self._cumulative_gaps: list[dict[str, Any]] = []
        self._cumulative_accepted_seqs: set[int] = set()
        self._cumulative_sent_seqs: list[int] = []

        # Outage and catch-up state
        self._is_catching_up = False
        self._is_reconnecting = False
        self._drain_pel_needed = True
        self._last_seen_capture_ts_ms: int | None = None
        self._last_still_degraded_time = 0.0
        self._debug_block_connect_until = 0.0

        # Background tasks
        self._tasks: list[asyncio.Task[None]] = []
        self._audio_read_task: asyncio.Task[None] | None = None
        self._events_task: asyncio.Task[None] | None = None

    @property
    def _provider_is_ready(self) -> bool:
        return bool(getattr(self.provider, "_is_open", False)) and not self._is_reconnecting

    async def start(self) -> None:
        """Initialize pipeline, resolve metadata, and spawn worker tasks."""
        await self._init_metadata()
        await self._ensure_consumer_group()

        # Open initial provider session
        try:
            await self._connect_provider()
        except Exception as exc:
            logger.warning(
                "Initial provider connection failed on %s: %s; entering reconnect loop",
                self.meeting_id,
                exc,
            )
            asyncio.create_task(self._handle_reconnect())

        # Pre-warm standby connection if warm standby enabled
        if self.settings.asr_warm_standby:
            asyncio.create_task(self._init_standby())

        # Record audit start with dynamic endpoint host
        import urllib.parse

        endpoint_url = (
            getattr(self.provider, "base_url", None)
            or getattr(self.provider, "_base_url", None)
            or self.settings.deepgram_base_url
        )
        parsed_endpoint = urllib.parse.urlparse(endpoint_url)
        endpoint_host = parsed_endpoint.netloc or parsed_endpoint.path or endpoint_url

        self.auditor.record_session_started(
            tenant_id=self.tenant_id,
            user_id="system_asr",
            meeting_id=self.meeting_id,
            provider=self.settings.asr_provider,
            endpoint_host=endpoint_host,
            language=self.tenant_config.language,
            mip_opt_out=True,
        )

        # Launch consumer loops
        self._audio_read_task = asyncio.create_task(self._audio_read_loop())
        self._tasks.append(self._audio_read_task)
        self._tasks.append(asyncio.create_task(self._ack_flush_loop()))
        self._events_task = asyncio.create_task(self._provider_events_loop())
        self._tasks.append(self._events_task)

    async def _init_metadata(self) -> None:
        """Fetch session hash metadata (local_user_name, attendees)."""
        try:
            data = await self.redis.hgetall(self._session_key)
            if data:
                d = {
                    (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
                        v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    )
                    for k, v in data.items()
                }
                local_user = d.get("local_user_name")
                attendees_raw = d.get("attendees")
                attendees = []
                if attendees_raw:
                    with contextlib.suppress(Exception):
                        attendees = json.loads(attendees_raw)
                self._speaker_resolver = SpeakerNameResolver(
                    local_user_name=local_user,
                    attendees=attendees,
                )
                origin_raw = d.get("origin_capture_ts")
                if origin_raw:
                    with contextlib.suppress(Exception):
                        self._timeline.meeting_origin_capture_ts = int(origin_raw)
        except Exception as exc:
            logger.warning("Failed to load metadata for %s: %s", self.meeting_id, exc)

        # Load cumulative worker metrics if this meeting is continuing across worker recycle (A1.2)
        try:
            cum_key = f"mm:{self.tenant_id}:asr_cumulative:{self.meeting_id}"
            cum_raw = await self.redis.hgetall(cum_key)
            if cum_raw:
                decoded = {
                    (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
                        v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    )
                    for k, v in cum_raw.items()
                }
                self._cumulative_chunks_sent = int(decoded.get("chunks_sent", 0))
                self._cumulative_bytes_sent = int(decoded.get("bytes_sent", 0))
                self._cumulative_reconnect_count = int(decoded.get("reconnects", 0))
                self._cumulative_dropped_chunks_count = int(decoded.get("dropped_chunks", 0))
                self._cumulative_degraded_s = float(decoded.get("degraded_seconds", 0.0))
                if "gaps" in decoded:
                    with contextlib.suppress(Exception):
                        self._cumulative_gaps = json.loads(decoded["gaps"])
                if "accepted_seqs" in decoded:
                    with contextlib.suppress(Exception):
                        self._cumulative_accepted_seqs = set(json.loads(decoded["accepted_seqs"]))
                if "sent_seqs" in decoded:
                    with contextlib.suppress(Exception):
                        self._cumulative_sent_seqs = json.loads(decoded["sent_seqs"])
                logger.info(
                    "Meeting %s loaded cumulative recycle state: chunks_sent=%d, reconnects=%d",
                    self.meeting_id,
                    self._cumulative_chunks_sent,
                    self._cumulative_reconnect_count,
                )
        except Exception as exc:
            logger.warning("Failed to load cumulative metrics for %s: %s", self.meeting_id, exc)

        # Seed deduplicator from existing finals on the internal bus for crash recovery failover
        try:
            recent_events = await self.redis.xrevrange(self._events_key, count=100)
            if recent_events:
                for _, fields in reversed(recent_events):
                    p_raw = fields.get(b"payload") or fields.get("payload")
                    if p_raw:
                        env = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
                        if env.get("event_type") == "transcript_final":
                            p = env.get("payload", {})
                            uid = p.get("utterance_id")
                            if uid:
                                ch = p.get("channel") if p.get("channel") is not None else 0
                                spk = p.get("speaker_id") or "spk_0"
                                s_ms = p.get("start_ms", 0)
                                self._highest_published_start_ms[(ch, spk)] = max(
                                    self._highest_published_start_ms[(ch, spk)], s_ms
                                )
                                self._dedupe.record_published(
                                    utterance_id=uid,
                                    channel=ch,
                                    text=p.get("text", ""),
                                    start_ms=s_ms,
                                    end_ms=p.get("end_ms", 0),
                                )
        except Exception as exc:
            logger.debug("Could not seed dedupe state from %s: %s", self._events_key, exc)

    async def _ensure_consumer_group(self) -> None:
        try:
            await self.redis.xgroup_create(self._audio_key, "asr", id="0", mkstream=True)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                logger.warning("Error creating asr group on %s: %s", self._audio_key, exc)

    async def _connect_provider(self) -> None:
        if (
            self.settings.asr_test_hooks
            and getattr(self, "_debug_block_connect_until", 0.0) > time.time()
        ):
            raise ConnectionError(
                f"Simulated provider outage: blocked until {self._debug_block_connect_until:.3f}"
            )

        # Reset sequence dispatch deduplication on every new provider session
        self._dispatched_seqs_set.clear()
        self._dispatched_seqs_window.clear()

        session = ProviderSession(
            tenant_id=self.tenant_id,
            meeting_id=self.meeting_id,
            sample_rate=self._sample_rate,
            channels=self._channels,
            language=self.tenant_config.language,
            keyterms=self.tenant_config.keyterms,
        )
        logger.info(
            "Opening provider session for %s/%s (language=%s, keyterms=%d)",
            self.tenant_id,
            self.meeting_id,
            self.tenant_config.language,
            len(self.tenant_config.keyterms),
        )
        await self.provider.open(session)
        session_key = f"mm:{self.tenant_id}:asr_session:{self.meeting_id}"
        session_data = json.dumps(
            {
                "tenant_id": self.tenant_id,
                "meeting_id": self.meeting_id,
                "language": self.tenant_config.language,
                "keyterms": list(self.tenant_config.keyterms),
            }
        )
        with contextlib.suppress(Exception):
            await self.redis.set(session_key, session_data, ex=86400)

    async def _init_standby(self) -> None:
        """Pre-warm idle standby provider connection if warm standby is enabled."""
        if not self.settings.asr_warm_standby or not self._is_running or self._meeting_ended:
            return
        if self._standby_provider and getattr(self._standby_provider, "_is_open", False):
            return
        try:
            from .providers import get_provider

            provider_kwargs: dict[str, Any] = {"redis_client": self.redis}
            if self.settings.asr_provider == "deepgram" and self.settings.deepgram_api_key:
                prov_base_url = getattr(self.provider, "base_url", self.settings.deepgram_base_url)
                provider_kwargs.update(
                    {
                        "api_key": self.settings.deepgram_api_key,
                        "base_url": prov_base_url,
                        "mip_opt_out": True,
                    }
                )
            standby = get_provider(self.settings.asr_provider, **provider_kwargs)
            session = ProviderSession(
                tenant_id=self.tenant_id,
                meeting_id=self.meeting_id,
                sample_rate=self._sample_rate,
                channels=self._channels,
                language=self.tenant_config.language,
                keyterms=self.tenant_config.keyterms,
            )
            await standby.open(session)
            self._standby_provider = standby
            logger.info("Warm standby provider connection pre-warmed for %s", self.meeting_id)
        except Exception as exc:
            logger.warning(
                "Failed to initialize warm standby provider for %s: %s",
                self.meeting_id,
                exc,
            )

    async def _audio_read_loop(self) -> None:
        """Read audio frames from Redis stream mm:{tenant}:audio:{meeting}."""
        while self._is_running:
            try:
                # If provider is not connected or reconnecting, do NOT read new audio.
                # Audio accumulates safely in Redis stream mm:{tenant}:audio:{meeting}
                # bounded by AUDIO_STREAM_MAXLEN without ack-and-drop.
                if not self._provider_is_ready:
                    await asyncio.sleep(0.05)
                    continue

                # 1. Periodic autoclaim for entries idle > 15s
                # (prevents re-claiming active replay window)
                with contextlib.suppress(Exception):
                    claimed = await self.redis.xautoclaim(
                        self._audio_key,
                        "asr",
                        self.worker_id,
                        min_idle_time=max(15000, self.settings.asr_replay_window_ms + 10000),
                        start_id="0-0",
                        count=20,
                    )
                    if claimed and len(claimed) >= 2 and claimed[1]:
                        for entry_id, fields in claimed[1]:
                            if not self._is_running or not self._provider_is_ready:
                                break
                            await self._process_raw_entry(entry_id, fields)

                # 2. Drain pending unacked entries delivered to this worker (PEL read with "0")
                if self._drain_pel_needed:
                    pending = await self.redis.xreadgroup(
                        "asr",
                        self.worker_id,
                        {self._audio_key: "0"},
                        count=50,
                    )
                    has_pending = False
                    if pending:
                        for _, messages in pending:
                            if messages:
                                has_pending = True
                                for entry_id, fields in messages:
                                    if not self._is_running or not self._provider_is_ready:
                                        break
                                    await self._process_raw_entry(entry_id, fields)
                    if not has_pending or not self._provider_is_ready:
                        self._drain_pel_needed = False

                if not self._is_running or not self._provider_is_ready:
                    continue

                # Worker-recycle policy (Requirement 3)
                if (
                    self._start_wall_time > 0
                    and (time.time() - self._start_wall_time)
                    > self.settings.asr_worker_max_meeting_seconds
                ):
                    logger.info(
                        "[WORKER_RECYCLE] Meeting %s exceeded max duration %.1fs; "
                        "initiating graceful recycle",
                        self.meeting_id,
                        self.settings.asr_worker_max_meeting_seconds,
                    )
                    if self._on_recycle:
                        asyncio.create_task(self._on_recycle(self.tenant_id, self.meeting_id))
                    break

                # 3. XREADGROUP for new entries (BLOCK 100ms)
                entries = await self.redis.xreadgroup(
                    "asr",
                    self.worker_id,
                    {self._audio_key: ">"},
                    count=50,
                    block=100,
                )
                if entries:
                    for _, messages in entries:
                        for entry_id, fields in messages:
                            if not self._is_running or not self._provider_is_ready:
                                break
                            await self._process_raw_entry(entry_id, fields)

            except asyncio.CancelledError:
                break
            except aioredis.ResponseError as exc:
                if "NOGROUP" in str(exc):
                    logger.warning(
                        "NOGROUP on %s; recreating consumer group 'asr'", self._audio_key
                    )
                    await self._ensure_consumer_group()
                    await asyncio.sleep(0.05)
                elif self._is_running:
                    logger.warning("Audio read loop ResponseError on %s: %s", self.meeting_id, exc)
                    await asyncio.sleep(0.1)
            except Exception as exc:
                if self._is_running:
                    logger.warning("Audio read loop error on %s: %s", self.meeting_id, exc)
                    await asyncio.sleep(0.1)

    def _mark_processed_eid(self, eid_str: str) -> None:
        if eid_str in self._processed_entry_ids:
            return
        if len(self._processed_entry_ids_window) >= (
            self._processed_entry_ids_window.maxlen or 5000
        ):
            old_eid = self._processed_entry_ids_window.popleft()
            self._processed_entry_ids.discard(old_eid)
        self._processed_entry_ids_window.append(eid_str)
        self._processed_entry_ids.add(eid_str)

    async def _process_raw_entry(self, entry_id: Any, fields: dict[Any, Any]) -> None:
        eid_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
        raw_data = fields.get(b"audio_data") or fields.get("audio_data")
        if not raw_data or not isinstance(raw_data, bytes):
            # Acknowledge corrupted entry immediately
            await self.redis.xack(self._audio_key, "asr", eid_str)
            self._mark_processed_eid(eid_str)
            return

        self.metrics.chunks_in += 1

        try:
            hdr, payload = decode_audio_frame(raw_data)
        except Exception as exc:
            logger.warning("Invalid audio header in %s: %s", self.meeting_id, exc)
            await self.redis.xack(self._audio_key, "asr", eid_str)
            self._mark_processed_eid(eid_str)
            return

        # Check Codec (Decision 1: v1 supports codec=1 PCM16 LE only)
        if hdr.codec != 1:
            # Codec 2 (Opus) is acked and dropped with single degraded event per meeting
            await self.redis.xack(self._audio_key, "asr", eid_str)
            self._mark_processed_eid(eid_str)
            self.metrics.dropped_chunks += 1
            if not self._unsupported_codec_reported:
                self._unsupported_codec_reported = True
                await self._emit_degraded_event(
                    state="degraded",
                    reason="unsupported_codec",
                )
            return

        self._channels = hdr.channels
        self._sample_rate = hdr.sample_rate
        prov_session = getattr(self.provider, "session", None)
        if prov_session and prov_session.channels != self._channels:
            prov_session.channels = self._channels

        # Sequence deduplication (Requirement 4: never re-stream duplicate chunks to provider)
        if hdr.client_seq in self._dispatched_seqs_set:
            self._mark_processed_eid(eid_str)
            return

        # Beyond the backlog limit: detect client_seq jump and publish ONE audio_gap event
        # Sequence tracking must be strictly monotonic across provider reconnects and
        # retransmits (S0-12)
        if self._last_seen_seq is None:
            self._last_seen_seq = hdr.client_seq
            self._last_seen_capture_ts_ms = hdr.capture_ts_ms
        elif hdr.client_seq > self._last_seen_seq:
            if hdr.client_seq > self._last_seen_seq + 1:
                gap_chunks = hdr.client_seq - (self._last_seen_seq + 1)
                from_seq = self._last_seen_seq + 1
                to_seq = hdr.client_seq - 1
                cap_start = self._last_seen_capture_ts_ms or (
                    hdr.capture_ts_ms - (gap_chunks * 100)
                )
                cap_end = hdr.capture_ts_ms
                gap_info = {
                    "from_seq": from_seq,
                    "to_seq": to_seq,
                    "gap_chunks": gap_chunks,
                    "capture_start_ms": cap_start,
                    "capture_end_ms": cap_end,
                }
                self._detected_gaps.append(gap_info)
                await self._emit_audio_gap_event(gap_info)

            self._last_seen_seq = hdr.client_seq
            self._last_seen_capture_ts_ms = hdr.capture_ts_ms
        # If hdr.client_seq <= self._last_seen_seq: retransmit or replay chunk.
        # NEVER move self._last_seen_seq backwards.

        # Ensure meeting origin capture timestamp is anchored consistently across workers
        if self._timeline.meeting_origin_capture_ts is None:
            with contextlib.suppress(Exception):
                res = await self.redis.hsetnx(
                    self._session_key, "origin_capture_ts", str(hdr.capture_ts_ms)
                )
                if res == 0:
                    val = await self.redis.hget(self._session_key, "origin_capture_ts")
                    if val:
                        self._timeline.meeting_origin_capture_ts = int(
                            val.decode() if isinstance(val, bytes) else val
                        )
                else:
                    self._timeline.meeting_origin_capture_ts = hdr.capture_ts_ms

        # Extract clock sync timing metadata from stream fields (Q1)
        raw_cap_server = fields.get(b"capture_server_ms") or fields.get("capture_server_ms")
        capture_server_ms = (
            int(raw_cap_server.decode() if isinstance(raw_cap_server, bytes) else raw_cap_server)
            if raw_cap_server
            else hdr.capture_ts_ms
        )
        raw_ts_ms = fields.get(b"timestamp_ms") or fields.get("timestamp_ms")
        gw_recv_ms = (
            int(raw_ts_ms.decode() if isinstance(raw_ts_ms, bytes) else raw_ts_ms)
            if raw_ts_ms
            else None
        )
        if gw_recv_ms is not None and capture_server_ms is not None:
            gw_recv_ms = max(capture_server_ms, gw_recv_ms)
        asr_recv_ms = int(time.time() * 1000)
        if gw_recv_ms is not None:
            asr_recv_ms = max(gw_recv_ms, asr_recv_ms)

        # Create AudioChunk (payload is raw PCM audio bytes)
        chunk = AudioChunk(
            data=payload,
            client_seq=hdr.client_seq,
            capture_ts_ms=hdr.capture_ts_ms,
            recv_ns=time.perf_counter_ns(),
        )

        # Track chunk in precision history
        origin = self._timeline.meeting_origin_capture_ts or hdr.capture_ts_ms
        chunk_start_ms = max(0, hdr.capture_ts_ms - origin)
        chunk_end_ms = chunk_start_ms + 100
        hist_entry = [
            chunk.client_seq,
            chunk_start_ms,
            chunk_end_ms,
            time.time(),
            time.perf_counter(),
            0.0,
            capture_server_ms,
            gw_recv_ms,
            asr_recv_ms,
        ]
        self._chunk_history.append(hist_entry)

        # Enqueue chunk
        if len(self._dispatched_seqs_window) >= (self._dispatched_seqs_window.maxlen or 5000):
            old_seq = self._dispatched_seqs_window.popleft()
            self._dispatched_seqs_set.discard(old_seq)
        self._dispatched_seqs_window.append(chunk.client_seq)
        self._dispatched_seqs_set.add(chunk.client_seq)

        await self._dispatch_chunk(eid_str, chunk, hist_entry)
        self._mark_processed_eid(eid_str)

    async def _dispatch_chunk(
        self, entry_id: str, chunk: AudioChunk, hist_entry: list[Any] | None = None
    ) -> None:
        """Deliver chunk to provider with catch-up pacing. Never ack-and-drop."""
        try:
            if self._provider_session_origin_wall is None:
                self._provider_session_origin_wall = time.time()

            await self.provider.send_audio(chunk)
            t_send = time.perf_counter()
            if hist_entry is not None:
                hist_entry[5] = t_send
            self._pending_acks.append((t_send, entry_id))
            self._sent_replay_history.append((t_send, chunk))
            self._timeline.register_chunk(chunk.capture_ts_ms)
            self._chunks_sent += 1
            self._bytes_sent += len(chunk.data)
            self._all_dispatched_client_seqs.append(chunk.client_seq)

            # Periodic GC collection to ensure RSS flatlines on long runs
            if self._chunks_sent % 3000 == 0:
                import gc

                gc.collect()

            # Prune replay history older than replay window
            while self._sent_replay_history and (t_send - self._sent_replay_history[0][0]) > (
                self.settings.asr_replay_window_ms / 1000.0
            ):
                self._sent_replay_history.popleft()

            # Catch-up pacing: if chunk was captured in the past (backlog during outage)
            now_ms = int(time.time() * 1000)
            chunk_age_ms = now_ms - chunk.capture_ts_ms
            if chunk_age_ms > 1000:
                self._is_catching_up = True
                chunk_dur_s = len(chunk.data) / (self._sample_rate * 2 * self._channels)
                catchup_speed = max(1.0, float(self.settings.asr_catchup_speed))
                pacing_sleep = chunk_dur_s / catchup_speed
                await asyncio.sleep(pacing_sleep)
            else:
                self._is_catching_up = False

        except Exception as exc:
            logger.warning("Provider send failed on %s: %s", self.meeting_id, exc)
            self._reconnect_buffer.append((entry_id, chunk))
            asyncio.create_task(self._handle_reconnect())

    async def _handle_reconnect(self) -> None:
        """Reconnect to provider with backoff and flush buffered audio."""
        if self._is_reconnecting:
            return
        self._is_reconnecting = True
        self._reconnect_count += 1
        self.metrics.reconnects += 1
        t_disconnect = time.time()
        self._degraded_since_ms = int(t_disconnect * 1000)
        self._last_still_degraded_time = t_disconnect
        degraded_event_sent = False

        import urllib.parse

        prov_base = (
            getattr(self.provider, "base_url", None)
            or getattr(self.provider, "_base_url", None)
            or self.settings.deepgram_base_url
            or "api.deepgram.com"
        )
        parsed_p = urllib.parse.urlparse(prov_base)
        effective_host = parsed_p.netloc or parsed_p.path or prov_base

        # 1. Warm Standby Swap Fast Path
        if (
            self.settings.asr_warm_standby
            and self._standby_provider
            and getattr(self._standby_provider, "_is_open", False)
        ):
            logger.info("Warm standby swap engaged for meeting %s", self.meeting_id)
            self._speaker_epoch += 1
            self._speaker_mapping.clear()
            old_prov = self.provider
            self.provider = self._standby_provider
            self._standby_provider = None
            with contextlib.suppress(Exception):
                asyncio.create_task(old_prov.close())
            asyncio.create_task(self._init_standby())

            self._timeline.reset_session()
            reaccept_time: float | None = None
            replay_chunks = list(self._sent_replay_history)
            bytes_per_sec = float(max(1, self._sample_rate * 2 * max(1, self._channels)))
            pre_replay_bytes = sum(len(c.data) for _, c in replay_chunks) + sum(
                len(c.data) for _, c in self._reconnect_buffer
            )
            self._provider_session_origin_wall = (
                max(0.0, time.time() - (pre_replay_bytes / bytes_per_sec))
                if pre_replay_bytes > 0
                else None
            )

            for _, chunk in replay_chunks:
                if self._provider_session_origin_wall is None:
                    self._provider_session_origin_wall = time.time()
                await self.provider.send_audio(chunk)
                if reaccept_time is None:
                    reaccept_time = time.time()
                self._timeline.register_chunk(chunk.capture_ts_ms)
                self.metrics.replayed_chunks += 1

            while self._reconnect_buffer:
                eid, chunk = self._reconnect_buffer.popleft()
                if self._provider_session_origin_wall is None:
                    self._provider_session_origin_wall = time.time()
                await self.provider.send_audio(chunk)
                if reaccept_time is None:
                    reaccept_time = time.time()
                t_send = time.perf_counter()
                self._pending_acks.append((t_send, eid))
                self._sent_replay_history.append((t_send, chunk))
                self._timeline.register_chunk(chunk.capture_ts_ms)
                self._chunks_sent += 1
                self._bytes_sent += len(chunk.data)
                self._all_dispatched_client_seqs.append(chunk.client_seq)

            if pre_replay_bytes > 0:
                self._provider_session_origin_wall = max(
                    0.0, time.time() - (pre_replay_bytes / bytes_per_sec)
                )

            recovery_time = reaccept_time if reaccept_time is not None else time.time()
            dur_s = recovery_time - t_disconnect
            self._total_degraded_s += dur_s
            self.metrics.degraded_seconds += dur_s
            self.metrics.record_reconnect_duration(dur_s * 1000.0)
            self._is_reconnecting = False
            self._degraded_active = False
            self._drain_pel_needed = True

            if degraded_event_sent or (recovery_time - t_disconnect) >= 2.0:
                await self._emit_degraded_event(
                    state="recovered",
                    reason="provider_unreachable",
                    since_ms=self._degraded_since_ms,
                )

            logger.info(
                "[RECONNECT_EVENT] meeting=%s (WARM_STANDBY_SWAP) "
                "drop_to_reaccept_ms=%.2f accepted_seqs=%d",
                self.meeting_id,
                dur_s * 1000.0,
                len(getattr(self.provider, "accepted_client_seqs", set())),
            )
            with contextlib.suppress(Exception):
                await self.redis.rpush(
                    f"mm:{self.tenant_id}:asr_reconnects:{self.meeting_id}",
                    json.dumps(
                        {
                            "reaccept_ms": dur_s * 1000.0,
                            "t_disconnect": t_disconnect,
                            "t_recovery": recovery_time,
                            "warm_standby": True,
                        }
                    ),
                )
            return

        async def _degraded_monitor() -> None:
            nonlocal degraded_event_sent
            delay = max(0.0, 2.0 - (time.time() - t_disconnect))
            if delay > 0:
                await asyncio.sleep(delay)
            if not self._is_reconnecting:
                return
            if not degraded_event_sent:
                degraded_event_sent = True
                self._degraded_active = True
                await self._emit_degraded_event(
                    state="degraded",
                    reason="provider_unreachable",
                    since_ms=self._degraded_since_ms,
                )
                self._last_still_degraded_time = time.time()

            while self._is_reconnecting:
                await asyncio.sleep(30.0)
                if not self._is_reconnecting:
                    break
                elapsed_s = round(time.time() - t_disconnect, 1)
                await self._emit_degraded_event(
                    state="still_degraded",
                    reason="provider_unreachable",
                    since_ms=self._degraded_since_ms,
                    elapsed_s=elapsed_s,
                )
                self._last_still_degraded_time = time.time()

        monitor_task = asyncio.create_task(_degraded_monitor())

        backoffs = [0.0, 0.20, 0.40, 0.50, 0.50, 1.0, 2.0]
        backoff_idx = 0
        attempt = 0

        while self._is_running:
            delay = backoffs[min(backoff_idx, len(backoffs) - 1)]
            if delay > 0:
                await asyncio.sleep(delay + random.uniform(0.005, 0.02))
            backoff_idx += 1
            attempt += 1

            attempt_start = time.time()
            elapsed_since_disc = attempt_start - t_disconnect

            try:
                with contextlib.suppress(Exception):
                    await self.provider.close()

                await self._connect_provider()

                # Reconnect successful: reset timeline session and resend replay window
                self._speaker_epoch += 1
                self._speaker_mapping.clear()
                self._timeline.reset_session()
                reaccept_time: float | None = None
                replay_chunks = list(self._sent_replay_history)
                bytes_per_sec = float(max(1, self._sample_rate * 2 * max(1, self._channels)))
                pre_replay_bytes = sum(len(c.data) for _, c in replay_chunks) + sum(
                    len(c.data) for _, c in self._reconnect_buffer
                )
                self._provider_session_origin_wall = (
                    max(0.0, time.time() - (pre_replay_bytes / bytes_per_sec))
                    if pre_replay_bytes > 0
                    else None
                )

                # Resend replay window audio
                for _, chunk in replay_chunks:
                    if self._provider_session_origin_wall is None:
                        self._provider_session_origin_wall = time.time()
                    await self.provider.send_audio(chunk)
                    if reaccept_time is None:
                        reaccept_time = time.time()
                    self._timeline.register_chunk(chunk.capture_ts_ms)
                    self.metrics.replayed_chunks += 1

                # Send buffered audio
                while self._reconnect_buffer:
                    eid, chunk = self._reconnect_buffer.popleft()
                    if self._provider_session_origin_wall is None:
                        self._provider_session_origin_wall = time.time()
                    await self.provider.send_audio(chunk)
                    if reaccept_time is None:
                        reaccept_time = time.time()
                    t_send = time.perf_counter()
                    self._pending_acks.append((t_send, eid))
                    self._sent_replay_history.append((t_send, chunk))
                    self._timeline.register_chunk(chunk.capture_ts_ms)
                    self._chunks_sent += 1
                    self._bytes_sent += len(chunk.data)
                    self._all_dispatched_client_seqs.append(chunk.client_seq)

                if pre_replay_bytes > 0:
                    self._provider_session_origin_wall = max(
                        0.0, time.time() - (pre_replay_bytes / bytes_per_sec)
                    )

                # Recovery complete
                recovery_time = reaccept_time if reaccept_time is not None else time.time()
                dur_s = recovery_time - t_disconnect
                self._total_degraded_s += dur_s
                self.metrics.degraded_seconds += dur_s
                self.metrics.record_reconnect_duration(dur_s * 1000.0)
                self._is_reconnecting = False
                self._degraded_active = False
                self._drain_pel_needed = True

                monitor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor_task

                logger.info(
                    "[RECONNECT_EVENT] meeting=%s drop_to_reaccept_ms=%.2f accepted_seqs=%d",
                    self.meeting_id,
                    dur_s * 1000.0,
                    len(getattr(self.provider, "accepted_client_seqs", set())),
                )
                with contextlib.suppress(Exception):
                    await self.redis.rpush(
                        f"mm:{self.tenant_id}:asr_reconnects:{self.meeting_id}",
                        json.dumps(
                            {
                                "reaccept_ms": dur_s * 1000.0,
                                "t_disconnect": t_disconnect,
                                "t_recovery": recovery_time,
                            }
                        ),
                    )

                if degraded_event_sent or (dur_s >= 2.0):
                    await self._emit_degraded_event(
                        state="recovered",
                        reason="provider_unreachable",
                        since_ms=self._degraded_since_ms,
                    )
                break
            except Exception as e:
                exc_type = type(e).__name__
                status_code = getattr(e, "status_code", None) or getattr(e, "code", None)
                attempt_info = {
                    "timestamp": time.time(),
                    "attempt": attempt,
                    "exc_type": exc_type,
                    "status": status_code,
                    "elapsed_s": round(elapsed_since_disc, 2),
                    "backoff_s": round(delay, 2),
                    "host": effective_host,
                    "exc_msg": str(e),
                }
                self._reconnect_attempts.append(attempt_info)
                logger.info(
                    "[RECONNECT_ATTEMPT] attempt=%d host=%s elapsed_s=%.2f backoff_s=%.2f "
                    "exc_type=%s exc_msg=%s",
                    attempt,
                    effective_host,
                    elapsed_since_disc,
                    delay,
                    exc_type,
                    e,
                )

    async def _ack_flush_loop(self) -> None:
        """Acknowledge entries that have aged out of the replay window."""
        replay_s = self.settings.asr_replay_window_ms / 1000.0
        while self._is_running:
            try:
                now = time.perf_counter()
                to_ack: list[str] = []

                while self._pending_acks and (now - self._pending_acks[0][0]) >= replay_s:
                    _, eid = self._pending_acks.popleft()
                    to_ack.append(eid)

                if to_ack:
                    await self.redis.xack(self._audio_key, "asr", *to_ack)
                    await self.redis.xdel(self._audio_key, *to_ack)
                    self.metrics.chunks_acked += len(to_ack)

                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Ack flush error on %s: %s", self.meeting_id, exc)
                    await asyncio.sleep(0.5)

    async def _provider_events_loop(self) -> None:
        """Receive provider events and publish them to appropriate streams."""
        while True:
            try:
                curr_prov = self.provider
                async for event in curr_prov.events():
                    if event.event_type in ("final", "interim"):
                        await self._handle_transcript_event(event)
                    elif event.event_type == "error":
                        if not self._meeting_ended:
                            await self._emit_degraded_event(
                                state="degraded",
                                reason="provider_error",
                            )
                    elif event.event_type == "closed":
                        if self._is_running and not self._meeting_ended:
                            asyncio.create_task(self._handle_reconnect())
                        break
                    if self.provider is not curr_prov:
                        break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Provider event loop error on %s: %s", self.meeting_id, exc)

            if not self._is_running or self._meeting_ended:
                break
            await asyncio.sleep(0.02)

    def _find_chunk_for_end_ms(
        self, end_ms: int
    ) -> tuple[int, float, float, float, int | None, int | None, int | None] | None:
        """Find chunk containing or nearest to end_ms.

        Returns (client_seq, t_recv_wall, t_recv_perf, t_send_perf,
        capture_server_ms, gw_recv_ms, asr_recv_ms).
        """
        if not self._chunk_history:
            return None
        best_match = None
        min_distance = float("inf")
        for entry in reversed(self._chunk_history):
            client_seq, c_start, c_end, t_r_wall, t_r_perf, t_s_perf = entry[:6]
            cap_srv = entry[6] if len(entry) > 6 else None
            gw_r = entry[7] if len(entry) > 7 else None
            asr_r = entry[8] if len(entry) > 8 else None
            match_tuple = (client_seq, t_r_wall, t_r_perf, t_s_perf, cap_srv, gw_r, asr_r)
            if c_start <= end_ms <= c_end:
                return match_tuple
            dist = abs(c_end - end_ms)
            if dist < min_distance:
                min_distance = dist
                best_match = match_tuple
        return best_match

    async def _handle_transcript_event(self, event: ProviderEvent) -> None:
        if not event.text or not event.text.strip():
            return

        provider_recv_ms = int(time.time() * 1000)

        if getattr(event, "recv_perf", 0.0) > 0 and event.is_final:
            parse_ms = max(0.001, (time.perf_counter() - event.recv_perf) * 1000.0)
            self.metrics.record_stage_timing("parse", parse_ms)

        # Stage 1: Timeline mapping
        t_tl_0 = time.perf_counter()
        start_ms, end_ms, start_epoch_ms = self._timeline.map_range(event.start_ms, event.end_ms)
        self.metrics.record_stage_timing("timeline", (time.perf_counter() - t_tl_0) * 1000.0)

        # Stage 2: Speaker resolution & alignment across epochs
        t_spk_0 = time.perf_counter()
        raw_spk = event.speaker

        if self._channels == 2:
            if event.channel == 0:
                spk_id = "spk_0"
            else:
                spk_id = f"spk_{raw_spk}" if raw_spk is not None else "spk_1"
        else:
            if self._speaker_epoch == 0:
                spk_id = f"spk_{raw_spk}"
            else:
                if raw_spk in self._speaker_mapping:
                    spk_id = self._speaker_mapping[raw_spk]
                else:
                    aligned = None
                    clean_text = event.text.strip().lower()
                    for prev_start, prev_end, prev_text, prev_spk in reversed(
                        self._published_utterances_for_alignment
                    ):
                        time_overlap = max(start_ms, prev_start) <= min(end_ms, prev_end) + 1000
                        if (
                            time_overlap
                            and clean_text
                            and (clean_text in prev_text or prev_text in clean_text)
                        ):
                            aligned = prev_spk
                            break
                    if aligned:
                        self._speaker_mapping[raw_spk] = aligned
                        spk_id = aligned
                    else:
                        spk_id = "spk_unknown"

        spk_name = None
        if self._speaker_resolver and spk_id != "spk_unknown":
            spk_name = self._speaker_resolver.resolve(
                channels=self._channels,
                channel=event.channel,
                speaker=raw_spk,
            )
        self.metrics.record_stage_timing("speaker", (time.perf_counter() - t_spk_0) * 1000.0)

        now_ms = max(provider_recv_ms, int(time.time() * 1000))

        if event.is_final:
            ch = event.channel if event.channel is not None else 0
            self._prev_interim_tokens.pop(ch, None)
            spk_key = (ch, spk_id)
            if start_ms < self._highest_published_start_ms[spk_key] - 2000:
                logger.debug(
                    "Dropping regression utterance on %s for %s (start_ms=%d < %d)",
                    self.meeting_id,
                    spk_key,
                    start_ms,
                    self._highest_published_start_ms[spk_key],
                )
                return

            uid = compute_utterance_id(self.meeting_id, event.channel, spk_id, start_ms)

            # Stage 3: Dedupe check
            t_dd_0 = time.perf_counter()
            if self._dedupe.is_duplicate(uid, event.channel, event.text, start_ms, end_ms):
                logger.debug("Dropping duplicate utterance on %s: %s", self.meeting_id, uid)
                return

            self._dedupe.record_published(uid, event.channel, event.text, start_ms, end_ms)
            self._published_utterances_for_alignment.append(
                (start_ms, end_ms, event.text.strip().lower(), spk_id)
            )
            self._highest_published_start_ms[spk_key] = max(
                self._highest_published_start_ms[spk_key], start_ms
            )
            self.metrics.record_stage_timing("dedupe", (time.perf_counter() - t_dd_0) * 1000.0)

            # End-to-end Latency computed directly from Provider Timeline (Requirement 2)
            # audio_end_wall = session_origin_wall + (provider start + duration in seconds)
            # where provider start + duration = event.end_ms / 1000.0
            t_pub_wall = time.time()
            if self._provider_session_origin_wall is None:
                self._provider_session_origin_wall = max(0.0, t_pub_wall - (event.end_ms / 1000.0))

            audio_end_wall = self._provider_session_origin_wall + (event.end_ms / 1000.0)
            e2e_ms = max(0.1, (t_pub_wall - audio_end_wall) * 1000.0)
            self.metrics.record_e2e_latency(e2e_ms)

            is_late = bool(
                self._is_catching_up or (audio_end_wall and (t_pub_wall - audio_end_wall) > 2.0)
            )

            # Early service overhead estimate for provider latency calculation
            svc_est_ms = 0.0
            if getattr(event, "recv_perf", 0.0) > 0:
                svc_est_ms = max(0.1, (time.perf_counter() - event.recv_perf) * 1000.0)

            # Provider latency: audio end to provider message receipt
            if e2e_ms is not None:
                prov_lat = max(0.1, e2e_ms - svc_est_ms)
                self.metrics.record_provider_latency(prov_lat)

            # Stage 4: Serialization
            t_ser_0 = time.perf_counter()
            chunk_match = self._find_chunk_for_end_ms(end_ms)
            chunk_seq = chunk_match[0] if chunk_match else None
            cap_srv_ms = chunk_match[4] if chunk_match and len(chunk_match) > 4 else None
            gw_recv_ms = chunk_match[5] if chunk_match and len(chunk_match) > 5 else None
            asr_recv_ms = chunk_match[6] if chunk_match and len(chunk_match) > 6 else None

            mapped_words: list[dict[str, Any]] | None = None
            raw_ev_words = getattr(event, "words", None) or []
            if raw_ev_words:
                timeline_shift_ms = start_ms - event.start_ms
                tmp_words: list[dict[str, Any]] = []
                for wd in raw_ev_words:
                    w_tok = str(wd.get("w", "")).strip()
                    if not w_tok:
                        continue
                    w_s_raw = int(wd.get("start_ms", event.start_ms))
                    w_e_raw = int(wd.get("end_ms", max(w_s_raw, event.end_ms)))
                    w_start_m = max(0, w_s_raw + timeline_shift_ms)
                    w_end_m = max(w_start_m, w_e_raw + timeline_shift_ms)
                    w_conf = round(float(wd.get("confidence", event.confidence)), 4)
                    raw_w_spk = wd.get("speaker")
                    w_spk: int | str | None
                    if raw_w_spk is None:
                        w_spk = spk_id
                    elif isinstance(raw_w_spk, (int, str)):
                        w_spk = raw_w_spk
                    else:
                        w_spk = str(raw_w_spk)
                    tmp_words.append(
                        {
                            "w": w_tok,
                            "start_ms": w_start_m,
                            "end_ms": w_end_m,
                            "confidence": w_conf,
                            "speaker": w_spk,
                        }
                    )
                if tmp_words:
                    mapped_words = tmp_words

            payload = {
                "utterance_id": uid,
                "speaker_id": spk_id,
                "speaker_name": spk_name,
                "speaker_epoch": self._speaker_epoch,
                "channel": event.channel,
                "text": event.text,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "start_epoch_ms": start_epoch_ms,
                "confidence": round(event.confidence, 3),
                "language": self.tenant_config.language,
                "provider": self.settings.asr_provider,
                "model": "nova-3",
                "chunk_seq": chunk_seq,
                "chunk_recv_wall": audio_end_wall,
                "final_recv_perf": getattr(event, "recv_perf", 0.0),
                "publish_wall": t_pub_wall,
                "e2e_ms": round(e2e_ms, 2) if e2e_ms is not None else None,
                "late": is_late,
                "words": mapped_words,
            }

            t0_cap = cap_srv_ms or start_epoch_ms
            if gw_recv_ms is not None and t0_cap is not None:
                gw_recv_ms = max(t0_cap, gw_recv_ms)
            if asr_recv_ms is not None and gw_recv_ms is not None:
                asr_recv_ms = max(gw_recv_ms, asr_recv_ms)
            prov_m = max(asr_recv_ms or t0_cap or 0, provider_recv_ms)
            pub_m = max(prov_m, int(time.time() * 1000))
            timing = {
                "t0_capture": t0_cap,
                "gw_recv": gw_recv_ms,
                "asr_recv": asr_recv_ms,
                "provider_msg": prov_m,
                "asr_pub": pub_m,
            }
            envelope = {
                "event_type": "transcript_final",
                "meeting_id": self.meeting_id,
                "tenant_id": self.tenant_id,
                "ts_ms": now_ms,
                "timing": timing,
                "payload": payload,
            }
            raw_env = json.dumps(envelope)
            self.metrics.record_stage_timing("serialize", (time.perf_counter() - t_ser_0) * 1000.0)

            # Stage 5: Redis XADD
            t_xadd_0 = time.perf_counter()
            pipe = self.redis.pipeline(transaction=False)
            pipe.xadd(
                self._events_key,
                {"payload": raw_env},
                maxlen=5000,
                approximate=True,
            )
            pipe.expire(self._events_key, 86400)
            await pipe.execute()
            t_pub_perf = time.perf_counter()
            xadd_ms = (t_pub_perf - t_xadd_0) * 1000.0
            self.metrics.record_stage_timing("xadd", xadd_ms)
            self.metrics.record_publish_latency(xadd_ms)

            # Service-added latency: Deepgram message receipt to internal bus publish
            if getattr(event, "recv_perf", 0.0) > 0:
                svc_ms = max(0.01, (t_pub_perf - event.recv_perf) * 1000.0)
                self.metrics.record_service_added_latency(svc_ms)

        else:
            # Interim result -> published to OUT stream only
            ch = event.channel if event.channel is not None else 0
            curr_tokens = [
                tok
                for tok in (t.strip(".,!?;:\"'()[]{}").lower() for t in event.text.split())
                if tok
            ]
            if event.is_stable:
                stable_prefix_words = len(curr_tokens)
            else:
                prev_tokens = self._prev_interim_tokens.get(ch, [])
                match_len = 0
                for a, b in zip(prev_tokens, curr_tokens, strict=False):
                    if a == b:
                        match_len += 1
                    else:
                        break
                stable_prefix_words = match_len
            self._prev_interim_tokens[ch] = curr_tokens

            raw_msg = getattr(event, "raw", {})
            alts = raw_msg.get("channel", {}).get("alternatives", [])
            if alts:
                words = alts[0].get("words", [])
                for w in words:
                    w_text = w.get("word", "").strip().lower()
                    if not w_text:
                        continue
                    w_end_s = float(w.get("end", 0.0))
                    w_end_prov_ms = int(w_end_s * 1000)
                    _, w_end_mtg_ms, _ = self._timeline.map_range(w_end_prov_ms, w_end_prov_ms)
                    w_key = (w_text, w_end_mtg_ms)
                    if w_key not in self._seen_interim_words:
                        if len(self._seen_interim_words_window) >= (
                            self._seen_interim_words_window.maxlen or 300
                        ):
                            old_w = self._seen_interim_words_window.popleft()
                            self._seen_interim_words.discard(old_w)
                        self._seen_interim_words_window.append(w_key)
                        self._seen_interim_words.add(w_key)

                        now_w = time.time()
                        if self._provider_session_origin_wall is None:
                            self._provider_session_origin_wall = max(0.0, now_w - w_end_s)
                        w_audio_end_wall = self._provider_session_origin_wall + w_end_s
                        i_lat = max(0.1, (now_w - w_audio_end_wall) * 1000.0)
                        self.metrics.record_interim_latency(i_lat)

            payload = {
                "speaker_id": spk_id,
                "speaker_name": spk_name,
                "channel": event.channel,
                "text": event.text,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "start_epoch_ms": start_epoch_ms,
                "confidence": round(event.confidence, 3),
                "language": self.tenant_config.language,
                "provider": self.settings.asr_provider,
                "model": "nova-3",
                "is_stable": event.is_stable,
                "stable_prefix_words": stable_prefix_words,
            }

            prov_m = max(start_epoch_ms, provider_recv_ms)
            pub_m = max(prov_m, int(time.time() * 1000))
            timing = {
                "t0_capture": start_epoch_ms,
                "asr_recv": prov_m,
                "provider_msg": prov_m,
                "asr_pub": pub_m,
            }

            envelope = {
                "event_type": "transcript_interim",
                "meeting_id": self.meeting_id,
                "tenant_id": self.tenant_id,
                "ts_ms": now_ms,
                "timing": timing,
                "payload": payload,
            }

            t0 = time.perf_counter()
            pipe = self.redis.pipeline(transaction=False)
            pipe.xadd(
                self._out_key,
                {
                    "event_type": "transcript_interim",
                    "payload": json.dumps(envelope),
                },
                maxlen=5000,
                approximate=True,
            )
            pipe.expire(self._out_key, 86400)
            await pipe.execute()
            self.metrics.record_publish_latency((time.perf_counter() - t0) * 1000)

    async def _emit_degraded_event(
        self,
        state: str,
        reason: str,
        since_ms: int | None = None,
        elapsed_s: float | None = None,
    ) -> None:
        """Publish transcript_service_degraded to BOTH internal bus and out stream."""
        now_ms = int(time.time() * 1000)
        if state == "degraded":
            self._degraded_active = True
            if not self._degraded_since_ms:
                self._degraded_since_ms = now_ms
        elif state == "recovered":
            self._degraded_active = False

        effective_since = since_ms if since_ms is not None else (self._degraded_since_ms or now_ms)
        if state == "recovered":
            self._degraded_since_ms = 0

        payload: dict[str, Any] = {
            "state": state,
            "reason": reason,
            "since_ms": effective_since,
        }
        if elapsed_s is not None:
            payload["elapsed_s"] = elapsed_s

        envelope = {
            "event_type": "transcript_service_degraded",
            "meeting_id": self.meeting_id,
            "tenant_id": self.tenant_id,
            "ts_ms": now_ms,
            "payload": payload,
        }
        json_env = json.dumps(envelope)

        pipe = self.redis.pipeline(transaction=False)
        # Out stream
        pipe.xadd(
            self._out_key,
            {
                "event_type": "transcript_service_degraded",
                "payload": json_env,
            },
            maxlen=5000,
            approximate=True,
        )
        pipe.expire(self._out_key, 86400)
        # Internal bus
        pipe.xadd(
            self._events_key,
            {"payload": json_env},
            maxlen=5000,
            approximate=True,
        )
        pipe.expire(self._events_key, 86400)
        with contextlib.suppress(Exception):
            await pipe.execute()

    async def _emit_audio_gap_event(self, gap_info: dict[str, Any]) -> None:
        """Publish ONE audio_gap event to BOTH internal bus and out stream."""
        now_ms = int(time.time() * 1000)
        envelope = {
            "event_type": "audio_gap",
            "meeting_id": self.meeting_id,
            "tenant_id": self.tenant_id,
            "ts_ms": now_ms,
            "payload": gap_info,
        }
        json_env = json.dumps(envelope)

        pipe = self.redis.pipeline(transaction=False)
        pipe.xadd(
            self._out_key,
            {
                "event_type": "audio_gap",
                "payload": json_env,
            },
            maxlen=5000,
            approximate=True,
        )
        pipe.expire(self._out_key, 86400)
        pipe.xadd(
            self._events_key,
            {"payload": json_env},
            maxlen=5000,
            approximate=True,
        )
        pipe.expire(self._events_key, 86400)
        with contextlib.suppress(Exception):
            await pipe.execute()

    async def stop(self, is_ended: bool = True) -> None:
        """Clean shutdown and drain audio."""
        if getattr(self, "_is_stopping", False):
            if hasattr(self, "_stop_future") and not self._stop_future.done():
                await self._stop_future
            return
        self._is_stopping = True
        try:
            loop = asyncio.get_running_loop()
            self._stop_future = loop.create_future()
        except RuntimeError:
            self._stop_future = None

        self._meeting_ended = is_ended
        self._is_running = False

        # 1. Drain ALL remaining audio from Redis (only if meeting ended)
        if is_ended:
            # Stop the background audio reader first so stop() is the sole XREADGROUP consumer
            # and no fetched batch is left stranded in the worker's PEL.
            if self._audio_read_task and not self._audio_read_task.done():
                with contextlib.suppress(Exception):
                    try:
                        await asyncio.wait_for(asyncio.shield(self._audio_read_task), timeout=2.0)
                    except TimeoutError:
                        self._audio_read_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await self._audio_read_task

            try:
                with contextlib.suppress(Exception):
                    # First drain any worker PEL entries fetched before exit but unprocessed
                    pel_cursor = "0"
                    while True:
                        pending = await self.redis.xreadgroup(
                            "asr",
                            self.worker_id,
                            {self._audio_key: pel_cursor},
                            count=100,
                        )
                        if not pending:
                            break
                        found_pel = False
                        for _, messages in pending:
                            if messages:
                                found_pel = True
                                for entry_id, fields in messages:
                                    eid_str = (
                                        entry_id.decode()
                                        if isinstance(entry_id, bytes)
                                        else str(entry_id)
                                    )
                                    pel_cursor = eid_str
                                    if eid_str not in self._processed_entry_ids:
                                        await self._process_raw_entry(entry_id, fields)
                        if not found_pel:
                            break

                    # Then drain all remaining unread entries from '>'
                    while True:
                        entries = await self.redis.xreadgroup(
                            "asr",
                            self.worker_id,
                            {self._audio_key: ">"},
                            count=100,
                            block=50,
                        )
                        if not entries:
                            break
                        found_any = False
                        for _, messages in entries:
                            if messages:
                                found_any = True
                                for entry_id, fields in messages:
                                    await self._process_raw_entry(entry_id, fields)
                        if not found_any:
                            break
            except Exception:
                pass

        # Drain all remaining buffered reconnect audio chunks to provider
        try:
            while self._reconnect_buffer and self.provider.is_open:
                eid, chunk = self._reconnect_buffer.popleft()
                await self.provider.send_audio(chunk)
                t_send = time.perf_counter()
                self._pending_acks.append((t_send, eid))
                self._sent_replay_history.append((t_send, chunk))
                self._timeline.register_chunk(chunk.capture_ts_ms)
                self._chunks_sent += 1
                self._bytes_sent += len(chunk.data)
                self._all_dispatched_client_seqs.append(chunk.client_seq)
                await asyncio.sleep(0.002)
        except Exception:
            pass

        # 2. Finish provider and flush finals (up to 8s timeout)
        try:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.provider.finish(), timeout=8.0)
        except Exception:
            pass

        # Close warm standby provider if present
        if self._standby_provider:
            with contextlib.suppress(Exception):
                await self._standby_provider.close()
            self._standby_provider = None

        # 3. Wait briefly for events loop to complete processing flushed events
        if self._events_task and not self._events_task.done():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(self._events_task), timeout=3.0)

        # 4. Cancel tasks
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # 4. Acknowledge remaining entries (whether recycling or ending, flushed audio must be acked)
        if self._pending_acks:
            to_ack = [eid for _, eid in self._pending_acks]
            with contextlib.suppress(Exception):
                await self.redis.xack(self._audio_key, "asr", *to_ack)
                await self.redis.xdel(self._audio_key, *to_ack)
                self.metrics.chunks_acked += len(to_ack)
            self._pending_acks.clear()

        cum_key = f"mm:{self.tenant_id}:asr_cumulative:{self.meeting_id}"

        if not is_ended:
            # Worker is recycling at max meeting duration or rebalancing:
            # Store cumulative progress so replacement worker continues without chunk loss or duplicate summary
            total_sent = self._cumulative_chunks_sent + self._chunks_sent
            total_bytes = self._cumulative_bytes_sent + self._bytes_sent
            total_reconn = self._cumulative_reconnect_count + self._reconnect_count
            total_dropped = self._cumulative_dropped_chunks_count + self._dropped_chunks_count
            total_deg = self._cumulative_degraded_s + self._total_degraded_s
            all_gaps = self._cumulative_gaps + self._detected_gaps
            all_acc = sorted(list(self._cumulative_accepted_seqs | set(getattr(self.provider, "accepted_client_seqs", set()))))
            all_sent = list(dict.fromkeys(self._cumulative_sent_seqs + list(self._all_dispatched_client_seqs)))

            cum_map = {
                "chunks_sent": str(total_sent),
                "bytes_sent": str(total_bytes),
                "reconnects": str(total_reconn),
                "dropped_chunks": str(total_dropped),
                "degraded_seconds": str(total_deg),
                "gaps": json.dumps(all_gaps),
                "accepted_seqs": json.dumps(all_acc),
                "sent_seqs": json.dumps(all_sent),
            }
            with contextlib.suppress(Exception):
                await self.redis.hset(cum_key, mapping=cum_map)
                await self.redis.expire(cum_key, 3600)

            with contextlib.suppress(Exception):
                await self.redis.set(
                    f"mm:{self.tenant_id}:asr_accepted_seqs:{self.meeting_id}",
                    json.dumps(all_acc),
                )
                await self.redis.set(
                    f"mm:{self.tenant_id}:asr_sent_seqs:{self.meeting_id}",
                    json.dumps(all_sent),
                )
        else:
            # Meeting has actually ended (meeting_ended / meeting_abandoned):
            # Compute cumulative totals across all workers that served this meeting
            total_sent = self._cumulative_chunks_sent + self._chunks_sent
            total_bytes = self._cumulative_bytes_sent + self._bytes_sent
            total_reconn = self._cumulative_reconnect_count + self._reconnect_count
            total_dropped = self._cumulative_dropped_chunks_count + self._dropped_chunks_count
            total_deg = self._cumulative_degraded_s + self._total_degraded_s
            all_gaps = self._cumulative_gaps + self._detected_gaps
            all_acc = sorted(list(self._cumulative_accepted_seqs | set(getattr(self.provider, "accepted_client_seqs", set()))))
            all_sent = list(dict.fromkeys(self._cumulative_sent_seqs + list(self._all_dispatched_client_seqs)))

            with contextlib.suppress(Exception):
                await self.redis.set(
                    f"mm:{self.tenant_id}:asr_accepted_seqs:{self.meeting_id}",
                    json.dumps(all_acc),
                )
                await self.redis.set(
                    f"mm:{self.tenant_id}:asr_sent_seqs:{self.meeting_id}",
                    json.dumps(all_sent),
                )

            # Record session summary audit (synchronously with 15s timeout & 3 retries)
            if not getattr(self, "_summary_recorded", False):
                self._summary_recorded = True
                await self.auditor.record_session_summary_sync(
                    tenant_id=self.tenant_id,
                    user_id="system_asr",
                    meeting_id=self.meeting_id,
                    provider=self.settings.asr_provider,
                    chunks_sent=total_sent,
                    bytes_sent=total_bytes,
                    reconnects=total_reconn,
                    dropped_chunks=total_dropped,
                    degraded_seconds=total_deg,
                    gaps=all_gaps,
                    timeout=15.0,
                    retries=3,
                )
                with contextlib.suppress(Exception):
                    await self.redis.delete(cum_key)
        if getattr(self, "_stop_future", None) and not self._stop_future.done():
            self._stop_future.set_result(None)
