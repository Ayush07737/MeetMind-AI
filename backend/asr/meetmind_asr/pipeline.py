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

        self._audio_key = audio_stream_key(tenant_id, meeting_id)
        self._events_key = events_stream_key(tenant_id, meeting_id)
        self._out_key = out_stream_key(tenant_id, meeting_id)
        self._session_key = meeting_session_key(tenant_id, meeting_id)

        self._timeline = TimelineMapper()
        self._dedupe = Deduplicator()
        self._speaker_resolver: SpeakerNameResolver | None = None
        self._highest_published_start_ms: dict[int, int] = collections.defaultdict(lambda: -1)

        # State & lifecycle
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

        # Audit stats
        self._chunks_sent = 0
        self._bytes_sent = 0
        self._reconnect_count = 0
        self._dropped_chunks_count = 0

        # Background tasks
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """Initialize pipeline, resolve metadata, and spawn worker tasks."""
        await self._init_metadata()
        await self._ensure_consumer_group()

        # Open initial provider session
        await self._connect_provider()

        # Record audit start
        self.auditor.record_session_started(
            tenant_id=self.tenant_id,
            user_id="system_asr",
            meeting_id=self.meeting_id,
            provider=self.settings.asr_provider,
            endpoint_host=self.settings.deepgram_base_url,
            language=self.tenant_config.language,
            mip_opt_out=True,
        )

        # Launch consumer loops
        self._tasks.append(asyncio.create_task(self._audio_read_loop()))
        self._tasks.append(asyncio.create_task(self._ack_flush_loop()))
        self._tasks.append(asyncio.create_task(self._provider_events_loop()))

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
                                s_ms = p.get("start_ms", 0)
                                self._highest_published_start_ms[ch] = max(
                                    self._highest_published_start_ms[ch], s_ms
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
        session = ProviderSession(
            tenant_id=self.tenant_id,
            meeting_id=self.meeting_id,
            sample_rate=self._sample_rate,
            channels=self._channels,
            language=self.tenant_config.language,
            keyterms=self.tenant_config.keyterms,
        )
        await self.provider.open(session)

    async def _audio_read_loop(self) -> None:
        """Read audio frames from Redis stream mm:{tenant}:audio:{meeting}."""
        while self._is_running:
            try:
                # 1. Periodic autoclaim for entries idle > 5s
                with contextlib.suppress(Exception):
                    claimed = await self.redis.xautoclaim(
                        self._audio_key,
                        "asr",
                        self.worker_id,
                        min_idle_time=self.settings.asr_audio_idle_ms,
                        start_id="0-0",
                        count=20,
                    )
                    if claimed and len(claimed) >= 2 and claimed[1]:
                        for entry_id, fields in claimed[1]:
                            await self._process_raw_entry(entry_id, fields)

                # 2. XREADGROUP for new entries (BLOCK 250ms)
                entries = await self.redis.xreadgroup(
                    "asr",
                    self.worker_id,
                    {self._audio_key: ">"},
                    count=20,
                    block=250,
                )
                if entries:
                    for _, messages in entries:
                        for entry_id, fields in messages:
                            await self._process_raw_entry(entry_id, fields)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Audio read loop error on %s: %s", self.meeting_id, exc)
                    await asyncio.sleep(0.1)

    async def _process_raw_entry(self, entry_id: Any, fields: dict[Any, Any]) -> None:
        eid_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
        raw_data = fields.get(b"audio_data") or fields.get("audio_data")
        if not raw_data or not isinstance(raw_data, bytes):
            # Acknowledge corrupted entry immediately
            await self.redis.xack(self._audio_key, "asr", eid_str)
            return

        self.metrics.chunks_in += 1

        try:
            hdr, payload = decode_audio_frame(raw_data)
        except Exception as exc:
            logger.warning("Invalid audio header in %s: %s", self.meeting_id, exc)
            await self.redis.xack(self._audio_key, "asr", eid_str)
            return

        # Check Codec (Decision 1: v1 supports codec=1 PCM16 LE only)
        if hdr.codec != 1:
            # Codec 2 (Opus) is acked and dropped with single degraded event per meeting
            await self.redis.xack(self._audio_key, "asr", eid_str)
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
        if self.provider.session and self.provider.session.channels != self._channels:
            self.provider.session.channels = self._channels

        # Gap detection
        if self._last_seen_seq is not None and hdr.client_seq > self._last_seen_seq + 1:
            _gap = hdr.client_seq - (self._last_seen_seq + 1)
            now = time.time()
            if now - self._last_gap_degraded_time >= 5.0:
                self._last_gap_degraded_time = now
                await self._emit_degraded_event(
                    state="degraded",
                    reason="audio_gap",
                )

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

        # Create AudioChunk (payload is raw PCM audio bytes)
        chunk = AudioChunk(
            data=payload,
            client_seq=hdr.client_seq,
            capture_ts_ms=hdr.capture_ts_ms,
            recv_ns=time.perf_counter_ns(),
        )

        # Enqueue chunk
        await self._dispatch_chunk(eid_str, chunk)

    async def _dispatch_chunk(self, entry_id: str, chunk: AudioChunk) -> None:
        """Deliver chunk to provider or buffer during provider outage."""
        # Check buffer capacity
        max_buffered_chunks = self.settings.asr_buffer_max_seconds * 10  # 10 chunks/sec
        if len(self._reconnect_buffer) >= max_buffered_chunks:
            # Buffer overflow: drop oldest chunk
            dropped_eid, _ = self._reconnect_buffer.popleft()
            self._dropped_chunks_count += 1
            self.metrics.dropped_chunks += 1
            # Acknowledge dropped chunk so it doesn't loop
            await self.redis.xack(self._audio_key, "asr", dropped_eid)
            await self._emit_degraded_event(
                state="degraded",
                reason="buffer_overflow",
            )

        if not self._reconnect_buffer:
            try:
                # Direct send
                await self.provider.send_audio(chunk)
                t_send = time.perf_counter()
                self._pending_acks.append((t_send, entry_id))
                self._sent_replay_history.append((t_send, chunk))
                self._timeline.register_chunk(chunk.capture_ts_ms)
                self._chunks_sent += 1
                self._bytes_sent += len(chunk.data)

                # Prune replay history older than replay window
                while self._sent_replay_history and (t_send - self._sent_replay_history[0][0]) > (
                    self.settings.asr_replay_window_ms / 1000.0
                ):
                    self._sent_replay_history.popleft()
                return
            except Exception as exc:
                logger.warning("Provider send failed on %s: %s", self.meeting_id, exc)
                self._reconnect_buffer.append((entry_id, chunk))
                asyncio.create_task(self._handle_reconnect())
        else:
            # Connection currently recovering; append to in-memory buffer
            self._reconnect_buffer.append((entry_id, chunk))

    async def _handle_reconnect(self) -> None:
        """Reconnect to provider with backoff and flush buffered audio."""
        if self._degraded_active:
            return
        self._degraded_active = True
        self._reconnect_count += 1
        self.metrics.reconnects += 1
        t_disconnect = time.time()
        self._degraded_since_ms = int(t_disconnect * 1000)

        backoffs = [0.25, 0.5, 1.0, 2.0]
        backoff_idx = 0
        degraded_event_sent = False

        while self._is_running:
            now = time.time()
            if not degraded_event_sent and (now - t_disconnect) >= 2.0:
                degraded_event_sent = True
                await self._emit_degraded_event(
                    state="degraded",
                    reason="provider_unreachable",
                    since_ms=self._degraded_since_ms,
                )

            delay = backoffs[min(backoff_idx, len(backoffs) - 1)] + random.uniform(0.01, 0.05)
            await asyncio.sleep(delay)
            backoff_idx += 1

            try:
                with contextlib.suppress(Exception):
                    await self.provider.close()

                await self._connect_provider()

                # Reconnect successful: reset timeline session and resend replay window
                self._timeline.reset_session()

                # Resend replay window audio
                for _, chunk in self._sent_replay_history:
                    await self.provider.send_audio(chunk)
                    self._timeline.register_chunk(chunk.capture_ts_ms)
                    self.metrics.replayed_chunks += 1

                # Send buffered audio
                while self._reconnect_buffer:
                    eid, chunk = self._reconnect_buffer.popleft()
                    await self.provider.send_audio(chunk)
                    t_send = time.perf_counter()
                    self._pending_acks.append((t_send, eid))
                    self._sent_replay_history.append((t_send, chunk))
                    self._timeline.register_chunk(chunk.capture_ts_ms)
                    self._chunks_sent += 1
                    self._bytes_sent += len(chunk.data)

                # Recovery complete
                recovery_time = time.time()
                dur_s = recovery_time - t_disconnect
                self._total_degraded_s += dur_s
                self.metrics.degraded_seconds += dur_s
                self._degraded_active = False

                if degraded_event_sent:
                    await self._emit_degraded_event(
                        state="recovered",
                        reason="provider_unreachable",
                        since_ms=self._degraded_since_ms,
                    )
                break
            except Exception as e:
                logger.debug("Provider reconnect attempt failed: %s", e)

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
        while self._is_running and not self._meeting_ended:
            try:
                async for event in self.provider.events():
                    if not self._is_running:
                        break

                    if event.event_type in ("final", "interim"):
                        await self._handle_transcript_event(event)
                    elif event.event_type == "error":
                        await self._emit_degraded_event(
                            state="degraded",
                            reason="provider_error",
                        )
                    elif event.event_type == "closed":
                        if self._is_running and not self._meeting_ended:
                            asyncio.create_task(self._handle_reconnect())
                        break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._is_running:
                    logger.warning("Provider event loop error on %s: %s", self.meeting_id, exc)

            if self._is_running and not self._meeting_ended:
                await asyncio.sleep(0.1)

    async def _handle_transcript_event(self, event: ProviderEvent) -> None:
        if not event.text or not event.text.strip():
            return

        # Map timeline
        start_ms, end_ms, start_epoch_ms = self._timeline.map_range(event.start_ms, event.end_ms)

        # Speaker name resolution
        spk_name = None
        if self._speaker_resolver:
            spk_name = self._speaker_resolver.resolve(
                channels=self._channels,
                channel=event.channel,
                speaker=event.speaker,
            )

        spk_id = f"spk_{event.speaker}"
        now_ms = int(time.time() * 1000)

        if event.is_final:
            ch = event.channel if event.channel is not None else 0
            if start_ms < self._highest_published_start_ms[ch]:
                return

            uid = compute_utterance_id(self.meeting_id, event.channel, spk_id, start_ms)

            # Dedupe check
            if self._dedupe.is_duplicate(uid, event.channel, event.text, start_ms, end_ms):
                return

            self._dedupe.record_published(uid, event.channel, event.text, start_ms, end_ms)
            self._highest_published_start_ms[ch] = max(
                self._highest_published_start_ms[ch], start_ms
            )

            payload = {
                "utterance_id": uid,
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
            }

            envelope = {
                "event_type": "transcript_final",
                "meeting_id": self.meeting_id,
                "tenant_id": self.tenant_id,
                "ts_ms": now_ms,
                "payload": payload,
            }

            # Published to INTERNAL bus only
            t0 = time.perf_counter()
            pipe = self.redis.pipeline(transaction=False)
            pipe.xadd(
                self._events_key,
                {"payload": json.dumps(envelope)},
                maxlen=5000,
                approximate=True,
            )
            pipe.expire(self._events_key, 86400)
            await pipe.execute()
            self.metrics.record_publish_latency((time.perf_counter() - t0) * 1000)

        else:
            # Interim result -> published to OUT stream only
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
            }

            envelope = {
                "event_type": "transcript_interim",
                "meeting_id": self.meeting_id,
                "tenant_id": self.tenant_id,
                "ts_ms": now_ms,
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
    ) -> None:
        """Publish transcript_service_degraded to BOTH internal bus and out stream."""
        now_ms = int(time.time() * 1000)
        envelope = {
            "event_type": "transcript_service_degraded",
            "meeting_id": self.meeting_id,
            "tenant_id": self.tenant_id,
            "ts_ms": now_ms,
            "payload": {
                "state": state,
                "reason": reason,
                "since_ms": since_ms if since_ms is not None else now_ms,
            },
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

    async def stop(self, is_ended: bool = True) -> None:
        """Clean shutdown and drain audio."""
        self._meeting_ended = is_ended
        self._is_running = False

        # 1. Drain remaining audio from Redis
        try:
            with contextlib.suppress(Exception):
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
                            await self._process_raw_entry(entry_id, fields)
        except Exception:
            pass

        # 2. Finish provider and flush finals (up to 5s timeout)
        try:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.provider.finish(), timeout=5.0)
        except Exception:
            pass

        # 3. Cancel tasks
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # 4. Acknowledge remaining entries
        if is_ended and self._pending_acks:
            to_ack = [eid for _, eid in self._pending_acks]
            with contextlib.suppress(Exception):
                await self.redis.xack(self._audio_key, "asr", *to_ack)
                self.metrics.chunks_acked += len(to_ack)

        # 5. Record session summary audit
        self.auditor.record_session_summary(
            tenant_id=self.tenant_id,
            user_id="system_asr",
            meeting_id=self.meeting_id,
            provider=self.settings.asr_provider,
            chunks_sent=self._chunks_sent,
            bytes_sent=self._bytes_sent,
            reconnects=self._reconnect_count,
            dropped_chunks=self._dropped_chunks_count,
            degraded_seconds=self._total_degraded_s,
        )
