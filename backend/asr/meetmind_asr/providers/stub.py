"""MeetMind AI — Stub ASR Provider (WP-B).

Deterministic test provider with:
- Scripted transcripts keyed to audio chunk timeline.
- Configurable latency.
- Configurable fault injection (disconnect at chunk N, outage duration, error on Nth chunk).
- Full tracking of received client_seq values for audit and lossless proof.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from .base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)


@dataclass
class StubFaultConfig:
    """Configures fault injection scenarios for the stub provider."""

    disconnect_at_chunk: int | None = None
    outage_duration_s: float = 0.0
    error_on_chunk: int | None = None
    error_on_open: bool = False
    latency_ms: float = 5.0
    final_delay_ms: float = 0.0


class StubProvider(ASRProvider):
    """Deterministic scriptable provider used for unit & acceptance tests."""

    recorded_sessions: dict[str, dict[str, Any]] = {}

    def __init__(
        self,
        fault_config: StubFaultConfig | None = None,
        scripted_phrases: list[str] | None = None,
        redis_client: Any | None = None,
        **kwargs: Any,
    ) -> None:
        self._fault_config = fault_config or StubFaultConfig()
        self._redis_client = redis_client
        self._replay_jitter_ms = int(
            kwargs.get("replay_jitter_ms", 0) or os.environ.get("ASR_STUB_REPLAY_JITTER_MS", "0")
        )
        self._load_env_faults()

        self._scripted_phrases = scripted_phrases or [
            "Welcome everyone to the quarterly review.",
            "We have achieved all key milestones.",
            "The engineering team delivered ahead of schedule.",
            "Let us proceed with the financial roadmap.",
        ]

        self.session: ProviderSession | None = None
        self.received_client_seqs: set[int] = set()
        self.received_chunks: list[AudioChunk] = []
        self._chunk_count = 0
        self._session_start_time: float = 0.0
        self._outage_until: float = 0.0
        self._is_open = False
        self._is_finished = False

        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._current_text_idx = 0
        self._chunk_buffer: list[AudioChunk] = []

    def _load_env_faults(self) -> None:
        """Allow configuring faults via env or JSON string."""
        raw_json = os.environ.get("ASR_STUB_FAULT_CONFIG")
        if raw_json:
            try:
                data = json.loads(raw_json)
                self._fault_config.disconnect_at_chunk = data.get("disconnect_at_chunk")
                self._fault_config.outage_duration_s = float(data.get("outage_duration_s", 0.0))
                self._fault_config.error_on_chunk = data.get("error_on_chunk")
                self._fault_config.error_on_open = bool(data.get("error_on_open", False))
                self._fault_config.latency_ms = float(data.get("latency_ms", 5.0))
                self._fault_config.final_delay_ms = float(data.get("final_delay_ms", 0.0))
            except Exception:
                pass

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=True,
            multichannel=True,
            keyterms=True,
            max_keyterms=100,
        )

    async def open(self, session: ProviderSession) -> None:
        now = time.time()
        if now < self._outage_until:
            raise ConnectionError(
                f"Stub provider in simulated outage ({self._outage_until - now:.2f}s remaining)"
            )

        if self._fault_config.error_on_open:
            raise ConnectionError("Stub provider simulated open failure")

        self.session = session
        self._is_open = True
        self._is_finished = False
        self._session_start_time = now

        StubProvider.recorded_sessions[session.meeting_id] = {
            "tenant_id": session.tenant_id,
            "keyterms": list(session.keyterms),
            "language": session.language,
        }

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed stub provider")

        self.received_client_seqs.add(chunk.client_seq)
        self.received_chunks.append(chunk)
        self._chunk_count += 1
        self._chunk_buffer.append(chunk)

        if self.session and self._redis_client:
            with contextlib.suppress(Exception):
                s_key = f"mm:{self.session.tenant_id}:stub_seqs:{self.session.meeting_id}"
                l_key = f"mm:{self.session.tenant_id}:stub_seqs_list:{self.session.meeting_id}"
                await self._redis_client.sadd(s_key, str(chunk.client_seq))
                await self._redis_client.rpush(l_key, str(chunk.client_seq))

        # Check error fault injection
        if (
            self._fault_config.error_on_chunk is not None
            and self._chunk_count == self._fault_config.error_on_chunk
        ):
            await self._event_queue.put(
                ProviderEvent(
                    event_type="error",
                    error="Simulated provider error on chunk",
                )
            )
            return

        # Check disconnect fault injection
        if (
            self._fault_config.disconnect_at_chunk is not None
            and self._chunk_count == self._fault_config.disconnect_at_chunk
        ):
            self._is_open = False
            self._outage_until = time.time() + self._fault_config.outage_duration_s
            await self._event_queue.put(
                ProviderEvent(
                    event_type="closed",
                    error="Simulated provider connection dropped",
                )
            )
            return

        if self._fault_config.latency_ms > 0:
            await asyncio.sleep(self._fault_config.latency_ms / 1000.0)

        phrase = self._scripted_phrases[self._current_text_idx % len(self._scripted_phrases)]
        words = phrase.split()

        # Emit interim event on every chunk
        interim_word_count = max(1, min(len(words), len(self._chunk_buffer)))
        interim_text = " ".join(words[:interim_word_count])

        is_two_channel = bool(self.session and self.session.channels == 2)
        interim_ch = (self._chunk_count % 2) if is_two_channel else 0

        # Provider offsets in ms relative to this session
        offset_ms = (self._chunk_count - 1) * 100
        await self._event_queue.put(
            ProviderEvent(
                event_type="interim",
                text=interim_text,
                start_ms=offset_ms,
                end_ms=offset_ms + 100,
                confidence=0.92,
                speaker=interim_ch,
                channel=interim_ch,
                is_final=False,
                is_stable=False,
            )
        )

        # Emit final event every 5 chunks (~500ms audio)
        if len(self._chunk_buffer) >= 5:
            start_offset = (self._chunk_count - len(self._chunk_buffer)) * 100
            end_offset = self._chunk_count * 100
            final_text = phrase
            final_ch = (self._current_text_idx % 2) if is_two_channel else 0

            if self._replay_jitter_ms > 0:
                jitter = (
                    (self._chunk_count * 31) % (2 * self._replay_jitter_ms + 1)
                ) - self._replay_jitter_ms
                start_offset = max(0, start_offset + jitter)
                end_offset = max(start_offset + 50, end_offset + jitter)
                if (self._chunk_count // 5) % 2 == 1:
                    w = final_text.split()
                    if len(w) > 2:
                        final_text = " ".join(w[1:] + [w[0]])

            self._current_text_idx += 1
            self._chunk_buffer.clear()

            if self._fault_config.final_delay_ms > 0:
                await asyncio.sleep(self._fault_config.final_delay_ms / 1000.0)

            await self._event_queue.put(
                ProviderEvent(
                    event_type="final",
                    text=final_text,
                    start_ms=start_offset,
                    end_ms=end_offset,
                    confidence=0.98,
                    speaker=final_ch,
                    channel=final_ch,
                    is_final=True,
                    is_stable=True,
                )
            )

    async def events(self) -> AsyncIterator[ProviderEvent]:
        while True:
            event = await self._event_queue.get()
            yield event
            if event.event_type == "closed" or (self._is_finished and self._event_queue.empty()):
                break

    async def finish(self) -> None:
        if self._chunk_buffer:
            start_offset = (self._chunk_count - len(self._chunk_buffer)) * 100
            end_offset = self._chunk_count * 100
            phrase = self._scripted_phrases[self._current_text_idx % len(self._scripted_phrases)]
            self._chunk_buffer.clear()
            await self._event_queue.put(
                ProviderEvent(
                    event_type="final",
                    text=phrase,
                    start_ms=start_offset,
                    end_ms=end_offset,
                    confidence=0.98,
                    speaker=0,
                    channel=0,
                    is_final=True,
                    is_stable=True,
                )
            )
        self._is_finished = True
        await self._event_queue.put(ProviderEvent(event_type="closed"))
        self._is_open = False

    async def close(self) -> None:
        self._is_open = False
        await self._event_queue.put(ProviderEvent(event_type="closed"))
