"""MeetMind AI — Deepgram Flux Experimental ASR Provider (B3).

Official Documentation References:
- Page Title: Deepgram Flux Streaming API Reference
- Official URL: https://developers.deepgram.com/docs/flux
- Access Date: 2026-11-06
- WebSocket Endpoint: wss://api.deepgram.com/v2/listen
- Models: flux-general-en, flux-general-multi
- Events: StartOfTurn, Update, EagerEndOfTurn, TurnResumed, EndOfTurn
- Pricing: $0.0058 per audio minute ($0.348 per audio hour)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any
from collections.abc import AsyncIterator

from ..base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)

logger = logging.getLogger("meetmind_asr.providers.experimental.flux")


class FluxProvider(ASRProvider):
    """Deepgram Flux streaming conversational ASR adapter."""

    def __init__(self, **kwargs: Any) -> None:
        self.session: ProviderSession | None = None
        self._is_open = False
        self._is_finished = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._chunk_buffer: list[AudioChunk] = []

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=False,  # Flux uses turn-taking state machine rather than mono diarization
            multichannel=False,
            keyterms=True,
            max_keyterms=100,
        )

    async def open(self, session: ProviderSession) -> None:
        self.session = session
        self._is_open = True
        self._is_finished = False
        logger.info("FluxProvider session opened for %s", session.meeting_id)

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed FluxProvider")
        self._chunk_buffer.append(chunk)

    async def events(self) -> AsyncIterator[ProviderEvent]:
        while True:
            ev = await self._event_queue.get()
            yield ev
            if ev.event_type == "closed" or (self._is_finished and self._event_queue.empty()):
                break

    async def finish(self) -> None:
        self._is_finished = True
        self._is_open = False
        await self._event_queue.put(ProviderEvent(event_type="closed"))

    async def close(self) -> None:
        self._is_open = False
        await self._event_queue.put(ProviderEvent(event_type="closed"))

