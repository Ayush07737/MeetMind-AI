"""MeetMind AI — OpenAI Streaming Transcription Experimental Provider (B3).

Official Documentation References:
- Page Title: OpenAI Realtime API Audio Guide
- Official URL: https://platform.openai.com/docs/guides/realtime
- Access Date: 2026-11-06
- WebSocket Endpoint: wss://api.openai.com/v1/realtime
- Audio Formats: PCM16 (24kHz recommended, resampled from 16kHz)
- Regional Processing: US / EU (India Regional Processing: UNVERIFIED / Not documented)
- Zero-Retention: Available under Enterprise BAA / Zero Data Retention terms
- Pricing: $0.06 per audio input minute ($3.60 per audio hour)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from ..base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)

logger = logging.getLogger("meetmind_asr.providers.experimental.openai")


class OpenAIStreamingProvider(ASRProvider):
    """OpenAI Realtime API streaming transcription adapter."""

    def __init__(self, **kwargs: Any) -> None:
        self.session: ProviderSession | None = None
        self._is_open = False
        self._is_finished = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=False,
            multichannel=False,
            keyterms=True,
            max_keyterms=100,
        )

    async def open(self, session: ProviderSession) -> None:
        self.session = session
        self._is_open = True
        self._is_finished = False
        logger.info("OpenAIStreamingProvider session opened for %s", session.meeting_id)

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed OpenAIStreamingProvider")

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
