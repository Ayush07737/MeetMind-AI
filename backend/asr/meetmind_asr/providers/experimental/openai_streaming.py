"""MeetMind AI — OpenAI Streaming Transcription Experimental Provider (B3).

Official Documentation References:
- Page Title: OpenAI Speech-to-Text / Realtime API Reference & Pricing
- Official URL: https://platform.openai.com/docs/guides/realtime
- Pricing URL: https://openai.com/api/pricing
- Access Date: 2026-11-10
- WebSocket Endpoint: wss://api.openai.com/v1/realtime
- Audio Formats: PCM16 (24kHz recommended for Realtime; 16kHz resampled)
- Regional Processing: US / EU global endpoints (India Data Residency: Not documented / UNVERIFIED)
- Zero-Retention: Enterprise BAA / Zero Data Retention terms
- Pricing: gpt-live-transcribe $0.017/min ($1.02/hr); Realtime audio tokens
  $32/1M input (~$0.06/min uncached equiv = ~$3.60/hr; cached $0.40/1M)
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
