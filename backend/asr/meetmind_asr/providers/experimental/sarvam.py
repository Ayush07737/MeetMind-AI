"""MeetMind AI — Sarvam Saaras v3 Experimental ASR Provider (B3).

Official Documentation References:
- Page Title: Sarvam AI Streaming Audio API Reference
- Official URL: https://docs.sarvam.ai/api-reference/audio-streaming
- Access Date: 2026-11-06
- WebSocket Endpoint: wss://api.sarvam.ai/v1/audio/transcribe/stream
- Languages: Hindi, English, Bengali, Gujarati, Kannada, Malayalam, Marathi,
  Odia, Punjabi, Tamil, Telugu
- Regional Processing: India (ap-south-1 / Mumbai local infrastructure)
- Pricing Source A: Official website (₹0.25/min = ~$0.0030/min = ~$0.18/audio hour)
- Pricing Source B: Marketplace/Cloud listing ($0.0050/min = ~$0.30/audio hour)
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

logger = logging.getLogger("meetmind_asr.providers.experimental.sarvam")


class SarvamProvider(ASRProvider):
    """Sarvam Saaras v3 streaming ASR adapter."""

    def __init__(self, **kwargs: Any) -> None:
        self.session: ProviderSession | None = None
        self._is_open = False
        self._is_finished = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=False,  # UNVERIFIED: streaming diarization omitted in v3 documentation
            multichannel=False,
            keyterms=True,
            max_keyterms=50,
        )

    async def open(self, session: ProviderSession) -> None:
        self.session = session
        self._is_open = True
        self._is_finished = False
        logger.info(
            "SarvamProvider session opened for %s (language=%s)",
            session.meeting_id,
            session.language,
        )

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed SarvamProvider")

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
