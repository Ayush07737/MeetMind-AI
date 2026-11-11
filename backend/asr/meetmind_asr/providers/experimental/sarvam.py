"""MeetMind AI — Sarvam Saaras v3 Experimental ASR Provider (B3).

Official Documentation References:
- Page Title: Sarvam AI Speech-to-Text Streaming API & Pricing Documentation
- Official URL: https://docs.sarvam.ai/api-reference-docs/speech-to-text/stt/stream
- Access Date: 2026-11-10
- WebSocket Endpoint: wss://api.sarvam.ai/speech-to-text/ws?model=saaras:v3
  (modes: transcribe|translate|verbatim|translit|codemix)
- Languages: 22 Indian languages + English
- Streaming Diarization & Keyterm Prompting on /speech-to-text/ws (saaras:v3):
  UNVERIFIED (live keyterm prompting documented on saaras:v4 batch/REST)
- Regional Processing: India native infrastructure (Mumbai / ap-south-1)
- Pricing: Standard STT streaming ~INR 30/hr (~INR 0.50/min, ~$0.35/hr; exact
  real-time slab UNVERIFIED without funded dashboard)
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
