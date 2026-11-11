"""MeetMind AI — Gladia Solaria-1 Experimental ASR Provider (A1 / B3).

Official Documentation References:
- Page Title: Gladia V2 Live Streaming API Reference
- Official URL: https://docs.gladia.io/api-reference/v2/live/init
- Access Date: 2026-11-10
- Connection Flow:
  1. POST https://api.gladia.io/v2/live[?region=eu-west|us-west] (header: x-gladia-key)
  2. Connect WebSocket to returned wss://api.gladia.io/v2/live?token=...
- Empirically Verified Live Checks (2026-11-10):
  - Region query param accepts ONLY 'eu-west' and 'us-west' (no India region;
    ap-south-1 returns HTTP 400 validation error).
  - Live diarization is NOT supported on /v2/live (passing diarization returns HTTP 400).
  - Custom vocabulary and Hinglish code-switching supported via realtime_processing
    and language_config.
- Pricing: EUR 0.68 per audio hour (~$0.74/hr USD equiv) for live Solaria-1 streaming
  (gladia.io/pricing, accessed 2026-11-10); 2-ch live billing UNVERIFIED on self-serve page.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx
import websockets

from ..base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)

logger = logging.getLogger("meetmind_asr.providers.experimental.gladia")


class GladiaProvider(ASRProvider):
    """Gladia Solaria-1 real-time streaming ASR experimental adapter."""

    def __init__(
        self,
        api_key: str | None = None,
        region: str = "eu-west",
        code_switching: bool = True,
        custom_vocabulary: bool = True,
        **kwargs: Any,
    ) -> None:
        self._api_key = api_key or ""
        self._region = region
        self._code_switching = code_switching
        self._custom_vocabulary = custom_vocabulary
        self.session: ProviderSession | None = None
        self._ws: Any = None
        self._read_task: asyncio.Task[None] | None = None
        self._is_open = False
        self._is_finished = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=False,  # Verified HTTP 400 on /v2/live; unsupported in live mode
            multichannel=False,
            keyterms=True,
            max_keyterms=100,
        )

    async def open(self, session: ProviderSession) -> None:
        self.session = session
        self._is_finished = False

        if not self._api_key:
            self._is_open = True
            logger.info(
                "GladiaProvider session opened (dry/no-key mode) for %s", session.meeting_id
            )
            return

        langs = ["en", "hi"] if self._code_switching else ["en"]
        vocab = [k.strip() for k in session.keyterms[:100] if k.strip()]

        payload: dict[str, Any] = {
            "model": "solaria-1",
            "encoding": "wav/pcm",
            "bit_depth": 16,
            "sample_rate": session.sample_rate,
            "channels": 1,
            "language_config": {
                "languages": langs,
                "code_switching": self._code_switching,
            },
            "messages_config": {
                "receive_partial_transcripts": True,
                "receive_final_transcripts": True,
            },
        }
        if self._custom_vocabulary and vocab:
            payload["realtime_processing"] = {
                "custom_vocabulary": True,
                "custom_vocabulary_config": {"vocabulary": vocab},
            }

        init_url = f"https://api.gladia.io/v2/live?region={self._region}"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                init_url,
                json=payload,
                headers={"x-gladia-key": self._api_key},
            )
            resp.raise_for_status()
            ws_url = resp.json()["url"]

        self._ws = await websockets.connect(ws_url, open_timeout=15.0)
        self._is_open = True
        self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            async for msg in self._ws:
                data = json.loads(msg)
                if data.get("type") == "transcript":
                    payload = data.get("data", {})
                    is_final = bool(payload.get("is_final", False))
                    utt = payload.get("utterance", {})
                    text = (utt.get("text") or "").strip()
                    if not text:
                        continue
                    start_ms = int(float(utt.get("start", 0.0)) * 1000)
                    end_ms = int(float(utt.get("end", 0.0)) * 1000)
                    await self._event_queue.put(
                        ProviderEvent(
                            event_type="final" if is_final else "interim",
                            text=text,
                            speaker=0,
                            channel=0,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            confidence=float(utt.get("confidence", 0.0)),
                            is_final=is_final,
                            raw=data,
                        )
                    )
        except Exception as exc:
            logger.debug("Gladia read loop ended: %s", exc)
        finally:
            await self._event_queue.put(ProviderEvent(event_type="closed"))

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed GladiaProvider")
        if self._ws is not None:
            await self._ws.send(chunk.data)

    async def events(self) -> AsyncIterator[ProviderEvent]:
        while True:
            ev = await self._event_queue.get()
            yield ev
            if ev.event_type == "closed" or (self._is_finished and self._event_queue.empty()):
                break

    async def finish(self) -> None:
        self._is_finished = True
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.send(json.dumps({"type": "stop_recording"}))
                if self._read_task:
                    await asyncio.wait_for(self._read_task, timeout=5.0)
        self._is_open = False
        await self._event_queue.put(ProviderEvent(event_type="closed"))

    async def close(self) -> None:
        self._is_open = False
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        await self._event_queue.put(ProviderEvent(event_type="closed"))
