"""MeetMind AI — AssemblyAI Universal-Streaming Experimental ASR Provider (A1 / B3).

Official Documentation References:
- Page Title: AssemblyAI Universal-Streaming v3 WebSocket API Reference
- Official URL: https://www.assemblyai.com/docs/streaming/universal-streaming
- Access Date: 2026-11-10
- WebSocket Endpoint: wss://streaming.assemblyai.com/v3/ws
- Regions: Global (auto-routed), US (streaming.us.assemblyai.com),
  EU (streaming.eu.assemblyai.com). No India data-residency endpoint documented.
- Empirically Verified Live Checks (2026-11-10):
  - Accepts speech_model=universal-3-6-pro and universal-streaming-english.
  - Supports live speaker_labels=true (returns Turn.speaker_label='A', etc.).
  - Supports keyterms_prompt query param (JSON-encoded string array).
  - Terminates cleanly via JSON {"type": "Terminate"}.
- Pricing: $0.15/hr base streaming / $0.45/hr Universal-3.6 Pro Realtime.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import urllib.parse
from collections.abc import AsyncIterator
from typing import Any

import websockets

from ..base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)

logger = logging.getLogger("meetmind_asr.providers.experimental.assemblyai")


class AssemblyAIProvider(ASRProvider):
    """AssemblyAI Universal-Streaming v3 real-time ASR experimental adapter."""

    def __init__(
        self,
        api_key: str | None = None,
        speech_model: str = "universal-3-6-pro",
        endpoint: str = "wss://streaming.assemblyai.com/v3/ws",
        **kwargs: Any,
    ) -> None:
        self._api_key = api_key or ""
        self._speech_model = speech_model
        self._endpoint = endpoint
        self.session: ProviderSession | None = None
        self._ws: Any = None
        self._read_task: asyncio.Task[None] | None = None
        self._is_open = False
        self._is_finished = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=True,
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
                "AssemblyAIProvider session opened (no-key mode) for %s", session.meeting_id
            )
            return

        params: list[tuple[str, str]] = [
            ("sample_rate", str(session.sample_rate)),
            ("speech_model", self._speech_model),
            ("speaker_labels", "true"),
        ]
        kt = [k.strip() for k in session.keyterms[:100] if k.strip()]
        if kt:
            params.append(("keyterms_prompt", json.dumps(kt)))

        url = f"{self._endpoint}?{urllib.parse.urlencode(params)}"
        self._ws = await websockets.connect(
            url,
            additional_headers={"Authorization": self._api_key},
            open_timeout=15.0,
        )
        self._is_open = True
        self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            async for msg in self._ws:
                data = json.loads(msg)
                if data.get("type") == "Turn":
                    text = (data.get("transcript") or "").strip()
                    if not text:
                        continue
                    is_final = bool(data.get("end_of_turn", False))
                    spk_raw = data.get("speaker_label") or "A"
                    spk_idx = max(0, ord(str(spk_raw)[0].upper()) - ord("A")) if str(spk_raw) else 0
                    words = data.get("words") or []
                    start_ms = int(words[0].get("start", 0)) if words else 0
                    end_ms = int(words[-1].get("end", 0)) if words else 0
                    await self._event_queue.put(
                        ProviderEvent(
                            event_type="final" if is_final else "interim",
                            text=text,
                            speaker=spk_idx,
                            channel=0,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            confidence=float(data.get("end_of_turn_confidence", 0.9)),
                            is_final=is_final,
                            raw=data,
                        )
                    )
        except Exception as exc:
            logger.debug("AssemblyAI read loop ended: %s", exc)
        finally:
            await self._event_queue.put(ProviderEvent(event_type="closed"))

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open:
            raise RuntimeError("Cannot send audio to closed AssemblyAIProvider")
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
                await self._ws.send(json.dumps({"type": "Terminate"}))
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
