"""MeetMind AI — Deepgram Nova-3 Streaming Provider (WP-C).

Implements real-time speech-to-text over WebSockets against Deepgram Nova-3.
Security Invariants:
- API key is wrapped in SecretStr and NEVER logged or printed in exceptions.
- mip_opt_out=true by default to prevent audio training on regulated customer data.
- Enforces keyterm prompt limit (max 100 terms).
- Session offsets are relative to provider session (mapped to capture timeline by TimelineMapper).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import socket
import time
import urllib.parse
from collections.abc import AsyncIterator
from typing import Any

import websockets
from pydantic import SecretStr

from .base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderSession,
)

logger = logging.getLogger(__name__)


def build_deepgram_url(
    base_url: str,
    session: ProviderSession,
    mip_opt_out: bool = True,
) -> str:
    """Construct the Deepgram streaming WebSocket URL with exact query parameters."""
    parsed = urllib.parse.urlparse(base_url)
    scheme = "wss" if parsed.scheme in ("https", "wss") else "ws"
    netloc = parsed.netloc or parsed.path
    path = "/v1/listen"

    params: list[tuple[str, str]] = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", str(session.sample_rate)),
        ("channels", str(session.channels)),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("language", session.language),
    ]

    if session.channels == 2:
        params.append(("multichannel", "true"))

    if mip_opt_out:
        params.append(("mip_opt_out", "true"))

    # Nova-3 Keyterm prompting (repeat keyterm param for each term)
    for term in session.keyterms:
        clean_term = term.strip()
        if clean_term:
            params.append(("keyterm", clean_term))

    query_str = urllib.parse.urlencode(params)
    return f"{scheme}://{netloc}{path}?{query_str}"


class DeepgramProvider(ASRProvider):
    """Streaming ASR provider backed by Deepgram Nova-3 WebSocket API."""

    def __init__(
        self,
        api_key: SecretStr,
        base_url: str = "wss://api.deepgram.com",
        mip_opt_out: bool = True,
        **kwargs: Any,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url
        self._mip_opt_out = mip_opt_out

        self._ws: Any = None
        self._session: ProviderSession | None = None
        self._is_open = False
        self._event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._read_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self.accepted_client_seqs: set[int] = set()

    @property
    def session(self) -> ProviderSession | None:
        return self._session

    @session.setter
    def session(self, value: ProviderSession | None) -> None:
        self._session = value

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            diarization=True,
            multichannel=True,
            keyterms=True,
            max_keyterms=100,
        )

    async def open(self, session: ProviderSession) -> None:
        if len(session.keyterms) > self.capabilities.max_keyterms:
            raise ValueError(
                f"Keyterms count ({len(session.keyterms)}) exceeds "
                f"maximum of {self.capabilities.max_keyterms}"
            )

        self._session = session
        url = build_deepgram_url(self._base_url, session, self._mip_opt_out)

        headers = {
            "Authorization": f"Token {self._api_key.get_secret_value()}",
        }

        try:
            self._ws = await websockets.connect(
                url,
                additional_headers=headers,
                family=socket.AF_INET,
            )
            self._is_open = True
        except Exception as exc:
            # Strictly do not include headers or secrets in error messages
            logger.warning("Deepgram connection failed: %s", type(exc).__name__)
            raise ConnectionError(f"Failed to connect to Deepgram: {type(exc).__name__}") from None

        self._read_task = asyncio.create_task(self._reader_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def send_audio(self, chunk: AudioChunk) -> None:
        if not self._is_open or not self._ws:
            raise ConnectionError("Deepgram WebSocket is not open")

        try:
            await self._ws.send(chunk.data)
            self.accepted_client_seqs.add(chunk.client_seq)
        except Exception as exc:
            logger.warning("Error sending audio frame to Deepgram: %s", type(exc).__name__)
            self._is_open = False
            raise ConnectionError("Connection dropped while sending audio") from None

    async def _keepalive_loop(self) -> None:
        """Send Deepgram KeepAlive frame every 4 seconds when connection is idle."""
        while self._is_open and self._ws:
            await asyncio.sleep(4.0)
            if self._is_open and self._ws:
                try:
                    await self._ws.send(json.dumps({"type": "KeepAlive"}))
                except Exception:
                    break

    async def _reader_loop(self) -> None:
        """Continuously receive and decode Deepgram JSON messages."""
        try:
            async for raw_msg in self._ws:
                t_recv = time.perf_counter()
                if isinstance(raw_msg, str):
                    try:
                        msg = json.loads(raw_msg)
                        await self._handle_message(msg, recv_perf=t_recv)
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON from Deepgram")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("Deepgram read loop exited with: %s", type(exc).__name__)
            await self._event_queue.put(
                ProviderEvent(event_type="error", error=f"Read error: {type(exc).__name__}")
            )
        finally:
            self._is_open = False
            await self._event_queue.put(ProviderEvent(event_type="closed"))

    async def _handle_message(self, msg: dict[str, Any], recv_perf: float = 0.0) -> None:
        msg_type = msg.get("type", "")

        if msg_type == "Results":
            is_final = bool(msg.get("is_final", False))
            speech_final = bool(msg.get("speech_final", False))
            channel_index = (
                msg.get("channel_index", [0, 0])[0]
                if isinstance(msg.get("channel_index"), list)
                else 0
            )

            channel_data = msg.get("channel", {})
            alternatives = channel_data.get("alternatives", [])
            if not alternatives:
                return

            best_alt = alternatives[0]
            transcript = best_alt.get("transcript", "").strip()
            confidence = float(best_alt.get("confidence", 0.0))
            words = best_alt.get("words", [])

            # Extract start and end seconds -> convert to ms relative to provider session
            start_sec = float(msg.get("start", 0.0))
            duration_sec = float(msg.get("duration", 0.0))
            start_ms = int(start_sec * 1000)
            end_ms = int((start_sec + duration_sec) * 1000)

            speaker = 0
            if words:
                speaker = int(words[0].get("speaker", 0))

            ev_type = "final" if is_final else "interim"
            await self._event_queue.put(
                ProviderEvent(
                    event_type=ev_type,
                    text=transcript,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    confidence=confidence,
                    speaker=speaker,
                    channel=channel_index,
                    is_final=is_final,
                    is_stable=speech_final or is_final,
                    raw=msg,
                    recv_perf=recv_perf,
                )
            )

        elif msg_type == "UtteranceEnd":
            await self._event_queue.put(
                ProviderEvent(
                    event_type="utterance_end",
                    raw=msg,
                    recv_perf=recv_perf,
                )
            )

        elif msg_type == "Error":
            err_msg = msg.get("message") or msg.get("err_msg") or "Deepgram provider error"
            await self._event_queue.put(
                ProviderEvent(
                    event_type="error",
                    error=str(err_msg),
                    raw=msg,
                    recv_perf=recv_perf,
                )
            )
            await self._event_queue.put(
                ProviderEvent(
                    event_type="utterance_end",
                    raw=msg,
                )
            )

        elif msg_type == "Error":
            err_msg = msg.get("message") or msg.get("err_msg") or "Deepgram provider error"
            await self._event_queue.put(
                ProviderEvent(
                    event_type="error",
                    error=str(err_msg),
                    raw=msg,
                )
            )

    async def events(self) -> AsyncIterator[ProviderEvent]:
        while True:
            event = await self._event_queue.get()
            yield event
            if event.event_type == "closed":
                break

    async def finish(self) -> None:
        """Send CloseStream message and wait for reader loop to complete."""
        if self._ws and self._is_open:
            with contextlib.suppress(Exception):
                await self._ws.send(json.dumps({"type": "CloseStream"}))
            # Give reader loop up to 3 seconds to receive final messages
            if self._read_task:
                with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(self._read_task), timeout=3.0)
        await self.close()

    async def close(self) -> None:
        self._is_open = False
        if self._keepalive_task:
            self._keepalive_task.cancel()
        if self._read_task:
            self._read_task.cancel()
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None

    def _debug_drop_connection(self) -> None:
        """Testing hook for Session D2 fault injection."""
        if self._ws:
            self._is_open = False
            with contextlib.suppress(Exception):
                if hasattr(self._ws, "transport") and self._ws.transport:
                    self._ws.transport.close()
                else:
                    asyncio.create_task(self._ws.close(1000, "Debug drop connection"))
