"""MeetMind AI — ASR Provider Base Interface (WP-B).

Defines the abstract interface for all streaming ASR providers (Deepgram, Stub).
Enforces capabilities disclosure and asynchronous streaming lifecycle.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Literal

ProviderEventType = Literal["interim", "final", "utterance_end", "error", "closed"]


@dataclass(frozen=True)
class ProviderCapabilities:
    """Discloses provider feature support and operational limits."""

    diarization: bool
    multichannel: bool
    keyterms: bool
    max_keyterms: int = 100


@dataclass
class ProviderSession:
    """Session configuration passed to ASRProvider.open()."""

    tenant_id: str
    meeting_id: str
    sample_rate: int
    channels: int
    language: str = "en"
    keyterms: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AudioChunk:
    """Audio frame delivered to the provider."""

    data: bytes
    client_seq: int
    capture_ts_ms: int
    recv_ns: int = 0


@dataclass(frozen=True)
class ProviderEvent:
    """Event emitted by ASRProvider.events()."""

    event_type: ProviderEventType
    text: str = ""
    start_ms: int = 0  # Provider session offset in ms
    end_ms: int = 0  # Provider session offset in ms
    confidence: float = 0.0
    speaker: int = 0
    channel: int = 0
    is_final: bool = False
    is_stable: bool = False
    raw: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class ASRProvider(ABC):
    """Abstract Base Class for streaming Speech-to-Text providers."""

    @property
    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """Return declared provider capabilities."""
        ...

    @abstractmethod
    async def open(self, session: ProviderSession) -> None:
        """Establish provider session / streaming connection."""
        ...

    @abstractmethod
    async def send_audio(self, chunk: AudioChunk) -> None:
        """Send a chunk of raw audio to the provider."""
        ...

    @abstractmethod
    def events(self) -> AsyncIterator[ProviderEvent]:
        """Stream transcription and status events asynchronously."""
        ...

    @abstractmethod
    async def finish(self) -> None:
        """Signal end of stream and flush remaining transcription results."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Terminate provider connection and cleanup resources immediately."""
        ...
