"""MeetMind AI — ASR Provider Factory & Exports."""

from __future__ import annotations

from typing import Any

from .base import (
    ASRProvider,
    AudioChunk,
    ProviderCapabilities,
    ProviderEvent,
    ProviderEventType,
    ProviderSession,
)
from .deepgram import DeepgramProvider, build_deepgram_url
from .stub import StubFaultConfig, StubProvider


def get_provider(name: str, **kwargs: Any) -> ASRProvider:
    """Instantiate the configured ASR provider."""
    if name == "stub":
        return StubProvider(**kwargs)
    elif name == "deepgram":
        return DeepgramProvider(**kwargs)
    raise ValueError(f"Unknown ASR provider: '{name}'")


__all__ = [
    "ASRProvider",
    "AudioChunk",
    "DeepgramProvider",
    "ProviderCapabilities",
    "ProviderEvent",
    "ProviderEventType",
    "ProviderSession",
    "StubFaultConfig",
    "StubProvider",
    "build_deepgram_url",
    "get_provider",
]
