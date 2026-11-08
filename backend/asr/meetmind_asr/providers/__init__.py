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
    import os

    if name in ("flux", "sarvam", "openai"):
        if not (os.environ.get("ASR_EXPERIMENTAL_PROVIDERS", "").lower() in ("true", "1")):
            raise RuntimeError(
                f"Provider '{name}' is experimental and requires ASR_EXPERIMENTAL_PROVIDERS=true"
            )
        from .experimental import FluxProvider, OpenAIStreamingProvider, SarvamProvider

        if name == "flux":
            return FluxProvider(**kwargs)
        elif name == "sarvam":
            return SarvamProvider(**kwargs)
        elif name == "openai":
            return OpenAIStreamingProvider(**kwargs)

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
