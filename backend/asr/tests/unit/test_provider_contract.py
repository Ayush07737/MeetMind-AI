"""Unit tests for ASR Provider Interface & Contract (WP-B).

Verifies:
- ASRProvider ABC contract compliance for StubProvider
- Provider session open, audio sending, and event stream termination
- Provider factory behavior (get_provider)
- Provider switching test: only ASR_PROVIDER config is needed to switch implementations
"""

from __future__ import annotations

import pytest
from meetmind_asr.providers import get_provider
from meetmind_asr.providers.base import (
    ASRProvider,
    AudioChunk,
    ProviderEvent,
    ProviderSession,
)
from meetmind_asr.providers.deepgram import DeepgramProvider
from meetmind_asr.providers.stub import StubFaultConfig, StubProvider
from pydantic import SecretStr


@pytest.mark.asyncio
async def test_stub_provider_contract_lifecycle():
    provider = StubProvider()
    assert isinstance(provider, ASRProvider)
    assert provider.capabilities.diarization is True
    assert provider.capabilities.multichannel is True

    session = ProviderSession(
        tenant_id="tenant_1",
        meeting_id="meeting_1",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=["Roadmap", "Budget"],
    )
    await provider.open(session)

    # Send 5 chunks
    for seq in range(1, 6):
        chunk = AudioChunk(
            data=b"\x00" * 3200,
            client_seq=seq,
            capture_ts_ms=1000 + seq * 100,
            recv_ns=0,
        )
        await provider.send_audio(chunk)

    # Finish provider
    await provider.finish()

    # Collect events
    events: list[ProviderEvent] = []
    async for event in provider.events():
        events.append(event)

    # We should have received interims, at least one final, and closed
    event_types = [e.event_type for e in events]
    assert "interim" in event_types
    assert "final" in event_types
    assert "closed" in event_types

    # Received seqs tracking
    assert provider.received_client_seqs == {1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_stub_provider_fault_injection():
    # Disconnect at chunk 3
    faults = StubFaultConfig(
        disconnect_at_chunk=3,
        outage_duration_s=0.5,
    )
    provider = StubProvider(fault_config=faults)
    session = ProviderSession(
        tenant_id="tenant_1",
        meeting_id="meeting_1",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=[],
    )
    await provider.open(session)

    # Chunks 1 and 2 succeed
    await provider.send_audio(AudioChunk(data=b"a", client_seq=1, capture_ts_ms=1000, recv_ns=0))
    await provider.send_audio(AudioChunk(data=b"b", client_seq=2, capture_ts_ms=1100, recv_ns=0))

    # Chunk 3 triggers disconnect
    await provider.send_audio(AudioChunk(data=b"c", client_seq=3, capture_ts_ms=1200, recv_ns=0))
    assert not provider._is_open

    # Reopening immediately during outage fails
    with pytest.raises(ConnectionError, match="outage"):
        await provider.open(session)


def test_provider_factory_and_clean_switch():
    # 1. Stub provider
    p_stub = get_provider("stub")
    assert isinstance(p_stub, StubProvider)

    # 2. Deepgram provider
    p_dg = get_provider("deepgram", api_key=SecretStr("dummy_key"))
    assert isinstance(p_dg, DeepgramProvider)

    # 3. Invalid provider
    with pytest.raises(ValueError, match="Unknown ASR provider"):
        get_provider("invalid_provider")
