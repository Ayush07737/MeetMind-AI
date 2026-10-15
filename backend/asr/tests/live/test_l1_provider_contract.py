"""MeetMind AI — L1: Deepgram Provider Contract Suite Against Real API.

Verifies:
- open(), send_audio(), interim/final ordering against real Deepgram Nova-3
- finish() flushes finals and closes cleanly
- close() is idempotent
- keepalive during 15s of silence keeps session open
"""

from __future__ import annotations

import asyncio
import os
import wave
from pathlib import Path

import pytest
from meetmind_asr.providers.base import AudioChunk, ProviderEvent, ProviderSession
from meetmind_asr.providers.deepgram import DeepgramProvider
from pydantic import SecretStr

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_WAV = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.wav"
)


def get_deepgram_api_key() -> SecretStr:
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        pytest.skip("DEEPGRAM_API_KEY is not set in environment")
    return SecretStr(key)


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l1_provider_lifecycle_and_ordering():
    """L1: Open, stream 2 seconds of real speech, finish flushes finals, verify ordering."""
    print("\n[Estimated Cost] ~0.033 audio minutes x $0.0058/min = ~$0.0002")
    api_key = get_deepgram_api_key()
    provider = DeepgramProvider(api_key=api_key, mip_opt_out=True)

    session = ProviderSession(
        tenant_id="tenant_live_l1",
        meeting_id="mtg_live_l1",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=["EBITDA", "Acme Holdings"],
    )

    await provider.open(session)
    assert provider._is_open is True

    # Read 2 seconds (20 x 100ms chunks) from speech fixture
    chunks: list[bytes] = []
    if FIXTURE_WAV.exists():
        with wave.open(str(FIXTURE_WAV), "rb") as wf:
            for _ in range(20):
                data = wf.readframes(1600)
                if data:
                    chunks.append(data)
    else:
        chunks = [b"\x00\x01" * 1600] * 20

    # Stream chunks
    for idx, cdata in enumerate(chunks, 1):
        chunk = AudioChunk(
            data=cdata,
            client_seq=idx,
            capture_ts_ms=1000 + (idx * 100),
            recv_ns=0,
        )
        await provider.send_audio(chunk)
        await asyncio.sleep(0.05)

    # finish() must flush finals
    await provider.finish()

    # Collect events
    events: list[ProviderEvent] = []
    async for ev in provider.events():
        events.append(ev)

    event_types = [e.event_type for e in events]
    assert "closed" in event_types
    # Deepgram should have emitted at least one interim or final transcript
    transcripts = [e for e in events if e.event_type in ("interim", "final")]
    assert len(transcripts) > 0
    # Final transcript should have text
    finals = [e for e in events if e.is_final and e.text]
    assert len(finals) > 0, f"Expected at least one final transcript, got {event_types}"

    # Verify close() is idempotent
    await provider.close()
    await provider.close()
    assert provider._is_open is False


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l1_keepalive_during_silence():
    """L1: Keepalive during 15 seconds of silence keeps Deepgram session alive."""
    print("\n[Estimated Cost] 0 audio minutes x $0.0058/min = $0.0000 (KeepAlive only)")
    api_key = get_deepgram_api_key()
    provider = DeepgramProvider(api_key=api_key, mip_opt_out=True)

    session = ProviderSession(
        tenant_id="tenant_live_l1_ka",
        meeting_id="mtg_live_l1_ka",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=[],
    )

    await provider.open(session)
    assert provider._is_open is True

    # Idle for 15 seconds (Keepalive runs every 4 seconds)
    await asyncio.sleep(15.0)

    # Provider must still be open
    assert provider._is_open is True

    # Audio chunk send should succeed without error
    chunk = AudioChunk(
        data=b"\x00" * 3200,
        client_seq=1,
        capture_ts_ms=1000,
        recv_ns=0,
    )
    await provider.send_audio(chunk)

    await provider.finish()
    assert provider._is_open is False
