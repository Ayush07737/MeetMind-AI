"""Unit tests for Deepgram Nova-3 Provider (WP-C).

Verifies:
- Streaming WebSocket URL query parameters
  (nova-3, mip_opt_out=true, diarize=true, interim_results=true)
- Repeated keyterm prompting parameter (Nova-3 parameter format)
- Multichannel flag when channels=2
- Indian English locale support (en-IN)
- Keyterm limit enforcement (max 100)
- Deepgram JSON message parsing (Results interim/final, UtteranceEnd, Error)
- Secret masking (API key never in URL, logs, or error messages)
- _debug_drop_connection hook availability
"""

from __future__ import annotations

import urllib.parse

import pytest
from meetmind_asr.providers.base import ProviderSession
from meetmind_asr.providers.deepgram import DeepgramProvider, build_deepgram_url
from pydantic import SecretStr


def test_build_deepgram_url_parameters():
    session = ProviderSession(
        tenant_id="tenant_alpha",
        meeting_id="meeting_123",
        sample_rate=16000,
        channels=1,
        language="en-IN",
        keyterms=["MeetMind", "TensorFlow", "Kubernetes"],
    )
    url = build_deepgram_url("wss://api.deepgram.com", session, mip_opt_out=True)
    parsed = urllib.parse.urlparse(url)
    assert parsed.scheme == "wss"
    assert parsed.netloc == "api.deepgram.com"
    assert parsed.path == "/v1/listen"

    qs = urllib.parse.parse_qs(parsed.query)
    assert qs["model"] == ["nova-3"]
    assert qs["encoding"] == ["linear16"]
    assert qs["sample_rate"] == ["16000"]
    assert qs["channels"] == ["1"]
    assert qs["diarize"] == ["true"]
    assert qs["interim_results"] == ["true"]
    assert qs["language"] == ["en-IN"]
    assert qs["mip_opt_out"] == ["true"]
    # Keyterms must be repeated
    assert qs["keyterm"] == ["MeetMind", "TensorFlow", "Kubernetes"]
    assert "multichannel" not in qs


def test_build_deepgram_url_multichannel():
    session = ProviderSession(
        tenant_id="tenant_alpha",
        meeting_id="meeting_123",
        sample_rate=16000,
        channels=2,
        language="en-US",
        keyterms=[],
    )
    url = build_deepgram_url("wss://api.deepgram.com", session, mip_opt_out=True)
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs["channels"] == ["2"]
    assert qs["multichannel"] == ["true"]


@pytest.mark.asyncio
async def test_keyterm_limit_enforcement():
    secret_key = SecretStr("mock_secret_key_12345")
    provider = DeepgramProvider(api_key=secret_key)
    assert provider.capabilities.max_keyterms == 100

    # 101 keyterms should fail
    excessive_keyterms = [f"term_{i}" for i in range(101)]
    session = ProviderSession(
        tenant_id="tenant_alpha",
        meeting_id="meeting_123",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=excessive_keyterms,
    )
    with pytest.raises(ValueError, match="exceeds maximum"):
        await provider.open(session)


@pytest.mark.asyncio
async def test_secret_not_leaked_on_connection_error():
    secret_value = "super_confidential_dg_key_999"
    secret_key = SecretStr(secret_value)
    # Point to non-existent endpoint to force error
    provider = DeepgramProvider(
        api_key=secret_key,
        base_url="wss://127.0.0.1:59999",  # unused port
    )
    session = ProviderSession(
        tenant_id="tenant_alpha",
        meeting_id="meeting_123",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=[],
    )

    with pytest.raises(ConnectionError) as exc_info:
        await provider.open(session)

    err_str = str(exc_info.value)
    assert secret_value not in err_str
    assert "Token" not in err_str


@pytest.mark.asyncio
async def test_message_parser_results_final_and_interim():
    provider = DeepgramProvider(api_key=SecretStr("mock_key"))

    # 1. Interim message
    interim_msg = {
        "type": "Results",
        "channel_index": [0, 0],
        "start": 1.25,
        "duration": 0.75,
        "is_final": False,
        "speech_final": False,
        "channel": {
            "alternatives": [
                {
                    "transcript": "Hello everyone",
                    "confidence": 0.88,
                    "words": [{"word": "Hello", "speaker": 0}],
                }
            ]
        },
    }
    await provider._handle_message(interim_msg)
    ev_interim = await provider._event_queue.get()
    assert ev_interim.event_type == "interim"
    assert ev_interim.text == "Hello everyone"
    assert ev_interim.start_ms == 1250
    assert ev_interim.end_ms == 2000
    assert not ev_interim.is_final
    assert not ev_interim.is_stable

    # 2. Final message
    final_msg = {
        "type": "Results",
        "channel_index": [1, 0],
        "start": 2.0,
        "duration": 1.5,
        "is_final": True,
        "speech_final": True,
        "channel": {
            "alternatives": [
                {
                    "transcript": "Welcome to MeetMind AI",
                    "confidence": 0.99,
                    "words": [{"word": "Welcome", "speaker": 2}],
                }
            ]
        },
    }
    await provider._handle_message(final_msg)
    ev_final = await provider._event_queue.get()
    assert ev_final.event_type == "final"
    assert ev_final.text == "Welcome to MeetMind AI"
    assert ev_final.channel == 1
    assert ev_final.speaker == 2
    assert ev_final.start_ms == 2000
    assert ev_final.end_ms == 3500
    assert ev_final.is_final
    assert ev_final.is_stable


@pytest.mark.asyncio
async def test_message_parser_utterance_end_and_error():
    provider = DeepgramProvider(api_key=SecretStr("mock_key"))

    # UtteranceEnd
    await provider._handle_message({"type": "UtteranceEnd"})
    ev_ue = await provider._event_queue.get()
    assert ev_ue.event_type == "utterance_end"

    # Error
    await provider._handle_message({"type": "Error", "message": "Rate limit exceeded"})
    ev_err = await provider._event_queue.get()
    assert ev_err.event_type == "error"
    assert "Rate limit exceeded" in str(ev_err.error)


def test_debug_drop_connection_hook_exists():
    provider = DeepgramProvider(api_key=SecretStr("mock_key"))
    assert hasattr(provider, "_debug_drop_connection")
    assert callable(provider._debug_drop_connection)
