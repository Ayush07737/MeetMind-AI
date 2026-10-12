"""Unit tests for ASR Service Configuration (Fail-Fast Validation).

Verifies:
- Default stub configuration loads cleanly
- Fail-fast when ASR_PROVIDER=deepgram without DEEPGRAM_API_KEY
- Fail-fast on invalid/negative limits
- Secret masking for DEEPGRAM_API_KEY
"""

from __future__ import annotations

import pytest
from meetmind_asr.config import AsrSettings
from pydantic import ValidationError


def test_default_stub_settings():
    settings = AsrSettings(
        _env_file=None,
        ASR_PROVIDER="stub",
        DEEPGRAM_API_KEY=None,
    )
    assert settings.asr_provider == "stub"
    assert settings.deepgram_api_key is None
    assert settings.asr_max_meetings_per_worker > 0
    assert settings.asr_replay_window_ms == 5000
    assert settings.asr_buffer_max_seconds == 30


def test_deepgram_requires_api_key():
    # Missing key
    with pytest.raises(ValidationError, match="DEEPGRAM_API_KEY is required"):
        AsrSettings(
            ASR_PROVIDER="deepgram",
            DEEPGRAM_API_KEY=None,
        )

    # Empty key
    with pytest.raises(ValidationError, match="DEEPGRAM_API_KEY is required"):
        AsrSettings(
            ASR_PROVIDER="deepgram",
            DEEPGRAM_API_KEY="   ",
        )


def test_deepgram_valid_with_api_key():
    settings = AsrSettings(
        ASR_PROVIDER="deepgram",
        DEEPGRAM_API_KEY="dg_secret_token_12345",
    )
    assert settings.asr_provider == "deepgram"
    assert settings.deepgram_api_key is not None
    assert settings.deepgram_api_key.get_secret_value() == "dg_secret_token_12345"
    # Ensure repr masks the secret
    assert "dg_secret_token_12345" not in repr(settings.deepgram_api_key)
    assert "**********" in repr(settings.deepgram_api_key) or "******" in repr(
        settings.deepgram_api_key
    )


@pytest.mark.parametrize(
    "invalid_kwargs",
    [
        {"ASR_MAX_MEETINGS_PER_WORKER": 0},
        {"ASR_MAX_MEETINGS_PER_WORKER": -1},
        {"ASR_REPLAY_WINDOW_MS": 0},
        {"ASR_REPLAY_WINDOW_MS": -500},
        {"ASR_BUFFER_MAX_SECONDS": 0},
        {"ASR_BUFFER_MAX_SECONDS": -10},
    ],
)
def test_invalid_positive_limits(invalid_kwargs):
    with pytest.raises(ValidationError):
        AsrSettings(**invalid_kwargs)
