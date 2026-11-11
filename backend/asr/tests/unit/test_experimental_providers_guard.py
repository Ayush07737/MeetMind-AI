"""Static Test: Experimental ASR Provider Flag Guard (B3).

Verifies that experimental adapters under backend/asr/meetmind_asr/providers/experimental/
cannot be selected unless ASR_EXPERIMENTAL_PROVIDERS=true.
"""

import pytest
from meetmind_asr.providers import get_provider


def test_experimental_providers_fail_closed_without_flag(monkeypatch):
    """Attempting to select experimental providers without flag must raise RuntimeError."""
    monkeypatch.delenv("ASR_EXPERIMENTAL_PROVIDERS", raising=False)

    for prov in ("flux", "sarvam", "openai", "gladia", "assemblyai"):
        with pytest.raises(RuntimeError) as exc_info:
            get_provider(prov)
        assert "is experimental and requires ASR_EXPERIMENTAL_PROVIDERS=true" in str(exc_info.value)

    # False or other strings must also fail
    monkeypatch.setenv("ASR_EXPERIMENTAL_PROVIDERS", "false")
    for prov in ("flux", "sarvam", "openai", "gladia", "assemblyai"):
        with pytest.raises(RuntimeError):
            get_provider(prov)


def test_experimental_providers_instantiate_with_flag(monkeypatch):
    """When ASR_EXPERIMENTAL_PROVIDERS=true, experimental providers instantiate successfully."""
    monkeypatch.setenv("ASR_EXPERIMENTAL_PROVIDERS", "true")

    flux = get_provider("flux")
    assert flux is not None
    assert flux.__class__.__name__ == "FluxProvider"

    sarvam = get_provider("sarvam")
    assert sarvam is not None
    assert sarvam.__class__.__name__ == "SarvamProvider"

    openai_p = get_provider("openai")
    assert openai_p is not None
    assert openai_p.__class__.__name__ == "OpenAIStreamingProvider"

    gladia = get_provider("gladia")
    assert gladia is not None
    assert gladia.__class__.__name__ == "GladiaProvider"

    assemblyai = get_provider("assemblyai")
    assert assemblyai is not None
    assert assemblyai.__class__.__name__ == "AssemblyAIProvider"
