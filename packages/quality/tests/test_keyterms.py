"""Unit tests for meetmind_quality.keyterms."""

import pytest
from meetmind_quality.keyterms import (
    MAX_DEEPGRAM_KEYTERMS,
    EngineeringKeytermProvider,
    StaticKeytermProvider,
)


@pytest.mark.asyncio
async def test_static_keyterm_provider_cap():
    terms = [f"term_{i}" for i in range(150)]
    provider = StaticKeytermProvider(terms)
    res = await provider.get_keyterms("tenant_any")

    assert len(res) == MAX_DEEPGRAM_KEYTERMS
    assert res[0] == "term_0"
    assert res[-1] == f"term_{MAX_DEEPGRAM_KEYTERMS - 1}"


@pytest.mark.asyncio
async def test_engineering_keyterm_provider():
    provider = EngineeringKeytermProvider(
        tenant_overrides={"tenant_custom": ["CustomServiceX", "InternalToolY"]}
    )

    terms_custom = await provider.get_keyterms("tenant_custom")
    assert "CustomServiceX" in terms_custom
    assert "Kubernetes" in terms_custom
    assert len(terms_custom) <= MAX_DEEPGRAM_KEYTERMS

    terms_default = await provider.get_keyterms("tenant_default")
    assert "CustomServiceX" not in terms_default
    assert "Kafka" in terms_default
    assert len(terms_default) <= MAX_DEEPGRAM_KEYTERMS
