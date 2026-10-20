"""MeetMind AI — Unit tests for Region Semantics & Deployment Guardrails (D2e).

Verifies:
1. Tenant asr_region is Optional (None = inherit); "global" is never a sentinel.
2. ASR_ALLOWED_REGIONS deployment guardrail (validates allowed regions, rejects unknown).
3. ASR_REGION must be in ASR_ALLOWED_REGIONS.
4. DEEPGRAM_BASE_URL is accepted only for loopback test servers (127.0.0.1, localhost).
5. resolve_asr_base_url derives host strictly from region via allow-list.
6. Non-global regional endpoints force mip_opt_out=true; DeepgramProvider fails fast if False.
7. Strict residency mode: tenant requesting region outside allowed emits degraded.
8. Preferred residency mode: tenant requesting region outside allowed
   falls back to global and audits.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from meetmind_asr.config import (
    AsrSettings,
    resolve_asr_base_url,
)
from meetmind_asr.providers.base import ProviderSession
from meetmind_asr.providers.deepgram import DeepgramProvider
from meetmind_asr.service import AsrService
from meetmind_asr.tenant_config import AsrTenantConfig
from meetmind_security.tenant import TenantConfig
from pydantic import SecretStr, ValidationError


def test_tenant_asr_region_optional_and_residency_mode():
    """Tenant asr_region defaults to None (inherit); residency_mode defaults to strict."""
    tc = TenantConfig(
        tenant_id="tenant_a",
        pg_schema="tenant_a",
        neo4j_namespace="tenant_a",
        qdrant_prefix="tenant_a",
        redis_prefix="tenant_a",
    )
    assert tc.asr_region is None
    assert tc.residency_mode == "strict"

    # Explicit global is NOT None sentinel
    tc_global = TenantConfig(
        tenant_id="tenant_b",
        pg_schema="tenant_b",
        neo4j_namespace="tenant_b",
        qdrant_prefix="tenant_b",
        redis_prefix="tenant_b",
        asr_region="global",
        residency_mode="preferred",
    )
    assert tc_global.asr_region == "global"
    assert tc_global.residency_mode == "preferred"

    # Invalid residency_mode fails validation
    with pytest.raises(ValueError):
        TenantConfig(
            tenant_id="tenant_c",
            pg_schema="tenant_c",
            neo4j_namespace="tenant_c",
            qdrant_prefix="tenant_c",
            redis_prefix="tenant_c",
            residency_mode="invalid",
        )


def test_asr_tenant_config_optional():
    """AsrTenantConfig supports None region and strict/preferred modes."""
    atc = AsrTenantConfig(tenant_id="tenant_a")
    assert atc.asr_region is None
    assert atc.residency_mode == "strict"

    atc_in = AsrTenantConfig(tenant_id="tenant_b", asr_region="in", residency_mode="strict")
    assert atc_in.asr_region == "in"


def test_asr_allowed_regions_validation():
    """ASR_ALLOWED_REGIONS guardrail rejects invalid regions and mismatches with ASR_REGION."""
    # Valid allowed regions
    settings = AsrSettings(
        _env_file=None,
        ASR_ALLOWED_REGIONS="in,global",
        ASR_REGION="in",
    )
    assert settings.allowed_regions_set == {"in", "global"}

    # Unknown region in ASR_ALLOWED_REGIONS fails fast
    with pytest.raises(ValidationError, match="Invalid regions in ASR_ALLOWED_REGIONS"):
        AsrSettings(
            _env_file=None,
            ASR_ALLOWED_REGIONS="global,mars_west_1",
            ASR_REGION="global",
        )

    # ASR_REGION not in ASR_ALLOWED_REGIONS fails fast
    with pytest.raises(ValidationError, match="contradicts deployment guardrail"):
        AsrSettings(
            _env_file=None,
            ASR_ALLOWED_REGIONS="global,eu",
            ASR_REGION="in",
        )


def test_deepgram_base_url_loopback_only():
    """DEEPGRAM_BASE_URL is accepted only for loopback test servers (127.0.0.1, localhost)."""
    # Allowed: 127.0.0.1
    s_loopback_ip = AsrSettings(
        _env_file=None,
        DEEPGRAM_BASE_URL="http://127.0.0.1:8080",
    )
    assert s_loopback_ip.deepgram_base_url == "http://127.0.0.1:8080"
    assert resolve_asr_base_url(s_loopback_ip, "global") == "http://127.0.0.1:8080"

    # Allowed: localhost
    s_localhost = AsrSettings(
        _env_file=None,
        DEEPGRAM_BASE_URL="http://localhost:9000",
    )
    assert resolve_asr_base_url(s_localhost, "in") == "http://localhost:9000"

    # Forbidden: external host
    with pytest.raises(ValidationError, match="not a loopback test server"):
        AsrSettings(
            _env_file=None,
            DEEPGRAM_BASE_URL="https://external-proxy.example.com",
        )

    # Default allowed regions: falls back to configured ASR_REGION only
    s_default_allowed = AsrSettings(_env_file=None, ASR_REGION="in", ASR_ALLOWED_REGIONS=None)
    assert s_default_allowed.allowed_regions_set == {"in"}

    s_au = AsrSettings(_env_file=None, ASR_REGION="au", ASR_ALLOWED_REGIONS="au")
    assert s_au.allowed_regions_set == {"au"}


def test_resolve_asr_base_url_from_allow_list():
    """resolve_asr_base_url resolves host strictly according to DEEPGRAM_REGION_HOSTS."""
    settings = AsrSettings(_env_file=None, DEEPGRAM_BASE_URL=None, ASR_REGION="global")
    assert resolve_asr_base_url(settings, "global") == "wss://api.deepgram.com"
    assert resolve_asr_base_url(settings, "in") == "wss://api.in.deepgram.com"
    assert resolve_asr_base_url(settings, "eu") == "wss://api.eu.deepgram.com"
    assert resolve_asr_base_url(settings, "au") == "wss://api.au.deepgram.com"

    with pytest.raises(ValueError, match="Invalid asr_region"):
        resolve_asr_base_url(settings, "unknown_region")


@pytest.mark.asyncio
async def test_deepgram_provider_refuses_regional_without_mip_opt_out():
    """DeepgramProvider refuses to open a session on a regional endpoint if mip_opt_out is False."""
    prov = DeepgramProvider(
        api_key=SecretStr("mock_key"),
        base_url="wss://api.in.deepgram.com",
        mip_opt_out=False,  # Violation on regional endpoint
    )
    session = ProviderSession(
        tenant_id="tenant_xyz",
        meeting_id="meeting_123",
        sample_rate=16000,
        channels=1,
    )
    with pytest.raises(ValueError, match="Refusing to open session"):
        await prov.open(session)


@pytest.mark.asyncio
async def test_service_strict_residency_mode_emits_degraded():
    """Strict residency mode: tenant requesting forbidden region emits degraded event."""
    settings = AsrSettings(
        _env_file=None,
        ASR_ALLOWED_REGIONS="global,in",
        ASR_REGION="in",
    )
    mock_redis = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)

    mock_t_provider = MagicMock()
    # Tenant requests 'eu', which is not in allowed regions
    mock_t_provider.get_config = AsyncMock(
        return_value=AsrTenantConfig(
            tenant_id="tenant_eu",
            asr_region="eu",
            residency_mode="strict",
        )
    )

    svc = AsrService(settings=settings)
    svc.redis = mock_redis
    svc.tenant_config_provider = mock_t_provider

    # Attempt to start pipeline
    await svc._start_pipeline(tenant="tenant_eu", meeting_id="meet_1")

    # In strict mode, pipeline must NOT be added to active pipelines
    assert "meet_1" not in svc._active_pipelines
    # Redis pipeline xadd was called to emit degraded event with reason='residency_violation'
    assert mock_pipe.xadd.called
    assert any("residency_violation" in str(c) for c in mock_pipe.xadd.call_args_list)


@pytest.mark.asyncio
async def test_service_preferred_residency_mode_falls_back_and_audits():
    """Preferred residency mode: tenant requesting forbidden region falls back
    to global and audits.
    """
    settings = AsrSettings(
        _env_file=None,
        ASR_ALLOWED_REGIONS="global,in",
        ASR_REGION="in",
        ASR_PROVIDER="stub",
    )
    mock_redis = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)
    mock_redis.xreadgroup = AsyncMock(return_value=[])
    mock_redis.xautoclaim = AsyncMock(return_value=("0", []))
    mock_redis.hgetall = AsyncMock(return_value={})

    mock_t_provider = MagicMock()
    # Tenant requests 'eu', which is not in allowed regions, but residency_mode='preferred'
    mock_t_provider.get_config = AsyncMock(
        return_value=AsrTenantConfig(
            tenant_id="tenant_eu_pref",
            asr_region="eu",
            residency_mode="preferred",
        )
    )

    svc = AsrService(settings=settings)
    svc.redis = mock_redis
    svc.tenant_config_provider = mock_t_provider
    svc.auditor._enqueue = MagicMock()

    from unittest.mock import patch

    with patch("meetmind_asr.service.MeetingPipeline.start", new_callable=AsyncMock):
        # Run _start_pipeline (stub provider will be created with global)
        await svc._start_pipeline(tenant="tenant_eu_pref", meeting_id="meet_pref")

    # Audit event asr_region_fallback was enqueued
    svc.auditor._enqueue.assert_called_once()
    call_args = svc.auditor._enqueue.call_args[0]
    assert call_args[0] == "tenant_eu_pref"
    assert call_args[2] == "asr_region_fallback"
    assert call_args[3]["requested_region"] == "eu"
    assert call_args[3]["effective_region"] == "global"
