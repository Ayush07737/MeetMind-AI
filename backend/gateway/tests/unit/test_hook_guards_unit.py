"""Unit tests verifying S0-6: Test hook guards must fail closed.

Verifies:
1. GatewaySettings refuses to start if allow_token_in_dev or gateway_bench_endpoints
   is enabled in any environment other than explicit allowlist ('dev', 'test', 'development').
2. AsrSettings refuses to start if asr_test_hooks is enabled in non-allowlisted environments.
3. meetmind_security.audit_log refuses execution if TEST_AUDIT_STATEMENT_DELAY_SECONDS
   is set in non-allowlisted environments or without ASR_TEST_HOOKS=true.
4. Default values: allow_token_in_dev=False, gateway_bench_endpoints=False, asr_test_hooks=False.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from meetmind_asr.config import AsrSettings
from meetmind_gateway.config import GatewaySettings
from meetmind_security.audit_log import write_audit_event
from pydantic import ValidationError


def test_hook_defaults_are_safe_by_default() -> None:
    """Verify that all dangerous test hooks default to False."""
    gw_s = GatewaySettings(
        clerk_jwks_url="https://test.clerk.accounts.dev/.well-known/jwks.json",
        clerk_issuer="https://test.clerk.accounts.dev",
        allowed_origins=["https://app.meetmind.ai"],
        tenant_org_map={"org_1": "tenant_1"},
    )
    assert gw_s.allow_token_in_dev is False, "allow_token_in_dev must default to False"
    assert gw_s.gateway_bench_endpoints is False, "gateway_bench_endpoints must default to False"

    asr_s = AsrSettings(
        asr_provider="stub",
        deepgram_api_key="mock",
    )
    assert asr_s.asr_test_hooks is False, "asr_test_hooks must default to False"


@pytest.mark.parametrize("forbidden_env", ["production", "prod", "staging", "", "qa", "preview"])
def test_gateway_bench_endpoints_forbidden_outside_allowlist(forbidden_env: str) -> None:
    """GatewaySettings must raise ValidationError if bench endpoints are enabled outside."""
    with pytest.raises(ValidationError) as exc:
        GatewaySettings(
            app_env=forbidden_env,
            gateway_bench_endpoints=True,
            clerk_jwks_url="https://test.clerk.accounts.dev/.well-known/jwks.json",
            clerk_issuer="https://test.clerk.accounts.dev",
            allowed_origins=["https://app.meetmind.ai"],
            tenant_org_map={"org_1": "tenant_1"},
        )
    assert "GATEWAY_BENCH_ENDPOINTS is forbidden" in str(exc.value)


@pytest.mark.parametrize("forbidden_env", ["production", "prod", "staging", "", "qa", "preview"])
def test_gateway_allow_token_in_dev_forbidden_outside_allowlist(forbidden_env: str) -> None:
    """GatewaySettings must raise ValidationError if allow_token_in_dev is enabled outside."""
    with pytest.raises(ValidationError) as exc:
        GatewaySettings(
            app_env=forbidden_env,
            allow_token_in_dev=True,
            clerk_jwks_url="https://test.clerk.accounts.dev/.well-known/jwks.json",
            clerk_issuer="https://test.clerk.accounts.dev",
            allowed_origins=["https://app.meetmind.ai"],
            tenant_org_map={"org_1": "tenant_1"},
        )
    assert "ALLOW_TOKEN_IN_DEV is forbidden" in str(exc.value)


@pytest.mark.parametrize("allowed_env", ["dev", "test", "development"])
def test_gateway_hooks_permitted_in_allowlist(allowed_env: str) -> None:
    """GatewaySettings permits hooks when app_env is in explicit allowlist."""
    gw_s = GatewaySettings(
        app_env=allowed_env,
        gateway_bench_endpoints=True,
        allow_token_in_dev=True,
        clerk_jwks_url="https://test.clerk.accounts.dev/.well-known/jwks.json",
        clerk_issuer="https://test.clerk.accounts.dev",
        allowed_origins=["https://app.meetmind.ai"],
        tenant_org_map={"org_1": "tenant_1"},
    )
    assert gw_s.gateway_bench_endpoints is True
    assert gw_s.allow_token_in_dev is True


@pytest.mark.parametrize("forbidden_env", ["production", "prod", "staging", "", "qa", "preview"])
def test_asr_test_hooks_forbidden_outside_allowlist(forbidden_env: str) -> None:
    """AsrSettings must raise ValidationError if asr_test_hooks is enabled outside allowlist."""
    with pytest.raises(ValidationError) as exc:
        AsrSettings(
            app_env=forbidden_env,
            asr_test_hooks=True,
            asr_provider="stub",
            deepgram_api_key="mock",
        )
    assert "ASR_TEST_HOOKS is forbidden" in str(exc.value)


@pytest.mark.parametrize("allowed_env", ["dev", "test", "development"])
def test_asr_test_hooks_permitted_in_allowlist(allowed_env: str) -> None:
    """AsrSettings permits asr_test_hooks in explicit allowlist."""
    asr_s = AsrSettings(
        app_env=allowed_env,
        asr_test_hooks=True,
        asr_provider="stub",
        deepgram_api_key="mock",
    )
    assert asr_s.asr_test_hooks is True


@pytest.mark.asyncio
async def test_audit_delay_hook_fails_closed_in_non_allowlist() -> None:
    """write_audit_event fails closed if delay hook is used outside allowlist."""
    env = {
        "TEST_AUDIT_STATEMENT_DELAY_SECONDS": "0.1",
        "APP_ENV": "staging",
        "ASR_TEST_HOOKS": "true",
    }
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(RuntimeError) as exc:
            await write_audit_event(
                tenant_id="tenant_1",
                user_id="user_1",
                event_type="test_event",
            )
        assert "TEST_AUDIT_STATEMENT_DELAY_SECONDS is forbidden" in str(exc.value)


@pytest.mark.asyncio
async def test_audit_delay_hook_fails_closed_without_asr_test_hooks() -> None:
    """write_audit_event fails closed if ASR_TEST_HOOKS is not explicitly true."""
    env = {
        "TEST_AUDIT_STATEMENT_DELAY_SECONDS": "0.1",
        "APP_ENV": "test",
        "ASR_TEST_HOOKS": "false",
    }
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(RuntimeError) as exc:
            await write_audit_event(
                tenant_id="tenant_1",
                user_id="user_1",
                event_type="test_event",
            )
        assert "requires ASR_TEST_HOOKS=true" in str(exc.value)
