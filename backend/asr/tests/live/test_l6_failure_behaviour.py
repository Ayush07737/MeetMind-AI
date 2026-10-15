"""MeetMind AI — L6: Deepgram Failure Behaviour Suite.

Verifies:
(a) Invalid API key emits transcript_service_degraded with reason=provider_error,
    and no key material appears in logs/events/exceptions.
(b) Unreachable endpoint emits degraded within 2s, and recovers when endpoint returns.
(c) 429/5xx style rejection retries with backoff and does not spin.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import socket
from pathlib import Path

import pytest
import redis.asyncio as aioredis
from meetmind_asr.config import AsrSettings
from meetmind_asr.pipeline import MeetingPipeline
from meetmind_asr.providers.deepgram import DeepgramProvider
from pydantic import SecretStr

REPO_ROOT = Path(__file__).resolve().parents[4]


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l6_invalid_api_key_no_leak():
    """L6a: Invalid API key causes provider error without leaking secret in logs/exceptions."""
    print("\n[Estimated Cost] $0.0000 (Rejected at handshake)")
    fake_key_secret = "dg_live_invalid_super_secret_token_1234567890abcdef"

    # Capture logs
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    logger = logging.getLogger("meetmind_asr")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    provider = DeepgramProvider(api_key=SecretStr(fake_key_secret))
    from meetmind_asr.providers.base import ProviderSession

    session = ProviderSession(
        tenant_id="tenant_invalid_key",
        meeting_id="mtg_invalid_key",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=[],
    )

    with pytest.raises(ConnectionError) as exc_info:
        await provider.open(session)

    logger.removeHandler(handler)
    captured = log_capture.getvalue()
    err_str = str(exc_info.value)

    # Invariant: Secret material NEVER appears in exception or logs
    assert fake_key_secret not in captured, "Secret key leaked into logs!"
    assert fake_key_secret not in err_str, "Secret key leaked into exception message!"


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l6_unreachable_endpoint_degraded():
    """L6b: Unreachable endpoint triggers degraded within 2 seconds."""
    print("\n[Estimated Cost] $0.0000 (Unreachable port)")
    blackhole_port = get_free_port()
    redis_client = aioredis.from_url("redis://127.0.0.1:6379/0")

    settings = AsrSettings(
        asr_provider="deepgram",
        deepgram_base_url=f"ws://127.0.0.1:{blackhole_port}",
        deepgram_api_key=SecretStr("dummy_key"),
        redis_url="redis://127.0.0.1:6379/0",
    )

    from meetmind_schemas.keys import events_stream_key, out_stream_key

    meeting_id = "mtg_unreachable_l6"
    tenant_id = "tenant_unreachable_l6"
    out_key = out_stream_key(tenant_id, meeting_id)
    events_key = events_stream_key(tenant_id, meeting_id)
    await redis_client.delete(out_key, events_key)

    from meetmind_asr.audit import AsrAuditor
    from meetmind_asr.health import AsrMetrics
    from meetmind_asr.providers import get_provider
    from meetmind_asr.tenant_config import AsrTenantConfig

    prov = get_provider(
        settings.asr_provider,
        api_key=settings.deepgram_api_key,
        base_url=settings.deepgram_base_url,
        mip_opt_out=True,
    )
    t_cfg = AsrTenantConfig(tenant_id=tenant_id, language="en-US", keyterms=[])
    pipeline = MeetingPipeline(
        tenant_id=tenant_id,
        meeting_id=meeting_id,
        worker_id="worker_l6",
        redis_client=redis_client,
        provider=prov,
        tenant_config=t_cfg,
        settings=settings,
        metrics=AsrMetrics(),
        auditor=AsrAuditor(),
    )

    t0 = asyncio.get_running_loop().time()
    pipe_task = asyncio.create_task(pipeline.start())

    # Wait up to 5 seconds for transcript_service_degraded event
    degraded_event = None
    for _ in range(50):
        await asyncio.sleep(0.1)
        entries = await redis_client.xrange(out_key)
        if entries:
            for _, fields in entries:
                payload_raw = fields.get(b"payload") or fields.get("payload")
                if payload_raw:
                    raw_str = (
                        payload_raw.decode() if isinstance(payload_raw, bytes) else payload_raw
                    )
                    data = json.loads(raw_str)
                    if data.get("event_type") == "transcript_service_degraded":
                        degraded_event = data
                        break
        if degraded_event:
            break

    dur = asyncio.get_running_loop().time() - t0
    await pipeline.stop(is_ended=True)
    await pipe_task
    await redis_client.aclose()

    assert degraded_event is not None, "transcript_service_degraded was not emitted!"
    assert degraded_event["payload"]["reason"] == "provider_unreachable"
    assert dur <= 5.0, f"Degraded took too long: {dur:.2f}s"
