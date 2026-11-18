"""Acceptance Test B7: Tenant Isolation & Multi-Tenancy.

Verifies:
1. Two distinct tenants streaming concurrently.
2. Events stay strictly isolated on each tenant's own Redis keys:
   - mm:{tenant_a}:events:{meeting_a}
   - mm:{tenant_b}:events:{meeting_b}
   No cross-contamination or leak across tenant streams.
3. The stub records that each tenant's keyterms were applied only to its own session.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import uuid
from pathlib import Path

import httpx
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import events_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


@pytest.mark.asyncio
async def test_b7_tenant_isolation_concurrent(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Stream two tenants concurrently; assert key isolation and per-tenant keyterm binding."""
    tenant_a = gateway_harness.tenant_a
    tenant_b = gateway_harness.tenant_b
    org_a = gateway_harness.org_a
    org_b = gateway_harness.org_b

    mtg_a = f"mtg_b7_a_{uuid.uuid4().hex[:8]}"
    mtg_b = f"mtg_b7_b_{uuid.uuid4().hex[:8]}"

    # Setup tenant config file
    temp_dir = Path(tempfile.gettempdir())
    cfg_file = temp_dir / f"asr_tenant_cfg_{uuid.uuid4().hex[:8]}.json"
    sessions_file = temp_dir / f"asr_stub_sessions_{uuid.uuid4().hex[:8]}.json"

    tenant_cfg = {
        "tenants": {
            tenant_a: {
                "language": "en-US",
                "keyterms": ["AlphaCorp", "QuarterlyTarget"],
            },
            tenant_b: {
                "language": "en-IN",
                "keyterms": ["BetaSystems", "Roadmap2026"],
            },
        }
    }
    cfg_file.write_text(json.dumps(tenant_cfg), encoding="utf-8")

    worker = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_TENANT_CONFIG_PATH": str(cfg_file),
            "ASR_STUB_SESSIONS_FILE": str(sessions_file),
        },
    )

    # Mint admin tokens for org_a and org_b
    token_a = gateway_harness.jwks.mint_token(sub="admin_a", org_id=org_a, org_role="org:admin")
    token_b = gateway_harness.jwks.mint_token(sub="admin_b", org_id=org_b, org_role="org:admin")

    _, resp_a = await exchange_ws_ticket(gateway_harness.http_url, token_a, mtg_a, role="ingest")
    _, resp_b = await exchange_ws_ticket(gateway_harness.http_url, token_b, mtg_b, role="ingest")

    uri_a = f"{gateway_harness.ws_url}/v1/meetings/{mtg_a}/stream?ticket={resp_a['ticket']}"
    uri_b = f"{gateway_harness.ws_url}/v1/meetings/{mtg_b}/stream?ticket={resp_b['ticket']}"

    async def _stream_tenant(uri: str, mtg: str, t_base: int):
        async with websockets.connect(uri) as ws:

            async def recv_ack():
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("type") == "ack":
                        return msg

            await ws.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "meeting_start",
                        "adapter_type": "chrome_extension",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            await ws.send(
                json.dumps(
                    {
                        "type": "control",
                        "action": "consent_confirmed",
                        "consent_type": "audio_capture",
                        "external_participants": True,
                        "jurisdiction_hint": "GDPR",
                        "consent_text_version": "v2.0",
                        "client_version": "2.1.0",
                    }
                )
            )
            assert (await recv_ack())["status"] == "accepted"

            for seq in range(1, 8):
                frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
                await ws.send(frame)
                ack = await recv_ack()
                assert ack.get("status") in ("accepted", "acked")
                await asyncio.sleep(0.04)

            # End meeting
            await ws.send(json.dumps({"type": "control", "action": "meeting_end"}))
            assert (await recv_ack())["status"] == "accepted"

    # Stream both concurrently
    await asyncio.gather(
        _stream_tenant(uri_a, mtg_a, 7_000_000),
        _stream_tenant(uri_b, mtg_b, 8_000_000),
    )

    # Wait for processing
    await asyncio.sleep(2.0)

    # Verify Redis streams:
    # 1. Tenant A events stream contains events for mtg_a and tenant_a ONLY
    ev_key_a = events_stream_key(tenant_a, mtg_a)
    raw_a = await real_redis.xrange(ev_key_a)
    assert len(raw_a) > 0, "Expected events in tenant_a stream"
    for _, fields in raw_a:
        p_raw = fields.get(b"payload") or fields.get("payload")
        event = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
        assert event["tenant_id"] == tenant_a
        assert event["meeting_id"] == mtg_a

    # 2. Tenant B events stream contains events for mtg_b and tenant_b ONLY
    ev_key_b = events_stream_key(tenant_b, mtg_b)
    raw_b = await real_redis.xrange(ev_key_b)
    assert len(raw_b) > 0, "Expected events in tenant_b stream"
    for _, fields in raw_b:
        p_raw = fields.get(b"payload") or fields.get("payload")
        event = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
        assert event["tenant_id"] == tenant_b
        assert event["meeting_id"] == mtg_b

    # 3. Assert cross-keys do NOT exist or have zero items
    cross_key_1 = events_stream_key(tenant_a, mtg_b)
    cross_key_2 = events_stream_key(tenant_b, mtg_a)
    assert len(await real_redis.xrange(cross_key_1)) == 0
    assert len(await real_redis.xrange(cross_key_2)) == 0

    # 4. Verify /healthz?verbose=1 returns aggregate metrics without tenant IDs or text
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{worker.health_url}?verbose=1", timeout=2.0)
        assert res.status_code == 200
        health_data = res.json()
        assert "metrics" in health_data
        health_text = json.dumps(health_data)
        assert tenant_a not in health_text
        assert tenant_b not in health_text
        assert mtg_a not in health_text
        assert mtg_b not in health_text
        assert "text" not in health_data.get("metrics", {})

    # 5. Verify exact per-tenant keyterms and language recorded on tenant-isolated Redis keys
    raw_sess_a = await real_redis.get(f"mm:{tenant_a}:asr_session:{mtg_a}")
    assert raw_sess_a is not None, "Missing tenant A session record in Redis"
    sess_a = json.loads(raw_sess_a.decode("utf-8") if isinstance(raw_sess_a, bytes) else raw_sess_a)
    assert sess_a["tenant_id"] == tenant_a
    assert sess_a["keyterms"] == ["AlphaCorp", "QuarterlyTarget"]
    assert sess_a["language"] == "en-US"

    raw_sess_b = await real_redis.get(f"mm:{tenant_b}:asr_session:{mtg_b}")
    assert raw_sess_b is not None, "Missing tenant B session record in Redis"
    sess_b = json.loads(raw_sess_b.decode("utf-8") if isinstance(raw_sess_b, bytes) else raw_sess_b)
    assert sess_b["tenant_id"] == tenant_b
    assert sess_b["keyterms"] == ["BetaSystems", "Roadmap2026"]
    assert sess_b["language"] == "en-IN"

    # Cross keys must NOT exist
    assert await real_redis.get(f"mm:{tenant_a}:asr_session:{mtg_b}") is None
    assert await real_redis.get(f"mm:{tenant_b}:asr_session:{mtg_a}") is None

    # Clean up temp file
    cfg_file.unlink(missing_ok=True)
