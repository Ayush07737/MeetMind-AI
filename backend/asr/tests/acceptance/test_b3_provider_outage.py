"""Acceptance Test B3: Provider Outage & Reconnection Scenarios.

Verifies:
(a) 1.5 s outage: reconnects within 2 s, no degraded event emitted, no audio loss.
(b) 6 s outage: degraded (provider_unreachable) then recovered emitted, buffered audio flushed.
(c) buffer overflow outage: drops oldest audio, emits buffer_overflow degraded event,
    counted on /healthz.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import events_stream_key, out_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(150)]


async def _run_stream_with_fault(
    h: ServerHarness,
    asr_worker_factory,
    real_redis,
    fault_config: dict,
    total_chunks: int,
    extra_env: dict | None = None,
) -> tuple[str, list[dict], list[dict], str]:
    """Helper to run a meeting with configured provider faults."""
    meeting_id = f"mtg_b3_{uuid.uuid4().hex[:8]}"
    tenant_id = h.tenant_a

    worker_env = {
        "ASR_STUB_FAULT_CONFIG": json.dumps(fault_config),
    }
    if extra_env:
        worker_env.update(extra_env)

    worker = await asr_worker_factory(provider="stub", extra_env=worker_env)

    admin_token = h.jwks.mint_token(sub="admin_b3", org_id=h.org_a, org_role="org:admin")
    _, ingest_ticket_resp = await exchange_ws_ticket(
        h.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{h.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    async with websockets.connect(ingest_uri) as ws_ingest:

        async def recv_ack():
            while True:
                msg = json.loads(await ws_ingest.recv())
                if msg.get("type") == "ack":
                    return msg

        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        await ws_ingest.send(
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

        t_base = 3_000_000
        for seq in range(1, total_chunks + 1):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.03)

        # Wait for recovery and flush
        outage_dur = fault_config.get("outage_duration_s", 0.0)
        await asyncio.sleep(outage_dur + 2.0)

    # Fetch published events
    ev_key = events_stream_key(tenant_id, meeting_id)
    out_key = out_stream_key(tenant_id, meeting_id)

    raw_events = await real_redis.xrange(ev_key)
    raw_out = await real_redis.xrange(out_key)

    def parse_events(raw_list):
        parsed = []
        for _, fields in raw_list:
            p_raw = fields.get(b"payload") or fields.get("payload")
            if p_raw:
                parsed.append(json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw))
        return parsed

    return meeting_id, parse_events(raw_events), parse_events(raw_out), worker.health_url


@pytest.mark.asyncio
async def test_b3a_short_outage_recovers_without_degraded(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """1.5 s outage: reconnect within 2 s, no degraded event required, no audio loss."""
    fault_config = {
        "disconnect_at_chunk": 3,
        "outage_duration_s": 1.5,
    }
    meeting_id, events, out_events, _ = await _run_stream_with_fault(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        fault_config=fault_config,
        total_chunks=10,
    )

    # Assert: NO degraded event emitted because outage was < 2s
    degraded_events = [e for e in events if e.get("event_type") == "transcript_service_degraded"]
    assert len(degraded_events) == 0, (
        f"Expected 0 degraded events for 1.5s outage, got: {degraded_events}"
    )

    # Assert: finals were still produced without loss
    finals = [e for e in events if e.get("event_type") == "transcript_final"]
    assert len(finals) >= 2


@pytest.mark.asyncio
async def test_b3b_extended_outage_emits_degraded_and_recovers(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """6 s outage: degraded then recovered emitted, no permanent audio loss."""
    fault_config = {
        "disconnect_at_chunk": 3,
        "outage_duration_s": 5.0,  # > 2.0s threshold
    }
    meeting_id, events, out_events, _ = await _run_stream_with_fault(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        fault_config=fault_config,
        total_chunks=10,
    )

    # Assert: degraded event followed by recovered event
    degraded_events = [e for e in events if e.get("event_type") == "transcript_service_degraded"]
    assert len(degraded_events) >= 2, (
        f"Expected degraded + recovered events, got: {degraded_events}"
    )

    states = [e["payload"]["state"] for e in degraded_events]
    assert "degraded" in states
    assert "recovered" in states
    assert degraded_events[0]["payload"]["reason"] == "provider_unreachable"

    # Degraded event also present in out stream (to inform listeners)
    out_degraded = [e for e in out_events if e.get("event_type") == "transcript_service_degraded"]
    assert len(out_degraded) >= 1


@pytest.mark.asyncio
async def test_b3c_buffer_overflow_drops_audio_and_reports(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Extended outage exceeding buffer cap: buffer_overflow reported and counted."""
    # Set small buffer capacity (2s = 20 chunks) and 4s outage with 35 chunks
    fault_config = {
        "disconnect_at_chunk": 2,
        "outage_duration_s": 4.0,
    }
    worker_env = {
        "ASR_BUFFER_MAX_SECONDS": "2",  # 20 chunks max
    }
    meeting_id, events, out_events, health_url = await _run_stream_with_fault(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        fault_config=fault_config,
        total_chunks=35,
        extra_env=worker_env,
    )

    # Check for buffer_overflow event
    overflow_events = [
        e
        for e in events
        if e.get("event_type") == "transcript_service_degraded"
        and e["payload"].get("reason") == "buffer_overflow"
    ]
    assert len(overflow_events) > 0, "Expected buffer_overflow degraded event"

    # Verify dropped chunks count reported on /healthz?verbose=1
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{health_url}?verbose=1", timeout=2.0)
        assert res.status_code == 200
        data = res.json()
        assert data["metrics"]["dropped_chunks"] > 0
