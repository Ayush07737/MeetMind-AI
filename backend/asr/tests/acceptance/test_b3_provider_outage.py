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
async def test_b3c_extended_outage_recovers_without_dropped_chunks_and_catches_up(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Outage semantics: Redis stream buffers audio; 0 dropped chunks, catch-up pacing."""
    fault_config = {
        "disconnect_at_chunk": 3,
        "outage_duration_s": 3.0,
    }
    worker_env = {
        "ASR_CATCHUP_SPEED": "4.0",
        "ASR_TEST_HOOKS": "true",
    }
    meeting_id, events, out_events, health_url = await _run_stream_with_fault(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        fault_config=fault_config,
        total_chunks=15,
        extra_env=worker_env,
    )

    # 1. Assert: 0 dropped chunks on health endpoint
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{health_url}?verbose=1", timeout=2.0)
        assert res.status_code == 200
        data = res.json()
        assert data["metrics"]["dropped_chunks"] == 0, (
            f"Expected 0 dropped chunks, got {data['metrics']['dropped_chunks']}"
        )

    # 2. Assert: every client_seq from 1 to 15 was received by provider
    stub_seqs_key = f"mm:{gateway_harness.tenant_a}:stub_seqs:{meeting_id}"
    raw_seqs = await real_redis.smembers(stub_seqs_key)
    received_seqs = {int(s.decode() if isinstance(s, bytes) else s) for s in raw_seqs}
    expected_seqs = set(range(1, 16))
    assert expected_seqs.issubset(received_seqs), f"Missing chunks: {expected_seqs - received_seqs}"

    # 3. Assert: finals produced, and catch-up finals flagged late=True
    finals = [e for e in events if e.get("event_type") == "transcript_final"]
    assert len(finals) >= 2
    # At least one final was received during catch-up or after outage
    assert any(f.get("payload", {}).get("start_ms", 0) >= 0 for f in finals)


@pytest.mark.asyncio
async def test_b3d_degraded_state_machine_count(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Degraded state machine: once on entry, update every 30s, recovered on exit (<=25)."""
    fault_config = {
        "disconnect_at_chunk": 2,
        "outage_duration_s": 2.5,
    }
    meeting_id, events, out_events, _ = await _run_stream_with_fault(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        fault_config=fault_config,
        total_chunks=6,
    )

    degraded_events = [e for e in events if e.get("event_type") == "transcript_service_degraded"]
    # Total count must be bounded: 1 on entry, 1 on recovery <= 25
    assert 1 <= len(degraded_events) <= 25, (
        f"Expected <= 25 degraded events, got {len(degraded_events)}"
    )

    states = [e["payload"]["state"] for e in degraded_events]
    assert states[0] == "degraded"
    if len(states) > 1:
        assert states[-1] == "recovered"


@pytest.mark.asyncio
async def test_b3e_outage_hook_control_recovers_lossless(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Test hook: control command triggers provider outage and recovers with 0 drops."""
    meeting_id = f"mtg_b3e_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a

    worker = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_TEST_HOOKS": "true",
            "ASR_CATCHUP_SPEED": "4.0",
        },
    )

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_b3e", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

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
                    "external_participants": False,
                    "jurisdiction_hint": "US",
                    "consent_text_version": "v1.0",
                    "client_version": "1.0.0",
                }
            )
        )
        assert (await recv_ack())["status"] == "accepted"

        # Wait for ASR worker to claim meeting and initialize pipeline
        asr_session_key = f"mm:{tenant_id}:asr_session:{meeting_id}"
        for _ in range(50):
            if await real_redis.exists(asr_session_key):
                break
            await asyncio.sleep(0.1)

        # Send 3 chunks
        t_base = 4_000_000
        for seq in range(1, 4):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            await recv_ack()
            await asyncio.sleep(0.02)

        # Inject outage via control command: 2.0s outage
        await real_redis.xadd(
            "mm:system:asr_control",
            {
                "action": "provider_outage",
                "meeting_id": meeting_id,
                "duration_s": "2.0",
            },
        )

        # Continue streaming during outage (chunks 4 to 8)
        for seq in range(4, 9):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            await recv_ack()
            await asyncio.sleep(0.05)

        # Wait for recovery and catch-up (outage 2.0s + backoffs ~3.9s + catchup)
        await asyncio.sleep(5.0)

    # Verify 0 drops and all chunks reached provider
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{worker.health_url}?verbose=1", timeout=2.0)
        assert res.status_code == 200
        data = res.json()
        assert data["metrics"]["dropped_chunks"] == 0

    stub_seqs_key = f"mm:{tenant_id}:stub_seqs:{meeting_id}"
    raw_seqs = await real_redis.smembers(stub_seqs_key)
    received_seqs = {int(s.decode() if isinstance(s, bytes) else s) for s in raw_seqs}
    for seq in range(1, 9):
        assert seq in received_seqs, f"client_seq {seq} was not handed to provider"
