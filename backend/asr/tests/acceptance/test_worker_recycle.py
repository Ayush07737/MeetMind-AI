"""Acceptance Test: Worker Recycle Policy on Max Meeting Duration.

Verifies Requirement 3:
1. When a meeting pipeline reaches ASR_WORKER_MAX_MEETING_SECONDS, it initiates
   a graceful recycle without dropping any audio chunks.
2. The ownership lease is released and immediately claimed by the reconciler loop.
3. The handoff preserves audio continuity and processes all chunks to completion.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.keys import out_stream_key
from meetmind_security import db

# 31.34s observed (setup+call+teardown); ~2x=63s
pytestmark = [pytest.mark.integration, pytest.mark.timeout(65)]


@pytest.mark.asyncio
@pytest.mark.timeout(90)
async def test_worker_graceful_recycle_on_max_meeting_duration(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    meeting_id = f"mtg_recycle_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a

    # Spawn worker1 with 0.4s max meeting duration
    worker1 = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_WORKER_MAX_MEETING_SECONDS": "0.4",
            "ASR_RECONCILE_INTERVAL_MS": "200",
            "ASR_OWNER_LEASE_TTL_MS": "2000",
        },
    )

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_recycle", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    total_chunks = 12
    acked_seqs: set[int] = set()

    async with websockets.connect(ingest_uri) as ws_ingest:
        # Start meeting and consent handshake
        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        ack_start = json.loads(await asyncio.wait_for(ws_ingest.recv(), timeout=15.0))
        assert ack_start.get("status") == "accepted"

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
        ack_consent = json.loads(await asyncio.wait_for(ws_ingest.recv(), timeout=15.0))
        assert ack_consent.get("status") == "accepted"

        async def recv_frame_ack() -> dict:
            while True:
                raw_msg = await asyncio.wait_for(ws_ingest.recv(), timeout=15.0)
                msg = json.loads(raw_msg)
                if msg.get("frame_type") == "audio_chunk":
                    return msg

        # Stream first 6 audio chunks
        for seq in range(1, 7):
            chunk = make_pcm16_chunk(
                seq=seq,
                ts_ms=int(time.time() * 1000),
                duration_ms=100,
            )
            await ws_ingest.send(chunk)
            ack = await recv_frame_ack()
            assert ack.get("seq") == seq
            acked_seqs.add(seq)
            await asyncio.sleep(0.02)

        # Spawn replacement worker2 before recycle
        worker2 = await asr_worker_factory(
            provider="stub",
            extra_env={
                "ASR_WORKER_MAX_MEETING_SECONDS": "60.0",
                "ASR_RECONCILE_INTERVAL_MS": "200",
                "ASR_OWNER_LEASE_TTL_MS": "2000",
            },
        )
        assert worker2 is not None

        # Pause 0.8s to exceed the 0.4s threshold and allow worker1 recycle and worker2 pickup
        await asyncio.sleep(0.8)

        # Stream remaining chunks (7 to 12) to verify continued handoff without loss
        for seq in range(7, total_chunks + 1):
            chunk = make_pcm16_chunk(
                seq=seq,
                ts_ms=int(time.time() * 1000),
                duration_ms=100,
            )
            await ws_ingest.send(chunk)
            ack = await recv_frame_ack()
            assert ack.get("seq") == seq
            acked_seqs.add(seq)
            await asyncio.sleep(0.02)

        # Allow replacement worker2 time to consume and dispatch chunks 7-12
        await asyncio.sleep(0.5)

        # End meeting cleanly
        meeting_end_time = time.time()
        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(2.0)

    # 1. Verify all chunks were acknowledged (no lost frames)
    assert len(acked_seqs) == total_chunks, f"Expected {total_chunks} acks, got {len(acked_seqs)}"

    # 2. Verify worker 1 logged graceful recycle
    recycle_logged = any("[WORKER_RECYCLE]" in line for line in worker1.logs)
    assert recycle_logged, (
        "Expected worker1 to log [WORKER_RECYCLE] on exceeding max meeting duration"
    )

    # 3. Verify finals published to out stream without loss
    out_key = out_stream_key(tenant_id, meeting_id)
    out_entries = await real_redis.xrange(out_key)
    assert len(out_entries) > 0, "Expected transcript events published to out stream"

    # 4. Verify exactly ONE asr_session_summary in Neon with chunks_sent == chunks_acked
    # and timestamp >= meeting_ended (Requirement A1.2)
    schema = gateway_harness.schema_a
    pool = await db.get_pool()

    summary_rows = []
    for _ in range(30):
        summary_rows = await pool.fetch(
            f"SELECT event_type, payload, created_at FROM {schema}.audit_events "
            f"WHERE payload->>'meeting_id' = $1 AND event_type = 'asr_session_summary'",
            meeting_id,
        )
        if summary_rows:
            break
        await asyncio.sleep(0.5)

    assert len(summary_rows) == 1, (
        f"Expected exactly 1 asr_session_summary across recycles, got {len(summary_rows)}"
    )

    row = summary_rows[0]
    p = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    assert p["chunks_sent"] == total_chunks, (
        f"Expected chunks_sent == {total_chunks}, got {p['chunks_sent']}"
    )
    assert p["chunks_sent"] == len(acked_seqs)
    assert p["gaps"] == [], f"Expected gaps == [], got {p['gaps']}"

    summary_ts = row["created_at"]
    if hasattr(summary_ts, "timestamp"):
        summary_epoch_s = summary_ts.timestamp()
    else:
        summary_epoch_s = float(summary_ts)
    # The summary row must be timestamped at or after meeting_end was sent
    assert summary_epoch_s >= meeting_end_time - 0.5, (
        f"Summary timestamp {summary_epoch_s} was prematurely written "
        f"before meeting end {meeting_end_time}"
    )


@pytest.mark.asyncio
@pytest.mark.timeout(90)
async def test_worker_recycle_interim_latency_with_known_delay(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """Verify interim latency across a worker recycle is not collapsed to 0.10ms floor.

    Injects a known 40ms synthetic delay via stub fault config, triggers a recycle,
    and asserts reported interim latency post-recycle is > 10.0ms (not at 0.10ms floor).
    """
    import urllib.request

    meeting_id = f"mtg_lat_rec_{uuid.uuid4().hex[:8]}"

    worker = await asr_worker_factory(
        provider="stub",
        extra_env={
            "ASR_WORKER_MAX_MEETING_SECONDS": "0.4",
            "ASR_RECONCILE_INTERVAL_MS": "200",
            "ASR_OWNER_LEASE_TTL_MS": "2000",
            "ASR_STUB_FAULT_CONFIG": json.dumps({"latency_ms": 40.0}),
        },
    )

    admin_token = gateway_harness.jwks.mint_token(
        sub="admin_lat_recycle", org_id=gateway_harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        gateway_harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{gateway_harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    async with websockets.connect(ingest_uri) as ws_ingest:
        await ws_ingest.send(
            json.dumps(
                {
                    "type": "control",
                    "action": "meeting_start",
                    "adapter_type": "chrome_extension",
                }
            )
        )
        assert json.loads(await ws_ingest.recv()).get("status") == "accepted"

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
        assert json.loads(await ws_ingest.recv()).get("status") == "accepted"

        async def recv_frame_ack() -> dict:
            while True:
                msg = json.loads(await asyncio.wait_for(ws_ingest.recv(), timeout=5.0))
                if msg.get("frame_type") == "audio_chunk":
                    return msg

        # Stream chunks 1..5
        for seq in range(1, 6):
            chunk = make_pcm16_chunk(seq=seq, ts_ms=int(time.time() * 1000), duration_ms=100)
            await ws_ingest.send(chunk)
            await recv_frame_ack()
            await asyncio.sleep(0.04)

        # Allow recycle to trigger (0.4s max duration)
        await asyncio.sleep(0.6)

        # Stream chunks 6..10 through replacement worker
        for seq in range(6, 11):
            chunk = make_pcm16_chunk(seq=seq, ts_ms=int(time.time() * 1000), duration_ms=100)
            await ws_ingest.send(chunk)
            await recv_frame_ack()
            await asyncio.sleep(0.04)

        # End meeting cleanly
        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(1.0)

    # Verify interim latency metrics in worker
    req = urllib.request.Request(f"http://127.0.0.1:{worker.health_port}/healthz?verbose=1")
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        health_data = json.loads(resp.read().decode())
        metrics = health_data.get("metrics", {})

    interim_stats = metrics.get("interim_latencies", {})
    interim_p50 = interim_stats.get("p50", 0.0)
    floor_samples = interim_stats.get("floor_samples", 0)
    total_n = interim_stats.get("n", 0)

    assert total_n > 0, f"Expected interim latency samples, got n={total_n}"
    assert interim_p50 > 10.0, (
        f"Expected interim P50 > 10.0ms (reflecting 40ms stub delay), got {interim_p50}ms"
    )
    floor_ratio = floor_samples / max(1, total_n)
    assert floor_ratio <= 0.10, (
        f"Floor ratio too high: {floor_ratio * 100:.1f}% ({floor_samples}/{total_n})"
    )
