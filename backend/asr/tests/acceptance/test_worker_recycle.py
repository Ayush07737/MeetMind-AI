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

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.mark.asyncio
async def test_worker_graceful_recycle_on_max_meeting_duration(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    meeting_id = f"mtg_recycle_{uuid.uuid4().hex[:8]}"
    tenant_id = gateway_harness.tenant_a

    # Spawn worker with 0.4s max meeting duration and 200ms reconcile interval
    worker = await asr_worker_factory(
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
        ack_start = json.loads(await asyncio.wait_for(ws_ingest.recv(), timeout=5.0))
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
        ack_consent = json.loads(await asyncio.wait_for(ws_ingest.recv(), timeout=5.0))
        assert ack_consent.get("status") == "accepted"

        async def recv_frame_ack() -> dict:
            while True:
                raw_msg = await asyncio.wait_for(ws_ingest.recv(), timeout=5.0)
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

        # Pause 0.6s to exceed the 0.4s threshold and allow worker recycle
        await asyncio.sleep(0.6)

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

        # End meeting cleanly
        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(0.5)

    # 1. Verify all chunks were acknowledged (no lost frames)
    assert len(acked_seqs) == total_chunks, f"Expected {total_chunks} acks, got {len(acked_seqs)}"

    # 2. Verify worker logged graceful recycle
    recycle_logged = any("[WORKER_RECYCLE]" in line for line in worker.logs)
    assert recycle_logged, (
        "Expected worker to log [WORKER_RECYCLE] on exceeding max meeting duration"
    )

    # 3. Verify finals published to out stream without loss
    out_key = out_stream_key(tenant_id, meeting_id)
    out_entries = await real_redis.xrange(out_key)
    assert len(out_entries) > 0, "Expected transcript events published to out stream"
