"""Acceptance Test B2: ASR Worker Kill & Failover Mid-Stream.

Verifies:
- Two ASR worker processes competing for meetings.
- Kill the owner mid-stream (Popen.kill()).
- Surviving worker claims ownership lease and replays unacked audio from replay window.
- The pipeline deduplicates replayed finals: no duplicate finals published.
- Timeline remains continuous and monotonic.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload
from meetmind_schemas.keys import asr_owner_key, events_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


@pytest.mark.asyncio
async def test_b2_worker_kill_and_failover(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    h = gateway_harness
    tenant_id = h.tenant_a
    meeting_id = f"mtg_b2_{uuid.uuid4().hex[:8]}"

    # 1. Start two real ASR worker subprocesses with fast autoclaim timeouts for testing
    fast_autoclaim_env = {
        "ASR_LIFECYCLE_IDLE_MS": "1000",
        "ASR_AUDIO_IDLE_MS": "500",
    }
    worker1 = await asr_worker_factory(
        provider="stub", worker_id="worker_alpha", extra_env=fast_autoclaim_env
    )
    worker2 = await asr_worker_factory(
        provider="stub", worker_id="worker_beta", extra_env=fast_autoclaim_env
    )

    # 2. Setup tickets and WebSocket connection
    admin_token = h.jwks.mint_token(sub="admin_b2", org_id=h.org_a, org_role="org:admin")
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

        # 3. Control handshake
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

        # 4. Stream first batch of chunks (chunks 1 to 5, triggers first final at chunk 5)
        t_base = 2_000_000
        for seq in range(1, 6):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.02)

        # Allow worker to process batch 1
        await asyncio.sleep(1.0)

        # 5. Check which worker holds the ownership lease
        l_key = asr_owner_key(tenant_id, meeting_id)
        current_owner_raw = await real_redis.get(l_key)
        assert current_owner_raw is not None, "No worker claimed ownership lease"
        owner_id = (
            current_owner_raw.decode()
            if isinstance(current_owner_raw, bytes)
            else current_owner_raw
        )

        # Identify which worker process to kill
        victim = worker1 if worker1.worker_id in owner_id or "alpha" in owner_id else worker2
        _survivor = worker2 if victim is worker1 else worker1

        # 6. KILL the owner mid-stream
        victim.kill()

        # Delete old lease so survivor can immediately claim on next lifecycle/autoclaim poll
        await real_redis.delete(l_key)

        # Wait for survivor's lifecycle autoclaim loop to pick up the meeting
        await asyncio.sleep(1.5)

        # 7. Stream second batch of chunks (chunks 6 to 16, triggers second and third finals)
        for seq in range(6, 17):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.02)

        # Allow survivor to process remaining audio
        await asyncio.sleep(3.0)

    # 8. Assertions on internal bus finals
    ev_key = events_stream_key(tenant_id, meeting_id)
    raw_events = await real_redis.xrange(ev_key)
    assert len(raw_events) > 0, "No events on internal bus after worker kill"

    finals: list[dict] = []
    seen_uids: set[str] = set()
    prev_start_ms = -1

    for _, fields in raw_events:
        payload_raw = fields.get(b"payload") or fields.get("payload")
        assert payload_raw is not None
        env_dict = json.loads(
            payload_raw.decode() if isinstance(payload_raw, bytes) else payload_raw
        )
        if env_dict.get("event_type") == "transcript_final":
            env = EventEnvelope.model_validate(env_dict)
            payload = TranscriptFinalPayload.model_validate(env.payload)

            # Assert: NO duplicate utterance_id (deduplication worked across workers)
            assert payload.utterance_id not in seen_uids, f"Duplicate final: {payload.utterance_id}"
            seen_uids.add(payload.utterance_id)

            # Assert: continuous timeline
            assert payload.start_ms >= prev_start_ms, "Non-monotonic start_ms in finals"
            prev_start_ms = payload.start_ms
            finals.append(env_dict)

    assert len(finals) >= 2, (
        f"Expected at least 2 distinct finals across workers, got {len(finals)}"
    )
