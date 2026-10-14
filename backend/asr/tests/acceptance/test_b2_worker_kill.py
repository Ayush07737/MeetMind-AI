"""Acceptance Test B2: ASR Worker Kill & Failover Mid-Stream.

Verifies:
- Two ASR worker processes competing for meetings.
- Kill the owner mid-stream (Popen.kill()) with NO manual Redis manipulation.
- Surviving worker takes over via lease expiry plus reconciliation.
- Takeover timing strictly within: lease TTL + reconcile interval + 2s.
- The stub's received client_seq set covers every sent chunk (duplicates only inside replay window).
- No duplicate utterance_id across workers.
- start_ms monotonic timeline.
- Survivor produces finals for post-kill chunks.
- Deterministic waits with polling.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import Counter

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

    lease_ttl_ms = 3000
    reconcile_interval_ms = 1000
    audio_idle_ms = 500

    # 1. Start two real ASR worker subprocesses with configurable lease TTL & reconcile interval
    fast_env = {
        "ASR_OWNER_LEASE_TTL_MS": str(lease_ttl_ms),
        "ASR_RECONCILE_INTERVAL_MS": str(reconcile_interval_ms),
        "ASR_AUDIO_IDLE_MS": str(audio_idle_ms),
    }
    worker1 = await asr_worker_factory(
        provider="stub", worker_id="worker_alpha", extra_env=fast_env
    )
    worker2 = await asr_worker_factory(provider="stub", worker_id="worker_beta", extra_env=fast_env)

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

        # Deterministically poll until first worker claims lease
        l_key = asr_owner_key(tenant_id, meeting_id)
        t_claim_start = time.monotonic()
        current_owner_raw = None
        while time.monotonic() - t_claim_start < 5.0:
            current_owner_raw = await real_redis.get(l_key)
            if current_owner_raw:
                break
            await asyncio.sleep(0.05)
        assert current_owner_raw is not None, "No worker claimed initial ownership lease"

        owner_id = (
            current_owner_raw.decode()
            if isinstance(current_owner_raw, bytes)
            else current_owner_raw
        )

        # Deterministically poll until first final arrives on internal bus
        ev_key = events_stream_key(tenant_id, meeting_id)
        t_f1_start = time.monotonic()
        first_final_arrived = False
        while time.monotonic() - t_f1_start < 6.0:
            events = await real_redis.xrange(ev_key)
            for _, fields in events:
                p_raw = fields.get(b"payload") or fields.get("payload")
                if p_raw:
                    env_p = json.loads(p_raw.decode() if isinstance(p_raw, bytes) else p_raw)
                    if env_p.get("event_type") == "transcript_final":
                        first_final_arrived = True
                        break
            if first_final_arrived:
                break
            await asyncio.sleep(0.05)
        assert first_final_arrived, "Initial owner did not produce first final before kill"

        # 5. Identify victim and survivor
        victim = worker1 if worker1.worker_id in owner_id or "alpha" in owner_id else worker2
        survivor = worker2 if victim is worker1 else worker1

        # 6. KILL the owner mid-stream (NO manual Redis manipulation!)
        t_kill = time.monotonic()
        victim.kill()

        # 7. Wait for survivor to take over via lease expiry plus reconciliation
        # Assert: takeover within lease TTL + reconcile interval + 2 s
        # 3.0 + 1.0 + 2.0 = 6.0 s
        max_takeover_wait = (lease_ttl_ms + reconcile_interval_ms) / 1000.0 + 2.0
        taken_over = False
        current_lease_holder = None
        while time.monotonic() - t_kill < max_takeover_wait:
            lease_val = await real_redis.get(l_key)
            if lease_val:
                current_lease_holder = (
                    lease_val.decode() if isinstance(lease_val, bytes) else str(lease_val)
                )
                if survivor.worker_id in current_lease_holder:
                    taken_over = True
                    break
            await asyncio.sleep(0.05)

        takeover_elapsed = time.monotonic() - t_kill
        assert taken_over, (
            f"Survivor {survivor.worker_id} did not take over within {max_takeover_wait:.2f}s "
            f"(elapsed={takeover_elapsed:.2f}s, lease_val={current_lease_holder})"
        )
        assert takeover_elapsed <= max_takeover_wait, (
            f"Takeover took {takeover_elapsed:.2f}s exceeding budget of {max_takeover_wait:.2f}s"
        )

        # 8. Stream second batch of chunks (chunks 6 to 16, triggers subsequent finals)
        for seq in range(6, 17):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.02)

        # Deterministically poll until all expected finals arrive on internal bus
        t_poll_finals = time.monotonic()
        finals: list[dict] = []
        seen_uids: set[str] = set()
        prev_start_ms = -1

        while time.monotonic() - t_poll_finals < 10.0:
            raw_events = await real_redis.xrange(ev_key)
            finals = []
            seen_uids = set()
            prev_start_ms = -1
            valid = True

            for _, fields in raw_events:
                payload_raw = fields.get(b"payload") or fields.get("payload")
                if not payload_raw:
                    continue
                env_dict = json.loads(
                    payload_raw.decode() if isinstance(payload_raw, bytes) else payload_raw
                )
                if env_dict.get("event_type") == "transcript_final":
                    env = EventEnvelope.model_validate(env_dict)
                    payload = TranscriptFinalPayload.model_validate(env.payload)

                    # Assert: NO duplicate utterance_id
                    if payload.utterance_id in seen_uids:
                        valid = False
                    seen_uids.add(payload.utterance_id)

                    # Assert: continuous timeline
                    if payload.start_ms < prev_start_ms:
                        valid = False
                    prev_start_ms = payload.start_ms

                    finals.append(env_dict)

            if valid and len(finals) >= 2:
                break
            await asyncio.sleep(0.1)

    # 9. Assertions on internal bus finals
    assert len(finals) >= 2, (
        f"Expected at least 2 distinct finals across workers, got {len(finals)}"
    )

    # 10. Assert: the stub's received client_seq set covers every sent chunk
    s_key = f"mm:{tenant_id}:stub_seqs:{meeting_id}"
    raw_seqs = await real_redis.smembers(s_key)
    received_set = {int(s) for s in raw_seqs}
    expected_all = set(range(1, 17))
    assert expected_all.issubset(received_set), (
        f"Received client_seq set does not cover all sent chunks. "
        f"Missing: {expected_all - received_set}, Received: {received_set}"
    )

    # Assert: duplicates only inside the replay window (chunks 1..5)
    l_key_seqs = f"mm:{tenant_id}:stub_seqs_list:{meeting_id}"
    raw_list = await real_redis.lrange(l_key_seqs, 0, -1)
    received_list = [int(s) for s in raw_list]
    counts = Counter(received_list)
    for seq in range(6, 17):
        assert counts[seq] == 1, (
            f"Post-kill chunk {seq} was received {counts[seq]} times (expected exactly 1)!"
        )
