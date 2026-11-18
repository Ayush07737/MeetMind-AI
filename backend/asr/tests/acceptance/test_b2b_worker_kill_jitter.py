"""Acceptance Test B2b: ASR Worker Kill Mid-Stream with Replay Jitter (P0-3).

Verifies:
- Two ASR worker processes competing for meetings.
- ASR_STUB_REPLAY_JITTER_MS=150 enabled (causes replay jitter and re-segmentation).
- Kill the owner mid-stream (Popen.kill()) with NO manual Redis manipulation.
- Surviving worker takes over via lease expiry plus reconciliation.
- Due to replay window (5s), survivor receives replayed audio chunks with jittered
  timestamps and altered boundaries.
- Deduplicator time-overlap rule catches jittered replays where utterance_id equality
  alone would fail.
- Assert: The final stream contains NO duplicate or overlapping-duplicate text on any channel.
- Assert: start_ms is monotonic.
- Assert: survivor produces finals for subsequent chunks.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
import websockets
from conftest import ServerHarness, exchange_ws_ticket, make_pcm16_chunk
from meetmind_asr.dedupe import normalize_text
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload
from meetmind_schemas.keys import asr_owner_key, events_stream_key

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]


@pytest.mark.asyncio
async def test_b2b_worker_kill_with_jitter_and_overlap_dedupe(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    h = gateway_harness
    tenant_id = h.tenant_a
    meeting_id = f"mtg_b2b_{uuid.uuid4().hex[:8]}"

    lease_ttl_ms = 3000
    reconcile_interval_ms = 1000
    audio_idle_ms = 500
    jitter_ms = 150

    # 1. Start two real ASR worker subprocesses with jitter enabled
    jitter_env = {
        "ASR_OWNER_LEASE_TTL_MS": str(lease_ttl_ms),
        "ASR_RECONCILE_INTERVAL_MS": str(reconcile_interval_ms),
        "ASR_AUDIO_IDLE_MS": str(audio_idle_ms),
        "ASR_STUB_REPLAY_JITTER_MS": str(jitter_ms),
    }
    worker1 = await asr_worker_factory(
        provider="stub", worker_id="worker_jitter_alpha", extra_env=jitter_env
    )
    worker2 = await asr_worker_factory(
        provider="stub", worker_id="worker_jitter_beta", extra_env=jitter_env
    )

    # 2. Setup tickets and WebSocket connection
    admin_token = h.jwks.mint_token(sub="admin_b2b", org_id=h.org_a, org_role="org:admin")
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
        t_base = 3_000_000
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

        # 6. KILL the owner mid-stream
        t_kill = time.monotonic()
        victim.kill()

        # 7. Wait for survivor to take over via lease expiry plus reconciliation
        max_takeover_wait = (lease_ttl_ms + reconcile_interval_ms) / 1000.0 + 2.0
        taken_over = False
        while time.monotonic() - t_kill < max_takeover_wait:
            lease_val = await real_redis.get(l_key)
            if lease_val:
                holder = lease_val.decode() if isinstance(lease_val, bytes) else str(lease_val)
                if survivor.worker_id in holder:
                    taken_over = True
                    break
            await asyncio.sleep(0.05)

        assert taken_over, (
            f"Survivor {survivor.worker_id} did not take over within {max_takeover_wait}s"
        )

        # 8. Stream second batch of chunks (chunks 6 to 18)
        for seq in range(6, 19):
            frame = make_pcm16_chunk(seq=seq, ts_ms=t_base + seq * 100)
            await ws_ingest.send(frame)
            ack = await recv_ack()
            assert ack.get("status") in ("accepted", "acked")
            await asyncio.sleep(0.02)

        # Deterministically poll until expected finals arrive on internal bus
        t_poll_finals = time.monotonic()
        finals_payloads: list[TranscriptFinalPayload] = []

        while time.monotonic() - t_poll_finals < 10.0:
            raw_events = await real_redis.xrange(ev_key)
            finals_payloads = []

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
                    finals_payloads.append(payload)

            if len(finals_payloads) >= 2:
                break
            await asyncio.sleep(0.1)

    assert len(finals_payloads) >= 2, (
        f"Expected at least 2 distinct finals across workers, got {len(finals_payloads)}"
    )

    # 9. Strict P0-3 deduplication checks:
    # (a) No duplicate utterance_id
    uids = [p.utterance_id for p in finals_payloads]
    assert len(uids) == len(set(uids)), f"Duplicate utterance_id found in finals: {uids}"

    # (b) Continuous timeline on each channel
    by_channel: dict[int, list[TranscriptFinalPayload]] = {}
    for p in finals_payloads:
        ch = p.channel if p.channel is not None else 0
        by_channel.setdefault(ch, []).append(p)

    for ch, ch_finals in by_channel.items():
        prev_start = -1
        for f in ch_finals:
            assert f.start_ms >= prev_start, (
                f"Non-monotonic start_ms on channel {ch}: {f.start_ms} < {prev_start}"
            )
            prev_start = f.start_ms

        # (c) Pairwise overlap check: no duplicate or overlapping-duplicate text
        for i in range(len(ch_finals)):
            for j in range(i + 1, len(ch_finals)):
                f1 = ch_finals[i]
                f2 = ch_finals[j]
                norm1 = normalize_text(f1.text)
                norm2 = normalize_text(f2.text)

                overlap_start = max(f1.start_ms, f2.start_ms)
                overlap_end = min(f1.end_ms, f2.end_ms)

                if overlap_start < overlap_end:
                    assert norm1 != norm2, (
                        f"Overlapping duplicate text on channel {ch}: '{norm1}' at "
                        f"[{f1.start_ms}, {f1.end_ms}] and [{f2.start_ms}, {f2.end_ms}]"
                    )
                    assert norm1 not in norm2 and norm2 not in norm1, (
                        f"Overlapping substring duplicate on channel {ch}: '{norm1}' vs '{norm2}'"
                    )
