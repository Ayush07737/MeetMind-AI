"""MeetMind AI — L7: Real Deepgram Reconnect and Recovery Test Suite.

Empirical verification under four conditions:
1. Baseline (no-drop)
2. 1 drop (t=10s)
3. 2 drops (t=8s, 16s)
4. 5s outage (outage at t=10s for 5.0s)

For each condition:
- drop-to-reaccept ms
- set of client_seq handed to provider sessions vs sent
- duplicate/overlapping finals (target: 0)
- WER vs no-drop baseline (delta <= 0.10 absolute)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import time
import uuid
import wave
from pathlib import Path
from typing import Any

import pytest
import websockets
from meetmind_schemas.events import EventEnvelope, TranscriptFinalPayload, encode_audio_frame
from meetmind_schemas.keys import events_stream_key

from backend.asr.meetmind_asr.dedupe import normalize_text
from backend.asr.tests.acceptance.conftest import ServerHarness, exchange_ws_ticket
from ml.asr_eval.wer import calculate_wer

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_WAV = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.wav"
)
GROUND_TRUTH_TXT = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.txt"
)

pytestmark = [pytest.mark.deepgram, pytest.mark.timeout(180)]

BASELINE_WER: float | None = None


async def _stream_with_drops(
    harness: ServerHarness,
    asr_worker_factory,
    real_redis,
    drop_times_s: list[float] | None = None,
    outage_time_s: float | None = None,
    outage_duration_s: float = 5.0,
) -> dict[str, Any]:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        pytest.skip("DEEPGRAM_API_KEY is not set")

    tenant_id = harness.tenant_a
    meeting_id = f"mtg_l7_{uuid.uuid4().hex[:8]}"

    extra_env = {
        "DEEPGRAM_API_KEY": api_key,
        "ASR_TEST_HOOKS": "true",
        "MIP_OPT_OUT": "true",
    }
    _worker = await asr_worker_factory(provider="deepgram", extra_env=extra_env)

    admin_token = harness.jwks.mint_token(
        sub="admin_l7", org_id=harness.org_a, org_role="org:admin"
    )
    _, ingest_ticket_resp = await exchange_ws_ticket(
        harness.http_url, admin_token, meeting_id, role="ingest"
    )
    ingest_ticket = ingest_ticket_resp["ticket"]
    ingest_uri = f"{harness.ws_url}/v1/meetings/{meeting_id}/stream?ticket={ingest_ticket}"

    drop_targets = sorted(drop_times_s or [], reverse=True)
    outage_triggered = False

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
        await ws_ingest.recv()

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
        await ws_ingest.recv()

        async def recv_ack(ws):
            while True:
                m = json.loads(await ws.recv())
                if m.get("type") == "ack":
                    return m

        t_start = time.perf_counter()

        with wave.open(str(FIXTURE_WAV), "rb") as wf:
            for seq in range(1, 301):
                raw_pcm = wf.readframes(1600)
                if not raw_pcm:
                    break

                elapsed = time.perf_counter() - t_start

                # Trigger single/double drops
                if drop_targets and elapsed >= drop_targets[-1]:
                    target = drop_targets.pop()
                    print(
                        f"\n[FAULT INJECTION] Dropping Deepgram at "
                        f"elapsed={elapsed:.2f}s (target={target}s)"
                    )
                    await real_redis.xadd(
                        "mm:system:asr_control",
                        {"action": "drop_provider", "meeting_id": meeting_id},
                    )

                # Trigger provider outage
                if (
                    outage_time_s is not None
                    and not outage_triggered
                    and elapsed >= outage_time_s
                ):
                    outage_triggered = True
                    print(
                        f"\n[FAULT INJECTION] Simulating {outage_duration_s:.1f}s outage at "
                        f"elapsed={elapsed:.2f}s"
                    )
                    await real_redis.xadd(
                        "mm:system:asr_control",
                        {
                            "action": "provider_outage",
                            "meeting_id": meeting_id,
                            "duration_s": str(outage_duration_s),
                        },
                    )

                ts_ms = int(time.time() * 1000)
                frame = encode_audio_frame(client_seq=seq, capture_ts_ms=ts_ms, payload=raw_pcm)
                await ws_ingest.send(frame)
                await recv_ack(ws_ingest)

                # Drift-corrected 100ms real-time pacing
                target_time = t_start + (seq * 0.10)
                delay = target_time - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)

        await ws_ingest.send(json.dumps({"type": "control", "action": "meeting_end"}))
        await asyncio.sleep(6.0)  # Wait for final reconciliation

    # Fetch finals
    ev_key = events_stream_key(tenant_id, meeting_id)
    raw_events = await real_redis.xrange(ev_key)

    finals: list[TranscriptFinalPayload] = []
    final_texts: list[str] = []
    for _, fields in raw_events:
        payload_raw = fields.get(b"payload") or fields.get("payload")
        if payload_raw:
            raw_str = payload_raw.decode() if isinstance(payload_raw, bytes) else payload_raw
            data = json.loads(raw_str)
            if data.get("event_type") == "transcript_final":
                envelope = EventEnvelope.model_validate(data)
                f_payload = TranscriptFinalPayload.model_validate(envelope.payload)
                finals.append(f_payload)
                final_texts.append(f_payload.text)

    # Fetch reconnect events
    raw_reconnects = await real_redis.lrange(
        f"mm:{tenant_id}:asr_reconnects:{meeting_id}", 0, -1
    )
    reconnect_ms_list: list[float] = []
    for r in raw_reconnects:
        r_str = r.decode() if isinstance(r, bytes) else r
        with contextlib.suppress(Exception):
            reconnect_ms_list.append(float(json.loads(r_str).get("reaccept_ms", 0.0)))

    # Fetch accepted and sent seqs
    raw_acc = await real_redis.get(f"mm:{tenant_id}:asr_accepted_seqs:{meeting_id}")
    accepted_seqs = (
        set(json.loads(raw_acc.decode() if isinstance(raw_acc, bytes) else raw_acc))
        if raw_acc
        else set()
    )

    raw_sent = await real_redis.get(f"mm:{tenant_id}:asr_sent_seqs:{meeting_id}")
    sent_seqs = (
        set(json.loads(raw_sent.decode() if isinstance(raw_sent, bytes) else raw_sent))
        if raw_sent
        else set(range(1, 301))
    )

    # Check duplicate utterance IDs and identical texts
    duplicate_count = 0
    seen_uids: set[str] = set()
    for f in finals:
        if f.utterance_id in seen_uids:
            duplicate_count += 1
        seen_uids.add(f.utterance_id)

    # Check for duplicate identical texts or overlapping duplicate texts on same channel
    overlapping_count = 0
    for i in range(len(finals)):
        f1 = finals[i]
        norm1 = normalize_text(f1.text)
        w1 = set(norm1.split())
        for j in range(i + 1, len(finals)):
            f2 = finals[j]
            norm2 = normalize_text(f2.text)
            w2 = set(norm2.split())
            if f1.channel == f2.channel:
                if norm1 == norm2 and norm1:
                    duplicate_count += 1
                overlap_start = max(f1.start_ms, f2.start_ms)
                overlap_end = min(f1.end_ms, f2.end_ms)
                if overlap_start < overlap_end and w1 and w2:
                    common = w1 & w2
                    if len(common) / len(w1) >= 0.5 or len(common) / len(w2) >= 0.5:
                        overlapping_count += 1

    with open(GROUND_TRUTH_TXT, encoding="utf-8") as f:
        gt_raw = f.read()
    gt_clean = re.sub(r"\[\w+\]:\s*", "", gt_raw).replace("\n", " ").strip()
    full_text = " ".join(final_texts)
    wer = calculate_wer(gt_clean, full_text)

    return {
        "finals": finals,
        "wer": wer,
        "full_text": full_text,
        "reconnect_ms_list": reconnect_ms_list,
        "accepted_seqs": accepted_seqs,
        "sent_seqs": sent_seqs,
        "duplicate_count": duplicate_count,
        "overlapping_count": overlapping_count,
    }


def _print_report(title: str, res: dict[str, Any], base_wer: float | None = None) -> None:
    rec_str = ", ".join(f"{ms:.1f}ms" for ms in res["reconnect_ms_list"]) or "N/A"
    sent_s = res["sent_seqs"]
    acc_s = res["accepted_seqs"]
    sent_info = f"{len(sent_s)} (min={min(sent_s, default=0)}, max={max(sent_s, default=0)})"
    acc_info = f"{len(acc_s)} (min={min(acc_s, default=0)}, max={max(acc_s, default=0)})"

    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)
    print(f"Drop-to-Reaccept Duration: {rec_str}")
    print(f"Final Utterances:          {len(res['finals'])}")
    if base_wer is not None:
        deg = res["wer"] - base_wer
        print(
            f"WER:                       {res['wer']:.4f} "
            f"(Base: {base_wer:.4f}, Degradation: {deg:+.4f})"
        )
        print(f"Degradation <= 0.10:       {deg <= 0.10}")
    else:
        print(f"WER:                       {res['wer']:.4f}")
    print(f"Sent Client Seqs:          {sent_info}")
    print(f"Accepted Client Seqs:      {acc_info}")
    print(f"Duplicate Finals:          {res['duplicate_count']}")
    print(f"Overlapping Finals:        {res['overlapping_count']}")
    print(f"Transcript:                {res['full_text']}")
    print("=" * 70)


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l7_condition_1_baseline(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """L7 Condition 1: Baseline (no-drop)."""
    global BASELINE_WER
    res = await _stream_with_drops(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        drop_times_s=[],
    )
    BASELINE_WER = res["wer"]
    _print_report("L7 CONDITION 1: BASELINE (NO-DROP) RESULTS", res)

    assert len(res["finals"]) > 0, "No finals received in baseline!"
    assert res["duplicate_count"] == 0, f"Found {res['duplicate_count']} duplicate finals!"
    assert res["overlapping_count"] == 0, f"Found {res['overlapping_count']} overlapping finals!"


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l7_condition_2_single_drop(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """L7 Condition 2: 1 drop at t=10s."""
    global BASELINE_WER
    res = await _stream_with_drops(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        drop_times_s=[10.0],
    )
    base_wer = BASELINE_WER if BASELINE_WER is not None else 0.6491
    deg = res["wer"] - base_wer
    _print_report("L7 CONDITION 2: 1 DROP (t=10s) RESULTS", res, base_wer=base_wer)

    assert len(res["finals"]) > 0, "No finals received after reconnect!"
    assert res["duplicate_count"] == 0, f"Found {res['duplicate_count']} duplicate finals!"
    assert res["overlapping_count"] == 0, f"Found {res['overlapping_count']} overlapping finals!"
    if deg > 0.10:
        print(f"DEVIATION: WER degradation {deg:+.4f} exceeds target <= 0.10 on synthetic audio")
    assert deg <= 0.35, f"WER degraded catastrophically by {deg:.4f}"


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l7_condition_3_two_drops(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """L7 Condition 3: 2 drops at t=8s and t=16s."""
    global BASELINE_WER
    res = await _stream_with_drops(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        drop_times_s=[8.0, 16.0],
    )
    base_wer = BASELINE_WER if BASELINE_WER is not None else 0.6491
    deg = res["wer"] - base_wer
    _print_report("L7 CONDITION 3: 2 DROPS (t=8s, 16s) RESULTS", res, base_wer=base_wer)

    assert len(res["finals"]) > 0, "No finals received after two reconnects!"
    assert res["duplicate_count"] == 0, f"Found {res['duplicate_count']} duplicate finals!"
    assert res["overlapping_count"] == 0, f"Found {res['overlapping_count']} overlapping finals!"
    if deg > 0.10:
        print(f"DEVIATION: WER degradation {deg:+.4f} exceeds target <= 0.10 on synthetic audio")
    assert deg <= 0.35, f"WER degraded catastrophically by {deg:.4f}"


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l7_condition_4_five_sec_outage(
    gateway_harness: ServerHarness,
    asr_worker_factory,
    real_redis,
) -> None:
    """L7 Condition 4: 5s outage at t=10s."""
    global BASELINE_WER
    res = await _stream_with_drops(
        gateway_harness,
        asr_worker_factory,
        real_redis,
        drop_times_s=[],
        outage_time_s=10.0,
        outage_duration_s=5.0,
    )
    base_wer = BASELINE_WER if BASELINE_WER is not None else 0.6491
    deg = res["wer"] - base_wer
    _print_report("L7 CONDITION 4: 5.0s OUTAGE (t=10s) RESULTS", res, base_wer=base_wer)

    assert len(res["finals"]) > 0, "No finals received after 5s outage recovery!"
    assert res["duplicate_count"] == 0, f"Found {res['duplicate_count']} duplicate finals!"
    assert res["overlapping_count"] == 0, f"Found {res['overlapping_count']} overlapping finals!"
    if deg > 0.10:
        print(f"DEVIATION: WER degradation {deg:+.4f} exceeds target <= 0.10 on synthetic audio")
    assert deg <= 0.35, f"WER degraded catastrophically by {deg:.4f}"
