"""MeetMind AI — L4: Keyterm A/B Comparison Against Real Deepgram.

Runs the same financial audio fixture through Deepgram Nova-3:
- Run A: without keyterms
- Run B: with tenant keyterms
Reports WER and count of recognised domain terms.
"""

from __future__ import annotations

import asyncio
import json
import os
import wave
from pathlib import Path

import pytest
from meetmind_asr.providers.base import AudioChunk, ProviderSession
from meetmind_asr.providers.deepgram import DeepgramProvider
from pydantic import SecretStr

from ml.asr_eval.wer import calculate_wer

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_WAV = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.wav"
)
GROUND_TRUTH_TXT = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.txt"
)
KEYTERMS_JSON = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "audio"
    / "generated"
    / "30s_two_speaker_overlap_mono_keyterms.json"
)


def get_deepgram_api_key() -> SecretStr:
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        pytest.skip("DEEPGRAM_API_KEY is not set in environment")
    return SecretStr(key)


async def transcribe_fixture(keyterms: list[str], seconds: float = 15.0) -> str:
    api_key = get_deepgram_api_key()
    provider = DeepgramProvider(api_key=api_key, mip_opt_out=True)

    session = ProviderSession(
        tenant_id="tenant_ab",
        meeting_id="mtg_ab",
        sample_rate=16000,
        channels=1,
        language="en-US",
        keyterms=keyterms,
    )
    await provider.open(session)

    # Read audio chunks
    with wave.open(str(FIXTURE_WAV), "rb") as wf:
        n_chunks = int(seconds * 10)  # 10 chunks/sec
        for seq in range(1, n_chunks + 1):
            data = wf.readframes(1600)
            if not data:
                break
            chunk = AudioChunk(
                data=data,
                client_seq=seq,
                capture_ts_ms=1000 + seq * 100,
                recv_ns=0,
            )
            await provider.send_audio(chunk)
            await asyncio.sleep(0.04)

    await provider.finish()

    finals: list[str] = []
    async for ev in provider.events():
        if ev.is_final and ev.text:
            finals.append(ev.text)

    return " ".join(finals)


@pytest.mark.deepgram
@pytest.mark.asyncio
async def test_l4_keyterm_ab_comparison():
    """Run A/B test with and without keyterms on finance audio."""
    print("\n[Estimated Cost] ~0.5 audio minutes x $0.0058/min = ~$0.0029")
    with open(KEYTERMS_JSON, encoding="utf-8") as f:
        target_keyterms: list[str] = json.load(f)

    with open(GROUND_TRUTH_TXT, encoding="utf-8") as f:
        gt_raw = f.read()
    # Strip speaker prefixes like [David]:
    import re

    gt_clean = re.sub(r"\[\w+\]:\s*", "", gt_raw).replace("\n", " ").strip()

    # Run A: Without keyterms
    transcript_a = await transcribe_fixture(keyterms=[], seconds=15.0)
    wer_a = calculate_wer(gt_clean, transcript_a)
    terms_found_a = [t for t in target_keyterms if t.lower() in transcript_a.lower()]

    # Run B: With keyterms
    transcript_b = await transcribe_fixture(keyterms=target_keyterms, seconds=15.0)
    wer_b = calculate_wer(gt_clean, transcript_b)
    terms_found_b = [t for t in target_keyterms if t.lower() in transcript_b.lower()]

    print("\n" + "=" * 60)
    print(" L4 KEYTERM A/B BENCHMARK RESULTS")
    print("=" * 60)
    print(
        f"Run A (No Keyterms):   WER = {wer_a:.4f} | "
        f"Terms Found = {len(terms_found_a)}/{len(target_keyterms)}"
    )
    print(
        f"Run B (With Keyterms): WER = {wer_b:.4f} | "
        f"Terms Found = {len(terms_found_b)}/{len(target_keyterms)}"
    )
    print(f"Terms recognised in A: {terms_found_a}")
    print(f"Terms recognised in B: {terms_found_b}")
    print("=" * 60)

    # Both runs should yield valid transcripts (soft assertions per spec: report numbers)
    assert len(transcript_a) > 0
    assert len(transcript_b) > 0
