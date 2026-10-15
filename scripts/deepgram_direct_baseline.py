"""MeetMind AI — Direct Deepgram Baseline Bench (Session D2b).

Streams audio directly to Deepgram Nova-3 with identical parameters
(no gateway, no Redis, no meetmind_asr worker) to establish the direct baseline WER.

Measures:
1. Full 30s fixture WER
2. Non-overlapping segments WER
3. Compares with MeetMind pipeline WER to detect lost words at start/finish()

Security:
- Never logs or prints DEEPGRAM_API_KEY.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import wave
from pathlib import Path

import websockets
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

for p in (
    REPO_ROOT,
    REPO_ROOT / "ml",
):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from ml.asr_eval.wer import calculate_wer  # noqa: E402

AUDIO_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "audio"
    / "generated"
    / "30s_two_speaker_overlap_mono.wav"
)
TXT_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "audio"
    / "generated"
    / "30s_two_speaker_overlap_mono.txt"
)
KT_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "audio"
    / "generated"
    / "30s_two_speaker_overlap_mono_keyterms.json"
)


async def run_direct_deepgram_stream(
    api_key: str,
    audio_path: Path,
    keyterms: list[str],
) -> list[dict]:
    params = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", "1"),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("language", "en"),
        ("mip_opt_out", "true"),
    ]
    for kt in keyterms:
        params.append(("keyterm", kt))

    url = f"wss://api.deepgram.com/v1/listen?{urllib.parse.urlencode(params)}"
    headers = {"Authorization": f"Token {api_key}"}

    final_events: list[dict] = []
    received_all_finals = asyncio.Event()

    audio_chunks: list[bytes] = []
    with wave.open(str(audio_path), "rb") as wf:
        while True:
            chunk = wf.readframes(1600)  # 100ms
            if not chunk:
                break
            audio_chunks.append(chunk)

    async with websockets.connect(
        url,
        additional_headers=headers,
        family=socket.AF_INET,
        open_timeout=15.0,
    ) as ws:

        async def reader():
            try:
                async for raw_msg in ws:
                    if isinstance(raw_msg, str):
                        data = json.loads(raw_msg)
                        if data.get("type") == "Results" and data.get("is_final"):
                            final_events.append(data)
            except Exception:
                pass
            finally:
                received_all_finals.set()

        reader_task = asyncio.create_task(reader())

        t_start = time.perf_counter()
        for idx, chunk in enumerate(audio_chunks):
            await ws.send(chunk)
            # Drift-corrected pacing: 100ms per chunk
            target = t_start + ((idx + 1) * 0.1)
            sleep_s = target - time.perf_counter()
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

        # Flush via CloseStream
        with contextlib.suppress(Exception):
            await ws.send(json.dumps({"type": "CloseStream"}))

        # Wait for remaining messages
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(received_all_finals.wait(), timeout=3.0)

        reader_task.cancel()
        with contextlib.suppress(Exception):
            await reader_task

    return final_events


def extract_transcript(final_events: list[dict]) -> str:
    texts: list[str] = []
    for ev in final_events:
        ch = ev.get("channel", {})
        alts = ch.get("alternatives", [])
        if alts:
            t = alts[0].get("transcript", "").strip()
            if t:
                texts.append(t)
    return " ".join(texts)


def extract_words_with_timestamps(final_events: list[dict]) -> list[dict]:
    words: list[dict] = []
    for ev in final_events:
        ch = ev.get("channel", {})
        alts = ch.get("alternatives", [])
        if alts:
            for w in alts[0].get("words", []):
                words.append(w)
    return words


def clean_ground_truth(txt: str) -> str:
    return re.sub(r"\[\w+\]:\s*", "", txt).replace("\n", " ").strip()


async def main() -> None:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] DEEPGRAM_API_KEY is not set.")
        sys.exit(1)

    print("=" * 80)
    print(" MeetMind AI — Direct Deepgram Nova-3 Baseline (No Pipeline)")
    print("=" * 80)

    keyterms: list[str] = []
    if KT_PATH.exists():
        keyterms = json.loads(KT_PATH.read_text(encoding="utf-8"))

    gt_raw = TXT_PATH.read_text(encoding="utf-8")
    gt_clean = clean_ground_truth(gt_raw)

    print(f"Streaming {AUDIO_PATH.name} (30.0s) directly to api.deepgram.com...")
    events = await run_direct_deepgram_stream(api_key, AUDIO_PATH, keyterms)
    hyp_full = extract_transcript(events)
    wer_full = calculate_wer(gt_clean, hyp_full)

    # Calculate non-overlapping segments WER
    # In 30s_two_speaker_overlap_mono:
    # Turn 1: 0.5s - ~6.5s (non-overlapping: 0.5s - 5.0s)
    # Turn 2: 5.0s - ~13.5s (non-overlapping: 6.5s - 12.0s)
    # Turn 3: 12.0s - ~20.0s (non-overlapping: 13.5s - 18.5s)
    # Turn 4: 18.5s - 25.0s (non-overlapping: 20.0s - 25.0s)
    # Ground truth non-overlapping phrases:
    # 1. "Welcome everyone to our investment committee meeting."
    # 2. "Our EBITDA grew thirty percent year over year,"
    # 3. "one hundred and fifty crore rupees for this series B round."
    # 4. "TCS and WIPRO agree."
    gt_non_overlap = (
        "Welcome everyone to our investment committee meeting. "
        "Our EBITDA grew thirty percent year over year "
        "one hundred and fifty crore rupees for this series B round "
        "TCS and WIPRO agree."
    )

    all_words = extract_words_with_timestamps(events)
    # Filter words outside overlap windows [5.0-6.8], [12.0-13.8], [18.5-20.2]
    overlap_windows = [(5.0, 6.8), (12.0, 13.8), (18.5, 20.2)]

    def in_overlap(start_s: float, end_s: float) -> bool:
        return any(max(start_s, ost) < min(end_s, oend) for ost, oend in overlap_windows)

    non_overlap_words = [
        w.get("word", "")
        for w in all_words
        if not in_overlap(float(w.get("start", 0)), float(w.get("end", 0)))
    ]
    hyp_non_overlap = " ".join(non_overlap_words)
    wer_non_overlap = calculate_wer(gt_non_overlap, hyp_non_overlap)

    print("-" * 80)
    print(" DIRECT DEEPGRAM BASELINE RESULTS:")
    print(f"   Final Results Count:         {len(events)}")
    print(f"   Total Recognized Words:      {len(all_words)}")
    print(f"   Full 30s Fixture WER:        {wer_full:.4f}")
    print(f"   Non-Overlapping Segment WER: {wer_non_overlap:.4f}")
    print(f"   Hypothesis (Full):           {hyp_full}")
    print(f"   Ground Truth (Clean):        {gt_clean}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
