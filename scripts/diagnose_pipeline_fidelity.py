"""MeetMind AI — Diagnose Pipeline vs Direct Deepgram Fidelity (D2c Item 4).

Investigates:
1. Audio hash: Raw PCM source vs bytes handed to provider
2. Direct Deepgram connection parameters vs Pipeline connection parameters
3. Exact finals produced by Direct Deepgram vs Pipeline
4. Pipeline with dedupe enabled vs dedupe disabled
5. Finish() flush behaviour
6. Language en vs en-IN parameters and recognition
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import wave
from pathlib import Path
from typing import Any

import websockets
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

for p in (
    REPO_ROOT,
    REPO_ROOT / "backend" / "asr",
    REPO_ROOT / "backend" / "gateway",
    REPO_ROOT / "backend" / "security",
    REPO_ROOT / "packages" / "shared-schemas" / "schemas_py",
):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from meetmind_asr.dedupe import Deduplicator  # noqa: E402
from meetmind_asr.providers.base import AudioChunk, ProviderSession  # noqa: E402
from meetmind_asr.providers.deepgram import DeepgramProvider, build_deepgram_url  # noqa: E402
from meetmind_asr.timeline import TimelineMapper  # noqa: E402
from pydantic import SecretStr  # noqa: E402

from ml.asr_eval.wer import calculate_wer  # noqa: E402

AUDIO_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.wav"
)
TXT_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "audio" / "generated" / "30s_two_speaker_overlap_mono.txt"
)
KT_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "audio"
    / "generated"
    / "30s_two_speaker_overlap_mono_keyterms.json"
)


def clean_ground_truth(txt: str) -> str:
    return re.sub(r"\[\w+\]:\s*", "", txt).replace("\n", " ").strip()


async def run_direct(
    api_key: str, pcm_chunks: list[bytes], keyterms: list[str], language: str = "en"
) -> dict[str, Any]:
    params = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", "1"),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("language", language),
        ("mip_opt_out", "true"),
    ]
    for kt in keyterms:
        params.append(("keyterm", kt))

    query_str = urllib.parse.urlencode(params)
    url = f"wss://api.deepgram.com/v1/listen?{query_str}"
    headers = {"Authorization": f"Token {api_key}"}

    final_messages: list[dict] = []
    received_all = asyncio.Event()

    async with websockets.connect(
        url, additional_headers=headers, family=socket.AF_INET, open_timeout=15.0
    ) as ws:

        async def reader():
            try:
                async for raw in ws:
                    if isinstance(raw, str):
                        data = json.loads(raw)
                        if data.get("type") == "Results" and data.get("is_final"):
                            final_messages.append(data)
            except Exception:
                pass
            finally:
                received_all.set()

        rt = asyncio.create_task(reader())

        t_start = time.perf_counter()
        sent_bytes = bytearray()
        for idx, chunk in enumerate(pcm_chunks):
            await ws.send(chunk)
            sent_bytes.extend(chunk)
            target = t_start + ((idx + 1) * 0.1)
            sleep_s = target - time.perf_counter()
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

        with contextlib.suppress(Exception):
            await ws.send(json.dumps({"type": "CloseStream"}))

        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(received_all.wait(), timeout=4.0)

        rt.cancel()
        with contextlib.suppress(Exception):
            await rt

    return {
        "url_params": params,
        "finals": final_messages,
        "sent_sha256": hashlib.sha256(sent_bytes).hexdigest(),
    }


async def run_pipeline_provider(
    api_key: str,
    pcm_chunks: list[bytes],
    keyterms: list[str],
    language: str = "en-US",
    disable_dedupe: bool = False,
) -> dict[str, Any]:
    session = ProviderSession(
        tenant_id="tenant_diag",
        meeting_id="mtg_diag",
        sample_rate=16000,
        channels=1,
        language=language,
        keyterms=keyterms,
    )
    prov = DeepgramProvider(api_key=SecretStr(api_key), mip_opt_out=True)
    await prov.open(session)

    timeline = TimelineMapper()
    dedupe = Deduplicator()

    events_received: list[dict] = []
    finals_published: list[dict] = []
    finals_dropped: list[dict] = []
    highest_published_start = -1

    async def event_collector():
        nonlocal highest_published_start
        async for ev in prov.events():
            if ev.event_type == "final":
                events_received.append(
                    {
                        "start_ms": ev.start_ms,
                        "end_ms": ev.end_ms,
                        "text": ev.text,
                        "confidence": ev.confidence,
                    }
                )
                start_ms, end_ms, _ = timeline.map_range(ev.start_ms, ev.end_ms)
                uid = f"u_{start_ms}"

                if not disable_dedupe:
                    if start_ms < highest_published_start - 2000:
                        finals_dropped.append(
                            {"reason": "regression", "text": ev.text, "start_ms": start_ms}
                        )
                        continue
                    if dedupe.is_duplicate(uid, ev.channel, ev.text, start_ms, end_ms):
                        finals_dropped.append(
                            {"reason": "duplicate", "text": ev.text, "start_ms": start_ms}
                        )
                        continue
                    dedupe.record_published(uid, ev.channel, ev.text, start_ms, end_ms)
                    highest_published_start = max(highest_published_start, start_ms)

                finals_published.append(
                    {
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "text": ev.text,
                        "confidence": ev.confidence,
                    }
                )

    coll_task = asyncio.create_task(event_collector())

    t_start = time.perf_counter()
    sent_bytes = bytearray()
    for idx, chunk in enumerate(pcm_chunks):
        audio_chunk = AudioChunk(data=chunk, client_seq=idx + 1, capture_ts_ms=idx * 100)
        await prov.send_audio(audio_chunk)
        timeline.register_chunk(audio_chunk.capture_ts_ms)
        sent_bytes.extend(chunk)
        target = t_start + ((idx + 1) * 0.1)
        sleep_s = target - time.perf_counter()
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

    # Finish provider
    await prov.finish()

    # Wait for collector
    with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(coll_task, timeout=3.0)
    coll_task.cancel()
    with contextlib.suppress(Exception):
        await coll_task

    url_str = build_deepgram_url("wss://api.deepgram.com", session, mip_opt_out=True)
    parsed_query = urllib.parse.parse_qsl(urllib.parse.urlparse(url_str).query)

    return {
        "url_params": parsed_query,
        "events_received": events_received,
        "finals_published": finals_published,
        "finals_dropped": finals_dropped,
        "sent_sha256": hashlib.sha256(sent_bytes).hexdigest(),
    }


async def main() -> None:
    api_key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] DEEPGRAM_API_KEY not set")
        sys.exit(1)

    print("=" * 80)
    print(" MeetMind AI — Pipeline vs Direct Deepgram Fidelity Diagnosis (D2c)")
    print("=" * 80)

    # 1. Read PCM chunks from fixture
    pcm_chunks: list[bytes] = []
    with wave.open(str(AUDIO_PATH), "rb") as wf:
        while True:
            c = wf.readframes(1600)
            if not c:
                break
            pcm_chunks.append(c)

    file_pcm_sha256 = hashlib.sha256(b"".join(pcm_chunks)).hexdigest()
    print(f"Source Audio File: {AUDIO_PATH.name} (30.0s, {len(pcm_chunks)} chunks)")
    print(f"Source Raw PCM SHA256: {file_pcm_sha256}")

    keyterms: list[str] = []
    if KT_PATH.exists():
        keyterms = json.loads(KT_PATH.read_text(encoding="utf-8"))
    print(f"Loaded Keyterms ({len(keyterms)}): {keyterms}")

    gt_clean = clean_ground_truth(TXT_PATH.read_text(encoding="utf-8"))
    print(f"Ground Truth ({len(gt_clean.split())} words):")
    print(f'  "{gt_clean}"')
    print("-" * 80)

    # 2. Run Direct Deepgram (language=en)
    print("Running Direct Deepgram (language=en)...")
    direct_res = await run_direct(api_key, pcm_chunks, keyterms, language="en")
    print(f"  Direct Sent Audio SHA256:     {direct_res['sent_sha256']}")
    print(f"  Matches Source File Raw PCM:  {direct_res['sent_sha256'] == file_pcm_sha256}")
    print(f"  Direct Finals Count:          {len(direct_res['finals'])}")
    direct_texts = []
    for f in direct_res["finals"]:
        alts = f.get("channel", {}).get("alternatives", [])
        if alts:
            direct_texts.append(alts[0].get("transcript", "").strip())
    direct_full = " ".join(t for t in direct_texts if t)
    direct_wer = calculate_wer(gt_clean, direct_full)
    print(f"  Direct WER:                   {direct_wer:.4f} ({direct_wer * 100:.2f}%)")
    print(f"  Direct Full Text ({len(direct_full.split())} words):")
    print(f'    "{direct_full}"')
    print("-" * 80)

    # 3. Run Pipeline Provider (language=en-US, default dedupe)
    print("Running Pipeline Provider (language=en-US, dedupe=enabled)...")
    pipe_res = await run_pipeline_provider(
        api_key, pcm_chunks, keyterms, language="en-US", disable_dedupe=False
    )
    print(f"  Pipeline Sent Audio SHA256:   {pipe_res['sent_sha256']}")
    print(f"  Matches Source File Raw PCM:  {pipe_res['sent_sha256'] == file_pcm_sha256}")
    print(f"  Deepgram Events Received:     {len(pipe_res['events_received'])}")
    print(f"  Finals Published:             {len(pipe_res['finals_published'])}")
    print(f"  Finals Dropped by Pipeline:   {len(pipe_res['finals_dropped'])}")
    for d in pipe_res["finals_dropped"]:
        print(f'    - Dropped ({d["reason"]}): "{d["text"]}" (start_ms={d["start_ms"]})')
    pipe_full = " ".join(f["text"] for f in pipe_res["finals_published"])
    pipe_wer = calculate_wer(gt_clean, pipe_full)
    print(f"  Pipeline WER:                 {pipe_wer:.4f} ({pipe_wer * 100:.2f}%)")
    print(f"  Pipeline Full Text ({len(pipe_full.split())} words):")
    print(f'    "{pipe_full}"')
    print("-" * 80)

    # 4. Run Pipeline Provider with dedupe disabled
    print("Running Pipeline Provider (language=en-US, dedupe=DISABLED)...")
    pipe_no_dedup = await run_pipeline_provider(
        api_key, pcm_chunks, keyterms, language="en-US", disable_dedupe=True
    )
    print(f"  Finals Published (no dedupe): {len(pipe_no_dedup['finals_published'])}")
    pipe_nd_full = " ".join(f["text"] for f in pipe_no_dedup["finals_published"])
    pipe_nd_wer = calculate_wer(gt_clean, pipe_nd_full)
    print(f"  Pipeline WER (no dedupe):     {pipe_nd_wer:.4f} ({pipe_nd_wer * 100:.2f}%)")
    print(f'  Full Text: "{pipe_nd_full}"')
    print("-" * 80)

    # 5. Connection Parameters Comparison
    print("Connection Parameters Comparison:")
    direct_dict = dict(direct_res["url_params"])
    pipe_dict = dict(pipe_res["url_params"])
    all_keys = sorted(set(direct_dict.keys()) | set(pipe_dict.keys()))
    for k in all_keys:
        v_dir = direct_dict.get(k, "<absent>")
        v_pipe = pipe_dict.get(k, "<absent>")
        match = "MATCH" if v_dir == v_pipe else "DIFF"
        print(f"  [{match:5s}] {k:18s} | Direct: {v_dir:15s} | Pipeline: {v_pipe:15s}")
    print("-" * 80)

    # 6. Test en vs en-IN on real Deepgram
    print("Testing language=en vs language=en-IN recognition on real Deepgram...")
    dir_en_in = await run_direct(api_key, pcm_chunks, keyterms, language="en-IN")
    en_in_texts = []
    for f in dir_en_in["finals"]:
        alts = f.get("channel", {}).get("alternatives", [])
        if alts:
            en_in_texts.append(alts[0].get("transcript", "").strip())
    en_in_full = " ".join(t for t in en_in_texts if t)
    en_in_wer = calculate_wer(gt_clean, en_in_full)
    d_finals_len = len(direct_res["finals"])
    d_words_len = len(direct_full.split())
    in_finals_len = len(dir_en_in["finals"])
    in_words_len = len(en_in_full.split())
    print(
        f"  language=en    Finals: {d_finals_len:2d} | "
        f"Words: {d_words_len:2d} | WER: {direct_wer:.4f}"
    )
    print(
        f"  language=en-IN Finals: {in_finals_len:2d} | "
        f"Words: {in_words_len:2d} | WER: {en_in_wer:.4f}"
    )
    print(f'  en text:    "{direct_full}"')
    print(f'  en-IN text: "{en_in_full}"')
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
