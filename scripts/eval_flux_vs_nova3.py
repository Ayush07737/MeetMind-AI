"""MeetMind AI — Deepgram Flux vs Nova-3 Live Comparison Evaluation (Q-0 P0-4).

Streams the identical 30-second audio fixture (30s_two_speaker_overlap_mono.wav)
in real-time 100ms PCM16 chunks to:
1. Deepgram Flux (/v2/listen, model=flux-general-en)
2. Deepgram Nova-3 (/v1/listen, model=nova-3)

Measures:
- Event sequence & types (Flux Update, EagerEndOfTurn, EndOfTurn vs Nova-3 interim/final)
- Turn / Interim latency
- Total transcript & Word Error Rate (WER)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
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
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from meetmind_asr.config import AsrSettings  # noqa: E402

from ml.asr_eval.wer import calculate_wer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("flux_eval")

FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated"
FIXTURE_WAV = FIXTURES_DIR / "30s_two_speaker_overlap_mono.wav"
FIXTURE_TXT = FIXTURES_DIR / "30s_two_speaker_overlap_mono.txt"


async def stream_flux(api_key: str, endpoint: str) -> dict[str, Any]:
    params = [
        ("model", "flux-general-en"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
    ]
    url = f"wss://{endpoint}/v2/listen?{urllib.parse.urlencode(params)}"
    headers = {"Authorization": f"Token {api_key}"}

    events: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    eager_turns: list[dict[str, Any]] = []
    end_of_turns: list[dict[str, Any]] = []
    all_transcripts: list[str] = []
    done_event = asyncio.Event()

    async with websockets.connect(
        url, additional_headers=headers, family=socket.AF_INET, open_timeout=10.0
    ) as ws:

        async def _reader():
            try:
                async for raw in ws:
                    t_recv = time.perf_counter()
                    if isinstance(raw, str):
                        data = json.loads(raw)
                        ev_type = data.get("event") or data.get("type")
                        events.append({"type": ev_type, "t_recv": t_recv, "data": data})
                        if ev_type == "Update":
                            t = data.get("transcript", "").strip()
                            if t:
                                updates.append({"text": t, "t_recv": t_recv})
                        elif ev_type == "EagerEndOfTurn":
                            eager_turns.append({"t_recv": t_recv, "data": data})
                        elif ev_type == "EndOfTurn":
                            end_of_turns.append({"t_recv": t_recv, "data": data})
                            t = data.get("transcript", "").strip()
                            if t:
                                all_transcripts.append(t)
            except Exception:
                pass
            finally:
                done_event.set()

        reader_task = asyncio.create_task(_reader())

        with wave.open(str(FIXTURE_WAV), "rb") as wf:
            chunk_size = 1600
            t_start = time.perf_counter()
            chunk_idx = 0
            while True:
                data = wf.readframes(chunk_size)
                if not data:
                    break
                chunk_idx += 1
                target = t_start + (chunk_idx * 0.1)
                now = time.perf_counter()
                if target > now:
                    await asyncio.sleep(target - now)
                await ws.send(data)

        # Flush Flux stream
        await ws.send(b"")
        await asyncio.sleep(2.0)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(done_event.wait(), timeout=5.0)
        await ws.close()
        reader_task.cancel()

    full_text = " ".join(all_transcripts)
    if not full_text and updates:
        full_text = updates[-1]["text"]

    return {
        "model": "Flux (flux-general-en)",
        "endpoint": endpoint,
        "total_events": len(events),
        "update_count": len(updates),
        "eager_turn_count": len(eager_turns),
        "end_of_turn_count": len(end_of_turns),
        "transcript": full_text,
        "updates": updates,
        "events": events,
    }


async def stream_nova3(api_key: str, endpoint: str) -> dict[str, Any]:
    params = [
        ("model", "nova-3"),
        ("encoding", "linear16"),
        ("sample_rate", "16000"),
        ("channels", "1"),
        ("language", "en"),
        ("diarize", "true"),
        ("interim_results", "true"),
        ("mip_opt_out", "true"),
    ]
    url = f"wss://{endpoint}/v1/listen?{urllib.parse.urlencode(params)}"
    headers = {"Authorization": f"Token {api_key}"}

    interims: list[dict[str, Any]] = []
    finals: list[dict[str, Any]] = []
    done_event = asyncio.Event()

    async with websockets.connect(
        url, additional_headers=headers, family=socket.AF_INET, open_timeout=10.0
    ) as ws:

        async def _reader():
            try:
                async for raw in ws:
                    t_recv = time.perf_counter()
                    if isinstance(raw, str):
                        data = json.loads(raw)
                        if data.get("type") == "Results":
                            alt = data.get("channel", {}).get("alternatives", [{}])[0]
                            t = alt.get("transcript", "").strip()
                            if data.get("is_final"):
                                if t:
                                    finals.append({"text": t, "t_recv": t_recv, "data": data})
                            else:
                                if t:
                                    interims.append({"text": t, "t_recv": t_recv})
            except Exception:
                pass
            finally:
                done_event.set()

        reader_task = asyncio.create_task(_reader())

        with wave.open(str(FIXTURE_WAV), "rb") as wf:
            chunk_size = 1600
            t_start = time.perf_counter()
            chunk_idx = 0
            while True:
                data = wf.readframes(chunk_size)
                if not data:
                    break
                chunk_idx += 1
                target = t_start + (chunk_idx * 0.1)
                now = time.perf_counter()
                if target > now:
                    await asyncio.sleep(target - now)
                await ws.send(data)

        # Signal end of stream
        await ws.send(b"")
        await asyncio.sleep(2.0)
        await ws.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(done_event.wait(), timeout=5.0)
        reader_task.cancel()

    full_text = " ".join(f["text"] for f in finals)
    return {
        "model": "Nova-3 (nova-3)",
        "endpoint": endpoint,
        "interim_count": len(interims),
        "final_count": len(finals),
        "transcript": full_text,
        "finals": finals,
        "interims": interims,
    }


async def main():
    print("=" * 80)
    print(" Deepgram Flux vs Nova-3 Live Comparison Evaluation (Q-0 P0-4)")
    print("=" * 80)

    load_dotenv()
    settings = AsrSettings()
    api_key = settings.deepgram_api_key.get_secret_value()
    ground_truth = FIXTURE_TXT.read_text(encoding="utf-8").strip()

    print(f"Fixture: {FIXTURE_WAV.name} (30s audio)")
    print(f'Ground Truth:\n  "{ground_truth}"\n')

    # 1. Stream Nova-3
    print("Streaming to Nova-3 (api.in.deepgram.com /v1/listen)...")
    res_nova = await stream_nova3(api_key, "api.in.deepgram.com")
    wer_nova = calculate_wer(ground_truth, res_nova["transcript"])
    print(f"  Nova-3 Finals:   {res_nova['final_count']}")
    print(f"  Nova-3 Interims: {res_nova['interim_count']}")
    print(f"  Nova-3 WER:      {wer_nova * 100:.2f}%")
    t_out_nova = res_nova["transcript"]
    print(f'  Nova-3 Output:   "{t_out_nova}"\n')

    # 2. Stream Flux
    print("Streaming to Flux (api.in.deepgram.com /v2/listen)...")
    res_flux = await stream_flux(api_key, "api.in.deepgram.com")
    wer_flux = calculate_wer(ground_truth, res_flux["transcript"])
    print(f"  Flux Updates:      {res_flux['update_count']}")
    print(f"  Flux EagerTurns:   {res_flux['eager_turn_count']}")
    print(f"  Flux EndOfTurns:   {res_flux['end_of_turn_count']}")
    print(f"  Flux WER:          {wer_flux * 100:.2f}%")
    t_out_flux = res_flux["transcript"]
    print(f'  Flux Output:       "{t_out_flux}"\n')

    # 3. Comparative Summary Table
    print("=" * 80)
    print(f"{'Metric / Feature':<30} | {'Nova-3 (/v1/listen)':<22} | {'Flux (/v2/listen)':<22}")
    print("-" * 80)
    print(f"{'Streaming Endpoint':<30} | {'/v1/listen':<22} | {'/v2/listen':<22}")
    print(f"{'Diarization in Streaming':<30} | {'Native (diarize=true)':<22} | {'UNVERIFIED':<22}")
    nova_interims_str = f"{res_nova['interim_count']} interims"
    flux_updates_str = f"{res_flux['update_count']} Updates"
    print(f"{'Continuous Interims':<30} | {nova_interims_str:<22} | {flux_updates_str:<22}")
    nova_finals_str = f"{res_nova['final_count']} finals"
    flux_eot_str = f"{res_flux['end_of_turn_count']} EndOfTurn"
    print(f"{'Finals / Turn Completions':<30} | {nova_finals_str:<22} | {flux_eot_str:<22}")
    nova_wer_str = f"{wer_nova * 100:.2f}%"
    flux_wer_str = f"{wer_flux * 100:.2f}%"
    print(f"{'Word Error Rate (WER)':<30} | {nova_wer_str:<22} | {flux_wer_str:<22}")
    ep = "api.in.deepgram.com"
    print(f"{'India Endpoint Support':<30} | {ep:<22} | {ep:<22}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
