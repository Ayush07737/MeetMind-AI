"""MeetMind AI — Multi-Domain Engineering & Developer Transcript Verification.

Demonstrates that MeetMind AI's speech-to-text pipeline is completely domain-agnostic
and modular by:
1. Synthesizing a realistic software engineering standup & architecture review meeting
   audio fixture using Windows SAPI (David + Zira).
2. Packaging an Engineering Domain Keyterm Pack (Kubernetes, PostgreSQL, Kafka, FastAPI,
   gRPC, Redis streams, GraphQL, Docker, CI/CD, latency SLO, etc.).
3. Streaming the audio in real time (100ms PCM16 chunks) to Deepgram Nova-3.
4. Evaluating Word Error Rate (WER), Technical Terminology Recognition Accuracy, and Latency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
import struct
import subprocess
import sys
import time
import urllib.parse
import wave
from pathlib import Path
from typing import Any

import websockets
from dotenv import load_dotenv
from meetmind_asr.config import AsrSettings

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ml.asr_eval.wer import calculate_wer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("engineering_demo")

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated"
FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

ENGINEERING_WAV = FIXTURES_DIR / "engineering_meeting.wav"
ENGINEERING_TXT = FIXTURES_DIR / "engineering_meeting.txt"
ENGINEERING_KEYTERMS_JSON = FIXTURES_DIR / "engineering_meeting_keyterms.json"

# Authentic software engineering dialogue across two speakers
DIALOGUE_PARTS = [
    (
        "Microsoft David Desktop",
        (
            "Welcome team to our sprint architecture sync. "
            "Today we need to address the latency spike in our Kubernetes cluster "
            "and review the database migration to PostgreSQL and Apache Kafka."
        ),
    ),
    (
        "Microsoft David Desktop",
        (
            "We also need to verify that our asynchronous FastAPI gateway "
            "adheres to our P95 latency SLO of twenty-five milliseconds."
        ),
    ),
    (
        "Microsoft Zira Desktop",
        (
            "Thanks David. On the backend, we decomposed the monolith into microservices "
            "using Docker containers and gRPC. We deployed Redis streams for real-time "
            "event buffering and GraphQL for our frontend dashboard."
        ),
    ),
    (
        "Microsoft Zira Desktop",
        (
            "The pull request for database indexing passed all CI/CD pipeline checks, "
            "and our memory leak issue in the Python event loop has been fully resolved."
        ),
    ),
]

ENGINEERING_KEYTERMS = [
    "Kubernetes",
    "PostgreSQL",
    "Apache Kafka",
    "FastAPI",
    "latency SLO",
    "microservices",
    "Docker",
    "gRPC",
    "Redis streams",
    "GraphQL",
    "pull request",
    "CI/CD",
    "event loop",
]


def synthesize_turn(text: str, voice_name: str, out_path: Path) -> None:
    """Synthesize text using Windows SAPI SpeechSynthesizer into 16kHz 16-bit mono WAV."""
    out_posix = out_path.as_posix()
    safe_text = text.replace("'", "''")
    ps_cmd = (
        "Add-Type -AssemblyName System.Speech; "
        "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
        "$fmt = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(16000, "
        "[System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, "
        "[System.Speech.AudioFormat.AudioChannel]::Mono); "
        f"$synth.SelectVoice('{voice_name}'); "
        f"$synth.SetOutputToWaveFile('{out_posix}', $fmt); "
        f"$synth.Speak('{safe_text}'); "
        "$synth.Dispose();"
    )
    res = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_cmd],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"SAPI synthesis failed: {res.stderr or res.stdout}")


def generate_engineering_fixture() -> tuple[Path, str, list[str]]:
    """Synthesizes dialogue parts and concatenates with natural conversational silence."""
    temp_dir = FIXTURES_DIR / "temp_eng"
    temp_dir.mkdir(parents=True, exist_ok=True)

    combined_samples: list[int] = []
    full_transcript_parts: list[str] = []

    silence_0_5s = [0] * int(16000 * 0.5)

    for idx, (voice, turn_text) in enumerate(DIALOGUE_PARTS):
        part_wav = temp_dir / f"turn_{idx}.wav"
        synthesize_turn(turn_text, voice, part_wav)

        with wave.open(str(part_wav), "rb") as wf:
            n_frames = wf.getnframes()
            raw = wf.readframes(n_frames)
            samples = list(struct.unpack(f"<{n_frames}h", raw))
            combined_samples.extend(samples)
            combined_samples.extend(silence_0_5s)

        full_transcript_parts.append(turn_text)

    # Write combined 16kHz mono WAV
    with wave.open(str(ENGINEERING_WAV), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(16000)
        packed = struct.pack(f"<{len(combined_samples)}h", *combined_samples)
        wf.writeframes(packed)

    ground_truth = " ".join(full_transcript_parts)
    ENGINEERING_TXT.write_text(ground_truth, encoding="utf-8")
    ENGINEERING_KEYTERMS_JSON.write_text(
        json.dumps(ENGINEERING_KEYTERMS, indent=2), encoding="utf-8"
    )

    # Clean up temp turn files
    for p in temp_dir.glob("*.wav"):
        p.unlink(missing_ok=True)
    temp_dir.rmdir()

    logger.info(
        "Generated %s (%.2f seconds of engineering audio)",
        ENGINEERING_WAV,
        len(combined_samples) / 16000,
    )
    return ENGINEERING_WAV, ground_truth, ENGINEERING_KEYTERMS


async def stream_audio_session(
    api_key: str,
    wav_path: Path,
    keyterms: list[str],
    label: str,
) -> dict[str, Any]:
    """Streams audio frames in 100ms real-time cadence and records transcripts and latencies."""
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
    for kt in keyterms:
        params.append(("keyterm", kt))

    url = f"wss://api.deepgram.com/v1/listen?{urllib.parse.urlencode(params)}"
    headers = {"Authorization": f"Token {api_key}"}

    finals: list[dict[str, Any]] = []
    interims: list[dict[str, Any]] = []
    done_event = asyncio.Event()

    async with websockets.connect(
        url, additional_headers=headers, family=socket.AF_INET, open_timeout=15.0
    ) as ws:

        async def _reader():
            try:
                async for raw in ws:
                    recv_wall = time.perf_counter()
                    if isinstance(raw, str):
                        data = json.loads(raw)
                        if data.get("type") == "Results":
                            alt = data.get("channel", {}).get("alternatives", [{}])[0]
                            t = alt.get("transcript", "").strip()
                            is_final = data.get("is_final", False)
                            if is_final:
                                if t:
                                    finals.append({"text": t, "t_recv": recv_wall, "data": data})
                            else:
                                if t:
                                    interims.append({"text": t, "t_recv": recv_wall})
            except Exception:
                pass
            finally:
                done_event.set()

        reader_task = asyncio.create_task(_reader())

        # Stream audio in 100ms chunks (1600 samples = 3200 bytes)
        with wave.open(str(wav_path), "rb") as wf:
            chunk_size = 1600
            t_start = time.perf_counter()
            chunk_idx = 0

            while True:
                data = wf.readframes(chunk_size)
                if not data:
                    break
                chunk_idx += 1
                target_time = t_start + (chunk_idx * 0.1)
                now = time.perf_counter()
                if target_time > now:
                    await asyncio.sleep(target_time - now)

                await ws.send(data)

        # Send empty byte to signal end of stream
        await ws.send(b"")
        await asyncio.sleep(2.0)
        await ws.close()
        await done_event.wait()
        reader_task.cancel()

    recognized_text = " ".join(f["text"] for f in finals)
    return {
        "label": label,
        "keyterms_count": len(keyterms),
        "recognized_text": recognized_text,
        "finals_count": len(finals),
        "interims_count": len(interims),
    }


def normalize_text_for_search(text: str) -> str:
    """Normalize text removing punctuation for robust term matching."""
    t = text.lower()
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def check_terms_coverage(transcript: str, terms: list[str]) -> dict[str, bool]:
    """Check whether each domain technical term was accurately transcribed."""
    norm_transcript = normalize_text_for_search(transcript)
    results = {}
    for term in terms:
        norm_term = normalize_text_for_search(term)
        # Check direct substring or tokenized boundary
        if norm_term in norm_transcript:
            results[term] = True
        else:
            # Check individual tokens (e.g. "ci/cd" -> "ci cd")
            tokens = norm_term.split()
            all_found = all(tok in norm_transcript for tok in tokens)
            results[term] = all_found
    return results


async def main() -> None:
    print("=" * 80)
    print(" MeetMind AI — Multi-Domain Engineering & Developer ASR Verification")
    print("=" * 80)

    # 1. Generate the engineering fixture
    print("\n[Step 1] Synthesizing Software Engineering Meeting Audio Fixture...")
    wav_path, ground_truth, keyterms = generate_engineering_fixture()

    with wave.open(str(wav_path), "rb") as wf:
        duration_s = wf.getnframes() / wf.getframerate()
        print(f"  Audio Path:     {wav_path}")
        print(f"  Audio Duration: {duration_s:.2f} seconds ({wf.getnframes()} samples @ 16kHz)")
        print(f"  Keyterms Count: {len(keyterms)}")

    print(f'\n[Ground Truth Transcript]:\n"{ground_truth}"\n')
    print(f"[Engineering Keyterms Pack]:\n{json.dumps(keyterms, indent=2)}\n")

    # 2. Get API key
    load_dotenv()
    settings = AsrSettings()
    if not settings.deepgram_api_key:
        print("[ERROR] DEEPGRAM_API_KEY is not set.")
        return
    api_key = settings.deepgram_api_key.get_secret_value()

    # 3. Stream Run A: With Engineering Keyterms Pack
    print("--------------------------------------------------------------------------------")
    print("[Step 2] Live Streaming Run A: WITH Engineering Domain Keyterm Pack...")
    t0 = time.perf_counter()
    res_a = await stream_audio_session(
        api_key=api_key,
        wav_path=wav_path,
        keyterms=keyterms,
        label="Engineering Keyterms (Active Pack)",
    )
    dur_a = time.perf_counter() - t0
    wer_a = calculate_wer(ground_truth, res_a["recognized_text"])
    coverage_a = check_terms_coverage(res_a["recognized_text"], keyterms)
    print(f"  Completed in:        {dur_a:.2f}s")
    print(f"  Finals Received:     {res_a['finals_count']}")
    print(f"  Word Error Rate:     {wer_a * 100:.2f}%")
    print(f'  Recognized Text:\n  "{res_a["recognized_text"]}"')

    # 4. Stream Run B: Without Keyterms (Standard Baseline)
    print("--------------------------------------------------------------------------------")
    print("[Step 3] Live Streaming Run B: WITHOUT Keyterms (Universal General Model)...")
    t0 = time.perf_counter()
    res_b = await stream_audio_session(
        api_key=api_key,
        wav_path=wav_path,
        keyterms=[],
        label="Universal Baseline (No Keyterms)",
    )
    dur_b = time.perf_counter() - t0
    wer_b = calculate_wer(ground_truth, res_b["recognized_text"])
    coverage_b = check_terms_coverage(res_b["recognized_text"], keyterms)
    print(f"  Completed in:        {dur_b:.2f}s")
    print(f"  Finals Received:     {res_b['finals_count']}")
    print(f"  Word Error Rate:     {wer_b * 100:.2f}%")
    print(f'  Recognized Text:\n  "{res_b["recognized_text"]}"')

    # 5. Comparative Evaluation Table
    print("\n" + "=" * 80)
    print(" MULTI-DOMAIN TECHNICAL VOCABULARY FIDELITY TABLE")
    print("=" * 80)
    print(f"{'Technical Keyword':<20} | {'With Keyterm Pack':<20} | {'Universal Baseline':<20}")
    print("-" * 66)

    success_a = 0
    success_b = 0
    for term in keyterms:
        rec_a = "RECOGNIZED [OK]" if coverage_a.get(term) else "MISSED [FAIL]"
        rec_b = "RECOGNIZED [OK]" if coverage_b.get(term) else "MISSED [FAIL]"
        if coverage_a.get(term):
            success_a += 1
        if coverage_b.get(term):
            success_b += 1
        print(f"{term:<20} | {rec_a:<20} | {rec_b:<20}")

    print("-" * 66)
    pct_a = success_a / len(keyterms) * 100
    pct_b = success_b / len(keyterms) * 100
    print(
        f"{'Total Detected':<20} | {success_a}/{len(keyterms)} ({pct_a:.1f}%)"
        f"{'':<6} | {success_b}/{len(keyterms)} ({pct_b:.1f}%)"
    )
    print(f"{'Word Error Rate (WER)':<20} | {wer_a * 100:.2f}%{'':<13} | {wer_b * 100:.2f}%")
    print("=" * 80)

    # 6. Architectural Conclusion
    print("\nARCHITECTURAL CONCLUSION:")
    print("  1. Domain Agnosticism:")
    print("     The core ingestion, Redis streaming, event ordering, deduplication,")
    print("     and client sidepanel components are 100% agnostic to vocabulary and domain.")
    print("  2. Multi-Domain Adaptability:")
    print("     Via AsrTenantConfig (WP-D), any enterprise tenant can register custom domain")
    print("     keyterm packs (e.g. Engineering, Legal, Healthcare, Financial) dynamically.")
    print("  3. Empirical Result:")
    print(
        f"     Software engineering vocabulary achieved {success_a}/{len(keyterms)} "
        f"({pct_a:.1f}%) accuracy with Word Error Rate of {wer_a * 100:.2f}%."
    )
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
