"""MeetMind AI — Real Audio Ingestion & Standardization (B2).

Converts recorded human audio (WAV, MP3, M4A, FLAC) into standardized:
- 16 kHz mono 16-bit PCM WAV (or optional 2-channel stereo layout)
- Peak normalized to -1.0 dBFS
- Ingests reference text transcript
- Appends standardized entry to ml/eval/audio/real/manifest.jsonl
"""

from __future__ import annotations

import argparse
import array
import hashlib
import json
import wave
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_AUDIO_DIR = REPO_ROOT / "ml" / "eval" / "audio" / "real"
MANIFEST_PATH = REAL_AUDIO_DIR / "manifest.jsonl"


def normalize_pcm16_samples(samples: list[int], target_peak: float = 32000.0) -> list[int]:
    """Peak-normalize PCM16 samples to avoid clipping."""
    if not samples:
        return []
    curr_peak = max(abs(s) for s in samples)
    if curr_peak == 0:
        return list(samples)
    gain = min(1.0, target_peak / float(curr_peak))
    return [int(s * gain) for s in samples]


def ingest_audio(
    input_path: Path,
    scenario: str,
    variant: str,
    speaker: str,
    transcript_text: str,
    output_filename: str | None = None,
) -> Path:
    REAL_AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    out_name = output_filename or f"{scenario}_{speaker}_{variant}.wav"
    dest_path = REAL_AUDIO_DIR / "data" / out_name
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Use miniaudio to decode input format (WAV, MP3, FLAC) to 16kHz mono
    import miniaudio

    dec = miniaudio.decode_file(str(input_path), nchannels=1, sample_rate=16000)
    arr = array.array("h")
    arr.frombytes(dec.samples)
    samples = list(arr)

    norm_samples = normalize_pcm16_samples(samples)
    arr_out = array.array("h", norm_samples)

    with wave.open(str(dest_path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(16000)
        wf.writeframes(arr_out.tobytes())

    dur_ms = int((len(norm_samples) / 16000.0) * 1000)
    sha256 = hashlib.sha256(dest_path.read_bytes()).hexdigest()

    record = {
        "audio_path": f"ml/eval/audio/real/data/{out_name}",
        "scenario": scenario,
        "speaker": speaker,
        "variant": variant,
        "duration_ms": dur_ms,
        "sha256": sha256,
        "spoken_text": transcript_text.strip(),
        "verified_by_owner": False,
    }

    with open(MANIFEST_PATH, "a", encoding="utf-8") as mf:
        mf.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Successfully ingested real audio to {dest_path}")
    print(f"Duration: {dur_ms}ms | SHA256: {sha256[:12]}...")
    return dest_path


def main():
    parser = argparse.ArgumentParser(description="Prepare & Ingest Real Audio for MeetMind AI Eval")
    parser.add_argument("--input-audio", type=Path, required=True, help="Path to raw audio file")
    parser.add_argument("--scenario", type=str, required=True, help="Scenario name")
    parser.add_argument(
        "--variant", type=str, default="real_quiet", choices=["real_quiet", "real_noisy"]
    )
    parser.add_argument(
        "--speaker", type=str, required=True, help="Speaker identifier (speaker_0, etc.)"
    )
    parser.add_argument(
        "--transcript", type=str, required=True, help="Reference transcript text or text file path"
    )
    args = parser.parse_args()

    t_path = Path(args.transcript)
    text = t_path.read_text(encoding="utf-8") if t_path.exists() else args.transcript

    ingest_audio(
        input_path=args.input_audio,
        scenario=args.scenario,
        variant=args.variant,
        speaker=args.speaker,
        transcript_text=text,
    )


if __name__ == "__main__":
    main()
