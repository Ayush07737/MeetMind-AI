"""MeetMind AI — Synthetic Audio Corpus Generator (B1 / Tier A).

Generates a deterministic, regenerable corpus across 8 financial & executive scenarios
featuring 4 distinct Indian-English voices (2 male, 2 female), with 5 audio variants:
1. clean (16 kHz mono)
2. noise_15db (15 dB SNR background noise)
3. telephony_8khz (8 kHz band-limited telephony)
4. speed_1_15x (1.15x speed)
5. two_channel (Ch0 local user, Ch1 remote)

Outputs:
- WAV files in ml/eval/audio/synthetic/data/ (gitignored)
- manifest.jsonl (committed)
- MANIFEST_SHA256.json (committed)
"""

from __future__ import annotations

import array
import asyncio
import hashlib
import json
import math
import os
import random
import sys
import wave
from pathlib import Path
from typing import Any

import edge_tts
import miniaudio

# Ensure local imports work
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from scenarios import SCENARIOS, VOICE_MAP

DATA_DIR = SCRIPT_DIR / "data"
MANIFEST_PATH = SCRIPT_DIR / "manifest.jsonl"
SHA256_MANIFEST_PATH = SCRIPT_DIR / "MANIFEST_SHA256.json"


async def synthesize_turn(text: str, voice: str, output_path: Path) -> None:
    """Synthesize text using Microsoft Edge Neural TTS and write 16kHz mono 16-bit PCM WAV."""
    temp_mp3 = output_path.with_suffix(".mp3")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        comm = edge_tts.Communicate(text, voice)
        await comm.save(str(temp_mp3))

        dec = miniaudio.decode_file(str(temp_mp3), nchannels=1, sample_rate=16000)
        with wave.open(str(output_path), "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(dec.samples)
    finally:
        if temp_mp3.exists():
            temp_mp3.unlink(missing_ok=True)


def read_pcm16_samples(wav_path: Path) -> list[int]:
    with wave.open(str(wav_path), "rb") as wf:
        n_frames = wf.getnframes()
        data = wf.readframes(n_frames)
        arr = array.array("h")
        arr.frombytes(data)
        return list(arr)


def write_pcm16_samples(samples: list[int], wav_path: Path, sample_rate: int = 16000, nchannels: int = 1) -> None:
    wav_path.parent.mkdir(parents=True, exist_ok=True)
    clamped = [max(-32768, min(32767, int(s))) for s in samples]
    arr = array.array("h", clamped)
    with wave.open(str(wav_path), "wb") as wf:
        wf.setnchannels(nchannels)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(arr.tobytes())


def make_noise_15db(samples: list[int], seed: int = 42) -> list[int]:
    """Add pink/Gaussian noise at 15 dB SNR."""
    if not samples:
        return []
    rng = random.Random(seed)
    power_sig = sum(s * s for s in samples) / len(samples)
    # SNR = 10 * log10(P_sig / P_noise) = 15 dB  =>  P_noise = P_sig * 10^(-1.5)
    power_noise = power_sig * (10.0 ** (-1.5))
    sigma = math.sqrt(max(1.0, power_noise))

    noisy: list[int] = []
    for s in samples:
        n = rng.gauss(0.0, sigma)
        noisy.append(int(s + n))
    return noisy


def make_telephony_8khz(samples: list[int]) -> list[int]:
    """Simulate 8 kHz telephony codec round-trip (decimate by 2, linear interpolate back)."""
    if len(samples) < 2:
        return list(samples)
    decimated = samples[::2]
    upsampled: list[int] = []
    for i in range(len(decimated) - 1):
        upsampled.append(decimated[i])
        upsampled.append(int((decimated[i] + decimated[i + 1]) / 2))
    upsampled.append(decimated[-1])
    return upsampled


def make_speed_1_15x(samples: list[int]) -> list[int]:
    """Linear time-compression to 1.15x speed."""
    if not samples:
        return []
    speed = 1.15
    new_len = int(len(samples) / speed)
    res: list[int] = []
    for i in range(new_len):
        orig_pos = i * speed
        idx = int(orig_pos)
        frac = orig_pos - idx
        if idx + 1 < len(samples):
            val = samples[idx] * (1.0 - frac) + samples[idx + 1] * frac
        else:
            val = samples[-1]
        res.append(int(val))
    return res


def make_two_channel(ch0_samples: list[int], ch1_samples: list[int]) -> list[int]:
    """Interleave two mono channels into stereo PCM16."""
    max_len = max(len(ch0_samples), len(ch1_samples))
    stereo: list[int] = []
    for i in range(max_len):
        s0 = ch0_samples[i] if i < len(ch0_samples) else 0
        s1 = ch1_samples[i] if i < len(ch1_samples) else 0
        stereo.append(s0)
        stereo.append(s1)
    return stereo


def generate_manifest(synth_records: list[dict[str, Any]]) -> None:
    """Generate JSONL manifest and SHA256 checksum dictionary."""
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        for r in synth_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    manifest_sha = hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest()
    sha_dict = {
        "manifest_path": "ml/eval/audio/synthetic/manifest.jsonl",
        "manifest_sha256": manifest_sha,
        "total_segments": len(synth_records),
        "voices": VOICE_MAP,
        "variants": ["clean", "noise_15db", "telephony_8khz", "speed_1_15x", "two_channel"],
        "owner_verified_count": 0,
    }
    with open(SHA256_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(sha_dict, f, indent=2)


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 80)
    print(" MeetMind AI — Synthetic Audio Corpus Generator (B1 / Tier A)")
    print(f" Output Data Directory: {DATA_DIR}")
    print("=" * 80)

    records: list[dict[str, Any]] = []

    for sc_name, turns in SCENARIOS.items():
        print(f"\nProcessing scenario: {sc_name} ({len(turns)} turns)...")
        time_cursor_ms = 0

        for turn_idx, turn in enumerate(turns):
            clean_rel = f"{sc_name}/turn_{turn_idx:02d}_clean.wav"
            clean_path = DATA_DIR / clean_rel

            if not clean_path.exists():
                print(f"  Synthesizing turn {turn_idx}: [{turn.voice}] '{turn.text[:45]}...'")
                await synthesize_turn(turn.text, turn.voice, clean_path)

            samples = read_pcm16_samples(clean_path)
            dur_ms = int((len(samples) / 16000.0) * 1000)

            # Generate variants:
            # 1. clean
            clean_rec = {
                "audio_path": f"ml/eval/audio/synthetic/data/{clean_rel}",
                "scenario": sc_name,
                "turn_index": turn_idx,
                "start_ms": time_cursor_ms,
                "end_ms": time_cursor_ms + dur_ms,
                "speaker": turn.speaker,
                "voice": turn.voice,
                "gender": turn.gender,
                "language_tag": turn.language_tag,
                "spoken_text": turn.text,
                "canonical_numbers": turn.canonical_numbers,
                "entities": turn.entities,
                "variant": "clean",
                "owner_verified_count": 0,
            }
            records.append(clean_rec)

            # 2. noise_15db
            noise_rel = f"{sc_name}/turn_{turn_idx:02d}_noise_15db.wav"
            noise_path = DATA_DIR / noise_rel
            if not noise_path.exists():
                noisy = make_noise_15db(samples, seed=turn_idx + 100)
                write_pcm16_samples(noisy, noise_path)
            rec_noise = dict(clean_rec)
            rec_noise["audio_path"] = f"ml/eval/audio/synthetic/data/{noise_rel}"
            rec_noise["variant"] = "noise_15db"
            records.append(rec_noise)

            # 3. telephony_8khz
            tel_rel = f"{sc_name}/turn_{turn_idx:02d}_telephony_8khz.wav"
            tel_path = DATA_DIR / tel_rel
            if not tel_path.exists():
                tel = make_telephony_8khz(samples)
                write_pcm16_samples(tel, tel_path)
            rec_tel = dict(clean_rec)
            rec_tel["audio_path"] = f"ml/eval/audio/synthetic/data/{tel_rel}"
            rec_tel["variant"] = "telephony_8khz"
            records.append(rec_tel)

            # 4. speed_1_15x
            spd_rel = f"{sc_name}/turn_{turn_idx:02d}_speed_1_15x.wav"
            spd_path = DATA_DIR / spd_rel
            if not spd_path.exists():
                spd = make_speed_1_15x(samples)
                write_pcm16_samples(spd, spd_path)
            rec_spd = dict(clean_rec)
            rec_spd["audio_path"] = f"ml/eval/audio/synthetic/data/{spd_rel}"
            rec_spd["variant"] = "speed_1_15x"
            rec_spd["end_ms"] = time_cursor_ms + int(dur_ms / 1.15)
            records.append(rec_spd)

            # 5. two_channel (Channel 0 = speaker_0 local user; Channel 1 = remote)
            ch2_rel = f"{sc_name}/turn_{turn_idx:02d}_two_channel.wav"
            ch2_path = DATA_DIR / ch2_rel
            if not ch2_path.exists():
                if turn.speaker == "speaker_0":
                    ch0 = samples
                    ch1 = [0] * len(samples)
                else:
                    ch0 = [0] * len(samples)
                    ch1 = samples
                stereo = make_two_channel(ch0, ch1)
                write_pcm16_samples(stereo, ch2_path, nchannels=2)
            rec_ch2 = dict(clean_rec)
            rec_ch2["audio_path"] = f"ml/eval/audio/synthetic/data/{ch2_rel}"
            rec_ch2["variant"] = "two_channel"
            records.append(rec_ch2)

            time_cursor_ms += dur_ms + 400

    generate_manifest(records)
    print("\n" + "=" * 80)
    print(" Corpus Generation Complete!")
    print(f" Total Audio Segments Manifested: {len(records)}")
    print(f" Manifest: {MANIFEST_PATH}")
    print(f" SHA256:   {SHA256_MANIFEST_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
