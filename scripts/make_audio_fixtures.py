"""MeetMind AI — Synthetic Audio Fixtures Generator (Part 1).

Generates deterministic 16 kHz PCM16 audio fixtures using Windows SAPI via PowerShell:
(a) 30s two-speaker mono with 3 controlled overlaps (second speaker starts 1.5s early)
(b) 30s two-channel version (channel 0 = local mic / David, channel 1 = remote tab / Zira)
(c) 5-minute meeting fixture with concatenated dialogue segments and silences
(d) 30-minute meeting fixture on demand (--minutes 30)

Produces matching ground-truth transcripts (.txt) and keyterm lists (.json) in:
    tests/fixtures/audio/generated/
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import struct
import subprocess
import wave
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated"
SCRATCH_DIR = OUTPUT_DIR / "temp"

# Keyterms dense in finance terminology
KEYTERMS = [
    "Acme Holdings",
    "Vertex Dynamics",
    "Infosys Technologies",
    "Reliance Industries",
    "Tata Consultancy Services",
    "EBITDA",
    "term sheet",
    "valuation",
    "series B round",
    "due diligence",
    "synergies",
    "quarterly guidance",
    "operating margins",
    "lakh rupees",
    "crore rupees",
    "TCS",
    "RELIANCE",
    "HDFC",
    "INFY",
    "WIPRO",
]

# Scripted dialogue segments
DIALOGUE_A = [
    (
        "Welcome everyone to our investment committee meeting. "
        "Today we review the term sheet for Acme Holdings and Vertex Dynamics. "
        "Our EBITDA grew thirty percent year over year, reaching twelve crore rupees. "
        "We are tracking TCS and RELIANCE performance against quarterly guidance."
    ),
    (
        "The proposed pre-money valuation stands at one hundred and fifty crore rupees "
        "for this series B round. We have conducted extensive due diligence on their "
        "technology stack and patent portfolio."
    ),
    (
        "Let us examine the revenue multiples. Vertex Dynamics achieved seventy-five lakh rupees "
        "in monthly recurring revenue. Compared to HDFC and INFY benchmarks, the growth "
        "trajectory remains outstanding."
    ),
]

DIALOGUE_B = [
    (
        "Thanks David. Regarding due diligence, Infosys Technologies reported strong "
        "operating margins in the same sector. I agree with the valuation. "
        "We should allocate fifty lakh rupees for initial working capital."
    ),
    (
        "The synergies between Acme Holdings and Vertex Dynamics look very promising. "
        "Tata Consultancy Services and WIPRO are also exploring strategic partnerships with them."
    ),
    (
        "Before signing the final term sheet, let us confirm the closing conditions. "
        "All board resolutions and shareholder agreements must be finalized by Friday."
    ),
]

# Concise dialogue specifically for 30s fixture to enable 3 clean 1.5s overlaps without pileup
DIALOGUE_30S_A = [
    (
        "Welcome everyone to our investment committee meeting. "
        "Today we review the term sheet for Acme Holdings."
    ),
    "The valuation stands at one hundred and fifty crore rupees for this series B round.",
]

DIALOGUE_30S_B = [
    "Thanks David. Our EBITDA grew thirty percent year over year, reaching twelve crore rupees.",
    "The synergies with Vertex Dynamics look very promising. TCS and WIPRO agree.",
]


def synthesize_turn(text: str, voice_name: str, out_path: Path) -> None:
    """Synthesize text using Windows SAPI SpeechSynthesizer into 16kHz 16-bit mono WAV."""
    out_posix = out_path.as_posix()
    # Escape single quotes in text for PowerShell
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


def read_pcm_samples(path: Path) -> list[int]:
    """Read raw 16-bit PCM mono samples from a WAV file."""
    with wave.open(str(path), "rb") as wf:
        n_frames = wf.getnframes()
        raw = wf.readframes(n_frames)
        return list(struct.unpack(f"<{n_frames}h", raw))


def write_mono_wav(path: Path, samples: list[int], sample_rate: int = 16000) -> None:
    """Write 16-bit signed PCM mono samples to a WAV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        raw = struct.pack(f"<{len(samples)}h", *samples)
        wf.writeframes(raw)


def write_stereo_wav(
    path: Path,
    ch0_samples: list[int],
    ch1_samples: list[int],
    sample_rate: int = 16000,
) -> None:
    """Write 16-bit signed PCM 2-channel samples (channel 0 = local, channel 1 = remote)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    max_len = max(len(ch0_samples), len(ch1_samples))
    interleaved: list[int] = []
    for i in range(max_len):
        s0 = ch0_samples[i] if i < len(ch0_samples) else 0
        s1 = ch1_samples[i] if i < len(ch1_samples) else 0
        interleaved.append(s0)
        interleaved.append(s1)

    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        raw = struct.pack(f"<{len(interleaved)}h", *interleaved)
        wf.writeframes(raw)


def build_30s_fixtures(voice_a: str, voice_b: str) -> None:
    """Generate (a) 30s mono with 3 controlled overlaps, and (b) 30s 2-channel version."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Synthesizing 30s dialogue turns...")

    # Four turns for 3 controlled overlaps (David, Zira, David, Zira)
    turns = [
        (DIALOGUE_30S_A[0], voice_a, "David"),
        (DIALOGUE_30S_B[0], voice_b, "Zira"),
        (DIALOGUE_30S_A[1], voice_a, "David"),
        (DIALOGUE_30S_B[1], voice_b, "Zira"),
    ]

    turn_samples: list[list[int]] = []
    for idx, (text, voice, spk) in enumerate(turns):
        t_path = SCRATCH_DIR / f"turn_{idx}_{spk}.wav"
        synthesize_turn(text, voice, t_path)
        turn_samples.append(read_pcm_samples(t_path))

    # Target duration: exactly 30.0 seconds = 480,000 samples @ 16kHz
    total_target_samples = 30 * 16000
    overlap_samples = int(1.5 * 16000)  # 1.5s = 24,000 samples

    # Turn 1 starts at t=0.5s (8,000 samples)
    # Turn 2 starts 1.5s before Turn 1 ends (Overlap 1)
    # Turn 3 starts 1.5s before Turn 2 ends (Overlap 2)
    # Turn 4 starts 1.5s before Turn 3 ends (Overlap 3)
    start_indices: list[int] = [8000]
    for i in range(1, 4):
        prev_end = start_indices[i - 1] + len(turn_samples[i - 1])
        st = max(0, prev_end - overlap_samples)
        start_indices.append(st)

    mono_float = [0.0] * total_target_samples
    ch0_track = [0] * total_target_samples  # Speaker A (David)
    ch1_track = [0] * total_target_samples  # Speaker B (Zira)

    transcript_lines: list[str] = []

    for i, (text, _, spk) in enumerate(turns):
        samples = turn_samples[i]
        st = start_indices[i]
        transcript_lines.append(f"[{spk}]: {text}")

        for s_idx, val in enumerate(samples):
            pos = st + s_idx
            if pos >= total_target_samples:
                break
            mono_float[pos] += float(val)

            if spk == "David":
                ch0_track[pos] = val
            else:
                ch1_track[pos] = val

    # Normalize mono track to peak 28,000 to eliminate any digital clipping/distortion
    peak = max(abs(s) for s in mono_float) or 1.0
    scale = min(1.0, 28000.0 / peak)
    mono_track = [int(s * scale) for s in mono_float]

    # Write (a) 30s Mono Overlap
    mono_path = OUTPUT_DIR / "30s_two_speaker_overlap_mono.wav"
    write_mono_wav(mono_path, mono_track)
    txt_mono = OUTPUT_DIR / "30s_two_speaker_overlap_mono.txt"
    txt_mono.write_text("\n".join(transcript_lines), encoding="utf-8")
    kt_mono = OUTPUT_DIR / "30s_two_speaker_overlap_mono_keyterms.json"
    kt_mono.write_text(json.dumps(KEYTERMS, indent=2), encoding="utf-8")
    logger.info("Wrote %s (duration: 30.0s, 3 controlled overlaps)", mono_path.name)

    # Write (b) 30s 2-Channel
    stereo_path = OUTPUT_DIR / "30s_two_channel.wav"
    write_stereo_wav(stereo_path, ch0_track, ch1_track)
    txt_stereo = OUTPUT_DIR / "30s_two_channel.txt"
    txt_stereo.write_text("\n".join(transcript_lines), encoding="utf-8")
    kt_stereo = OUTPUT_DIR / "30s_two_channel_keyterms.json"
    kt_stereo.write_text(json.dumps(KEYTERMS, indent=2), encoding="utf-8")
    logger.info("Wrote %s (duration: 30.0s, ch0=David, ch1=Zira)", stereo_path.name)


def build_longer_meeting(
    duration_minutes: int,
    voice_a: str,
    voice_b: str,
) -> None:
    """Generate multi-minute meeting fixtures with concatenated dialogue segments and silences."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    target_samples = duration_minutes * 60 * 16000
    fname_base = f"{duration_minutes}min_meeting"
    logger.info("Building %d-minute fixture (%d samples)...", duration_minutes, target_samples)

    # Synthesize all dialogue pieces once
    dialogue_pool: list[tuple[str, str, list[int]]] = []
    for idx, text in enumerate(DIALOGUE_A):
        p = SCRATCH_DIR / f"pool_a_{idx}.wav"
        if not p.exists():
            synthesize_turn(text, voice_a, p)
        dialogue_pool.append(("David", text, read_pcm_samples(p)))

    for idx, text in enumerate(DIALOGUE_B):
        p = SCRATCH_DIR / f"pool_b_{idx}.wav"
        if not p.exists():
            synthesize_turn(text, voice_b, p)
        dialogue_pool.append(("Zira", text, read_pcm_samples(p)))

    samples: list[int] = []
    transcript_lines: list[str] = []
    turn_counter = 0

    while len(samples) < target_samples:
        spk, text, s_data = dialogue_pool[turn_counter % len(dialogue_pool)]
        turn_counter += 1
        samples.extend(s_data)
        transcript_lines.append(f"[{spk}]: {text}")

        # Add 1-2s natural silence
        silence_dur = 16000 if turn_counter % 2 == 0 else 24000
        samples.extend([0] * silence_dur)

    # Trim to exact target length
    final_samples = samples[:target_samples]

    wav_path = OUTPUT_DIR / f"{fname_base}.wav"
    write_mono_wav(wav_path, final_samples)
    txt_path = OUTPUT_DIR / f"{fname_base}.txt"
    txt_path.write_text("\n".join(transcript_lines), encoding="utf-8")
    kt_path = OUTPUT_DIR / f"{fname_base}_keyterms.json"
    kt_path.write_text(json.dumps(KEYTERMS, indent=2), encoding="utf-8")
    logger.info("Wrote %s (duration: %.1f minutes)", wav_path.name, duration_minutes)


def main() -> None:
    parser = argparse.ArgumentParser(description="MeetMind AI Audio Fixtures Generator")
    parser.add_argument(
        "--minutes",
        type=int,
        default=5,
        help="Generate fixture of specified duration (default: 5, use 30 for benchmark run)",
    )
    args = parser.parse_args()

    # Detect SAPI voices
    voice_a = "Microsoft David Desktop"
    voice_b = "Microsoft Zira Desktop"

    logger.info("Using SAPI voices: Speaker A='%s', Speaker B='%s'", voice_a, voice_b)

    # 1. Build 30s fixtures (a & b)
    build_30s_fixtures(voice_a, voice_b)

    # 2. Build 5-minute fixture (c)
    build_longer_meeting(5, voice_a, voice_b)

    # 3. Build 30-minute fixture if requested (d)
    if args.minutes >= 30:
        build_longer_meeting(args.minutes, voice_a, voice_b)

    # Check for optional user recordings
    user_rec_dir = REPO_ROOT / "ml" / "asr_eval" / "recordings"
    if user_rec_dir.exists():
        user_wavs = list(user_rec_dir.glob("*.wav"))
        if user_wavs:
            logger.info("Found %d user evaluation recording(s) in %s", len(user_wavs), user_rec_dir)
            for uw in user_wavs:
                ref_txt = uw.with_suffix(".txt")
                logger.info(
                    "  User recording: %s (reference transcript exists: %s)",
                    uw.name,
                    ref_txt.exists(),
                )

    # Clean up temp files
    shutil.rmtree(SCRATCH_DIR, ignore_errors=True)
    logger.info("Audio fixture generation complete. Artifacts written to %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
