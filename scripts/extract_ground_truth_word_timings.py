"""Extract ground-truth word timings for 30s_two_speaker_overlap_mono.wav.

Uses Windows SAPI SpeakProgress to get the exact millisecond offset of every word
in each turn, then applies the turn start offset from make_audio_fixtures.py.
Saves ground truth to tests/fixtures/audio/generated/30s_word_timings.json.
"""

from __future__ import annotations

import json
import subprocess
import wave
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "tests" / "fixtures" / "audio" / "generated"

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

TURNS = [
    (DIALOGUE_30S_A[0], "Microsoft David Desktop", "David"),
    (DIALOGUE_30S_B[0], "Microsoft Zira Desktop", "Zira"),
    (DIALOGUE_30S_A[1], "Microsoft David Desktop", "David"),
    (DIALOGUE_30S_B[1], "Microsoft Zira Desktop", "Zira"),
]


def extract_turn_words(text: str, voice: str) -> list[dict]:
    safe_text = text.replace("'", "''")
    ps_script = f"""
    Add-Type -AssemblyName System.Speech
    $synth = New-Object System.Speech.Synthesis.SpeechSynthesizer
    $synth.SelectVoice('{voice}')
    $words = New-Object System.Collections.ArrayList
    $synth.add_SpeakProgress({{
        param($sender, $e)
        $words.Add([PSCustomObject]@{{
            text = $e.Text
            audio_pos_ms = $e.AudioPosition.TotalMilliseconds
            char_pos = $e.CharacterPosition
            char_count = $e.CharacterCount
        }}) | Out-Null
    }})
    $synth.SetOutputToNull()
    $synth.Speak('{safe_text}')
    $synth.Dispose()
    $words | ConvertTo-Json
    """
    res = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_script],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"PowerShell failed: {res.stderr}")
    return json.loads(res.stdout)


def get_turn_durations() -> list[int]:
    """Synthesize turns to get exact sample lengths."""
    lengths = []
    temp_wav = OUTPUT_DIR / "temp_turn.wav"
    for text, voice, _ in TURNS:
        safe_text = text.replace("'", "''")
        out_posix = temp_wav.as_posix()
        ps_cmd = (
            "Add-Type -AssemblyName System.Speech; "
            "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
            "$fmt = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(16000, "
            "[System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, "
            "[System.Speech.AudioFormat.AudioChannel]::Mono); "
            f"$synth.SelectVoice('{voice}'); "
            f"$synth.SetOutputToWaveFile('{out_posix}', $fmt); "
            f"$synth.Speak('{safe_text}'); "
            "$synth.Dispose();"
        )
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            check=True,
            capture_output=True,
        )
        with wave.open(str(temp_wav), "rb") as wf:
            lengths.append(wf.getnframes())
    temp_wav.unlink(missing_ok=True)
    return lengths


def main() -> None:
    turn_lengths = get_turn_durations()
    overlap_samples = int(1.5 * 16000)  # 24000
    start_indices = [8000]
    for i in range(1, 4):
        prev_end = start_indices[i - 1] + turn_lengths[i - 1]
        st = max(0, prev_end - overlap_samples)
        start_indices.append(st)

    all_words = []
    for turn_idx, (text, voice, spk) in enumerate(TURNS):
        words = extract_turn_words(text, voice)
        turn_st_ms = start_indices[turn_idx] / 16.0
        turn_len_ms = turn_lengths[turn_idx] / 16.0

        for w_idx, w in enumerate(words):
            word_st_ms = int(turn_st_ms + float(w["audio_pos_ms"]))
            # Word end is either next word start or turn end
            if w_idx + 1 < len(words):
                word_end_ms = int(turn_st_ms + float(words[w_idx + 1]["audio_pos_ms"]))
            else:
                word_end_ms = int(turn_st_ms + turn_len_ms)

            all_words.append(
                {
                    "word": w["text"].strip().lower().rstrip(".,?!"),
                    "speaker": spk,
                    "turn": turn_idx,
                    "start_ms": word_st_ms,
                    "end_ms": word_end_ms,
                }
            )

    out_file = OUTPUT_DIR / "30s_word_timings.json"
    out_file.write_text(json.dumps(all_words, indent=2), encoding="utf-8")
    print(f"Extracted {len(all_words)} ground-truth words to {out_file}")
    for sample in all_words[:10]:
        print(
            f"  {sample['word']:<12} [{sample['speaker']}]: "
            f"start_ms={sample['start_ms']:5d} end_ms={sample['end_ms']:5d}"
        )


if __name__ == "__main__":
    main()
