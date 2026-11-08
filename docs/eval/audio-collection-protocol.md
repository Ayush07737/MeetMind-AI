# MeetMind AI — Human Audio Collection & Ingestion Protocol (Tier B)

**Document ID**: PROTOCOL-EVAL-AUDIO-002  
**Status**: Official Protocol  
**Stage**: Q-0b / Tier B Grounding

---

## 1. Overview & Purpose

While synthetic audio (Tier A) validates acoustic wiring, number representations, and basic Hinglish phonemes, real human speech (Tier B) provides the definitive benchmark for word error rate, natural conversational prosody, accent variations, and real-room reverberation.

This protocol defines the exact procedure for recording, processing, and ingesting human speech based on the identical 8 financial scenarios defined in the synthetic corpus.

---

## 2. Participant & Role Assignment

The session requires **3 human speakers** (or minimum 2):

| Speaker Label | Voice Persona           | Characteristics                                                        | Reading Assignment                |
| :------------ | :---------------------- | :--------------------------------------------------------------------- | :-------------------------------- |
| **Speaker A** | Partner / Meeting Lead  | Authoritative, measured pace (~130 wpm), Indian English                | `speaker_0` turns                 |
| **Speaker B** | Principal / CFO         | Analytical, fast pace (~155 wpm), technical financial vocabulary       | `speaker_1` turns                 |
| **Speaker C** | Founder / Legal Counsel | Conversational, natural code-switching (Hinglish), spontaneous cadence | `speaker_2` and `speaker_3` turns |

---

## 3. Acoustic Environment & Equipment

### 3.1 Quiet Room Baseline (Variant: `real_quiet`)

- **Noise Floor**: Ambient noise $< 35\text{ dBA}$ (no HVAC hum, fans, or open windows).
- **Microphone**: Cardioid condenser USB mic or high-quality headset (e.g. Jabra Evolve2 or Rode NT-USB) positioned 5–8 cm from mouth, slightly off-axis to eliminate plosives.
- **Capture Resolution**: Raw capture at 44.1 kHz or 48 kHz, 16-bit or 24-bit PCM WAV.

### 3.2 Noisy Environment (Variant: `real_noisy`)

- Repeat the recording in a shared workspace or room with ambient background conversation (~50–55 dBA) or played-back office ambient noise at ~15 dB SNR.

---

## 4. Spoken Numbering & Pronunciation Rules

To ensure valid comparison between human speech and synthetic ground truth:

1. **Numbers**: Read numbers phonetically as written in the script:
   - _"INR 12 crore"_ $\rightarrow$ speak _"I N R twelve crore"_.
   - _"Rs. 150 crore"_ $\rightarrow$ speak _"Rupees one hundred fifty crore"_ or _"one hundred fifty crore rupees"_.
   - _"150 bps"_ $\rightarrow$ speak _"one hundred fifty basis points"_ or _"one hundred fifty bps"_.
   - _"2.5x"_ $\rightarrow$ speak _"two point five x"_.
2. **Hinglish Turns**: Speak Romanised Hindi phrases naturally with native colloquial inflection (e.g., _"yeh terms acceptable hain"_).
3. **Pacing**: Maintain professional executive discussion tempo. Do not over-enunciate or artificially slow down.

---

## 5. Ingestion & Conversion Workflow

Recordings are converted and standardized into the MeetMind AI evaluation pipeline using `scripts/prepare_real_audio.py`:

```bash
uv run python scripts/prepare_real_audio.py \
  --input-audio path/to/recording.wav \
  --scenario scenario_01_investment_committee \
  --variant real_quiet \
  --speaker speaker_0 \
  --transcript path/to/reference_transcript.txt
```

### Automatic Conversions:

- Resampling to **16 kHz mono 16-bit PCM LE**.
- Peak normalization to **-1.0 dBFS** (averting clipping).
- SHA256 checksum verification and entry generation in `ml/eval/audio/real/manifest.jsonl`.
- Headed with `PRELIMINARY (SYNTHETIC ONLY)` until real audio is ingested.
