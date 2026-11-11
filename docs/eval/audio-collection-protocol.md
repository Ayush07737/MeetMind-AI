# MeetMind AI — Human Audio Collection & Ingestion Protocol (Tier B)

**Document ID**: PROTOCOL-EVAL-AUDIO-002  
**Status**: Official Protocol (Engineering Pack v1)  
**Stage**: Q-0b Closeout / Tier B Grounding

---

## 1. Overview & Purpose

While synthetic audio (Tier A, `ml/eval/audio/synthetic_eng/`) validates acoustic wiring, version numbers, engineering identifiers, and basic Hinglish phonemes, real human speech (Tier B) provides the definitive benchmark for word error rate, natural conversational prosody, accent variations, and room acoustics.

This protocol defines the exact procedure for recording, standardizing, and ingesting human speech across the **10 Engineering & Development scenarios** in `docs/eval/reading_scripts/`:

- `scenario_01_daily_standup`
- `scenario_02_sprint_planning`
- `scenario_03_design_review_adr`
- `scenario_04_code_review`
- `scenario_05_incident_review`
- `scenario_06_release_go_no_go`
- `scenario_07_oncall_handoff`
- `scenario_08_tech_debt_prioritisation`
- `scenario_09_api_contract_discussion`
- `scenario_10_disagreement_unresolved`

---

## 2. Participant & Role Assignment

Each session requires **4 speakers** (or minimum 2 alternating human speakers):

| Speaker Label | Voice Persona                   | Characteristics                                               | Reading Assignment                 |
| :------------ | :------------------------------ | :------------------------------------------------------------ | :--------------------------------- |
| `speaker_0`   | **Tech Lead / Staff Engineer**  | Authoritative, measured pace (~135 wpm), Indian English       | `speaker_0` turns (optional Ch0)   |
| `speaker_1`   | **Engineering Manager**         | Conversational, facilitative (~145 wpm), Indian English       | `speaker_1` turns (remote Ch1)     |
| `speaker_2`   | **Senior SRE / DevOps Lead**    | Analytical, fast technical pace (~155 wpm), metrics & scripts | `speaker_2` + mixed Hinglish turns |
| `speaker_3`   | **Platform Engineer / Backend** | Code-switching English + colloquial Romanised Hindi           | `speaker_3` + `speaker_hinglish_*` |

Full per-turn reading prompts are located in `docs/eval/reading_scripts/READING_SCRIPTS_MASTER.md` and `docs/eval/reading_scripts/scenario_01..10.md`.

---

## 3. Acoustic Environment & Equipment

### 3.1 Quiet Room Baseline (Variant: `real_quiet`)

- **Noise Floor**: Ambient noise $< 35\text{ dBA}$ (minimal HVAC hum, no external street noise).
- **Microphone**: Cardioid USB condenser mic or wideband enterprise headset placed 5–8 cm off-axis.
- **Capture Resolution**: Raw PCM WAV at 44.1 kHz or 48 kHz, 16-bit or 24-bit.

### 3.2 Noisy Environment (Variant: `real_noisy`)

- Record a second pass in a typical open engineering workspace (~50–55 dBA ambient sound or ~15 dB SNR room noise).

---

## 4. Spoken Pronunciation & Numbering Rules

1. **Identifiers & Tickets**: Speak verbatim as written in the scenario scripts (e.g., _"PLAT one zero one"_, _"PR four eight two"_, _"ADR zero zero eight"_, _"INC four zero nine"_).
2. **Metrics & Versions**: Speak naturally without pausing artificially between words:
   - _"p99 latency 450 milliseconds at 2500 rps"_
   - _"PostgreSQL sixteen point two"_, _"Python three point one three"_
3. **Hinglish Code-Switching**: Read Romanised Hinglish turns with natural conversational inflection (e.g., _"Logs check kar lo ek baar, Grafana dashboard pe memory usage 85 percent dikha raha hai"_).

---

## 5. Ingestion & Conversion Workflow

Recorded audio files are standardized and appended to `ml/eval/audio/real/manifest.jsonl` via `scripts/prepare_real_audio.py`:

```bash
uv run python scripts/prepare_real_audio.py \
  --input-audio path/to/recording.wav \
  --scenario scenario_01_daily_standup \
  --variant real_quiet \
  --speaker speaker_0 \
  --transcript path/to/reference_transcript.txt
```

### Automated Standardization

- Decodes input WAV / MP3 / FLAC via `miniaudio` and resamples to **16 kHz mono 16-bit PCM LE**.
- Peak-normalizes amplitudes to **-1.0 dBFS** (`target_peak=32000.0`) to prevent clipping.
- Computes SHA256 checksum, duration in milliseconds, and records `owner_verified_count: 0` (`verified_by_owner: false`) until owner playback sign-off.
- Benchmark reports remain labelled **PRELIMINARY (SYNTHETIC ONLY)** until verified Tier B human recordings are ingested.
