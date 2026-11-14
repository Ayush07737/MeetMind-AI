# MeetMind AI — ASR Provider Bake-Off Report (Q-0d Repair)

**Document ID**: EVAL-ASR-BAKEOFF-002  
**Status**: Synthetic Corpus Only (`owner_verified_count=0`, non-authoritative for production accuracy graduation; Phase A1 Repair Complete)  
**Evaluation Standard**: Q-0d Section A1 + Addendum Section E  
**Execution Date**: 2026-11-12  
**Evidence Artifacts**:

- Streaming Bake-Off (`N_eff=150`, 3 interleaved reps, `default` + `aggressive`): `logs/bakeoff_streaming_q0d.log` (`logs/bakeoff_streaming_q0d.json`)
- Async Accuracy-Lane Bake-Off (`N_eff=150`): `logs/bakeoff_async_q0d.log` (`logs/bakeoff_async_q0d.json`)
- Dry-Run Validation (`--dry-run`): `logs/bakeoff_dry_run_q0d.log` (`logs/bakeoff_dry_run_q0d.json`)
- Key Check (`scripts/check_provider_keys.py`): `logs/check_provider_keys_q0d.log`

> [!WARNING]
> **Invalidation Notice (2026-11-10 Q-0b Hinglish WER Figures)**:  
> All Hinglish WER and Hinglish-inflated overall WER figures reported on **2026-11-10** (`logs/bakeoff_eng_v2.log` / `EVAL-ASR-BAKEOFF-001` — specifically Nova-3 `en-IN` `0.568`, Nova-3 `multi` `0.531`, Flux `0.593`, Gladia Solaria-1 `1.135` / `1.1866`, AssemblyAI Universal-3.6 `0.979`) are **INVALID (script artefact)**. Multilingual providers output Devanagari or Mixed script (`Mixed`/`Devanagari`) against a Romanized reference transcript; the earlier ASCII normalizer stripped Unicode Devanagari codepoints or scored mismatched script tokens as insertions/substitutions. Do **not** cite any 2026-11-10 Hinglish WER numbers.

---

## 1. Corpus, Script Normalization & Latency Definitions (A1.1–A1.4)

- **Corpus**: `ml/eval/audio/synthetic_eng/manifest.jsonl` (`1100` segments across `200` unique turns, `10` engineering scenarios, `6` variants, `owner_verified_count=0`).
- **Sample Design (A1.2)**: Stratified `N_eff = 150` unique segments (`25` per variant across all `6` variants: `clean`, `noise_15db`, `telephony_8khz`, `speed_1_15x`, `two_channel`, `overlap`; `10` scenarios; `101` Hinglish + `49` English), evaluated over **`3` interleaved rotated repetitions** (`450` trials per candidate x configuration). Segment-level bootstrap 95% CIs (`1000` resamples over unique segments).
- **Script Detection & Transliteration (A1.1)**:
  - **Library & License**: `indic-transliteration==2.3.82` (**MIT License**), mapping `DEVANAGARI -> ITRANS -> Romanized lowercase ASCII` in [`ml/eval/asr_bakeoff/normalize.py`](file:///e:/MeetMind%20AI/ml/eval/asr_bakeoff/normalize.py) (`normalize_for_wer(..., transliterate=True)`).
  - **Metrics Reported Separately**:
    - `H_Script`: Unicode script classification per hypothesis (`Latin`, `Devanagari`, `Mixed`).
    - `H_Tr_WER`: Common-script Romanized transliterated Hinglish WER (`indic-transliteration` MIT).
    - `H_Raw_WER`: Un-transliterated Hinglish WER (retained solely to quantify the magnitude of script artefacts).
    - `H_EngWER`: Score restricted to pure English tokens present in the Hinglish reference (sensitive to phonetic Hindi spellings appearing as Roman approximations).
    - `NTER` (Numeric Token Error Rate), `ID_Match` (Exact engineering ID match: Jira/Linear/PR/service tokens), and `EER` (Entity Error Rate).
- **All Three Harness-Clock Latency Definitions (A1.4)**:
  1. **Word Interim Latency (`WordLag50`)**: `first_partial_word_k_perf_ms - est_word_k_send_perf_ms`
  2. **Time-to-Stable-Text (`StabP50` / `StabP95`)**: `earliest_stable_word_k_perf_ms - est_word_k_send_perf_ms` (first interim emission of word $k$ never retracted/changed in later interims or final).
  3. **Vendor End-of-Turn Latency (`EOT_P50` / `EOT_P95`, plus `Eager50` for Flux)**: `vendor_eot_final_recv_perf_ms - last_speech_chunk_sent_perf_ms` (measured identically to vendor turn-detection claims). _Note_: TTS WAV clips contain ~0.3–0.8 s trailing synthesized silence before `last_speech_chunk_sent_perf_ms` EOF; when aggressive VAD/endpointing triggers finalization on trailing silence before EOF arrives, `max(0.0, ...)` clamps negative deltas to `0.0 ms` (observed on Gladia and AssemblyAI aggressive).

---

## 2. Official Provider Documentation, Pricing & Deprecations (Verified 2026-11-11 / 2026-11-12)

### 2.1 OpenAI Verification (`gpt-live-transcribe`, `gpt-transcribe`, Deprecation 2027-02-26) (A1.6)

Verified on OpenAI official documentation pages cached during evaluation (`2026-11-11`):

- **Deprecation & Shutdown Date**: OpenAI _Deprecations | OpenAI API_ (`https://developers.openai.com/api/docs/deprecations`, accessed `2026-11-11`) explicitly announces deprecation and removal on **February 26, 2027** for `whisper-1`, `gpt-4o-transcribe`, `gpt-4o-mini-transcribe` (`gpt-4o-mini-transcribe-2025-12-15`), and `gpt-4o-transcribe-diarize`. Recommended replacements: **`gpt-live-transcribe`** (streaming) and **`gpt-transcribe`** (pre-recorded).
- **Streaming Replacement (`gpt-live-transcribe`)**: OpenAI _Pricing | OpenAI API_ & _Realtime Transcription / Speech-to-Text Guide_ (`https://openai.com/api/pricing`, `https://developers.openai.com/api/docs/guides/realtime-transcription`, accessed `2026-11-11`):
  - **Rate**: `$0.017 / min` (`$1.02 / hr` mono -> `$102.00 / 100h`).
  - **Endpoint**: WebSocket / WebRTC `v1/realtime/transcription_sessions` (`gpt-live-transcribe`).
  - **Keywords / Vocabulary**: Supported via session prompt instructions (`transcription` prompt).
  - **Speaker Labels**: Unsupported on realtime transcription sessions (confirmed in official guide).
- **Async Replacement (`gpt-transcribe`)**: OpenAI `/v1/audio/transcriptions` (`gpt-transcribe`, `$0.0045 / min` = `$0.27 / hr` = `$27.00 / 100h`, diarization `UNVERIFIED` vs legacy `gpt-4o-transcribe-diarize`).
- **Execution Status**: Both OpenAI candidates remain **`NOT RUN: MISSING KEY (OPENAI_API_KEY)`** (`scripts/check_provider_keys.py`).

### 2.2 Streaming & Async Pricing Matrix & Account Tier Verification (A1.7)

- **Stale Rate Corrected**: The prior harness constant `$0.0058/min` for Deepgram Nova-3 is stale and removed from [`ml/eval/asr_bakeoff/run_bakeoff.py`](file:///e:/MeetMind%20AI/ml/eval/asr_bakeoff/run_bakeoff.py).
- **Deepgram Account Tier Status**: Calling `GET /v1/projects/{id}/projects` succeeded (`HTTP 200`), whereas `GET /v1/projects/{id}/balances` returned **`HTTP 403`** (`logs/bakeoff_dry_run_q0d.log`). Because the API key lacks billing read permissions, whether the owner's account is on **Pay-As-You-Go (`$0.0077/min`)**, promotional/Free credits, or **Growth (`$4,000+/yr`, `$0.0065/min`)** is **`UNVERIFIED` via API**. Cost tables default to **published PAYG rates** (`https://deepgram.com/pricing`, accessed `2026-11-11`), with Growth shown parenthetically. Note Deepgram's official FAQ states: _"Multichannel audio bills by total processed duration. A ten-minute stereo file contains twenty minutes of audio across two channels, so it bills as twenty minutes."_

| Provider / Model                      | Official Docs & Pricing Page                              | Access Date |   Mode    |           Mono Base (`$/hr`)            |   Keyterms + Diarize Add-ons (`$/hr`)    | 100h Mono Base (`$`) | 100h w/ Add-ons (`$`) |    100h 2-Ch (`$`, 2x Mono) | ZDR / Training Control                                        |
| :------------------------------------ | :-------------------------------------------------------- | :---------: | :-------: | :-------------------------------------: | :--------------------------------------: | -------------------: | --------------------: | --------------------------: | :------------------------------------------------------------ |
| **Deepgram Nova-3 (`en-IN`)**         | Deepgram Pricing (`https://deepgram.com/pricing`)         | 2026-11-11  | Streaming | `$0.462` (`$0.0077/m`; Growth `$0.390`) | KT `+$0.078` + Diar `+$0.120` = `$0.660` |              `$46.2` |               `$66.0` |                     `$92.4` | `mip_opt_out=true` (self-serve)                               |
| **Deepgram Nova-3 (`multi`)**         | Deepgram Pricing (`https://deepgram.com/pricing`)         | 2026-11-11  | Streaming | `$0.552` (`$0.0092/m`; Growth `$0.468`) | KT `+$0.078` + Diar `+$0.120` = `$0.750` |              `$55.2` |               `$75.0` |                    `$110.4` | `mip_opt_out=true` (self-serve)                               |
| **Deepgram Flux (`flux-general-en`)** | Deepgram Pricing (`https://deepgram.com/pricing`)         | 2026-11-11  | Streaming | `$0.462` (`$0.0077/m`; Growth `$0.390`) |    KT `+$0.078` (no diar) = `$0.540`     |              `$46.2` |               `$54.0` | `UNVERIFIED` (`/v2/listen`) | `mip_opt_out=true` (self-serve)                               |
| **Gladia Solaria-1 Live**             | Gladia Pricing (`https://www.gladia.io/pricing`)          | 2026-11-11  | Streaming |      `EUR 0.68` (`~$0.734` @ 1.08)      |  Custom vocab incl.; Diar `Async only`   |              `$73.4` |               `$73.4` |             `$146.9` (`2x`) | Self-serve trains by default; ZDR Enterprise only             |
| **AssemblyAI U-3.6 Pro**              | AssemblyAI Pricing (`https://www.assemblyai.com/pricing`) | 2026-11-11  | Streaming |         `$0.450` (KT included)          |       Diarize `+$0.120` = `$0.570`       |              `$45.0` |               `$57.0` |       `$90.0` (dual stream) | Enterprise ZDR / no training default                          |
| **OpenAI `gpt-live-transcribe`**      | OpenAI Pricing (`https://openai.com/api/pricing`)         | 2026-11-11  | Streaming |          `$1.020` (`$0.017/m`)          |    Keywords incl.; no speaker labels     |             `$102.0` |              `$102.0` |                `UNVERIFIED` | Enterprise ZDR (`in.api.openai.com` unsupported for Realtime) |
| **Deepgram Nova-3 Async**             | Deepgram Pricing (`Pre-recorded`)                         | 2026-11-11  |   Async   |         `$0.258` (`$0.0043/m`)          | KT `+$0.078` + Diar `+$0.120` = `$0.456` |              `$25.8` |               `$45.6` |                     `$51.6` | `mip_opt_out=true`                                            |
| **AssemblyAI U-3.5 Pro Async**        | AssemblyAI Pricing (`Pre-recorded`)                       | 2026-11-11  |   Async   |                `$0.210`                 | KT `+$0.050` + Diar `+$0.020` = `$0.280` |              `$21.0` |               `$28.0` |                     `$42.0` | Enterprise ZDR                                                |
| **Gladia Solaria-1 Async**            | Gladia Pricing (`Async 0-100h`)                           | 2026-11-11  |   Async   |         `EUR 0.27` (`~$0.292`)          |       Diar + Custom vocab included       |              `$29.2` |               `$29.2` |                     `$58.4` | Enterprise ZDR                                                |
| **Gladia Solaria-3 Async**            | Gladia Pricing (`Async 0-100h`, 5 langs)                  | 2026-11-11  |   Async   |         `EUR 0.16` (`~$0.173`)          |       Diar + Custom vocab included       |              `$17.3` |               `$17.3` |                     `$34.6` | Enterprise ZDR                                                |
| **OpenAI `gpt-transcribe`**           | OpenAI Pricing (`Pre-recorded`)                           | 2026-11-11  |   Async   |         `$0.270` (`$0.0045/m`)          |               `UNVERIFIED`               |              `$27.0` |               `$27.0` |                `UNVERIFIED` | Enterprise ZDR (`None` retention stated for `/v1/audio/*`)    |

---

## 3. Host-to-Cloud Network Probe & Gladia Live Diarization Proof (`logs/bakeoff_streaming_q0d.log`)

### 3.1 Host -> Cloud Network Latency (`India Residential/VPC Path`)

| Endpoint           | Host                       | DNS P50 (ms) | TCP P50 (ms) | TLS P50 (ms) | WS Upgrade (ms) | Ping RTT P50 (ms) | Ping RTT P95 (ms) | RTT Delta vs DG IN |
| :----------------- | :------------------------- | -----------: | -----------: | -----------: | --------------: | ----------------: | ----------------: | -----------------: |
| **Deepgram India** | `api.in.deepgram.com`      |          0.4 |         71.2 |         80.7 |           546.6 |          **72.5** |          **81.7** |      **`+0.0 ms`** |
| Gladia `eu-west`   | `api.gladia.io`            |          0.5 |        205.1 |        222.0 |           886.3 |             209.2 |             232.7 |        `+136.7 ms` |
| Gladia `us-west`   | `api.gladia.io`            |          0.4 |        232.0 |        263.9 |          1574.5 |             329.2 |             339.5 |        `+256.7 ms` |
| AssemblyAI Global  | `streaming.assemblyai.com` |         32.3 |        213.3 |        231.7 |           931.8 |             214.5 |             217.1 |        `+142.0 ms` |

### 3.2 Gladia Live Diarization Body Probe & 2-Speaker Mono/Stereo Checks (A1.5)

Using Gladia's documented JSON body field `"diarization": true` (`POST https://api.gladia.io/v2/live?region=eu-west`) fails schema validation with `HTTP 400`, matching Gladia's pricing page (_"Speaker diarization: Async only"_):

- **Redacted Request**:
  - Header: `x-gladia-key: <REDACTED>`
  - JSON Body: `{"model": "solaria-1", "encoding": "wav/pcm", "bit_depth": 16, "sample_rate": 16000, "channels": 1, "diarization": true}`
- **Verbatim HTTP 400 Response (`logs/bakeoff_streaming_q0d.log`)**:
  ```json
  {
    "statusCode": 400,
    "timestamp": "2026-11-12T11:06:48.277Z",
    "path": "/v2/live?region=eu-west",
    "message": "Invalid parameter(s). See validation_errors for more details.",
    "validation_errors": ["property diarization should not exist"],
    "request_id": "G-90f5c799"
  }
  ```
- **Live 2-Speaker Mono & Overlap Probes**:
  - `deepgram_nova3` (`en-IN`): Sequential `spk=2` (`['0', '1']`), Overlap `spk=2` (`['0', '1']`)
  - `deepgram_nova3_multi`: Sequential `spk=2` (`['0', '1']`), Overlap `spk=2` (`['0', '1']`)
  - `assemblyai_universal`: Sequential `spk=2` (`['A', 'B']`), Overlap `spk=2` (`['A', 'B']`)
  - `deepgram_flux`: **`UNSUPPORTED`** (`TurnInfo` omits mono speaker labels)
  - `gladia_solaria1`: **`UNSUPPORTED`** (JSON body `diarization: true` rejected HTTP 400)
- **Native 2-Channel Stereo Probe**: Both `deepgram_nova3_2ch` (`channels_detected=[0, 1]`) and `gladia_solaria1_2ch` (`channels_detected=[0, 1]`) succeeded live on stereo audio.

---

## 4. Streaming Bake-Off Results: `[default]` vs `[aggressive]` (`N_eff = 150 x 3 reps`, `logs/bakeoff_streaming_q0d.log`)

### 4.1 Effective Configuration Settings Logged (A1.3)

| Candidate `[Config]`                           | Effective Endpointing / Turn-Detection Settings                                                                                                         |
| :--------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Deepgram Nova-3 (en-IN / multi) [default]`    | `endpointing=10` (API default `10ms`), `utterance_end_ms=unset`, `vad_events=false`, `diarize=true`, `interim_results=true`, `mip_opt_out=true`         |
| `Deepgram Nova-3 (en-IN / multi) [aggressive]` | `endpointing=10ms`, `utterance_end_ms=1000ms`, `vad_events=true`, `diarize=true`, `interim_results=true`, `mip_opt_out=true`                            |
| `Deepgram Flux [default]`                      | `eot_threshold=0.7` (default), `eager_eot_threshold=unset`, `eot_timeout_ms=5000`, `mip_opt_out=true`                                                   |
| `Deepgram Flux [aggressive]`                   | `eot_threshold=0.5`, `eager_eot_threshold=0.4`, `eot_timeout_ms=1000`, `mip_opt_out=true`                                                               |
| `Gladia Solaria-1 Live [default]`              | `endpointing=0.05` (50ms default), `maximum_duration_without_endpointing=5` (default), `custom_vocabulary=true`, `code_switching=true`                  |
| `Gladia Solaria-1 Live [aggressive]`           | `endpointing=0.01` (10ms), `maximum_duration_without_endpointing=5`, `receive_speech_events=true`, `custom_vocabulary=true`, `code_switching=true`      |
| `AssemblyAI U-3.6 Pro [default]`               | Balanced defaults (`min_turn_silence=640ms`, `max_turn_silence=768ms`, `speaker_labels=true`, `speech_model=universal-3-6-pro`)                         |
| `AssemblyAI U-3.6 Pro [aggressive]`            | Min-latency (`min_turn_silence=128ms`, `max_turn_silence=400ms`, `interruption_delay=0`, `end_of_turn_confidence_threshold=0.4`, `speaker_labels=true`) |

### 4.2 Streaming Accuracy & Script-Aware Hinglish Matrix (Data Only)

> **AssemblyAI Concurrency-Quota Note**: Under `--concurrency 8` (AAI semaphore `5`), `AssemblyAI U-3.6 Pro` hit `ConnectionClosedError: received 1008 (policy violation)` on `304/450` `[default]` (`N_eff=104`, `N_ok=146`) and `300/450` `[aggressive]` (`N_eff=83`, `N_ok=150`) trials. A supplemental concurrency=2 check (`AAI_CONCURRENCY_2_PROBE`) returned `('OK', None, 94)` on both streams, confirming the `1008` errors were account concurrent-WebSocket quota exhaustion rather than codec errors. AssemblyAI metrics below reflect the `146` and `150` completed sessions.

| Candidate `[Config]`                         | `N_eff` | `N_ok` | `Err` | Overall WER `[Seg 95% CI]`                                      | `H_Tr_WER` | `H_Raw_WER` | `H_EngWER` |     `NTER` | `ID_Match` |      `EER` | `H_Script` Distribution            |
| :------------------------------------------- | ------: | -----: | ----: | :-------------------------------------------------------------- | ---------: | ----------: | ---------: | ---------: | ---------: | ---------: | :--------------------------------- |
| Deepgram Nova-3 (`en-IN`) `[default]`        |     150 |    450 |     0 | `0.414 [0.375, 0.451]`                                          |     0.5416 |      0.5416 | **0.2726** |     0.2816 |     0.5714 |     0.2984 | `{"Latin": 303}`                   |
| **Deepgram Nova-3 (`multi`) `[default]`**    |     150 |    450 |     0 | **`0.245 [0.222, 0.269]`**                                      | **0.2830** |      0.5738 |     0.5022 |     0.3010 |     0.6571 |     0.3374 | `{"Mixed": 303}`                   |
| Deepgram Flux `[default]`                    |     150 |    450 |     0 | `0.454 [0.408, 0.501]`                                          |     0.5976 |      0.5976 |     0.5741 |     0.2816 |     0.6571 |     0.2823 | `{"Latin": 303}`                   |
| Gladia Solaria-1 Live `[default]`            |     150 |    450 |     0 | `0.388 [0.333, 0.445]`                                          |     0.4996 |      0.8446 |     0.9406 |     0.2524 |     0.3524 |     0.3185 | `{"Mixed": 252, "Devanagari": 51}` |
| AssemblyAI U-3.6 Pro `[default]`             |     104 |    146 |   304 | `0.386 [0.340, 0.437]`                                          |     0.5211 |      0.8951 |     0.9825 |     0.1753 | **0.8649** |     0.2568 | `{"Devanagari": 56, "Mixed": 43}`  |
| Deepgram Nova-3 (`en-IN`) `[aggressive]`     |     150 |    449 |     1 | `0.414 [0.375, 0.451]`                                          |     0.5416 |      0.5416 | **0.2773** |     0.2825 |     0.5714 |     0.2992 | `{"Latin": 302}`                   |
| **Deepgram Nova-3 (`multi`) `[aggressive]`** |     150 |    449 |     1 | **`0.245 [0.222, 0.268]`**                                      | **0.2830** |      0.5738 |     0.5022 |     0.3019 |     0.6571 |     0.3369 | `{"Mixed": 302}`                   |
| Deepgram Flux `[aggressive]`                 |     150 |    449 |     1 | `0.448 [0.404, 0.491]`                                          |     0.5884 |      0.5884 |     0.5635 |     0.2825 |     0.6571 |     0.2871 | `{"Latin": 302}`                   |
| Gladia Solaria-1 Live `[aggressive]`         |     150 |    448 |     2 | `0.382 [0.345, 0.420]`                                          |     0.4792 |      0.8206 |     0.9224 |     0.2362 |     0.3714 |     0.3221 | `{"Mixed": 253, "Devanagari": 48}` |
| AssemblyAI U-3.6 Pro `[aggressive]`          |      83 |    150 |   300 | `0.370 [0.316, 0.425]`                                          |     0.4883 |      0.8673 |     0.9266 | **0.1296** | **0.8611** | **0.2268** | `{"Mixed": 43, "Devanagari": 55}`  |
| Sarvam Saaras v4/v3                          |       — |      — |     — | **NOT RUN** (`HTTP 402 CREDITS EXHAUSTED`, WS `quota_exceeded`) |          — |           — |          — |          — |          — |          — | —                                  |
| OpenAI `gpt-live-transcribe`                 |       — |      — |     — | **NOT RUN** (`MISSING KEY: OPENAI_API_KEY`)                     |          — |           — |          — |          — |          — |          — | —                                  |

### 4.3 Streaming Latency (All 3 Definitions), Stability & 100h Cost

| Candidate `[Config]`                      | `WordLag50` (ms) | `StabP50` (ms) | `StabP95` (ms) | `EOT_P50` (ms) | `EOT_P95` (ms) | `Eager50` (ms) | Retraction Rate | 100h Mono (`$`) | 100h Add-ons (`$`) | 100h 2-Ch (`$`) |           `ALL_GATES`            | Score (`0..100`) |
| :---------------------------------------- | ---------------: | -------------: | -------------: | -------------: | -------------: | -------------: | --------------: | --------------: | -----------------: | --------------: | :------------------------------: | ---------------: |
| **Deepgram Nova-3 (`en-IN`) `[default]`** |           1095.3 |     **1325.6** |         3799.3 |         2434.3 |         3451.0 |            N/A |           3.71% |         `$46.2` |            `$66.0` |         `$92.4` |         False (`G_Acc`)          |        **73.89** |
| **Deepgram Nova-3 (`multi`) `[default]`** |           1109.9 |     **1341.6** |         3699.8 |         2455.2 |         3462.1 |            N/A |           3.87% |         `$55.2` |            `$75.0` |        `$110.4` |         False (`G_Acc`)          |        **73.73** |
| Deepgram Flux `[default]`                 |        **829.5** |     **1207.8** |         3507.2 |          932.7 |         2878.9 |            N/A |       **3.15%** |         `$46.2` |            `$54.0` |       `UNVERIF` |     False (`G_Diar`,`G_Acc`)     |            57.07 |
| Gladia Solaria-1 Live `[default]`         |           1286.4 |         2789.5 |         7072.1 |           0.0* |          790.0 |            N/A |          14.80% |         `$73.4` |            `$73.4` |        `$146.9` | False (`G_Diar`,`G_ZDR`,`G_Acc`) |            33.63 |
| AssemblyAI U-3.6 Pro `[default]`          |           2868.1 |         3313.8 |         7085.4 |          512.0 |         1623.2 |            N/A |           7.47% |         `$45.0` |            `$57.0` |         `$90.0` |     False (`G_ZDR`,`G_Acc`)      |            49.63 |
| Deepgram Nova-3 (`en-IN`) `[aggressive]`  |           1192.0 |         1527.3 |         3931.6 |      **651.8** |         2793.1 |            N/A |           3.72% |         `$46.2` |            `$66.0` |         `$92.4` |         False (`G_Acc`)          |            71.98 |
| Deepgram Nova-3 (`multi`) `[aggressive]`  |           1232.1 |         1542.4 |         3765.9 |      **726.3** |         3138.7 |            N/A |           3.88% |         `$55.2` |            `$75.0` |        `$110.4` |         False (`G_Acc`)          |            71.88 |
| Deepgram Flux `[aggressive]`              |            944.2 |         1378.6 |     **3468.6** |      **224.4** |         1224.1 |       **64.9** |           3.34% |         `$46.2` |            `$54.0` |       `UNVERIF` |     False (`G_Diar`,`G_Acc`)     |            55.59 |
| Gladia Solaria-1 Live `[aggressive]`      |           1318.7 |         2696.2 |         6382.1 |           0.0* |          585.9 |            N/A |          13.02% |         `$73.4` |            `$73.4` |        `$146.9` | False (`G_Diar`,`G_ZDR`,`G_Acc`) |            33.69 |
| AssemblyAI U-3.6 Pro `[aggressive]`       |           2805.8 |         3225.0 |         6360.8 |           0.0* |          718.0 |            N/A |           5.75% |         `$45.0` |            `$57.0` |         `$90.0` |     False (`G_ZDR`,`G_Acc`)      |            50.56 |

_\*`EOT_P50 = 0.0 ms` occurs because TTS trailing silence triggers vendor EOT prior to last audio chunk EOF._

### 4.4 Per-Variant Breakdown (`WER` / `ID_Match` / `StabP50` ms / `EOT_P50` ms)

| Candidate `[Config]`                | `clean` (`25`)                     | `noise_15db` (`25`)                | `telephony_8khz` (`25`)            | `speed_1_15x` (`25`)               | `two_channel` (`25`)               | `overlap` (`25`)                   |
| :---------------------------------- | :--------------------------------- | :--------------------------------- | :--------------------------------- | :--------------------------------- | :--------------------------------- | :--------------------------------- |
| **Nova-3 `en-IN` `[default]`**      | `0.416` / `0.67` / `1085` / `2409` | `0.498` / `0.67` / `986` / `2438`  | `0.474` / `0.75` / `1094` / `2452` | `0.458` / `1.00` / `1140` / `2483` | `0.449` / `0.33` / `1086` / `2470` | `0.189` / `0.47` / `1909` / `2304` |
| **Nova-3 `en-IN` `[aggressive]`**   | `0.416` / `0.67` / `1172` / `670`  | `0.498` / `0.67` / `1108` / `574`  | `0.474` / `0.75` / `1125` / `559`  | `0.458` / `1.00` / `1288` / `712`  | `0.449` / `0.33` / `1281` / `624`  | `0.189` / `0.47` / `2251` / `655`  |
| **Nova-3 `multi` `[default]`**      | `0.240` / `0.67` / `1090` / `2426` | `0.311` / `0.00` / `1058` / `2452` | `0.207` / `1.00` / `1195` / `2493` | `0.267` / `1.00` / `1314` / `2484` | `0.273` / `0.67` / `1107` / `2454` | `0.171` / `0.63` / `1915` / `2328` |
| **Nova-3 `multi` `[aggressive]`**   | `0.240` / `0.67` / `1220` / `738`  | `0.311` / `0.00` / `1185` / `741`  | `0.207` / `1.00` / `1319` / `661`  | `0.267` / `1.00` / `1509` / `816`  | `0.273` / `0.67` / `1318` / `681`  | `0.171` / `0.63` / `2181` / `682`  |
| **Flux `[default]`**                | `0.470` / `0.67` / `977` / `824`   | `0.539` / `0.33` / `1081` / `371`  | `0.521` / `0.75` / `1039` / `1095` | `0.512` / `1.00` / `1057` / `1075` | `0.479` / `0.67` / `897` / `856`   | `0.205` / `0.63` / `1614` / `1613` |
| **Flux `[aggressive]`**             | `0.460` / `0.67` / `1154` / `228`  | `0.517` / `0.33` / `1123` / `200`  | `0.526` / `0.75` / `1081` / `0`    | `0.514` / `1.00` / `1201` / `264`  | `0.469` / `0.67` / `1109` / `12`   | `0.204` / `0.63` / `1961` / `530`  |
| **Gladia Solaria-1 `[default]`**    | `0.435` / `0.33` / `2614` / `0`    | `0.423` / `0.67` / `3375` / `54`   | `0.448` / `0.50` / `2839` / `0`    | `0.373` / `0.67` / `2692` / `174`  | `0.463` / `0.00` / `2863` / `0`    | `0.184` / `0.28` / `2641` / `0`    |
| **Gladia Solaria-1 `[aggressive]`** | `0.410` / `0.33` / `2439` / `0`    | `0.438` / `0.67` / `3074` / `64`   | `0.410` / `0.50` / `2439` / `0`    | `0.394` / `0.67` / `2931` / `88`   | `0.398` / `0.00` / `2614` / `0`    | `0.241` / `0.32` / `2673` / `0`    |
| **AssemblyAI U-3.6 `[default]`**    | `0.441` / `1.00` / `3364` / `582`  | `0.439` / `1.00` / `3167` / `512`  | `0.419` / `0.67` / `3612` / `451`  | `0.455` / `1.00` / `3231` / `852`  | `0.413` / `0.67` / `3489` / `437`  | `0.191` / `0.86` / `3227` / `418`  |
| **AssemblyAI U-3.6 `[aggressive]`** | `0.415` / `1.00` / `3001` / `0`    | `0.486` / `0.00` / `3140` / `0`    | `0.378` / `1.00` / `3080` / `0`    | `0.446` / `1.00` / `3233` / `84`   | `0.334` / `0.00` / `3136` / `0`    | `0.122` / `1.00` / `3421` / `0`    |

---

## 5. Async Accuracy-Lane Bake-Off (`N_eff = 150`, `logs/bakeoff_async_q0d.log`, A1.8)

Evaluated pre-recorded accuracy candidates on the identical `N_eff = 150` corpus plus 10 stitched 2-speaker multi-turn scenario conversations for speaker turn assignment (`2SpkAcc`):

| Candidate (Pre-Recorded)        | `N_ok` | `Err` | WER `[95% CI]`                                    | `H_Tr_WER` | `H_Raw_WER` | `H_EngWER` |     `NTER` | `ID_Match` |      `EER` | `2SpkAcc` (`N=10`) | `JobP50` (ms) | `JobP95` (ms) |     `$/100h` (Add-ons) |
| :------------------------------ | -----: | ----: | :------------------------------------------------ | ---------: | ----------: | ---------: | ---------: | ---------: | ---------: | -----------------: | ------------: | ------------: | ---------------------: |
| Deepgram Nova-3 Async (`en-IN`) |    147 |     3 | `0.413 [0.372, 0.453]`                            |     0.5495 |      0.5495 | **0.2118** |     0.2553 |     0.6061 |     0.2966 |             0.6667 |    **5132.7** |   **12257.8** | `$45.6` (`$25.8` base) |
| AssemblyAI U-3.5 Pro Async      |    149 |     1 | `0.420 [0.373, 0.466]`                            |     0.5859 |      0.9641 |     1.1379 | **0.2059** |     0.6571 |     0.3607 |         **0.7667** |       13073.9 |       27868.8 | `$28.0` (`$21.0` base) |
| Gladia Solaria-1 Async          |    150 |     0 | `0.376 [0.340, 0.410]`                            |     0.4822 |      0.8107 |     0.9229 |     0.3010 |     0.3714 |     0.3629 |             0.7334 |       13680.2 |       25713.3 |                `$29.2` |
| **Gladia Solaria-3 Async**      |    150 |     0 | **`0.262 [0.228, 0.299]`**                        | **0.3403** |      0.6400 |     0.6727 |     0.2913 | **0.7429** | **0.2823** |             0.7334 |       13364.0 |       27282.8 |            **`$17.3`** |
| Sarvam REST (`saaras:v3`)       |      — |     — | **NOT RUN** (`HTTP 402 insufficient_quota_error`) |          — |           — |          — |          — |          — |          — |                  — |             — |             — |                      — |
| OpenAI `gpt-transcribe`         |      — |     — | **NOT RUN** (`MISSING KEY: OPENAI_API_KEY`)       |          — |           — |          — |          — |          — |          — |                  — |             — |             — |                      — |

---

## 6. Runtime Q4 Completion & Harness Hygiene (A2, A5, A6)

- **A2 Runtime `words` + `stable_prefix_words`**: [`backend/asr/meetmind_asr/pipeline.py`](file:///e:/MeetMind%20AI/backend/asr/meetmind_asr/pipeline.py) and Deepgram/Stub parsers populate bounded per-word timings/confidences (`words[]`) and consecutive-interim prefix stability (`stable_prefix_words`). Stub service overhead (`logs/a2_stub_after_q0d.log`, `N=59` finals, `N=139` interims):
  - **Before A2**: `P50 = 0.21 ms`, `P95 = 0.95 ms`
  - **After A2**: `P50 = 0.23 ms`, `P95 = 1.04 ms` (delta `+0.09 ms` P95; ceiling `<= 25.0 ms` P95).
- **A5 Harness Kernel-Power Fix**: `scripts/asr_latency_bench.py` offloads the `wevtutil` Kernel-Power query via `asyncio.to_thread()` to prevent event-loop blocking at teardown. The owner's `2026-11-10` 30m run remains the clean baseline; the 60m soak awaits owner execution.
- **A6 Owner-Side Open Gaps (Explicit Ledger)**:
  1. `owner_verified_count = 0` on `ml/eval/audio/synthetic_eng/manifest.jsonl`.
  2. Real meeting audio (`Indian-English` + `Hinglish` multi-speaker engineering meetings) is not yet recorded (`docs/eval/audio-collection-protocol.md`).
  3. External held-out engineering cases (`EVAL_HELDOUT_PATH`) are not yet authored.

---

## 7. CLI Reference & Reproducible PowerShell Commands (A1.9)

### 7.1 Verbatim `--help` Output (`uv run python ml/eval/asr_bakeoff/run_bakeoff.py --help`)

```text
usage: run_bakeoff.py [-h] [--mode {streaming,async,both}]
                      [--sample-size SAMPLE_SIZE] [--reps REPS]
                      [--configs CONFIGS] [--concurrency CONCURRENCY]
                      [--seed SEED] [--dry-run] [--json-out JSON_OUT]

MeetMind AI ASR Bake-off Evaluation Runner

options:
  -h, --help            show this help message and exit
  --mode {streaming,async,both}
                        Evaluation mode: live streaming, async pre-recorded,
                        or both (default: streaming)
  --sample-size SAMPLE_SIZE
                        Stratified unique segment count (default: 150)
  --reps REPS           Interleaved streaming repetitions (default: 3)
  --configs CONFIGS     Comma-separated streaming configs (default:
                        default,aggressive)
  --concurrency CONCURRENCY
                        Concurrent segment workers (default: 8)
  --seed SEED           Stratified sampling seed
  --dry-run             Validate keys, stratification, network & Gladia probe
                        without streaming paid audio
  --json-out JSON_OUT   Path to write JSON results artifact
```

### 7.2 Reproducible PowerShell Commands

```powershell
# Check env var presence & aliases without leaking secrets
uv run python scripts/check_provider_keys.py 2>&1 | Tee-Object logs/check_provider_keys_q0d.log

# Zero-cost preflight (keys, stratification, TLS/TCP, Gladia live diarization HTTP 400 probe)
uv run python ml/eval/asr_bakeoff/run_bakeoff.py --dry-run --sample-size 150 --json-out logs/bakeoff_dry_run_q0d.json 2>&1 | Tee-Object logs/bakeoff_dry_run_q0d.log

# Full interleaved streaming bake-off (Deepgram Nova-3 en-IN & multi, Flux, Gladia Solaria-1, AssemblyAI U-3.6 Pro)
uv run python ml/eval/asr_bakeoff/run_bakeoff.py --mode streaming --sample-size 150 --reps 3 --configs default,aggressive --concurrency 8 --json-out logs/bakeoff_streaming_q0d.json 2>&1 | Tee-Object logs/bakeoff_streaming_q0d.log

# Async accuracy-lane bake-off (Deepgram Nova-3 Async, AssemblyAI U-3.5 Pro Async, Gladia Solaria-1/3 Async)
uv run python ml/eval/asr_bakeoff/run_bakeoff.py --mode async --sample-size 150 --concurrency 8 --json-out logs/bakeoff_async_q0d.json 2>&1 | Tee-Object logs/bakeoff_async_q0d.log

# Lane 0 + unique-utterance CI budget gate check
uv run python scripts/budget_check.py --log logs/q0a_stats30_india.log --bakeoff-json logs/bakeoff_streaming_q0d.json 2>&1 | Tee-Object logs/budget_check_q0d.log
```
