# PRELIMINARY (SYNTHETIC ONLY) — MeetMind AI ASR Provider Bake-Off Report

**Document ID**: EVAL-ASR-BAKEOFF-001  
**Status**: Preliminary (Synthetic Engineering Corpus v2 Only; `owner_verified_count=0`)  
**Evaluation Standard**: Q-0b Closeout Section A1 + Addendum Section E  
**Execution Date**: 2026-11-10  
**Evidence Log**: `logs/bakeoff_eng_v2.log`  
**JSON Artifact**: `logs/bakeoff_summary.json`

---

## 1. Evaluation Corpus & Methodology (Data Only)

- **Corpus**: `ml/eval/audio/synthetic_eng/manifest.jsonl` (`manifest_sha256=8b126e3c4b696c28cf2d124643d8c38e3a67b3d5156c7c69f222861844cd6790`)
- **Corpus Size**: `1100` manifested audio segments across `200` unique turns, `10` engineering scenarios, `6` variants (`clean`, `noise_15db`, `telephony_8khz`, `speed_1_15x`, `overlap`, `two_channel`), and `owner_verified_count=0` (Edge Neural TTS profiles `en-IN-PrabhatNeural`, `en-IN-NeerjaNeural`, `en-IN-NeerjaExpressiveNeural`, `hi-IN-MadhurNeural`, `hi-IN-SwaraNeural`; `AZURE_SPEECH_KEY` absent).
- **Bake-Off Sample**: Stratified `15` segments covering all `10` scenarios (`8` `en` + `7` `hinglish` per repetition) x `3` interleaved rotated repetitions (`N = 45` trials per evaluated candidate).
- **Shared Normalizer**: `ml/eval/asr_bakeoff/normalize.py` applied identically to reference and hypothesis (`WER`, `Hinglish WER`, `NTER`, `IEMR`, `ID Exact Match`, `EER`) with `1000`-resample segment-level bootstrap 95% confidence intervals. Note: Devanagari Unicode outputs from multilingual providers are stripped to ASCII `[a-z0-9]` tokens by the Latin normalizer unless Romanized/transliterated.
- **Harness-Clock Latency Definitions**:
  - `first_partial_lag_ms`: `first_interim_recv_perf_ms - first_speech_chunk_send_perf_ms`
  - `word_interim_latency_ms`: `first_partial_word_k_perf_ms - est_word_k_send_perf_ms`
  - `time_to_stable_text_ms`: `earliest_stable_word_k_perf_ms - est_word_k_send_perf_ms`
  - `final_flush_ms`: `last_final_recv_perf_ms - last_speech_chunk_send_perf_ms`

---

## 2. Official Provider Documentation & Pricing Table (Access Date: 2026-11-10)

| Specification             | Deepgram Nova-3 (`en-IN` / `multi`)                                                                                                                      | Deepgram Flux                                                                                         | Gladia Solaria-1                                                                             | AssemblyAI Universal-3.6                                                                                                  | Sarvam Saaras v3                                                                                                | OpenAI Realtime / Live Transcribe                                                                                         |
| :------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------ |
| **Official Docs Page**    | Deepgram Models & Languages Overview (`https://developers.deepgram.com/docs/models-languages-overview`)                                                  | Deepgram Flux Streaming API (`https://developers.deepgram.com/docs/flux`)                             | Gladia V2 Live Streaming API Reference (`https://docs.gladia.io/api-reference/v2/live/init`) | AssemblyAI Universal-Streaming v3 API (`https://www.assemblyai.com/docs/streaming/universal-streaming`)                   | Sarvam AI Speech-to-Text Streaming Docs (`https://docs.sarvam.ai/api-reference-docs/speech-to-text/stt/stream`) | OpenAI Realtime API Guide (`https://platform.openai.com/docs/guides/realtime`)                                            |
| **Access Date**           | 2026-11-10                                                                                                                                               | 2026-11-10                                                                                            | 2026-11-10                                                                                   | 2026-11-10                                                                                                                | 2026-11-10                                                                                                      | 2026-11-10                                                                                                                |
| **Streaming Endpoint**    | `wss://api.in.deepgram.com/v1/listen`                                                                                                                    | `wss://api.in.deepgram.com/v2/listen`                                                                 | `POST /v2/live` -> `wss://api.gladia.io/v2/live`                                             | `wss://streaming.assemblyai.com/v3/ws`                                                                                    | `wss://api.sarvam.ai/speech-to-text/ws`                                                                         | `wss://api.openai.com/v1/realtime`                                                                                        |
| **India Region Endpoint** | Documented (`api.in.deepgram.com`)                                                                                                                       | Documented (`api.in.deepgram.com`)                                                                    | None (`eu-west` \| `us-west` only; `ap-south-1` returns HTTP 400)                            | None documented (`global`, `us`, `eu`)                                                                                    | Documented (India / `ap-south-1`)                                                                               | `UNVERIFIED` (US/EU)                                                                                                      |
| **Mono Live Diarization** | Supported (`diarize=true`)                                                                                                                               | Unsupported (`TurnInfo` omits speaker)                                                                | Unsupported (`HTTP 400` on `/v2/live`)                                                       | Supported (`speaker_labels=true`)                                                                                         | `UNVERIFIED` (Omitted on stream API)                                                                            | `UNVERIFIED`                                                                                                              |
| **Keyterm / Vocabulary**  | `keyterm` (`<=100` `en-IN`; `<=500` tokens on `multi`)                                                                                                   | `keyterm` (`<=100`)                                                                                   | `custom_vocabulary` (`solaria-1` config)                                                     | `keyterms_prompt` (JSON array)                                                                                            | `UNVERIFIED` on `saaras:v3` WS (`saaras:v4` REST)                                                               | Session prompt instructions                                                                                               |
| **Zero-Retention Flag**   | `mip_opt_out=true` (API parameter)                                                                                                                       | `mip_opt_out=true` (API parameter)                                                                    | Enterprise contract only (no self-serve `/v2/live` ZDR flag)                                 | ZDR enterprise toggle / no training default                                                                               | Enterprise agreement                                                                                            | Enterprise BAA / ZDR                                                                                                      |
| **Mono Live Pricing**     | `$0.462/hr` base (`$0.0077/min`) + `+$0.078/hr` keyterms (`$0.540/hr` w/ KT); `multi`: `$0.552/hr` base (`$0.0092/min`) (`https://deepgram.com/pricing`) | `$0.462/hr` (`$0.0077/min` PAYG; `flux-general-multi` `$0.0078/min`) (`https://deepgram.com/pricing`) | `EUR 0.68/hr` (`~$0.740/hr` USD equiv) self-serve live (`https://gladia.io/pricing`)         | `$0.450/hr` Universal-3.6 Pro (KT incl.; session-duration billed; `$0.15/hr` base) (`https://www.assemblyai.com/pricing`) | `~INR 30/hr` (`~INR 0.50/min`, `~$0.350/hr`)                                                                    | `$1.02/hr` (`gpt-live-transcribe`, `$0.017/min`); Realtime tokens `~$3.60/hr` uncached (`https://openai.com/api/pricing`) |
| **2-Ch Live Pricing**     | `$0.924/hr` base (`2x` mono rate); `$1.104/hr` `multi`                                                                                                   | `UNVERIFIED` on `/v2/listen`                                                                          | `UNVERIFIED` on `/v2/live`                                                                   | `UNVERIFIED` on v3 WS                                                                                                     | `UNVERIFIED`                                                                                                    | `UNVERIFIED`                                                                                                              |

---

## 3. Host-to-Cloud Network Probe Measurements (`logs/bakeoff_eng_v2.log`)

| Endpoint          | Host                       | DNS P50 (ms) | TCP P50 (ms) | TLS P50 (ms) | WS Upgrade (ms) | Ping RTT P50 (ms) | Ping RTT P95 (ms) | RTT Delta vs Deepgram IN |
| :---------------- | :------------------------- | -----------: | -----------: | -----------: | --------------: | ----------------: | ----------------: | -----------------------: |
| Deepgram India    | `api.in.deepgram.com`      |          0.5 |         77.8 |         79.9 |           560.8 |              64.5 |             106.3 |                `+0.0 ms` |
| Gladia `eu-west`  | `api.gladia.io`            |          0.5 |        322.3 |        310.6 |           935.6 |             200.8 |             224.1 |              `+136.4 ms` |
| Gladia `us-west`  | `api.gladia.io`            |          0.6 |        196.4 |        217.1 |          2060.2 |             468.9 |             588.3 |              `+404.4 ms` |
| AssemblyAI Global | `streaming.assemblyai.com` |          5.8 |        221.9 |        242.3 |          1133.0 |             227.8 |             454.0 |              `+163.3 ms` |

---

## 4. Candidate Accuracy Matrix (`N = 45` Trials/Candidate; 95% Bootstrap CI)

| Candidate                             | N_ok | Err | Rec | Overall WER `[95% CI]`                             | English WER (`n=24`) | Hinglish WER (`n=21`) `[95% CI]` |   NTER |   IEMR | ID Match |    EER |
| :------------------------------------ | ---: | --: | --: | :------------------------------------------------- | -------------------: | :------------------------------- | -----: | -----: | -------: | -----: |
| Deepgram Nova-3 (`en-IN`, 85 KT)      |   45 |   0 |   0 | `0.330 [0.242, 0.414]`                             |             `0.1211` | `0.568 [0.509, 0.635]`           | 0.1111 | 0.1429 |   0.8571 | 0.2083 |
| Deepgram Nova-3 (`multi`, 65 KT)      |   45 |   0 |   0 | `0.370 [0.289, 0.445]`                             |             `0.2283` | `0.531 [0.489, 0.574]`           | 0.1111 | 0.4286 |   0.5714 | 0.2917 |
| Deepgram Flux (`flux-general-en`)     |   45 |   0 |   0 | `0.347 [0.256, 0.439]`                             |             `0.1315` | `0.593 [0.513, 0.676]`           | 0.1111 | 0.1429 |   0.8571 | 0.2083 |
| Gladia Solaria-1 (`cs_on`, `eu-west`) |   45 |   0 |   0 | `0.596 [0.401, 0.836]`                             |             `0.1233` | `1.135 [0.827, 1.478]`           | 0.3333 | 0.2857 |   0.7143 | 0.2917 |
| AssemblyAI Universal-3.6              |   45 |   0 |   0 | `0.471 [0.326, 0.616]`                             |             `0.0271` | `0.979 [0.953, 1.004]`           | 0.2222 | 0.0000 |   1.0000 | 0.2083 |
| Sarvam Saaras v3                      |    — |   — |   — | **NOT RUN** (`CREDITS EXHAUSTED`, WS close `1003`) |                    — | —                                |      — |      — |        — |      — |
| OpenAI Realtime                       |    — |   — |   — | **NOT RUN** (`MISSING KEY`: `OPENAI_API_KEY`)      |                    — | —                                |      — |      — |        — |      — |

---

## 5. Latency, Stability, Retraction & Pricing Matrix (Data Only)

| Candidate                 | First Partial P50 (ms) | Word Interim P50 (ms) | Stable Text P50 (ms) | Stable Text P95 (ms) | Retraction Rate | Final Flush P95 (ms) | RTT+ vs DG IN (ms) |       Mono `$/hr` | 2-Ch `$/hr` |
| :------------------------ | ---------------------: | --------------------: | -------------------: | -------------------: | --------------: | -------------------: | -----------------: | ----------------: | :---------- |
| Deepgram Nova-3 (`en-IN`) |                 1068.2 |                 754.8 |                864.6 |               2086.2 |           3.69% |                  0.0 |               +0.0 | 0.462 (+0.078 KT) | 0.924       |
| Deepgram Nova-3 (`multi`) |                 1213.8 |                 743.1 |                966.5 |               2390.0 |           5.74% |                  0.0 |               +0.0 |             0.552 | 1.104       |
| Deepgram Flux             |                  840.9 |                 426.0 |                608.9 |               2342.3 |           3.41% |               1725.8 |               +0.0 |             0.462 | UNVERIFIED  |
| Gladia Solaria-1          |                  944.1 |                 391.2 |               1558.8 |               6217.9 |          19.70% |                540.7 |             +136.4 |             0.740 | UNVERIFIED  |
| AssemblyAI Universal-3.6  |                 1104.5 |                1866.8 |               2080.1 |               5046.6 |           3.98% |                693.8 |             +163.3 |             0.450 | UNVERIFIED  |

---

## 6. 2-Speaker Mono Diarization & Gladia Ablation Data

### 6.1 2-Speaker Mono Sequential & 500 ms Overlap Probes (`turn_00` Male + `turn_01` Female)

- `POST https://api.gladia.io/v2/live?region=eu-west` with `{"diarization": true}`: `HTTP 400` (`400 Bad Request`).

| Candidate                 | Sequential 2-Spk Detected | Sequential Labels | Overlap 2-Spk Detected | Overlap Labels    |
| :------------------------ | ------------------------: | :---------------- | ---------------------: | :---------------- |
| Deepgram Nova-3 (`en-IN`) |                         2 | `['0', '1']`      |                      2 | `['0', '1']`      |
| Deepgram Nova-3 (`multi`) |                         2 | `['0', '1']`      |                      2 | `['0', '1']`      |
| Deepgram Flux             |                         0 | `['UNSUPPORTED']` |                      0 | `['UNSUPPORTED']` |
| Gladia Solaria-1          |                         0 | `['UNSUPPORTED']` |                      0 | `['UNSUPPORTED']` |
| AssemblyAI Universal-3.6  |                         2 | `['A', 'B']`      |                      2 | `['A', 'B']`      |

### 6.2 Gladia Ablations (`N_ID = 6`, `N_HINGLISH = 6`)

| Gladia Subset Configuration                                    |   N |    WER |   IEMR |   NTER |    EER | Retraction Rate |
| :------------------------------------------------------------- | --: | -----: | -----: | -----: | -----: | --------------: |
| `gladia_vocab_on_subset` (`custom_vocabulary=True`)            |   6 | 0.1422 | 0.3333 | 0.2000 | 0.1765 |          17.75% |
| `gladia_vocab_off_subset` (`custom_vocabulary=False`)          |   6 | 0.1946 | 0.5000 | 0.2000 | 0.3529 |          14.62% |
| `gladia_cs_on_hinglish` (`code_switching=True`, `['en','hi']`) |   6 | 1.1866 | 0.0000 | 0.5000 | 0.6000 |          30.71% |
| `gladia_cs_off_hinglish` (`code_switching=False`, `['en']`)    |   6 | 0.5792 | 0.0000 | 0.0000 | 0.2000 |          18.51% |

---

## 7. Hard Gates & Owner-Default Weighted Scorecard (`0..100`, Data Only)

Weights: Accuracy `0.35` (`0.40` WER + `0.25` Hinglish WER + `0.20` ID_Match + `0.15` NTER), Diarization `0.15`, Latency/Stability `0.20`, Vocabulary `0.10`, Cost `0.10`, Residency/Compliance `0.05`, Reliability/SDK `0.05`.  
Hard Gates: `G_Diar` (`seq_spk >= 2`), `G_RTT<=150` (`RTT+ms <= 150.0`), `G_Vocab`, `G_ZDR`, `G_Acc` (`mean WER <= 0.15` AND `ID_Match >= 0.85`).

| Candidate                 | `G_Diar` | `G_RTT<=150` | `G_Vocab` | `G_ZDR` | `G_Acc` | `ALL_PASS` | Weighted Score (`0..100`) |
| :------------------------ | :------: | :----------: | :-------: | :-----: | :-----: | :--------: | ------------------------: |
| Deepgram Nova-3 (`en-IN`) |   True   |     True     |   True    |  True   |  False  |   False    |                 **78.45** |
| Deepgram Nova-3 (`multi`) |   True   |     True     |   True    |  True   |  False  |   False    |                 **74.63** |
| Deepgram Flux             |  False   |     True     |   True    |  True   |  False  |   False    |                 **64.69** |
| AssemblyAI Universal-3.6  |   True   |    False     |   True    |  True   |  False  |   False    |                 **61.72** |
| Gladia Solaria-1          |  False   |     True     |   True    |  False  |  False  |   False    |                 **40.46** |
