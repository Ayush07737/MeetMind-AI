# ADR 012 — Two-Lane ASR Strategy, Provider Abstraction, and Empirical Decision Log

**Status**: Accepted Architecture (`Deepgram Nova-3` stays v1 Live Default behind `ASR_PROVIDER`; Decision Log Populated from Q-0d A1 Streaming & Async Runs; Synthetic-Only Caveat Applies)  
**Date**: 2026-11-12 (Stage Q-0d — Section C / Q7)  
**Decision makers**: Engineering & Speech Platform Team

---

## Context

MeetMind AI has two distinct speech recognition consumers with opposing latency vs. accuracy trade-offs:

1. **In-Meeting Real-Time Intelligence (Lanes 0–2, ADR-008)**: Needs low India network RTT (`api.in.deepgram.com` ~72.5 ms P50), rapid `time_to_stable_text` (`~1.3 s` P50) with low interim retraction (`< 4%`), live mono speaker diarization and dual-channel (`channels=2`) local/remote separation, engineering vocabulary boosting (`keyterm`), word-level confidence scores (`words[]` + `stable_prefix_words`) for ASR-uncertainty abstention (ADR-009), and self-serve zero-retention/no-training guarantees (`mip_opt_out=true`).
2. **Post-Meeting Accuracy & Decision Genome Persistence (§8, §12, §22)**: Executes without real-time UI pressure after meeting segments conclude, where Hinglish code-switching fidelity, engineering identifier precision (`PLAT-123`, `PR-482`), numeric exactness, and multi-speaker turn assignment matter more than first-word lag.

Before Q-0d, provider selection lacked transliterated Hinglish scoring (`indic-transliteration`, MIT), relied on small (`N=15`) samples without default vs. aggressive endpointing comparisons, carried a stale Deepgram pricing constant (`$0.0058/min`), and did not benchmark async accuracy-lane candidates.

---

## Decision

### 1. Two-Lane ASR Architecture

| Track                                                        | When It Runs                                                                                                 | Primary Optimization Objective                                                                                                                                                                                         | Production Configuration / Guardrails                                                                                                                                                                                                                                                      | Target Downstream Consumers                                       |
| :----------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------- |
| **Live Lane (Real-Time Streaming)**                          | Continuous during meeting (`gateway` -> `meetmind_asr`)                                                      | `time_to_stable_text` (`StabP50`), low retraction rate (`< 5%`), India-path RTT, live mono + 2-ch speaker attribution, custom vocabulary (`keyterm`), `words[]` confidence + `stable_prefix_words`, `mip_opt_out=true` | Remains `ASR_PROVIDER=deepgram` (**Nova-3**, `wss://api.in.deepgram.com/v1/listen`, `endpointing=10`, `interim_results=true`, `diarize=true`, `mip_opt_out=true`). Experimental alternatives live strictly behind `ASR_EXPERIMENTAL_PROVIDERS` and never alter default production routing. | §6 Trigger Classifier, §7/§11 Lane 0–2 Cards, §16 Live Transcript |
| **Accuracy Lane (Optional Async / Slightly Behind Meeting)** | Post-meeting (`meeting_ended`) or delayed clean-up pass when tenant accuracy policy enables re-transcription | Script-normalized Hinglish & English WER, identifier exact match (`ID_Match`), numeric exact match (`1 - NTER`), multi-turn speaker diarization (`2SpkAcc`), low per-hour cost                                         | Asynchronous batch pass feeding §8 Decision Genome reconciliation (`Claim` / `Decision` / `Outcome`) and §12 post-meeting digest when higher-accuracy Hinglish/code-switching or multi-speaker re-diarization is configured (subject to ADR-007/ADR-010 tenant residency/ZDR approval).    | §8 Decision Genome, §12 Digest, §22 Outcome Tracker               |

### 2. Provider Abstraction Boundary

- Production provider switching is strictly gated by `ASR_PROVIDER` (`deepgram | stub`) and touches only `backend/asr/meetmind_asr/config.py` and `backend/asr/meetmind_asr/providers/`.
- Candidate adapters (`deepgram_flux`, `gladia_solaria1`, `assemblyai_universal`, `sarvam`, `openai_streaming`) are isolated in `ml/eval/` and `providers/experimental/` behind `ASR_EXPERIMENTAL_PROVIDERS`.
- Runtime `meetmind_asr` remains contract-isolated (no imports from `meetmind_gateway`, zero file writes or transcript logging above `DEBUG`, `words[]` + `stable_prefix_words` overhead verified `+0.09 ms` P95 delta -> `1.04 ms` P95 vs `25.0 ms` limit in `logs/a2_stub_after_q0d.log`).

### 3. Scorecard Weights & Hard Gates

Weighted scorecard (`0..100`) and hard binary gates encoded in [`ml/eval/asr_bakeoff/run_bakeoff.py`](file:///e:/MeetMind%20AI/ml/eval/asr_bakeoff/run_bakeoff.py) and `docs/eval/asr-bakeoff-report.md`:

- **Dimension Weights**: Accuracy `0.35` (`0.40` Overall WER + `0.25` Transliterated Hinglish WER + `0.20` ID Match + `0.15` NTER), Diarization `0.15`, Latency & Stability `0.20`, Custom Vocabulary `0.10`, Cost `0.10`, Residency & ZDR Compliance `0.05`, Reliability / SDK `0.05`.
- **Hard Gates**:
  1. `G_Diar`: Sequential & overlap 2-speaker mono diarization supported (`speakers_detected >= 2`).
  2. `G_RTT<=150`: India-path ping RTT delta `<= +150.0 ms` vs `api.in.deepgram.com` (`72.5 ms` baseline).
  3. `G_Vocab`: Streaming keyterm / custom vocabulary boosting supported.
  4. `G_ZDR`: Self-serve or contracted Zero Data Retention / no-training flag enforced (`mip_opt_out=true` or equivalent).
  5. `G_Acc`: Synthetic/target graduation threshold `WER <= 0.15` AND `ID_Match >= 0.85`.

### 4. Cost per 100 Audio Hours by Configuration (Official Pricing, Verified 2026-11-11)

Deepgram rates reflect regular PAYG (`$0.0077/min` mono; `HTTP 403` on `/balances` prevents automated inspection of account-specific Growth/credit status; Growth rates `$0.0065/min` shown where applicable).

| Lane      | Provider & Configuration                      |         Mono Base `$/100h` | Add-ons Included                            | Mono w/ Add-ons `$/100h` | 2-Channel `$/100h` |
| :-------- | :-------------------------------------------- | -------------------------: | :------------------------------------------ | -----------------------: | -----------------: |
| **Live**  | Deepgram Nova-3 (`en-IN`, default/aggressive) | `$46.20` (`$39.00` Growth) | Keyterms (`$7.80`) + Diarization (`$12.00`) |             **`$66.00`** |    `$92.40` (base) |
| **Live**  | Deepgram Nova-3 (`multi`, default/aggressive) | `$55.20` (`$46.80` Growth) | Keyterms (`$7.80`) + Diarization (`$12.00`) |             **`$75.00`** |   `$110.40` (base) |
| **Live**  | Deepgram Flux (`flux-general-en`)             | `$46.20` (`$39.00` Growth) | Keyterms (`$7.80`; no diarization)          |                 `$54.00` |       `UNVERIFIED` |
| **Live**  | Gladia Solaria-1 Live (`eu-west`)             |   `$73.40` (`EUR 0.68/hr`) | Custom vocab incl.; diar `UNSUPPORTED`      |                 `$73.40` |          `$146.90` |
| **Live**  | AssemblyAI U-3.6 Pro (global/us)              |                   `$45.00` | Keyterms incl.; Diarization (`+$12.00`)     |                 `$57.00` |           `$90.00` |
| **Live**  | OpenAI `gpt-live-transcribe` (`NOT RUN`)      |   `$102.00` (`$0.017/min`) | Prompt keywords; no speaker labels          |                `$102.00` |       `UNVERIFIED` |
| **Async** | Deepgram Nova-3 Async (`en-IN`)               |                   `$25.80` | Keyterms (`$7.80`) + Diarization (`$12.00`) |                 `$45.60` |    `$51.60` (base) |
| **Async** | AssemblyAI U-3.5 Pro Async                    |                   `$21.00` | Keyterms (`$5.00`) + Diarization (`$2.00`)  |                 `$28.00` |    `$42.00` (base) |
| **Async** | Gladia Solaria-1 Async                        |   `$29.20` (`EUR 0.27/hr`) | Diarization + Custom vocab included         |                 `$29.20` |           `$58.40` |
| **Async** | Gladia Solaria-3 Async (5 langs)              |   `$17.30` (`EUR 0.16/hr`) | Diarization + Custom vocab included         |             **`$17.30`** |           `$34.60` |
| **Async** | OpenAI `gpt-transcribe` (`NOT RUN`)           |   `$27.00` (`$0.0045/min`) | `UNVERIFIED`                                |                 `$27.00` |       `UNVERIFIED` |

---

## Decision Log (Populated from 2026-11-12 Q-0d Empirical Data)

_All metrics below cite `logs/bakeoff_streaming_q0d.log` (`N_eff=150`, `3` reps) and `logs/bakeoff_async_q0d.log` (`N_eff=150`). Because synthetic corpus has `owner_verified_count=0`, decisions remain subject to validation on owner-recorded Indian-English/Hinglish meetings before GA._

1. **Live Default Retained — Deepgram Nova-3 (`api.in.deepgram.com`, `en-IN` baseline + `multi` evaluation track)**:
   - **Evidence**: Nova-3 (`en-IN` `[default]` `Score=73.89`, `multi` `[default]` `Score=73.73`) is the **only** evaluated candidate passing all four operational hard gates (`G_Diar=True`, `G_RTT=True` at `+0.0 ms`, `G_Vocab=True`, `G_ZDR=True`). It delivers `StabP50 = 1325.6 ms` (`en-IN`) / `1341.6 ms` (`multi`), low retraction (`3.71%` / `3.87%`), live mono `2-spk` + native `2-ch` stereo support, and India residency (`api.in.deepgram.com`).
   - **Action on `en-IN` vs `multi` for Hinglish**: Transliterated scoring (`indic-transliteration`, MIT) proved that **`Nova-3 (multi)` cuts overall synthetic WER from `0.414` (`en-IN`) to `0.245` (`[0.222, 0.269]`)** and **Hinglish transliterated WER from `0.5416` to `0.2830`** (with Hinglish `ID_Match = 1.000`), because `multi` transcribes Hindi words phonetically into Devanagari/Mixed script rather than distorting them into English approximations (`+9.0/100h` cost delta: `$75` vs `$66`). However, before switching the tenant language default from `en-IN` to `multi`, Q-1/real-audio testing must measure downstream entity/number extraction behavior over Mixed Devanagari+Latin vs ASCII Romanized output (note `en-IN` had lower `H_EngWER=0.2726` vs `0.5022` on Hinglish English tokens).
   - **Aggressive Endpointing Note**: Adding `utterance_end_ms=1000, vad_events=true` reduced Nova-3 `EOT_P50` from `2434 ms` to `652 ms` (`en-IN`) with zero WER degradation (`0.414 -> 0.414`), at a small `+202 ms` `StabP50` shift (`1325.6 -> 1527.3 ms`). We keep `[default]` (`endpointing=10`) unchanged in production today until real human turn-pause telemetry confirms no premature split during code-switching pauses.

2. **Candidates Not Selected for v1 Live Default (With Evidence)**:
   - **Deepgram Flux**: Fastest `WordLag50 = 829.5 ms`, `StabP50 = 1207.8 ms` (`[default]`), and `[aggressive]` `EOT_P50 = 224.4 ms` (`Eager50 = 64.9 ms`), lowest retraction (`3.15%`). Rejected as v1 meeting default because **`G_Diar = False`** (`TurnInfo` omits mono speaker labels) and higher synthetic WER (`0.454` / `0.448`). Best reserved for future single-user push-to-talk / 2-channel mic-only modes.
   - **Gladia Solaria-1 Live**: Rejected as v1 default — fails **`G_Diar`** (live `POST /v2/live` with `{"diarization": true}` rejected with `HTTP 400`, `request_id: "G-90f5c799"`), fails **`G_ZDR`** on self-serve (no ZDR, default training unless Enterprise), high retraction (`13.02%–14.80%`), slow stabilization (`StabP50 = 2696–2790 ms`, `StabP95 = 6.38–7.07 s`), EU/US-only (`+136.7 ms` RTT), and highest tested live cost (`$73.40/100h` mono).
   - **AssemblyAI Universal-3.6 Pro**: Achieved the best live `ID_Match` (`0.865` / `0.861`) and lowest `NTER` (`0.130` `[aggressive]`), supporting mono speaker labels. Not selected for v1 live default due to slow word stabilization (`StabP50 = 3313.8 ms` `[default]` / `3225.0 ms` `[aggressive]`, missing Lane 0/Lane 1 targets), `+142.0 ms` India RTT delta (no India region), `G_ZDR = False` without Enterprise agreement, and **account concurrent-WebSocket quota failures (`HTTP/WS 1008 policy violation` on ~300/450 streams at concurrency `5`, vs clean `concurrency=2` probe)**.
   - **Sarvam Saaras (`v4/v3`) & OpenAI (`gpt-live-transcribe`)**: Status **`NOT RUN`** (`Sarvam`: `HTTP 402` credits exhausted; `OpenAI`: `OPENAI_API_KEY` absent). OpenAI also carries `$1.02/hr` cost, no live speaker labels, and no India audio residency (ADR-010).

3. **Async Accuracy Lane Recommendation (Post-Meeting / §8 Feed, Pending Residency Review)**:
   - On `N_eff = 150` pre-recorded evaluation (`logs/bakeoff_async_q0d.log`):
     - **`Gladia Solaria-3 Async`** achieved the lowest overall WER (`0.262 [0.228, 0.299]`), highest `ID_Match` (`0.7429`), lowest `EER` (`0.2823`), strong `2SpkAcc` (`0.7334`), zero errors (`150/150`), and lowest cost (`$17.30/100h`).
     - **`AssemblyAI U-3.5 Pro Async`** achieved the highest 2-speaker conversation turn assignment (`2SpkAcc = 0.7667`) and lowest `NTER` (`0.2059`) at `$28.00/100h`.
     - **`Deepgram Nova-3 Async` (`en-IN`)** delivered the fastest turnaround (`5.13 s` P50 vs `13.1–13.7 s`) and `mip_opt_out=true` India/global ZDR compliance at `$45.60/100h` (with `H_EngWER = 0.2118`), though `en-IN` lagged on mixed Hinglish WER (`0.413`).
   - **Architectural Action**: Wire the optional §12 / §8 **Post-Meeting Accuracy Lane** behind a tenant policy switch: tenants requiring India/ZDR compliance today use **Deepgram Nova-3 Async** (evaluate `multi` async next once real recordings arrive); US/EU non-strict-residency tenants opting into Enterprise ZDR can utilize **Gladia Solaria-3 Async** (`$17.30/100h`) or **AssemblyAI U-3.5 Pro Async** (`$28.00/100h`) to reconcile Decision Genome claims/numbers after the meeting.

---

## Consequences

- `meetmind_asr` stays on `ASR_PROVIDER=deepgram` (`Nova-3`, `api.in.deepgram.com`, `mip_opt_out=true`) with `words[]` and `stable_prefix_words` live.
- No candidate passes `G_Acc` (`WER <= 0.15` & `ID_Match >= 0.85`) across noisy/telephony/overlapping synthetic Edge TTS speech; final production tuning (`en-IN` vs `multi` and `utterance_end_ms`) awaits **real owner-recorded engineering meetings** (`A6`).
- All external held-out quality gates (`Clause Q-C4`) remain decoupled from ASR provider selection via word-confidence uncertainty gating (`meetmind_quality.policy`).
