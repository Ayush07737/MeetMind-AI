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

## Decision Log (PROVISIONAL — Subject to Real-Audio Verification & Freeze Criteria)

_All empirical metrics cite `logs/bakeoff_streaming_q0d.log` (`N_eff=150`, `3` reps), `logs/bakeoff_async_q0d.log` (`N_eff=150`), and `logs/region_probe_results.json` (Q-0e). Because the synthetic corpus has `owner_verified_count=0`, all decisions remain **PROVISIONAL** pending validation on real human meetings with the exact missing evidence documented below._

1. **Live Lane Default: Deepgram Nova-3 (`api.in.deepgram.com`) — [PROVISIONAL]**:
   - **Status**: PROVISIONAL Live Default behind `ASR_PROVIDER=deepgram`.
   - **Evidence Base**: Passed all 4 hard gates (`G_Diar=True`, `G_RTT=True` at `+0.0 ms`, `G_Vocab=True`, `G_ZDR=True`). Low India RTT (`72.5 ms` P50), stable text `1325.6 ms` P50, low retraction (`3.71%`), live 2-speaker mono and 2-channel stereo.
   - **Missing Evidence Required for Freeze**:
     - Validation on owner-recorded real human engineering meetings (natural code-switching pauses and unprompted speaker overlap).
     - Downstream card accuracy verification comparing `multi` (Devanagari/Latin script mix) vs `en-IN` (Romanized text) to ensure trigger classifiers handle script variance without degradation.
   - **Endpointing Stance**: Production default remains `endpointing=10ms(default)`; aggressive tuning (`utterance_end_ms=1000`) is held in experimental lane until human conversational pause telemetry proves no mid-sentence turn splitting.

2. **Alternative Live Candidates — [PROVISIONAL DECISIONS]**:
   - **Smallest.ai Pulse Streaming**: [PROVISIONAL EVALUATION] Evaluated in Q-0e (`wss://api.smallest.ai/waves/v1/stt/live`, `waves-api.smallest.ai`). Co-located in Mumbai AWS (`ap-south-1`, WS upgrade `437 ms`, ping `48 ms`). Connects and returns live interims and finals. Missing evidence: multi-speaker live diarization accuracy over extended meetings and enterprise data residency guarantees.
   - **Deepgram Flux**: [PROVISIONAL REJECTION FOR V1 MEETING DEFAULT] Fastest first-word lag (`829.5 ms`) and end-of-turn latency (`224.4 ms`), but fails `G_Diar` (omits mono speaker labels) and higher synthetic WER (`0.454`). Retained as experimental candidate for push-to-talk / single-user dictation.
   - **Gladia Solaria-1 Live**: [PROVISIONAL REJECTION FOR V1 LIVE] Confirmed unsupported live diarization (`HTTP 400`, `property diarization should not exist`), fails self-serve ZDR, high retraction (`13.0%–14.8%`), slow stabilization (`2.7–2.8 s`), and cross-region RTT penalty.
   - **AssemblyAI Universal-Streaming**: [PROVISIONAL REJECTION FOR V1 LIVE] Strong identifier and numeric accuracy, but slow word stabilization (`3.2–3.3 s`), lacks India hosting (`+142 ms` RTT), and suffers WebSocket concurrency rejections (`WS 1008`) on standard tier accounts.
   - **Soniox Real-Time**: [PROVISIONAL NOT RUN — NO CREDITS] WebSocket probe authenticates, but returns `error_code: 402 (organization_balance_exhausted)`.
   - **Sarvam Saaras**: [PROVISIONAL NOT RUN — NO CREDITS] Account credits exhausted (`HTTP 402`).
   - **OpenAI Streaming**: [PROVISIONAL NOT RUN — MISSING KEY] `OPENAI_API_KEY` absent from environment.

3. **Accuracy Lane Recommendation (Post-Meeting / §8 Decision Genome) — [PROVISIONAL]**:
   - **Status**: PROVISIONAL Recommendation for post-meeting digest and graph reconciliation.
   - **Evidence**: On synthetic pre-recorded evaluation, **Gladia Solaria-3 Async** achieved lowest WER (`0.262`) and lowest cost (`$17.30/100h`); **AssemblyAI U-3.5 Async** achieved best 2-speaker turn assignment (`0.7667`) at `$28.00/100h`; **Deepgram Nova-3 Async** delivered fastest turnaround (`5.13 s`) and verified India ZDR compliance at `$45.60/100h`.
   - **Missing Evidence Required for Freeze**: Signed tenant enterprise agreements for GDPR/DPDP zero-data retention on Gladia/AssemblyAI before enabling them for production tenant data.

---

## ASR FREEZE CRITERIA (Owner Gate to Permanently Halt Provider Evaluation)

To prevent ongoing benchmark churn and permanently freeze the ASR tier so development focuses exclusively on core product intelligence (§6–§22), the following explicit freeze criteria must be satisfied:

1. **Owner 30-Minute Instrument Run Verified**:
   - One valid 30-minute stats run on the owner's machine with 0 stalls, pacing error $\le 0.05\%$, zero invalidating Modern Standby overlaps (> 1.0s), and pipeline overhead P50 $\le 10\text{ ms}$, P95 $\le 25\text{ ms}$.
2. **Worker Handover & Lossless Recycle Verified**:
   - Automated acceptance test (`test_worker_recycle.py`) passes against real Redis and Neon, proving meetings hitting the 3,600s cap transfer to replacement workers with 0 lost chunks, 0 missing gaps, and exactly one `asr_session_summary` where `chunks_sent == chunks_acked`.
3. **Production Configuration Frozen**:
   - **Live Provider**: `ASR_PROVIDER=deepgram`, `model=nova-3`, `endpointing=10`, `diarize=true`, `mip_opt_out=true`.
   - **Base URL**: Default resolved dynamically via `ASR_REGION` (`in` $\to$ `wss://api.in.deepgram.com/v1/listen`).
   - **Identifier Recovery Engine**: Active in downstream extraction pipeline (`ml/eval/identifier_recovery/`), delivering $> 73\%$ match rate with $0.00\%$ wrong substitutions.
4. **Permanent Freeze Trigger**:
   - Upon owner sign-off of Q-0e report and the valid 30-minute stats confirmation, all ASR provider exploration is **OFFICIALLY FROZEN**.
   - No further provider bake-offs, alternative adapters, or vendor feature chases will be conducted. All speech recognition code is locked except for regression fixes and dictionary/keyterm additions.

---

## Consequences

- `meetmind_asr` stays on `ASR_PROVIDER=deepgram` (`Nova-3`, `api.in.deepgram.com`, `mip_opt_out=true`) with `words[]` and `stable_prefix_words` live.
- Candidate-set identifier recovery (`ml/eval/identifier_recovery/`) bridges the synthetic identifier gap (+10.2% match gain, 0% wrong substitutions) without waiting for vendor model updates.
- All external held-out quality gates (`Clause Q-C4`) remain decoupled from ASR provider selection via word-confidence uncertainty gating (`meetmind_quality.policy`).
