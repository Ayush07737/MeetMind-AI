# ADR 012 — Two-Lane ASR Strategy, Provider Abstraction, and Empirical Decision Log

**Status**: ASR selection closed (Deepgram Nova-3 frozen as v1 Live Default; Smallest.ai Pulse evaluated head-to-head vs hard gates; Decision Log and Revisit Triggers finalized)  
**Date**: 2026-11-13 (Updated Stage Q-0e — Final Bake-off & Strategic Freeze)  
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

## Decision Log & Final Findings (Stage Q-0e)

_All empirical metrics cite `logs/bakeoff_streaming_q0d.log` (`N_eff=150`, `3` reps), `logs/bakeoff_async_q0d.log` (`N_eff=150`), `logs/bakeoff_pulse_h2h.json`, and `logs/region_probe_results.json`. With Stage Q-0e complete, ASR provider selection is closed and frozen._

### 1. Live Lane Frozen Default: Deepgram Nova-3 (`api.in.deepgram.com`)

- **Status**: **FROZEN PRODUCTION DEFAULT** behind `ASR_PROVIDER=deepgram`.
- **Evidence Base**: Passed all 4 operational hard gates (`G_Diar=True`, `G_RTT=True` at `+0.0 ms`, `G_Vocab=True`, `G_ZDR=True`). Stable India network path (`72.5 ms` P50), stable text `1325.6 ms` P50, low retraction (`3.71%`), live 2-speaker mono diarization, and dual-channel local/remote stereo.
- **Endpointing Stance**: Production default is frozen at `endpointing=10ms(default)`. Aggressive endpointing (`utterance_end_ms=1000`) remains behind experimental configuration flags until live human conversational telemetry validates turn pause behavior.

### 2. Smallest.ai Pulse Streaming: Head-to-Head Evaluation vs Hard Gates

Evaluated head-to-head against Deepgram Nova-3 in Stage Q-0e (`wss://api.smallest.ai/waves/v1/stt/live`, `waves-api.smallest.ai`):

- **`G_RTT<=150`**: **PASS**. Co-located in AWS Mumbai (`ap-south-1`). Ping latency `48.0 ms` P50, WebSocket upgrade `437.0 ms`, RTT delta `-24.5 ms` vs Deepgram baseline (`72.5 ms`).
- **`G_ZDR`**: **PASS**. Zero data retention and no model training guarantee confirmed by provider terms.
- **`G_Diar`**: **FAIL**. Returns 0 speakers on live streaming audio (`speakers_detected=0`, `speaker_labels=['UNSUPPORTED']`). Live mono multi-speaker diarization is absent from the streaming API; diarization is batch-only or requires enterprise custom enablement.
- **`G_Vocab`**: **FAIL**. No streaming keyterm prompting or custom vocabulary boosting parameters exist in the live API schema.
- **`G_Acc`**: **FAIL** vs Nova-3 `multi`. Pulse Hinglish transliterated WER scored `0.476 [0.426, 0.522]` (vs Nova-3 `multi` `0.289 [0.207, 0.379]`). Pulse identifier match scored `0.400` (vs Nova-3 `multi` `0.660`).
- **Configuration & Documentation Note**: Official documentation (`docs.smallest.ai`) contains no `finalize_immediate` parameter; client flush is triggered via `{"type": "finalize"}`. Default evaluated language was `language=en`; Indic/code-switch options include `language=north_indic` and `language=hi`. The speculative aggressive configuration label was removed as unverified.

### 3. Deepgram Nova-3 `multi` vs `en-IN` Model Trade-off

- **Accuracy Comparison**:
  - Nova-3 `multi`: English WER `0.153 [0.105, 0.204]`, Hinglish transliterated WER `0.289 [0.207, 0.379]`, 85/15 composite WER `0.173 [0.131, 0.219]`, 70/30 composite WER `0.194 [0.152, 0.239]`.
  - Nova-3 `en-IN`: English WER `0.163 [0.107, 0.226]`, Hinglish transliterated WER `0.550 [0.470, 0.631]`, 85/15 composite WER `0.221 [0.169, 0.278]`, 70/30 composite WER `0.279 [0.230, 0.334]`.
- **Script Finding & Downstream Impact**:
  - While `multi` demonstrates markedly superior phonetic accuracy on Hinglish phrases, it emits mixed Devanagari and Latin scripts (`267` Latin / `36` Devanagari tokens).
  - In contrast, `en-IN` outputs 100% Romanized Latin characters (`{"Latin": 303}`).
  - Because downstream trigger classification (§6) and card extraction (§7/§11) expect Romanized text, selecting `multi` requires inline transliteration normalization (`indic-transliteration`, MIT license) before downstream queue dispatch.

### 4. Harness End-of-Turn (EOT) Latency Flush Artefact

- **Observation**: Early bake-off iterations reported `EOT_P50 = 0.0 ms` for several aggressive endpointing configurations.
- **Root Cause**: The latency harness previously measured elapsed time from the last transmitted audio chunk including trailing synthetic silence. When providers finalized turns before silence packets finished transmitting or upon silence detection, negative or zero durations were clamped to `0.0 ms`.
- **Fix Applied**: The timing harness now anchors measurements strictly to the last active speech frame and explicitly flags clamped zero values, reporting true end-of-turn finalization latency.

### 5. Alternative Candidate Disposition Summary

- **Deepgram Flux**: Lowest first-word lag (`829.5 ms`) and EOT (`224.4 ms`), but rejected for v1 meetings due to lack of speaker diarization (`G_Diar=False`) and higher overall WER (`0.454`).
- **Gladia Solaria-1 Live**: Rejected due to unsupported live diarization (`HTTP 400`), lack of self-serve ZDR, high interim retraction (`13.0%–14.8%`), and cross-region RTT penalty.
- **AssemblyAI Universal-Streaming**: Strong identifier accuracy but disqualified by slow stabilization (`3.2–3.3 s`), cross-region US latency (`+142 ms`), and concurrency caps (`WS 1008`).
- **Soniox Real-Time / Sarvam Saaras**: Disqualified due to account balance exhaustion (`402`).
- **OpenAI Streaming**: Excluded from live meeting pipeline due to absence of speaker diarization and high cost ($0.017/min).

---

## ASR FREEZE & REVISIT TRIGGERS

**ASR selection is officially CLOSED and FROZEN.**
All speech recognition development is locked to `ASR_PROVIDER=deepgram` (`Nova-3`). Engineering effort will not be diverted to further provider evaluations, adapter benchmarking, or vendor chasing.

### Explicit Revisit Triggers

The ASR selection will only be reopened if one of the following five conditions is triggered:

1. **Real-Audio Meeting Results Divergence**: Empirical testing on owner-recorded real human meetings shows conversational code-switching WER or speaker attribution accuracy diverging by more than **10%** from synthetic calibration benchmarks.
2. **New Cloud Region Requirement**: An enterprise tenant requires local data residency in a geographic region where Deepgram does not operate an in-region Point of Presence (PoP).
3. **Vendor Pricing Disruption**: Deepgram alters pricing by more than **30%**, or a competing enterprise-grade provider offers comparable capability at $\ge 50\%$ cost reduction.
4. **Outage / SLA Breach**: Deepgram experiences sustained availability drops below contract SLA or fails the streaming WebSocket latency budget ($> 200\text{ ms}$ P95) over production workloads.
5. **Smallest.ai Feature Enablement**: Smallest.ai officially ships documented live streaming multi-speaker diarization (`G_Diar`) and streaming custom keyterm vocabulary boosting (`G_Vocab`), resolving its two failing hard gates.

---

## Consequences

- `meetmind_asr` is frozen on `ASR_PROVIDER=deepgram` (`Nova-3`, `api.in.deepgram.com`, `mip_opt_out=true`) with `words[]` and `stable_prefix_words` enabled.
- Engineering pack identifier recovery (`ml/eval/identifier_recovery/`) bridges synthetic identifier gaps (+10.2% match gain, 0% wrong substitutions) independently of vendor acoustic models.
- Downstream stages (§6 Trigger Classifier, §7 Decision Engine, §11 Cards) proceed with guaranteed ASR contracts and known timing envelopes.
