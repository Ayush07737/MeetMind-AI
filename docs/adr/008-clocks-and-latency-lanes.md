# ADR 008 — Clocks, Synchronized T0 Anchor, and Proposed Latency Lanes

**Status**: Accepted (Clocks & Budget Constraints); Lanes 0–2 Proposed Design (`NOT MET` until live T0-clock run)  
**Date**: 2026-11-12 (Stage Q-0d; formalizing Q-0 Q1, Q2, and Addendum Section C)  
**Decision makers**: Engineering & Platform Architecture Team

---

## Context

MeetMind AI promises real-time in-meeting intelligence ("tight loop: speech → transcript → trigger → retrieval → reasoning → display") where a late or fabricated insight is worse than none. Prior to Q-0, Gateway (#1) and ASR (#2) logged timestamps using un-calibrated client capture clocks (`capture_ts_ms` in the 18-byte audio binary header) and ASR-service receipt clocks (`time.perf_counter()` after Redis dequeue). Under client clock skew of even ±2.5 seconds, end-to-end latency measurements become invalid, negative, or un-auditable.

Additionally, benchmark runs across unique utterances show substantial P95 sampling variance between identically configured 30-minute sessions (e.g., `1859.91 ms` vs `2177.88 ms` across 510 correlated finals over 85 unique utterances), requiring robust statistical gating rather than noisy pooled tail thresholds.

---

## Decision

### 1. Canonical T0 Anchor & SNTP WebSocket Clock Synchronization

- **Canonical Clock**: All stage timestamps across Gateway (#1), ASR (#2), and downstream pipeline components (§6–§18) MUST use **server UTC epoch milliseconds**.
- **T0 Definition** (`docs/latency/clocks.md`):
  $$T_0 = \text{capture\_server\_ms} + \text{utterance\_offset\_ms}$$
  where $\text{capture\_server\_ms} = \text{capture\_ts\_ms} + \Delta_{\text{offset}}$ is the capture time of the final audio sample triggering an event, translated onto the server clock by the Gateway.
- **Protocol (`clock_sync` / `clock_sync_reply` / `clock_report`)**:
  - Clients execute 3 SNTP-style probes (`t1 -> (t1, t2, t3) -> t4`) at `meeting_start` and every `60 s`, selecting the sample with minimum RTT:
    $$\text{RTT} = (t_4 - t_1) - (t_3 - t_2), \qquad \Delta_{\text{offset}} = \frac{(t_2 - t_1) + (t_3 - t_4)}{2}$$
  - Gateway stores `clock_offset_ms` in `mm:{tenant}:session:{meeting}`, enriches every audio stream entry in `mm:{tenant}:audio:{meeting}` with `capture_server_ms`, and audits `clock_sync_established` plus `clock_skew_detected` whenever $|\Delta_{\text{new}} - \Delta_{\text{previous}}| > 500\text{ ms}$.
- **Mathematical Error Bound**: Under asymmetric uplink/downlink delays ($d_{\text{up}} \ne d_{\text{down}}$), offset error satisfies $|\epsilon| = |d_{\text{up}} - d_{\text{down}}| / 2 \le \text{RTT} / 2$. Verified in unit tests (`test_offset_estimator_asymmetric_delay`: `20 ms` up / `120 ms` down yields worst-case error `50 ms`) and real-gateway acceptance tests (`test_gateway_clock_sync_acceptance` with `-2.5 s` simulated skew achieves `<= 100 ms` stream error).
- **Stage Tracing (`EventEnvelope.timing`)**: Nine optional server-epoch fields (`t0_capture`, `gw_recv`, `asr_recv`, `provider_msg`, `asr_pub`, `classify_pub`, `retrieval_done`, `card_pub`, `ui_render`) enforced monotonic via `meetmind_schemas.timing.validate_monotonic()`.

### 2. Budget as Code & Hard Classification Technology Constraint (Addendum C)

All stage latency ceilings, memory slope headrooms, and own-overhead allowances live declaratively in `docs/latency/budget.yaml` (loaded by Python & TS helpers and enforced by `scripts/budget_check.py`).

> **Mandatory Architectural Constraint (Addendum Section C — §6 Design Rule)**:
> The real-time trigger classification stage (`asr_pub -> classify_pub`: **target $\le 200\text{ ms}$ P50, $\le 300\text{ ms}$ P95**) **CANNOT use a generative LLM API call**. External LLM round-trips (typically $>800\text{ ms}$ + regional TLS/prefill variance) violate the classification ceiling by construction. §6 MUST be implemented exclusively with deterministic pattern/lexicon rules, regex cascades, or small local models/embeddings executing in $\le 50\text{ ms}$.

### 3. Latency Lanes Architecture (Two-Track Speed vs. Depth)

To reconcile the 500 ms aspirational interim target against empirical cloud ASR floors (~585–608 ms interim P50 and ~434–440 ms final P50 on `api.in.deepgram.com` measured on ASR-receipt clock), MeetMind AI adopts explicit lanes:

| Lane                   | Scope                                                  | Clock Interval                   | Target P50 | Target P95 | Mechanism / Constraint                                                                                         | Status                               |
| :--------------------- | :----------------------------------------------------- | :------------------------------- | :--------: | :--------: | :------------------------------------------------------------------------------------------------------------- | :----------------------------------- |
| **Lane 0 — Instant**   | Live transcript + deterministic entity/ticket/PR chips | `t0_capture -> ui_render`        | `1000 ms`  | `2000 ms`  | Streaming ASR interim (`stable_prefix_words`) + regex/lexicon (`<= 40 ms`) + local `ContextCache`              | Proposed (`NOT MET` on T0 clock yet) |
| **Lane 1 — Retrieval** | Tier-1 verified `InsightCard` (`kind=fact`)            | `t0_capture -> ui_render`        | `1500 ms`  | `3000 ms`  | Rules/local classifier (`<= 150 ms`) + vector/cache lookup (`<= 450 ms`) + zero-LLM deterministic verification | Proposed (`NOT MET` on T0 clock yet) |
| **Lane 2 — Reasoned**  | Tier-2 analytical revision (`revision > 1`)            | `t0_capture -> ui_render`        |   `null`   | `4000 ms`  | Async/slow-path LLM reasoning gated through `verify_card()` + higher-revision UI replace                       | Proposed (`NOT MET` on T0 clock yet) |
| **Tier 3 / Action**    | Action recommendation (`kind=action_recommendation`)   | `t0_capture -> card_pub`         | `3500 ms`  | `5000 ms`  | User-approved Trust Gate execution (external mutation itself has no latency SLA)                               | Contract Ceiling                     |
| **Post-Meeting**       | Digest, Accuracy ASR pass & Outcome linkage            | `meeting_ended -> summary_ready` |   `null`   |   `null`   | Accuracy-first async ASR + Decision Genome §22 linkage                                                         | No real-time target                  |

_Note_: The legacy `500 ms` speech-to-interim goal is preserved in `budget.yaml` strictly with `gate: false, aspirational` and is never hidden behind a synthetic pass.

### 4. Statistical Gating & T0-Clock Remaining Gaps (`scripts/budget_check.py`)

- **Log Provenance**: `budget_check.py` requires an explicit `--log` path, prints its `LastWriteTime`, and never confuses ASR-receipt-clock measurements with full T0-clock measurements.
- **Per-Unique-Utterance Median Gate (A3)**: Rather than gating on unstable single-run pooled P95s (which swing ~300 ms across 510 correlated finals from 85 unique utterances), `budget_check.py` groups finals by normalized text/utterance key, computes the per-unique-utterance median P95 and a deterministic bootstrap 95% CI upper bound (`gate <= 3500 ms` ceiling, informational comparison vs `target_p95_ms = 2000 ms`), and integrates live A1 `time_to_stable_text` from `--bakeoff-json` as the Lane 0 ASR ceiling indicator.
- **Service Overhead Gate**: Own pipeline overhead across Gateway + ASR + stub downstream stages MUST stay `<= 25.0 ms` P95 per stage (excluding simulated provider/network allocations).
- **Remaining Unmeasured T0 Legs** (explicitly tracked in `budget.yaml` `lanes.t0_gap_measurements`):
  1. `client_capture_to_gateway_ingress_ms` (Extension §3 / Desktop §4 not yet live-benchmarked)
  2. `clock_sync_residual_error_ms` under real wireless jitter in field conditions
  3. `gateway_to_redis_and_asr_recv_ms` in production topology
  4. `outbound_card_pub_to_ui_render_ms` (Frontend §16 DOM paint leg)

---

## Consequences

- §6 (Trigger Classifier), §10 (Pre-Meeting Brief), §11 (Live Agent), and §16 (Frontend) cannot merge into production if their timing telemetry violates `budget.yaml` ceilings or invokes an LLM inside §6.
- All performance reports MUST cite exact log paths, `LastWriteTime`, `N == total`, and explicitly distinguish ASR-receipt-clock baselines from end-to-end T0-clock measurements.
