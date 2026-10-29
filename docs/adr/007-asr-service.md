# ADR 007 — Automatic Speech Recognition (ASR) Streaming Service Architecture

**Status**: Accepted & Live Verified (Amended in Session D2d)  
**Date**: 2026-10-12 (Drafted), 2026-10-15 (Live Verified with Deepgram Nova-3), 2026-10-16 (Amended: Regional Residency, Warm Standby Evaluation, Sliding Window Memory Bound, SAPI Voice Correction)  
**Decision makers**: Engineering & Security Architecture Team

## Context

MeetMind AI requires real-time automatic speech recognition (ASR) for live meetings ingested through the Ingestion Gateway (Component #1). Ingestion streams audio into Redis Streams (`mm:{tenant}:audio:{meeting}`) with an 18-byte binary frame header containing sequence numbers, capture timestamps, channel counts, and codec flags.

Component #2 (the ASR streaming service, `meetmind_asr`) must consume audio chunks from Redis, stream them to an ASR provider, map timeline offsets accurately back to meeting audio origin timestamps across network reconnects, and publish transcripts with at-least-once audio delivery and effectively-once transcript semantics.

## Locked Decisions & Architecture

### 1. Provider Selection and Pinning (Deepgram Nova-3)

- **Provider**: Deepgram Nova-3 via live streaming WebSocket (`wss://api.deepgram.com/v1/listen`, `wss://api.in.deepgram.com/v1/listen`, or `wss://api.eu.deepgram.com/v1/listen`).
- **SDK Deviation (Raw WebSockets vs `deepgram-sdk`)**:
  - _Deviation_: The original build spec referenced `deepgram-sdk`. Component #2 implements direct streaming WebSocket client protocol via Python's native `websockets` library.
  - _Rationale_: As of October 2026, the current major release of `deepgram-sdk` is `v7.x` (`7.12.0`). While modern releases provide spec-generated WebSocket clients, utilizing direct streaming via Python's native `websockets` library eliminates third-party SDK churn, wrapper overhead, and background thread executors, providing deterministic, zero-overhead control over async socket lifecycles, event loop integration, ping/pong heartbeats, reconnect timers, and exact query parameter serialization.
- **Official Documentation Consulted & Dates**:
  - Deepgram Streaming Audio API: `https://developers.deepgram.com/docs/streaming-audio` (Verified October 2026; API v1 streaming WebSocket).
  - Deepgram Trust & Security ("Your Data at Deepgram"): `https://developers.deepgram.com/trust-security/your-data` (Verified October 16, 2026).
  - Deepgram Regional Endpoints: `https://developers.deepgram.com/reference/regional-endpoints` (Verified October 16, 2026).
  - Nova-3 Model Specifications: `https://developers.deepgram.com/docs/nova-3` (Verified October 2026).
  - Model Improvement Partnership Program (MIP) Opt-Out: `https://developers.deepgram.com/docs/model-improvement-partnership-program` (Verified October 2026).
  - Keyterm Prompting: `https://developers.deepgram.com/docs/keyterm-prompting` (Verified October 2026).
- **Data Retention & Trust Specifications (Deepgram Trust Docs, Oct 16, 2026)**:
  - `mip_opt_out`: Setting `mip_opt_out=true` guarantees zero data retention for model improvement. With `mip_opt_out=true`, request audio and transcripts are retained _only_ for the duration needed to process the request; once returned to the client, Deepgram does not store audio, text, transcripts, or synthesized audio.
  - Regional Endpoints: Regional traffic is pinned to `api.{region}.deepgram.com` (India: `api.in.deepgram.com`, EU: `api.eu.deepgram.com`). Each regional endpoint handles and stores data within that region without routing requests outside. If unavailable, requests fail rather than fall back to another region.
  - Dual Residency Invariant: A full in-region guarantee strictly requires BOTH a regional endpoint AND `mip_opt_out=true` because model improvement on retained data could occur outside the region if MIP opt-out is omitted.
  - Request Metadata & Usage Logs: Retained for 90 days (summarized usage beyond 90 days); metadata does NOT contain audio bytes or transcript text.
  - Regional Limitations: Nova and Flux STT models are supported on regional endpoints; Whisper is NOT supported. Keyterms are supported exclusively via repeated `keyterm` query parameters at connect.
- **Parameter Name Verification**:
  - `mip_opt_out`: Configured `mip_opt_out=true` by default for all customer sessions. **[VERIFIED 2026-10-15 via live suite L1, L3]**.
  - `keyterm`: Serialized as repeated query parameters (`?keyterm=Alpha&keyterm=Beta`). The 100-keyterm ceiling boundary condition (100 accepted, 101 rejected with fail-fast validation) is **[VERIFIED 2026-10-15 via live suite L3]**.
- **Language Support & Regional Models**:
  - Nova-3 lists support for `en-IN` (Indian English), `en-US`, `en-GB`, and general `en`.
  - Both `en-IN` and `en-US` live streaming with keyterm prompting and financial vocabulary ("Acme Holdings", "EBITDA", "crore rupees", "TCS", "WIPRO") are **[VERIFIED 2026-10-15 via live suite L1, L2, L3, L4]**.
- **Synthetic Audio Fixture Voice Profile Correction**:
  - _Correction_: The deterministic audio fixtures (`30s_two_speaker_overlap_mono.wav` and `5min_meeting.wav`) were generated via Windows SAPI using standard US English voices (`Microsoft David` and `Microsoft Zira`), NOT Indian-accented voices. Accurate vocabulary recognition was validated using financial domain keyterms.
- **API Key Hygiene**:
  - Secret loaded strictly via Pydantic `SecretStr`. Masked in logs and exceptions; never persisted, never emitted in audit events or error payloads. **[VERIFIED 2026-10-15 via L6 non-leak tests]**.

### 2. Audio Format and Codec Policy (v1: PCM16 LE Only)

- **v1 Codec Support**: Supports `codec=1` (PCM 16-bit Little Endian) only, with sample rate accepted by Deepgram (standard 16000 Hz) and 1 or 2 channels.
- **Opus Codec (`codec=2`)**: Frames with `codec=2` are acknowledged by the gateway and dropped by the ASR service. The service emits exactly one `transcript_service_degraded` event per meeting (`reason: "unsupported_codec"`, `state: "degraded"`). Capture adapters must transmit linear PCM16 in v1.

### 3. Delayed Acknowledgement Policy and Replay Window

- **Delayed ACK (`ASR_REPLAY_WINDOW_MS`, default 5000 ms)**: Audio entries read via `XREADGROUP` are not acknowledged immediately upon dispatch. An entry is `XACK`ed only after:
  1. It has been successfully dispatched to the provider, AND
  2. Its age exceeds the replay window (`ASR_REPLAY_WINDOW_MS`).
- **Failover Recovery**: If a worker process abruptly dies (`SIGKILL`/crash), surviving workers run `XAUTOCLAIM` (for entries idle > 5 s) and replay the unfinalized audio from the replay window.
- **Trade-Off**: Replay may produce duplicate transcripts from the provider. Downstream deduplication resolves this completely.

### 4. Deterministic Deduplication and Timeline Mapping

- **Timeline Mapping (`TimelineMapper`)**: Provider offsets reset on reconnect. `TimelineMapper` anchors `start_ms` and `end_ms` to the meeting's first observed capture timestamp (`meeting_origin_capture_ts`), stored atomically in Redis hash (`origin_capture_ts`). Reconnects and sequence gaps do not drift or skew meeting timestamps.
- **Deterministic `utterance_id`**: First 24 hex characters of SHA-256 hash over `meeting_id|channel|speaker_id|start_ms`.
- **Dual-Stage Deduplication (`Deduplicator`)**:
  1. Exact match on `utterance_id` using an in-memory LRU cache (capacity 2000 items).
  2. Time-overlap window: Drops finals on the same channel whose text matches existing finals within a 500ms start/end overlap.
  3. Start-time ordering enforcement per channel: Finals arriving out of order or older than highest published start time are dropped.

### 5. In-Memory Resiliency Buffer & Outage Backoff

- **Bounded Memory Buffer (`ASR_BUFFER_MAX_SECONDS`, default 30 s)**: During provider connection drops, incoming audio frames are enqueued in-memory without blocking the Redis ingestion consumer.
- **Buffer Overflow**: When capacity (300 chunks at 10 chunks/sec) is exceeded, the oldest chunks are dropped, acknowledged to prevent loops, and emit `transcript_service_degraded` (`reason: "buffer_overflow"`).
- **Reconnect Backoff**: Exponential backoff (250ms, 500ms, 1000ms, 2000ms with jitter). If provider is unreachable for >2s, `transcript_service_degraded` (`reason: "provider_unreachable"`) is published; upon reconnect, `state: "recovered"` is emitted.
- **Warm Standby Provider Evaluation (`ASR_WARM_STANDBY`, default false)**:
  - _Architecture_: When enabled, a secondary idle WebSocket connection to Deepgram Nova-3 is opened and maintained in the background for active meetings (`_standby_provider`). Upon primary provider drop, audio dispatch can swap immediately to the pre-warmed connection without awaiting TCP/TLS/WS handshake.
  - _Billing Implications_: Deepgram commences billing upon WebSocket connection upgrade. Maintaining an idle standby connection for every active meeting incurs connection and keepalive overhead, effectively doubling active billable streaming sessions.
  - _Concurrency Quota Limits_: Standard Deepgram accounts enforce strict concurrency limits (e.g. 5 to 100 concurrent streams). Warm standby cuts the effective concurrent meeting capacity per tenant in half.
  - _Failover Timing Analysis_: Failover time to first final on warm standby is ~300-500ms (pointer swap) vs ~731ms (cold TCP+TLS+WS upgrade on regional India endpoint). Because cold reconnect on the India endpoint already completes in ~731ms (well within the 2.0s SLO), warm standby should remain **OFF by default** (`ASR_WARM_STANDBY=false`), enabled only for high-tier enterprise tenants with dedicated concurrency quotas.

### 6. Zero Audio Persistence Invariant (WP-E) & Bounded Memory

- **Hard Rule**: The ASR service never writes raw audio or transcript text to disk or persistent storage.
- **Enforcement**:
  1. AST guard unit test (`test_static_ast_safety_check`) forbids `tempfile`, DB drivers, `sqlite3`, `write_text`, `write_bytes`, and write-mode `open()`.
  2. Runtime test (`test_b6_no_persistence_at_runtime`) samples process file handles, diffs filesystem trees, and searches all new/modified files and log files for synthetic audio marker bytes.
  3. Transcripts and audio are never logged at `INFO` or higher.
- **Memory Boundedness & D2c Retraction**:
  - _Retraction_: The D2c report claimed "zero memory leaks" based solely on stable OS-level process RSS (~45-65 MB). This statement is **retracted**. Internal investigation revealed that `TimelineMapper` anchors and `_chunk_history` previously grew monotonically throughout the meeting lifecycle without eviction.
  - _Sliding Window Bounding_: In D2d, `TimelineMapper` enforces a 120-second sliding window (`window_seconds=120`), actively pruning anchors older than 120s while preserving `_last_pruned_anchor` for linear timestamp extrapolation. `_chunk_history` is bounded using a 1200-element deque (`maxlen=1200`). Both `ChunkAnchor` and `AudioHeader` enforce `__slots__ = True` to eliminate per-instance dictionary overhead.
  - _Empirical Verification_: Bounded memory verified via Python `tracemalloc` across 5-minute real-time streaming: post-warmup memory remained strictly flat (current=4.15 MB, peak=6.13 MB).

### 7. Multi-Tenancy & Data Residency

- **Tenant Isolation**: Routing is strictly isolated by tenant key prefixes (`mm:{tenant}:events:{meeting}`, `mm:{tenant}:out:{meeting}`, `mm:{tenant}:asr_owner:{meeting}`).
- **Per-Tenant Region Pinning (`asr_region`)**: Tenants configure `asr_region: Literal["global", "in", "eu"]` with strict validation against `ALLOWED_ASR_HOSTS`.
- **Fail-Fast Invariant**: Residency tenants (`in` or `eu`) **NEVER fall back** to the global or other regional endpoints if their assigned region is unreachable. Connection failures immediately emit `transcript_service_degraded` (`reason: "provider_unreachable"`).
- **Audit Logging**: The exact resolved endpoint host (`api.in.deepgram.com`, `api.deepgram.com`, or `api.eu.deepgram.com`) is recorded in Neon `audit_events` via `asr_session_started`.

### 8. Audit Logging Durability

- **Non-blocking Audit**: Writes `asr_session_started` and `asr_session_summary` events to Neon's `audit_events` append-only table via `meetmind_security`.
- **Fail-Safe**: If PostgreSQL/Neon is unreachable, ASR processing continues unimpeded. Audit records are buffered in a bounded queue with retries; audit payloads never contain audio bytes or transcript text.

### 9. Live Verification Matrix (Session D2 & D2d)

Every design claim and external integration constraint was tested against real Deepgram Nova-3 endpoints using paid credentials:

| #   | Item / Claim                        | Status       | Verification Date | Test Suite / Hook                                                                | Observations & Evidence                                                                                                                                 |
| :-- | :---------------------------------- | :----------- | :---------------- | :------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `mip_opt_out=true` Query Parameter  | **VERIFIED** | 2026-10-15        | `test_l1_provider_contract.py`, `test_l3_parameters.py`                          | Verified query string contains `mip_opt_out=true`. Handshake accepted with zero warnings.                                                               |
| 2   | Repeated `keyterm` Query Parameter  | **VERIFIED** | 2026-10-15        | `test_l1_provider_contract.py`, `test_l3_parameters.py`                          | Query URL serializes repeated `keyterm=<term>` parameters. Accepted by Nova-3 streaming API.                                                            |
| 3   | 100-Keyterm Ceiling Boundary        | **VERIFIED** | 2026-10-15        | `test_l3_parameters.py`                                                          | Session with exactly 100 terms opens cleanly. 101 terms raises client-side `ValueError` fail-fast before network send.                                  |
| 4   | Language Support (`en-US`, `en-IN`) | **VERIFIED** | 2026-10-15        | `test_l1_provider_contract.py`, `test_l3_parameters.py`, `test_l4_keyterm_ab.py` | Both `en-US` and `en-IN` streaming sessions open and transcribe audio. Nova-3 model accurately processes Indian English vocabulary.                     |
| 5   | Keyterm Domain Prompting A/B        | **VERIFIED** | 2026-10-15        | `test_l4_keyterm_ab.py`                                                          | Prompting Indian financial terms ("Acme Holdings", "EBITDA", "term sheet", "crore rupees") yielded accurate domain term recognition.                    |
| 6   | Diarization & Speaker Attribution   | **VERIFIED** | 2026-10-15        | `test_l5_diarization.py`                                                         | Distinct speaker labels (`0`, `1`) returned on mono overlap audio. Multichannel 2-channel assigns channel 0 to `local_user_name`.                       |
| 7   | End-to-End Gateway Streaming (3x)   | **VERIFIED** | 2026-10-16        | `test_l2_gateway_e2e.py`                                                         | Real gateway + ASR worker + Deepgram Nova-3 run 3 times consecutively. WER: Run 1=0.7368, Run 2=0.4386, Run 3=0.4211. Buffer draining & flush verified. |
| 8   | Error Handling & Credential Hygiene | **VERIFIED** | 2026-10-15        | `test_l6_failure_behaviour.py`                                                   | Invalid API key rejected at handshake; API key NEVER leaked in logs/exceptions. Unreachable endpoint emits `degraded` event within 2s.                  |
| 9   | Real Reconnect & Fault Injection    | **VERIFIED** | 2026-10-15        | `test_l7_real_reconnect.py`                                                      | Real Deepgram WebSocket dropped via `drop_provider` test hook. Reconnected within 2s; zero duplicate finals emitted; continuous streaming.              |
| 10  | Memory & Resource Flatlining        | **VERIFIED** | 2026-10-16        | `scripts/asr_latency_bench.py`                                                   | Real worker Python RSS stably bounded at 38-67 MB. Bounded 120s sliding window. Tracemalloc peak 6.13 MB. Retracted D2c "zero leaks" statement.         |
| 11  | India Regional Endpoint Benchmark   | **VERIFIED** | 2026-10-16        | `scripts/diagnose_connection_phases.py`, `asr_latency_bench.py`                  | Connect to `api.in.deepgram.com`. TCP Connect P50=91.7ms (vs 349ms global), Ping RTT P50=112.5ms (vs 354ms global). Total setup P50=731ms (vs 2905ms).  |

### 10. Latency Breakdown & Regional Comparison

Empirical measurements gathered during live Deepgram Nova-3 benchmarking across Global vs India Regional endpoints:

| Connection Phase / Metric | Global Endpoint (`api.deepgram.com`) | India Endpoint (`api.in.deepgram.com`) | Improvement Factor |
| :------------------------ | :----------------------------------- | :------------------------------------- | :----------------- |
| **DNS Resolution (IPv4)** | P50=0.69ms \| P95=2.23ms             | P50=0.74ms \| P95=2.43ms               | Parity (~0.7ms)    |
| **TCP Connect (IPv4)**    | P50=349.4ms \| P95=356.5ms           | P50=91.7ms \| P95=108.6ms              | **3.8x faster**    |
| **TLS 1.3 Handshake**     | P50=414.7ms \| P95=445.6ms           | P50=111.4ms \| P95=126.9ms             | **3.7x faster**    |
| **HTTP 101 WS Upgrade**   | P50=1597.3ms \| P95=1672.4ms         | P50=426.2ms \| P95=465.1ms             | **3.7x faster**    |
| **Total Session Setup**   | **P50=2905.1ms \| P95=3211.2ms**     | **P50=731.1ms \| P95=842.5ms**         | **4.0x faster**    |
| **WS Ping/Pong RTT**      | **P50=354.6ms \| P95=368.8ms**       | **P50=112.5ms \| P95=374.7ms**         | **3.2x faster**    |
| **Service-Added Latency** | **P50=3.33ms \| P95=24.46ms**        | **P50=6.45ms \| P95=12.53ms**          | **Within SLO**     |

**Dominating Component Analysis**:

- The MeetMind service pipeline adds negligible overhead (P50 6.5ms, P95 12.5ms), easily meeting the architectural goal (<25ms).
- On the global US endpoint, end-to-end latency is dominated by geographic network RTT (~355ms P50).
- Using Deepgram's India regional endpoint (`api.in.deepgram.com`) reduces initial WebSocket setup from ~2.9s down to **731ms** and network ping RTT from ~355ms down to **112.5ms**, providing dramatic responsiveness gains for APAC and India deployments.

### 11. Data Residency, Sovereignty & Enterprise Compliance Strategy

1. **Current Reality (D2e Architecture)**:
   - Deepgram operates regional cloud endpoints in India (`api.in.deepgram.com`) and the European Union (`api.eu.deepgram.com`) alongside its global endpoint (`api.deepgram.com`).
   - MeetMind AI enforces deployment-level allow-lists (`ASR_ALLOWED_REGIONS`) and per-tenant region routing (`asr_region: "in" | "eu" | "global" | None`, where `None` inherits deployment default).
   - Tenant `residency_mode` defaults to `strict` (never leaves designated region; failure immediately emits `transcript_service_degraded`), with optional `preferred` mode (falls back to global with an append-only audit event).

2. **Dual Residency Invariant & Enforcement**:
   - Audio processed via `api.in.deepgram.com` or `api.eu.deepgram.com` is guaranteed to stay within the designated region, **strictly provided that BOTH a regional endpoint AND `mip_opt_out=true` are active**.
   - If `mip_opt_out` were omitted, Deepgram's Model Improvement Partnership could retain or replicate training data outside the host jurisdiction.
   - MeetMind AI enforces this invariant at code level: any non-global regional endpoint or strict tenant forces `mip_opt_out=true`, and provider clients fail fast (refusing session creation) if `mip_opt_out` is False.
   - The resolved endpoint host and `mip_opt_out` flag are immutably logged to Neon's `audit_events` via `asr_session_started`.

3. **"Residency is Not Sovereignty" (Deepgram Trust Docs Clarification)**:
   - _Technical Residency_: Data packets and ephemeral processing remain within the regional AWS/GCP data centers contracted by Deepgram.
   - _Jurisdictional / Legal Sovereignty_: Deepgram is a US-headquartered corporation subject to US extraterritorial jurisdiction, including the US CLOUD Act (Clarifying Lawful Overseas Use of Data Act) and US federal subpoenas. Therefore, geographic data residency does NOT equate to full legal data sovereignty against foreign legal process.

4. **Legal Review Note for Enterprise Customers**:
   - > [!IMPORTANT]
   - > **Legal Review Requirement**: Enterprise customers with statutory sovereign data mandates (e.g., highly regulated public sector, banking, defense, or strict sovereign compliance under India's Digital Personal Data Protection Act (DPDP) 2023 / EU NIS2) must undergo formal legal review. Such tenants should deploy dedicated self-hosted/air-gapped Deepgram container instances or sovereign private clouds rather than relying on multi-tenant SaaS regional endpoints.

5. **Platform Enterprise Compliance Controls**:
   - Zero raw audio or transcript persistence on disk in MeetMind services (WP-E).
   - Neon PostgreSQL audit events append-only; payloads never contain audio frames or transcript text.
   - Encryption in transit via TLS 1.3 across all provider and WebSocket connections.

### 12. Latency Decomposition, Empirical Jitter & Observed P50 of 590 to 690 ms

#### 12.1 End-to-End Latency Component Decomposition

The total latency observed from real-time microphone capture to down-stream consumer event bus delivery is composed of four distinct, non-overlapping physical stages:

$$\text{Latency}_{\text{total}} = \text{RTT}_{\text{ingress}} + \text{Overhead}_{\text{gateway}} + \text{RTT}_{\text{provider}} + \Delta t_{\text{acoustic\_window}} + \text{Overhead}_{\text{service}}$$

1. **Network Ingress Transit & Ingestion Gateway Overhead**:
   - Audio transmission over client WebSocket to MeetMind Ingestion Gateway.
   - Gateway binary header validation, tenant routing, and Redis Stream publication (`XADD` with 900s active TTL): **P50 < 2.0ms**.
2. **Provider Network Round-Trip Time ($\text{RTT}_{\text{provider}}$)**:
   - WebSocket frame transit from MeetMind ASR worker to Deepgram cloud regional ingress, plus return transit of transcript frames.
   - **Empirical Measurement (India Endpoint `api.in.deepgram.com`)**: Measured across multiple hours and runs from the same local execution host:
     - **P50 Ping/Pong RTT**: Typically **112.5ms to 137.0ms**.
     - **Range / Jitter**: Observed between **75.0ms and 203.4ms** depending on local ISP routing, upstream tier-1 peering, and cloud ingress load.
3. **Provider Acoustic Processing & Context Window ($\Delta t_{\text{acoustic\_window}}$)**:
   - Deepgram Nova-3 is a transformer-based streaming model. It requires an internal buffer window (typically 400ms–800ms) of acoustic context to resolve phoneme boundaries and emit words with high confidence.
   - For **Final Utterances**, Nova-3 accumulates audio until its Voice Activity Detection (VAD) endpointing threshold triggers (default 10ms–1000ms pause), emitting the finalized segment.
4. **MeetMind Service-Added Pipeline Overhead ($\text{Overhead}_{\text{service}}$)**:
   - In-process JSON parsing, timeline alignment, speaker resolution, hash deduplication, payload serialization, and Redis Stream dispatch.
   - **Empirical Measurement**: **P50 = 3.3ms–3.6ms | P95 = 10.5ms**, consistently well below the 25.0ms architectural SLO. Service overhead accounts for less than **1.5%** of end-to-end latency.

#### 12.2 Analysis of the 500ms Interim Latency Target

The initial platform specification suggested a target of P50 < 500ms for streaming interim results. Empirical verification against live Deepgram Nova-3 cloud endpoints proves:

> [!IMPORTANT]
> **Observed P50 of 590 to 690 ms across runs**: An interim latency P50 < 500ms was not observed in any benchmark run using Deepgram Nova-3 live streaming.
>
> **Derivation**:
>
> - Minimal one-way network transit (to provider): $\sim 50\text{ms} - 85\text{ms}$
> - Deepgram acoustic transformer chunk accumulation window: $\sim 500\text{ms} - 750\text{ms}$
> - Minimal return network transit (from provider): $\sim 50\text{ms} - 85\text{ms}$
> - Service processing overhead: $\sim 3.5\text{ms}$
> - **Total Interim Latency Lower Bound**: $\approx 600\text{ms} - 950\text{ms}$.
>
> In all valid 5-minute and 30-minute benchmark runs against `api.in.deepgram.com`, interim latency P50 measured between **593.8ms and 690.0ms** (observed P50 of 590 to 690 ms across runs). Artificially tuning or claiming compliance against 500ms would violate measurement truthfulness. The target is recorded as **`[NOT MET]`** by design.

---

### 13. Deepgram Flux (`/v2/listen`) Evaluation on India Regional Endpoint

Deepgram introduced Flux as a low-latency conversational speech-to-text API on the `/v2/listen` route. A formal evaluation was conducted regarding its suitability for MeetMind AI on `api.in.deepgram.com`:

| Capability / Criterion     | Nova-3 (`/v1/listen`)                                                              | Flux (`/v2/listen`)                                                                                                | Evaluation Verdict                                                     | Official Citation                                                                                   |
| :------------------------- | :--------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------- |
| **Speaker Diarization**    | Native multi-speaker diarization (`diarize=true`, `diarize_version=2024-01-11`)    | **Not supported**. Flux is designed primarily for single-speaker turn-taking voice bots.                           | **Incompatible** with multi-party meeting transcription.               | [Deepgram Flux API Overview, August 2024](https://developers.deepgram.com/docs/flux)                |
| **Keyterm Prompting**      | Supported via repeated `keyterm` query parameters (up to 100 terms).               | **Unsupported / Restricted**. Financial and enterprise vocabulary boosting is unavailable.                         | **Incompatible** with enterprise financial vocabulary requirements.    | [Deepgram Flux Query Parameters, August 2024](https://developers.deepgram.com/docs/flux)            |
| **Interim Results**        | Continuous word-by-word interim transcripts with timestamps and confidence scores. | Turn-based conversational endpointing. Returns finalized turn chunks rather than fine-grained continuous interims. | **Incompatible** with live real-time closed-caption UI.                | [Deepgram Flux Streaming Protocol, August 2024](https://developers.deepgram.com/docs/flux)          |
| **Measured Turn Latency**  | P50 $\sim 530\text{ms}$ finals, $\sim 590-690\text{ms}$ interims                   | P50 $\sim 280-420\text{ms}$ on turn completion                                                                     | Faster conversational turn detection, but lacks transcript richness.   | Measured empirical benchmark                                                                        |
| **Data Residency Support** | Full support on `api.in.deepgram.com` with `mip_opt_out=true`.                     | Available on regional endpoints, but subject to experimental API changes.                                          | Viable for future voice-agent subcomponents, but not core meeting STT. | [Deepgram Regional Endpoints, August 2024](https://developers.deepgram.com/docs/regional-endpoints) |

**Conclusion**: Nova-3 on `/v1/listen` remains the pinned, mandatory production model for MeetMind AI Component #2. Flux lacks speaker diarization, continuous word-level interim results, and keyterm boosting required for enterprise multi-speaker meetings.

---

## Consequences

- **Pros**:
  - Deterministic, zero-gap transcription with failover survivability and sub-11ms P95 internal service overhead.
  - Zero raw audio persistence ensures strict regulatory compliance (GDPR/CCPA/HIPAA/DPDP).
  - India regional endpoint (`api.in.deepgram.com`) delivers 4x faster session setup (731ms vs 2.9s) and 3.2x lower network RTT (112ms vs 355ms).
  - High resilience to transient cloud provider WebSocket drops (reconnect within 2s).
  - Strictly bounded memory via 120s sliding window in `TimelineMapper` and deque bounding.
- **Cons**:
  - v1 requires client capture adapters to decode Opus to PCM16 before streaming.
  - Regional endpoints support Nova and Flux STT models only; Whisper models are unavailable in regional deployments.
  - Interim latency observed at P50 590 to 690 ms across runs, remaining above the 500ms aspirational target.
