# ADR 007 — Automatic Speech Recognition (ASR) Streaming Service Architecture

**Status**: Accepted  
**Date**: 2026-10-12  
**Decision makers**: Engineering & Security Architecture Team

## Context

MeetMind AI requires real-time automatic speech recognition (ASR) for live meetings ingested through the Ingestion Gateway (Component #1). Ingestion streams audio into Redis Streams (`mm:{tenant}:audio:{meeting}`) with an 18-byte binary frame header containing sequence numbers, capture timestamps, channel counts, and codec flags.

Component #2 (the ASR streaming service, `meetmind_asr`) must consume audio chunks from Redis, stream them to an ASR provider, map timeline offsets accurately back to meeting audio origin timestamps across network reconnects, and publish transcripts with at-least-once audio delivery and effectively-once transcript semantics.

## Locked Decisions & Architecture

### 1. Provider Selection and Pinning (Deepgram Nova-3)

- **Provider**: Deepgram Nova-3 via live streaming WebSocket (`wss://api.deepgram.com/v1/listen` or regional endpoint `wss://api.eu.deepgram.com/v1/listen`).
- **SDK Deviation (Raw WebSockets vs `deepgram-sdk`)**:
  - _Deviation_: The original build spec referenced `deepgram-sdk`. Component #2 implements direct streaming WebSocket client protocol via Python's native `websockets` library.
  - _Rationale_: `deepgram-sdk` (v3) wraps `websockets`/`httpx` with background threads and abstraction layers that introduce event loop impedance, thread-safety hazards, and silent failure modes during high-concurrency reconnects and backoff. Direct WebSocket streaming grants deterministic, zero-overhead control over async socket lifecycles, ping/pong heartbeats, reconnect timers, and exact URL query parameter serialization.
- **Official Documentation Consulted & Dates**:
  - Deepgram Streaming Audio API: `https://developers.deepgram.com/docs/streaming-audio` (Verified October 2026; API v1 streaming WebSocket).
  - Nova-3 Model Specifications: `https://developers.deepgram.com/docs/nova-3` (Verified October 2026).
  - Model Improvement Partnership Program (MIP) Opt-Out: `https://developers.deepgram.com/docs/model-improvement-partnership-program` (Verified October 2026).
  - Keyterm Prompting: `https://developers.deepgram.com/docs/keyterm-prompting` (Verified October 2026).
- **Parameter Name Verification**:
  - `mip_opt_out`: Verified against official Deepgram documentation. Setting `mip_opt_out=true` instructs Deepgram not to retain or use audio for model training. Configured `mip_opt_out=true` by default for all customer sessions.
  - `keyterm`: Verified for Nova-3. Supersedes legacy `keywords`. Serialized as repeated query parameters (e.g. `?keyterm=Alpha&keyterm=Beta`), strictly limited to 100 keyterms maximum.
- **Language Support & Regional Models**:
  - Nova-3 supports `en-IN` (Indian English), `en-US`, `en-GB`, and general `en`.
  - Keyterm prompting is officially supported on Nova-3 for English variants, including `en-IN`.
- **What Could NOT Be Verified in Session D1**:
  - Live network WebSocket streaming against Deepgram production cloud servers (`wss://api.deepgram.com`) using live paid API keys. Live Deepgram cloud verification, audio fixtures, and the 30-minute benchmark are explicitly out of scope for D1 and scheduled for Session D2.
- **API Key Hygiene**:
  - Secret loaded strictly via Pydantic `SecretStr`. Masked in logs and exceptions; never persisted, never emitted in audit events or error payloads.

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

### 6. Zero Audio Persistence Invariant (WP-E)

- **Hard Rule**: The ASR service never writes raw audio or transcript text to disk or persistent storage.
- **Enforcement**:
  1. AST guard unit test (`test_static_ast_safety_check`) forbids `tempfile`, DB drivers, `sqlite3`, `write_text`, `write_bytes`, and write-mode `open()`.
  2. Runtime test (`test_b6_no_persistence_at_runtime`) samples process file handles, diffs filesystem trees, and searches all new/modified files and log files for synthetic audio marker bytes.
  3. Transcripts and audio are never logged at `INFO` or higher.

### 7. Multi-Tenancy & Data Residency

- **Tenant Isolation**: Routing is strictly isolated by tenant key prefixes (`mm:{tenant}:events:{meeting}`, `mm:{tenant}:out:{meeting}`, `mm:{tenant}:asr_owner:{meeting}`).
- **Data Residency Disclosure**: Live audio streams exit the customer perimeter to Deepgram's cloud endpoints (`api.deepgram.com` or `api.eu.deepgram.com`). Sub-processor disclosures and enterprise BAAs must be configured accordingly.

### 8. Audit Logging Durability

- **Non-blocking Audit**: Writes `asr_session_started` and `asr_session_summary` events to Neon's `audit_events` append-only table via `meetmind_security`.
- **Fail-Safe**: If PostgreSQL/Neon is unreachable, ASR processing continues unimpeded. Audit records are buffered in a bounded queue with retries; audit payloads never contain audio bytes or transcript text.

## Consequences

- **Pros**:
  - Deterministic, zero-gap transcription with failover survivability.
  - Zero raw audio persistence ensures strict regulatory compliance (GDPR/CCPA/HIPAA).
  - High resilience to transient cloud provider WebSocket drops.
- **Cons**:
  - v1 requires client capture adapters to decode Opus to PCM16 before streaming.
  - Audio data crosses network boundary to Deepgram managed infrastructure.
