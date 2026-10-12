# ADR 006 — Ephemeral Audio Retention, Non-Persistence Invariants, Distributed Janitor, and Gateway Resilience

**Status**: Accepted  
**Date**: 2026-10-11  
**Decision makers**: Engineering & Security Architecture Team

## Context

MeetMind AI captures live audio streams from meeting participants across client adapters (Chrome Extension, Native Desktop, and Meeting Bots) to generate real-time transcription and intelligence events.

A core security and privacy invariant of MeetMind AI is that **raw audio must never persist to permanent disk storage**. Raw audio frames are strictly transient in-flight buffers required for near-real-time streaming automatic speech recognition (ASR). Once transcribed, raw audio has no operational purpose and poses significant compliance, privacy, and regulatory risk (e.g., GDPR, CCPA, wiretapping statutes).

Additionally, distributed systems at the ingestion layer must guarantee high resilience and fault tolerance:

1. **Worker Process Termination**: If a gateway worker process is abruptly killed mid-stream, client capture adapters must be able to reconnect to another gateway worker and immediately take over the stream without data loss or gaps.
2. **Abandoned Meetings**: If a client abruptly crashes or loses network connectivity without sending `meeting_end`, stream buffers and session locks must not linger indefinitely.
3. **Consumer Failures**: If downstream processing consumers (e.g. ASR workers) crash after reading audio entries from Redis Streams but before acknowledging them (`XACK`), surviving workers must reliably recover unacknowledged audio chunks.
4. **Subscriber Disconnections**: If read-only listeners (e.g. side-panel dashboards) suffer brief network drops, they must resume their outbound event streams without losing events or receiving duplicates.

## Decision

We enacted the following architecture, invariants, and automated controls across Component #1:

### 1. Ephemeral In-Memory Audio Invariant (WP6)

- **Dedicated Ephemeral Redis (`AUDIO_REDIS_URL`)**: Audio streams are routed to an in-memory Redis instance (`mm:{tenant}:audio:{meeting}`) separate from persistent application state.
- **Strict Startup Non-Persistence Verification**:
  During application startup (lifespan hook `assert_audio_redis_not_persistent`), the gateway verifies via `CONFIG GET save` and `CONFIG GET appendonly` that:
  - RDB snapshotting is disabled (`save` is empty string).
  - Append-Only File is disabled (`appendonly == 'no'`).
  - If persistence is enabled, the gateway **refuses to start** outside development environments. If `CONFIG` is disallowed by managed cloud providers, startup requires an explicit acknowledgment flag `AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED=true`.
- **Bounded Stream Length**: All audio stream writes (`XADD`) enforce an upper bound of `maxlen=AUDIO_STREAM_MAXLEN` (default 20,000 entries, equivalent to ~33 minutes of 100ms audio chunks). Older entries are trimmed automatically to constrain memory footprint.

### 2. Time-To-Live (TTL) and Retention Lifecycle

- **Active Streaming Expiry**: Every audio chunk published via `publish_audio` refreshes the Redis stream key TTL to **15 minutes (900 seconds)**.
- **Grace Period on Meeting End**: Upon receiving an explicit `meeting_end` control frame, the gateway sets an expedited **5-minute (300 seconds) grace TTL** on audio and transcript streams, allowing downstream consumers a final window to drain in-flight buffers before automatic purge.
- **Distributed Abandonment Janitor (`janitor.py`)**:
  A background task running across gateway instances uses a distributed Redis lease lock (`mm:system:janitor:lock`) to sweep for abandoned meetings every 60 seconds. Meetings with no active writer heartbeat for >120 seconds have their writer leases released, status set to `abandoned`, and grace TTLs applied.

### 3. Resilience, Worker-Kill, and Consumer Recovery (WP4 / WP6)

- **Single-Writer Lease Takeover (K7)**:
  Single-writer exclusivity is enforced using a composite lease value `user_id:client_instance_id`. If a worker dies and the client reconnects to a different worker with the same `client_instance_id`, an atomic Lua CAS script updates the lease owner immediately without waiting for the 15-second TTL to expire. Conflicting writers from different instances are rejected with `4409 WRITER_CONFLICT`.
- **Zero-Gap Stream Continuity**:
  The 18-byte binary audio frame header carries a monotonic `client_seq` and `capture_ts_ms`. Reconnecting clients retransmit unacknowledged frames; the gateway acknowledges duplicates without re-publishing, ensuring Redis stream entries contain all sequence numbers exactly once, in order, with zero gaps.
- **Consumer Group Recovery via `XAUTOCLAIM`**:
  Downstream consumer groups (e.g. `asr`) reading via `XREADGROUP` that crash before `XACK` leave messages in a Pending Entries List (PEL). Surviving workers reclaim these entries via `XAUTOCLAIM` with zero message loss.
- **Subscriber Event Resumption**:
  Listener endpoints (`/v1/meetings/{id}/listen`) accept a `last_event_id` query parameter. Reconnecting listeners receive all outbound events starting immediately after `last_event_id`, preserving delivery guarantees across disconnections.

### 4. Fail-Closed Audit Durability (K1 / T11)

- If PostgreSQL (Neon) becomes unreachable during `meeting_start` or `consent_confirmed`, the connection is immediately terminated with close code `4503 AUDIT_UNAVAILABLE`.
- Ingestion **never** proceeds without durable database recording of consent and audit events.

## Consequences

- **Pros**:
  - Full compliance with privacy and data protection mandates; raw audio cannot leak into persistent storage or backups.
  - Guaranteed recovery and zero audio loss during gateway process crashes and redeployments.
  - Automatic cleanup of zombie sessions, freeing memory without human intervention.
  - Strict audit trail: consent changes are always persisted before audio frames can be ingested.
- **Operational Considerations**:
  - Downstream consumers must drain and process audio within the 5-minute post-meeting grace window or the 15-minute inactivity window.
  - Cloud-managed Redis services (e.g. AWS ElastiCache, Upstash) that disable the `CONFIG` command require setting `AUDIO_REDIS_PERSISTENCE_ACKNOWLEDGED=true` in production configurations once manual infrastructure verification is completed.
