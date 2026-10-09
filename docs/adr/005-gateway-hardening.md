# ADR 005 — Ingestion Gateway Hardening, Protocol State Machine, Role Separation, and Acceptance Harness

**Status**: Accepted  
**Date**: 2026-10-09  
**Decision makers**: Engineering & Security Architecture Team  

## Context

Component #1 (Ingestion Gateway) is the foundational edge service of MeetMind AI. It receives raw audio streams and transcript events from client capture adapters (Chrome Extension, Native Desktop App, and Meeting Bot) and orchestrates them into multi-tenant Redis Streams for downstream processing by ASR and downstream ML components.

Prior implementation milestones exposed several architectural and operational requirements that required formal hardening:

1. **Protocol State Machine & Consent Invariant**: No audio frames may be processed before an explicit, audited `consent_confirmed` event. Clients streaming frames out-of-order, retransmitting packets, or dropping packets required deterministic handling (deduplication vs. gap recording).
2. **Endpoint Role Separation & Exclusivity**: A single meeting must permit only one active ingest stream writer at any time to avoid corrupted or interleaved audio tracks. Simultaneous listeners (e.g. side-panels, web dashboards) require read-only subscription channels (`/listen`) and must never be permitted to inject binary audio frames.
3. **Acceptance Test Fidelity & Drift Prevention**: Testing distributed async protocols using mocks (`AsyncMock`, `unittest.mock.patch`, `FakeRedis`) can mask subtle network, event-loop, TLS/HTTP, and database concurrency deadlocks. Real acceptance criteria required proof against live services.
4. **Token Security & Key Rotation**: Passing JWTs directly in WebSocket query parameters risks credential leakage in server access logs and browser histories. Furthermore, identity providers (Clerk) rotate RS256 signing keys periodically; cached JWKS sets must refresh dynamically without service disruption.
5. **Denial of Service & Pool Exhaustion**: Malicious or buggy clients flooding pre-consent audio frames could exhaust PostgreSQL connection pools if every invalid frame immediately triggered a blocking database write.

## Decision

We instituted the following architectural and security mechanisms across Component #1:

### 1. Dual-Endpoint Architecture & Role-Based Permissions
- **Ingest Endpoint (`/v1/meetings/{id}/stream` or `/ws/ingest/{id}`)**: Restricted exclusively to clients holding an authenticated `ingest` ticket. Requires binary audio framing compliant with the 18-byte binary header specification (magic bytes `MM`, sequence number, timestamp, sample rate, channels, codec).
- **Listen Endpoint (`/v1/meetings/{id}/listen`)**: Dedicated read-only fan-out subscription channel for UI side panels and web clients. Read-only listeners receive real-time JSON and binary events forwarded from the meeting's Redis streams. Any attempt by a listener to transmit data frames triggers immediate closure with code `4400` (`PROTOCOL_VIOLATION`).
- **Single-Writer Exclusivity (`4409 WRITER_CONFLICT`)**: The ingestion gateway acquires an atomic Redis lock (`SET NX PX`) bound to the meeting session. If a second writer attempts to connect while an active ingest session exists, the connection is rejected with close code `4409` (`WRITER_CONFLICT`).

### 2. Single-Use Ticket Exchange Flow
- Clients exchange their Clerk session JWT via `POST /v1/ws-ticket` specifying `meeting_id` and intended `role`.
- The gateway validates the token, verifies tenant isolation, generates a 128-bit cryptographically secure ticket, and stores it in Redis with an atomic TTL (30 seconds).
- Connecting to the WebSocket consumes the ticket via atomic `GETDEL`. Replayed, expired, or mismatched tickets are rejected with `4401` (`UNAUTHORIZED`) or `4403` (`FORBIDDEN`).

### 3. Dynamic Key Rotation & Resilient Identity Extraction
- `ClerkAuthenticator` catches `PyJWKClientError` during JWT signature verification, invalidates cached JWKS sets, and re-fetches the latest public keys from the JWKS endpoint before raising authentication errors.
- Organization claim parsing supports both top-level string `org_id` and Clerk's compact dictionary claim format `o: {"id": "...", "rol": "...", "slg": "..."}`, preventing unhashable type runtime errors.

### 4. Zero-Mock Acceptance Test Harness
- Acceptance tests located in `backend/gateway/tests/acceptance/` run exclusively against:
  - Real Uvicorn child processes spawned on dynamic loopback ports.
  - Real Redis instances running on `localhost:6379`.
  - Real Neon serverless PostgreSQL database with throwaway per-test schemas (`test_tenant_a_*`, `test_tenant_b_*`) initialized via plain-SQL migrations.
  - An ephemeral local HTTP server (`LocalJWKSServer`) serving valid RS256 JWKS sets.
- A mandatory guard test (`test_guard.py`) uses Python AST and token parsing to enforce that `AsyncMock`, `MagicMock`, `patch(`, `monkeypatch`, and `FakeRedis` are never imported or invoked in acceptance tests.

### 5. Audit Event Rate Limiting & Resource Protection
- To protect the Neon PostgreSQL database connection pool from write starvation during high-frequency frame floods, audit logging employs an in-memory sliding window rate limiter. Rapid pre-consent frame bursts are throttled to a maximum of 5 database writes per window while still terminating violating connections.
- Idle connections receive an active watchdog timeout (`4408 IDLE_TIMEOUT`), preventing hanging sockets and lingering Redis state.

## Consequences

- **Pros**:
  - Deterministic single-writer stream integrity with zero interleaved audio packets.
  - Provable multi-tenant isolation across Neon PostgreSQL schemas, Redis keys, and Neo4j namespaces.
  - High confidence through realistic end-to-end acceptance tests that mirror production runtime conditions.
  - Resilient identity integration with Clerk, surviving automatic key rotations without downtime.
  - Robust protection against audit log write exhaustion and connection leaks.
- **Cons**:
  - Acceptance tests require active infrastructure (running Redis and network connectivity to the Neon test branch).
  - Multi-process Uvicorn test execution introduces socket startup latency compared to purely in-memory mock tests.
