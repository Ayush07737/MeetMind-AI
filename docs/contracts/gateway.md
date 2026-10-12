# MeetMind AI — Ingestion Gateway Interface Contract

**Component**: #1 Ingestion Gateway  
**Version**: 1.0.0  
**Status**: Stable / Accepted

This document specifies the interface contract, protocols, payload formats, error codes, and operational invariants for the MeetMind AI Ingestion Gateway.

---

## 1. REST API Contract

### 1.1 Create Meeting — `POST /v1/meetings`

Creates a server-issued unguessable meeting identifier bound to the caller's tenant and user identity (K6).

- **Headers**:
  - `Authorization: Bearer <clerk_session_jwt>` (required)
- **Request Body** (optional JSON):
  ```json
  {
    "meeting_id": "optional_preselected_id_min_22_chars"
  }
  ```
  If omitted or empty, the server generates a 32-character cryptographically random unguessable ID (`uuid4().hex`).
- **Response** (`201 Created`):
  ```json
  {
    "meeting_id": "9f8b7a6c5d4e3f2a1b0c9d8e7f6a5b4c",
    "tenant_id": "tenant_abc123",
    "owner_id": "user_2xYz9...",
    "status": "pending_consent",
    "created_at": "2026-10-11T09:00:00Z"
  }
  ```
- **Error Codes**:
  - `401 Unauthorized`: Missing or invalid Bearer token.
  - `403 Forbidden`: Mismatched tenant or insufficient organization role.

### 1.2 Issue WebSocket Ticket — `POST /v1/ws-ticket`

Exchanges a Clerk session JWT for a single-use, 128-bit cryptographically secure WebSocket ticket bound to the meeting, caller role, and client instance (K6, K7).

- **Headers**:
  - `Authorization: Bearer <clerk_session_jwt>` (required)
- **Request Body** (JSON):
  ```json
  {
    "meeting_id": "9f8b7a6c5d4e3f2a1b0c9d8e7f6a5b4c",
    "role": "ingest",
    "client_instance_id": "e4b3c2d1-0000-4000-8000-abcdef123456"
  }
  ```
  - `role`: `"ingest"` (write audio/transcripts) or `"listen"` (read-only outbound event subscriber).
  - `client_instance_id`: Unique client UUID to facilitate seamless worker-kill lease takeover.
- **Response** (`200 OK`):
  ```json
  {
    "ticket": "a1b2c3d4e5f60718293a4b5c6d7e8f90",
    "expires_in": 30,
    "role": "ingest"
  }
  ```
- **Security & Multi-Tenancy Invariants**:
  - Meeting existence verification: Meeting must exist within the caller's tenant.
  - **404 Parity**: Requesting a non-existent meeting or a meeting belonging to another tenant returns the exact same generic `404 Not Found` response (`{"detail": "Meeting not found"}`), while recording distinct internal security audit events.
  - Single-use: Consumed atomically upon WebSocket handshake (`GETDEL`). Replayed tickets return `4401 UNAUTHORIZED`.

---

## 2. Audio Ingestion WebSocket — `/v1/meetings/{id}/stream`

Ingest endpoint for capture adapters (Chrome Extension, Native Desktop App, Meeting Bot).

- **Query Parameters**:
  - `ticket`: One-time ticket from `POST /v1/ws-ticket`.
- **Single-Writer Exclusivity (K7)**:
  - Exactly one writer allowed per meeting at any time.
  - Writer lease stored in Redis at `mm:{tenant}:writer:{meeting}` as `user_id:client_instance_id` with 15-second TTL.
  - Reconnecting clients presenting the same `client_instance_id` take over the lease immediately via an atomic Lua CAS script. The superseded connection is closed with `4409 WRITER_CONFLICT` (reason: `SUPERSEDED`).
  - Conflicting writers from different instances are rejected with `4409 WRITER_CONFLICT`.

### 2.1 Protocol State Machine

```
[CONNECTED]
    |
    v (receives 'meeting_start')
[PENDING_CONSENT]
    |
    v (receives 'consent_confirmed' + Neon audit written)
[ACTIVE]
    |
    v (receives 'meeting_end' or disconnect)
[ENDED]
```

- **Invariant**: Any audio or transcript data frame sent before `consent_confirmed` is rejected with `CONSENT_REQUIRED`, and recorded to Neon `audit_events` (`frame_rejected_no_consent`).
- **Audit Durability (Fail-Closed)**: If Neon is unreachable during `meeting_start` or `consent_confirmed`, the connection is immediately closed with `4503 AUDIT_UNAVAILABLE`.

### 2.2 Control Frame Schema

Client-to-server JSON frames:

```json
// Meeting Start
{
  "type": "control",
  "action": "meeting_start",
  "adapter_type": "chrome_extension"
}

// Consent Confirmed (Mandatory Fields)
{
  "type": "control",
  "action": "consent_confirmed",
  "consent_type": "audio_capture",
  "external_participants": false,
  "jurisdiction_hint": "US",
  "consent_text_version": "v1.0",
  "client_version": "1.0.0"
}

// Meeting End
{
  "type": "control",
  "action": "meeting_end"
}
```

### 2.3 Binary Audio Frame Schema

Audio chunks must be framed with the 18-byte big-endian binary header:

| Offset | Type     | Field           | Description                                                    |
| ------ | -------- | --------------- | -------------------------------------------------------------- |
| 0..1   | `uint16` | Magic           | `0x4D4D` (ASCII `"MM"`)                                        |
| 2..5   | `uint32` | `client_seq`    | Monotonically increasing sequence number (1, 2, 3...)          |
| 6..13  | `uint64` | `capture_ts_ms` | Unix timestamp in milliseconds when audio was captured         |
| 14..15 | `uint16` | `sample_rate`   | Sample rate in Hz (e.g. 16000)                                 |
| 16     | `uint8`  | `channels`      | Channel count (1 = Mono, 2 = Stereo)                           |
| 17     | `uint8`  | `codec`         | Audio codec (1 = PCM 16-bit LE, 2 = Opus)                      |
| 18..N  | `bytes`  | `payload`       | Raw audio bytes (e.g. 3,200 bytes for 100ms 16kHz 16-bit mono) |

### 2.4 Server Acknowledgements

Upon successfully processing and publishing an audio frame to Redis, the server sends an immediate JSON acknowledgement:

```json
{
  "type": "ack",
  "seq": 1,
  "frame_type": "audio_chunk",
  "status": "acked",
  "timestamp": "2026-10-11T09:00:00.123456Z"
}
```

- **Deduplication**: If `client_seq <= last_acked_client_seq`, the server immediately sends an `ack` without re-publishing to Redis.
- **Gap Tracking**: If `client_seq > last_acked_client_seq + 1`, a gap is logged and tracked in metadata fields.

---

## 3. Read-Only Listener WebSocket — `/v1/meetings/{id}/listen`

Subscription endpoint for UI side panels, dashboard live streams, and monitoring tools.

- **Query Parameters**:
  - `ticket`: One-time ticket with `role="listen"`.
  - `last_event_id`: (optional) Last received Redis Stream message ID (e.g., `1712839200000-0` or `0-0`). Enables zero-loss resumption after reconnection.
- **Read-Only Invariant**: Any text or binary data frame sent by a listener is rejected with `4400 PROTOCOL_VIOLATION`.
- **Fan-Out**: Multiple concurrent listeners can subscribe to the same meeting simultaneously without duplicating stream entries.

### 3.1 Allow-Listed Outbound Event Types (K5)

Only four client-facing event types are permitted:

1. `intelligence_event`
2. `pre_meeting_brief`
3. `transcript_interim`
4. `transcript_service_degraded`

All outbound events MUST have a top-level `payload` dictionary. Internal events (e.g., `transcript_final`, `trigger_classified`, `raw_audio_chunk`) are dropped with a warning.

```json
{
  "type": "intelligence_event",
  "event_type": "intelligence_event",
  "meeting_id": "9f8b7a6c5d4e3f2a1b0c9d8e7f6a5b4c",
  "payload": {
    "summary": "Key discussion point",
    "confidence": 0.95
  }
}
```

---

## 4. WebSocket Close Codes

| Code   | Name                 | Trigger                                                           |
| ------ | -------------------- | ----------------------------------------------------------------- |
| `1000` | `NORMAL_CLOSURE`     | Clean disconnect upon `meeting_end`                               |
| `1011` | `INTERNAL_ERROR`     | Redis failure, stream publish error                               |
| `4400` | `PROTOCOL_VIOLATION` | Invalid frame format, data sent to listener, missing TLS headers  |
| `4401` | `UNAUTHORIZED`       | Invalid token, expired ticket, ticket replay                      |
| `4403` | `FORBIDDEN`          | Mismatched tenant, unauthorized role                              |
| `4408` | `IDLE_TIMEOUT`       | No consent received within watchdog window (30s)                  |
| `4409` | `WRITER_CONFLICT`    | Competing writer lease, or superseded by client instance takeover |
| `4429` | `RATE_LIMITED`       | Frame rate or byte limit exceeded                                 |
| `4503` | `AUDIT_UNAVAILABLE`  | Neon PostgreSQL unreachable during consent change                 |

---

## 5. Redis Key Hierarchy & Retention Invariants (WP4, WP6)

All keys follow strictly normalized lower-case tenant names matching `^[a-z0-9_]{1,40}$`:

| Key Pattern                        | Type   | Retention / Expiration                                            |
| ---------------------------------- | ------ | ----------------------------------------------------------------- |
| `mm:{tenant}:audio:{meeting}`      | Stream | `maxlen=20000`, 15m active write TTL, 5m post-meeting grace TTL   |
| `mm:{tenant}:transcript:{meeting}` | Stream | `maxlen=20000`, 15m active write TTL, 5m post-meeting grace TTL   |
| `mm:{tenant}:events:{meeting}`     | Stream | Internal event bus (never forwarded to clients)                   |
| `mm:{tenant}:out:{meeting}`        | Stream | Outbound client events, 24h TTL                                   |
| `mm:{tenant}:session:{meeting}`    | Hash   | Meeting state, owner, timestamps, 24h TTL                         |
| `mm:{tenant}:writer:{meeting}`     | String | Single-writer lease (`user:instance`), 15s TTL refreshed every 5s |
| `mm:{tenant}:listeners:{meeting}`  | Set    | Active listener connection IDs, 15s TTL                           |
| `mm:{tenant}:ticket:{ticket}`      | String | One-time ticket hash, 30s atomic TTL                              |
| `mm:system:janitor:lock`           | String | Distributed janitor sweep election lock, 30s TTL                  |
