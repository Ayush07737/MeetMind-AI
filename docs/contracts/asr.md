# MeetMind AI — Component #2 (ASR Streaming Pipeline) Contract Specification

**Status**: Accepted  
**Service Package**: `backend/asr/meetmind_asr`  
**Execution**: `uv run python -m meetmind_asr`

---

## 1. Streams & Redis Keys

| Key / Stream                      | Purpose                                                                                           | Max Length      | TTL                            |
| :-------------------------------- | :------------------------------------------------------------------------------------------------ | :-------------- | :----------------------------- |
| `mm:system:lifecycle`             | Global meeting lifecycle events stream (`meeting_active`, `meeting_ended`, `meeting_abandoned`)   | 10,000 (approx) | Persistent                     |
| `mm:{tenant}:audio:{meeting}`     | Input binary audio frames consumed by ASR consumer group `asr`                                    | 6,000 (approx)  | 900 s (active) / 300 s (grace) |
| `mm:{tenant}:events:{meeting}`    | Internal bus stream for downstream services (`transcript_final`, `transcript_service_degraded`)   | 5,000 (approx)  | 86,400 s (24 h)                |
| `mm:{tenant}:out:{meeting}`       | Client-facing outbound stream for listeners (`transcript_interim`, `transcript_service_degraded`) | 5,000 (approx)  | 86,400 s (24 h)                |
| `mm:{tenant}:asr_owner:{meeting}` | Distributed worker ownership lease (holds `worker_id`)                                            | N/A (string)    | 15 s (heartbeat every 5 s)     |
| `mm:{tenant}:meeting:{meeting}`   | Meeting session hash (contains `local_user_name`, `attendees`, `origin_capture_ts`)               | N/A (hash)      | 86,400 s (24 h)                |

---

## 2. Event Envelope & Schemas

Every published event follows the standard `EventEnvelope` format with a top-level `payload` dict:

```json
{
  "event_type": "transcript_final | transcript_interim | transcript_service_degraded",
  "meeting_id": "mtg_0192384a",
  "tenant_id": "tenant_default",
  "ts_ms": 1791790000000,
  "payload": { ... }
}
```

### 2.1. `transcript_final` (Internal Bus Only)

Published to `mm:{tenant}:events:{meeting}` only. Never forwarded directly to clients.

| Field            | Type          | Description                                                              |
| :--------------- | :------------ | :----------------------------------------------------------------------- |
| `utterance_id`   | `str`         | First 24 hex characters of `sha256(meeting\|channel\|speaker\|start_ms)` |
| `speaker_id`     | `str`         | Speaker identifier (`spk_0`, `spk_1`, scoped to channel)                 |
| `speaker_name`   | `str \| null` | Human participant name (resolved via metadata when possible)             |
| `channel`        | `int \| null` | Audio channel index (0 for local, 1 for remote)                          |
| `text`           | `str`         | Finalized transcript text                                                |
| `start_ms`       | `int`         | Offset from meeting audio capture origin (ms)                            |
| `end_ms`         | `int`         | Offset from meeting audio capture origin (ms)                            |
| `start_epoch_ms` | `int`         | Approximate UNIX epoch timestamp of utterance start                      |
| `confidence`     | `float`       | ASR model confidence score [0.0 - 1.0]                                   |
| `language`       | `str`         | Language code (e.g., `en`, `en-IN`, `en-US`)                             |
| `provider`       | `str`         | Provider identifier (`deepgram` or `stub`)                               |
| `model`          | `str`         | ASR model identifier (`nova-3` or `scripted`)                            |

### 2.2. `transcript_interim` (Client Out Stream Only)

Published to `mm:{tenant}:out:{meeting}` only. Dropped under backpressure.

| Field            | Type          | Description                                      |
| :--------------- | :------------ | :----------------------------------------------- |
| `speaker_id`     | `str`         | Speaker identifier (`spk_0`, `spk_1`)            |
| `speaker_name`   | `str \| null` | Human participant name                           |
| `channel`        | `int \| null` | Audio channel index                              |
| `text`           | `str`         | Real-time interim transcript hypothesis          |
| `start_ms`       | `int`         | Offset from meeting audio capture origin (ms)    |
| `end_ms`         | `int`         | Offset from meeting audio capture origin (ms)    |
| `start_epoch_ms` | `int`         | Approximate UNIX epoch timestamp                 |
| `confidence`     | `float`       | Interim confidence score                         |
| `language`       | `str`         | Language code                                    |
| `provider`       | `str`         | Provider identifier                              |
| `model`          | `str`         | Model identifier                                 |
| `is_stable`      | `bool`        | True if model marked hypothesis as stable prefix |

### 2.3. `transcript_service_degraded` (Both Streams)

Published to both `mm:{tenant}:events:{meeting}` and `mm:{tenant}:out:{meeting}`.

| Field      | Type  | Description                                                                                                          |
| :--------- | :---- | :------------------------------------------------------------------------------------------------------------------- |
| `state`    | `str` | State indicator: `"degraded"` or `"recovered"`                                                                       |
| `reason`   | `str` | Reason code: `"provider_unreachable"`, `"provider_error"`, `"unsupported_codec"`, `"buffer_overflow"`, `"audio_gap"` |
| `since_ms` | `int` | UNIX epoch timestamp when condition began (ms)                                                                       |

---

## 3. Consumer Group & Lease Protocol

### 3.1. Meeting Discovery

- Consumer group: `asr` on stream `mm:system:lifecycle`.
- On `meeting_active`: ASR worker acquires lease `mm:{tenant}:asr_owner:{meeting}` via atomic Lua CAS.
- If lease is secured, starts `MeetingPipeline`. Duplicate `meeting_active` events for an active meeting are acknowledged immediately.
- Pending lifecycle entries idle > 10 s are reclaimed via `XAUTOCLAIM`.
- On `meeting_ended` / `meeting_abandoned`: Remaining audio is drained, provider finished (5s timeout), pending entries acknowledged, and lease released.

### 3.2. Audio Reader

- Consumer group: `asr` on stream `mm:{tenant}:audio:{meeting}`.
- Reads via `XREADGROUP` (`BLOCK=100`, `COUNT=50`).
- Reclaims unacknowledged entries idle > 5 s via `XAUTOCLAIM`.
- Enforces delayed ack: Entries are `XACK`ed only after being sent to the provider AND aging beyond `ASR_REPLAY_WINDOW_MS` (default 5000ms).

### 3.3. Worker Heartbeat & CAS Lease

- Lease key: `mm:{tenant}:asr_owner:{meeting}` with TTL 15 s.
- Heartbeat loop: Refreshed every 5 s using atomic Lua CAS:
  ```lua
  if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[2])
  else
      return nil
  end
  ```
- If the lease is lost, the pipeline halts immediately without acknowledging entries.
