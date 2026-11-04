# MeetMind AI — Clock Model, Synchronization & Stage Timestamps

**Document ID**: SPEC-LATENCY-CLOCKS-001  
**Status**: Accepted Contract  
**Components Affected**: Gateway (#1), ASR (#2), Intelligence Pipeline (§6–§18)

---

## 1. The Clock Synchronization Problem

MeetMind AI guarantees strict end-to-end latency budgets (e.g. speech to interim text $\le 1.0\text{s}$ P50; speech to verified insight card $\le 2.0\text{s}$ P50). However, audio capture occurs on distributed client devices (Chrome extensions, desktop electron adapters, meeting bots) whose internal system clocks frequently drift by several seconds or minutes relative to standard UTC and the backend server infrastructure.

Measuring end-to-end latency across heterogeneous systems without clock synchronization produces invalid, negative, or wildly inflated metrics. To establish a verifiable, audit-proof latency chain, MeetMind AI enforces a unified server-clock epoch reference system.

---

## 2. Definitions & The T0 Anchor

### 2.1 The T0 Capture Anchor

**T0** is defined as the capture timestamp (converted to the **SERVER CLOCK** epoch millisecond) of the final audio sample of the utterance that triggers an event or insight:

$$T_0 = \text{capture\_server\_ms} + \text{utterance\_offset\_ms}$$

- **Server-Clock Epoch Milliseconds**: Every stage timestamp across the entire platform is strictly an integer representing UTC epoch milliseconds according to the server's monotonic clock.
- **Client Audio Capture**: Capture adapters stamp each 100ms PCM16 frame with their local monotonic capture time `capture_ts_ms` in the 18-byte binary header.
- **Server Conversion**: The Gateway translates each client capture timestamp into `capture_server_ms` using the established session clock offset:
  $$\text{capture\_server\_ms} = \text{capture\_ts\_ms} + \Delta_{\text{offset}}$$

---

## 3. Clock Synchronization Protocol (SNTP Handshake)

To establish $\Delta_{\text{offset}}$ without introducing third-party NTP dependencies or external network sockets, MeetMind AI integrates a lightweight SNTP-style handshake directly into the WebSocket control channel.

### 3.1 Handshake Flow

```
Client (Capture Adapter)                     Gateway (meetmind_gateway)
        |                                                |
        |--- 1. control: clock_sync (t1) --------------->|  [Records t2 = recv_epoch_ms]
        |                                                |  [Records t3 = send_epoch_ms]
        |<-- 2. control: clock_sync_reply (t1, t2, t3) --|
        |
 [Records t4 = recv_epoch_ms]
 [Computes RTT and Offset]
        |
        |--- 3. control: clock_report (offset, rtt) ---->|
        |                                                |  [Stores in Redis session hash]
        |                                                |  [Audits if shift > 500ms]
```

1. **Step 1 (`clock_sync`)**:
   The client samples its current epoch millisecond ($t_1$) and sends a control message:
   ```json
   {
     "type": "control",
     "action": "clock_sync",
     "t1": 1730500000100
   }
   ```
2. **Step 2 (`clock_sync_reply`)**:
   The Gateway immediately records the receive timestamp ($t_2$), formats a reply, records the send timestamp ($t_3$), and transmits:
   ```json
   {
     "type": "control",
     "action": "clock_sync_reply",
     "t1": 1730500000100,
     "t2": 1730500002600,
     "t3": 1730500002601
   }
   ```
3. **Step 3 (Client Calculation)**:
   Upon receiving the reply at client epoch millisecond $t_4$:
   $$\text{RTT} = (t_4 - t_1) - (t_3 - t_2)$$
   $$\Delta_{\text{offset}} = \frac{(t_2 - t_1) + (t_3 - t_4)}{2}$$
4. **Step 4 (`clock_report`)**:
   The client executes 3 consecutive probes at `meeting_start` and selects the sample with the **minimum RTT**. The client then sends:
   ```json
   {
     "type": "control",
     "action": "clock_report",
     "offset_ms": 2500,
     "rtt_ms": 42,
     "samples": 3
   }
   ```
5. **Periodic Resynchronization**:
   The client repeats this protocol every **60 seconds**.

---

## 4. Gateway Handling & Stream Enrichment

1. **Session Storage**:
   Upon receiving `clock_report`, the Gateway stores the latest `clock_offset_ms` into the meeting session hash:
   `HSET mm:{tenant}:meeting:{meeting} clock_offset_ms <offset>`
2. **Audit Logging & Shift Detection**:
   - The initial `clock_report` generates an append-only audit event: `clock_sync_established`.
   - Any subsequent periodic update where $|\Delta_{\text{new}} - \Delta_{\text{previous}}| > 500\text{ms}$ generates an audit warning: `clock_skew_detected`.
3. **Audio Stream Injection**:
   When reading incoming audio frames, the Gateway computes:
   `capture_server_ms = capture_ts_ms + clock_offset_ms`
   and adds `capture_server_ms` to the Redis stream field in `mm:{tenant}:audio:{meeting}` alongside binary audio payload.

---

## 5. Error Bound Analysis

Under asymmetric network paths where uplink delay $d_{\text{up}} \neq d_{\text{down}}$ (downlink delay):

$$t_2 = t_1 + \Delta_{\text{true}} + d_{\text{up}}$$
$$t_4 = t_3 - \Delta_{\text{true}} + d_{\text{down}}$$

The computed offset is:
$$\Delta_{\text{computed}} = \Delta_{\text{true}} + \frac{d_{\text{up}} - d_{\text{down}}}{2}$$

The estimation error is:
$$\epsilon = \frac{d_{\text{up}} - d_{\text{down}}}{2}$$

Since $0 \le d_{\text{up}} \le \text{RTT}$ and $0 \le d_{\text{down}} \le \text{RTT}$, the maximum theoretical error is strictly bounded by:

$$|\epsilon| \le \frac{\text{RTT}}{2}$$

- **Symmetric Network ($d_{\text{up}} \approx d_{\text{down}}$)**: Error $|\epsilon| \le 5\text{ms}$.
- **High Asymmetry ($20\text{ms}$ up / $120\text{ms}$ down, $\text{RTT}=140\text{ms}$)**: Worst-case error $|\epsilon| = \frac{100}{2} = 50\text{ms}$.
- **Operational Reality**: For India cloud endpoints where $\text{RTT} \le 70\text{ms}$, worst-case clock estimation error is $\le 35\text{ms}$, well within the platform allowance ($100\text{ms}$).

---

## 6. EventEnvelope Timing Extension

Every internal event envelope optionally carries a `timing` object tracking the exact server epoch millisecond of each processing milestone:

```json
{
  "event_type": "transcript_final",
  "meeting_id": "mtg_01a",
  "tenant_id": "tenant_default",
  "ts_ms": 1730500005000,
  "timing": {
    "t0_capture": 1730500002500,
    "gw_recv": 1730500002530,
    "asr_recv": 1730500002535,
    "provider_msg": 1730500004990,
    "asr_pub": 1730500005000,
    "classify_pub": 1730500005120,
    "retrieval_done": 1730500006200,
    "card_pub": 1730500006800,
    "ui_render": 1730500006850
  },
  "payload": { ... }
}
```

### Stage Field Specifications

- `t0_capture`: Monotonic capture server time of the triggering audio sample.
- `gw_recv`: Timestamp when the Gateway completed reading the audio frame from the client WebSocket.
- `asr_recv`: Timestamp when the ASR pipeline read the audio chunk from Redis Streams.
- `provider_msg`: Timestamp when the ASR worker received the final/interim message from Deepgram.
- `asr_pub`: Timestamp when the ASR pipeline published the event to Redis.
- `classify_pub`: Timestamp when the intent/category classifier published the classification event.
- `retrieval_done`: Timestamp when vector/graph retrieval completed.
- `card_pub`: Timestamp when the insight card was validated and published to the outbound stream.
- `ui_render`: Client-reported timestamp when the card rendered in the user's viewport.
