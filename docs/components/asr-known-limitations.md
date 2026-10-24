# Component #2 (ASR) — Known Limitations & Operational Realities

This document records the empirical constraints, platform quirks, and architectural boundaries discovered and verified during Component #2 (ASR streaming service) development, testing, and live verification.

---

## 1. Operating System & Timer Granularity (Windows Local Development)

### 1.1 Timer Quantum & Sleep Jitter

- **Mechanism**: The default Windows kernel thread scheduling quantum is ~15.625ms (64 timer ticks per second).
- **Impact**: In Python `asyncio`, calls to `asyncio.sleep(0.100)` or sequential sleep operations experience jitter of up to 15.6ms to 30ms unless explicit high-resolution multimedia timers (`timeBeginPeriod(1)`) are configured system-wide.
- **Benchmarking Mitigation**:
  - The verification harness (`scripts/asr_latency_bench.py`) anchors audio frame transmission to an absolute monotonic start time rather than relative incremental sleeps:
    $$t_{\text{target}} = t_{\text{session\_start}} + (\text{seq} \times 0.100)$$
  - Calibration test thresholds dynamically adapt: **$\le 35.0\text{ms}$ on Windows (`win32`)** vs **$\le 15.0\text{ms}$ on Linux/POSIX**.

---

## 2. Process Working Set & Memory Observability

### 2.1 The 300-Second Windows Memory Trimming Sawtooth

- **Mechanism**: The Windows Memory Manager periodically evaluates process working sets and page access rates, forcing page trimming roughly every 300 seconds.
- **Impact on RSS / USS**: Process Resident Set Size (RSS) and Unique Set Size (USS) cyclically plummet (e.g. from ~60 MB to ~18 MB) before expanding back up to ~38 MB. A simple linear regression across raw RSS/USS samples is dominated by this OS-level sawtooth, producing volatile and inaccurate slope metrics (e.g. +0.347 MB/min).
- **Authoritative Measurement Protocol**:
  - Raw RSS is rejected as a memory leak indicator.
  - A run is declared **PASS** for memory stability strictly when:
    1. **In-process Tracemalloc Slope ($T \ge 5\text{m}$)**: $\le 0.02\text{ MB/min}$
    2. **USS Minimum-Envelope Slope ($T \ge 5\text{m}$)**: $\le 0.05\text{ MB/min}$
  - Snapshot diffs from Minute 5 to Minute 30 must demonstrate flat or shrinking heap allocations across internal stage buffers.

### 2.2 In-Memory Object Bounding

- **Interim Words**: Handled via `_seen_interim_words_window` (`deque(maxlen=300)`) and `_seen_interim_words` set. When the deque pops oldest items, the set is pruned, bounding peak memory to $\le 300$ active tuple records.
- **Timeline & History**: `_chunk_history` is bounded to `maxlen=600`, and `TimelineMapper` prunes historical anchors beyond a 120-second sliding window while retaining `_last_pruned_anchor`.
- **Latency Percentiles**: All rolling metrics in `AsrMetrics` enforce bounded ring deques (`maxlen=100` or `maxlen=300`).

---

## 3. Network Transit & Regional Jitter

### 3.1 Ping/Pong RTT Range to Cloud Regional Ingress

- **Measurements**: Empirical testing from development hosts to Deepgram's India cloud endpoint (`api.in.deepgram.com` in AWS Mumbai `ap-south-1`) shows:
  - **P50 WebSocket Ping/Pong RTT**: $112\text{ms} - 137\text{ms}$
  - **Range & Jitter**: Varies between **$75.0\text{ms}$ and $203.4\text{ms}$** depending on ISP routing, upstream peering, and cloud gateway load.
- **Session Handshake**: Cold connection setup (DNS + TCP + TLS 1.3 + HTTP 101 WS upgrade) takes P50 ~535ms, P95 ~660ms, well within the 2.0s SLO for automatic failover.

---

## 4. Nova-3 Interim Latency vs Platform Target

### 4.1 Physical Bounds of Streaming ASR

- **Platform Target**: The build specification established an aspirational target of P50 < 500ms for interim transcripts.
- **Physical Reality**: Deepgram Nova-3 is a large streaming transformer model requiring an internal acoustic chunk accumulation window (400ms–800ms) to resolve acoustic ambiguity before outputting token predictions. Combined with two-way network transit ($2 \times 50\text{ms} - 85\text{ms}$) and socket serialization, the physical lower bound for interim results is:
  $$\text{Interim Latency} \approx \text{RTT} + \Delta t_{\text{acoustic}} \approx 600\text{ms} - 1000\text{ms}$$
- **Operational Policy**:
  - Interim latency is recorded honestly as **`[NOT MET]`** without artificial mathematical offsets.
  - Zero latency verdicts may be printed for `INVALID` runs, and any interim latency below the physical speed-of-light minimum ($\text{P50} < \frac{\text{RTT}}{2}$) is flagged as **`INVALID`**.

---

## 5. Codec Compatibility (v1 Scope)

### 5.1 Linear PCM16 Only

- **Supported**: `codec=1` (linear PCM 16-bit Little Endian at 16000 Hz, 1 or 2 channels).
- **Unsupported**: `codec=2` (compressed Opus frames).
- **Behavior**: Chunks with `codec=2` are acknowledged by the Gateway to prevent client socket loops, then dropped by the ASR service with exactly one `transcript_service_degraded` event per meeting (`reason: "unsupported_codec"`). Capture adapters must perform client-side linear decoding in v1.

---

## 6. Audio Stream Retention & Lifecycle Expiration

### 6.1 Redis Stream 15-Minute Active Streaming Expiry

- **Active TTL**: In `meetmind_gateway/redis_streams.py`, each `XADD` to `mm:{tenant}:audio:{meeting}` refreshes the key expiration to **900 seconds (15 minutes)**.
- **Inactivity Purging**: The Gateway Janitor task (`janitor.py`) scans for idle meetings where active streaming has halted or listener count drops to 0. If a meeting approaches 900 seconds without activity, it is transitioned to `abandoned`, writing an audit record and purging expired Redis streams.
