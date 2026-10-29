# Component #2 (ASR) — Known Limitations & Operational Realities

This document records the empirical constraints, platform quirks, and architectural boundaries discovered and verified during Component #2 (ASR streaming service) development, testing, and live verification.

---

## 1. Operating System & Timer Granularity (Windows Local Development)

### 1.1 Timer Quantum & Calibration Precision

- **Mechanism**: The default Windows kernel thread scheduling quantum is ~15.625ms (64 timer ticks per second).
- **Mitigation & Contract**: System timer resolution is set to 1ms via `ctypes.windll.winmm.timeBeginPeriod(1)`. Measured sleep calibration jitter across 5 consecutive runs is $< 2.0\text{ms}$ (mean 1.83ms, max 2.01ms), strictly satisfying the **$\le 10.0\text{ms}$ contract threshold** across all platforms without platform-specific threshold relaxation.
- **Frame Scheduling**: Audio frame transmission is anchored to monotonic elapsed time ($t_{\text{target}} = t_0 + \text{seq} \times 0.100$) rather than relative incremental sleeps to prevent compounding drift.

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
- **Latency Percentiles**: All rolling metrics in `AsrMetrics` enforce bounded ring deques (`maxlen=100` or `maxlen=300`). Full samples in benchmark mode are appended off the hot path to JSONL/history arrays for comprehensive post-run evaluation.

---

## 3. Network Transit & Regional Jitter

### 3.1 Ping/Pong RTT Range to Cloud Regional Ingress

- **Measurements**: Empirical testing from development hosts to Deepgram's India cloud endpoint (`api.in.deepgram.com` in AWS Mumbai `ap-south-1`) shows:
  - **P50 WebSocket Ping/Pong RTT**: $112\text{ms} - 137\text{ms}$
  - **Range & Jitter**: Varies between **$75.0\text{ms}$ and $203.4\text{ms}$** depending on ISP routing, upstream peering, and cloud gateway load.
- **Session Handshake**: Cold connection setup (DNS + TCP + TLS 1.3 + HTTP 101 WS upgrade) takes P50 ~535ms, P95 ~660ms, well within the 2.0s SLO for automatic failover.

---

## 4. Nova-3 Interim Latency vs Platform Target

### 4.1 Observed P50 of 590 to 690 ms Across Runs

- **Platform Target**: The build specification established an aspirational target of P50 < 500ms for interim transcripts.
- **Observed Measurements**: Deepgram Nova-3 is a streaming transformer model requiring an internal acoustic chunk accumulation window (400ms–800ms) to resolve acoustic ambiguity before outputting token predictions. Combined with two-way network transit ($2 \times 50\text{ms} - 85\text{ms}$) and socket serialization, empirical testing demonstrates an **observed P50 of 590 to 690 ms across runs** (and up to 850–1000ms under high network jitter).
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

## 6. Audio Stream Retention & Freeze Root Cause Retraction

### 6.1 Redis Stream TTL and Retraction of Freeze Hypothesis

- **Active TTL Refreshed Per Frame**: In `meetmind_gateway/redis_streams.py`, each `XADD` to `mm:{tenant}:audio:{meeting}` refreshes the key expiration to **900 seconds (15 minutes)**. Because the TTL is refreshed on every individual audio frame received, active streams never expire during transmission.
- **Retraction**: We explicitly retract the earlier hypothesis that 900s stream TTL expiration or burst ingestion caused the two pipeline freezes observed during early exploratory testing. Later 30-minute benchmark runs (18,000+ frames) repeatedly crossed the 900s mark with zero frame loss, zero stream expiration, and continuous transcript processing. The root cause of the earlier freezes is unknown (two isolated occurrences, both observed prior to disabling incremental sleep).
- **Inactivity Purging**: The Gateway Janitor task (`janitor.py`) scans for idle meetings where active streaming has halted or listener count drops to 0. If a meeting approaches 900 seconds without activity, it is transitioned to `abandoned`, writing an audit record and purging expired Redis streams.
