# ADR 013 — Multi-Region Deployment Architecture and Provider Routing Table

**Status**: Proposed / Design Specification Only (No production infrastructure changes in Q-0e; aligns with §2, §4, ADR-007, ADR-008, and ADR-012)  
**Date**: 2026-11-14 (Stage Q-0e — Section E3)  
**Decision makers**: Engineering & Speech Platform Team

---

## Context

MeetMind AI captures live enterprise meeting audio across distributed participant geographies. Empirical network probing across global endpoints (`scripts/region_probe.py`, Q-0e Section E1) demonstrated that:

1. **Cross-Region Handshake Penalties**: Connecting to US-based ASR provider endpoints (`api.deepgram.com`, `api.assemblyai.com`, `stt-rt.soniox.com`) from India incurs a TCP + TLS + WebSocket Upgrade handshake latency of **1,300 ms to 1,530 ms**, consuming up to 510% of the Lane-0 P95 budget (`300 ms`).
2. **Co-located Handshake Efficiencies**: Connecting to regional endpoints in India (`api.in.deepgram.com`, `waves-api.smallest.ai`) incurs handshake latencies of **420 ms to 714 ms** with network RTT P50 of **30 ms to 75 ms**.
3. **First-Word Audio Truncation Risk**: If an ASR WebSocket session is initiated only upon participant speech detection, the 400–1,500 ms connection upgrade delay induces buffer bloat, pipeline backpressure, or speech truncation.
4. **Data Residency Compliance**: Enterprise tenants require that raw PCM audio frames, ephemeral streams, and transcript data never leave their home compliance perimeter (e.g. India DPDP, EU GDPR).

To achieve single-digit millisecond pipeline overhead (SLO P50 < 10 ms, P95 < 25 ms) globally while satisfying strict data residency, MeetMind AI requires a formal multi-region architecture.

---

## Decision

### 1. Architectural Topology: Regional Edge Pods + Global Control Plane

MeetMind AI decomposes its infrastructure into **Independent Regional Pods** and a **Global Control Plane**:

```
+-------------------------------------------------------------------------------------------------+
|                                    GLOBAL CONTROL PLANE                                         |
|  - Clerk User & Session Auth                                - Tenant Registry & Routing Table   |
|  - Global Neon PostgreSQL (Schema Migrations, Tenant Config) - Billing & Usage Aggregation      |
+---------------------------------------------------------------+---------------------------------+
                                                                |
                     +------------------------------------------+-----------------------------------------+
                     |                                                                                    |
                     v                                                                                    v
+-----------------------------------------------+    +-----------------------------------------------+
|          REGIONAL POD: INDIA (MUMBAI)         |    |         REGIONAL POD: EUROPE (FRANKFURT)      |
|  - MeetMind Gateway (FastAPI / WebSocket)     |    |  - MeetMind Gateway (FastAPI / WebSocket)     |
|  - Ephemeral Redis 7 (In-Memory Streams)      |    |  - Ephemeral Redis 7 (In-Memory Streams)      |
|  - MeetMind ASR Worker Pods (Local Workers)   |    |  - MeetMind ASR Worker Pods (Local Workers)   |
|  - Neon Dev/Prod Regional Replica Branch      |    |  - Neon Dev/Prod Regional Replica Branch      |
|  - Retrieval & Vector Cache (Qdrant Regional) |    |  - Retrieval & Vector Cache (Qdrant Regional) |
+-----------------------------------------------+    +-----------------------------------------------+
                     |                                                                                    |
                     v                                                                                    v
          Local Regional ASR Endpoints                         Local Regional ASR Endpoints
     - Deepgram India (api.in.deepgram.com)               - Deepgram Europe (api.eu.deepgram.com)
     - Smallest.ai Mumbai (waves-api.smallest.ai)         - Gladia Europe (api.gladia.io)
```

#### A. Regional Edge Pod Components

Every Regional Pod is self-contained and handles the complete audio-to-intelligence pipeline:

- **`meetmind_gateway`**: Terminates Chrome Extension WebSockets directly at the edge via Anycast / latency-based DNS routing.
- **In-Memory Redis 7**: Houses ephemeral audio streams (`{tenant}:audio:meeting:{id}:stream`) with strict 15m active / 5m grace TTL. Persistence (RDB/AOF) is strictly disabled (`save ""` and `appendonly no`).
- **`meetmind_asr` Workers**: Consumes local Redis streams and pumps audio to the co-located ASR provider endpoint.
- **Local Retrieval Cache**: Regional Qdrant and Redis caches holding tenant sprint boards, glossary keyterms, and decision context.
- **Zero Cross-Region Hot Path**: Audio frames, Redis stream reads, and ASR WebSocket streaming never cross regional boundaries during a live meeting.

#### B. Global Control Plane Components

- **Clerk Authentication**: Validates session JWTs and user credentials globally.
- **Neon Control Branch**: Stores tenant metadata, organization profiles, and regional pod assignments.
- **Audit Queue Workers**: Non-blocking ingestion of `asr_session_started` and `asr_session_summary` rows.

---

### 2. Tenant-to-Region Pinning and Latency-Based Routing

1. **Explicit Home Region Pinning**:
   - Each tenant organization has a configured `home_region` in their tenant profile (e.g. `in-south-1` for Indian enterprises, `eu-central-1` for European enterprises, `us-east-1` for North America).
   - All meeting metadata, encryption keys, and long-term intelligence artifacts remain bound to the tenant's pinned region.
2. **Edge DNS Latency Routing**:
   - DNS (`gateway.meetmind.ai`) resolves via latency-based GeoDNS routing (AWS Route 53 or Cloudflare Anycast) to direct clients to the nearest active gateway pod.
   - If a participant joins from outside the tenant's home region (e.g. an EU participant in an India-hosted meeting), audio streams to the tenant's designated meeting pod to preserve single-pod stream locality.

---

### 3. Provider Routing Table & Failover Rules

Each Regional Pod maintains a deterministic, configuration-driven **Provider Routing Table**:

| Pod Region                  | Priority 1 (Primary)                        | Priority 2 (Fallback)                             | Priority 3 (Accuracy/Batch)               | Failover Trigger Thresholds                                 |
| :-------------------------- | :------------------------------------------ | :------------------------------------------------ | :---------------------------------------- | :---------------------------------------------------------- |
| **India (`in-south-1`)**    | **Deepgram Nova-3** (`api.in.deepgram.com`) | **Smallest.ai Pulse** (`waves-api.smallest.ai`)   | Deepgram Nova-3 Async (`en-IN` / `multi`) | Consecutive WS reconnects >= 2 OR RTT > 250 ms OR 5xx >= 3% |
| **Europe (`eu-central-1`)** | **Deepgram Nova-3** (`api.eu.deepgram.com`) | **Gladia Solaria-1** (`api.gladia.io`)            | Gladia Solaria-3 Async (`api.gladia.io`)  | Consecutive WS reconnects >= 2 OR RTT > 200 ms OR 5xx >= 3% |
| **US East (`us-east-1`)**   | **Deepgram Nova-3** (`api.deepgram.com`)    | **AssemblyAI U-Streaming** (`api.assemblyai.com`) | AssemblyAI U-3.5 Async                    | Consecutive WS reconnects >= 2 OR RTT > 180 ms OR 5xx >= 3% |

#### Dynamic Failover Circuit Breakers

- **Heartbeat & Keepalive**: Gateway monitors provider latency every 10s.
- **Health Gate**: If the primary endpoint fails 2 consecutive handshakes or exceeds RTT thresholds, the worker automatically diverts new streams to the fallback provider without dropping raw audio frames (buffered in Redis Stream replay PEL).

---

### 4. Meeting Join Pre-Warming (Zero-Lag First Speech)

**Invariant**: The provider ASR WebSocket session **MUST BE OPENED AT MEETING JOIN**, before first speech.

- **Pre-Warming**: When the Chrome extension receives user consent (`consent_confirmed`) and gateway initializes the meeting session, `MeetingPipeline` immediately establishes the upstream provider WebSocket handshake.
- **Rationale**: Measuring 400 ms to 1,500 ms for TCP+TLS+WS upgrade means deferring connection to first speech creates a noticeable stutter or drops initial syllables. Keeping the socket pre-warmed with silent keepalive guarantees first speech is processed in $\le 10\text{ ms}$.

---

### 5. Multi-Region SLOs in `budget.yaml`

Regional performance budgets are formalized in [`docs/latency/budget.yaml`](file:///e:/MeetMind%20AI/docs/latency/budget.yaml):

```yaml
regions:
  in-south-1:
    network_rtt_p50_ms: 75.0
    network_rtt_p95_ms: 120.0
    ws_upgrade_p95_ms: 600.0
    lane_0_budget_p95_ms: 300.0
  eu-central-1:
    network_rtt_p50_ms: 45.0
    network_rtt_p95_ms: 80.0
    ws_upgrade_p95_ms: 400.0
    lane_0_budget_p95_ms: 250.0
  us-east-1:
    network_rtt_p50_ms: 35.0
    network_rtt_p95_ms: 65.0
    ws_upgrade_p95_ms: 350.0
    lane_0_budget_p95_ms: 250.0
```

---

### 6. What Is Deliberately NOT Built Yet

To preserve development focus and scope discipline:

1. **No Live Cross-Pod Replication**: We do not synchronize in-flight meeting audio between multiple cloud regions. A meeting is pinned to a single pod for its duration.
2. **No Multi-Cloud Mesh**: All pods are hosted on standard cloud infrastructure with identical Docker/Linux runtime containers.
3. **No Automatic Geographic Sharding of Existing Sessions**: Reconnecting clients reconnect to the same meeting pod via session affinity.
4. **No Third-Party Managed Gateway**: Gateway and ASR core remain strictly inside the MeetMind monorepo code base.

---

## Consequences

- **Latency Assurance**: Regional pod co-location limits network RTT to $< 75\text{ ms}$, ensuring Lane-0 P95 targets ($\le 300\text{ ms}$) are consistently met.
- **Regulatory Compliance**: Indian and European enterprises achieve strict local data residency without audio frames transiting foreign boundaries.
- **Operational Simplicity**: Every pod runs identical code from the MeetMind monorepo, parameterized solely by environment variables (`ASR_REGION`, `TENANT_HOME_REGION`).
