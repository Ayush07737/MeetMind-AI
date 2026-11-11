# Scenario 10 Disagreement Unresolved

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "We are discussing event bus architecture for Component six. There is a disagreement between Redis Streams and Kafka."

_Key Entities_: Redis (service), Kafka (service)

### Turn 02: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "I strongly advocate for Redis Streams. It is already deployed in docker-compose.dev.yml and production."

_Key Entities_: Redis (service)

### Turn 03: SPEAKER_1 (Engineering Manager — Female — English)

> "However Priya points out that Redis Streams lacks multi region active active replication out of the box."

_Key Entities_: Priya (person), Redis (service)

### Turn 04: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Our latency budget requires p95 below 10 milliseconds. Redis Streams gives us 1.2 milliseconds publish latency."

_Key Entities_: 10 milliseconds (metric), Redis (service), 1.2 milliseconds (metric)

### Turn 05: SPEAKER_3 (Platform Engineer — Male — English)

> "Kafka publish latency is 15 milliseconds because of disk fsync and acks equal all quorum replication."

_Key Entities_: Kafka (service), 15 milliseconds (metric)

### Turn 06: SPEAKER_1 (Engineering Manager — Female — English)

> "If an entire AWS availability zone fails, in memory Redis Streams loses in flight meeting transcripts."

_Key Entities_: AWS (service), Redis (service)

### Turn 07: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Security invariant WP six states raw audio and transient transcripts must never persist to permanent disk."

### Turn 08: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Lekin enterprise audit compliance ke liye 30 din ka message retention mandatory ho sakta hai na?"

_Key Entities_: 30 din (metric)

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Audit events already route to Neon PostgreSQL audit log table which is append only and durable."

_Key Entities_: Neon (service), PostgreSQL (service)

### Turn 10: SPEAKER_1 (Engineering Manager — Female — English)

> "What about backpressure handling when retrieval reasoning pipeline falls behind speech ingestion?"

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Redis Streams consumer groups support pending entries list and delayed acks with 5 second grace window."

_Key Entities_: Redis (service), 5 second (metric)

### Turn 12: SPEAKER_3 (Platform Engineer — Male — English)

> "Kafka handles backpressure better because consumer offsets can lag for hours without memory pressure."

_Key Entities_: Kafka (service)

### Turn 13: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Notice that Redis memory is capped: maxlen 300 entries per stream ensures bounded footprint."

_Key Entities_: Redis (service), 300 (number)

### Turn 14: SPEAKER_1 (Engineering Manager — Female — English)

> "We still have an unresolved disagreement regarding cross region disaster recovery."

### Turn 15: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Can Redis Enterprise active active CRDT replication solve the disaster recovery requirement?"

_Key Entities_: Redis (service)

### Turn 16: SPEAKER_3 (Platform Engineer — Male — English)

> "We do not have empirical latency numbers for Redis CRDT cross region replication between Mumbai and Frankfurt."

_Key Entities_: Redis (service)

### Turn 17: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Unresolved Question: What is the exact cross region replication RPO and RTO for Redis Streams versus Kafka?"

_Key Entities_: Redis (service), Kafka (service)

_Decisions_: Unresolved: Disaster recovery RPO/RTO across regions for Redis Streams vs Kafka

### Turn 18: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: We will not make a premature architectural commitment today."

_Decisions_: Hold decision on cross-region event bus pending prototype spike

### Turn 19: SPEAKER_2 (Senior SRE / DevOps Lead — Female — Hinglish (Code-Switched))

> "Agreed. Dono systems pe 2 din ka prototype spike run karte hain."

_Key Entities_: 2 din (metric)

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Vikram and Ananya will conduct joint spike benchmark and present empirical latency data next Wednesday."

_Key Entities_: Vikram (person), Ananya (person), next Wednesday (date)

_Action Items_: Vikram and Ananya: Benchmark Redis Streams vs Kafka cross-region (Due: next Wednesday)
