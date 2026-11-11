# Scenario 03 Design Review Adr

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Welcome to the architectural design review for ADR zero zero eight: vector datastore selection."

_Key Entities_: ADR zero zero eight (adr)

### Turn 02: SPEAKER_1 (Engineering Manager — Female — English)

> "Today we are evaluating PostgreSQL with pgvector extension versus Qdrant standalone vector database."

_Key Entities_: PostgreSQL (service), pgvector (library), Qdrant (service)

### Turn 03: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "I ran benchmarks on 10 million vectors with 1536 dimensions using cosine similarity metric."

_Key Entities_: 10 million vectors (metric), 1536 dimensions (metric)

### Turn 04: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "What were the observed p95 and p99 query latencies under 500 queries per second?"

_Key Entities_: p95 (metric), p99 (metric), 500 queries per second (metric)

### Turn 05: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Qdrant achieved p95 of 18 milliseconds and p99 of 25 milliseconds with HNSW indexing."

_Key Entities_: Qdrant (service), 18 milliseconds (metric), 25 milliseconds (metric), HNSW (library)

### Turn 06: SPEAKER_3 (Platform Engineer — Male — English)

> "In contrast, pgvector on PostgreSQL sixteen showed p95 of 85 milliseconds and p99 of 140 milliseconds."

_Key Entities_: pgvector (library), PostgreSQL (service), sixteen (version), 85 milliseconds (metric), 140 milliseconds (metric)

### Turn 07: SPEAKER_1 (Engineering Manager — Female — English)

> "Why is pgvector latency substantially higher on the 10 million vector dataset?"

_Key Entities_: pgvector (library), 10 million (metric)

### Turn 08: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "PostgreSQL shared buffers got thrashing because the HNSW index size is 28 gigabytes exceeding RAM."

_Key Entities_: PostgreSQL (service), HNSW (library), 28 gigabytes (metric)

### Turn 09: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Qdrant me memory mapped payload storage hai, isliye RAM footprint sirf 8 gigabytes tha."

_Key Entities_: Qdrant (service), 8 gigabytes (metric)

### Turn 10: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "What about tenant filtering? In MeetMind AI every vector search must filter by tenant ID."

_Key Entities_: MeetMind AI (service)

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Qdrant supports pre-filtering on payload schema with zero latency penalty using collection partitions."

_Key Entities_: Qdrant (service)

### Turn 12: SPEAKER_1 (Engineering Manager — Female — English)

> "What are the operational trade-offs of running a dedicated Qdrant cluster versus unified PostgreSQL?"

_Key Entities_: Qdrant (service), PostgreSQL (service)

### Turn 13: SPEAKER_3 (Platform Engineer — Male — English)

> "Qdrant requires its own Kubernetes statefulset with persistent volume claims and snapshot backups."

_Key Entities_: Qdrant (service), Kubernetes (service)

### Turn 14: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Our latency budget for Lane 1 retrieval is 150 milliseconds. Qdrant gives us 125 milliseconds headroom."

_Key Entities_: 150 milliseconds (metric), Qdrant (service), 125 milliseconds (metric)

### Turn 15: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Adopt Qdrant for vector embeddings and retain Neon PostgreSQL for relational audit logs."

_Key Entities_: Qdrant (service), Neon (service), PostgreSQL (service)

_Decisions_: Adopt Qdrant for embeddings and retain Neon PostgreSQL for relational audit log

### Turn 16: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Vikram will draft ADR zero zero eight and update architecture docs by Thursday evening."

_Key Entities_: Vikram (person), ADR zero zero eight (adr), Thursday evening (date)

_Action Items_: Vikram: Draft ADR-008 document (Due: Thursday evening)

### Turn 17: SPEAKER_3 (Platform Engineer — Male — English)

> "I will write Terraform manifests for Qdrant Kubernetes deployment in us-east-1 under INFRA four zero two."

_Key Entities_: Terraform (library), Qdrant (service), Kubernetes (service), INFRA four zero two (ticket)

_Action Items_: Ananya: Write Terraform manifests for Qdrant (Due: next week)

### Turn 18: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Ensure snapshot backups are configured to Amazon S3 every 4 hours with 30 day retention policy."

_Key Entities_: Amazon S3 (service), 4 hours (metric), 30 day (metric)

_Decisions_: Configure Qdrant snapshots to S3 every 4 hours with 30-day retention

### Turn 19: SPEAKER_1 (Engineering Manager — Female — Hinglish (Code-Switched))

> "Security team ko disaster recovery plan bhej dena approval ke liye."

_Action Items_: Priya: Send DR plan to security team (Due: Friday)

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Design review is concluded. ADR zero zero eight is approved with unanimous consensus."

_Key Entities_: ADR zero zero eight (adr)

_Decisions_: ADR-008 approved
