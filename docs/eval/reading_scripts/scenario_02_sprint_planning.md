# Scenario 02 Sprint Planning

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_1 (Engineering Manager — Female — English)

> "Welcome to sprint planning for sprint forty two. Our total velocity target is 45 story points."

_Key Entities_: 45 story points (metric)

### Turn 02: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "First item is ENG seven eight nine: migrating our Python backend from version 3.10 to Python 3.13."

_Key Entities_: ENG seven eight nine (ticket), Python (library), 3.10 (version), 3.13 (version)

### Turn 03: SPEAKER_3 (Platform Engineer — Male — English)

> "With uv package manager, our dependency resolution will drop from 45 seconds to under 2 seconds."

_Key Entities_: uv (library), 45 seconds (metric), 2 seconds (metric)

### Turn 04: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "I estimate ENG seven eight nine at 5 story points because of asyncio event loop changes in Python 3.13."

_Key Entities_: ENG seven eight nine (ticket), 5 story points (metric), asyncio (library), 3.13 (version)

### Turn 05: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Agreed on 5 points. Rahul will own the Python 3.13 migration by Friday November fifteenth."

_Key Entities_: Rahul (person), 3.13 (version), Friday November fifteenth (date)

_Decisions_: Commit ENG-789 at 5 story points for Sprint 42

_Action Items_: Rahul: Complete Python 3.13 migration (Due: Friday November 15)

### Turn 06: SPEAKER_1 (Engineering Manager — Female — English)

> "Next ticket is DATA two zero four: partitioning Kafka audit topic across 12 partitions."

_Key Entities_: DATA two zero four (ticket), Kafka (service), 12 partitions (metric)

### Turn 07: SPEAKER_3 (Platform Engineer — Male — English)

> "Currently we have only 3 partitions handling 15000 messages per second, which causes consumer lag."

_Key Entities_: 3 partitions (metric), 15000 messages per second (metric)

### Turn 08: SPEAKER_2 (Senior SRE / DevOps Lead — Male — Hinglish (Code-Switched))

> "Lag badhta jaa raha hai, weekend pe consumer rebalance me do ghante lag gaye the."

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Right. Increasing partitions to 12 allows 12 concurrent workers. Estimate is 3 story points."

_Key Entities_: 12 partitions (metric), 12 concurrent workers (metric), 3 story points (metric)

### Turn 10: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Vikram will execute Kafka topic repartitioning on staging Wednesday night."

_Key Entities_: Vikram (person), Kafka (service)

_Decisions_: Execute Kafka topic repartitioning on staging Wednesday night

_Action Items_: Vikram: Kafka topic repartitioning on staging (Due: Wednesday night)

### Turn 11: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Third item is CORE six zero one: implementing tenant isolation router with Neon schema routing."

_Key Entities_: CORE six zero one (ticket), Neon (service)

### Turn 12: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "That requires dynamic schema creation and connection pooling via PgBouncer with 50 max connections."

_Key Entities_: PgBouncer (service), 50 max connections (metric)

### Turn 13: SPEAKER_3 (Platform Engineer — Male — English)

> "I estimate CORE six zero one at 8 story points. It is a 2 week effort involving security audit."

_Key Entities_: CORE six zero one (ticket), 8 story points (metric), 2 week (metric)

### Turn 14: SPEAKER_1 (Engineering Manager — Female — English)

> "Can we split CORE six zero one into phase one for schema routing and phase two for connection limits?"

_Key Entities_: CORE six zero one (ticket)

### Turn 15: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Yes. Phase one is 5 points for this sprint; phase two will be 3 points in sprint forty three."

_Key Entities_: 5 points (metric), 3 points (metric)

_Decisions_: Split CORE-601 into 5 points for Sprint 42 and 3 points for Sprint 43

### Turn 16: SPEAKER_3 (Platform Engineer — Male — English)

> "Ananya will take phase one and deliver the schema router pull request by next Tuesday."

_Key Entities_: Ananya (person), next Tuesday (date)

_Action Items_: Ananya: Deliver schema router PR (Due: next Tuesday)

### Turn 17: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "What about ClickHouse ingestion benchmark under ticket INFRA nine zero nine?"

_Key Entities_: ClickHouse (service), INFRA nine zero nine (ticket)

### Turn 18: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Sneha will benchmark ClickHouse ingestion at 50000 events per second by Wednesday."

_Key Entities_: Sneha (person), ClickHouse (service), 50000 events per second (metric)

_Action Items_: Sneha: Benchmark ClickHouse ingestion (Due: Wednesday)

### Turn 19: SPEAKER_1 (Engineering Manager — Female — English)

> "Total committed points for sprint forty two is 42 points. That leaves 3 points buffer."

_Key Entities_: 42 points (metric), 3 points (metric)

_Decisions_: Commit 42 story points for Sprint 42

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Sprint backlog is committed. Let us start development on sprint forty two tickets."
