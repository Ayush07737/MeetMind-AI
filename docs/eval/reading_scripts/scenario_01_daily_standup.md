# Scenario 01 Daily Standup

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Good morning everyone. Let us start today's standup. Vikram, what is your update on PLAT one zero one?"

_Key Entities_: PLAT one zero one (ticket), Vikram (person)

### Turn 02: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Yesterday I investigated the Redis cache latency spike. The p99 latency reached 450 milliseconds at 2500 rps."

_Key Entities_: Redis (service), p99 (metric), 450 milliseconds (metric), 2500 rps (metric)

### Turn 03: SPEAKER_1 (Engineering Manager — Female — English)

> "Was that caused by unindexed queries on PostgreSQL sixteen point two?"

_Key Entities_: PostgreSQL (service), sixteen point two (version)

### Turn 04: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Yes exactly. I opened PR four eight two to add a composite index on tenant ID and session ID."

_Key Entities_: PR four eight two (pr)

### Turn 05: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Great. Rahul will review PR four eight two before noon today."

_Key Entities_: Rahul (person), PR four eight two (pr)

_Action Items_: Rahul: Review PR four eight two (Due: today noon)

### Turn 06: SPEAKER_3 (Platform Engineer — Male — English)

> "On my side, ticket ENG four five two one is blocked on Kafka cluster upgrade to version three point seven."

_Key Entities_: ENG four five two one (ticket), Kafka (service), three point seven (version)

### Turn 07: SPEAKER_1 (Engineering Manager — Female — English)

> "Why is Kafka upgrade blocked? Did the staging migration fail?"

_Key Entities_: Kafka (service)

### Turn 08: SPEAKER_3 (Platform Engineer — Male — English)

> "The consumer group rebalance timed out because max poll interval was set to 300 seconds."

_Key Entities_: Kafka (service), 300 seconds (metric)

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Let us bump max poll interval to 600 seconds in Helm. Vikram will own that config change by tomorrow."

_Key Entities_: 600 seconds (metric), Helm (service), Vikram (person)

_Decisions_: Bump Kafka max poll interval to 600 seconds in Helm chart

_Action Items_: Vikram: Update Helm chart config for Kafka (Due: tomorrow)

### Turn 10: SPEAKER_2 (Senior SRE / DevOps Lead — Male — Hinglish (Code-Switched))

> "Logs check kar lo ek baar, Grafana dashboard pe memory usage 85 percent dikha raha hai."

_Key Entities_: Grafana (service), 85 percent (metric)

### Turn 11: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Understood. The memory leak is in the gRPC telemetry exporter. It consumed 500 megabytes in two hours."

_Key Entities_: gRPC (library), 500 megabytes (metric), two hours (metric)

### Turn 12: SPEAKER_1 (Engineering Manager — Female — English)

> "What about FRONT three zero three ticket for the dashboard widget?"

_Key Entities_: FRONT three zero three (ticket)

### Turn 13: SPEAKER_3 (Platform Engineer — Male — English)

> "Sneha completed TypeScript schemas in shared schemas package. PR five zero one is ready for review."

_Key Entities_: Sneha (person), TypeScript (library), PR five zero one (pr)

### Turn 14: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "I tested the build with Vite and Vitest. All 24 unit tests passed in 1.2 seconds."

_Key Entities_: Vite (library), Vitest (library), 1.2 seconds (metric)

### Turn 15: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Is Docker compose dev environment updated with Qdrant version one point eight?"

_Key Entities_: Docker (service), Qdrant (service), one point eight (version)

### Turn 16: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Yes, docker compose file is updated. Run docker compose pull to fetch latest image."

_Key Entities_: docker (service)

### Turn 17: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: We will hold the production deployment until the PostgreSQL index finishes building."

_Key Entities_: PostgreSQL (service)

_Decisions_: Hold production deployment until PostgreSQL index finishes building

### Turn 18: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Agreed. Index build on the 50 gigabyte table will take approximately 45 minutes."

_Key Entities_: 50 gigabyte (metric), 45 minutes (metric)

### Turn 19: SPEAKER_3 (Platform Engineer — Female — Hinglish (Code-Switched))

> "Main staging environment verify kar lungi, PR merge kar dena testing ke baad."

_Key Entities_: PR (pr)

_Action Items_: Sneha: Verify staging environment (Due: after testing)

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Sounds good. Standup adjourned. Let us sync on Slack channel infra alerts if latency spikes."

_Key Entities_: Slack (service)
