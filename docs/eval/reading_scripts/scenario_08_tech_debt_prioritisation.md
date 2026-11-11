# Scenario 08 Tech Debt Prioritisation

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_1 (Engineering Manager — Female — English)

> "In this meeting we are prioritising our Q4 technical debt roadmap. We allocated 20 percent engineering bandwidth."

_Key Entities_: 20 percent (metric)

### Turn 02: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Item one: Decoupling the legacy monolithic billing module from the API gateway codebase."

### Turn 03: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Currently any billing change requires deploying all 12 backend microservices simultaneously."

_Key Entities_: 12 (number)

### Turn 04: SPEAKER_3 (Platform Engineer — Male — English)

> "It also carries 4500 lines of dead code from the old Stripe API integration."

_Key Entities_: 4500 lines (metric), Stripe (service)

### Turn 05: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Refactoring billing into standalone service under ticket TECH eight zero one will take 3 weeks."

_Key Entities_: TECH eight zero one (ticket), 3 weeks (metric)

### Turn 06: SPEAKER_1 (Engineering Manager — Female — English)

> "What is item two on our technical debt audit list?"

### Turn 07: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Item two: Decommissioning our self hosted Cassandra cluster which costs 4200 dollars per month."

_Key Entities_: Cassandra (service), 4200 dollars (metric)

### Turn 08: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Cassandra me compaction issues aate rehte hain, aur maintenance overhead bahut zyaada hai."

_Key Entities_: Cassandra (service)

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "We already migrated historical meetings to ClickHouse and PostgreSQL. Cassandra has zero active reads."

_Key Entities_: ClickHouse (service), PostgreSQL (service), Cassandra (service)

### Turn 10: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Vikram will drop Cassandra keyspace after final backup and terminate EC2 instances by Friday."

_Key Entities_: Vikram (person), Cassandra (service), Friday (date)

_Decisions_: Decommission Cassandra cluster by Friday to save $4200/month

_Action Items_: Vikram: Backup and terminate Cassandra EC2 instances (Due: Friday)

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "That immediately saves us 50400 dollars in annual infrastructure costs."

_Key Entities_: 50400 dollars (metric)

### Turn 12: SPEAKER_3 (Platform Engineer — Male — English)

> "Item three: Upgrading frontend monorepo tooling from Webpack to Vite."

_Key Entities_: Webpack (library), Vite (library)

### Turn 13: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Current production build takes 4 minutes 20 seconds. With Vite it builds in 18 seconds."

_Key Entities_: 4 minutes 20 seconds (metric), Vite (library), 18 seconds (metric)

### Turn 14: SPEAKER_1 (Engineering Manager — Female — English)

> "That is a 14x improvement in developer iteration speed. Who will take the Vite migration?"

_Key Entities_: 14x (metric), Vite (library)

### Turn 15: SPEAKER_3 (Platform Engineer — Male — English)

> "Ananya will own the Vite migration under ticket FRONT four one zero over next sprint."

_Key Entities_: Ananya (person), Vite (library), FRONT four one zero (ticket)

_Action Items_: Ananya: Migrate frontend build to Vite (Due: next sprint)

### Turn 16: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Item four: Upgrading Redis cluster from Redis six to Redis seven point two for active ACL support."

_Key Entities_: Redis (service), seven point two (version)

### Turn 17: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Redis seven gives us function scripts and per key permissions which strengthens our multi tenancy."

_Key Entities_: Redis (service)

### Turn 18: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Approve Cassandra teardown, Vite migration, and Redis seven upgrade as top 3 priorities."

_Key Entities_: Cassandra (service), Vite (library), Redis (service)

_Decisions_: Approved Q4 tech debt: Cassandra teardown, Vite migration, Redis 7 upgrade

### Turn 19: SPEAKER_3 (Platform Engineer — Female — Hinglish (Code-Switched))

> "Jira board pe priorities update kar diye hain."

_Key Entities_: Jira (service)

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Excellent prioritisation session. Let us execute on these high leverage refactorings."
