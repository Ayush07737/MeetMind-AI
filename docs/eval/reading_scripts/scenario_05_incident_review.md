# Scenario 05 Incident Review

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_1 (Engineering Manager — Female — English)

> "We are holding the blameless post-mortem for incident INC four zero nine which occurred yesterday."

_Key Entities_: INC four zero nine (incident)

### Turn 02: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "At 14:32 UTC, our PagerDuty alerted on gateway error rate spiking from 0.3 percent to 12.5 percent."

_Key Entities_: 14:32 UTC (date), PagerDuty (service), 0.3 percent (metric), 12.5 percent (metric)

### Turn 03: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "What was the customer impact? Did we breach our 99.9 percent uptime service level objective?"

_Key Entities_: 99.9 percent (metric)

### Turn 04: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Yes. The degraded state lasted 18 minutes affecting 340 active enterprise meetings."

_Key Entities_: 18 minutes (metric), 340 (number)

### Turn 05: SPEAKER_3 (Platform Engineer — Male — English)

> "I checked the Kubernetes node metrics. Node k8s worker zero four experienced an out of memory event."

_Key Entities_: Kubernetes (service)

### Turn 06: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Why did node worker zero four run out of memory? Which pod was exceeding limits?"

### Turn 07: SPEAKER_3 (Platform Engineer — Male — English)

> "The Redis container had no memory limit set in Helm chart and consumed 24 gigabytes of host RAM."

_Key Entities_: Redis (service), Helm (service), 24 gigabytes (metric)

### Turn 08: SPEAKER_2 (Senior SRE / DevOps Lead — Male — Hinglish (Code-Switched))

> "Host ne kernel OOM killer trigger kar diya, aur gateway pod kill ho gaya bina graceful shutdown ke."

### Turn 09: SPEAKER_1 (Engineering Manager — Female — English)

> "When the gateway pod was killed, why did the standby replica not take over immediately?"

### Turn 10: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Because the readiness probe had a 30 second initial delay and failure threshold of 3 attempts."

_Key Entities_: 30 second (metric), 3 (number)

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "That caused 90 seconds of dead routing where Envoy proxy continued forwarding traffic to the dead pod."

_Key Entities_: 90 seconds (metric), Envoy (service)

### Turn 12: SPEAKER_3 (Platform Engineer — Male — English)

> "Additionally, the Redis writer lease TTL was set to 60 seconds so clients could not acquire new lease."

_Key Entities_: Redis (service), 60 seconds (metric)

### Turn 13: SPEAKER_1 (Engineering Manager — Female — English)

> "Root cause is confirmed: missing container resource limits leading to OOM cascade and slow failover."

_Decisions_: Root cause: Unbounded Redis memory triggered OOM killer cascade on gateway node

### Turn 14: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Action item one: Vikram will set strict memory requests and limits of 8 gigabytes on Redis pods."

_Key Entities_: Vikram (person), 8 gigabytes (metric), Redis (service)

_Action Items_: Vikram: Set 8GB memory limits on Redis pods (Due: Monday)

### Turn 15: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Action item two: I will reduce Envoy readiness probe failure threshold from 3 to 1 attempt."

_Key Entities_: Envoy (service), 3 (number), 1 (number)

_Action Items_: Vikram: Tune Envoy readiness probe failure threshold (Due: Tuesday)

### Turn 16: SPEAKER_3 (Platform Engineer — Male — English)

> "Action item three: Ananya will reduce Redis writer lease heartbeat interval from 10 to 3 seconds."

_Key Entities_: Ananya (person), Redis (service), 10 (metric), 3 seconds (metric)

_Action Items_: Ananya: Reduce writer lease heartbeat to 3 seconds (Due: Wednesday)

### Turn 17: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Schedule a chaos engineering test next Thursday to verify pod eviction failover in under 5 seconds."

_Key Entities_: next Thursday (date), 5 seconds (metric)

_Decisions_: Schedule chaos engineering failover test next Thursday

_Action Items_: Priya: Run chaos engineering pod failover test (Due: next Thursday)

### Turn 18: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Rahul will write the customer incident summary for our enterprise SLA credits by Friday."

_Key Entities_: Rahul (person), Friday (date)

_Action Items_: Rahul: Write customer incident report (Due: Friday)

### Turn 19: SPEAKER_2 (Senior SRE / DevOps Lead — Female — Hinglish (Code-Switched))

> "Post-mortem documentation Confluence pe upload kar di hai, sabhi review kar lena."

_Key Entities_: Confluence (service)

### Turn 20: SPEAKER_1 (Engineering Manager — Female — English)

> "Thank you team. INC four zero nine is closed with all 4 action items assigned."

_Key Entities_: INC four zero nine (incident), 4 (number)

_Decisions_: Incident INC-409 resolved and closed
