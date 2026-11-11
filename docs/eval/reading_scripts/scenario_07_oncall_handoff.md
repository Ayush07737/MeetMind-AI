# Scenario 07 Oncall Handoff

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Welcome to the weekly on-call shift handoff. I am handing over primary pager to Ananya."

_Key Entities_: Ananya (person)

### Turn 02: SPEAKER_3 (Platform Engineer — Male — English)

> "Thanks Vikram. How was the past week? How many pages did you receive?"

_Key Entities_: Vikram (person)

### Turn 03: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "We had 14 total alerts. Fortunately only 2 required manual intervention."

_Key Entities_: 14 (number), 2 (number)

### Turn 04: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "What caused the 12 non-actionable alerts? We need to reduce alert fatigue."

_Key Entities_: 12 (number)

### Turn 05: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "The Prometheus alert for Redis CPU exceeding 80 percent fired 10 times during batch cache invalidation."

_Key Entities_: Prometheus (service), Redis (service), 80 percent (metric), 10 (number)

### Turn 06: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Cache invalidation spikes CPU for only 3 seconds. Let us increase alert duration threshold to 5 minutes."

_Key Entities_: 3 seconds (metric), 5 minutes (metric)

_Decisions_: Tune Redis CPU alert threshold to require 5 minutes persistence

_Action Items_: Vikram: Update Prometheus alert rule for Redis CPU (Due: Wednesday)

### Turn 07: SPEAKER_3 (Platform Engineer — Male — English)

> "What were the 2 actionable alerts that required intervention?"

_Key Entities_: 2 (number)

### Turn 08: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "First was OpenTelemetry collector memory usage reaching 95 percent due to uncompressed trace batches."

_Key Entities_: OpenTelemetry (service), 95 percent (metric)

### Turn 09: SPEAKER_1 (Engineering Manager — Female — English)

> "Did that cause trace data loss for downstream Jaeger indexing?"

_Key Entities_: Jaeger (service)

### Turn 10: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "No, the file exporter buffered traces. I restarted the collector pod and enabled zstd compression."

_Key Entities_: zstd (library)

### Turn 11: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Dusra alert konsa tha? Kya Neon database connection pool me issue tha?"

_Key Entities_: Neon (service)

### Turn 12: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Yes, exactly. At 03:00 AM Neon compute scaled down after 5 minutes of idle, causing a cold start delay."

_Key Entities_: Neon (service), 5 minutes (metric)

### Turn 13: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Cold start latency on Neon dev branch was 2.8 seconds. We should set minimum compute units to 0.25."

_Key Entities_: Neon (service), 2.8 seconds (metric), 0.25 (number)

### Turn 14: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Rahul will configure Neon autoscaling floor to 0.25 compute units on production branch."

_Key Entities_: Rahul (person), Neon (service), 0.25 (number)

_Decisions_: Set Neon compute floor to 0.25 units to eliminate cold starts

_Action Items_: Rahul: Configure Neon compute floor (Due: Friday)

### Turn 15: SPEAKER_3 (Platform Engineer — Male — English)

> "Any ongoing maintenance scheduled for the upcoming on-call shift?"

### Turn 16: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "AWS is performing network maintenance in us-east-1a on Saturday between 02:00 and 04:00 UTC."

_Key Entities_: AWS (service), Saturday (date)

### Turn 17: SPEAKER_3 (Platform Engineer — Male — English)

> "I will ensure our Kubernetes pods are balanced across availability zones 1a, 1b, and 1c."

_Key Entities_: Kubernetes (service)

_Action Items_: Ananya: Verify pod distribution across AZs (Due: Friday)

### Turn 18: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Escalation path: if primary pager is not acknowledged in 10 minutes, secondary is Rahul."

_Key Entities_: 10 minutes (metric), Rahul (person)

### Turn 19: SPEAKER_1 (Engineering Manager — Female — Hinglish (Code-Switched))

> "PagerDuty me shift rotation update kar diya hai. Good luck Ananya!"

_Key Entities_: PagerDuty (service), Ananya (person)

### Turn 20: SPEAKER_3 (Platform Engineer — Male — English)

> "Shift handover complete. I have the primary on-call pager."

_Decisions_: Ananya assumes primary on-call
