# Scenario 09 Api Contract Discussion

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Today we are reviewing proposed changes to our Protocol Buffer schema for Ingestion Gateway v2."

_Key Entities_: Protocol Buffer (library)

### Turn 02: SPEAKER_1 (Engineering Manager — Female — English)

> "We must ensure strict backward compatibility for mobile extension clients running version one point zero."

_Key Entities_: one point zero (version)

### Turn 03: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "In PR four nine zero, field 4 session token was removed. That is a breaking wire format change."

_Key Entities_: PR four nine zero (pr), 4 (number)

### Turn 04: SPEAKER_3 (Platform Engineer — Male — English)

> "Protobuf rule: never delete or renumber fields. We must mark field 4 as reserved."

_Key Entities_: 4 (number)

### Turn 05: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Correct. Rahul will update PR four nine zero to mark field 4 reserved and add field 7 client instance ID."

_Key Entities_: Rahul (person), PR four nine zero (pr), 4 (number), 7 (number)

_Decisions_: Mark Protobuf field 4 reserved and allocate field 7 for client_instance_id

_Action Items_: Rahul: Update Protobuf schema in PR four nine zero (Due: today)

### Turn 06: SPEAKER_1 (Engineering Manager — Female — English)

> "What about the new audio frame metadata? We need capture timestamp in milliseconds on every frame."

### Turn 07: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "We added uint64 capture ts ms as field 8 and uint32 sequence number as field 9 in AudioChunk message."

_Key Entities_: 8 (number), 9 (number)

### Turn 08: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Client side clock skew ke liye humne NTP synchronization check add kiya tha na?"

_Key Entities_: NTP (library)

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Yes. The gateway clamps timestamps if client clock differs by more than 60 seconds from server clock."

_Key Entities_: 60 seconds (metric)

### Turn 10: SPEAKER_1 (Engineering Manager — Female — English)

> "How do we publish these schema definitions to our TypeScript client repository?"

_Key Entities_: TypeScript (library)

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "We use buf CLI in GitHub Actions workflow to generate both Python Pydantic classes and TypeScript types."

_Key Entities_: buf (library), GitHub Actions (service), Python (library), Pydantic (library), TypeScript (library)

### Turn 12: SPEAKER_3 (Platform Engineer — Male — English)

> "Running buf breaking against main branch caught the deleted field 4 automatically."

_Key Entities_: buf (library), 4 (number)

### Turn 13: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Buf breaking check should be made a required status check on all pull requests."

_Decisions_: Enforce buf breaking CI check on all schema PRs

### Turn 14: SPEAKER_1 (Engineering Manager — Female — English)

> "What is our deprecation policy for clients running v1 API?"

### Turn 15: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "We provide a 60 day deprecation notice with telemetry logging of v1 requests in Datadog."

_Key Entities_: 60 day (metric), Datadog (service)

### Turn 16: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Vikram will write the deprecation notice in API documentation by next Wednesday."

_Key Entities_: Vikram (person), next Wednesday (date)

_Action Items_: Vikram: Publish API deprecation notice (Due: next Wednesday)

### Turn 17: SPEAKER_3 (Platform Engineer — Male — English)

> "I will verify buf generate produces valid Zod schemas matching shared schemas package."

_Key Entities_: buf (library), Zod (library)

_Action Items_: Ananya: Verify generated Zod schemas (Due: tomorrow)

### Turn 18: SPEAKER_1 (Engineering Manager — Female — Hinglish (Code-Switched))

> "Schema contract lock ho gaya hai, breaking changes avoid karna hi best practice hai."

### Turn 19: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Decision: Protocol Buffer schema v2 approved with reserved field 4 and buf breaking enforcement in CI."

_Key Entities_: Protocol Buffer (library), 4 (number), ci (service)

_Decisions_: Approve Protobuf v2 with reserved field 4 and buf breaking gate in CI

### Turn 20: SPEAKER_1 (Engineering Manager — Female — English)

> "Meeting concluded. Excellent work maintaining schema integrity across our polyglot stack."
