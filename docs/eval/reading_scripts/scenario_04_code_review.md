# Scenario 04 Code Review

**Scenario Context**: Engineering team discussions involving architectural and operational decisions.

### Turn 01: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Let us review PR four eight two by Rahul: implementing resilient gRPC connection pool."

_Key Entities_: PR four eight two (pr), Rahul (person), gRPC (library)

### Turn 02: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "I looked at line 145 in connection pool manager. The channel is not closed when max lifetime expires."

_Key Entities_: 145 (number)

### Turn 03: SPEAKER_3 (Platform Engineer — Male — English)

> "Good catch. If the channel is not closed, file descriptors leak until process hits ulimit of 1024."

_Key Entities_: 1024 (number)

### Turn 04: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "I will add context manager with channel.close in async context block to guarantee cleanup."

_Action Items_: Rahul: Add channel.close cleanup to PR four eight two (Due: today)

### Turn 05: SPEAKER_1 (Engineering Manager — Female — English)

> "Also look at lines 210 to 225: the reconnect backoff has no random jitter added."

_Key Entities_: 210 (number), 225 (number)

### Turn 06: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Without jitter, when the gateway restarts 500 extension clients will reconnect in synchronized wave."

_Key Entities_: 500 (number)

### Turn 07: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Understood. We will implement full jitter formula with base delay 100 milliseconds and max delay 5 seconds."

_Key Entities_: 100 milliseconds (metric), 5 seconds (metric)

_Decisions_: Adopt full jitter exponential backoff on client reconnects

### Turn 08: SPEAKER_3 (Platform Engineer — Male — Hinglish (Code-Switched))

> "Bhai unit tests me timeout test case missing hai, network partition simulate karke dekhna padega."

### Turn 09: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Agreed. I will add test_grpc_reconnect_jitter with mock server injecting 503 service unavailable."

_Key Entities_: 503 (number)

_Action Items_: Rahul: Add jitter unit test with mock server (Due: tomorrow)

### Turn 10: SPEAKER_1 (Engineering Manager — Female — English)

> "What about PR five one five for ticket FRONT three zero four? Sneha submitted the card renderer."

_Key Entities_: PR five one five (pr), FRONT three zero four (ticket), Sneha (person)

### Turn 11: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "PR five one five looks clean. It validates Zod schema for InsightCard and passes all 18 Vitest tests."

_Key Entities_: PR five one five (pr), Zod (library), InsightCard (library), 18 (number), Vitest (library)

### Turn 12: SPEAKER_3 (Platform Engineer — Male — English)

> "One question on PR five one five: does it handle revision out of order delivery?"

_Key Entities_: PR five one five (pr)

### Turn 13: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Yes, line 78 checks current revision number and discards incoming card if revision is lower or equal."

_Key Entities_: 78 (number)

### Turn 14: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Excellent revision semantics. That matches the quality contract specification exactly."

### Turn 15: SPEAKER_1 (Engineering Manager — Female — English)

> "Decision: Approve PR five one five once CI pipeline finishes on GitHub Actions."

_Key Entities_: PR five one five (pr), GitHub Actions (service)

_Decisions_: Approve PR-515 upon CI green

### Turn 16: SPEAKER_3 (Platform Engineer — Female — Hinglish (Code-Switched))

> "CI pass ho gaya hai, main merge button click kar rahi hoon."

### Turn 17: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Rahul will finish updating PR four eight two by 5 PM today and request re-review."

_Key Entities_: Rahul (person), PR four eight two (pr), 5 PM (date)

_Action Items_: Rahul: Address comments on PR four eight two (Due: 5 PM today)

### Turn 18: SPEAKER_2 (Senior SRE / DevOps Lead — Female — English)

> "Vikram will do the final approval on PR four eight two once the jitter test passes."

_Key Entities_: Vikram (person), PR four eight two (pr)

_Action Items_: Vikram: Final re-review of PR four eight two (Due: today)

### Turn 19: SPEAKER_1 (Engineering Manager — Female — English)

> "Both PRs are well tracked. Thank you for the rigorous code review everyone."

### Turn 20: SPEAKER_0 (Tech Lead / Staff Engineer — Male — English)

> "Meeting concluded. Let us keep our main branch green."
