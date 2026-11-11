# MeetMind AI — Spec Amendment: Engineering Domain Pack v0 (Design Only)

**Status**: Design Specification Only (Q-0c — No product features built in this milestone)  
**Owner Decision Date**: 2026-11-07  
**Supersedes**: Financial & Sales beachhead ordering (moved to `SPEC-ONLY` until traction gates in `docs/product/traction-gates.md` are met).

---

## 1. Domain Scope (Beachhead Vertical)

MeetMind AI prioritizes **Engineering & Development Teams** as the sole v1 domain pack, targeting 10 recurring workflows:

1. Daily engineering standup
2. Sprint planning with estimates
3. Architecture & ADR design review
4. Pull request & code review discussion
5. Production incident review (timeline, root cause, action items)
6. Release Go / No-Go gate
7. On-call handoff
8. Tech-debt prioritisation
9. API & schema contract discussion
10. Unresolved disagreement / explicit abstention gate

---

## 2. Engineering Taxonomy Additions (12 Types)

| Event / Claim Category   | Required Structured Attributes            | Grounding / Verification Policy                                  |
| :----------------------- | :---------------------------------------- | :--------------------------------------------------------------- |
| `decision_proposed`      | `proposal_text`, `proposer`               | Requires explicit speaker utterance + stable prefix              |
| `decision_agreed`        | `decision_text`, `approvers`, `as_of`     | Requires explicit consensus; abstains on unresolved conflict     |
| `action_item`            | `task`, `owner`, `due`                    | Must ground `owner` in meeting roster or emit `owner_unassigned` |
| `blocker`                | `blocker_summary`, `blocked_artifact`     | Linked to ticket/PR/service when cited                           |
| `risk`                   | `risk_summary`, `severity`, `mitigation?` | Marked `inferred` unless cited from postmortem/ADR               |
| `estimate`               | `value`, `unit` (points, days, hours, ms) | Passes through `meetmind_quality.numbers` normaliser             |
| `ticket_reference`       | `canonical_id` (e.g. `PLAT-101`)          | Exact-match verified against `ticket` evidence snapshot          |
| `pr_reference`           | `canonical_id` (e.g. `PR-482`)            | Exact-match verified against `pull_request` / `commit` snapshot  |
| `incident_reference`     | `canonical_id` (e.g. `INC-409`)           | Exact-match verified against `incident` evidence snapshot        |
| `deployment_reference`   | `environment`, `version`, `commit_sha?`   | Verified against deployment / release state (`as_of`)            |
| `disagreement_signalled` | `topic`, `positions`, `participants`      | Forces `ABSTAIN` on consensus claims; surfaces unresolved state  |
| `owner_unassigned`       | `task`, `reason`                          | Emitted whenever an agreed action item lacks a verified owner    |

---

## 3. Read-Only Evidence Sources & Verified API / Webhook Limits

All connectors are **read-only first** (`Zero Autonomous Mutation Invariant`: MeetMind AI never merges PRs, edits issues, or posts external mutations without explicit human approval). Access date for all official docs below: **2026-11-10**.

| Source               | Read-Only Artifacts                                                                         | Official API & Webhook Limits (Verified 2026-11-10)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| :------------------- | :------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **GitHub**           | PR titles/descriptions, review status, merge/CI checks, commit metadata, ADR Markdown paths | **REST Primary**: 5,000 req/hr per authenticated user/OAuth app (GitHub Enterprise / org-owned apps up to 15,000 req/hr; unauthenticated 60/hr). **Secondary**: max 100 concurrent reqs, 900 pts/min per endpoint, <= 90s CPU time/60s. **Webhooks**: payload capped at **25 MB** (oversized events dropped). Ref: [Rate limits for the REST API — GitHub Docs](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api) & [Webhook events and payloads — GitHub Docs](https://docs.github.com/en/webhooks/webhook-events-and-payloads) (accessed 2026-11-10). |
| **Jira Cloud**       | Issue keys (`PLAT-*`, `ENG-*`), title, status, assignee, priority, sprint metadata          | **Points-based rate limit**: Tier 1 global app pool + Tier 2 per-tenant pool (~500,000 pts/hr standard, HTTP 429 + `Retry-After`). **Webhooks**: max **100 active admin webhooks per site** and **100 per app/tenant**; best-effort delivery (not guaranteed; correlate via `X-Atlassian-Webhook-Trace`). Ref: [Rate limiting — Atlassian Developer](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/) & [Webhooks — Atlassian Developer](https://developer.atlassian.com/cloud/jira/platform/webhooks/) (accessed 2026-11-10).                                      |
| **Linear**           | Issues, cycles/sprints, labels, triage/assignee state                                       | **GraphQL Leaky Bucket**: token refill `LIMIT_AMOUNT / LIMIT_PERIOD` (exact numeric bucket tier per workspace plan: **UNVERIFIED** without active workspace query headers; pagination default 50 rows). **Webhooks**: HTTP POST JSON, **5 s** response timeout window; persistent failures auto-disable webhook. Ref: [Rate limiting — Linear Developers](https://linear.app/developers/rate-limiting) & [Webhooks — Linear Developers](https://linear.app/developers/webhooks) (accessed 2026-11-10).                                                                                 |
| **Slack**            | Selected engineering channels (read-only incident/deploy threads), user display names       | **Web API Tiers**: Tier 1 (~1 req/min), Tier 2 (~20/min, e.g. `conversations.history`), Tier 3 (~50/min, `conversations.list`), Tier 4 (~100/min); HTTP 429 + `Retry-After`. **Events API**: **30,000 events/workspace/app/60 min** (`app_rate_limited` beyond); **3 s** HTTP 2xx ack requirement; >= 95% failure/hr pauses subscription. Ref: [Rate Limits — Slack API](https://api.slack.com/apis/rate-limits) & [Events API — Slack API](https://api.slack.com/apis/events-api) (accessed 2026-11-10).                                                                              |
| **Confluence Cloud** | Engineering RFCs, runbooks, architecture design pages                                       | **Points-based REST/GraphQL limits** shared with Atlassian Forge/Connect/3LO tenant quotas (HTTP 429 + `Retry-After`). Webhook active count & per-event payload size ceiling in Confluence: **UNVERIFIED** on exact Confluence-specific docs (shares Atlassian Connect/Forge webhook infrastructure; assumed same 100/app/tenant subject to live verification). Ref: [Confluence Cloud Rate Limiting — Atlassian Developer](https://developer.atlassian.com/cloud/confluence/rate-limiting/) (accessed 2026-11-10).                                                                    |
| **Notion**           | Architecture docs, sprint specs, postmortem pages                                           | **API Rate Limits**: **180 req/min** (~3 req/s) standard plans, **600 req/min** (~10 req/s) Business/Enterprise per integration token + shared workspace cap; HTTP 429 + `Retry-After`; max **1,000 blocks/page** pagination ceiling. **Webhooks/Workers**: ingress **600 req/min** per worker / **1,200/min** per workspace; queued runs **600/hr** (burst 60/min). Exact public Integration Webhook retry backoff schedule outside Workers: **UNVERIFIED**. Ref: [Request limits — Notion Developers](https://developers.notion.com/reference/request-limits) (accessed 2026-11-10). |

---

## 4. "No Source Code" Indexing Policy & Engineering Consent Text

### 4.1 Strict "No Source Code" Storage Invariant

- MeetMind AI **MUST NOT** clone git repositories, fetch file blobs, chunk, embed, vector-index, or store proprietary source code (`.py`, `.ts`, `.go`, `.rs`, etc.) in Qdrant, Neo4j, Redis, or LLM context windows.
- Permitted ingestion fields are restricted strictly to **identifiers** (`PLAT-101`, `PR-482`, `sha[:12]`), **titles**, **status / state + `as_of`**, **owner / assignee**, and **canonical URLs / links**.

### 4.2 Default Consent Notice Text (Engineering Organizations)

> "MeetMind AI is joining this meeting in live audio-stream mode for **[Org Name]**. Raw audio is processed transiently in memory only (zero disk persistence, zero third-party model training opt-out enabled) to transcribe discussion and look up referenced Jira/Linear tickets, GitHub PR titles/statuses, incidents, and ADRs in read-only mode. No source code is read or stored. Continue speaking to confirm consent, or ask the host to pause MeetMind AI at any time."

---

## 5. Outcome Tracker Chain

MeetMind AI tracks engineering commitments across five linked verification states (read-only attribution):

```
decision (decision_agreed + owner) -> ticket (PLAT-*) -> PR (PR-*) -> deploy (env + version/sha) -> metric (p99 / error_rate / SLO)
```

---

## 6. Deferred Verticals (SPEC-ONLY — Zero Code Permitted)

**Financial Advisory Pack (`SPEC-ONLY`)**: Reserved future vertical for wealth-management client calls requiring SEBI/FINRA disclaimer rules, INR crore/lakh numeric verification, portfolio-holding freshness checks, and restricted compliance role visibility; zero connectors, prompts, or classifiers may be built until all traction gates (`docs/product/traction-gates.md`) are met.

**Enterprise Sales Pack (`SPEC-ONLY`)**: Reserved future vertical for B2B discovery and deal-review calls tracking MEDDPICC qualification signals, CRM opportunity stages, pricing commitments, and competitive battlecards; zero connectors, prompts, or classifiers may be built until all traction gates (`docs/product/traction-gates.md`) are met.
