# MeetMind AI — Product Traction Gates (Engineering-First Sequencing)

**Effective Date**: 2026-11-07  
**Decision**: Engineering & Development Pack v0 is the sole implementation priority. Financial and Sales domain packs remain specification-only until every gate below is achieved and signed off by the owner. (Not agent build tasks.)

---

## Gate Prerequisites Before Vertical Expansion

| Gate ID  | Metric / Milestone                            | Quantitative Threshold                                                                 | Verification Source                     |
| :------- | :-------------------------------------------- | :------------------------------------------------------------------------------------- | :-------------------------------------- |
| **TG-1** | **Active Engineering Teams & Weekly Cadence** | >= 10 engineering teams, each completing >= 3 meetings/week for >= 4 consecutive weeks | Tenant session & meeting ledger         |
| **TG-2** | **Card Engagement (Open / Accept Rate)**      | >= 60% of surfaced InsightCards opened or accepted                                     | UI card interaction telemetry           |
| **TG-3** | **Held-Out Grounding Safety**                 | Displayed-unsupported-claim rate == 0 on held-out evaluation set                       | Automated `meetmind_quality` eval suite |
| **TG-4** | **Decision Logging with Ownership**           | >= 30 decisions logged with verified explicit owners                                   | Engineering outcome tracker ledger      |
| **TG-5** | **Decision-to-Artifact Traceability**         | >= 20% of logged decisions linked to a canonical ticket or pull request                | Outcome tracker linkage audit           |
| **TG-6** | **Commercial Willingness-to-Pay Validation**  | >= 3 teams answering a structured paid-price test                                      | Owner commercial validation notes       |

---

## Sequencing Policy

1. Until **TG-1 through TG-6** are simultaneously satisfied, **zero product code** (connectors, classifiers, entity extractors, or UI views) may be written for Financial Services or Enterprise Sales.
2. Financial and Sales requirements remain isolated in design specifications only (`SPEC-ONLY`).
3. Any deviation requires explicit written owner sign-off after verifying the gates above.
