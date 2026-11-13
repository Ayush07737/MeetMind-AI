# ADR 011 — Decision Genome Graph Mapping (Q8 Schemas Only)

**Status**: Accepted Schema Contract (Schemas, Fixtures & Validation Rules Built in Q-0d; Zero Neo4j Driver/Cypher Code)  
**Date**: 2026-11-12 (Stage Q-0d — Q7 & Q8)  
**Decision makers**: Engineering & Platform Architecture Team

---

## Context

Component #8 (Decision Genome on Neo4j Community Edition) persists a queryable lineage connecting what was said in engineering meetings, what factual claims and assumptions supported or challenged a decision, who approved any external action, what work artifacts (`Ticket`, `PullRequest`, `Deployment`, `Incident`, `Service`) executed the decision, and what measurable `Outcome` resulted (§22 Outcome Tracker).

Without a shared schema contract between the Quality Contracts (`meetmind_schemas.cards`, `meetmind_quality.policy`) and §8 Graph persistence, downstream workers would invent incompatible IDs or lose claim-to-evidence provenance during translation.

---

## Decision

### 1. Canonical Graph Schemas (`meetmind_schemas.graph`, Python & TypeScript)

All 13 node types and 10 relationship types are defined identically in `packages/shared-schemas/schemas_py/meetmind_schemas/graph.py` (Pydantic v2) and `packages/shared-schemas/src/graph.ts` (Zod), backed by shared JSON fixtures under `packages/shared-schemas/fixtures/graph/`:

- **13 Node Types** (discriminated on `node_type`):
  - **Reasoning & Evidence Core**: `Statement`, `Claim`, `Evidence`, `Assumption`, `Decision`, `Approval`, `Action`, `Outcome`
  - **Context, Work & Governance**: `Meeting`, `Participant`, `Entity`, `Commitment`, `RiskFlag`
- **5 Engineering `Entity` Subtypes** (`EntityNode.entity_subtype`): `Ticket`, `PullRequest`, `Deployment`, `Incident`, `Service` — enforcing ADR/Engineering Pack's **"No Source Code" storage invariant** (stores identifiers, titles, status, `as_of`, owner, and URIs; never code blobs).
- **10 Directed Relationship Types**: `SAID_IN`, `ASSERTS`, `SUPPORTED_BY`, `ASSUMES`, `LED_TO`, `APPROVED_BY`, `OWNED_BY`, `EXECUTED_AS`, `RESULTED_IN`, `REFERENCES`.

### 2. Zero-Translation Provenance Mapping

Three upstream systems write directly into `GraphDocument` payloads without minting surrogate IDs:

| Upstream Producer                        | Source Identifiers / Fields                                                                              | Target Graph Nodes & Edges                                                                                                                                                                                   | Preserved Provenance Invariants                                                                                                                                        |
| :--------------------------------------- | :------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ASR (#2) + Classifier (§6)**           | `utterance_id`, `speaker_id`, `meeting_id`, `tenant_id`, `ts_ms`                                         | `Statement` (`utterance_id`), `Participant`, `Meeting`, `(Statement)-[:SAID_IN]->(Meeting)`, `(Statement)-[:ASSERTS]->(Claim \| Assumption)`                                                                 | Exact `utterance_id` + `source_event_ids` trace every graph node back to an immutable Redis bus event                                                                  |
| **InsightCard (§7 / §11 / Q3)**          | `Claim.claim_id`, `Evidence.source_id`, `Evidence.span`, `as_of`, `restricted`, `visible_roles`          | `Claim` (`node_id = claim_id`), `Evidence` (`node_id = source_id`), `(Claim)-[:SUPPORTED_BY]->(Evidence)`                                                                                                    | `claim_id` and `evidence.source_id` are **reused verbatim** as graph `node_id`s; `restricted` & `visible_roles` propagate unchanged                                    |
| **Decision Synthesis & §13 Trust Gate**  | `policy_decision`, `audit_event_id`, `approver_id`, `required_approvers`, `expected_outcome`             | `Decision` (`status: proposed\|agreed\|disputed\|approved\|executed\|reviewed`), `Approval` (`decision`, `audit_event_id`), `(Decision)-[:APPROVED_BY]->(Approval)`, `(Decision)-[:OWNED_BY]->(Participant)` | Every state-changing `Decision` links via `LED_TO` from supporting `Claim`/`Assumption` nodes and captures explicit `Approval.audit_event_id` from Neon `audit_events` |
| **§14 Connectors & §22 Outcome Tracker** | `external_ref` (`PLAT-101`, `PR-482`, `deploy-2026.11.12`), metric baseline/target/actual, `review_date` | `Action` (`status`, `action_class`, `external_ref`), `Entity` (`Ticket`, `PullRequest`, `Deployment`, `Incident`, `Service`), `Outcome` (`status`, `actual_value`, `evaluated_at`)                           | `(Decision)-[:EXECUTED_AS]->(Action)`, `(Action)-[:REFERENCES]->(Entity)`, `(Entity \| Action)-[:RESULTED_IN]->(Outcome)` validates end-to-end engineering lifecycle   |

### 3. Multi-Tenancy Isolation for Neo4j Community Edition

Because Neo4j Community Edition does not support multi-database isolation:

- Every node carries required `tenant_id`, `created_at`, `source_event_ids` (`min_length=1`), and `restricted` (when `restricted=True`, `visible_roles` MUST be non-empty).
- All label generation routes through `tenant_node_label(tenant_id, node_type)` (exact counterpart of `TenantRouter.neo4j_namespace` in `backend/security/meetmind_security/tenant.py`), yielding `{tenant_id}_{NodeType}` (e.g., `tenant_acme_Decision`).
- `validate_graph_lifecycle()` rejects any cross-tenant edge, missing endpoint, or invalid engineering lifecycle linkage before persistence.

### 4. Boundary Discipline in Q-0d

Per Q-0 Q8 & Q-0d Section D, Q-0d delivers **only** the typed Python/TS schemas, shared fixtures, end-to-end lifecycle validator, and this ADR. Zero `neo4j` driver dependencies or Cypher query strings exist in the codebase until Component #8 begins.

---

## Consequences

- Component #8 MUST persist graphs using `meetmind_schemas.graph` and `TenantRouter.neo4j_namespace`, guaranteeing bi-directional traceability from any Neo4j `Outcome` or `Approval` back to the spoken `Statement` and cited `Evidence` span.
- Restricted cards (`restricted=True`) retain their `visible_roles` access control inside the Decision Genome, preventing cross-role leakage during pre-meeting brief queries (§10).
