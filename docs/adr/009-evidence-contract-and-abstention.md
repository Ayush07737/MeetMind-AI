# ADR 009 — InsightCard Evidence Contract and Abstention-by-Default Policy

**Status**: Accepted (Schemas, Verifier & Policy Library Built & Tested in Q-0c; Runtime ASR `words` & `stable_prefix_words` Finished in Q-0d A2)  
**Date**: 2026-11-12 (Stage Q-0d; formalizing Q-0 Q3, Q4, Q5)  
**Decision makers**: Engineering & Platform Architecture Team

---

## Context

In engineering, architecture, and incident meetings, displaying a fabricated ticket status, wrong PR merge state, misheard latency/RPS metric (`"450 milliseconds"` vs `"4.5 seconds"`), or leaked credential spoken aloud causes immediate trust collapse. Generative summaries without verbatim span attribution also prevent automated precision/hallucination audits and force ad-hoc translation layers before persisting insights to the Decision Genome (§8).

---

## Decision

### 1. Canonical `InsightCard` Envelope (`meetmind_schemas.cards`, Python & TypeScript)

Every live intelligence item published on the outbound bus (`intelligence_event.payload.card`) conforms strictly to the shared Pydantic/Zod `InsightCard` schema:

- **Identity & Partial Revisions**: `card_id`, `revision >= 1`, `supersedes?`, `meeting_id`, `tenant_id`, `trace_id`.
  - Clients render ONLY the highest `revision` received per `card_id`. Lower or duplicate revisions are deterministic no-ops (enforced via `CardRevisionTracker` in Python & TS).
  - Tier 1 (`tier=1`, retrieval-only, `kind=fact`) publishes first within Lane 1; Tier 2 (`tier=2`, reasoned) revises the same `card_id` with an incremented `revision`.
- **Structural Grounding**: `kind` (`fact | suggestion | action_recommendation | alert`), `title`, plain structured `blocks[]` (HTML tags forbidden), `claims[]`, `confidence_band` (`high | medium | low` — **never a raw numeric percentage**), `confidence_basis` (`asr_confidence`, `retrieval_score`, `verifier_result`, `source_freshness`), `origin` (`internal | external | mixed`), `status` (`verified | inferred`), `restricted` (bool) + `visible_roles[]`, `timing`, `trigger`, `late`, and `abstain_reason?`.
- **Claim & Evidence Schema**: Every `Claim` carries a durable `claim_id`, `text`, `type` (`number | entity | date | assertion`), optional normalized `value`/`unit`, `status`, and `evidence[]` entries (`source_type`, `source_id`, `uri?`, `span`, `retrieved_at`, `as_of`). Notice that `claim_id` and `evidence[].source_id` are reused verbatim as Neo4j node IDs in ADR-011 (zero translation).

### 2. Fail-Closed Dual-Side Outbound Validation (`validate_outbound_card`)

Both the downstream publisher helper and the Ingestion Gateway outbound listener forwarder (`backend/gateway/meetmind_gateway/streams/redis_streams.py`) enforce `validate_outbound_card(card)` before sending any client WebSocket frame:

1. `status == "verified"` requires every claim to be `verified` with at least one non-empty `evidence` record.
2. `status == "inferred"` MUST carry an explicit visible label block (`[Inferred]` / `Inferred:` prefix) and is restricted to `kind in ("suggestion", "alert")` (never `fact` or `action_recommendation`).
3. Any card containing an unverified claim or failing schema validation is **dropped prior to WebSocket delivery** (counter incremented + bounded security audit event emitted); unverified inferences may be stored post-meeting only.
4. `restricted=True` cards are filtered per connected listener's role against `visible_roles[]` inside Gateway fan-out; mismatched listeners never receive the frame.

### 3. Deterministic `confidence_band()` Function

`confidence_band(components)` maps `(claims_verified, retrieval_score, source_freshness_ok, asr_confidence)` deterministically without LLM judgment:

- **`high`**: all claims verified AND `retrieval_score >= 0.80` AND `source_freshness_ok == True` AND quoted-word `asr_confidence >= 0.90`.
- **`medium`**: all claims verified AND `retrieval_score >= 0.60` AND `asr_confidence >= 0.75`, with **at most one** borderline component (`retrieval_score < 0.80`, `!source_freshness_ok`, or `asr_confidence < 0.90`).
- **`low`**: unverified claims or `>= 2` failed/borderline components.

### 4. Abstention-by-Default & Verification Library (`packages/quality`, `meetmind_quality`)

ABSTENTION IS THE DEFAULT whenever grounding is incomplete or ambiguous:

| Condition / Gate Trigger                                                                                | Policy Outcome (`publish_decision`)      | Engineering Rationale                                                                                 |
| :------------------------------------------------------------------------------------------------------ | :--------------------------------------- | :---------------------------------------------------------------------------------------------------- |
| **Missing evidence** (`no matching ticket/PR/ADR record`)                                               | `abstain` (`missing_evidence`)           | Prevents hallucinated issue statuses or nonexistent runbooks                                          |
| **Stale evidence** (`as_of` age `> max_age_hours`)                                                      | `abstain` (`stale_evidence`)             | Prevents stale deployment/status claims after newer merges                                            |
| **Conflicting evidence** (2+ authoritative sources disagree)                                            | `abstain` (`conflicting_evidence`)       | Surfaces ambiguity / stores for review instead of guessing                                            |
| **Low ASR confidence on quoted numbers/IDs** (`asr_uncertainty > 0.35`, i.e., `word.confidence < 0.65`) | `store_for_later` (`low_asr_confidence`) | Candidate card MUST be labelled `"heard as ..."` and CANNOT be marked `verified`                      |
| **Identifier confusion** (`PLAT-123` vs `PLAT-213`, `PR-482` vs `PR-428`)                               | Rejected by `verify_card()` -> `abstain` | Canonical case/hyphen-insensitive exact-ID verification blocks off-by-digit matches                   |
| **Numeric confusion** (`450 ms` vs `4.5 s`, `12 crore` vs `2 crore`)                                    | Rejected by `numbers.py` -> `abstain`    | Indian (`lakh`, `crore`, `₹`) + SI/engineering (`ms`, `s`, `rps`, `%`, `bps`) normalization tolerance |
| **Restricted / spoken secret** (`API keys`, `JWTs`, `AWS keys`, `MNPI`)                                 | `abstain` (`restricted_secret`)          | Never displayed on general UI feeds; scrubbed from live claims                                        |
| **Unresolved dissent** (`disagreement_signalled`)                                                       | `abstain` on consensus claims            | Preserves explicit team disagreement rather than hallucinating agreement                              |

### 5. Streaming ASR Prerequisites (Amendment A1 & Q5 Flow Contracts)

- **`words` on `transcript_final`** (`backend/asr/meetmind_asr/pipeline.py`, bounded list `[{w, start_ms, end_ms, confidence, speaker?}]`): Populated live from Deepgram and Stub provider responses in Q-0d A2 while keeping ASR service-added P95 overhead `< 25 ms`. Enables word-level ASR uncertainty checks on every quoted number or ticket ID.
- **`stable_prefix_words` on `transcript_interim`**: Counts unchanged leading tokenized words across consecutive interims so downstream Lane 0/Lane 1 speculative triggers (`trigger_candidate` → `trigger_confirmed`) only fire on stable prefixes (`docs/contracts/asr.md`).

---

## Consequences

- Any future §7–§12 reasoning pipeline can only surface live cards via `verify_card() -> publish_decision() -> validate_outbound_card()`.
- Golden evaluation metrics (`scripts/eval_quality.py`) require `displayed_unsupported_claim_rate == 0.0` on held-out cases before any reasoning pipeline graduates out of internal testing.
