"""MeetMind AI — Quality Grounding & Claim Verification Library (Q4).

Verifies that discrete claims made by InsightCards are fully supported by
cited evidence spans, ensuring zero hallucinated numbers or identifiers.
"""

from __future__ import annotations

from typing import Any

from meetmind_schemas.cards import Claim, InsightCard

from meetmind_quality.numbers import (
    normalize_number_expression,
    normalize_spoken_identifier,
)


def verify_claim(claim: Claim, evidence_store: dict[str, str]) -> tuple[bool, str | None]:
    """Verify an individual claim against its cited evidence spans."""
    if not claim.evidence:
        return False, "No supporting evidence cited"

    for ev in claim.evidence:
        span_text = ev.span
        # If source_id is in store, ensure cited span exists within stored document
        if ev.source_id in evidence_store:
            stored_doc = evidence_store[ev.source_id]
            span_norm = normalize_number_expression(span_text)
            doc_norm = normalize_number_expression(stored_doc)
            if span_text not in stored_doc and span_norm not in doc_norm:
                return False, f"Cited span not found in source document '{ev.source_id}'"

        # Check claim grounding according to type
        norm_span = normalize_number_expression(span_text).lower()

        if claim.type == "identifier":
            # Must have exact canonical identifier match in span
            canon_id = normalize_spoken_identifier(claim.value or claim.text)
            norm_span_id = normalize_spoken_identifier(span_text)
            if canon_id not in norm_span_id:
                return False, f"Identifier '{canon_id}' not grounded in evidence span"

        elif claim.type == "number":
            # Number must be explicitly grounded in span
            if claim.value is not None:
                val_str = str(claim.value)
                int_str = (
                    str(int(claim.value))
                    if isinstance(claim.value, float) and claim.value.is_integer()
                    else val_str
                )
                if (
                    val_str not in norm_span
                    and int_str not in norm_span
                    and normalize_number_expression(claim.text) not in norm_span
                ):
                    return False, f"Numeric claim value '{claim.value}' not found in evidence span"

        elif claim.type == "entity":
            ent = str(claim.value or claim.text)
            if ent.lower() not in norm_span:
                return False, f"Entity '{ent}' not grounded in evidence span"

        elif claim.type == "state":
            if ev.as_of is None:
                return False, f"State claim '{claim.claim_id}' requires evidence as_of snapshot"
            expected_state = str(claim.value or claim.text).lower()
            if expected_state not in norm_span and expected_state not in span_text.lower():
                return False, f"State value '{expected_state}' not grounded in snapshot span"

    return True, None


def verify_card(card: InsightCard, evidence_store: dict[str, str] | None = None) -> dict[str, Any]:
    """Verify all claims of an InsightCard against the evidence store (Q4)."""
    store = evidence_store or {}
    claim_results: dict[str, dict[str, Any]] = {}
    unverified: list[str] = []

    for claim in card.claims:
        ok, reason = verify_claim(claim, store)
        claim_results[claim.claim_id] = {"verified": ok, "reason": reason}
        if not ok:
            unverified.append(claim.claim_id)

    all_ok = len(unverified) == 0 and len(card.claims) > 0

    return {
        "verified": all_ok,
        "claims_verified_count": len(card.claims) - len(unverified),
        "total_claims": len(card.claims),
        "claim_results": claim_results,
        "unverified_claims": unverified,
    }
