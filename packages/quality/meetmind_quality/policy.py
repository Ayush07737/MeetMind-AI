"""MeetMind AI — Quality Publication Policy & Abstention Library (Q4).

Implements strict publication decisions for InsightCards.
The platform invariant: a late or wrong insight is worse than none.
The default decision is ALWAYS ABSTAIN unless all verification and freshness gates pass.
"""

from __future__ import annotations

import time
from typing import Literal

from meetmind_schemas.cards import InsightCard

PublicationDecision = Literal["PUBLISH", "ABSTAIN"]

DEFAULT_MAX_EVIDENCE_AGE_MS = 300_000  # 5 minutes
DEFAULT_MAX_ASR_UNCERTAINTY = 0.35


def publish_decision(
    card: InsightCard,
    verification: dict,
    asr_uncertainty: float = 0.0,
    max_evidence_age_ms: int = DEFAULT_MAX_EVIDENCE_AGE_MS,
    current_ts_ms: int | None = None,
) -> tuple[bool, PublicationDecision, str | None]:
    """Determine whether an InsightCard is permitted to be published to users.

    Returns:
        (should_publish, decision, reason_if_abstained)

    Rules:
    1. Default is ABSTAIN.
    2. Card must not be flagged late or have pre-existing abstain_reason.
    3. Verification must be completely successful (all claims verified against cited spans).
    4. Card must contain at least one claim and one evidence item.
    5. No cited evidence can be older than max_evidence_age_ms (staleness gate).
    6. If card has numeric or identifier claims, asr_uncertainty must not exceed threshold.
    7. Confidence band 'low' must abstain unless explicit user request trigger.
    """
    now_ms = current_ts_ms if current_ts_ms is not None else int(time.time() * 1000)

    # Late or pre-existing abstain check
    if card.late:
        return False, "ABSTAIN", "Card marked late against latency budget"
    if card.abstain_reason:
        return False, "ABSTAIN", f"Card has pre-existing abstain reason: '{card.abstain_reason}'"

    # Verification check
    if not verification.get("verified", False):
        unverified = verification.get("unverified_claims", [])
        return (
            False,
            "ABSTAIN",
            f"Card verification failed: {len(unverified)} unverified claims ({unverified})",
        )

    # Empty card check
    if not card.claims:
        return False, "ABSTAIN", "Card contains no claims"

    # Staleness check
    has_evidence = False
    for claim in card.claims:
        if not claim.evidence:
            return False, "ABSTAIN", f"Claim '{claim.claim_id}' has no supporting evidence"
        for ev in claim.evidence:
            has_evidence = True
            if isinstance(ev.retrieved_at, int):
                retrieved_ms = ev.retrieved_at
            else:
                try:
                    retrieved_ms = int(ev.retrieved_at)
                except ValueError:
                    retrieved_ms = now_ms
            age_ms = now_ms - retrieved_ms
            if age_ms < 0:
                age_ms = 0
            if age_ms > max_evidence_age_ms:
                return (
                    False,
                    "ABSTAIN",
                    f"Evidence '{ev.source_id}' is stale ({age_ms} ms > {max_evidence_age_ms} ms)",
                )

    if not has_evidence:
        return False, "ABSTAIN", "No evidence found on card claims"

    # ASR uncertainty check for numeric/identifier claims
    has_sensitive_claims = any(c.type in ("number", "identifier") for c in card.claims)
    if has_sensitive_claims and asr_uncertainty > DEFAULT_MAX_ASR_UNCERTAINTY:
        return (
            False,
            "ABSTAIN",
            (
                f"ASR uncertainty {asr_uncertainty:.3f} exceeds threshold "
                f"{DEFAULT_MAX_ASR_UNCERTAINTY:.3f} for numeric/identifier claims"
            ),
        )

    # Confidence band gate
    if card.confidence_band == "low" and card.trigger.category != "explicit_user":
        return (
            False,
            "ABSTAIN",
            "Confidence band is 'low' without explicit user trigger",
        )

    # All gates passed -> PUBLISH
    return True, "PUBLISH", None
