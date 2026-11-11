"""MeetMind AI — InsightCard Quality Contract (Q3 / Q-0c).

Defines canonical Pydantic models, validation rules, confidence band computation,
and revision semantics for live InsightCards delivered across WebSocket and Redis streams.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SourceType = Literal[
    "internal",
    "external",
    "mixed",
    "ticket",
    "pull_request",
    "commit",
    "incident",
    "adr",
    "doc",
]

ClaimType = Literal[
    "number",
    "entity",
    "date",
    "assertion",
    "identifier",
    "state",
]

CardKind = Literal["fact", "suggestion", "action_recommendation", "alert"]
CardTier = Literal[1, 2, 3]
ConfidenceBand = Literal["high", "medium", "low"]
CardOrigin = Literal["internal", "external", "mixed"]
CardStatus = Literal["verified", "inferred"]


class Evidence(BaseModel):
    """Grounding evidence item supporting an individual claim."""

    model_config = ConfigDict(extra="forbid")

    source_type: SourceType
    source_id: str
    uri: str | None = None
    span: str
    retrieved_at: int | str
    as_of: int | str | None = None


class Claim(BaseModel):
    """Discrete factual claim asserted by an InsightCard."""

    model_config = ConfigDict(extra="forbid")

    claim_id: str
    text: str
    type: ClaimType
    value: str | float | None = None
    unit: str | None = None
    status: CardStatus
    evidence: list[Evidence] = Field(default_factory=list)


class TextBlock(BaseModel):
    """Structured text block (no HTML allowed)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["paragraph", "header", "bullet"]
    content: str


class CardTiming(BaseModel):
    """End-to-end timing timestamps in epoch milliseconds."""

    model_config = ConfigDict(extra="forbid")

    trigger_ts_ms: int
    published_ts_ms: int
    retrieval_ms: float | None = None
    reasoning_ms: float | None = None


class CardTrigger(BaseModel):
    """Contextual trigger that prompted the creation of this card."""

    model_config = ConfigDict(extra="forbid")

    utterance_id: str
    category: str
    trigger_confidence: float
    asr_confidence: float
    text_stability: float


class InsightCard(BaseModel):
    """InsightCard payload carried in intelligence_event (Q3)."""

    model_config = ConfigDict(extra="forbid")

    card_id: str
    revision: int = 1
    supersedes: str | None = None
    meeting_id: str
    tenant_id: str
    trace_id: str
    kind: CardKind
    tier: CardTier
    title: str
    blocks: list[TextBlock] = Field(default_factory=list)
    claims: list[Claim] = Field(default_factory=list)
    confidence_band: ConfidenceBand
    confidence_basis: dict[str, float] = Field(default_factory=dict)
    origin: CardOrigin
    status: CardStatus
    restricted: bool = False
    visible_roles: list[str] = Field(default_factory=list)
    timing: CardTiming
    trigger: CardTrigger
    late: bool = False
    abstain_reason: str | None = None
    inferred_label: str | None = None


def compute_confidence_band(components: dict[str, float]) -> ConfidenceBand:
    """Deterministic confidence band computation (Q3).

    HIGH requires:
    - retrieval_score >= 0.8
    - source_freshness >= 0.7
    - asr_confidence >= 0.9
    - verifier_result >= 1.0 (all claims verified)

    MEDIUM if exactly one component is borderline (0.6 <= score < threshold) and none fail outright.
    LOW otherwise.
    """
    retrieval = components.get("retrieval_score", 0.0)
    freshness = components.get("source_freshness", 0.0)
    asr = components.get("asr_confidence", 0.0)
    verifier = components.get("verifier_result", 0.0)

    # Check HIGH boundary
    if retrieval >= 0.8 and freshness >= 0.7 and asr >= 0.9 and verifier >= 1.0:
        return "high"

    # Check borderline scores
    borderline_count = 0
    severe_fail = False

    if retrieval >= 0.8:
        pass
    elif retrieval >= 0.6:
        borderline_count += 1
    else:
        severe_fail = True

    if freshness >= 0.7:
        pass
    elif freshness >= 0.5:
        borderline_count += 1
    else:
        severe_fail = True

    if asr >= 0.9:
        pass
    elif asr >= 0.75:
        borderline_count += 1
    else:
        severe_fail = True

    if verifier >= 1.0:
        pass
    elif verifier >= 0.7:
        borderline_count += 1
    else:
        severe_fail = True

    if not severe_fail and borderline_count == 1:
        return "medium"

    return "low"


def validate_outbound_card(card: InsightCard) -> tuple[bool, str | None]:
    """Validate card against outbound publishing invariants (Q3).

    Rules:
    (a) status='verified' requires every claim verified with at least one evidence entry.
    (b) 'inferred' cards must carry a visible label field and only kinds 'suggestion|alert'.
    (c) a card with any unverified claim is rejected for live display.
    (d) every card has source/origin, timestamp, band, and status.
    (e) restricted cards must have non-empty visible_roles.
    """
    # (d) Check core fields
    if not card.card_id or not card.timing.published_ts_ms or not card.confidence_band:
        return False, "Missing mandatory card envelope fields"

    # (e) Restricted checks
    if card.restricted and not card.visible_roles:
        return False, "Restricted card must specify non-empty visible_roles"

    # (a) Verified status checks
    if card.status == "verified":
        if not card.claims:
            return False, "Verified card must contain at least one claim"
        for claim in card.claims:
            if claim.status != "verified":
                return False, f"Verified card contains unverified claim '{claim.claim_id}'"
            if not claim.evidence:
                return False, f"Verified claim '{claim.claim_id}' has no supporting evidence"

    # (b) Inferred card checks
    if card.status == "inferred":
        if card.kind not in ("suggestion", "alert"):
            return False, f"Inferred card cannot have kind '{card.kind}' (only suggestion|alert)"
        if not card.inferred_label:
            return False, "Inferred card must carry visible inferred_label"

    # (c) Live display rejection of mixed/unverified claims on verified cards
    if card.status != "inferred":
        for claim in card.claims:
            if claim.status == "inferred":
                return False, f"Live display card rejected: claim '{claim.claim_id}' is unverified"

    return True, None
