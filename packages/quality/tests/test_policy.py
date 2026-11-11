"""Unit tests for meetmind_quality.policy."""

from meetmind_quality.policy import publish_decision
from meetmind_schemas.cards import (
    CardTiming,
    CardTrigger,
    Claim,
    Evidence,
    InsightCard,
    TextBlock,
)


def _make_test_card(
    claims: list[Claim],
    confidence_band: str = "high",
    category: str = "latency_spike",
    late: bool = False,
    abstain_reason: str | None = None,
) -> InsightCard:
    return InsightCard(
        card_id="c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c",
        revision=1,
        meeting_id="meeting_eng_01",
        tenant_id="tenant_eng",
        trace_id="trace_eng_12345",
        kind="fact",
        tier=1,
        title="Incident Review",
        blocks=[TextBlock(type="paragraph", content="Incident review for INC-892.")],
        claims=claims,
        confidence_band=confidence_band,
        confidence_basis={"retrieval_score": 0.95},
        origin="internal",
        status="verified",
        timing=CardTiming(
            trigger_ts_ms=1000,
            published_ts_ms=1500,
            retrieval_ms=120.0,
            reasoning_ms=25.0,
        ),
        trigger=CardTrigger(
            utterance_id="utt_01",
            category=category,
            trigger_confidence=0.95,
            asr_confidence=0.96,
            text_stability=1.0,
        ),
        late=late,
        abstain_reason=abstain_reason,
    )


def test_publish_decision_success():
    ev = Evidence(
        source_type="internal",
        source_id="doc-1",
        retrieved_at=95_000,
        span="INC-892 caused outage",
    )
    claim = Claim(
        claim_id="cl-1",
        text="INC-892 caused outage",
        type="identifier",
        value="INC-892",
        status="verified",
        evidence=[ev],
    )
    card = _make_test_card([claim])
    verification = {"verified": True, "unverified_claims": []}

    ok, dec, reason = publish_decision(
        card=card,
        verification=verification,
        asr_uncertainty=0.1,
        max_evidence_age_ms=300_000,
        current_ts_ms=100_000,
    )
    assert ok is True
    assert dec == "PUBLISH"
    assert reason is None


def test_publish_decision_abstain_unverified():
    ev = Evidence(
        source_type="internal",
        source_id="doc-1",
        retrieved_at=95_000,
        span="INC-892 caused outage",
    )
    claim = Claim(
        claim_id="cl-1",
        text="INC-892 caused outage",
        type="identifier",
        value="INC-892",
        status="verified",
        evidence=[ev],
    )
    card = _make_test_card([claim])
    verification = {"verified": False, "unverified_claims": ["cl-1"]}

    ok, dec, reason = publish_decision(
        card=card,
        verification=verification,
        asr_uncertainty=0.1,
        current_ts_ms=100_000,
    )
    assert ok is False
    assert dec == "ABSTAIN"
    assert "Card verification failed" in reason


def test_publish_decision_abstain_stale_evidence():
    ev = Evidence(
        source_type="internal",
        source_id="doc-1",
        retrieved_at=10_000,  # 90,000 ms ago
        span="INC-892 caused outage",
    )
    claim = Claim(
        claim_id="cl-1",
        text="INC-892 caused outage",
        type="identifier",
        value="INC-892",
        status="verified",
        evidence=[ev],
    )
    card = _make_test_card([claim])
    verification = {"verified": True, "unverified_claims": []}

    # Stale if max age is 60,000 ms
    ok, dec, reason = publish_decision(
        card=card,
        verification=verification,
        asr_uncertainty=0.1,
        max_evidence_age_ms=60_000,
        current_ts_ms=100_000,
    )
    assert ok is False
    assert dec == "ABSTAIN"
    assert "stale" in reason


def test_publish_decision_abstain_high_asr_uncertainty():
    ev = Evidence(
        source_type="internal",
        source_id="doc-1",
        retrieved_at=95_000,
        span="PLAT-101 is blocking",
    )
    claim = Claim(
        claim_id="cl-1",
        text="PLAT-101 is blocking",
        type="identifier",
        value="PLAT-101",
        status="verified",
        evidence=[ev],
    )
    card = _make_test_card([claim])
    verification = {"verified": True, "unverified_claims": []}

    # High uncertainty > 0.35
    ok, dec, reason = publish_decision(
        card=card,
        verification=verification,
        asr_uncertainty=0.45,
        current_ts_ms=100_000,
    )
    assert ok is False
    assert dec == "ABSTAIN"
    assert "ASR uncertainty" in reason
