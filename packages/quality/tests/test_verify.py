"""Unit tests for meetmind_quality.verify."""

from meetmind_quality.verify import verify_card, verify_claim
from meetmind_schemas.cards import (
    CardTiming,
    CardTrigger,
    Claim,
    Evidence,
    InsightCard,
    TextBlock,
)


def _make_sample_card(claims: list[Claim]) -> InsightCard:
    return InsightCard(
        card_id="c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c",
        revision=1,
        meeting_id="meeting_eng_01",
        tenant_id="tenant_eng",
        trace_id="trace_eng_12345",
        kind="fact",
        tier=1,
        title="Architecture Discussion",
        blocks=[TextBlock(type="paragraph", content="We are discussing ADR-042.")],
        claims=claims,
        confidence_band="high",
        origin="internal",
        status="verified",
        timing=CardTiming(
            trigger_ts_ms=1000,
            published_ts_ms=1500,
            retrieval_ms=100.0,
            reasoning_ms=20.0,
        ),
        trigger=CardTrigger(
            utterance_id="utt_01",
            category="architecture",
            trigger_confidence=0.95,
            asr_confidence=0.96,
            text_stability=1.0,
        ),
    )


def test_verify_claim_numeric_grounded():
    ev = Evidence(
        source_type="internal",
        source_id="transcript_1",
        retrieved_at=1000,
        span="p95 latency is 450 ms under load",
    )
    claim = Claim(
        claim_id="cl-1",
        text="p95 latency is 450 ms",
        type="number",
        value=450.0,
        status="verified",
        evidence=[ev],
    )
    ok, err = verify_claim(
        claim, evidence_store={"transcript_1": "p95 latency is 450 ms under load"}
    )
    assert ok is True
    assert err is None


def test_verify_claim_numeric_unsupported():
    ev = Evidence(
        source_type="internal",
        source_id="transcript_1",
        retrieved_at=1000,
        span="p95 latency is 450 ms under load",
    )
    claim = Claim(
        claim_id="cl-1",
        text="p95 latency is 850 ms",
        type="number",
        value=850.0,
        status="verified",
        evidence=[ev],
    )
    ok, err = verify_claim(
        claim, evidence_store={"transcript_1": "p95 latency is 450 ms under load"}
    )
    assert ok is False
    assert "not found in evidence span" in err


def test_verify_claim_identifier_grounded():
    ev = Evidence(
        source_type="internal",
        source_id="transcript_1",
        retrieved_at=1000,
        span="we are tracking this in PLAT-101",
    )
    claim = Claim(
        claim_id="cl-1",
        text="Ticket PLAT-101 is blocking release",
        type="identifier",
        value="PLAT-101",
        status="verified",
        evidence=[ev],
    )
    ok, err = verify_claim(
        claim, evidence_store={"transcript_1": "we are tracking this in PLAT-101"}
    )
    assert ok is True
    assert err is None


def test_verify_card_all_claims():
    ev = Evidence(
        source_type="internal",
        source_id="doc_1",
        retrieved_at=1000,
        span="ADR-042 mandates Redis Streams for pubsub",
    )
    claim = Claim(
        claim_id="cl-1",
        text="ADR-042 mandates Redis",
        type="identifier",
        value="ADR-042",
        status="verified",
        evidence=[ev],
    )
    card = _make_sample_card([claim])
    res = verify_card(card, evidence_store={"doc_1": "ADR-042 mandates Redis Streams for pubsub"})
    assert res["verified"] is True
    assert res["claims_verified_count"] == 1
    assert len(res["unverified_claims"]) == 0


def test_verify_claim_state_requires_as_of_and_match():
    ev_with_snapshot = Evidence(
        source_type="pull_request",
        source_id="PR-482",
        retrieved_at=1000,
        as_of="2026-11-10T10:00:00Z",
        span="PR-482 status is merged, assignee Rahul",
    )
    claim_ok = Claim(
        claim_id="cl-state-1",
        text="PR-482 is merged",
        type="state",
        value="merged",
        status="verified",
        evidence=[ev_with_snapshot],
    )
    ok, err = verify_claim(
        claim_ok,
        evidence_store={"PR-482": "PR-482 status is merged, assignee Rahul"},
    )
    assert ok is True
    assert err is None

    ev_no_snapshot = Evidence(
        source_type="pull_request",
        source_id="PR-482",
        retrieved_at=1000,
        as_of=None,
        span="PR-482 status is merged",
    )
    claim_missing_as_of = Claim(
        claim_id="cl-state-2",
        text="PR-482 is merged",
        type="state",
        value="merged",
        status="verified",
        evidence=[ev_no_snapshot],
    )
    ok2, err2 = verify_claim(claim_missing_as_of, evidence_store={})
    assert ok2 is False
    assert "as_of" in (err2 or "").lower()

    claim_wrong_state = Claim(
        claim_id="cl-state-3",
        text="PR-482 is open",
        type="state",
        value="open",
        status="verified",
        evidence=[ev_with_snapshot],
    )
    ok3, err3 = verify_claim(claim_wrong_state, evidence_store={})
    assert ok3 is False
    assert "state" in (err3 or "").lower()
