"""Unit tests for InsightCard Quality Contract (Q3 / Q-0c)."""

from __future__ import annotations

import json
from pathlib import Path

from meetmind_schemas.cards import (
    InsightCard,
    compute_confidence_band,
    validate_outbound_card,
)

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "cards"


def test_valid_card_fixture():
    data = json.loads((FIXTURES_DIR / "valid_card.json").read_text(encoding="utf-8"))
    card = InsightCard.model_validate(data)
    assert card.card_id == "c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
    assert card.confidence_band == "high"
    assert card.claims[0].type == "identifier"
    assert card.claims[0].evidence[0].source_type == "pull_request"

    is_valid, reason = validate_outbound_card(card)
    assert is_valid is True
    assert reason is None


def test_invalid_unverified_claim():
    data = json.loads((FIXTURES_DIR / "invalid_unverified_claim.json").read_text(encoding="utf-8"))
    card = InsightCard.model_validate(data)
    is_valid, reason = validate_outbound_card(card)
    assert is_valid is False
    assert "unverified" in (reason or "").lower()


def test_invalid_missing_evidence():
    data = json.loads((FIXTURES_DIR / "invalid_missing_evidence.json").read_text(encoding="utf-8"))
    card = InsightCard.model_validate(data)
    is_valid, reason = validate_outbound_card(card)
    assert is_valid is False
    assert "evidence" in (reason or "").lower()


def test_invalid_inferred_bad_kind():
    data = json.loads((FIXTURES_DIR / "invalid_inferred_bad_kind.json").read_text(encoding="utf-8"))
    card = InsightCard.model_validate(data)
    is_valid, reason = validate_outbound_card(card)
    assert is_valid is False
    assert "kind" in (reason or "").lower()


def test_invalid_restricted_no_roles():
    data = json.loads(
        (FIXTURES_DIR / "invalid_restricted_no_roles.json").read_text(encoding="utf-8")
    )
    card = InsightCard.model_validate(data)
    is_valid, reason = validate_outbound_card(card)
    assert is_valid is False
    assert "visible_roles" in (reason or "").lower()


def test_confidence_band_computation():
    # HIGH
    high_comps = {
        "retrieval_score": 0.85,
        "source_freshness": 0.75,
        "asr_confidence": 0.95,
        "verifier_result": 1.0,
    }
    assert compute_confidence_band(high_comps) == "high"

    # MEDIUM (borderline retrieval score)
    med_comps = {
        "retrieval_score": 0.65,
        "source_freshness": 0.75,
        "asr_confidence": 0.95,
        "verifier_result": 1.0,
    }
    assert compute_confidence_band(med_comps) == "medium"

    # LOW (multiple borderline or severe fail)
    low_comps = {
        "retrieval_score": 0.40,
        "source_freshness": 0.75,
        "asr_confidence": 0.95,
        "verifier_result": 1.0,
    }
    assert compute_confidence_band(low_comps) == "low"


def test_revision_semantics_store():
    # Simulate a client or cache rendering highest revision
    store: dict[str, InsightCard] = {}

    def apply_card(c: InsightCard) -> bool:
        existing = store.get(c.card_id)
        if existing and existing.revision >= c.revision:
            return False  # Discard lower or duplicate
        store[c.card_id] = c
        return True

    data = json.loads((FIXTURES_DIR / "valid_card.json").read_text(encoding="utf-8"))
    c_rev1 = InsightCard.model_validate(data)
    c_rev2 = InsightCard.model_validate({**data, "revision": 2, "tier": 2})

    assert apply_card(c_rev1) is True
    assert store[c_rev1.card_id].revision == 1

    # Apply rev 2
    assert apply_card(c_rev2) is True
    assert store[c_rev1.card_id].revision == 2

    # Duplicate rev 2
    assert apply_card(c_rev2) is False

    # Out of order delivery: rev 1 arrives after rev 2
    assert apply_card(c_rev1) is False
    assert store[c_rev1.card_id].revision == 2


def test_a1_word_timing_and_stable_prefix():
    from meetmind_schemas.events import (
        TranscriptFinalPayload,
        TranscriptInterimPayload,
        WordTiming,
    )

    final_p = TranscriptFinalPayload(
        utterance_id="0123456789abcdef01234567",
        speaker_id="spk_0",
        channel=0,
        text="Merge PR-482 today",
        start_ms=1000,
        end_ms=2200,
        start_epoch_ms=1793000001000,
        confidence=0.97,
        language="en-IN",
        provider="deepgram",
        model="nova-3",
        words=[
            WordTiming(w="Merge", start_ms=1000, end_ms=1250, confidence=0.98, speaker=0),
            WordTiming(w="PR-482", start_ms=1300, end_ms=1800, confidence=0.96, speaker=0),
            WordTiming(w="today", start_ms=1850, end_ms=2200, confidence=0.97, speaker=0),
        ],
    )
    assert final_p.words is not None
    assert len(final_p.words) == 3
    assert final_p.words[1].w == "PR-482"

    interim_p = TranscriptInterimPayload(
        speaker_id="spk_0",
        channel=0,
        text="Merge PR-482",
        start_ms=1000,
        end_ms=1800,
        start_epoch_ms=1793000001000,
        confidence=0.91,
        language="en-IN",
        provider="deepgram",
        model="nova-3",
        is_stable=True,
        stable_prefix_words=2,
    )
    assert interim_p.stable_prefix_words == 2
