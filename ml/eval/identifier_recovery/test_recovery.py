"""MeetMind AI — Identifier Recovery Unit Tests (Q-0e Section D)."""

from ml.eval.identifier_recovery.board import (
    BoardItem,
    build_simulated_sprint_board,
)
from ml.eval.identifier_recovery.matcher import IdentifierRecoveryEngine


def test_simulated_sprint_board_inventory():
    """Verify board contains exactly 200 tickets and 50 pull requests (250 total)."""
    board = build_simulated_sprint_board()
    assert len(board) == 250
    tickets = [it for it in board if it.item_type == "ticket"]
    prs = [it for it in board if it.item_type == "pull_request"]
    assert len(tickets) == 200
    assert len(prs) == 50
    all_ids = {it.identifier for it in board}
    assert len(all_ids) == 250
    assert "PLAT-101" in all_ids
    assert "PR-482" in all_ids
    assert "PLAT-123" in all_ids
    assert "PLAT-213" in all_ids
    assert "PR-428" in all_ids


def test_clean_and_phonetic_recovery():
    """Verify accurate recovery of spoken engineering terms with minor phonetic distortion."""
    engine = IdentifierRecoveryEngine(min_confidence_margin=1, max_allowed_distance=1)

    # Exact spoken match
    res = engine.recover("Vikram is working on plat one zero one today.")
    assert "PLAT-101" in res.recovered_text
    assert len(res.substitutions) == 1
    assert res.substitutions[0].matched_id == "PLAT-101"

    # Phonetic slip ('platte' instead of 'plat')
    res2 = engine.recover("what is your update on Platte 101?")
    assert "PLAT-101" in res2.recovered_text
    assert res2.substitutions[0].matched_id == "PLAT-101"

    # Phonetic slip ('to' instead of 'two')
    res3 = engine.recover("Review pr four eight to before deployment.")
    assert "PR-482" in res3.recovered_text
    assert res3.substitutions[0].matched_id == "PR-482"


def test_confusable_pairs_abstention():
    """Verify abstention behavior on confusable pairs with insufficient confidence margin.

    When both PLAT-123 and PLAT-213 exist on the board, an ambiguous/transposed input
    that is equidistant or separated by margin < min_margin MUST ABSTAIN, never substitute falsely.
    """
    # Construct test board with only confusable pairs
    confusable_board = [
        BoardItem("PLAT-123", "ticket", "Ticket 123", ("plat 123", "plat one two three")),
        BoardItem("PLAT-213", "ticket", "Ticket 213", ("plat 213", "plat two one three")),
        BoardItem("PR-482", "pull_request", "PR 482", ("pr 482", "pr four eight two")),
        BoardItem("PR-428", "pull_request", "PR 428", ("pr 428", "pr four two eight")),
    ]
    conf_engine = IdentifierRecoveryEngine(
        board=confusable_board, min_confidence_margin=2, max_allowed_distance=1
    )

    # Clean exact matches still succeed
    exact_123 = conf_engine.recover("Check plat one two three")
    assert "PLAT-123" in exact_123.recovered_text

    exact_213 = conf_engine.recover("Check plat two one three")
    assert "PLAT-213" in exact_213.recovered_text

    # Ambiguous input 'plat 113': dist 1 to PLAT-123 and dist 1 to PLAT-213 (margin=0) -> ABSTAIN
    ambig_res = conf_engine.recover("Check plat one one three")
    assert len(ambig_res.substitutions) == 0
    assert len(ambig_res.abstentions) == 1
    assert "ambiguous_confusable_margin_0" in ambig_res.abstentions[0].reason

    # Ambiguous input 'pr 418' has distance 1 from PR-428 and distance 1 from PR-482
    ambig_pr = conf_engine.recover("Look at pr four one eight")
    assert len(ambig_pr.substitutions) == 0
    assert len(ambig_pr.abstentions) == 1
    assert "ambiguous" in ambig_pr.abstentions[0].reason


def test_out_of_vocabulary_and_distance_threshold():
    """Verify that unknown identifiers abstain and never produce false substitutions."""
    engine = IdentifierRecoveryEngine(min_confidence_margin=2, max_allowed_distance=1)

    # Distance > 1 from any valid ticket
    res = engine.recover("Discussing plat nine nine nine nine")
    assert len(res.substitutions) == 0
    assert len(res.abstentions) == 1
    assert "exceeds_max" in res.abstentions[0].reason

    # Unknown prefix
    res2 = engine.recover("Looking at custom-9999")
    assert len(res2.substitutions) == 0
    assert len(res2.abstentions) == 0
    assert res2.recovered_text == "Looking at custom-9999"
