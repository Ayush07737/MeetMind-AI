"""MeetMind AI — Identifier Recovery Candidate-Set Re-scorer Package."""

from ml.eval.identifier_recovery.board import (
    CONFUSABLE_TEST_PAIRS,
    CORPUS_TRUE_IDENTIFIERS,
    BoardItem,
    build_simulated_sprint_board,
)
from ml.eval.identifier_recovery.matcher import (
    Abstention,
    IdentifierRecoveryEngine,
    RecoveryResult,
    Substitution,
)

__all__ = [
    "BoardItem",
    "build_simulated_sprint_board",
    "CORPUS_TRUE_IDENTIFIERS",
    "CONFUSABLE_TEST_PAIRS",
    "IdentifierRecoveryEngine",
    "RecoveryResult",
    "Substitution",
    "Abstention",
]
