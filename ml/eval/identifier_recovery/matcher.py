"""MeetMind AI — Candidate-Set Identifier Re-scorer & Abstention Engine (Q-0e Section D).

Given an ASR hypothesis and a closed list of valid engineering identifiers (e.g. sprint board),
identifies spoken/noisy candidate spans, computes normalized spoken-form distance against
valid board items, enforces a confidence margin over the second-best candidate, and
strictly ABSTAINS on confusable pairs or sub-threshold matches.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field

from ml.eval.identifier_recovery.board import BoardItem, build_simulated_sprint_board


@dataclass(frozen=True)
class Substitution:
    raw_span: str
    matched_id: str
    distance: int
    margin: int
    confidence: float
    start_char: int
    end_char: int


@dataclass(frozen=True)
class Abstention:
    raw_span: str
    top_candidates: tuple[str, ...]
    best_distance: int
    margin: int
    reason: str
    start_char: int
    end_char: int


@dataclass(frozen=True)
class RecoveryResult:
    original_text: str
    recovered_text: str
    substitutions: list[Substitution] = field(default_factory=list)
    abstentions: list[Abstention] = field(default_factory=list)


# Words that phonetically sound like digits in common ASR errors
WORD_TO_DIGIT: dict[str, str] = {
    "zero": "0",
    "oh": "0",
    "one": "1",
    "won": "1",
    "two": "2",
    "to": "2",
    "too": "2",
    "three": "3",
    "tree": "3",
    "four": "4",
    "for": "4",
    "fore": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "ate": "8",
    "nine": "9",
}

# Supported prefix normalizations (handles common ASR phonetics)
PREFIX_MAP: dict[str, str] = {
    "plat": "PLAT",
    "platte": "PLAT",
    "plate": "PLAT",
    "core": "CORE",
    "front": "FRONT",
    "data": "DATA",
    "eng": "ENG",
    "infra": "INFRA",
    "inc": "INC",
    "tech": "TECH",
    "adr": "ADR",
    "pr": "PR",
    "p r": "PR",
    "peer": "PR",
}


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Standard Levenshtein distance."""
    if s1 == s2:
        return 0
    if len(s1) == 0:
        return len(s2)
    if len(s2) == 0:
        return len(s1)

    v0 = list(range(len(s2) + 1))
    v1 = [0] * (len(s2) + 1)

    for i in range(len(s1)):
        v1[0] = i + 1
        for j in range(len(s2)):
            cost = 0 if s1[i] == s2[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        v0 = list(v1)

    return v0[len(s2)]


def _normalize_digit_string(s: str) -> str:
    """Convert spoken words or digits to canonical digit sequence (e.g. 'one zero one' -> '101')."""
    tokens = s.lower().replace("-", " ").split()
    digits = []
    for tok in tokens:
        if tok in WORD_TO_DIGIT:
            digits.append(WORD_TO_DIGIT[tok])
        elif tok.isdigit():
            digits.append(tok)
    return "".join(digits)


class IdentifierRecoveryEngine:
    """Re-scores candidate spoken identifiers against a closed sprint board."""

    def __init__(
        self,
        board: Sequence[BoardItem] | None = None,
        min_confidence_margin: int = 2,
        max_allowed_distance: int = 1,
    ) -> None:
        self.board = list(board or build_simulated_sprint_board())
        self.min_confidence_margin = min_confidence_margin
        self.max_allowed_distance = max_allowed_distance
        self._build_index()

    def _build_index(self) -> None:
        """Index board items by prefix and canonical number for fast filtering."""
        self.prefix_index: dict[str, list[tuple[str, str, BoardItem]]] = {}
        for item in self.board:
            pfx, _, num = item.identifier.partition("-")
            pfx_norm = pfx.upper()
            if pfx_norm not in self.prefix_index:
                self.prefix_index[pfx_norm] = []
            self.prefix_index[pfx_norm].append((pfx_norm, num, item))

    def recover(self, hypothesis: str) -> RecoveryResult:
        """Scan hypothesis, match spoken identifier spans against board, and apply substitutions."""
        if not hypothesis:
            return RecoveryResult(hypothesis, hypothesis)

        # Regex to locate candidate identifier spans:
        # e.g. "plat 101", "platte 101", "plat one zero one", "pr 482", "pr four eight to", "PR482"
        pfx_pattern = "|".join(re.escape(k) for k in PREFIX_MAP)
        digit_words_pat = "|".join(re.escape(k) for k in WORD_TO_DIGIT)
        digit_word_re = rf"(?:\d+|{digit_words_pat})"
        span_regex = re.compile(
            rf"\b({pfx_pattern})[\s\-]+((?:{digit_word_re}[\s\-]*){{1,5}})\b",
            re.IGNORECASE,
        )

        substitutions: list[Substitution] = []
        abstentions: list[Abstention] = []
        applied_spans: list[tuple[int, int, str]] = []

        for match in span_regex.finditer(hypothesis):
            raw_pfx = match.group(1).lower()
            raw_num_part = match.group(2).strip()
            raw_span = match.group(0)
            start_c, end_c = match.start(), match.end()

            canonical_pfx = PREFIX_MAP.get(raw_pfx)
            if not canonical_pfx or canonical_pfx not in self.prefix_index:
                continue

            extracted_digits = _normalize_digit_string(raw_num_part)
            if not extracted_digits:
                continue

            # Score all candidates with the same prefix
            candidates = self.prefix_index[canonical_pfx]
            scored: list[tuple[int, str]] = []
            for _, cand_num, item in candidates:
                d = _levenshtein_distance(extracted_digits, cand_num)
                scored.append((d, item.identifier))

            scored.sort(key=lambda x: x[0])
            best_dist, best_id = scored[0]
            second_dist, second_id = scored[1] if len(scored) > 1 else (999, "NONE")
            margin = second_dist - best_dist

            # Exact match: instant accept (margin=999 if unique)
            repl_id = f"{best_id} " if raw_span.endswith(" ") else best_id
            if best_dist == 0:
                confidence = 1.0
                substitutions.append(
                    Substitution(
                        raw_span=raw_span,
                        matched_id=best_id,
                        distance=0,
                        margin=margin,
                        confidence=confidence,
                        start_char=start_c,
                        end_char=end_c,
                    )
                )
                applied_spans.append((start_c, end_c, repl_id))
                continue

            # Distance threshold check: abstain if distance too large
            if best_dist > self.max_allowed_distance:
                abstentions.append(
                    Abstention(
                        raw_span=raw_span,
                        top_candidates=(best_id, second_id),
                        best_distance=best_dist,
                        margin=margin,
                        reason=f"distance_{best_dist}_exceeds_max_{self.max_allowed_distance}",
                        start_char=start_c,
                        end_char=end_c,
                    )
                )
                continue

            # Confidence margin check: abstain on confusable pairs
            # (e.g. if second best is also very close, margin < min_confidence_margin)
            if margin < self.min_confidence_margin:
                abstentions.append(
                    Abstention(
                        raw_span=raw_span,
                        top_candidates=(best_id, second_id),
                        best_distance=best_dist,
                        margin=margin,
                        reason=f"ambiguous_confusable_margin_{margin}_below_required_{self.min_confidence_margin}",
                        start_char=start_c,
                        end_char=end_c,
                    )
                )
                continue

            # Clear confident win: accept substitution
            conf = max(0.0, 1.0 - (best_dist / float(max(len(extracted_digits), 1))))
            repl_id = f"{best_id} " if raw_span.endswith(" ") else best_id
            substitutions.append(
                Substitution(
                    raw_span=raw_span,
                    matched_id=best_id,
                    distance=best_dist,
                    margin=margin,
                    confidence=round(conf, 3),
                    start_char=start_c,
                    end_char=end_c,
                )
            )
            applied_spans.append((start_c, end_c, repl_id))

        # Reconstruct text with substitutions applied from right to left
        recovered = hypothesis
        for s_idx, e_idx, repl in sorted(applied_spans, key=lambda x: x[0], reverse=True):
            recovered = recovered[:s_idx] + repl + recovered[e_idx:]

        return RecoveryResult(
            original_text=hypothesis,
            recovered_text=recovered,
            substitutions=substitutions,
            abstentions=abstentions,
        )
