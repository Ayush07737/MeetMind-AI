"""MeetMind AI — Word Error Rate (WER) Evaluation (Part 1).

Standard word error rate calculation using Levenshtein distance on normalized
word token sequences:
    WER = (Substitutions + Deletions + Insertions) / Total Reference Words

Normalization:
- Lowercase
- Punctuation stripped (except intra-word apostrophes/hyphens normalized to tokens)
- Whitespace normalized
- Number words left as-is
"""

from __future__ import annotations

import re
from typing import Any


def normalize_transcript(text: str) -> list[str]:
    """Normalize text into word tokens: lowercase, stripped punctuation, normalized whitespace."""
    if not text:
        return []
    # Replace non-alphanumeric characters with spaces
    cleaned = re.sub(r"[^\w\s]", " ", text.lower())
    # Split on whitespace to get tokens
    return cleaned.split()


def compute_wer(reference: str, hypothesis: str) -> float:
    """Compute standard Word Error Rate between reference and hypothesis strings."""
    ref_words = normalize_transcript(reference)
    hyp_words = normalize_transcript(hypothesis)

    if not ref_words:
        return 0.0 if not hyp_words else 1.0

    r_len = len(ref_words)
    h_len = len(hyp_words)

    # DP table: (r_len + 1) x (h_len + 1)
    dp = [[0] * (h_len + 1) for _ in range(r_len + 1)]

    for i in range(r_len + 1):
        dp[i][0] = i
    for j in range(h_len + 1):
        dp[0][j] = j

    for i in range(1, r_len + 1):
        for j in range(1, h_len + 1):
            if ref_words[i - 1] == hyp_words[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                substitution = dp[i - 1][j - 1] + 1
                deletion = dp[i - 1][j] + 1
                insertion = dp[i][j - 1] + 1
                dp[i][j] = min(substitution, deletion, insertion)

    return float(dp[r_len][h_len]) / float(r_len)


def wer_details(reference: str, hypothesis: str) -> dict[str, Any]:
    """Compute detailed breakdown of WER operations."""
    ref_words = normalize_transcript(reference)
    hyp_words = normalize_transcript(hypothesis)

    r_len = len(ref_words)
    h_len = len(hyp_words)

    if not ref_words:
        wer = 0.0 if not hyp_words else 1.0
        return {
            "wer": wer,
            "ref_count": 0,
            "hyp_count": len(hyp_words),
            "substitutions": 0,
            "deletions": 0,
            "insertions": len(hyp_words),
        }

    dp = [[0] * (h_len + 1) for _ in range(r_len + 1)]
    for i in range(r_len + 1):
        dp[i][0] = i
    for j in range(h_len + 1):
        dp[0][j] = j

    for i in range(1, r_len + 1):
        for j in range(1, h_len + 1):
            if ref_words[i - 1] == hyp_words[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = min(
                    dp[i - 1][j - 1] + 1,  # sub
                    dp[i - 1][j] + 1,  # del
                    dp[i][j - 1] + 1,  # ins
                )

    # Backtrace
    i, j = r_len, h_len
    subs = 0
    dels = 0
    ins = 0

    while i > 0 or j > 0:
        if i > 0 and j > 0 and ref_words[i - 1] == hyp_words[j - 1]:
            i -= 1
            j -= 1
        elif i > 0 and j > 0 and dp[i][j] == dp[i - 1][j - 1] + 1:
            subs += 1
            i -= 1
            j -= 1
        elif i > 0 and dp[i][j] == dp[i - 1][j] + 1:
            dels += 1
            i -= 1
        elif j > 0 and dp[i][j] == dp[i][j - 1] + 1:
            ins += 1
            j -= 1
        else:
            break

    total_errors = subs + dels + ins
    wer = total_errors / float(r_len)

    return {
        "wer": round(wer, 4),
        "ref_count": r_len,
        "hyp_count": h_len,
        "substitutions": subs,
        "deletions": dels,
        "insertions": ins,
        "total_errors": total_errors,
    }


calculate_wer = compute_wer
