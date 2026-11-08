"""MeetMind AI — Shared ASR Evaluation Normalizer (B4).

Applied identically to reference and hypothesis transcripts for fair, reproducible
evaluation across ASR candidates (Deepgram Nova-3, Flux, Sarvam Saaras v3, OpenAI).

Rules:
1. Lowercase all text.
2. Punctuation removal: strip commas, periods, quotes, exclamation marks, question marks,
   semicolons, colons, brackets, parentheses.
3. Currency & Fused Token Normalization:
   - "rs.", "rs", "inr", "rupees" -> "inr"
   - fused tokens: "rs150" -> "inr 150", "inr12" -> "inr 12", "$50m" -> "50 million usd"
   - "$", "usd", "dollars" -> "usd"
4. Spoken to Digits Number Normalization:
   - "zero".."nineteen", "twenty".."ninety" mapped to integer strings.
   - Compound words: "twenty five" -> "25".
   - Indian denominations: "lakh", "lakhs", "lac", "lacs" -> "lakh";
     "crore", "crores", "cr" -> "crore".
   - Western denominations: "thousand", "million", "billion" preserved or canonicalized.
   - Percentages: "percent", "pct", "%" -> "percent".
   - Basis points: "basis points", "bps" -> "bps".
5. Exact match extraction of canonical numbers for Numeric-Token Error Rate (NTER).
"""

from __future__ import annotations

import re
from typing import Any

# Word to number mapping
SMALL_NUMBERS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
}

TENS_NUMBERS = {
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}


def normalize_text(text: str) -> str:
    """Normalize text into canonical token sequence."""
    if not text:
        return ""

    t = text.lower()

    # 1. Expand currency abbreviations and fused currency tokens
    t = re.sub(r"\brs\.?\s*", "inr ", t)
    t = re.sub(r"\binr\s*", "inr ", t)
    t = re.sub(r"\brupees?\b", "inr", t)
    t = re.sub(r"\$", "usd ", t)
    t = re.sub(r"\bdollars?\b", "usd", t)

    # Handle fused prefix: e.g. inr12 -> inr 12, rs150 -> inr 150
    t = re.sub(r"\binr(\d+)", r"inr \1", t)
    t = re.sub(r"\brs(\d+)", r"inr \1", t)
    t = re.sub(r"\busd(\d+)", r"usd \1", t)

    # 2. Handle percentage and basis points
    t = re.sub(r"%", " percent", t)
    t = re.sub(r"\bpct\b", "percent", t)
    t = re.sub(r"\bbasis\s+points?\b", "bps", t)

    # 3. Strip punctuation
    t = re.sub(r"[\.,;:!\?\"'\(\)\[\]\{\}\\\/\-_~`@#\^&\*\+=]", " ", t)

    # 4. Canonicalize denomination spellings
    t = re.sub(r"\b(crores?|cr)\b", "crore", t)
    t = re.sub(r"\b(lakhs?|lacs?)\b", "lakh", t)
    t = re.sub(r"\bthousands?\b", "thousand", t)
    t = re.sub(r"\bmillions?\b", "million", t)
    t = re.sub(r"\bbillions?\b", "billion", t)

    # 5. Convert small spoken numbers (e.g. "twenty five" -> "25", "two crore" -> "2 crore")
    tokens = t.split()
    normalized_tokens: list[str] = []
    i = 0
    n = len(tokens)

    while i < n:
        tok = tokens[i]
        # Check tens + unit: e.g. "twenty", "five" -> "25"
        if tok in TENS_NUMBERS and i + 1 < n and tokens[i + 1] in SMALL_NUMBERS:
            val = TENS_NUMBERS[tok] + SMALL_NUMBERS[tokens[i + 1]]
            normalized_tokens.append(str(val))
            i += 2
        elif tok in TENS_NUMBERS:
            normalized_tokens.append(str(TENS_NUMBERS[tok]))
            i += 1
        elif tok in SMALL_NUMBERS:
            normalized_tokens.append(str(SMALL_NUMBERS[tok]))
            i += 1
        else:
            normalized_tokens.append(tok)
            i += 1

    return " ".join(normalized_tokens)


def extract_canonical_numbers(text: str) -> list[dict[str, Any]]:
    """Extract canonical numerical values with units from text for exact-match NTER."""
    norm = normalize_text(text)
    tokens = norm.split()
    numbers: list[dict[str, Any]] = []

    i = 0
    while i < len(tokens):
        tok = tokens[i]
        curr_unit = None

        # Check if preceded by currency
        if tok in ("inr", "usd"):
            curr_unit = tok.upper()
            if i + 1 < len(tokens):
                i += 1
                tok = tokens[i]
            else:
                i += 1
                continue

        # Check if tok is a number
        try:
            val = float(tok)
            unit = curr_unit or "COUNT"
            mult = 1.0

            # Look ahead for multiplier / unit
            if i + 1 < len(tokens):
                next_tok = tokens[i + 1]
                if next_tok == "crore":
                    mult = 10_000_000.0
                    i += 1
                elif next_tok == "lakh":
                    mult = 100_000.0
                    i += 1
                elif next_tok == "thousand":
                    mult = 1_000.0
                    i += 1
                elif next_tok == "million":
                    mult = 1_000_000.0
                    i += 1
                elif next_tok == "billion":
                    mult = 1_000_000_000.0
                    i += 1
                elif next_tok == "percent":
                    unit = "%"
                    i += 1
                elif next_tok == "bps":
                    unit = "BPS"
                    i += 1

            # Look ahead for trailing currency if not set
            if unit == "COUNT" and i + 1 < len(tokens) and tokens[i + 1] in ("inr", "usd"):
                unit = tokens[i + 1].upper()
                i += 1

            numbers.append(
                {
                    "raw_token": tok,
                    "canonical_value": val * mult,
                    "unit": unit,
                }
            )
        except ValueError:
            pass

        i += 1

    return numbers


def compute_normalized_wer(reference: str, hypothesis: str) -> float:
    """Compute WER between normalized reference and hypothesis."""
    norm_ref = normalize_text(reference)
    norm_hyp = normalize_text(hypothesis)

    ref_words = norm_ref.split()
    hyp_words = norm_hyp.split()

    if not ref_words:
        return 0.0 if not hyp_words else 1.0

    r_len = len(ref_words)
    h_len = len(hyp_words)

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
                    dp[i - 1][j - 1] + 1,  # substitution
                    dp[i - 1][j] + 1,  # deletion
                    dp[i][j - 1] + 1,  # insertion
                )

    return float(dp[r_len][h_len]) / float(r_len)


def compute_numeric_token_error_rate(reference: str, hypothesis: str) -> dict[str, Any]:
    """Compute exact-match numeric-token error rate between reference and hypothesis."""
    ref_nums = extract_canonical_numbers(reference)
    hyp_nums = extract_canonical_numbers(hypothesis)

    if not ref_nums:
        return {
            "nter": 0.0,
            "total_ref_numbers": 0,
            "correct": 0,
            "missing": 0,
            "extra": len(hyp_nums),
        }

    # Match based on (canonical_value, unit)
    matched = 0
    hyp_copy = list(hyp_nums)

    for r in ref_nums:
        r_val = r["canonical_value"]
        r_unit = r["unit"]
        found_idx = None
        for idx, h in enumerate(hyp_copy):
            # Check numerical tolerance (within 0.1% for float precision)
            val_diff = abs(h["canonical_value"] - r_val)
            val_match = (val_diff < 1e-4) if r_val == 0 else (val_diff / abs(r_val) < 0.001)
            unit_match = (h["unit"] == r_unit) or (r_unit == "COUNT" and h["unit"] != "%")
            if val_match and unit_match:
                found_idx = idx
                break
        if found_idx is not None:
            matched += 1
            hyp_copy.pop(found_idx)

    total_ref = len(ref_nums)
    err_rate = (total_ref - matched) / total_ref

    return {
        "nter": round(err_rate, 4),
        "total_ref_numbers": total_ref,
        "correct": matched,
        "missing": total_ref - matched,
        "extra": len(hyp_copy),
    }
