"""MeetMind AI — Quality & Number Normalization Library (Q4).

Handles normalisation of Indian and international numbering, spoken identifiers,
units, versions, and detects phonetic/ASR acoustic confusions.
"""

from __future__ import annotations

import re

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

DIGIT_WORDS = {
    "zero": "0",
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
}

ID_PREFIXES = ["plat", "eng", "front", "infra", "data", "core", "tech", "sec", "inc", "adr", "pr"]


def normalize_spoken_identifier(text: str) -> str:
    """Normalize spoken engineering identifiers into canonical uppercase form (e.g. PLAT-101)."""
    t = text.lower()
    prefix_choices = "|".join(ID_PREFIXES)
    num_words = r"(?:(?:zero|one|two|three|four|five|six|seven|eight|nine|\d+)\s*)+"
    prefix_pat = rf"\b({prefix_choices})\s+({num_words})"

    def _repl(m: re.Match) -> str:
        pfx = m.group(1).upper()
        tokens = m.group(2).strip().split()
        digits = []
        for tok in tokens:
            if tok in DIGIT_WORDS:
                digits.append(DIGIT_WORDS[tok])
            elif tok.isdigit():
                digits.append(tok)
        return f"{pfx}-{''.join(digits)}"

    t = re.sub(prefix_pat, _repl, t)
    t = re.sub(
        rf"\b({prefix_choices})\s*-\s*(\d+)\b",
        lambda m: f"{m.group(1).upper()}-{m.group(2)}",
        t,
    )
    t = re.sub(
        rf"\b({prefix_choices})\s+(\d+)\b",
        lambda m: f"{m.group(1).upper()}-{m.group(2)}",
        t,
    )
    return t


def extract_engineering_identifiers(text: str) -> list[str]:
    """Extract canonical engineering identifiers (e.g. ['PLAT-101', 'PR-482', 'INC-409'])."""
    norm = normalize_spoken_identifier(text)
    prefix_choices = "|".join(ID_PREFIXES)
    matches = re.findall(rf"\b({prefix_choices})-(\d+)\b", norm, flags=re.IGNORECASE)
    return [f"{pfx.upper()}-{num}" for pfx, num in matches]


def normalize_number_expression(text: str) -> str:
    """Normalize Indian and Western denominations, currencies, and spoken numbers."""
    t = text.lower()

    # Currency normalization
    t = re.sub(r"\brs\.?\s*", "inr ", t)
    t = re.sub(r"\brupees?\b", "inr", t)
    t = re.sub(r"\$", "usd ", t)
    t = re.sub(r"\bdollars?\b", "usd", t)

    # Metrics
    t = re.sub(r"\bmilli\s*seconds?\b", "ms", t)
    t = re.sub(r"\brequests?\s+per\s+second\b", "rps", t)
    t = re.sub(r"\bmegabytes?\b", "mb", t)
    t = re.sub(r"\bgigabytes?\b", "gb", t)

    # Denominations
    t = re.sub(r"\b(crores?|cr)\b", "crore", t)
    t = re.sub(r"\b(lakhs?|lacs?)\b", "lakh", t)
    t = re.sub(r"%", " percent", t)
    t = re.sub(r"\bbasis\s+points?\b", "bps", t)

    # Spoken compounds (e.g. twelve crore, two crore, twenty five)
    tokens = t.split()
    norm_tokens = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok in TENS_NUMBERS and i + 1 < n and tokens[i + 1] in SMALL_NUMBERS:
            norm_tokens.append(str(TENS_NUMBERS[tok] + SMALL_NUMBERS[tokens[i + 1]]))
            i += 2
        elif tok in TENS_NUMBERS:
            norm_tokens.append(str(TENS_NUMBERS[tok]))
            i += 1
        elif tok in SMALL_NUMBERS:
            norm_tokens.append(str(SMALL_NUMBERS[tok]))
            i += 1
        else:
            norm_tokens.append(tok)
            i += 1

    return " ".join(norm_tokens)


def parse_numeric_value(text: str) -> tuple[float | None, str | None]:
    """Parse numerical float value and unit from expression."""
    norm = normalize_number_expression(text)
    tokens = norm.split()
    unit = None
    mult = 1.0

    val = None
    for i, tok in enumerate(tokens):
        try:
            val = float(tok)
            # Check trailing tokens
            if i + 1 < len(tokens):
                nxt = tokens[i + 1]
                if nxt == "crore":
                    mult = 10_000_000.0
                    if i + 2 < len(tokens) and tokens[i + 2] in ("inr", "usd"):
                        unit = tokens[i + 2].upper()
                    else:
                        unit = "INR"
                elif nxt == "lakh":
                    mult = 100_000.0
                    unit = "INR"
                elif nxt == "percent":
                    unit = "%"
                elif nxt == "bps":
                    unit = "BPS"
                elif nxt in ("ms", "rps", "mb", "gb"):
                    unit = nxt
                elif nxt in ("inr", "usd"):
                    unit = nxt.upper()
            break
        except ValueError:
            continue

    if val is not None:
        return val * mult, unit or "COUNT"
    return None, None


def check_asr_confusion(ref_val: str, hyp_val: str) -> bool:
    """Detect known high-risk acoustic confusions between reference and hypothesis.

    Examples:
    - '2 crore' vs '12 crore'
    - 'PLAT-123' vs 'PLAT-213' (transposed digits)
    - 'PR-482' vs 'PR-428'
    """
    ref_norm = normalize_spoken_identifier(normalize_number_expression(ref_val)).strip()
    hyp_norm = normalize_spoken_identifier(normalize_number_expression(hyp_val)).strip()

    if ref_norm == hyp_norm:
        return False  # Exact match, not a confusion

    # Check numeric confusion: two vs twelve, fifteen vs fifty, 1.5 vs 15 crore
    if ("2 crore" in ref_norm and "12 crore" in hyp_norm) or (
        "12 crore" in ref_norm and "2 crore" in hyp_norm
    ):
        return True
    if ("1.5 crore" in ref_norm and "15 crore" in hyp_norm) or (
        "15 crore" in ref_norm and "1.5 crore" in hyp_norm
    ):
        return True
    if ("15" in ref_norm and "50" in hyp_norm) or ("50" in ref_norm and "15" in hyp_norm):
        return True

    # Check identifier digit transposition or single-digit substitution
    ref_m = re.match(r"^([A-Z]+)-(\d+)$", ref_norm)
    hyp_m = re.match(r"^([A-Z]+)-(\d+)$", hyp_norm)
    return bool(
        ref_m and hyp_m and ref_m.group(1) == hyp_m.group(1) and ref_m.group(2) != hyp_m.group(2)
    )
