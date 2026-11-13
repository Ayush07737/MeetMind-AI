"""MeetMind AI — Shared ASR Evaluation Normalizer (A1.1 / B4).

Applied identically to reference and hypothesis transcripts for fair, reproducible
evaluation across ASR candidates (Deepgram Nova-3, Flux, Gladia, AssemblyAI, Sarvam, OpenAI).

Rules:
1. Script Detection & Common-Script Transliteration:
   - Detect Latin vs Devanagari vs Mixed script per hypothesis (`detect_script`).
   - Transliterate Devanagari spans to Romanized Latin using `indic-transliteration`
     (ITRANS + documented casual Hinglish orthographic cleanup; MIT License) so Hinglish
     WER is never inflated by script mismatch.
   - Expose English-tokens-only WER for Hinglish (`compute_hinglish_english_wer`) alongside
     common-script Hinglish WER (`compute_hinglish_translit_wer`).
2. Lowercase all text & remove punctuation (preserving version dots and identifier hyphens).
3. Currency & Fused Token Normalization:
   - "rs.", "rs", "inr", "rupees" -> "inr"
   - fused tokens: "rs150" -> "inr 150", "inr12" -> "inr 12"
   - "$", "usd", "dollars" -> "usd"
4. Spoken to Digits Number Normalization & Canonical Numeric/Identifier Extraction (NTER/IEMR).
"""

from __future__ import annotations

import re
from typing import Any

from indic_transliteration import sanscript

TRANSLIT_LIBRARY = "indic-transliteration"
TRANSLIT_LICENSE = "MIT"
TRANSLIT_SCHEME = "sanscript.DEVANAGARI -> sanscript.ITRANS + lowercase Hinglish cleanup"

# Closed-set conversational Hindi words used in Engineering Corpus v2 Hinglish segments.
# Used by `filter_english_tokens` so English-only Hinglish WER isolates technical/numeric/
# English recognition quality independently of Hindi transliteration conventions.
HINGLISH_HINDI_STOPWORDS: frozenset[str] = frozenset(
    {
        "aa",
        "aate",
        "aur",
        "baad",
        "baar",
        "badhta",
        "bar",
        "bhai",
        "bhej",
        "bhi",
        "bina",
        "dekhna",
        "dena",
        "dikha",
        "din",
        "diya",
        "diye",
        "do",
        "dono",
        "dusra",
        "ek",
        "gaya",
        "gaye",
        "ghante",
        "hai",
        "hain",
        "ho",
        "hoon",
        "humne",
        "isliye",
        "jaa",
        "kar",
        "karke",
        "karte",
        "karti",
        "ke",
        "ki",
        "kiya",
        "ko",
        "konsa",
        "kya",
        "lag",
        "lekin",
        "lena",
        "liye",
        "lo",
        "lungi",
        "main",
        "me",
        "mein",
        "na",
        "ne",
        "padega",
        "pe",
        "raha",
        "rahe",
        "rahi",
        "rehte",
        "sabhi",
        "sakta",
        "se",
        "sirf",
        "tha",
        "the",
        "zyaada",
        "zyada",
        "bhejna",
        "check_hi",
        "chuka",
        "chuki",
        "ka",
        "karna",
        "karni",
        "karo",
        "kia",
        "kuch",
        "par",
        "phir",
        "toh",
        "wala",
        "wali",
        "yeh",
        "ye",
    }
)

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

ALL_NUMBERS = {
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
    "ten": "10",
    "eleven": "11",
    "twelve": "12",
    "thirteen": "13",
    "fourteen": "14",
    "fifteen": "15",
    "sixteen": "16",
    "seventeen": "17",
    "eighteen": "18",
    "nineteen": "19",
    "twenty": "20",
    "thirty": "30",
    "forty": "40",
    "fifty": "50",
    "sixty": "60",
    "seventy": "70",
    "eighty": "80",
    "ninety": "90",
}

NUM_WORDS_PAT = "|".join(ALL_NUMBERS.keys())
ID_PREFIXES = ["plat", "eng", "front", "infra", "data", "core", "tech", "sec", "inc", "adr", "pr"]
ID_PREFIX_PAT = "|".join(ID_PREFIXES)

_DEVA_SPAN_RE = re.compile(r"[\u0900-\u097F]+")


def detect_script(text: str) -> str:
    """Classify hypothesis script using Unicode Devanagari vs Latin codepoints.

    Returns one of: ``'Empty'``, ``'Latin'``, ``'Devanagari'``, ``'Mixed'``, ``'Other'``.
    """
    if not text or not text.strip():
        return "Empty"
    n_deva = 0
    n_latin = 0
    n_alpha_other = 0
    for ch in text:
        if "\u0900" <= ch <= "\u097f":
            n_deva += 1
        elif "a" <= ch.lower() <= "z":
            n_latin += 1
        elif ch.isalpha():
            n_alpha_other += 1
    if n_deva > 0 and n_latin > 0:
        return "Mixed"
    if n_deva > 0:
        return "Devanagari"
    if n_latin > 0:
        return "Latin"
    if n_alpha_other > 0:
        return "Other"
    return "Other"


def transliterate_devanagari_to_latin(text: str) -> str:
    """Transliterate Devanagari spans to Romanized Latin using indic-transliteration (MIT).

    Leaves existing Latin/ASCII characters, numbers, and symbols intact while converting
    Devanagari spans (`[\\u0900-\\u097F]+`) via `sanscript.ITRANS`, followed by documented
    lowercase Hinglish normalization (candra-o `ॅ`/`ॉ` -> `o`, anusvara `M` -> `n`,
    geminate/schwa tail trim, and `|` danda -> space).
    """
    if not text:
        return ""
    if not _DEVA_SPAN_RE.search(text):
        return text

    # Pre-normalize Devanagari candra-o/e and nukta forms common in transliterated English loanwords
    cleaned = text.replace("\u0949", "\u094b").replace("\u0945", "\u0947").replace("\u0964", " ")

    def _conv(m: re.Match[str]) -> str:
        raw_deva = m.group(0)
        it = sanscript.transliterate(raw_deva, sanscript.DEVANAGARI, sanscript.ITRANS)
        # Map ITRANS ASCII diacritics while preserving vowel-case distinction for schwa trimming
        it = it.replace("~N", "n").replace("N^", "n").replace("Ch", "ch").replace("sh", "sh")
        it = it.replace("Sh", "sh").replace(".n", "n").replace(".N", "n").replace("M", "n")
        it = it.replace("R^i", "ri").replace("R^I", "ri")
        # Trim unwritten final inherent schwa (lowercase `-a`, NOT long vowel `-A`)
        if len(it) > 2 and it.endswith("a") and it[-2].lower() not in "aeiou":
            it = it[:-1]
        return it.lower()

    return _DEVA_SPAN_RE.sub(_conv, cleaned)


def filter_english_tokens(text: str, transliterate: bool = True) -> list[str]:
    """Extract normalized tokens excluding closed-set Hinglish Hindi glue words."""
    src = transliterate_devanagari_to_latin(text) if transliterate else text
    norm = normalize_text(src)
    return [tok for tok in norm.split() if tok not in HINGLISH_HINDI_STOPWORDS]


def normalize_engineering_terms(text: str) -> str:
    """Normalize spoken engineering identifiers, metrics, and versions."""
    t = text.lower()

    # 1. Percentiles
    t = re.sub(r"\bp\s+ninety\s+nine\b", "p99", t)
    t = re.sub(r"\bp\s+ninety\s+five\b", "p95", t)
    t = re.sub(r"\bp\s+fifty\b", "p50", t)

    # 2. Units and metrics
    t = re.sub(r"\bmilli\s*seconds?\b", "ms", t)
    t = re.sub(r"\bmillis\b", "ms", t)
    t = re.sub(r"\brequests?\s+per\s+second\b", "rps", t)
    t = re.sub(r"\bmegabytes?\b", "mb", t)
    t = re.sub(r"\bgigabytes?\b", "gb", t)

    # 3. Versions (e.g. python three point thirteen -> python 3.13)
    ver_regex = (
        r"\b(python|postgresql|postgres|version|v)\s+"
        r"((?:(?:" + NUM_WORDS_PAT + r"|\d+)\s+)?point\s+(?:" + NUM_WORDS_PAT + r"|\d+)"
        r"(?:\s+point\s+(?:" + NUM_WORDS_PAT + r"|\d+))?)\b"
    )

    def _repl_ver(m: re.Match) -> str:
        pfx = m.group(1)
        body = m.group(2).strip().split()
        res = []
        for w in body:
            if w == "point":
                res.append(".")
            elif w in ALL_NUMBERS:
                res.append(ALL_NUMBERS[w])
            elif w.isdigit():
                res.append(w)
            else:
                res.append(w)
        return f"{pfx} " + "".join(res)

    t = re.sub(ver_regex, _repl_ver, t)

    # 4. Spoken identifiers (e.g. plat one zero one -> plat-101, pr four eight two -> pr-482)
    id_regex = r"\b(" + ID_PREFIX_PAT + r")\s+((?:(?:" + NUM_WORDS_PAT + r"|\d+)\s*)+)"

    def _repl_id(m: re.Match) -> str:
        pfx = m.group(1)
        raw_seq = m.group(2).strip().split()
        digits = []
        for w in raw_seq:
            if w in ALL_NUMBERS:
                digits.append(ALL_NUMBERS[w])
            elif w.isdigit():
                digits.append(w)
        return f"{pfx}-" + "".join(digits) + " "

    t = re.sub(id_regex, _repl_id, t)
    t = re.sub(r"\b(" + ID_PREFIX_PAT + r")\s+(\d+)\b", r"\1-\2", t)
    return t


def normalize_text(text: str, transliterate_devanagari: bool = True) -> str:
    """Normalize text into canonical token sequence."""
    if not text:
        return ""

    # 0. Transliterate Devanagari spans if enabled, then normalize engineering terms & versions
    src = transliterate_devanagari_to_latin(text) if transliterate_devanagari else text
    t = normalize_engineering_terms(src)

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

    # 3. Strip punctuation, preserving hyphens inside identifiers and periods in numbers/versions
    t = re.sub(r"(?<=\d),(?=\d)", "", t)
    t = re.sub(r"(?<!\d)\.|\.(?!\d)", " ", t)
    t = re.sub(r"[,;:!\?\"'\(\)\[\]\{\}\\\/_~`@#\^&\*\+=]", " ", t)
    # Strip standalone hyphens not joining words/digits
    t = re.sub(r"(?<![a-zA-Z0-9])-(?![a-zA-Z0-9])", " ", t)

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


def _word_error_rate(ref_words: list[str], hyp_words: list[str]) -> float:
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


def compute_normalized_wer(
    reference: str, hypothesis: str, transliterate_devanagari: bool = True
) -> float:
    """Compute WER between normalized reference and hypothesis.

    When ``transliterate_devanagari`` is True (default), Devanagari characters in either
    string are transliterated to Romanized Latin before normalization so code-switched
    hypotheses are not penalized purely by script choice.
    """
    norm_ref = normalize_text(reference, transliterate_devanagari=transliterate_devanagari)
    norm_hyp = normalize_text(hypothesis, transliterate_devanagari=transliterate_devanagari)
    return _word_error_rate(norm_ref.split(), norm_hyp.split())


def compute_hinglish_english_wer(reference: str, hypothesis: str) -> float:
    """Compute WER restricted to non-Hindi (English + numeric + ID) tokens on Hinglish turns."""
    ref_eng = filter_english_tokens(reference, transliterate=True)
    hyp_eng = filter_english_tokens(hypothesis, transliterate=True)
    return _word_error_rate(ref_eng, hyp_eng)


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


def extract_identifiers(text: str) -> list[str]:
    """Extract canonical identifiers (e.g. ['PLAT-101', 'PR-482', 'INC-409'])."""
    norm = normalize_text(text)
    matches = re.findall(
        r"\b(" + ID_PREFIX_PAT + r")-(\d+)\b",
        norm,
    )
    return [f"{pfx.upper()}-{num}" for pfx, num in matches]


def compute_identifier_exact_match(reference: str, hypothesis: str) -> dict[str, Any]:
    """Compute exact match rate and error rate for engineering identifiers."""
    ref_ids = extract_identifiers(reference)
    hyp_ids = extract_identifiers(hypothesis)

    if not ref_ids:
        return {
            "iemr": 0.0,
            "total_ref": 0,
            "correct": 0,
            "missing": 0,
            "extra": len(hyp_ids),
        }

    hyp_copy = list(hyp_ids)
    matched = 0

    for r in ref_ids:
        if r in hyp_copy:
            matched += 1
            hyp_copy.remove(r)

    total_ref = len(ref_ids)
    err_rate = (total_ref - matched) / total_ref

    return {
        "iemr": round(err_rate, 4),
        "match_rate": round(matched / total_ref, 4) if total_ref > 0 else 1.0,
        "total_ref": total_ref,
        "correct": matched,
        "missing": total_ref - matched,
        "extra": len(hyp_copy),
    }


def compute_entity_error_rate(
    reference: str, hypothesis: str, expected_entities: list[dict[str, Any]] | list[str]
) -> dict[str, Any]:
    """Compute error rate for domain named entities."""
    ent_names: list[str] = []
    for e in expected_entities:
        if isinstance(e, dict):
            ent_names.append(e.get("text", "").lower())
        elif isinstance(e, str):
            ent_names.append(e.lower())
    ent_names = [e for e in ent_names if e]

    if not ent_names:
        return {"entity_error_rate": 0.0, "total_entities": 0, "correct": 0, "missing": 0}

    norm_hyp = normalize_text(hypothesis)
    matched = 0
    for ent in ent_names:
        norm_ent = normalize_text(ent)
        if norm_ent and norm_ent in norm_hyp:
            matched += 1

    total = len(ent_names)
    err_rate = (total - matched) / total

    return {
        "entity_error_rate": round(err_rate, 4),
        "total_entities": total,
        "correct": matched,
        "missing": total - matched,
    }
