"""Unit tests for Word Error Rate (WER) computation."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on sys.path
REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ml.asr_eval.wer import compute_wer, wer_details  # noqa: E402


def test_wer_exact_match() -> None:
    ref = "Welcome everyone to the quarterly financial review."
    hyp = "Welcome everyone to the quarterly financial review."
    assert compute_wer(ref, hyp) == 0.0


def test_wer_normalization_case_and_punctuation() -> None:
    ref = "Hello, World! EBITDA is up 12%."
    hyp = "hello world ebitda is up 12"
    assert compute_wer(ref, hyp) == 0.0


def test_wer_substitution() -> None:
    ref = "the cat sat on the mat"
    hyp = "the dog sat on the mat"
    # 1 substitution out of 6 words -> 1/6
    assert abs(compute_wer(ref, hyp) - (1.0 / 6.0)) < 1e-4


def test_wer_deletion() -> None:
    ref = "the cat sat on the mat"
    hyp = "the cat sat"
    # 3 deletions out of 6 words -> 0.5
    assert compute_wer(ref, hyp) == 0.5


def test_wer_insertion() -> None:
    ref = "the cat sat"
    hyp = "the big black cat sat"
    # 2 insertions out of 3 words -> 2/3
    assert abs(compute_wer(ref, hyp) - (2.0 / 3.0)) < 1e-4


def test_wer_empty_strings() -> None:
    assert compute_wer("", "") == 0.0
    assert compute_wer("", "some hypothesis") == 1.0
    assert compute_wer("some reference", "") == 1.0


def test_wer_details_breakdown() -> None:
    ref = "Infosys announced twenty crore rupee dividend"
    hyp = "Infosys declared twenty crore dividend payout"
    details = wer_details(ref, hyp)
    assert details["ref_count"] == 6
    assert details["hyp_count"] == 6
    assert details["substitutions"] >= 1
    assert details["wer"] > 0.0
    assert "total_errors" in details
