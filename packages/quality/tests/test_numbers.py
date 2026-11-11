"""Unit tests for meetmind_quality.numbers."""

from meetmind_quality.numbers import (
    check_asr_confusion,
    extract_engineering_identifiers,
    normalize_number_expression,
    normalize_spoken_identifier,
)


def test_normalize_spoken_identifier():
    assert normalize_spoken_identifier("plat one zero one") == "PLAT-101"
    assert normalize_spoken_identifier("plat 101") == "PLAT-101"
    assert normalize_spoken_identifier("pr four eight two") == "PR-482"
    assert normalize_spoken_identifier("eng four five two one") == "ENG-4521"
    assert normalize_spoken_identifier("infra 20") == "INFRA-20"


def test_normalize_number_expression():
    # Indian numbering & currency
    assert "1.5 crore" in normalize_number_expression("1.5 crore")
    assert "25 lakh" in normalize_number_expression("25 lakh")
    assert "5 lakh inr" in normalize_number_expression("5 lakh rupees")
    assert "50 thousand usd" in normalize_number_expression("fifty thousand dollars")

    # Units
    assert "450 ms" in normalize_number_expression("450 milliseconds")
    assert "2500 rps" in normalize_number_expression("2500 requests per second")
    assert "16 gb" in normalize_number_expression("16 gigabytes")


def test_extract_engineering_identifiers():
    text = "We need to fix PLAT-101 and merge PR-482 before ENG-4521 is deployed."
    ids = extract_engineering_identifiers(text)
    assert ids == ["PLAT-101", "PR-482", "ENG-4521"]


def test_check_asr_confusion():
    # 50 vs 15 confusion
    assert check_asr_confusion("50 ms", "15 ms") is True
    # 1.5 crore vs 15 crore confusion
    assert check_asr_confusion("1.5 crore", "15 crore") is True
    # 12 vs 2 crore
    assert check_asr_confusion("12 crore", "2 crore") is True
    # Identifier digit confusion
    assert check_asr_confusion("PLAT-101", "PLAT-102") is True
    # Unrelated strings
    assert check_asr_confusion("Kafka cluster", "Kafka cluster") is False
