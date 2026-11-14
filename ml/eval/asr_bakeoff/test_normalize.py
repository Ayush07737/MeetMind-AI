"""Unit tests for shared ASR evaluation normalizer (B4)."""

from ml.eval.asr_bakeoff.normalize import (
    compute_entity_error_rate,
    compute_hinglish_english_wer,
    compute_identifier_exact_match,
    compute_normalized_wer,
    compute_numeric_token_error_rate,
    detect_script,
    extract_canonical_numbers,
    extract_identifiers,
    normalize_text,
    transliterate_devanagari_to_latin,
)


def test_lowercase_and_punctuation():
    text = "Hello, World! This is a test: meeting #42 (approved)."
    norm = normalize_text(text)
    assert norm == "hello world this is a test meeting 42 approved"


def test_currency_and_fused_tokens():
    text = (
        "The ARR is INR 12 crore, whereas Rs. 150 crore was projected. Inr12 and rs150 are valid."
    )
    norm = normalize_text(text)
    assert "inr 12 crore" in norm
    assert "inr 150 crore" in norm


def test_indian_denominations_and_numbers():
    text = "We raised two crore fifty lakh rupees."
    norm = normalize_text(text)
    assert norm == "we raised 2 crore 50 lakh inr"


def test_percentages_and_basis_points():
    text = "EBITDA margin grew by 15% and yield expanded by 25 basis points."
    norm = normalize_text(text)
    assert "15 percent" in norm
    assert "25 bps" in norm


def test_canonical_number_extraction():
    text = "Revenue reached INR 12 crore with a 15% EBITDA margin and Rs. 50 lakh net profit."
    nums = extract_canonical_numbers(text)
    assert len(nums) == 3
    # 12 crore = 120,000,000 INR
    assert nums[0]["canonical_value"] == 120_000_000.0
    assert nums[0]["unit"] == "INR"
    # 15% = 15 %
    assert nums[1]["canonical_value"] == 15.0
    assert nums[1]["unit"] == "%"
    # 50 lakh = 5,000,000 INR
    assert nums[2]["canonical_value"] == 5_000_000.0
    assert nums[2]["unit"] == "INR"


def test_normalized_wer():
    ref = "We invested INR 12 crore into ZetaGrid Technologies."
    hyp = "We invested 12 crore into ZetaGrid Technologies."
    # With normalizer, "INR 12 crore" vs "12 crore" handles common phrasing
    wer = compute_normalized_wer(ref, hyp)
    assert wer <= 0.15


def test_nter_perfect_match():
    ref = "The company reported INR 12 crore and 50 lakh profit with 18% margin."
    hyp = "The company reported 12 crore INR and 50 lakh inr profit with 18 percent margin."
    res = compute_numeric_token_error_rate(ref, hyp)
    assert res["nter"] == 0.0
    assert res["missing"] == 0
    assert res["correct"] == 3


def test_nter_mismatch():
    ref = "The valuation is Rs. 150 crore."
    hyp = "The valuation is Rs. 15 crore."  # Mismatch (150 crore vs 15 crore)
    res = compute_numeric_token_error_rate(ref, hyp)
    assert res["nter"] == 1.0
    assert res["correct"] == 0
    assert res["missing"] == 1


def test_spoken_identifiers():
    text = "We are working on PLAT one zero one and opened PR four eight two for review."
    norm = normalize_text(text)
    assert "plat-101" in norm
    assert "pr-482" in norm

    ids = extract_identifiers(text)
    assert "PLAT-101" in ids
    assert "PR-482" in ids


def test_metrics_and_units():
    text = (
        "p ninety nine latency was 450 milliseconds at 2500 requests per second with 500 megabytes."
    )
    norm = normalize_text(text)
    assert "p99 latency was 450 ms at 2500 rps with 500 mb" in norm


def test_versions_normalization():
    text = "Migrating to Python three point thirteen and PostgreSQL sixteen point two."
    norm = normalize_text(text)
    assert "python 3.13" in norm
    assert "postgresql 16.2" in norm


def test_identifier_exact_match_iemr():
    ref = "Ticket PLAT one zero one and PR four eight two are ready."
    hyp_good = "Ticket plat-101 and pr-482 are ready."
    res_good = compute_identifier_exact_match(ref, hyp_good)
    assert res_good["iemr"] == 0.0
    assert res_good["correct"] == 2
    assert res_good["missing"] == 0

    hyp_bad = "Ticket plat-213 and pr-482 are ready."  # ASR confusion PLAT-101 vs PLAT-213
    res_bad = compute_identifier_exact_match(ref, hyp_bad)
    assert res_bad["iemr"] == 0.5
    assert res_bad["correct"] == 1
    assert res_bad["missing"] == 1


def test_entity_error_rate():
    ref = "Deploying Kafka and Redis to Kubernetes."
    hyp = "Deploying Kafka and Postgres to Kubernetes."
    expected = ["Kafka", "Redis", "Kubernetes"]
    res = compute_entity_error_rate(ref, hyp, expected)
    assert res["total_entities"] == 3
    assert res["correct"] == 2
    assert res["missing"] == 1
    assert res["entity_error_rate"] == 0.3333


def test_script_detection():
    assert detect_script("Grafana memory usage 85 percent") == "Latin"
    assert detect_script("लॉग्स चेक कर लो एक बार") == "Devanagari"
    assert detect_script("Logs check कर लो Grafana पर 85%") == "Mixed"
    assert detect_script("   ") == "Empty"


def test_devanagari_transliteration_and_hinglish_scoring():
    ref = "Logs check kar lo ek baar, Grafana dashboard pe memory usage 85 percent dikha raha hai."
    hyp_deva = "Logs check कर लो एक बार Grafana dashboard पे memory usage 85% दिखा रहा है।"
    raw_wer = compute_normalized_wer(ref, hyp_deva, transliterate_devanagari=False)
    translit_wer = compute_normalized_wer(ref, hyp_deva, transliterate_devanagari=True)
    eng_wer = compute_hinglish_english_wer(ref, hyp_deva)
    tr = transliterate_devanagari_to_latin(hyp_deva)
    assert "kar" in tr
    assert "ek" in tr
    assert translit_wer < raw_wer
    assert translit_wer <= 0.25
    assert eng_wer == 0.0
