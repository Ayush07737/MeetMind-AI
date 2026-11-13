"""Unit tests for Golden Evaluation Harness (Q6 / Addendum Section D)."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from meetmind_quality import eval as eval_mod
from meetmind_quality.eval import (
    DEFAULT_SEED,
    DEFAULT_SEED_PATH,
    AbstainAlways,
    EvalCaseInput,
    NaiveFabricator,
    RetrievalOnly,
    clopper_pearson_upper,
    dump_cases_jsonl,
    evaluate_split,
    load_eval_cases,
    run_evaluation,
    summarize_rate,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_seed_corpus_completeness_and_audio_refs():
    seed_file = REPO_ROOT / DEFAULT_SEED_PATH
    assert seed_file.is_file(), f"Missing {seed_file}"
    cases = load_eval_cases(seed_file)
    assert len(cases) >= 60
    assert {c.seed for c in cases} == {DEFAULT_SEED}

    required_types = {
        "ticket_status",
        "pr_mention",
        "action_item",
        "decision_with_dissent",
        "blocker",
        "no_evidence",
        "conflicting_evidence",
        "stale_evidence",
        "identifier_confusion",
        "numeric_confusion",
        "hinglish_snippet",
        "restricted_secret",
        "incident_timeline",
    }
    present_types = {c.case_type for c in cases}
    assert required_types.issubset(present_types)

    audio_linked = [c for c in cases if c.audio_ref is not None]
    assert len(audio_linked) >= 10
    for c in audio_linked:
        assert (REPO_ROOT / c.audio_ref.path).is_file(), f"Missing audio_ref: {c.audio_ref.path}"


def test_clopper_pearson_and_insufficient_n():
    # N=60, k=0 -> one-sided 95% upper bound ~4.87% (< 5%)
    bound = clopper_pearson_upper(0, 60, alpha=0.05)
    assert 0.048 <= bound <= 0.049

    rate = summarize_rate("displayed_unsupported_claim_rate", 0, 60, min_cases_for_claim=300)
    assert rate.point_estimate == 0.0
    assert rate.sufficient_n is False
    assert rate.flag == "INSUFFICIENT N"
    assert 0.048 <= (rate.cp_95_upper or 0.0) <= 0.049

    rate_300 = summarize_rate("displayed_unsupported_claim_rate", 0, 300, min_cases_for_claim=300)
    assert rate_300.sufficient_n is True
    assert rate_300.flag == "OK"


def test_reference_baselines_do_not_access_expected_fields():
    """AST scan ensuring reference pipelines never access any expected_* attribute or key."""
    assert "expected_" not in " ".join(EvalCaseInput.model_fields.keys())
    for cls in (AbstainAlways, RetrievalOnly, NaiveFabricator):
        src = inspect.getsource(cls)
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr.startswith("expected_"):
                raise AssertionError(f"{cls.__name__} accesses expected_* attribute '{node.attr}'")
            if isinstance(node, ast.Name) and node.id.startswith("expected_"):
                raise AssertionError(f"{cls.__name__} references expected_* name '{node.id}'")
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and "expected_" in node.value
            ):
                raise AssertionError(
                    f"{cls.__name__} contains string literal with expected_: {node.value!r}"
                )
    # Also ensure EvalCaseInput helper exists cleanly
    assert hasattr(eval_mod, "EvalCase")


def test_baseline_calibration_gates():
    cases = load_eval_cases(REPO_ROOT / DEFAULT_SEED_PATH)

    abstain_rep = evaluate_split(AbstainAlways(), cases, "seed (NON-AUTHORITATIVE)")
    assert abstain_rep.displayed_unsupported_claim_rate is not None
    assert abstain_rep.displayed_unsupported_claim_rate.numerator == 0
    assert abstain_rep.trigger_recall is not None
    assert abstain_rep.trigger_recall.point_estimate == 0.0

    retrieval_rep = evaluate_split(RetrievalOnly(), cases, "seed (NON-AUTHORITATIVE)")
    assert retrieval_rep.provisional_gates_met is True, f"Failures: {retrieval_rep.gate_failures}"
    assert retrieval_rep.displayed_unsupported_claim_rate is not None
    assert retrieval_rep.displayed_unsupported_claim_rate.numerator == 0
    assert (retrieval_rep.abstention_correctness.point_estimate or 0.0) >= 0.95
    assert (retrieval_rep.supported_claim_precision.point_estimate or 0.0) >= 0.98
    assert retrieval_rep.latency_overhead_p95_ms is not None

    fab_rep = evaluate_split(NaiveFabricator(), cases, "seed (NON-AUTHORITATIVE)")
    assert fab_rep.provisional_gates_met is False
    assert (fab_rep.displayed_unsupported_claim_rate.point_estimate or 0.0) > 0.0


def test_heldout_env_loading_and_not_provided(monkeypatch, tmp_path: Path):
    seed_file = REPO_ROOT / DEFAULT_SEED_PATH
    monkeypatch.delenv("EVAL_HELDOUT_PATH", raising=False)
    res_missing = run_evaluation(RetrievalOnly(), seed_path=seed_file)
    assert res_missing["heldout_status"] == "NOT PROVIDED"
    assert res_missing["heldout"]["provided"] is False

    # Create temporary heldout subset outside repo
    cases = load_eval_cases(seed_file)[:10]
    heldout_file = tmp_path / "external_heldout.jsonl"
    dump_cases_jsonl(cases, heldout_file)
    monkeypatch.setenv("EVAL_HELDOUT_PATH", str(heldout_file))

    res_present = run_evaluation(RetrievalOnly(), seed_path=seed_file)
    assert res_present["heldout_status"] == "PROVIDED"
    assert res_present["heldout"]["provided"] is True
    assert res_present["heldout"]["split_label"] == "heldout"
    assert res_present["heldout"]["total_cases"] == 10
