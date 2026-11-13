"""MeetMind AI — Golden Evaluation Harness & Calibrated Baselines (Q6 / Q-0d).

Evaluates any InsightPipeline implementation against deterministic seed cases and
optional external held-out cases (`EVAL_HELDOUT_PATH`), reporting:
- Trigger precision & recall
- Supported-claim precision
- Displayed-unsupported-claim rate (hallucination rate)
- Abstention correctness
- Calibration (precision per confidence band)
- Processing latency overhead from timing stamps
- Exact one-sided 95% Clopper-Pearson confidence bounds + INSUFFICIENT N flag (< 300)
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Literal, Protocol

from meetmind_schemas.cards import (
    CardTiming,
    CardTrigger,
    Claim,
    ClaimType,
    ConfidenceBand,
    Evidence,
    InsightCard,
    SourceType,
    TextBlock,
    compute_confidence_band,
    validate_outbound_card,
)
from pydantic import BaseModel, ConfigDict, Field

from meetmind_quality.numbers import (
    check_asr_confusion,
    extract_engineering_identifiers,
    normalize_number_expression,
    normalize_spoken_identifier,
    parse_numeric_value,
)
from meetmind_quality.policy import (
    DEFAULT_MAX_ASR_UNCERTAINTY,
    DEFAULT_MAX_EVIDENCE_AGE_MS,
    publish_decision,
)
from meetmind_quality.verify import verify_card

DEFAULT_SEED = 20261112
DEFAULT_MIN_CASES_FOR_CLAIM = 300
DEFAULT_SEED_PATH = Path("ml/eval/golden/seed_engineering.jsonl")


class WordToken(BaseModel):
    """Word-level ASR token with timestamp and confidence."""

    model_config = ConfigDict(extra="forbid")

    w: str
    start_ms: int
    end_ms: int
    confidence: float
    speaker: str | int | None = None


class TranscriptSegment(BaseModel):
    """Spoken transcript segment with timing and ASR word confidences."""

    model_config = ConfigDict(extra="forbid")

    segment_id: str = Field(min_length=1)
    speaker_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    start_ms: int
    end_ms: int
    confidence: float = Field(ge=0.0, le=1.0)
    language_tag: str = "en"
    words: list[WordToken] = Field(default_factory=list)


class EvidenceRecord(BaseModel):
    """Read-only evidence item available to retrieval during evaluation."""

    model_config = ConfigDict(extra="forbid")

    source_id: str = Field(min_length=1)
    source_type: SourceType
    content: str = Field(min_length=1)
    uri: str | None = None
    retrieved_at: int
    as_of: int | None = None
    topic_key: str | None = None
    state_label: str | None = None


class AudioRef(BaseModel):
    """Optional pointer into the synthetic_eng or recorded audio corpus."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(min_length=1)
    start_ms: int
    end_ms: int
    segment_id: str | None = None


class ExpectedClaimSpec(BaseModel):
    """Ground-truth expected supported claim for harness scoring only."""

    model_config = ConfigDict(extra="forbid")

    claim_type: ClaimType
    text: str
    source_id: str
    value: str | float | None = None
    unit: str | None = None


class EvalCaseInput(BaseModel):
    """Sanitized input exposed to InsightPipeline implementations (NO expected_* fields)."""

    model_config = ConfigDict(extra="forbid")

    case_id: str = Field(min_length=1)
    case_type: str = Field(min_length=1)
    meeting_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    current_ts_ms: int
    transcript_segments: list[TranscriptSegment] = Field(default_factory=list)
    evidence_store: dict[str, EvidenceRecord] = Field(default_factory=dict)
    audio_ref: AudioRef | None = None


class EvalCase(BaseModel):
    """Full golden evaluation case record stored in JSONL."""

    model_config = ConfigDict(extra="forbid")

    case_id: str = Field(min_length=1)
    case_type: str = Field(min_length=1)
    seed: int = DEFAULT_SEED
    meeting_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    current_ts_ms: int
    transcript_segments: list[TranscriptSegment] = Field(default_factory=list)
    evidence_store: dict[str, EvidenceRecord] = Field(default_factory=dict)
    audio_ref: AudioRef | None = None
    expected_triggers: list[str] = Field(default_factory=list)
    expected_claims: list[ExpectedClaimSpec] = Field(default_factory=list)
    expected_abstain: bool = False
    expected_abstain_reason_kind: str | None = None
    expected_confidence_band: ConfidenceBand | None = None

    def to_input(self) -> EvalCaseInput:
        """Return sanitized EvalCaseInput without any expected_* ground truth."""
        return EvalCaseInput(
            case_id=self.case_id,
            case_type=self.case_type,
            meeting_id=self.meeting_id,
            tenant_id=self.tenant_id,
            current_ts_ms=self.current_ts_ms,
            transcript_segments=self.transcript_segments,
            evidence_store=self.evidence_store,
            audio_ref=self.audio_ref,
        )


class PipelineOutput(BaseModel):
    """Result returned by an InsightPipeline for one EvalCaseInput."""

    model_config = ConfigDict(extra="forbid")

    triggered_categories: list[str] = Field(default_factory=list)
    card: InsightCard | None = None
    abstained: bool = True
    abstain_reason: str | None = None
    processing_latency_ms: float | None = None


class InsightPipeline(Protocol):
    """Protocol that every insight pipeline or baseline implements."""

    name: str

    def evaluate_case(self, case_input: EvalCaseInput) -> PipelineOutput:
        """Process one case input and return triggers + optional card or abstention."""


class EvalGatesConfig(BaseModel):
    """Provisional quality gates per Q-0 Q6 and Addendum Section D."""

    model_config = ConfigDict(extra="forbid")

    max_displayed_unsupported_claim_rate: float = 0.0
    min_abstention_correctness: float = 0.95
    min_supported_claim_precision: float = 0.98
    min_high_band_precision: float = 0.95
    min_cases_for_claim: int = DEFAULT_MIN_CASES_FOR_CLAIM


def _binom_cdf(k: int, n: int, p: float) -> float:
    """Exact binomial CDF P(X <= k | n, p) evaluated in log-space."""
    if p <= 0.0:
        return 1.0 if k >= 0 else 0.0
    if p >= 1.0:
        return 1.0 if k >= n else 0.0
    log_p = math.log(p)
    log_q = math.log(1.0 - p)
    lg_n1 = math.lgamma(n + 1)
    total = 0.0
    for i in range(k + 1):
        log_pmf = lg_n1 - math.lgamma(i + 1) - math.lgamma(n - i + 1) + i * log_p + (n - i) * log_q
        total += math.exp(log_pmf)
    return min(1.0, total)


def clopper_pearson_upper(k: int, n: int, alpha: float = 0.05) -> float:
    """Exact one-sided 1 - alpha (default 95%) Clopper-Pearson upper confidence bound."""
    if n <= 0:
        return 1.0
    if k >= n:
        return 1.0
    if k < 0:
        k = 0
    if k == 0:
        return 1.0 - (alpha ** (1.0 / n))
    lo, hi = 0.0, 1.0
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if _binom_cdf(k, n, mid) > alpha:
            lo = mid
        else:
            hi = mid
    return hi


def clopper_pearson_lower(k: int, n: int, alpha: float = 0.05) -> float:
    """Exact one-sided 1 - alpha (default 95%) Clopper-Pearson lower confidence bound."""
    if n <= 0 or k <= 0:
        return 0.0
    if k >= n:
        return alpha ** (1.0 / n)
    return 1.0 - clopper_pearson_upper(n - k, n, alpha=alpha)


class RateMetric(BaseModel):
    """Statistical summary for one binomial proportion."""

    model_config = ConfigDict(extra="forbid")

    name: str
    numerator: int
    denominator: int
    point_estimate: float | None
    cp_95_lower: float | None
    cp_95_upper: float | None
    min_cases_for_claim: int = DEFAULT_MIN_CASES_FOR_CLAIM
    sufficient_n: bool
    flag: str


def summarize_rate(
    name: str,
    numerator: int,
    denominator: int,
    min_cases_for_claim: int = DEFAULT_MIN_CASES_FOR_CLAIM,
) -> RateMetric:
    """Compute point estimate, exact 95% one-sided CP bounds, and INSUFFICIENT N status."""
    sufficient = denominator >= min_cases_for_claim
    if denominator <= 0:
        return RateMetric(
            name=name,
            numerator=0,
            denominator=0,
            point_estimate=None,
            cp_95_lower=None,
            cp_95_upper=None,
            min_cases_for_claim=min_cases_for_claim,
            sufficient_n=False,
            flag="INSUFFICIENT N",
        )
    est = numerator / denominator
    return RateMetric(
        name=name,
        numerator=numerator,
        denominator=denominator,
        point_estimate=est,
        cp_95_lower=clopper_pearson_lower(numerator, denominator),
        cp_95_upper=clopper_pearson_upper(numerator, denominator),
        min_cases_for_claim=min_cases_for_claim,
        sufficient_n=sufficient,
        flag="OK" if sufficient else "INSUFFICIENT N",
    )


class SplitEvalReport(BaseModel):
    """Evaluation output for a single dataset split (seed or heldout)."""

    model_config = ConfigDict(extra="forbid")

    split_label: Literal["seed (NON-AUTHORITATIVE)", "heldout"]
    provided: bool = True
    seed: int | None = None
    total_cases: int = 0
    audio_ref_cases: int = 0
    trigger_precision: RateMetric | None = None
    trigger_recall: RateMetric | None = None
    supported_claim_precision: RateMetric | None = None
    displayed_unsupported_claim_rate: RateMetric | None = None
    abstention_correctness: RateMetric | None = None
    band_calibration: dict[ConfidenceBand, RateMetric] = Field(default_factory=dict)
    latency_overhead_n: int = 0
    latency_overhead_p50_ms: float | None = None
    latency_overhead_p95_ms: float | None = None
    provisional_gates_met: bool | None = None
    gate_failures: list[str] = Field(default_factory=list)


# ── Deterministic Reference Pipelines (NO expected_* access permitted) ────────

_SECRET_PATTERN = re.compile(
    r"(?:sk-[a-z0-9_-]{8,}|akia[0-9a-z]{8,}|ghp_[a-z0-9]{8,}|bearer\s+[a-z0-9._-]{8,}"
    r"|\b(?:api[_ ]?key|secret|password|auth[_ ]?token)\s*(?:is|=|:)\s*[a-z0-9_-]{6,})",
    re.IGNORECASE,
)

_DISSENT_PATTERNS = (
    "disagree",
    "dissent",
    "not agreed",
    "unresolved disagreement",
    "unresolved question",
    "no consensus",
    "objection",
    "hold off on deciding",
    "strongly oppose",
    "reject this proposal",
)

_TRIGGER_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("blocker", ("blocked", "blocker", "stuck on")),
    ("action_item", ("will review", "will own", "will write", "action item", "by friday")),
    ("decision_proposed", ("decision:", "let us merge", "let us bump", "propose we")),
    ("incident_reference", ("incident", "post-mortem", "outage", "oom killer")),
    ("disagreement_signalled", _DISSENT_PATTERNS),
]

_LOW_CONFIDENCE_THRESHOLD = 1.0 - DEFAULT_MAX_ASR_UNCERTAINTY


class AbstainAlways:
    """Reference baseline: always abstains (0 hallucinations, 0 recall)."""

    name = "AbstainAlways"

    def evaluate_case(self, case_input: EvalCaseInput) -> PipelineOutput:
        t0 = time.perf_counter_ns()
        elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
        return PipelineOutput(
            triggered_categories=[],
            card=None,
            abstained=True,
            abstain_reason=f"abstain_always:{case_input.case_id}",
            processing_latency_ms=elapsed,
        )


def _extract_triggers_from_text(text: str) -> list[str]:
    triggers: list[str] = []
    ids = extract_engineering_identifiers(text)
    for ident in ids:
        if ident.startswith("PR-"):
            if "pr_reference" not in triggers:
                triggers.append("pr_reference")
        elif ident.startswith("INC-"):
            if "incident_reference" not in triggers:
                triggers.append("incident_reference")
        elif "ticket_reference" not in triggers:
            triggers.append("ticket_reference")

    low = text.lower()
    for cat, needles in _TRIGGER_KEYWORDS:
        if any(n in low for n in needles) and cat not in triggers:
            triggers.append(cat)
    return triggers


class RetrievalOnly:
    """Deterministic exact-match + normalization + abstention-first baseline."""

    name = "RetrievalOnly"

    def _find_spoken_metric_values(self, text: str) -> list[tuple[float, str, str]]:
        """Extract numeric + unit spans from transcript text."""
        norm = normalize_number_expression(text)
        found: list[tuple[float, str, str]] = []
        pattern = re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(ms|rps|mb|gb|percent|bps|inr|usd|seconds?|minutes?|hours?|points?)\b",
            re.IGNORECASE,
        )
        for m in pattern.finditer(norm):
            val, unit = parse_numeric_value(m.group(0))
            if val is not None and unit is not None:
                found.append((val, unit, m.group(0)))
        return found

    def evaluate_case(self, case_input: EvalCaseInput) -> PipelineOutput:
        t0 = time.perf_counter_ns()
        full_text = " ".join(seg.text for seg in case_input.transcript_segments).strip()
        triggers = _extract_triggers_from_text(full_text)

        # Gate 1: Never leak or display spoken secrets / credentials
        if _SECRET_PATTERN.search(full_text):
            elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason="restricted_credential_detected",
                processing_latency_ms=elapsed,
            )

        # Gate 2: Dissent / unresolved disagreement blocks decision consensus display
        low_text = full_text.lower()
        if any(p in low_text for p in _DISSENT_PATTERNS):
            elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason="disagreement_signalled",
                processing_latency_ms=elapsed,
            )

        # Gate 3: Gather candidate identifiers and check ASR word confidence
        spoken_ids = extract_engineering_identifiers(full_text)
        all_words = [w for seg in case_input.transcript_segments for w in seg.words]
        low_conf_words = [w for w in all_words if w.confidence < _LOW_CONFIDENCE_THRESHOLD]
        avg_asr_conf = (
            sum(w.confidence for w in all_words) / len(all_words)
            if all_words
            else (
                sum(s.confidence for s in case_input.transcript_segments)
                / max(1, len(case_input.transcript_segments))
            )
        )
        asr_uncertainty = max(
            0.0, 1.0 - min((w.confidence for w in all_words), default=avg_asr_conf)
        )

        if not case_input.evidence_store:
            elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason="no_evidence_in_store",
                processing_latency_ms=elapsed,
            )

        # Gate 4: Match evidence records against spoken anchors
        matched_records: list[EvidenceRecord] = []
        norm_full = normalize_spoken_identifier(normalize_number_expression(full_text)).lower()

        for rec in case_input.evidence_store.values():
            rec_ids = extract_engineering_identifiers(rec.source_id + " " + rec.content)
            id_overlap = bool(spoken_ids and any(sid in rec_ids for sid in spoken_ids))
            metric_overlap = False
            for spoken_val, spoken_unit, _raw in self._find_spoken_metric_values(full_text):
                for ev_val, ev_unit, _ev_raw in self._find_spoken_metric_values(rec.content):
                    if spoken_unit == ev_unit and abs(spoken_val - ev_val) < 1e-6:
                        metric_overlap = True
            # Action-item or blocker keyword grounding via topic_key
            topic_overlap = False
            if rec.topic_key and rec.topic_key.lower() in norm_full:
                topic_overlap = True

            if id_overlap or metric_overlap or topic_overlap:
                matched_records.append(rec)

        if not matched_records:
            elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason="no_matching_evidence_for_utterance",
                processing_latency_ms=elapsed,
            )

        # Detect conflicting evidence records on same topic_key or entity ID
        topic_states: dict[str, set[str]] = {}
        for rec in matched_records:
            key = rec.topic_key or rec.source_id.split("#")[0]
            state_val = (rec.state_label or rec.content).strip().lower()
            topic_states.setdefault(key, set()).add(state_val)
        for states in topic_states.values():
            if len(states) > 1:
                elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
                return PipelineOutput(
                    triggered_categories=triggers,
                    card=None,
                    abstained=True,
                    abstain_reason="conflicting_evidence_records",
                    processing_latency_ms=elapsed,
                )

        # Check potential ASR ID/number transposition confusion against store
        for sid in spoken_ids:
            for rec in case_input.evidence_store.values():
                for ev_id in extract_engineering_identifiers(rec.source_id + " " + rec.content):
                    if check_asr_confusion(sid, ev_id) and low_conf_words:
                        elapsed = (time.perf_counter_ns() - t0) / 1_000_000.0
                        return PipelineOutput(
                            triggered_categories=triggers,
                            card=None,
                            abstained=True,
                            abstain_reason=f"asr_identifier_confusion:{sid}_vs_{ev_id}",
                            processing_latency_ms=elapsed,
                        )

        # Build verified claims strictly from matched records
        claims: list[Claim] = []
        raw_text_store = {k: v.content for k, v in case_input.evidence_store.items()}
        min_freshness_score = 1.0

        for idx, rec in enumerate(matched_records):
            age_ms = max(0, case_input.current_ts_ms - rec.retrieved_at)
            if age_ms > DEFAULT_MAX_EVIDENCE_AGE_MS:
                min_freshness_score = 0.2
            rec_ids = extract_engineering_identifiers(rec.source_id + " " + rec.content)
            matching_id = next((sid for sid in spoken_ids if sid in rec_ids), None)
            if matching_id:
                claims.append(
                    Claim(
                        claim_id=f"{case_input.case_id}_clm_{idx}",
                        text=f"{matching_id}: {rec.content}",
                        type="identifier",
                        value=matching_id,
                        status="verified",
                        evidence=[
                            Evidence(
                                source_type=rec.source_type,
                                source_id=rec.source_id,
                                uri=rec.uri,
                                span=rec.content,
                                retrieved_at=rec.retrieved_at,
                                as_of=rec.as_of or rec.retrieved_at,
                            )
                        ],
                    )
                )
            else:
                spoken_metrics = self._find_spoken_metric_values(full_text)
                ev_metrics = self._find_spoken_metric_values(rec.content)
                matched_metric = next(
                    (
                        (v, u)
                        for v, u, _ in spoken_metrics
                        if any(abs(v - ev_v) < 1e-6 and u == ev_u for ev_v, ev_u, _ in ev_metrics)
                    ),
                    None,
                )
                if matched_metric is not None:
                    m_val, m_unit = matched_metric
                    claims.append(
                        Claim(
                            claim_id=f"{case_input.case_id}_clm_{idx}",
                            text=rec.content,
                            type="number",
                            value=m_val,
                            unit=m_unit,
                            status="verified",
                            evidence=[
                                Evidence(
                                    source_type=rec.source_type,
                                    source_id=rec.source_id,
                                    uri=rec.uri,
                                    span=rec.content,
                                    retrieved_at=rec.retrieved_at,
                                    as_of=rec.as_of or rec.retrieved_at,
                                )
                            ],
                        )
                    )
                else:
                    claims.append(
                        Claim(
                            claim_id=f"{case_input.case_id}_clm_{idx}",
                            text=rec.content,
                            type="assertion",
                            value=rec.topic_key,
                            status="verified",
                            evidence=[
                                Evidence(
                                    source_type=rec.source_type,
                                    source_id=rec.source_id,
                                    uri=rec.uri,
                                    span=rec.content,
                                    retrieved_at=rec.retrieved_at,
                                    as_of=rec.as_of or rec.retrieved_at,
                                )
                            ],
                        )
                    )

        last_end_ms = max((s.end_ms for s in case_input.transcript_segments), default=0)
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000.0
        pub_ts_ms = case_input.current_ts_ms

        band = compute_confidence_band(
            {
                "retrieval_score": 0.92,
                "source_freshness": min_freshness_score,
                "asr_confidence": avg_asr_conf,
                "verifier_result": 1.0,
            }
        )
        first_trigger_cat = triggers[0] if triggers else "ticket_reference"
        first_seg_id = (
            case_input.transcript_segments[0].segment_id
            if case_input.transcript_segments
            else "seg_0"
        )
        card = InsightCard(
            card_id=f"card_{case_input.case_id}",
            revision=1,
            meeting_id=case_input.meeting_id,
            tenant_id=case_input.tenant_id,
            trace_id=f"trace_{case_input.case_id}",
            kind="fact",
            tier=1,
            title=f"Verified Engineering Context ({case_input.case_id})",
            blocks=[TextBlock(type="paragraph", content=claims[0].text)],
            claims=claims,
            confidence_band=band,
            confidence_basis={
                "retrieval_score": 0.92,
                "source_freshness": min_freshness_score,
                "asr_confidence": avg_asr_conf,
                "verifier_result": 1.0,
            },
            origin="internal",
            status="verified",
            restricted=False,
            visible_roles=[],
            timing=CardTiming(
                trigger_ts_ms=pub_ts_ms - max(1, int(round(elapsed_ms))),
                published_ts_ms=pub_ts_ms,
                retrieval_ms=round(elapsed_ms, 3),
                reasoning_ms=0.0,
            ),
            trigger=CardTrigger(
                utterance_id=f"{first_seg_id}__{last_end_ms}",
                category=first_trigger_cat,
                trigger_confidence=0.95,
                asr_confidence=avg_asr_conf,
                text_stability=1.0,
            ),
            late=False,
        )

        # Enforce both outbound invariant and verification + abstention policy
        valid_out, reason_out = validate_outbound_card(card)
        if not valid_out:
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason=reason_out,
                processing_latency_ms=elapsed_ms,
            )

        ver = verify_card(card, raw_text_store)
        should_pub, _decision, abstain_reason = publish_decision(
            card=card,
            verification=ver,
            asr_uncertainty=asr_uncertainty,
            current_ts_ms=case_input.current_ts_ms,
        )
        if not should_pub:
            return PipelineOutput(
                triggered_categories=triggers,
                card=None,
                abstained=True,
                abstain_reason=abstain_reason,
                processing_latency_ms=elapsed_ms,
            )

        return PipelineOutput(
            triggered_categories=triggers,
            card=card,
            abstained=False,
            abstain_reason=None,
            processing_latency_ms=elapsed_ms,
        )


class NaiveFabricator:
    """Deliberately flawed calibration baseline that invents unverified numbers/claims."""

    name = "NaiveFabricator"

    def evaluate_case(self, case_input: EvalCaseInput) -> PipelineOutput:
        t0 = time.perf_counter_ns()
        first_seg = (
            case_input.transcript_segments[0]
            if case_input.transcript_segments
            else TranscriptSegment(
                segment_id="seg_0",
                speaker_id="spk_0",
                text="placeholder",
                start_ms=0,
                end_ms=100,
                confidence=0.9,
            )
        )
        fabricated_claim = Claim(
            claim_id=f"fab_{case_input.case_id}",
            text="Fabricated SLA: latency is 9999 ms across PLAT-999 with zero blockers.",
            type="number",
            value=9999.0,
            unit="ms",
            status="verified",
            evidence=[
                Evidence(
                    source_type="internal",
                    source_id="fabricated_memory",
                    span="PLAT-999 telemetry=9999ms fabricated",
                    retrieved_at=case_input.current_ts_ms,
                    as_of=case_input.current_ts_ms,
                )
            ],
        )
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000.0
        card = InsightCard(
            card_id=f"fab_card_{case_input.case_id}",
            revision=1,
            meeting_id=case_input.meeting_id,
            tenant_id=case_input.tenant_id,
            trace_id=f"fab_trace_{case_input.case_id}",
            kind="fact",
            tier=1,
            title="Fabricated Insight",
            blocks=[TextBlock(type="paragraph", content=fabricated_claim.text)],
            claims=[fabricated_claim],
            confidence_band="high",
            confidence_basis={"retrieval_score": 0.99, "asr_confidence": 0.99},
            origin="internal",
            status="verified",
            timing=CardTiming(
                trigger_ts_ms=case_input.current_ts_ms - 1,
                published_ts_ms=case_input.current_ts_ms,
                retrieval_ms=elapsed_ms,
            ),
            trigger=CardTrigger(
                utterance_id=first_seg.segment_id,
                category="ticket_reference",
                trigger_confidence=0.99,
                asr_confidence=first_seg.confidence,
                text_stability=1.0,
            ),
        )
        return PipelineOutput(
            triggered_categories=["ticket_reference"],
            card=card,
            abstained=False,
            processing_latency_ms=elapsed_ms,
        )


def load_eval_cases(path: Path) -> list[EvalCase]:
    """Load golden evaluation cases from JSONL."""
    cases: list[EvalCase] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            cases.append(EvalCase.model_validate_json(stripped))
    return cases


def _claim_is_supported(claim: Claim, case: EvalCase) -> bool:
    """Determine if a published claim matches truth in case evidence_store and expected_claims."""
    if not claim.evidence:
        return False
    # Check any restricted secret leak into claim text
    full_spoken = " ".join(s.text for s in case.transcript_segments)
    secret_match = _SECRET_PATTERN.search(full_spoken)
    if secret_match and secret_match.group(0).lower() in claim.text.lower():
        return False

    raw_store = {k: v.content for k, v in case.evidence_store.items()}
    ver = verify_card(
        InsightCard(
            card_id="tmp",
            meeting_id=case.meeting_id,
            tenant_id=case.tenant_id,
            trace_id="tmp",
            kind="fact",
            tier=1,
            title="tmp",
            claims=[claim],
            confidence_band="high",
            origin="internal",
            status="verified",
            timing=CardTiming(trigger_ts_ms=0, published_ts_ms=1),
            trigger=CardTrigger(
                utterance_id="u",
                category="c",
                trigger_confidence=1.0,
                asr_confidence=1.0,
                text_stability=1.0,
            ),
        ),
        raw_store,
    )
    if not ver["verified"]:
        return False
    # Every cited source_id must actually exist in case.evidence_store
    for ev in claim.evidence:
        if ev.source_id not in case.evidence_store:
            return False
        age_ms = max(0, case.current_ts_ms - case.evidence_store[ev.source_id].retrieved_at)
        if age_ms > DEFAULT_MAX_EVIDENCE_AGE_MS:
            return False

    # And must align with at least one expected claim if expected_abstain is False
    if case.expected_abstain:
        return False
    if not case.expected_claims:
        return True
    claim_norm = normalize_spoken_identifier(normalize_number_expression(claim.text)).lower()
    for exp in case.expected_claims:
        if any(ev.source_id == exp.source_id for ev in claim.evidence):
            exp_val = str(exp.value or exp.text).lower()
            if exp_val in claim_norm or str(claim.value or "").lower() == exp_val:
                return True
    return False


def _percentile(sorted_vals: list[float], pct: float) -> float | None:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (pct / 100.0) * (len(sorted_vals) - 1)
    lo = math.floor(rank)
    hi = min(len(sorted_vals) - 1, lo + 1)
    frac = rank - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def evaluate_split(
    pipeline: InsightPipeline,
    cases: list[EvalCase],
    split_label: Literal["seed (NON-AUTHORITATIVE)", "heldout"],
    gates: EvalGatesConfig | None = None,
) -> SplitEvalReport:
    """Evaluate a pipeline on one list of cases and return structured stats + gate checks."""
    cfg = gates or EvalGatesConfig()
    if not cases:
        return SplitEvalReport(split_label=split_label, provided=False)

    seed_val = cases[0].seed if cases else None
    audio_ref_count = sum(1 for c in cases if c.audio_ref is not None)

    trig_tp = 0
    trig_pred = 0
    trig_true = 0

    claims_displayed = 0
    claims_supported = 0
    claims_unsupported = 0

    abstain_correct = 0
    abstain_total = len(cases)

    band_counts: dict[ConfidenceBand, list[int]] = {
        "high": [0, 0],
        "medium": [0, 0],
        "low": [0, 0],
    }
    latencies: list[float] = []

    for case in cases:
        out = pipeline.evaluate_case(case.to_input())

        if out.processing_latency_ms is not None:
            latencies.append(float(out.processing_latency_ms))
        elif out.card is not None:
            latencies.append(
                float(max(0, out.card.timing.published_ts_ms - out.card.timing.trigger_ts_ms))
            )

        pred_set = set(out.triggered_categories)
        true_set = set(case.expected_triggers)
        trig_tp += len(pred_set & true_set)
        trig_pred += len(pred_set)
        trig_true += len(true_set)

        is_abstained = out.abstained or out.card is None
        if case.expected_abstain:
            if is_abstained:
                abstain_correct += 1
        else:
            if not is_abstained:
                abstain_correct += 1

        if not is_abstained and out.card is not None:
            all_claims_ok = len(out.card.claims) > 0
            for clm in out.card.claims:
                claims_displayed += 1
                ok = _claim_is_supported(clm, case)
                if ok:
                    claims_supported += 1
                else:
                    claims_unsupported += 1
                    all_claims_ok = False
            band_counts[out.card.confidence_band][1] += 1
            if all_claims_ok:
                band_counts[out.card.confidence_band][0] += 1

    trig_prec = summarize_rate("trigger_precision", trig_tp, trig_pred, cfg.min_cases_for_claim)
    trig_rec = summarize_rate("trigger_recall", trig_tp, trig_true, cfg.min_cases_for_claim)
    clm_prec = summarize_rate(
        "supported_claim_precision",
        claims_supported,
        claims_displayed,
        cfg.min_cases_for_claim,
    )
    halluc_rate = summarize_rate(
        "displayed_unsupported_claim_rate",
        claims_unsupported,
        claims_displayed,
        cfg.min_cases_for_claim,
    )
    abstain_acc = summarize_rate(
        "abstention_correctness", abstain_correct, abstain_total, cfg.min_cases_for_claim
    )
    band_cal = {
        band: summarize_rate(f"calibration_{band}", counts[0], counts[1], cfg.min_cases_for_claim)
        for band, counts in band_counts.items()
    }

    gate_failures: list[str] = []
    if (
        halluc_rate.point_estimate is not None
        and halluc_rate.point_estimate > cfg.max_displayed_unsupported_claim_rate
    ):
        gate_failures.append(
            f"displayed_unsupported_claim_rate={halluc_rate.point_estimate:.4f} > "
            f"{cfg.max_displayed_unsupported_claim_rate:.4f}"
        )
    if (
        abstain_acc.point_estimate is None
        or abstain_acc.point_estimate < cfg.min_abstention_correctness
    ):
        gate_failures.append(
            f"abstention_correctness={abstain_acc.point_estimate} < "
            f"{cfg.min_abstention_correctness:.4f}"
        )
    if (
        clm_prec.point_estimate is not None
        and clm_prec.point_estimate < cfg.min_supported_claim_precision
    ):
        gate_failures.append(
            f"supported_claim_precision={clm_prec.point_estimate:.4f} < "
            f"{cfg.min_supported_claim_precision:.4f}"
        )
    high_cal = band_cal["high"]
    if (
        high_cal.point_estimate is not None
        and high_cal.point_estimate < cfg.min_high_band_precision
    ):
        gate_failures.append(
            f"high_band_precision={high_cal.point_estimate:.4f} < {cfg.min_high_band_precision:.4f}"
        )

    lat_sorted = sorted(latencies)
    return SplitEvalReport(
        split_label=split_label,
        provided=True,
        seed=seed_val,
        total_cases=len(cases),
        audio_ref_cases=audio_ref_count,
        trigger_precision=trig_prec,
        trigger_recall=trig_rec,
        supported_claim_precision=clm_prec,
        displayed_unsupported_claim_rate=halluc_rate,
        abstention_correctness=abstain_acc,
        band_calibration=band_cal,
        latency_overhead_n=len(lat_sorted),
        latency_overhead_p50_ms=_percentile(lat_sorted, 50.0),
        latency_overhead_p95_ms=_percentile(lat_sorted, 95.0),
        provisional_gates_met=len(gate_failures) == 0,
        gate_failures=gate_failures,
    )


def run_evaluation(
    pipeline: InsightPipeline,
    seed_path: Path = DEFAULT_SEED_PATH,
    heldout_env_var: str = "EVAL_HELDOUT_PATH",
    gates: EvalGatesConfig | None = None,
) -> dict[str, Any]:
    """Run an InsightPipeline across seed and optional external held-out cases."""
    seed_cases = load_eval_cases(seed_path)
    seed_report = evaluate_split(
        pipeline=pipeline,
        cases=seed_cases,
        split_label="seed (NON-AUTHORITATIVE)",
        gates=gates,
    )

    heldout_raw = os.environ.get(heldout_env_var, "").strip()
    if heldout_raw:
        heldout_path = Path(heldout_raw)
        if heldout_path.is_file():
            heldout_cases = load_eval_cases(heldout_path)
            heldout_report = evaluate_split(
                pipeline=pipeline,
                cases=heldout_cases,
                split_label="heldout",
                gates=gates,
            )
        else:
            heldout_report = SplitEvalReport(split_label="heldout", provided=False)
    else:
        heldout_report = SplitEvalReport(split_label="heldout", provided=False)

    return {
        "pipeline": pipeline.name,
        "seed_path": str(seed_path),
        "heldout_env_var": heldout_env_var,
        "heldout_status": "PROVIDED" if heldout_report.provided else "NOT PROVIDED",
        "seed": seed_report.model_dump(),
        "heldout": heldout_report.model_dump(),
    }


def dump_cases_jsonl(cases: list[EvalCase], path: Path) -> None:
    """Serialize EvalCase records deterministically to JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for c in cases:
            fh.write(json.dumps(c.model_dump(), ensure_ascii=False) + "\n")
