"""Deterministic Golden Evaluation Seed Generator (Q6 / Q-0d).

Generates 65 engineering-domain evaluation cases (5 per each of the 13 required case types)
with fixed seed 20261112, deterministic timestamps, per-word ASR confidences, read-only evidence
snapshots, expected triggers/claims/abstentions, and optional audio_ref pointers into
ml/eval/audio/synthetic_eng/data/.
"""

from __future__ import annotations

import random
from pathlib import Path

from meetmind_quality.eval import (
    DEFAULT_SEED,
    DEFAULT_SEED_PATH,
    AudioRef,
    EvalCase,
    EvidenceRecord,
    ExpectedClaimSpec,
    TranscriptSegment,
    WordToken,
    dump_cases_jsonl,
)

BASE_TS_MS = 1_794_000_000_000


def _make_words(
    text: str, start_ms: int, conf_map: dict[str, float] | None = None, base_conf: float = 0.96
) -> list[WordToken]:
    """Build deterministic per-word timings and confidences."""
    words: list[WordToken] = []
    cur = start_ms
    overrides = conf_map or {}
    for tok in text.split():
        clean = tok.strip(".,?!:")
        conf = overrides.get(clean, overrides.get(tok, base_conf))
        end = cur + max(180, len(tok) * 45)
        words.append(WordToken(w=tok, start_ms=cur, end_ms=end, confidence=conf, speaker=0))
        cur = end + 40
    return words


def build_seed_cases(seed: int = DEFAULT_SEED) -> list[EvalCase]:
    """Deterministically generate 65 golden engineering cases across all 13 categories."""
    rng = random.Random(seed)
    cases: list[EvalCase] = []

    # 1. ticket_status (5 cases) — all verifiable -> PUBLISH
    ticket_specs = [
        (
            "PLAT-101",
            "Vikram, what is the status update on PLAT one zero one?",
            "PLAT-101 status=in_progress assignee=Vikram service=checkout-api",
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_00_clean.wav",
            0,
            9984,
            "scenario_01_daily_standup_t00_clean",
        ),
        (
            "ENG-789",
            "ENG seven eight nine is targeted for Python migration this sprint.",
            "ENG-789 status=in_review points=5 owner=Rahul",
            "ml/eval/audio/synthetic_eng/data/scenario_02_sprint_planning/turn_01_clean.wav",
            8920,
            18856,
            "scenario_02_sprint_planning_t01_clean",
        ),
        (
            "FRONT-303",
            "What about FRONT three zero three ticket for the dashboard widget?",
            "FRONT-303 status=ready_for_qa assignee=Sneha",
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_11_clean.wav",
            95408,
            100232,
            "scenario_01_daily_standup_t11_clean",
        ),
        (
            "PLAT-204",
            "Please check PLAT two zero four for PostgreSQL connection pooling.",
            "PLAT-204 status=done merged_pr=PR-510",
            None,
            0,
            0,
            None,
        ),
        (
            "ENG-912",
            "ENG nine one two covers Prometheus exporter cardinality limits.",
            "ENG-912 status=open priority=P1 assignee=Ananya",
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (tid, spoken, ev_text, wav_path, s_ms, e_ms, seg_id) in enumerate(ticket_specs):
        ts_now = BASE_TS_MS + idx * 1000
        cid = f"seed_ticket_status_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="ticket_status",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4200,
                        confidence=0.96,
                        words=_make_words(spoken, s_ms, base_conf=0.96),
                    )
                ],
                evidence_store={
                    tid: EvidenceRecord(
                        source_id=tid,
                        source_type="ticket",
                        content=ev_text,
                        uri=f"https://jira.internal/browse/{tid}",
                        retrieved_at=ts_now - 15_000,
                        as_of=ts_now - 15_000,
                        topic_key=tid,
                    )
                },
                audio_ref=(
                    AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
                    if wav_path
                    else None
                ),
                expected_triggers=["ticket_reference"],
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{tid}: {ev_text}",
                        source_id=tid,
                        value=tid,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # 2. pr_mention (5 cases) — all verifiable -> PUBLISH
    pr_specs = [
        (
            "PR-482",
            "Yes exactly. I opened PR four eight two to add a composite index on tenant ID.",
            "PR-482 status=approved checks=green branch=feat/checkout-idx",
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_03_clean.wav",
            28968,
            37464,
            "scenario_01_daily_standup_t03_clean",
        ),
        (
            "PR-501",
            "Sneha completed schemas. PR five zero one is ready for review.",
            "PR-501 status=open reviews=1_approved checks=green",
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_12_clean.wav",
            100632,
            109608,
            "scenario_01_daily_standup_t12_clean",
        ),
        (
            "PR-612",
            "PR six one two lowers Redis pool timeout to 50 milliseconds.",
            "PR-612 status=merged commit=a1b2c3d4 checks=green",
            "ml/eval/audio/synthetic_eng/data/scenario_04_code_review/turn_00_clean.wav",
            0,
            6000,
            None,
        ),
        (
            "PR-744",
            "Check PR seven four four before deploying gateway canary.",
            "PR-744 status=approved author=Vikram checks=green",
            None,
            0,
            0,
            None,
        ),
        (
            "PR-815",
            "PR eight one five bumps uv lockfile for Deepgram WebSocket client.",
            "PR-815 status=open checks=green reviewer=Rahul",
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (pr_id, spoken, ev_text, wav_path, s_ms, e_ms, seg_id) in enumerate(pr_specs):
        ts_now = BASE_TS_MS + 10_000 + idx * 1000
        cid = f"seed_pr_mention_{idx + 1:02d}"
        audio_ref = None
        if wav_path and Path(wav_path).exists():
            audio_ref = AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="pr_mention",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_1",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4500,
                        confidence=0.95,
                        words=_make_words(spoken, s_ms, base_conf=0.95),
                    )
                ],
                evidence_store={
                    pr_id: EvidenceRecord(
                        source_id=pr_id,
                        source_type="pull_request",
                        content=ev_text,
                        uri=f"https://github.com/acme/repo/pull/{pr_id.split('-')[1]}",
                        retrieved_at=ts_now - 20_000,
                        as_of=ts_now - 20_000,
                        topic_key=pr_id,
                    )
                },
                audio_ref=audio_ref,
                expected_triggers=["pr_reference"],
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{pr_id}: {ev_text}",
                        source_id=pr_id,
                        value=pr_id,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # 3. action_item (5 cases) — owner + due date grounded -> PUBLISH
    action_specs = [
        (
            "PR-482",
            "Great. Rahul will review PR four eight two before noon today.",
            "PR-482 reviewer_assigned=Rahul target_due=today_noon",
            ["pr_reference", "action_item"],
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_04_clean.wav",
            37864,
            43648,
            "scenario_01_daily_standup_t04_clean",
        ),
        (
            "ENG-789",
            (
                "Agreed on 5 points. Rahul will own ENG seven eight nine migration "
                "by Friday November fifteenth."
            ),
            "ENG-789 owner=Rahul due=2026-11-15 scope=Python_3.13_migration",
            ["ticket_reference", "action_item"],
            "ml/eval/audio/synthetic_eng/data/scenario_02_sprint_planning/turn_04_clean.wav",
            36976,
            46120,
            "scenario_02_sprint_planning_t04_clean",
        ),
        (
            "INC-409",
            "Vikram will set strict 8 gigabytes memory limits for INC four zero nine by Friday.",
            "INC-409 action_owner=Vikram remediation=8gb_helm_limit due=Friday",
            ["incident_reference", "action_item"],
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_13_clean.wav",
            107775,
            116007,
            "scenario_05_incident_review_t13_clean",
        ),
        (
            "PLAT-310",
            "Ananya will review PLAT three one zero load test report by Thursday.",
            "PLAT-310 assignee=Ananya deliverable=load_test_report due=Thursday",
            ["ticket_reference", "action_item"],
            None,
            0,
            0,
            None,
        ),
        (
            "PR-525",
            "Sneha will review PR five two five before Friday release cut.",
            "PR-525 reviewer=Sneha due=Friday release_cut=true",
            ["pr_reference", "action_item"],
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (ref_id, spoken, ev_text, trigs, wav_path, s_ms, e_ms, seg_id) in enumerate(
        action_specs
    ):
        ts_now = BASE_TS_MS + 20_000 + idx * 1000
        cid = f"seed_action_item_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="action_item",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4800,
                        confidence=0.95,
                        words=_make_words(spoken, s_ms, base_conf=0.95),
                    )
                ],
                evidence_store={
                    ref_id: EvidenceRecord(
                        source_id=ref_id,
                        source_type="ticket"
                        if ref_id.startswith(("PLAT", "ENG"))
                        else ("incident" if ref_id.startswith("INC") else "pull_request"),
                        content=ev_text,
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                        topic_key=ref_id,
                    )
                },
                audio_ref=(
                    AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
                    if wav_path
                    else None
                ),
                expected_triggers=trigs,
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{ref_id}: {ev_text}",
                        source_id=ref_id,
                        value=ref_id,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # 4. decision_with_dissent (5 cases) — unresolved disagreement -> MUST ABSTAIN
    dissent_texts = [
        (
            (
                "We are discussing ADR-014 event bus. There is a disagreement "
                "between Redis Streams and Kafka; no consensus yet."
            ),
            "ml/eval/audio/synthetic_eng/data/scenario_10_disagreement_unresolved/turn_00_clean.wav",
            0,
            9696,
            "scenario_10_disagreement_unresolved_t00_clean",
        ),
        (
            (
                "We still have an unresolved disagreement regarding cross region "
                "disaster recovery replication."
            ),
            "ml/eval/audio/synthetic_eng/data/scenario_10_disagreement_unresolved/turn_13_clean.wav",
            112648,
            119032,
            "scenario_10_disagreement_unresolved_t13_clean",
        ),
        (
            (
                "Hold off on deciding PR-482 deploy; Priya raised an objection "
                "on replica lock duration."
            ),
            None,
            0,
            0,
            None,
        ),
        (
            (
                "Several engineers strongly oppose dropping Neo4j namespace "
                "prefixing; dissent remains open."
            ),
            None,
            0,
            0,
            None,
        ),
        (
            (
                "Team did not reach agreement on PLAT-101 timeout target and "
                "not agreed on rollback threshold."
            ),
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (spoken, wav_path, s_ms, e_ms, seg_id) in enumerate(dissent_texts):
        ts_now = BASE_TS_MS + 30_000 + idx * 1000
        cid = f"seed_decision_dissent_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="decision_with_dissent",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_2",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4500,
                        confidence=0.94,
                        words=_make_words(spoken, s_ms, base_conf=0.94),
                    )
                ],
                evidence_store={
                    "ADR-014": EvidenceRecord(
                        source_id="ADR-014",
                        source_type="adr",
                        content="ADR-014 status=proposed pending_benchmark_spike",
                        retrieved_at=ts_now - 5_000,
                        as_of=ts_now - 5_000,
                        topic_key="ADR-014",
                    )
                },
                audio_ref=(
                    AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
                    if wav_path
                    else None
                ),
                expected_triggers=["disagreement_signalled"],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="disagreement_signalled",
            )
        )

    # 5. blocker (5 cases) — grounded blocker -> PUBLISH
    blocker_specs = [
        (
            "ENG-4521",
            (
                "On my side, ticket ENG four five two one is blocked on "
                "Kafka cluster upgrade to version 3.7."
            ),
            "ENG-4521 status=blocked blocker=kafka_upgrade_3.7 team=platform",
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_05_clean.wav",
            44048,
            52352,
            "scenario_01_daily_standup_t05_clean",
        ),
        (
            "PLAT-118",
            "PLAT one one eight is blocked awaiting staging schema migration lock.",
            "PLAT-118 status=blocked blocker=staging_migration_lock",
            None,
            0,
            0,
            None,
        ),
        (
            "PR-509",
            "PR five zero nine merge is blocked on flaky Windows Redis integration test.",
            "PR-509 status=blocked blocker=ci_windows_redis_check",
            None,
            0,
            0,
            None,
        ),
        (
            "ENG-615",
            "Ticket ENG six one five is blocked by missing IAM role approval.",
            "ENG-615 status=blocked blocker=iam_role_approval",
            None,
            0,
            0,
            None,
        ),
        (
            "PLAT-299",
            "PLAT two nine nine rollout is stuck on Envoy readiness probe mismatch.",
            "PLAT-299 status=blocked blocker=envoy_readiness_probe",
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (bid, spoken, ev_text, wav_path, s_ms, e_ms, seg_id) in enumerate(blocker_specs):
        ts_now = BASE_TS_MS + 40_000 + idx * 1000
        cid = f"seed_blocker_{idx + 1:02d}"
        trigs = ["pr_reference" if bid.startswith("PR-") else "ticket_reference", "blocker"]
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="blocker",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_3",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4600,
                        confidence=0.95,
                        words=_make_words(spoken, s_ms, base_conf=0.95),
                    )
                ],
                evidence_store={
                    bid: EvidenceRecord(
                        source_id=bid,
                        source_type="pull_request" if bid.startswith("PR-") else "ticket",
                        content=ev_text,
                        retrieved_at=ts_now - 12_000,
                        as_of=ts_now - 12_000,
                        topic_key=bid,
                    )
                },
                audio_ref=(
                    AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
                    if wav_path
                    else None
                ),
                expected_triggers=trigs,
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{bid}: {ev_text}",
                        source_id=bid,
                        value=bid,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # 6. no_evidence (5 cases) — empty or non-matching store -> MUST ABSTAIN
    no_ev_specs = [
        ("PLAT-901", "Please verify PLAT nine zero one before deploy."),
        ("PR-992", "Did anyone merge PR nine nine two into release branch?"),
        ("ENG-830", "ENG eight three zero has an unverified QPS claim of 12000 rps."),
        ("INC-777", "Check INC seven seven seven root cause doc in Confluence."),
        ("PLAT-412", "Is PLAT four one two assigned to SRE on-call right now?"),
    ]
    for idx, (_missing_id, spoken) in enumerate(no_ev_specs):
        ts_now = BASE_TS_MS + 50_000 + idx * 1000
        cid = f"seed_no_evidence_{idx + 1:02d}"
        store = (
            {}
            if idx % 2 == 0
            else {
                "UNRELATED-100": EvidenceRecord(
                    source_id="UNRELATED-100",
                    source_type="doc",
                    content="UNRELATED-100 onboarding runbook placeholder",
                    retrieved_at=ts_now - 5_000,
                    topic_key="UNRELATED-100",
                )
            }
        )
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="no_evidence",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=0,
                        end_ms=3900,
                        confidence=0.95,
                        words=_make_words(spoken, 0, base_conf=0.95),
                    )
                ],
                evidence_store=store,
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="no_evidence",
            )
        )

    # 7. conflicting_evidence (5 cases) — two sources disagree -> MUST ABSTAIN
    conflict_specs = [
        (
            "PR-482",
            "What is the current merge state of PR four eight two?",
            "merged",
            "closed_unmerged",
        ),
        ("PLAT-101", "Confirm assignee status on PLAT one zero one.", "in_progress", "done"),
        ("INC-409", "Is incident INC four zero nine still active?", "mitigated", "sev1_active"),
        (
            "ENG-789",
            "Check current sprint state of ENG seven eight nine.",
            "ready_for_prod",
            "blocked",
        ),
        ("PR-501", "Can we release PR five zero one right now?", "checks_green", "checks_failing"),
    ]
    for idx, (cid_ref, spoken, st_a, st_b) in enumerate(conflict_specs):
        ts_now = BASE_TS_MS + 60_000 + idx * 1000
        cid = f"seed_conflicting_evidence_{idx + 1:02d}"
        src_type = (
            "pull_request"
            if cid_ref.startswith("PR-")
            else ("incident" if cid_ref.startswith("INC-") else "ticket")
        )
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="conflicting_evidence",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_1",
                        text=spoken,
                        start_ms=0,
                        end_ms=4100,
                        confidence=0.95,
                        words=_make_words(spoken, 0, base_conf=0.95),
                    )
                ],
                evidence_store={
                    f"{cid_ref}#github": EvidenceRecord(
                        source_id=f"{cid_ref}#github",
                        source_type=src_type,
                        content=f"{cid_ref} status={st_a}",
                        retrieved_at=ts_now - 8_000,
                        as_of=ts_now - 8_000,
                        topic_key=cid_ref,
                        state_label=st_a,
                    ),
                    f"{cid_ref}#slack": EvidenceRecord(
                        source_id=f"{cid_ref}#slack",
                        source_type="doc",
                        content=f"{cid_ref} status={st_b}",
                        retrieved_at=ts_now - 6_000,
                        as_of=ts_now - 6_000,
                        topic_key=cid_ref,
                        state_label=st_b,
                    ),
                },
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="conflicting_evidence",
            )
        )

    # 8. stale_evidence (5 cases) — retrieved_at exceeds max_evidence_age_ms (300s) -> MUST ABSTAIN
    stale_specs = [
        ("PLAT-105", "Check if PLAT one zero five canary passed staging.", 600_000),
        ("PR-390", "Is PR three nine zero still open for backend review?", 900_000),
        ("INC-311", "What is the current severity on INC three one one?", 450_000),
        ("ENG-512", "Is ENG five one two Redis TTL ticket closed?", 1_800_000),
        ("PLAT-220", "Did PLAT two two zero finish staging soak?", 360_000),
    ]
    for idx, (sid_ref, spoken, age_ms) in enumerate(stale_specs):
        ts_now = BASE_TS_MS + 70_000 + idx * 1000
        cid = f"seed_stale_evidence_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="stale_evidence",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=0,
                        end_ms=4000,
                        confidence=0.95,
                        words=_make_words(spoken, 0, base_conf=0.95),
                    )
                ],
                evidence_store={
                    sid_ref: EvidenceRecord(
                        source_id=sid_ref,
                        source_type="ticket",
                        content=f"{sid_ref} status=passed_staging",
                        retrieved_at=ts_now - age_ms,
                        as_of=ts_now - age_ms,
                        topic_key=sid_ref,
                    )
                },
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="stale_evidence",
            )
        )

    # 9. identifier_confusion (5 cases) — off-by-digit IDs -> MUST ABSTAIN
    id_confusion_specs = [
        (
            "We should deploy PLAT one two three before tonight.",
            "PLAT-123",
            "PLAT-213",
            "one",
            0.52,
        ),
        (
            "Merge PR four eight two into checkout main branch.",
            "PR-482",
            "PR-428",
            "eight",
            0.55,
        ),
        (
            "Close ENG four five two one after soak verification.",
            "ENG-4521",
            "ENG-4512",
            "two",
            0.49,
        ),
        (
            "Open PLAT three zero one runbook in Confluence.",
            "PLAT-301",
            "PLAT-310",
            "zero",
            0.54,
        ),
        (
            "Review PR six two four database rollback migration.",
            "PR-624",
            "PR-642",
            "two",
            0.51,
        ),
    ]
    for idx, (spoken, heard_id, store_id, low_tok, low_conf) in enumerate(id_confusion_specs):
        ts_now = BASE_TS_MS + 80_000 + idx * 1000
        cid = f"seed_identifier_confusion_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="identifier_confusion",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_1",
                        text=spoken,
                        start_ms=0,
                        end_ms=4200,
                        confidence=0.81,
                        words=_make_words(spoken, 0, conf_map={low_tok: low_conf}, base_conf=0.94),
                    )
                ],
                evidence_store={
                    store_id: EvidenceRecord(
                        source_id=store_id,
                        source_type="pull_request" if store_id.startswith("PR-") else "ticket",
                        content=f"{store_id} status=approved (heard candidate was {heard_id})",
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                        topic_key=store_id,
                    )
                },
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="identifier_confusion",
            )
        )

    # 10. numeric_confusion (5 cases) — mismatched scales/digits -> MUST ABSTAIN
    num_confusion_specs = [
        (
            "Redis cache p99 reached 450 milliseconds at peak load.",
            "metrics#1",
            "Redis cache p99 baseline is 4500 ms (4.5 seconds)",
            "450",
            0.53,
        ),
        (
            "PostgreSQL index backfill needs 15 gigabytes headroom.",
            "metrics#2",
            "PostgreSQL index backfill allocated 50 gb headroom",
            "15",
            0.56,
        ),
        (
            "Gateway latency limit is 250 milliseconds per turn.",
            "metrics#3",
            "Gateway latency target verified at 250 ms",
            "250",
            0.50,
        ),
        (
            "Event stream cap is 12 thousand messages per partition.",
            "metrics#4",
            "Event stream cap configured to 2000 messages",
            "12",
            0.48,
        ),
        (
            "Daily traffic hit 2 crore requests with zero drops.",
            "metrics#5",
            "Daily traffic measured at 12 crore requests",
            "2",
            0.52,
        ),
    ]
    for idx, (spoken, src_id, ev_content, low_tok, low_conf) in enumerate(num_confusion_specs):
        ts_now = BASE_TS_MS + 90_000 + idx * 1000
        cid = f"seed_numeric_confusion_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="numeric_confusion",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_2",
                        text=spoken,
                        start_ms=0,
                        end_ms=4300,
                        confidence=0.80,
                        words=_make_words(spoken, 0, conf_map={low_tok: low_conf}, base_conf=0.94),
                    )
                ],
                evidence_store={
                    src_id: EvidenceRecord(
                        source_id=src_id,
                        source_type="doc",
                        content=ev_content,
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                    )
                },
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="numeric_confusion",
            )
        )

    # 11. hinglish_snippet (5 cases) — real hinglish audio_ref + grounded PR/metric -> PUBLISH
    hinglish_specs = [
        (
            "PR-482",
            (
                "Main staging environment verify kar lungi, PR four eight two "
                "merge kar dena testing ke baad."
            ),
            "PR-482 status=approved checks=green staging_verify=pending",
            ["pr_reference"],
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_18_clean.wav",
            148872,
            155280,
            "scenario_01_daily_standup_t18_clean",
        ),
        (
            "ENG-4521",
            "Logs check kar lo ek baar, ENG four five two one pe memory 85 percent dikha raha hai.",
            "ENG-4521 host_memory=85 percent container=telemetry-exporter",
            ["ticket_reference"],
            "ml/eval/audio/synthetic_eng/data/scenario_01_daily_standup/turn_09_clean.wav",
            76200,
            84576,
            "scenario_01_daily_standup_t09_clean",
        ),
        (
            "INC-409",
            "Host ne kernel OOM killer trigger kar diya for INC four zero nine.",
            "INC-409 root_cause=kernel_oom_killer pod=meetmind-gateway",
            ["incident_reference"],
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_07_clean.wav",
            60807,
            68367,
            "scenario_05_incident_review_t07_clean",
        ),
        (
            "INC-409",
            "Post-mortem documentation for INC four zero nine Confluence pe upload kar di hai.",
            "INC-409 doc_status=published_confluence",
            ["incident_reference"],
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_18_clean.wav",
            148847,
            155279,
            "scenario_05_incident_review_t18_clean",
        ),
        (
            "PLAT-101",
            "Donon checks pass ho gaye hain PLAT one zero one ke liye staging cluster par.",
            "PLAT-101 staging_checks=passed cluster=ap-south-1",
            ["ticket_reference"],
            None,
            0,
            0,
            None,
        ),
    ]
    for idx, (ref_id, spoken, ev_text, trigs, wav_path, s_ms, e_ms, seg_id) in enumerate(
        hinglish_specs
    ):
        ts_now = BASE_TS_MS + 100_000 + idx * 1000
        cid = f"seed_hinglish_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="hinglish_snippet",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_2",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms or 4500,
                        confidence=0.93,
                        language_tag="hinglish",
                        words=_make_words(spoken, s_ms, base_conf=0.93),
                    )
                ],
                evidence_store={
                    ref_id: EvidenceRecord(
                        source_id=ref_id,
                        source_type="incident"
                        if ref_id.startswith("INC")
                        else ("pull_request" if ref_id.startswith("PR") else "ticket"),
                        content=ev_text,
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                        topic_key=ref_id,
                    )
                },
                audio_ref=(
                    AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id)
                    if wav_path
                    else None
                ),
                expected_triggers=trigs,
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{ref_id}: {ev_text}",
                        source_id=ref_id,
                        value=ref_id,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # 12. restricted_secret (5 cases) — credential spoken aloud must NEVER display -> MUST ABSTAIN
    secret_specs = [
        "Hold on, set DEEPGRAM key for PR-482 to sk-live-987654321abcdef in staging.",
        "Temporary AWS token is AKIAIOSFODNN7EXAMPLE for PLAT-101 migration script.",
        "Use GitHub PAT ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456 to trigger workflow.",
        "The webhook header is Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9_fake_sig.",
        "Database password is supersecretvalue99 on ENG-789 test box.",
    ]
    for idx, spoken in enumerate(secret_specs):
        ts_now = BASE_TS_MS + 110_000 + idx * 1000
        cid = f"seed_restricted_secret_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="restricted_secret",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=0,
                        end_ms=4200,
                        confidence=0.96,
                        words=_make_words(spoken, 0, base_conf=0.96),
                    )
                ],
                evidence_store={
                    "PR-482": EvidenceRecord(
                        source_id="PR-482",
                        source_type="pull_request",
                        content="PR-482 staging workflow check",
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                        topic_key="PR-482",
                    )
                },
                expected_triggers=[],
                expected_claims=[],
                expected_abstain=True,
                expected_abstain_reason_kind="restricted_secret",
            )
        )

    # 13. incident_timeline (5 cases) — grounded incident timeline facts -> PUBLISH
    incident_specs = [
        (
            "INC-409",
            "We are holding the post-mortem for incident INC four zero nine.",
            "INC-409 severity=sev1 started_utc=14:32_UTC status=mitigated",
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_00_clean.wav",
            0,
            7032,
            "scenario_05_incident_review_t00_clean",
        ),
        (
            "INC-409",
            "At 14:32 UTC PagerDuty alerted on INC four zero nine gateway error rate spike.",
            "INC-409 timeline_alert=14:32_UTC metric=error_rate_spike",
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_01_clean.wav",
            7432,
            17128,
            "scenario_05_incident_review_t01_clean",
        ),
        (
            "INC-409",
            "The INC four zero nine degraded state lasted 18 minutes affecting 340 meetings.",
            "INC-409 duration=18_minutes impacted_meetings=340",
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_03_clean.wav",
            26496,
            34800,
            "scenario_05_incident_review_t03_clean",
        ),
        (
            "INC-409",
            "Wait time of 90 seconds happened during INC four zero nine Envoy dead routing.",
            "INC-409 timeline_dead_routing=90_seconds component=envoy_proxy",
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_10_clean.wav",
            83727,
            90831,
            "scenario_05_incident_review_t10_clean",
        ),
        (
            "INC-409",
            "Thank you team. INC four zero nine is closed with all 4 action items assigned.",
            "INC-409 status=closed action_items_count=4",
            "ml/eval/audio/synthetic_eng/data/scenario_05_incident_review/turn_19_clean.wav",
            155679,
            162951,
            "scenario_05_incident_review_t19_clean",
        ),
    ]
    for idx, (inc_id, spoken, ev_text, wav_path, s_ms, e_ms, seg_id) in enumerate(incident_specs):
        ts_now = BASE_TS_MS + 120_000 + idx * 1000
        cid = f"seed_incident_timeline_{idx + 1:02d}"
        cases.append(
            EvalCase(
                case_id=cid,
                case_type="incident_timeline",
                seed=seed,
                meeting_id=f"mtg_{cid}",
                tenant_id="acme_eng",
                current_ts_ms=ts_now,
                transcript_segments=[
                    TranscriptSegment(
                        segment_id=f"seg_{cid}",
                        speaker_id="spk_0",
                        text=spoken,
                        start_ms=s_ms,
                        end_ms=e_ms,
                        confidence=0.96,
                        words=_make_words(spoken, s_ms, base_conf=0.96),
                    )
                ],
                evidence_store={
                    inc_id: EvidenceRecord(
                        source_id=inc_id,
                        source_type="incident",
                        content=ev_text,
                        uri=f"https://incidents.internal/{inc_id}",
                        retrieved_at=ts_now - 10_000,
                        as_of=ts_now - 10_000,
                        topic_key=inc_id,
                    )
                },
                audio_ref=AudioRef(path=wav_path, start_ms=s_ms, end_ms=e_ms, segment_id=seg_id),
                expected_triggers=["incident_reference"],
                expected_claims=[
                    ExpectedClaimSpec(
                        claim_type="identifier",
                        text=f"{inc_id}: {ev_text}",
                        source_id=inc_id,
                        value=inc_id,
                    )
                ],
                expected_abstain=False,
                expected_confidence_band="high",
            )
        )

    # Touch rng once to guarantee deterministic ordering stamp if extended
    _ = rng.random()
    return cases


if __name__ == "__main__":
    generated = build_seed_cases(DEFAULT_SEED)
    dump_cases_jsonl(generated, DEFAULT_SEED_PATH)
    print(
        f"Wrote {len(generated)} deterministic seed cases "
        f"(seed={DEFAULT_SEED}) to {DEFAULT_SEED_PATH}"
    )
