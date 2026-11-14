# MeetMind AI — Golden Evaluation Corpus (`ml/eval/golden/`)

> **Q-0 Q6 & Addendum Section D Compliance**  
> **Seed split (`seed_engineering.jsonl`) is NON-AUTHORITATIVE.** Provisional quality gates govern release readiness on the **external held-out split (`EVAL_HELDOUT_PATH`)** stored outside this repository.

## Files

- `seed_engineering.jsonl`: Deterministic 65-case engineering seed corpus (`seed = 20261112`) spanning all 13 engineering quality scenarios (ticket status, PR mention, action item with owner & due date, decision with dissent, blocker, no evidence, conflicting evidence, stale evidence, identifier confusion, numeric confusion, Hinglish snippet, restricted credential/secret spoken aloud, incident timeline). A subset carries verified `audio_ref` pointers into `ml/eval/audio/synthetic_eng/`.
- `generate_seed.py`: Reproducible builder for `seed_engineering.jsonl` using fixed seed `20261112`.
- `partner_template.jsonl`: Anonymised schema template for design-partner real call evaluation cases.

## Design-Partner Anonymisation Checklist

Before adding design-partner transcripts or audio clips to the external held-out dataset (`EVAL_HELDOUT_PATH`):

1. **Direct Identifiers**: Replace all real person names, emails, customer company names, and Slack handles with deterministic placeholders (`[PARTICIPANT_A]`, `[CUSTOMER_ORG]`). Never commit partner names or voices into git.
2. **Credentials & Secrets**: Any spoken API keys, tokens, passwords, or private hosts must stay marked `expected_abstain: true` and must be stripped or syntheticised in `partner_template.jsonl` derivatives if not testing redaction.
3. **Repo / Tenant URI Scrubbing**: Normalize internal URLs to non-resolvable synthetic domains unless stored strictly in the owner's encrypted external held-out folder.
4. **No Source Code**: Consistent with the Engineering Domain Pack invariant (`docs/spec-amendments/engineering-pack.md`), never place proprietary source code snippets inside `evidence_store` spans or transcript gold annotations.

## Running the Harness & `EVAL_HELDOUT_PATH`

The held-out evaluation file lives **outside** this workspace (`EVAL_HELDOUT_PATH=/absolute/path/to/heldout_engineering.jsonl`) so coding agents cannot inspect or overfit to held-out cases.

```powershell
# Run baseline calibration suite on seed (prints DATA ONLY; heldout reported NOT PROVIDED when unset)
uv run python scripts/eval_quality.py --all-baselines

# Owner run with external held-out file:
$env:EVAL_HELDOUT_PATH="D:\secure\meetmind_heldout\engineering_v1.jsonl"
uv run python scripts/eval_quality.py --pipeline RetrievalOnly
```

## Statistical Honesty Rules (Addendum Section D)

- Every reported proportion prints `N` alongside exact one-sided 95% **Clopper-Pearson** confidence bounds (`cp_95_upper` for failure/hallucination rates, `cp_95_lower` for precision/abstention rates).
- For `N = 60` and `0` failures, the one-sided 95% upper bound is `4.87%` (not `0%` risk).
- Any metric with `N < min_cases_for_claim` (default `300`) is automatically stamped **`INSUFFICIENT N`**.
