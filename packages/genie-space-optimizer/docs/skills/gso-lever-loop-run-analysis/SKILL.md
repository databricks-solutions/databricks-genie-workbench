---
name: gso-lever-loop-run-analysis
description: Use when analyzing, validating, debugging, or postmortem-ing a Genie Space Optimizer lever-loop Databricks Job run by job_id + run_id. Covers Phase A journey replay, Phase B/C decision-trace and RCA-groundedness checks, Phase D scoreboard and bucketing checks, Phase E pilot-run merge-readiness validation (zero validator warnings, raise_on_violation flip, accuracy non-regression vs variance baseline, decision-trace hard-gate, sanity-broken PR), MLflow trace/artifact inspection, and postmortem generation.
---

# GSO Lever Loop Run Analysis

> **Canonical example.** Run
> `0ade1a99-9406-4a68-a3bc-8c77be78edcb` is the canonical example
> of a `MERGE_GATE_GAP` produced by stacked
> `ACCEPTANCE_TARGET_BLIND` + `PATCH_CAP_RCA_BLIND_RANKING` +
> `BLAST_RADIUS_OVERDROP_ON_NONSEMANTIC`. The fixture's analysis
> is preserved at `docs/runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md`
> for pattern matching against new runs.

Use this skill when asked to analyze, validate, debug, or postmortem a Genie Space Optimizer lever-loop Databricks Job run, including the Phase E real-run pilot that gates the burn-down-to-merge roadmap.

## Required Inputs

- `job_id`: Databricks Job ID.
- `run_id`: Databricks parent Job Run ID.

## Optional Inputs

- `profile`: Databricks CLI profile. If omitted, ask for the profile unless the workspace is already obvious from context.
- `task_key`: Databricks task key. Default: `lever_loop`.
- `experiment_id`: MLflow experiment ID. If omitted, resolve it from run parameters, stdout markers, MLflow tags, or job output.
- `output_dir`: Directory for reports. Default: `packages/genie-space-optimizer/docs/runid_analysis`.
- `iteration`: Iteration number to focus on. If omitted, analyze all iterations.
- `phase`: One of `A`, `B`, `C`, `D`, `E`, or `all` (default `all`). When set to `E`, the Phase E Merge-Readiness Health checklist runs in addition to the per-iteration checklists. When set to a single earlier phase, focus the report on that phase's checks.
- `baseline_run_id`: Required when `phase=E`. The Phase A variance-baseline run ID used for the accuracy non-regression check. If omitted with `phase=E`, ask the user.
- `repo_root`: Path to the repo for codebase-state checks (Phase E only). Default: workspace root. Used to verify `raise_on_violation`, the decision-trace hard-gate replay test, and the sanity-broken PR commit/CI artifact.
- `bundle_dir`: Pre-built evidence bundle root directory. If supplied, the skill skips Step 0 and reads `bundle_dir/evidence/manifest.json` directly. If omitted, the skill runs `evidence_bundle` itself.
- `auto_backfill`: When `true`, instructs the bundle invocation to run `mlflow_backfill` automatically if decision-trail artifacts are missing on the anchor run. Default: `false` (operator approves before any writes).

## Required Related Skills

Use these skills as needed:

- `databricks-jobs` for Databricks Jobs CLI/API inspection.
- `retrieving-mlflow-traces` for MLflow trace search/fetch.
- `analyze-mlflow-trace` for detailed trace analysis.
- `querying-mlflow-metrics` for metrics and aggregate trace/run analysis.
- `systematic-debugging` for root-cause workflow.
- `gso-replay-cycle-intake` — peer skill; the **write-side** counterpart. This skill is read-only; when the user asks to "advance the burn-down", "intake the cycle", "promote the fixture", or to act on the analysis output, hand off there. See "Cross-Skill Hand-offs" below.

## Operating Principle

Do not guess. Follow systematic debugging:

1. Gather evidence from Databricks Jobs state, task output, stdout markers, MLflow runs/artifacts/traces, replay fixture, and decision validation.
2. Identify where the failure occurred: infrastructure, input handoff, eval, RCA, strategist, proposal, gate, applier, acceptance, Phase B trace persistence, convergence, or reporting.
3. State one root-cause hypothesis at a time.
4. Recommend the smallest next diagnostic or code action.

For Phase E specifically, distinguish between three failure surfaces — the recommended next action depends on which one tripped:

- **Pilot-run gap** (rerun candidate): a defect surfaced during the real run itself (validator warning, missing decision record, transcript section empty on a real iteration, scoreboard nonsense, bucket misclassification). Rerun is on the table only after the underlying defect is fixed; a rerun without a fix repeats the same failure.
- **Merge-gate gap** (code/test work): the codebase has not yet flipped `raise_on_violation`, lacks the decision-trace hard-gate replay test, or has no sanity-broken PR CI evidence. Independent of any pilot run; does not need a rerun.
- **Baseline regression** (rollback or rescope): the pilot completed cleanly but accuracy regressed against the Phase A variance baseline. Investigate the regressing iteration's decision trace; consider rolling back the offending PR or rescoping Phase E.

## Analysis Workflow

**0. Acquire the evidence bundle (preferred path).** If `bundle_dir` is not supplied:

   ```bash
   python -m genie_space_optimizer.tools.evidence_bundle \
       --job-id <job_id> --run-id <run_id> --profile <profile> \
       --output-dir packages/genie-space-optimizer/docs/runid_analysis \
       [--auto-backfill]
   ```

   The bundle is idempotent — re-running is cheap. The CLI prints `manifest.json` to stdout and writes the canonical evidence layout to `<output-dir>/<opt_run_id>/evidence/`.

   **Triage the manifest first.** Read `<bundle_dir>/evidence/manifest.json`. Every subsequent step reads from `manifest.artifacts_pulled` paths or directly from disk, never via live CLI:
   - Phase A: `evidence/mlflow/<anchor>/phase_a/journey_validation/iter_*.json`
   - Phase B: `evidence/mlflow/<anchor>/phase_b/decision_trace/iter_*.json` and `phase_b/operator_transcript/iter_*.txt`
   - Markers: `evidence/markers.json`
   - Job state: `evidence/job_run.json`
   - Stdout/stderr: `evidence/lever_loop_stdout.txt`, `evidence/lever_loop_stderr.txt`
   - **Notebook output**: `evidence/lever_loop_notebook_output.json` *(fall back to this whenever `lever_loop_stdout.txt` is empty — the lever-loop is a notebook task in production and `databricks jobs get-run-output` returns empty `logs` for notebook tasks; the structured per-iteration `phase_b` summary lives in `notebook_output.result`)*
   - Replay fixture: `evidence/replay_fixture.json`
   - MLflow audit: `evidence/mlflow_audit.{md,json}`

   If the bundle does not yet capture `lever_loop_notebook_output.json` (older bundle versions don't), pull it manually before walking the checklist:

   ```bash
   databricks jobs get-run-output <lever_task_run_id> --profile <profile> --output json \
     > <bundle_dir>/evidence/lever_loop_notebook_output.json
   ```

   Then parse `notebook_output.result` (a JSON-encoded string) for the canonical lever-loop result dict — `iteration_counter`, `levers_attempted`, `levers_accepted`, `levers_rolled_back`, and the `phase_b` summary block (`decision_records_total`, `iter_record_counts`, `iter_violation_counts`, `no_records_iterations`, `artifact_paths`, `producer_exceptions`, `target_qids_missing_count`, `total_violations`). This block is the de-facto "stdout" for any run whose harness predates the `GSO_*_V1` markers.

   - If `manifest.exit_status == "incomplete"`, list every entry in `manifest.missing_pieces`. Decide whether each gap is blocking analysis (the postmortem cannot answer the operator's question without it) or merely informational.
   - If a `PHASE_*_ARTIFACT_MISSING_ON_ANCHOR` gap is blocking and `replay_fixture` is present, recommend rerunning the bundle with `--auto-backfill`.

   **Trace fetch decision rule.** Invoke `trace_fetcher` only if **all** of the following hold:
   - The current root-cause hypothesis cannot be confirmed or refuted from `evidence/markers.json` + `phase_a/` + `phase_b/` artifacts alone.
   - `manifest.trace_fetch_recommendations` contains at least one entry whose `reason` matches the open hypothesis (e.g., `UNRESOLVED_REASON_CODE` for opaque abandons, `INCOMPLETE_DECISION_TRACE` for journey-violation iterations).
   - The trace ids requested are bounded (≤10 traces; reject if recommendations imply more — split into a follow-up).

   When the rule fires, run:

   ```bash
   python -m genie_space_optimizer.tools.trace_fetcher \
       --bundle-dir <bundle_dir> --from-recommendations
   ```

   Then re-read `manifest.json` (the trace fetcher updates `artifacts_pulled.traces`) and continue the checklist with the new evidence in `evidence/traces/<trace_id>.json`. **Do not pull traces speculatively.** If logs and decision-trail artifacts already explain the failure, traces are noise.

   When writing the postmortem at `<output-dir>/<opt_run_id>/postmortem.md`, cite specific bundle files (e.g., `evidence/mlflow/mr-1/phase_b/decision_trace/iter_04.json:23`) so future readers can re-verify each claim from the same on-disk evidence.

   The legacy live-CLI workflow below (Steps 1–10) is the **fallback path** when the bundle is unavailable (e.g., `databricks` CLI access is broken or `evidence_bundle` was not run). When using the bundle, Steps 1–10 still describe the *checks* the analysis should make; just substitute "read from bundle file at X" for "run CLI command X".

---

1. Validate Databricks CLI auth:

   ```bash
   databricks auth profiles
   ```

2. Fetch parent job run:

   ```bash
   databricks jobs get-run --run-id <run_id> --profile <profile> --output json
   ```

3. Locate the task run whose `task_key` equals `lever_loop` unless the caller supplied another task key.

4. Fetch task output:

   ```bash
   databricks jobs get-run-output --run-id <task_run_id> --profile <profile> --output json
   ```

5. Parse stable stdout markers when present:

   - `GSO_RUN_MANIFEST_V1`
   - `GSO_ITERATION_SUMMARY_V1`
   - `GSO_PHASE_B_V1`
   - `GSO_CONVERGENCE_V1`
   - existing replay markers:
     - `===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===`
     - `===PHASE_A_REPLAY_FIXTURE_JSON_END===`

6. Resolve MLflow experiment ID in this order:

   1. Explicit `experiment_id` input.
   2. `GSO_RUN_MANIFEST_V1.mlflow_experiment_id`.
   3. Job/run parameters named `MLFLOW_EXPERIMENT_ID`, `mlflow_experiment_id`, or `experiment_id`.
   4. MLflow tags containing `genie.run_id`, `genie.databricks.parent_run_id`, or `genie.databricks.lever_loop_task_run_id`.
   5. Ask the user for the experiment ID if it cannot be resolved.

7. Search MLflow for related runs/traces. Use the MLflow skills for exact CLI syntax available in the environment. Prefer writing large trace output to files before parsing.

8. Evaluate the run using the checklist below.

9. Write a markdown postmortem:

   ```text
   packages/genie-space-optimizer/docs/runid_analysis/<job_id>_<run_id>_analysis.md
   ```

10. Write a JSON sidecar:

   ```text
   packages/genie-space-optimizer/docs/runid_analysis/<job_id>_<run_id>_analysis.json
   ```

## Evaluation Checklist

### Databricks Jobs Health

- Parent run exists.
- Parent run terminal state is recorded.
- `lever_loop` task run exists.
- `lever_loop` task terminal state is recorded.
- Retries/attempts are listed.
- Task output was retrieved or marked truncated/unavailable.

### Phase A Journey And Replay Health

- Replay fixture marker is present or MLflow fixture artifact is present.
- Each populated iteration has `journey_validation`.
- Journey validation violation count is reported.
- `test_run_replay_airline_real_v1_within_burndown_budget` relevance is noted when the real fixture is refreshed.

### Phase B Trace And Transcript Health

- `GSO_PHASE_B_V1` marker exists per iteration or degraded mode is declared.
- `decision_records` are present in the replay fixture or Phase B artifacts.
- `phase_b/decision_trace/iter_<N>.json` exists or missing path is reported.
- `phase_b/operator_transcript/iter_<N>.txt` exists or missing path is reported.
- Transcript contains the fixed sections:
  - `Iteration Summary`
  - `Hard Failures And QID State`
  - `RCA Cards With Evidence`
  - `AG Decisions And Rationale`
  - `Proposal Survival And Gate Drops`
  - `Applied Patches And Acceptance`
  - `Observed Results And Regressions`
  - `Unresolved QID Buckets`
  - `Next Suggested Action`
- Decision validation count is zero or listed.

When `lever_loop_notebook_output.json` is available, run these two cross-checks before concluding:

- **Producer-gap check.** Compare `notebook_output.phase_b.iter_record_counts` length against `notebook_output.result.iteration_counter`. If `len(iter_record_counts) < iteration_counter` AND the missing iters are not enumerated in `no_records_iterations`, classify as `PHASE_B_TRACE_GAP` — producer side. Iters that ran but emitted no records and aren't typed as "no records" are silently broken; the persistence layer can't help.
- **Persistence-claim check.** For every path in `notebook_output.phase_b.artifact_paths`, verify it actually exists on at least one MLflow run in the experiment (recursive `MlflowClient.list_artifacts` over every sibling). When a path is *claimed* by the result block but *absent* on every run, classify as `PHASE_B_PERSIST_SILENT_FAILURE` — the harness's exception-suppressed Phase B persistence path swallowed a real error. Recommend deploying `genie.phase_b.partial=true` tagging (committed in the `fix/gso-lossless-contract-replay-gate` branch) so the next run surfaces the failure rather than burying it.

### RCA-Groundedness Health

For each sampled or failing decision record, check:

- `evidence_refs`
- `rca_id`
- `root_cause`
- `target_qids`
- `expected_effect`
- `observed_effect`
- `reason_code`
- `next_action`
- `regression_qids` when regressions or rollback debt exist

If any field is missing, classify the run as `DEGRADED_TRACE_CONTRACT` unless the decision has a typed reason explaining why the field is absent.

### Optimizer Outcome Health

- Iteration count.
- Accepted AGs.
- Rolled-back AGs.
- Skipped AGs.
- Gate drops by reason.
- Applier skips/rejections by reason.
- Acceptance/rollback reason.
- Accuracy delta when present.
- Convergence reason.
- Unresolved qid buckets and next suggested action.

### Lever-Loop Mechanics Health

Run this section every analysis. It surfaces failure modes that look
like generic "MERGE_GATE_GAP" in the verdict but actually point at a
specific lever-loop policy. The signals come from
`tools.lever_loop_stdout_parser.parse_lever_loop_stdout(...)`; if the
bundle's `gso_postmortem_bundle/` is present, the same signals come
from the typed per-stage capture instead of stdout grepping.

- **Acceptance target-blindness check.** For each accepted AG, assert
  `target_fixed_qids ≠ ∅` OR `thresholds_met=True`. Any acceptance
  with `target_fixed_qids=()` and unmet thresholds → emit
  `ACCEPTANCE_TARGET_BLIND` finding.
- **No-causal-applyable detection.** For each AG, count proposals
  with `rca_id=parent_ag.rca_id` that were dropped by `blast_radius`,
  `rca_groundedness`, or applyability gates. If non-zero AND any
  non-RCA proposal was applied → emit
  `CAUSAL_PATCH_BLOCKED_NONCAUSAL_APPLIED`.
- **`patch_cap` RCA-blindness.** Inspect each iteration's
  `PATCH SURVIVAL` block (or `06_safety_gates/output.json` under
  Phase H). If a proposal with `rca_id≠None` was dropped at
  `patch_cap` (`reason=lower_causal_rank`) while a sibling with
  `rca=None` was selected (`patch_cap_selected`) → emit
  `PATCH_CAP_RCA_BLIND_RANKING`.
- **Blast-radius lever distribution.** Group `blast_radius` drops by
  `patch_type`. If non-semantic levers
  (`update_column_description`, `add_column_synonym`,
  `add_metric_view_instruction`, `add_table_instruction`,
  `update_table_description`) appear in the drop set → emit
  `BLAST_RADIUS_OVERDROP_ON_NONSEMANTIC`.
- **Strategist coverage.** Count strategist-emitted AGs per iteration
  vs `len(hard_clusters)`. If less than one AG per cluster → emit
  `STRATEGIST_SINGLE_AG_GAP`.
- **Diagnostic-AG fallback rate.** Count proposals from AGs whose
  `rca_id=None`. If high (>0 in any iteration) → emit
  `DIAGNOSTIC_AG_RCA_INHERITANCE_GAP`.
- **Incidental resolution detection.** For each iteration where
  global accuracy improved, list QIDs that flipped from fail to
  pass. Compare to that iteration's `target_fixed_qids`. Any flip
  not in `target_fixed_qids` is annotated as `INCIDENTAL_RESOLUTION`
  (informational; this is the trigger for the
  `ACCEPTANCE_TARGET_BLIND` rule above).
- **Instruction propagation completeness.** Where applied
  `patch_type ∈ {add_*_instruction, add_example_sql}`, verify the
  next iteration's Genie SQL for the targeted QID reflects the
  instruction. Mismatch → emit `INSTRUCTION_NOT_HONORED_BY_GENIE`
  (informational; not optimizer-fixable).

### MLflow Trace Health

- Experiment ID resolved.
- Related MLflow runs found.
- Related traces found or explicitly unavailable.
- Error traces summarized.
- Assessment/scorer errors separated from application errors.
- Long/slow traces listed when latency is relevant.

### Phase E Merge-Readiness Health

Run this section only when `phase=E` or `phase=all` AND the run under review is positioned as the Phase E pilot. The checklist has two halves: pilot-run validation (depends on this run's outputs) and merge-gate state (depends on the codebase, independent of any single run).

#### Pilot-run validation (this run's outputs)

Each item maps to one bullet from roadmap lines 360-367:

- **Zero validator warnings.** Sum journey-contract violations + decision-trace validation count across all iterations. Pass requires `0`. Any non-zero is a `PHASE_E_VALIDATOR_WARNINGS_PRESENT` blocker.
- **Decision trace completeness.** For every iteration, `phase_b/decision_trace/iter_<N>.json` exists, contains records of all 10 `DecisionType` values that had at least one corresponding event, and the cross-projection check holds.
- **Operator transcript diagnosability.** A failed iteration's transcript must let an operator name the failure surface (which gate, which proposal, which qid, which bucket) without grepping `harness.py` or raw stdout. Spot-check by picking one rolled-back or unresolved iteration and reading only its transcript: can you name (a) the AG that rolled back, (b) the gate that dropped, (c) the unresolved bucket and next action? Three yes = pass.
- **Scoreboard sensibility.** `journey_completeness_pct`, `decision_trace_completeness_pct`, and `rca_loop_closure_pct` are all in `[0.0, 1.0]`. `accuracy_delta` is finite and matches the iteration's eval delta. `dominant_signal` resolves to one of the seven defined rungs (HEALTHY / RCA_GAP / TARGETING_GAP / etc.) — no `UNKNOWN` or empty string.
- **Bucket interpretability.** Pick 3-5 unresolved qids from the final iteration's `Unresolved QID Buckets` section. For each, read the per-qid evidence (linked decision records via `evidence_record_ids`) and confirm the assigned `FailureBucket` matches the actual earliest broken link in the RCA invariant chain. Misclassification on more than one qid out of five is a `PHASE_E_BUCKET_INTERPRETABILITY_FAIL`.
- **RCA loop state coverage.** Every unresolved qid in the final iteration has a `ClassificationResult` with `bucket != None` and `evidence_record_ids` non-empty. Sentinel-only is acceptable for resolved qids; unresolved qids without evidence is a gap.
- **Accuracy non-regression vs Phase A baseline.** Required input: `baseline_run_id`. Pull the variance-baseline arbiter accuracy from `baseline_run_id`'s final iteration; compare to this pilot's final-iteration arbiter accuracy. Non-regression means within the variance band (typically ±2 percentage points unless a tighter band is set in `expected_canonical_decisions`). A regression beyond the band is `PHASE_E_ACCURACY_REGRESSION` and is **not** a rerun candidate without a code investigation.

#### Merge-gate state (codebase, independent of this run)

These four items are read from `repo_root`, not from the run output:

- **`raise_on_violation` flipped.** Confirm `_validate_journeys_at_iteration_end` is called with `raise_on_violation=True` in `harness.py`. Today's call site (around line 17218 pre-Phase E) passes `False`. Pass requires the flip. If still `False`, this is `PHASE_E_MERGE_GATE_NOT_WIRED`.
- **Decision-trace hard-gate replay test exists.** A test in `tests/replay/` must fail closed when a required `DecisionRecord` is missing — distinct from the cross-projection structure test which validates layout. Search for tests that assert presence-and-completeness of canonical record types (`PATCH_APPLIED`, `RCA_FORMED`, `ACCEPTANCE_DECIDED`, `QID_RESOLUTION`) and would CI-fail if any went missing on a synthetic gap fixture. Absence of such a test is `PHASE_E_MERGE_GATE_NOT_WIRED`.
- **Sanity-broken PR CI evidence captured.** A closed PR (or saved CI run link) must exist proving CI failed loudly when one `_emit_ag_outcome_journey` call or one required decision record was dropped. Without this evidence, the gates are unproven even if they exist on paper.
- **No partially-shipped Phase D plans.** All tasks across the three Phase D plans (`2026-05-04-operator-scoreboard-plan.md`, `2026-05-04-failure-bucketing-classifier-plan.md`, `2026-05-04-harness-extractions-phase-1-plan.md`) are landed against their respective task lists. Partial completion is a soft block — the pilot can run, but merge cannot.
- **Decision-trail artifact integrity (E.0).** Run `python -m genie_space_optimizer.tools.mlflow_audit --opt-run-id <id>` and confirm `phase_a/journey_validation/`, `phase_b/decision_trace/`, and `phase_b/operator_transcript/` are present on the lever_loop anchor run for every iteration. If the audit reports any prefix missing on the anchor, this is `PHASE_E0_DECISION_TRAIL_MISSING` — see Degraded Analysis Rules for routing.

#### Phase E verdict

After completing both halves, classify the run as one of:

- `READY_TO_MERGE` — pilot-run validation all green, merge-gate state all four items pass.
- `PILOT_NEEDS_RERUN` — a pilot-run defect surfaced; underlying code fix required before rerun. Name the fix.
- `MERGE_GATE_GAP` — pilot-run validation passed but one or more merge-gate items unfinished. Pilot does not need to rerun.
- `BASELINE_REGRESSION` — pilot completed but accuracy regressed beyond the variance band.
- `BLOCKED` — multiple of the above; list each.

## Failure Taxonomy

Classify the primary failure as one of:

- `DATABRICKS_JOB_FAILURE`
- `INPUT_HANDOFF_FAILURE`
- `BASELINE_EVAL_FAILURE`
- `RCA_EVIDENCE_GAP`
- `RCA_GROUNDING_GAP`
- `STRATEGIST_GAP`
- `PROPOSAL_GAP`
- `TARGETING_GAP`
- `GATE_OR_CAP_GAP`
- `APPLIER_FAILURE`
- `ROLLBACK_OR_ACCEPTANCE_GAP`
- `PHASE_B_TRACE_GAP` — at least one of:
  - **producer-gap subkind**: `len(iter_record_counts) < iteration_counter` and the missing iters are not in `no_records_iterations` (records were never emitted, never typed).
  - **persistence-silent-failure subkind**: `phase_b.artifact_paths` claims artifacts that don't exist on any MLflow run (the persistence path's `try/except: logger.debug` block swallowed a real error). Pre-deployment of `genie.phase_b.partial` tagging, this is invisible from MLflow alone.
- `MLFLOW_ARTIFACT_GAP`
- `CONVERGENCE_OR_PLATEAU_GAP`
- `MODEL_CEILING`
- `UNKNOWN_NEEDS_MORE_EVIDENCE`
- `ACCEPTANCE_TARGET_BLIND` — an AG was accepted via the
  attribution-drift branch (`accepted_with_attribution_drift`) while
  thresholds were unmet and the named target qid stayed hard.
  Resolved by enabling `GSO_TARGET_AWARE_ACCEPTANCE`.
- `CAUSAL_PATCH_BLOCKED_NONCAUSAL_APPLIED` — every RCA-matched
  proposal in an AG was dropped upstream while non-causal proposals
  were applied. Resolved by enabling `GSO_NO_CAUSAL_APPLYABLE_HALT`.
- `PATCH_CAP_RCA_BLIND_RANKING` — `patch_cap` selected a proposal
  with `rca_id=None` over a sibling with `rca_id≠None` at equal
  relevance. Resolved by enabling `GSO_RCA_AWARE_PATCH_CAP`.
- `BLAST_RADIUS_OVERDROP_ON_NONSEMANTIC` — non-semantic patches
  (column descriptions, synonyms, instructions) dropped at the
  blast-radius gate. Resolved by enabling
  `GSO_LEVER_AWARE_BLAST_RADIUS`.
- `STRATEGIST_SINGLE_AG_GAP` — strategist emitted one AG when
  multiple hard clusters needed coverage. Tracked but not
  flag-resolved; addressed by Tier-3 strategist multi-AG work.
- `DIAGNOSTIC_AG_RCA_INHERITANCE_GAP` — diagnostic AG materialized
  for a known cluster did not inherit the cluster's `rca_id`,
  causing rca_groundedness drops. Resolved by Task F of the
  optimizer plan.
- `INCIDENTAL_RESOLUTION` — informational; a QID flipped from fail
  to pass without being in any AG's `target_qids`. By itself benign;
  becomes diagnostic when combined with `ACCEPTANCE_TARGET_BLIND`.
- `INSTRUCTION_NOT_HONORED_BY_GENIE` — informational; Genie SQL did
  not reflect an applied instruction patch. Not optimizer-fixable;
  surfaces a Genie-side issue.

### Phase E specific (use only when `phase=E`)

- `PHASE_E_VALIDATOR_WARNINGS_PRESENT` — one or more journey or decision-trace validations emitted on the pilot run. Blocks merge.
- `PHASE_E_ACCURACY_REGRESSION` — pilot final-iteration accuracy fell below the Phase A variance band vs `baseline_run_id`. Blocks merge; not a rerun candidate without code investigation.
- `PHASE_E_TRANSCRIPT_NOT_DIAGNOSTIC` — operator transcript renders the eight sections but one or more is empty on an iteration that should have had records, or required fields are missing such that an operator cannot diagnose without log-grep.
- `PHASE_E_SCOREBOARD_NONSENSICAL` — scoreboard metric out of `[0.0, 1.0]`, infinite/NaN delta, or `dominant_signal` unresolved.
- `PHASE_E_BUCKET_INTERPRETABILITY_FAIL` — more than one of five spot-checked unresolved qids was bucketed to a downstream link instead of the earliest broken link in the RCA invariant chain.
- `PHASE_E_MERGE_GATE_NOT_WIRED` — `raise_on_violation` still `False`, decision-trace hard-gate replay test absent, or sanity-broken PR CI evidence missing. Independent of any pilot run.
- `PHASE_E_PARTIAL_PHASE_D` — at least one task across the three Phase D plans is unlanded. Soft block on merge; pilot may proceed.

## Report Format

Every report must contain:

```markdown
# GSO Lever Loop Run Analysis: <job_id>/<run_id>

## Metadata

## Executive Summary

## Evidence Collected

## Databricks Job And Task State

## Phase A Journey And Replay Health

## Phase B Trace And Transcript Health

## RCA-Groundedness Health

## Optimizer Outcome

## MLflow Runs And Traces

## Root Cause Hypothesis

## Recommended Next Actions

## Evidence Appendix
```

When `phase=E` (or `phase=all` for a Phase E pilot), the report must also contain:

```markdown
## Phase E Merge-Readiness Health

### Pilot-Run Validation

| Check | Result | Evidence |
|---|---|---|
| Zero validator warnings | PASS / FAIL (count) | (per-iteration count breakdown) |
| Decision trace completeness | PASS / FAIL | (artifact paths + missing types) |
| Transcript diagnosability spot-check | PASS / FAIL | (iteration sampled + 3 named answers) |
| Scoreboard sensibility | PASS / FAIL | (out-of-range metrics, if any) |
| Bucket interpretability spot-check | PASS / FAIL | (qids sampled + correctness) |
| RCA loop state coverage | PASS / FAIL | (unresolved qids missing evidence) |
| Accuracy non-regression vs baseline | PASS / FAIL (delta) | (baseline_run_id + variance band) |

### Merge-Gate State

| Check | Result | Evidence |
|---|---|---|
| `raise_on_violation=True` flipped | PASS / FAIL | (harness.py line + commit) |
| Decision-trace hard-gate replay test exists | PASS / FAIL | (test file path + assertion type) |
| Sanity-broken PR CI evidence captured | PASS / FAIL | (PR URL + CI failure link) |
| Phase D plans fully landed | PASS / FAIL | (per-plan task tally) |

### Phase E Verdict

One of `READY_TO_MERGE`, `PILOT_NEEDS_RERUN`, `MERGE_GATE_GAP`, `BASELINE_REGRESSION`, or `BLOCKED` with one paragraph naming the smallest concrete next action.
```

## Degraded Analysis Rules

If task output is truncated, say so and rely on MLflow artifacts and markers.

If MLflow experiment ID cannot be resolved, write the report with `MLFLOW_EXPERIMENT_UNRESOLVED` and ask the user for the experiment ID. Try this resolution chain first before asking:
1. Explicit `experiment_id` input.
2. `GSO_RUN_MANIFEST_V1.mlflow_experiment_id` from markers.
3. `MLFLOW_EXPERIMENT_ID`/`mlflow_experiment_id`/`experiment_id` in `job_run.job_parameters`.
4. `experiment_name` in `job_run.job_parameters` → resolve via `MlflowClient.get_experiment_by_name(name).experiment_id`.
5. Ask the operator.

If `GSO_*_V1` markers are missing AND `lever_loop_stdout.txt` is empty, the lever-loop is a notebook task and stdout is not a capture surface. Pull `lever_loop_notebook_output.json` (see Step 0) and read `notebook_output.result.phase_b` instead. Do not declare missing markers a blocker on its own — the structured result is the substitute.

If the audit anchor resolved to `enrichment_snapshot` (or any non-`lever_loop` `genie.run_type`) AND `mlflow_audit.sibling_runs[*].artifact_paths` are empty for `phase_a/`/`phase_b/` prefixes, the deployed harness is not tagging any run with `genie.run_type=lever_loop`. The audit cannot find the canonical anchor. Recover by querying MLflow directly:

```python
from mlflow.tracking import MlflowClient
client = MlflowClient()
exp = client.get_experiment_by_name("<experiment_name from job_parameters>")
runs = client.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string=f"tags.`genie.run_id` = '{opt_run_id}'",  # NOT genie.optimization_run_id
    max_results=200,
)
```

Then inspect each run's tags for `genie.stage=full_eval` and `genie.iteration=<N>` — those are the per-iteration anchors. List artifacts on each. If `phase_b/decision_trace/iter_<N>.json` and `phase_b/operator_transcript/iter_<N>.txt` are absent across **every** iteration run, you have confirmed the persistence-silent-failure subkind of `PHASE_B_TRACE_GAP`.

If evidence conflicts, report the conflict rather than choosing one source silently. Specifically: when `notebook_output.phase_b.artifact_paths` lists a path that does not exist on any MLflow run, surface the conflict in the postmortem's "Evidence Collected" section — do not silently treat one as authoritative.

For Phase E specifically:

- If `baseline_run_id` is unavailable when `phase=E`, write the report with `PHASE_E_BASELINE_UNRESOLVED` in the accuracy non-regression row and ask the user. Do not fabricate a baseline from this run's iteration 0.
- If the pilot did not complete (truncation, infrastructure failure, run cancelled mid-iteration), classify the run with the appropriate non-Phase-E label (`DATABRICKS_JOB_FAILURE` etc.) and explicitly state that Phase E validation cannot proceed until a complete pilot run exists.
- If the parent job run is `RUNNING` but the `lever_loop` task is `TERMINATED/SUCCESS` (i.e., `finalize` and `deploy` are still in progress), proceed with the Phase E checklist for the lever loop. Mark `Metadata` with `finalize_state`/`deploy_state` as `pending` and exclude finalize-only outputs (UC champion promotion, repeatability re-run results) from the verdict — they will need a separate follow-up postmortem once the parent terminates.
- If merge-gate state checks find `repo_root` unset or inaccessible, mark each merge-gate row `UNVERIFIED` and ask the user to confirm or rerun with `repo_root` set.
- If pilot-run validation passes but merge-gate state shows multiple gaps, do not classify as `READY_TO_MERGE` — use `MERGE_GATE_GAP` and list the missing items in priority order.
- If the `mlflow_audit` CLI reports decision-trail artifacts missing on the lever_loop anchor for any iteration, classify the run as `MERGE_GATE_GAP` with the label `PHASE_E0_DECISION_TRAIL_MISSING`, and recommend running `python -m genie_space_optimizer.tools.mlflow_backfill --opt-run-id <id> --replay-fixture <path>` followed by a fresh audit pass before re-attempting the Phase E pilot.
- **Disambiguating `MERGE_GATE_GAP` vs `PILOT_NEEDS_RERUN` when decision-trail artifacts are absent across the entire experiment.** This is the most common confusion. Use this routing:
  - The lever loop terminated cleanly (TERMINATED/SUCCESS) AND `notebook_output.phase_b.artifact_paths` claims paths that don't exist anywhere in MLflow → **`MERGE_GATE_GAP`** with subkind `PHASE_B_PERSIST_SILENT_FAILURE`. Re-running the same harness will reproduce the same gap. The fix is to deploy the `genie.phase_b.partial` tagging branch so the next run surfaces the underlying exception, then patch the producer or persistence path it identifies.
  - The lever loop terminated cleanly AND `notebook_output.phase_b.iter_violation_counts` contains any value > 0 → **`MERGE_GATE_GAP`** even without missing artifacts. Phase E exit criteria require zero validator warnings; this is a contract violation, not a re-run candidate.
  - The lever loop terminated cleanly AND `len(notebook_output.phase_b.iter_record_counts) < iteration_counter` AND missing iters are not in `no_records_iterations` → **`MERGE_GATE_GAP`** with subkind `PHASE_B_PRODUCER_GAP`. Producers are silently not appending; re-running won't help.
  - The lever loop did NOT terminate cleanly (FAILED, TIMEDOUT, INTERNAL_ERROR) → likely `PILOT_NEEDS_RERUN` after the underlying defect is fixed. Inspect the task error first.
  - When in doubt, prefer `MERGE_GATE_GAP` over `PILOT_NEEDS_RERUN`. Re-runs are expensive (real Genie hours) and pre-deploy reruns reproduce pre-deploy bugs.

## Cross-Skill Hand-offs

This skill is **read-only**; it never overwrites fixtures, edits the burn-down ledger, modifies test budget literals, or creates git commits. When the user's intent crosses into write-side ops, hand off explicitly to `gso-replay-cycle-intake` rather than attempting those actions here.

- **Orchestration entry point.** When the operator says "postmortem this run" without specifying a bundle dir, the recommended path is the `gso-postmortem` skill, which sequences `evidence_bundle` → this skill → optional `trace_fetcher` → optional `gso-replay-cycle-intake` hand-off.
- **Trace fetcher.** This skill *may* invoke `python -m genie_space_optimizer.tools.trace_fetcher` according to the "Trace fetch decision rule" in Step 0. It does not invoke any other write-side commands.

This skill **calls** `gso-replay-cycle-intake` in two cases:

1. **Post-analysis intake request** — after a postmortem completes, if the user says "advance the burn-down", "intake this cycle", "promote the fixture", or "tighten the budget", hand off with `cycle_number`, the `databricks://<job_id>/<run_id>` source (or the resolved MLflow artifact path), and the postmortem's one-line summary as `notes`.
2. **Phase E `READY_TO_MERGE` intake** — when the Phase E checklist verdict is `READY_TO_MERGE` and the user wants to lock the burn-down at the merge baseline, hand off with the pilot run's job/run reference. Tightening the burn-down budget on a clean merge run is the canonical capstone for Phase E.

This skill **is called by** `gso-replay-cycle-intake` in three cases:

1. **Fixture acquisition for a `databricks://` source** — the intake skill defers job-run resolution and MLflow artifact lookup here. Return the resolved fixture path (or MLflow artifact reference) so it can copy it into the cycle's raw fixture slot.
2. **Step 6 regression triage** — when the intake skill's Step 4 finds violations above the prior `BURNDOWN_BUDGET`, it reverts and hands off here with the cycle-N raw fixture path and the prior cycle's commit SHA. Produce a postmortem identifying the regressing emit pattern; the intake skill will not promote until a fix lands and a re-run is at-or-below budget.
3. **Notes derivation** — when the user did not supply `notes` for the ledger row, the intake skill may ask this skill for a one-paragraph "what changed since cycle N-1" summary derived from the postmortem's Recommended Next Actions section.

Hand-offs in either direction are **explicit**. State "Handing off to `gso-replay-cycle-intake` to advance the burn-down" (or the converse) before invoking the peer skill so the audit trail is clear.

## Safety

- Do not run destructive Databricks commands.
- Do not cancel, rerun, repair, or delete jobs unless the user explicitly asks.
- Do not include tokens, credentials, full bearer headers, or raw secrets in reports.
- Quote only short evidence snippets.
- Do not overwrite replay fixtures, edit the burn-down ledger, change `BURNDOWN_BUDGET`, or create git commits — those are write-side operations owned by `gso-replay-cycle-intake`. If the user requests them, hand off rather than attempting them here.
