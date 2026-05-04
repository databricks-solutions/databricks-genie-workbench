---
name: gso-lever-loop-run-analysis
description: Use when analyzing, validating, debugging, or postmortem-ing a Genie Space Optimizer lever-loop Databricks Job run by job_id + run_id. Covers Phase A journey replay, Phase B/C decision-trace and RCA-groundedness checks, Phase D scoreboard and bucketing checks, Phase E pilot-run merge-readiness validation (zero validator warnings, raise_on_violation flip, accuracy non-regression vs variance baseline, decision-trace hard-gate, sanity-broken PR), MLflow trace/artifact inspection, and postmortem generation.
---

# GSO Lever Loop Run Analysis

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
- `bundle_dir`: Pre-built evidence bundle root directory. If supplied, read `bundle_dir/evidence/manifest.json` and use the bundle artifacts as the primary evidence source.
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
2. Identify where the failure occurred in the lever-loop process: infrastructure, input handoff, `evaluation_state`, `rca_evidence`, `cluster_formation`, `action_group_selection`, `proposal_generation`, `safety_gates`, `applied_patches`, `post_patch_evaluation`, `acceptance_decision`, `learning_next_action`, Phase B trace persistence, convergence, or reporting.
3. State one root-cause hypothesis at a time.
4. Recommend the smallest next diagnostic or code action.

For Phase E specifically, distinguish between three failure surfaces — the recommended next action depends on which one tripped:

- **Pilot-run gap** (rerun candidate): a defect surfaced during the real run itself (validator warning, missing decision record, transcript section empty on a real iteration, scoreboard nonsense, bucket misclassification). Rerun is on the table only after the underlying defect is fixed; a rerun without a fix repeats the same failure.
- **Merge-gate gap** (code/test work): the codebase has not yet flipped `raise_on_violation`, lacks the decision-trace hard-gate replay test, or has no sanity-broken PR CI evidence. Independent of any pilot run; does not need a rerun.
- **Baseline regression** (rollback or rescope): the pilot completed cleanly but accuracy regressed against the Phase A variance baseline. Investigate the regressing iteration's decision trace; consider rolling back the offending PR or rescoping Phase E.

## Analysis Workflow

0. **Acquire or load the evidence bundle.** If `bundle_dir` is supplied by `gso-postmortem`, start from `bundle_dir/evidence/manifest.json` and cite on-disk artifacts from that bundle. If no bundle is supplied, run the bundle helper first:

   ```bash
   python -m genie_space_optimizer.tools.evidence_bundle \
       --job-id <job_id> --run-id <run_id> --profile <profile> \
       --output-dir packages/genie-space-optimizer/docs/runid_analysis \
       [--auto-backfill]
   ```

   When the bundle is available, prefer bundle files over live CLI calls for analysis. Still inspect the parent job run for task-attempt ordering, because older bundle versions may have anchored to the first `lever_loop` task rather than the latest one.

1. Validate Databricks CLI auth:

   ```bash
   databricks auth profiles
   ```

2. Fetch parent job run:

   ```bash
   databricks jobs get-run --run-id <run_id> --profile <profile> --output json
   ```

3. Locate the task run whose `task_key` equals `lever_loop` unless the caller supplied another task key. If the parent run contains multiple `lever_loop` attempts, analyze the latest task run by task `start_time`/`end_time` when present, otherwise by the order returned in `job_run.tasks`. Record every attempt in the report. If the latest attempt failed while an earlier attempt succeeded, classify the latest attempt as the primary run outcome unless the operator explicitly asks to analyze the latest successful optimizer attempt.

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

For every unresolved hard QID and every rolled-back or skipped AG, map evidence to the first broken lever-loop stage:

- `evaluation_state`: stale or conflicting full-eval rows, soft-cluster currency drift, or failed-question/regression-bucket mismatch.
- `rca_evidence`: missing RCA cards, `rca_cards_present=false`, weak counterfactuals, or ASI/root-cause conflicts.
- `cluster_formation`: stale soft clusters, hard/soft bucket drift, or clusters built from non-current eval rows.
- `action_group_selection`: uncovered hard clusters, repeated coverage-gap AGs, wrong lever selection for the root cause, or buffered AG reuse that ignores prior gate outcomes.
- `proposal_generation`: `Proposals (0 total)`, no candidate for a causal root cause, or SQL-shape root causes producing only instruction/metadata proposals.
- `safety_gates`: structural-gate drops, blast-radius drops, applyability drops, or cap decisions that remove the only causal patch.
- `applied_patches`: patch ID collisions, selected/applied reconciliation conflicts, or applied patch surface not matching selected patch identity.
- `post_patch_evaluation`: candidate eval missing, failed-question list inconsistent with accepted baseline, or QID status transitions not emitted.
- `acceptance_decision`: target-fixed attribution drift, accepted non-causal gains, rejected/accepted regression debt, or regression buckets missing newly hard QIDs.
- `learning_next_action`: repeated no-op iterations, repeated deterministic gate drops, missing DOA dedupe, or failure to switch strategy after a typed gate/proposer failure.

When the run uses legacy stdout rather than Phase H artifacts, still produce this stage map from transcript sections, replay fixture, and markers. Do not collapse distinct failure modes: `structural_gate_dropped_instruction_only` means a proposal existed and was rejected; `proposal_generation_empty` means the proposer returned no proposals.

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
- `PHASE_B_TRACE_GAP`
- `MLFLOW_ARTIFACT_GAP`
- `CONVERGENCE_OR_PLATEAU_GAP`
- `MODEL_CEILING`
- `UNKNOWN_NEEDS_MORE_EVIDENCE`

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

## Optimizer Improvement Next Steps

## Recommended Next Actions

## Evidence Appendix
```

`## Optimizer Improvement Next Steps` is required for every postmortem, even when the verdict is `READY_TO_MERGE`. It must be grounded and code-actionable:

- Tie each next step to a specific lever-loop stage from the mechanics checklist.
- Cite the run evidence: QID, AG, iteration, gate/drop/rollback marker, artifact path, or transcript section.
- Name the likely module/file to change, for example `optimization/control_plane.py`, `optimization/harness.py`, `optimization/optimizer.py`, `optimization/rca.py`, `optimization/cluster_driven_synthesis.py`, `optimization/applier.py`, `optimization/static_judge_replay.py`, `tools/evidence_bundle.py`, or `tools/marker_parser.py`.
- State the smallest code/test change needed before rerun.
- Separate optimizer-improvement work from Databricks/harness reliability work.
- Do not recommend "rerun" as the primary next step unless the failed stage was purely infrastructure and the transcript proves no optimizer logic executed.

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

If MLflow experiment ID cannot be resolved, write the report with `MLFLOW_EXPERIMENT_UNRESOLVED` and ask the user for the experiment ID.

If `GSO_*_V1` markers are missing, run legacy-mode analysis from existing section banners and replay markers, then recommend adding marker support.

If evidence conflicts, report the conflict rather than choosing one source silently.

For Phase E specifically:

- If `baseline_run_id` is unavailable when `phase=E`, write the report with `PHASE_E_BASELINE_UNRESOLVED` in the accuracy non-regression row and ask the user. Do not fabricate a baseline from this run's iteration 0.
- If the pilot did not complete (truncation, infrastructure failure, run cancelled mid-iteration), classify the run with the appropriate non-Phase-E label (`DATABRICKS_JOB_FAILURE` etc.) and explicitly state that Phase E validation cannot proceed until a complete pilot run exists.
- If merge-gate state checks find `repo_root` unset or inaccessible, mark each merge-gate row `UNVERIFIED` and ask the user to confirm or rerun with `repo_root` set.
- If pilot-run validation passes but merge-gate state shows multiple gaps, do not classify as `READY_TO_MERGE` — use `MERGE_GATE_GAP` and list the missing items in priority order.

## Cross-Skill Hand-offs

This skill is **read-only**; it never overwrites fixtures, edits the burn-down ledger, modifies test budget literals, or creates git commits. When the user's intent crosses into write-side ops, hand off explicitly to `gso-replay-cycle-intake` rather than attempting those actions here.

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
