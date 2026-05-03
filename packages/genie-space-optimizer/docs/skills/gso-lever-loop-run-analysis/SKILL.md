---
name: gso-lever-loop-run-analysis
description: Analyze Genie Space Optimizer lever-loop Databricks Job runs using Job ID and Run ID. Use for Phase A/B/C/D validation, RCA-grounded optimizer debugging, operator transcript checks, MLflow trace/artifact inspection, and postmortem generation.
---

# GSO Lever Loop Run Analysis

Use this skill when asked to analyze, validate, debug, or postmortem a Genie Space Optimizer lever-loop Databricks Job run.

## Required Inputs

- `job_id`: Databricks Job ID.
- `run_id`: Databricks parent Job Run ID.

## Optional Inputs

- `profile`: Databricks CLI profile. If omitted, ask for the profile unless the workspace is already obvious from context.
- `task_key`: Databricks task key. Default: `lever_loop`.
- `experiment_id`: MLflow experiment ID. If omitted, resolve it from run parameters, stdout markers, MLflow tags, or job output.
- `output_dir`: Directory for reports. Default: `packages/genie-space-optimizer/docs/runid_analysis`.
- `iteration`: Iteration number to focus on. If omitted, analyze all iterations.

## Required Related Skills

Use these skills as needed:

- `databricks-jobs` for Databricks Jobs CLI/API inspection.
- `retrieving-mlflow-traces` for MLflow trace search/fetch.
- `analyze-mlflow-trace` for detailed trace analysis.
- `querying-mlflow-metrics` for metrics and aggregate trace/run analysis.
- `systematic-debugging` for root-cause workflow.

## Operating Principle

Do not guess. Follow systematic debugging:

1. Gather evidence from Databricks Jobs state, task output, stdout markers, MLflow runs/artifacts/traces, replay fixture, and decision validation.
2. Identify where the failure occurred: infrastructure, input handoff, eval, RCA, strategist, proposal, gate, applier, acceptance, Phase B trace persistence, convergence, or reporting.
3. State one root-cause hypothesis at a time.
4. Recommend the smallest next diagnostic or code action.

## Analysis Workflow

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

### MLflow Trace Health

- Experiment ID resolved.
- Related MLflow runs found.
- Related traces found or explicitly unavailable.
- Error traces summarized.
- Assessment/scorer errors separated from application errors.
- Long/slow traces listed when latency is relevant.

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

## Degraded Analysis Rules

If task output is truncated, say so and rely on MLflow artifacts and markers.

If MLflow experiment ID cannot be resolved, write the report with `MLFLOW_EXPERIMENT_UNRESOLVED` and ask the user for the experiment ID.

If `GSO_*_V1` markers are missing, run legacy-mode analysis from existing section banners and replay markers, then recommend adding marker support.

If evidence conflicts, report the conflict rather than choosing one source silently.

## Safety

- Do not run destructive Databricks commands.
- Do not cancel, rerun, repair, or delete jobs unless the user explicitly asks.
- Do not include tokens, credentials, full bearer headers, or raw secrets in reports.
- Quote only short evidence snippets.
