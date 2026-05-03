# Lever Loop MLflow Tags And Artifacts Contract

The run analyzer skill resolves MLflow evidence using tags and artifact paths. These names must remain stable.

## Required Parent Run Tags

- `genie.run_id`
- `genie.databricks.job_id`
- `genie.databricks.parent_run_id`
- `genie.databricks.lever_loop_task_run_id`
- `genie.phase_b.partial`

## Per-Iteration Tags

- `journey_validation.iter_<N>.violations`
- `journey_validation.iter_<N>.is_valid`
- `decision_trace.iter_<N>.records`
- `decision_trace.iter_<N>.violations`

## Artifacts

- `phase_a/journey_validation/iter_<N>.json`
- `phase_b/decision_trace/iter_<N>.json`
- `phase_b/operator_transcript/iter_<N>.txt`

## Resolution Order For Experiment ID

1. Explicit analyzer input.
2. `GSO_RUN_MANIFEST_V1.mlflow_experiment_id`.
3. Databricks run/job parameters named `MLFLOW_EXPERIMENT_ID`, `mlflow_experiment_id`, or `experiment_id`.
4. Databricks job parameter `experiment_name` resolved via `MlflowClient.get_experiment_by_name(name).experiment_id`.
5. MLflow search by `genie.databricks.parent_run_id`.
6. User-provided experiment ID.

## Linking-Tag Compatibility (read-side)

The canonical linking tag is `genie.optimization_run_id`. The deployed harness as of 2026-05-03 sets that tag only on `genie.run_type=strategy` and `enrichment_snapshot` runs; iteration `full_eval` runs receive `genie.run_id=<opt_run_id>` instead, and the lever-loop run itself does not carry `genie.run_type=lever_loop` at all.

Read-side tooling (audit, run-analysis skill) **must accept either** `genie.optimization_run_id` OR `genie.run_id` as the linking tag, and must not require `genie.run_type=lever_loop` to anchor. Concretely, the audit's filter

```text
tags.`genie.optimization_run_id` = '<opt_run_id>'
```

should fall back to

```text
tags.`genie.run_id` = '<opt_run_id>'
```

when the first query returns zero matches. Anchor selection should pick the most recent iteration's `full_eval` run (highest `genie.iteration`) when no `lever_loop` run_type exists.

Write-side (this branch and beyond) should converge to a single canonical schema:

| `genie.run_type` | Required tags | Why |
|---|---|---|
| `lever_loop` | `genie.optimization_run_id`, `genie.run_id`, `genie.space_id`, `genie.databricks.{job_id, parent_run_id, lever_loop_task_run_id}` | Canonical anchor for the audit; missing today on the production harness. |
| `strategy` | `genie.optimization_run_id`, `genie.run_id`, `genie.iteration`, `genie.stage=strategy` | Already correct on production. |
| `full_eval` (per-iteration) | `genie.optimization_run_id`, `genie.run_id`, `genie.iteration`, `genie.stage=full_eval`, `genie.ag_id` (when applicable) | Production sets `genie.run_id` but not `genie.optimization_run_id`; should set both. |
| `enrichment_snapshot` | `genie.optimization_run_id`, `genie.run_id`, `genie.stage=enrichment` | Already produces `model_snapshots/iter_-1/` artifacts on production. |
| `finalize_*` | `genie.optimization_run_id`, `genie.run_id`, `genie.iteration`, `genie.stage=finalize_{repeatability, held_out}` | Production sets `genie.run_id` only. |

## Notebook-Output Fallback (when stdout is empty)

The lever-loop is a notebook task (`run_lever_loop` notebook). `databricks jobs get-run-output <task_run_id>` returns empty `logs` and `error` fields for notebook tasks; the structured result lives in `notebook_output.result` as a JSON-encoded string with this canonical shape:

```json
{
  "iteration_counter": <int>,
  "levers_attempted": [<int>, ...],
  "levers_accepted": [<int>, ...],
  "levers_rolled_back": [<int>, ...],
  "_debug_ref_sqls_count": <int>,
  "_debug_failure_rows_loaded": <int>,
  "phase_b": {
    "contract_version": "v1",
    "decision_records_total": <int>,
    "iter_record_counts": [<int>, ...],
    "iter_violation_counts": [<int>, ...],
    "no_records_iterations": [<int>, ...],
    "artifact_paths": ["phase_b/decision_trace/iter_<N>.json", ...],
    "producer_exceptions": {<producer_name>: <int>},
    "target_qids_missing_count": <int>,
    "total_violations": <int>
  }
}
```

The bundle CLI persists this verbatim at `evidence/lever_loop_notebook_output.json` so the analyzer skill can read it without re-shelling to the Databricks CLI. Two cross-checks the analyzer must perform on this block:

1. `len(phase_b.iter_record_counts)` should equal `iteration_counter`. A shorter list with no compensating entries in `no_records_iterations` is a producer gap.
2. Every path in `phase_b.artifact_paths` should exist on at least one MLflow sibling run. A claimed-but-missing path is a persistence-silent-failure.
