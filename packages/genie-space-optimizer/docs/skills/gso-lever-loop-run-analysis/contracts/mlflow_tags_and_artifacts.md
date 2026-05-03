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
4. MLflow search by `genie.databricks.parent_run_id`.
5. User-provided experiment ID.
