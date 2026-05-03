# Lever Loop Stdout Marker Contract

The lever-loop task emits single-line JSON markers so run analysis can rely on stable machine-readable output instead of freeform log scraping.

## Format

```text
<MARKER_NAME> {"compact":"json"}
```

JSON must be emitted with sorted keys and compact separators.

## Markers

### `GSO_RUN_MANIFEST_V1`

Emitted once near lever-loop start and once near end.

Required fields:

- `optimization_run_id`
- `databricks_job_id`
- `databricks_parent_run_id`
- `lever_loop_task_run_id`
- `mlflow_experiment_id`
- `space_id`
- `event`

Allowed `event` values:

- `start`
- `end`

### `GSO_ITERATION_SUMMARY_V1`

Emitted once per iteration after iteration state is known.

Required fields:

- `optimization_run_id`
- `iteration`
- `accepted_count`
- `rolled_back_count`
- `skipped_count`
- `gate_drop_count`
- `decision_record_count`
- `journey_violation_count`

### `GSO_PHASE_B_V1`

Emitted once per iteration after Phase B trace/transcript persistence.

Required fields:

- `optimization_run_id`
- `iteration`
- `decision_record_count`
- `decision_validation_count`
- `transcript_chars`
- `decision_trace_artifact`
- `operator_transcript_artifact`
- `persist_ok`

### `GSO_CONVERGENCE_V1`

Emitted once when the lever loop terminates.

Required fields:

- `optimization_run_id`
- `reason`
- `iteration_counter`
- `best_accuracy`
- `thresholds_met`

## Safety

Marker JSON must not include credentials, tokens, raw bearer headers, or full SQL payloads.
