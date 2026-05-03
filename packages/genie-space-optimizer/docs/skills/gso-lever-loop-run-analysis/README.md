# GSO Lever Loop Run Analysis Skill

This skill analyzes Genie Space Optimizer lever-loop Databricks Job runs from a Databricks Job ID and Run ID.

It is designed for repeatable run postmortems and optimizer improvement work. It combines:

- Databricks Jobs CLI state.
- Lever-loop stdout markers.
- Replay fixture markers.
- MLflow run/artifact/trace evidence.
- Phase B `OptimizationTrace` and operator transcript artifacts.
- Systematic debugging.

Default report output:

```text
packages/genie-space-optimizer/docs/runid_analysis/<job_id>_<run_id>_analysis.md
packages/genie-space-optimizer/docs/runid_analysis/<job_id>_<run_id>_analysis.json
```

Required inputs:

- `job_id`
- `run_id`

Optional inputs:

- `profile`
- `task_key`
- `experiment_id`
- `output_dir`
- `iteration`

The skill must run read-only Databricks and MLflow operations unless the user explicitly asks for repair, rerun, or mutation.
