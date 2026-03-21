---
name: Align DAG Notebooks
overview: Fix the lever-skipped bug in run_deploy.py, bring all 6 notebooks to consistent documentation quality (training-grade), align run_evaluation_only.py with shared helpers, and standardize dbutils.notebook.exit() usage across all notebooks.
todos:
  - id: fix-deploy-skipped
    content: "P0: Add lever_skipped branching to run_deploy.py (mirror finalize pattern)"
    status: completed
  - id: eval-shared-helpers
    content: "P1: Replace inline helpers in run_evaluation_only.py with _helpers.py imports + partial()"
    status: completed
  - id: eval-dedup-sql-context
    content: "P1: Remove inline _ensure_sql_context/_quote_identifier from run_evaluation_only.py, import from harness"
    status: completed
  - id: docs-finalize
    content: "P1: Add training-quality markdown documentation to run_finalize.py (upstream values, internals, failure modes, summary)"
    status: completed
  - id: docs-deploy
    content: "P1: Add training-quality markdown documentation to run_deploy.py (architecture, upstream values, internals, failure modes, summary)"
    status: completed
  - id: docs-eval
    content: "P2: Add training-quality markdown documentation to run_evaluation_only.py (relationship to DAG, caller, result flow, failure modes)"
    status: completed
  - id: standardize-exit
    content: "P3: Add dbutils.notebook.exit() to preflight, baseline, finalize, deploy for consistent operator UX"
    status: completed
isProject: false
---

# Align DAG Notebooks — Consistency, Bug Fix, and Training-Quality Docs

## P0 Bug Fix: `run_deploy.py` lever-skipped handling

[run_deploy.py](src/genie_space_optimizer/jobs/run_deploy.py) lines 67-69 hardcode `taskKey="lever_loop"` for `model_id` and `iteration_counter`. When lever loop is skipped (baseline meets thresholds), lever_loop publishes its values before `dbutils.notebook.exit()` so this works today — but it is fragile and inconsistent with how [run_finalize.py](src/genie_space_optimizer/jobs/run_finalize.py) handles it (lines 95-104 with explicit `lever_skipped` branching).

**Fix:** Add the same `lever_skipped` check pattern from finalize:

```python
lever_skipped_raw = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="skipped")
lever_skipped = str(lever_skipped_raw).lower() in ("true", "1")
if lever_skipped:
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="model_id")
    iteration_counter = 0
else:
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="model_id")
    iteration_counter = int(dbutils.jobs.taskValues.get(taskKey="lever_loop", key="iteration_counter"))
```

## P1: `run_evaluation_only.py` — shared helpers and dedup

- **Replace inline helpers** (lines 50-63) with imports from [_helpers.py](src/genie_space_optimizer/jobs/_helpers.py) + `partial()`, matching the pattern in all 5 DAG notebooks. Use `_TASK_LABEL = "EVALUATION"`.
- **Remove inline `_quote_identifier` and `_ensure_sql_context`** (lines 66-75). Import `_ensure_sql_context` from [harness.py](src/genie_space_optimizer/optimization/harness.py) (line 137) instead. This eliminates duplicate code that could drift.
- Remove the now-unused `datetime`/`timezone` import (only needed for the old inline `_ts`).

## P1: Training-quality docs for `run_finalize.py`

Add interstitial markdown cells matching the style in [run_preflight.py](src/genie_space_optimizer/jobs/run_preflight.py) and [run_baseline.py](src/genie_space_optimizer/jobs/run_baseline.py):

- **Before imports block:** "Imports and Helpers" table (same pattern as preflight line 69-89)
- **Before task value reading** (line 83): "Reading Upstream Task Values" section explaining the `lever_skipped` branching pattern, with a table of keys read from `preflight`, `baseline_eval`, and `lever_loop`
- **Before benchmark loading** (line 125): "Loading Benchmarks for Repeatability" section
- **Before `_run_finalize` call** (line 137): "What `_run_finalize` Does Internally" section covering:
  - Repeatability testing (2 passes, SQL hash comparison)
  - Terminal status determination (CONVERGED / MAX_ITERATIONS / STALLED / FAILED)
  - Heartbeat mechanism (`FINALIZE_TIMEOUT_SECONDS`, `FINALIZE_HEARTBEAT_SECONDS`)
  - Model promotion
  - Report generation
- **Before task value publication** (line 168): "Publishing Task Values" section with table of keys published and who consumes them
- **Known Failure Modes** section: timeout, repeatability regressions, MLflow errors
- **Summary** cell at end

## P1: Training-quality docs for `run_deploy.py`

Add documentation cells following the same structure:

- **Architecture section:** DAG position, condition task gating, upstream/downstream
- **Imports and Helpers** table
- **Reading Upstream Task Values** section with the lever_skipped branching (after the bug fix)
- **What `_run_deploy` Does Internally** section: deploy stages, Delta state writes, DABs integration status
- **Known Failure Modes:** empty deploy_target (skipped), deploy errors, permission issues
- **Summary** cell at end

## P2: Training-quality docs for `run_evaluation_only.py`

Add documentation structure:

- **Relationship to the DAG** — explicitly not part of the 5-task DAG; called by `run_evaluation_via_job()` in [harness.py](src/genie_space_optimizer/optimization/harness.py) line 2243
- **Who calls this** — the harness submits it as a standalone Databricks Job run for serverless/isolated evaluation
- **How results flow back** — `dbutils.notebook.exit(accuracy)`, caller polls job status and reads exit value
- **Imports and Helpers** table
- **Widget Parameters** — already present, keep as-is
- **Known Failure Modes** — same MLflow harness issues as baseline (reference baseline's docs)
- **Summary** cell

## P3: Standardize `dbutils.notebook.exit()` across all 5 DAG notebooks

Currently only `run_lever_loop.py` (line 350) and `run_evaluation_only.py` (line 144) call it. For operator convenience (quick status in the Workflows UI):

- Add `dbutils.notebook.exit("Task 1 Completed")` at the end of `run_preflight.py`
- Add `dbutils.notebook.exit("Task 2 Completed")` at the end of `run_baseline.py`
- Add `dbutils.notebook.exit(...)` with finalize status at the end of `run_finalize.py`
- Add `dbutils.notebook.exit(...)` with deploy status at the end of `run_deploy.py`
- `run_lever_loop.py` already exits with debug_info — keep as-is
