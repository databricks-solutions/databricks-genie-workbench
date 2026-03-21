# 10 -- Operations Guide

[Back to Index](00-index.md) | Previous: [09 Deployment Guide](09-deployment-guide.md) | Next: [Appendix A -- Configuration Reference](appendices/A-configuration-reference.md)

---

## Monitoring Optimization Runs

### Via the UI

The run detail page (`/runs/{runId}`) provides real-time monitoring:

- **Pipeline steps** -- 5-step progress tracker with status, duration, and summaries
- **Lever detail** -- per-lever status (accepted, rolled back, skipped) with score deltas
- **Iteration chart** -- score progression across iterations
- **ASI panel** -- failure analysis breakdown by judge and failure type
- **Provenance panel** -- end-to-end traceability from judge verdicts through patches to gate outcomes

The page auto-polls every 5 seconds until a terminal status is reached.

### Via the API

Poll `GET /api/genie/runs/{run_id}` or `GET /api/genie/trigger/status/{run_id}` programmatically. See [07 -- API Reference](07-api-reference.md) for details.

### Via Databricks Workflows

The optimization job is visible in the Databricks Workflows UI. Each run shows the 6-task DAG with per-task status, logs, and output.

```bash
# Get job run details
databricks runs get <job_run_id> -p <profile> -o json | python3 -c "
import sys, json
for t in json.load(sys.stdin).get('tasks', []):
    print(f\"{t['task_key']}: {t.get('state', {}).get('result_state', 'RUNNING')}\")
"
```

---

## Stale Run Reconciliation

Runs can get stuck in `QUEUED` or `IN_PROGRESS` if the Databricks Job terminates without updating Delta state. The backend automatically reconciles stale runs.

### How It Works

`_reconcile_active_runs()` runs when:
- The space detail page loads
- A new optimization is triggered

For each active run, it:
1. Checks the actual Databricks Job state via `ws.jobs.get_run()`
2. If the job is `TERMINATED`, `SKIPPED`, or `INTERNAL_ERROR` but Delta status is still active: marks it `FAILED` with `convergence_reason` like `job_terminated_without_state_update:failed`
3. If a run has been `QUEUED` for > 10 minutes with no `job_run_id`: marks it `FAILED` with `stale_queued_no_job_run`

### Manual Reconciliation

If you see a stuck run, simply load the space detail page in the UI. The reconciliation runs automatically and the stuck run will be marked as `FAILED`.

---

## Diagnostic SQL Queries

Use these queries against the Delta state tables (`{catalog}.{schema}`) to diagnose issues.

### Check run status

```sql
SELECT run_id, space_id, status, convergence_reason,
       baseline_accuracy, optimized_accuracy, best_iteration,
       created_at, updated_at
FROM {catalog}.{schema}.genie_opt_runs
WHERE run_id = '<run_id>'
```

### Check stage timeline

```sql
SELECT stage, status, error_message, duration_seconds, started_at
FROM {catalog}.{schema}.genie_opt_stages
WHERE run_id = '<run_id>'
ORDER BY started_at
```

### Check iteration scores

```sql
SELECT iteration, lever, eval_scope, overall_accuracy,
       total_questions, correct_count, thresholds_met, scores_json
FROM {catalog}.{schema}.genie_opt_iterations
WHERE run_id = '<run_id>'
ORDER BY iteration, lever
```

### Check patch history

```sql
SELECT lever, patch_type, target_object, risk_level,
       rolled_back, rollback_reason, patch_index
FROM {catalog}.{schema}.genie_opt_patches
WHERE run_id = '<run_id>'
ORDER BY iteration, patch_index
```

### Check ASI failure analysis

```sql
SELECT question_id, judge, failure_type, severity,
       blame_set, counterfactual_fix, arbiter_verdict
FROM {catalog}.{schema}.genie_eval_asi_results
WHERE run_id = '<run_id>' AND iteration = 0
ORDER BY question_id, judge
```

### Check provenance chain

```sql
SELECT question_id, judge, resolved_root_cause, signal_type,
       cluster_id, proposal_id, patch_type, gate_type, gate_result
FROM {catalog}.{schema}.genie_opt_provenance
WHERE run_id = '<run_id>'
ORDER BY iteration, lever, question_id
```

### Check arbiter corrections

```sql
SELECT question_id, arbiter_verdict, question_text
FROM {catalog}.{schema}.genie_eval_asi_results
WHERE run_id = '<run_id>' AND iteration = 0 AND arbiter_verdict = 'genie_correct'
```

### Check flagged questions

```sql
SELECT question_id, question_text, reason, status
FROM {catalog}.{schema}.genie_opt_flagged_questions
WHERE space_id = '<space_id>'
ORDER BY created_at DESC
```

### Check finalize heartbeats

```sql
SELECT stage, status, detail_json, error_message, duration_seconds
FROM {catalog}.{schema}.genie_opt_stages
WHERE run_id = '<run_id>'
  AND stage IN ('FINALIZE_STARTED', 'FINALIZE_HEARTBEAT', 'FINALIZE_TERMINAL')
ORDER BY started_at
```

---

## Log Inspection

### App logs

```bash
# Recent logs
databricks apps logs genie-space-optimizer -p <profile>

# Or via the apx CLI (dev mode)
apx dev logs -f
```

Key log lines to look for:

| Log Pattern | Meaning |
|-------------|---------|
| `App wheel: genie_space_optimizer-x.y.z.whl (size=... bytes)` | Wheel loaded successfully |
| `OBO token not available` | Falling back to SP client |
| `Reconciling stale run` | Auto-fixing a stuck run |
| `Job submission: run_id=...` | Optimization job submitted |
| `Configured connection pool size=20` | urllib3 pool resized for concurrent API calls |
| `Flagged N/M benchmarks as temporal-stale (excluded from accuracy)` | Temporal benchmarks with 0-row GT SQL excluded |
| `Restored N reflection entries from Delta for resume` | Lever loop resumed with prior iteration context |

### Job logs

```bash
# Get task-by-task status
databricks runs get <job_run_id> -p <profile> -o json

# Get output for a specific task
databricks runs get-output <job_run_id> -p <profile> -o json
```

### MLflow experiment

Navigate to the MLflow experiment at `/Shared/genie-space-optimizer/{space_id}/{domain}` to view:
- Evaluation traces with judge verdicts
- Per-iteration metrics
- LoggedModel versions with config snapshots

---

## Health Checks

### Application Health

| Check | Command | Expected |
|-------|---------|----------|
| App running | `databricks apps get genie-space-optimizer -p <profile>` | Status: RUNNING |
| API responding | `curl <app-url>/api/genie/version` | `{"version": "x.y.z"}` |
| Wheel loaded | App logs contain `App wheel:` | Correct version and size |

### Infrastructure Health

| Check | Command | Expected |
|-------|---------|----------|
| SQL Warehouse | `databricks warehouses get <id> -p <profile>` | State: RUNNING |
| Genie API | `databricks api get /api/2.0/genie/spaces -p <profile>` | List of spaces |
| Delta tables | `SHOW TABLES IN {catalog}.{schema} LIKE 'genie_opt_*'` | 8 tables |
| MLflow | Check experiment exists at expected path | Experiment accessible |

### Permission Health

Navigate to the Settings page in the app. All schemas should show "Read: granted" and spaces should show "CAN_MANAGE: yes".

---

## Wheel Management

### Stale Wheel Detection

The app logs the resolved wheel at startup. If the wheel size is unexpectedly small or the log line is missing, the wrong wheel may be deployed.

```bash
# Check current wheel
databricks apps logs genie-space-optimizer -p <profile> | grep "App wheel:"

# Force redeploy
make deploy PROFILE=<profile>
```

### Stale Wheel Cleanup

The job launcher automatically cleans up old wheel directories during `submit_optimization()`. The `make deploy` pipeline also cleans stale wheels from the workspace build path.

---

## Common Operational Tasks

### Restart the app

```bash
databricks apps deploy genie-space-optimizer -p <profile>
```

### Force-fail a stuck run

Load the space detail page -- reconciliation runs automatically. If the run is truly stuck (job is still running but unresponsive), cancel the job first:

```bash
databricks runs cancel <job_run_id> -p <profile>
```

Then load the space detail page to trigger reconciliation.

### View all runs for a space

```sql
SELECT run_id, status, convergence_reason, baseline_accuracy, optimized_accuracy, created_at
FROM {catalog}.{schema}.genie_opt_runs
WHERE space_id = '<space_id>'
ORDER BY created_at DESC
```

### Clean up old run data

Delta tables support time travel. To reclaim space:

```sql
VACUUM {catalog}.{schema}.genie_opt_runs RETAIN 720 HOURS
VACUUM {catalog}.{schema}.genie_opt_stages RETAIN 720 HOURS
VACUUM {catalog}.{schema}.genie_opt_iterations RETAIN 720 HOURS
VACUUM {catalog}.{schema}.genie_opt_patches RETAIN 720 HOURS
```

---

Next: [Appendix A -- Configuration Reference](appendices/A-configuration-reference.md)
