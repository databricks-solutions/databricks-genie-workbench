# Appendix B -- Troubleshooting

[Back to Index](../00-index.md) | Previous: [Appendix A -- Configuration Reference](A-configuration-reference.md) | Next: [Appendix C -- References](C-references.md)

---

## Convergence Reason Decoder

The `convergence_reason` field on a run tells you exactly what happened:

| Value | Status | Meaning |
|-------|--------|---------|
| `threshold_met` | CONVERGED | All quality thresholds met |
| `no_further_improvement` | STALLED | No levers improved score enough |
| `max_iterations` | MAX_ITERATIONS | Hit iteration limit |
| `baseline_meets_thresholds` | CONVERGED | No optimization needed |
| `finalize_timeout` | FAILED | Finalize exceeded soft timeout (default 6600s) |
| `finalize_error` | FAILED | Unhandled exception during finalize |
| `error_in_PREFLIGHT_STARTED` | FAILED | Preflight task crashed |
| `error_in_BASELINE_EVAL` | FAILED | Baseline evaluation crashed |
| `error_in_LEVER_{N}_*` | FAILED | Lever N task crashed |
| `error_in_FINALIZE_*` | FAILED | Finalize task crashed |
| `job_submission_error: {detail}` | FAILED | Job couldn't be submitted |
| `job_terminated_without_state_update:failed` | FAILED | Job crashed before writing terminal state |
| `job_run_lookup_failed` | FAILED | Couldn't check job status |
| `stale_queued_no_job_run` | FAILED | QUEUED for >10 min with no job_run_id |
| `user_discarded` | DISCARDED | User clicked Discard |

---

## Error-Solution Matrix by Task

### Task 1: Preflight

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `PermissionDenied` on Genie API | App SP lacks Genie access | Grant CAN_MANAGE on the Genie Space to the SP |
| `SCHEMA_NOT_FOUND` / `CATALOG_NOT_FOUND` | Wrong catalog/schema config | Fix `GENIE_SPACE_OPTIMIZER_CATALOG` / `GENIE_SPACE_OPTIMIZER_SCHEMA` |
| `permission denied` on information_schema | SP lacks UC grants; REST also failed | Use Settings page grants or run `grant_app_uc_permissions.py` |
| `Could not create MLflow experiment` | Shared path not writable | Create `/Shared/genie-optimization/` or grant SP write access |
| `benchmark_count = 0` | LLM generation returned empty | Verify `databricks-claude-opus-4-6` endpoint exists |
| Timeout (>600s) | Slow UC metadata or LLM calls | Increase timeout or check warehouse performance |

### Task 2: Baseline Evaluation

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `AttributeError: 'NoneType'` | Transient MLflow harness bug | Auto-retries up to 4 times; falls back to single-worker |
| `ResourceExhausted` / HTTP 429 | Genie rate limit | Built-in backoff; should self-heal |
| `TABLE_OR_VIEW_NOT_FOUND` | Benchmark references missing table | Regenerate benchmarks |
| `Infrastructure SQL errors detected` | Multiple infra errors | Default: fail fast. Set `FAIL_ON_INFRA_EVAL_ERRORS=false` to continue |
| `permission denied` on data tables | SP can't SELECT from tables | Grant SELECT to SP |
| Timeout (>3600s) | Many benchmarks x many judges | Reduce `TARGET_BENCHMARK_COUNT` |

### Task 3: Lever Loop

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `thresholds_met` early exit | Baseline already met targets | Not an error; `SKIPPED: baseline meets thresholds` |
| All levers rolled back | Every patch caused regression | Run completes as STALLED; review ASI results |
| `Genie API rate limit (429)` | Too many API calls | Built-in rate limiting + backoff |
| `LLM endpoint not found` | Endpoint misconfigured | Verify `databricks-claude-opus-4-6` exists |
| Patch application error | Invalid LLM-generated patch | Patch rolled back; lever skipped |
| `SQL context lost` | Spark session reset | Auto-recovers via `_ensure_sql_context()` |
| ASI `failure_type: None` | QID extraction failed | Verify wheel is up to date; redeploy |

### Task 4: Finalize

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| Repeatability < 50% | Genie gives inconsistent SQL | Not a code bug; Genie config is non-deterministic |
| MLflow model promotion failure | Model not loadable | Check MLflow experiment manually |
| `finalize_timeout` | Exceeded soft timeout | Increase `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` |
| `finalize_error` | Unhandled exception | Check `error_message` in FINALIZE_TERMINAL stage |

### Cross-Task Issues

| Issue | Symptom | Resolution |
|-------|---------|------------|
| Wheel not found | Job fails immediately | Run `make deploy` |
| Stale wheel | Old behavior despite changes | Use `make deploy` (handles `.gitignore` removal) |
| Notebook not found | Path not found error | Check SP permissions on `/Workspace/Shared/` |
| Stale run stuck in QUEUED | No progress for >10 min | Load space detail page to trigger reconciliation |
| Job terminated but run shows IN_PROGRESS | Delta state not updated | Load space detail page for reconciliation |
| NaN/Inf in scores | JSON serialization error | Backend scrubs to null; if you see NaN, report a bug |
| Connection pool is full | urllib3 warnings during concurrent eval | Auto-configured at startup (`CONNECTION_POOL_SIZE=20`); increase if warnings persist |

---

## Common Scenarios

### "Optimization job not found"

The Databricks Job hasn't been deployed yet:

```bash
make deploy PROFILE=<profile>
```

### "Space not found" on the dashboard

You need `CAN_EDIT` permission on the Genie Space. The app filters to spaces you can modify.

### Delta tables not available

Normal on first deployment. Tables are created automatically on first optimization run.

### Preflight fails with permission errors

The preflight uses the UC REST API by default. If it falls back to Spark SQL and that also fails, grant UC access:

1. Navigate to Settings page and copy the grant commands, or
2. Run: `python resources/grant_app_uc_permissions.py --profile <profile> --app-name genie-space-optimizer --catalog <cat> --schema <sch>`

### Benchmark dates look stale

The optimizer auto-detects and rewrites relative time references. Explicit years (e.g., "for 2024") are not rewritten. If dates still look wrong, manually check the benchmark SQL.

### Many benchmarks flagged as "temporal-stale"

The optimizer runs each temporal benchmark's GT SQL via Spark before scoring. If the GT SQL returns 0 rows (typically because date-relative filters no longer match any data), the benchmark is flagged `temporal_stale=True` and excluded from the accuracy denominator. If many benchmarks are flagged, the underlying data may not cover the current time period. Consider regenerating benchmarks with `TARGET_BENCHMARK_COUNT` or checking that the source tables contain recent data.

### "Connection pool is full, discarding connection" warnings

The optimizer automatically configures a larger urllib3 connection pool (`CONNECTION_POOL_SIZE = 20`) on the `WorkspaceClient` at job startup via `configure_connection_pool()`. If warnings persist despite this, increase `CONNECTION_POOL_SIZE` in `config.py`. This typically happens during concurrent evaluation where many Genie API calls, Statement Execution API polls, and LLM judge calls run simultaneously.

### Both GT and Genie SQL return 0 rows (`both_empty`)

When both the ground-truth SQL and Genie's generated SQL return 0 rows, the comparison result is `both_empty`. This is not an error -- it typically means both queries use filters that don't match current data (e.g., date ranges with no records). These comparisons are excluded from the accuracy denominator and do not affect the optimization score.

### Score didn't improve

Not all Genie Spaces benefit from metadata optimization. If the baseline is already high (>90%), there may be limited room for improvement. The run will report STALLED or CONVERGED.

### Spark session errors ("InvalidAccessKeyId", "ExpiredToken")

The backend auto-detects stale AWS STS credentials and recreates the Spark session. Persistent errors indicate workspace credential vending is misconfigured.

### Delta "SCHEMA_CHANGE_SINCE_ANALYSIS" errors

The `read_table` helper automatically retries with `REFRESH TABLE`. If persistent, check for concurrent runs conflicting.

### Stale wheel after deployment

Always use `make deploy` which handles the `.build/.gitignore` issue:

```bash
make deploy PROFILE=<profile>
```

Verify the correct wheel loaded:

```bash
databricks apps logs genie-space-optimizer -p <profile> | grep "App wheel:"
```

---

## Diagnostic Checklist

When investigating a failed run:

1. Check `convergence_reason` via API or Delta query
2. Check stage timeline for the failed stage and error message
3. Check job task status in Databricks Workflows UI
4. Check app logs for the specific error
5. If preflight failed: verify UC grants and Genie Space access
6. If baseline failed: check Genie API availability and benchmark validity
7. If lever loop failed: check ASI results and patch history
8. If finalize failed: check heartbeats and timeout settings

---

Next: [Appendix C -- References](C-references.md)
