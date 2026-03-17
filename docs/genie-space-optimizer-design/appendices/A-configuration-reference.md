# Appendix A -- Configuration Reference

[Back to Index](../00-index.md) | Next: [Appendix B -- Troubleshooting](B-troubleshooting.md)

---

All tunable parameters are defined in `src/genie_space_optimizer/common/config.py`. Parameters can be overridden via environment variables, job parameters, or the `thresholds` argument to `optimize_genie_space()`.

---

## Quality Thresholds

These define the target scores for each quality dimension. If all are met, the run converges.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DEFAULT_THRESHOLDS["syntax_validity"]` | 98.0 | Generated SQL parses correctly |
| `DEFAULT_THRESHOLDS["schema_accuracy"]` | 95.0 | Correct tables/columns referenced |
| `DEFAULT_THRESHOLDS["logical_accuracy"]` | 90.0 | Correct aggregations, filters, GROUP BY |
| `DEFAULT_THRESHOLDS["semantic_equivalence"]` | 90.0 | Same business metric as expected |
| `DEFAULT_THRESHOLDS["completeness"]` | 90.0 | All requested dimensions included |
| `DEFAULT_THRESHOLDS["response_quality"]` | 0.0 | NL analysis accuracy (advisory, not blocking) |
| `DEFAULT_THRESHOLDS["result_correctness"]` | 85.0 | Correct result values |
| `DEFAULT_THRESHOLDS["asset_routing"]` | 95.0 | Correct asset type selected |
| `REPEATABILITY_TARGET` | 90.0 | Repeatability score target |

---

## Rate Limits and Timing

| Parameter | Default | Env Override | Description |
|-----------|---------|-------------|-------------|
| `RATE_LIMIT_SECONDS` | 12 | -- | Delay between Genie API calls |
| `GENIE_RATE_LIMIT_RETRIES` | 3 | -- | Retries on HTTP 429 |
| `GENIE_RATE_LIMIT_BASE_DELAY` | 30 | -- | Base delay for 429 backoff (seconds) |
| `PROPAGATION_WAIT_SECONDS` | 30 | `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT` | Wait after config patch before eval |
| `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS` | 90 | `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING` | Wait after entity matching changes |
| `GENIE_POLL_INITIAL` | 3 | -- | Initial Genie query poll interval (seconds) |
| `GENIE_POLL_MAX` | 10 | -- | Max Genie query poll interval (seconds) |
| `GENIE_MAX_WAIT` | 120 | -- | Max wait for Genie query completion (seconds) |
| `JOB_POLL_INTERVAL` | 30 | -- | Job status poll interval (seconds) |
| `JOB_MAX_WAIT` | 3600 | -- | Max wait for job completion (seconds) |
| `UI_POLL_INTERVAL` | 5 | -- | Frontend polling interval (seconds) |
| `SQL_STATEMENT_POLL_LIMIT` | 30 | -- | Max SQL statement polls |
| `SQL_STATEMENT_POLL_INTERVAL` | 2 | -- | SQL statement poll interval (seconds) |
| `INLINE_EVAL_DELAY` | 12 | -- | Delay between inline eval queries |
| `CONNECTION_POOL_SIZE` | 20 | -- | urllib3 connection pool size on WorkspaceClient |

---

## Iteration and Convergence

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MAX_ITERATIONS` | 3 | Maximum adaptive loop iterations |
| `REGRESSION_THRESHOLD` | 5.0 | Percentage drop that triggers rollback |
| `SLICE_GATE_TOLERANCE` | 15.0 | Per-dimension tolerance for slice gate |
| `ENABLE_SLICE_GATE` | False | Whether slice gate is enabled |
| `SLICE_GATE_MIN_REDUCTION` | 0.5 | Minimum reduction factor for slice gate |
| `MAX_NOISE_FLOOR` | 5.0 | Min score improvement to accept (below = cosmetic) |
| `PLATEAU_ITERATIONS` | 2 | Consecutive no-improvement iterations before stopping |
| `CONSECUTIVE_ROLLBACK_LIMIT` | 2 | Consecutive rollbacks before stopping |
| `DIMINISHING_RETURNS_EPSILON` | 2.0 | Min improvement % for recent accepted iterations |
| `DIMINISHING_RETURNS_LOOKBACK` | 2 | How many recent accepted iterations to check |
| `REFLECTION_WINDOW_FULL` | 3 | Recent reflection entries shown in full detail |

---

## Arbiter and Benchmark Correction

| Parameter | Default | Description |
|-----------|---------|-------------|
| `GENIE_CORRECT_CONFIRMATION_THRESHOLD` | 2 | Evals confirming `genie_correct` before auto-correction |
| `NEITHER_CORRECT_REPAIR_THRESHOLD` | 2 | `neither_correct` verdicts before GT repair attempt |
| `NEITHER_CORRECT_QUARANTINE_THRESHOLD` | 3 | Consecutive `neither_correct` + failed repair before quarantine |

---

## Per-Question Persistence and Escalation

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PERSISTENCE_MIN_FAILURES` | 2 | Non-passing evals before question appears in persistence summary |
| `TVF_REMOVAL_MIN_ITERATIONS` | 2 | Consecutive failing iterations before TVF removal considered |
| `TVF_REMOVAL_BLAME_THRESHOLD` | 2 | Iterations TVF blamed in ASI for auto-removal |
| `REPEATABILITY_EXTRA_QUERIES` | 2 | Extra Genie queries per question during repeatability test |

---

## LLM Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LLM_ENDPOINT` | `databricks-claude-opus-4-6` | Foundation Model API endpoint name |
| `LLM_TEMPERATURE` | 0 | Deterministic output |
| `LLM_MAX_RETRIES` | 3 | Retries on LLM failures |

---

## Benchmark Generation

| Parameter | Default | Description |
|-----------|---------|-------------|
| `TARGET_BENCHMARK_COUNT` | 20 | Number of evaluation questions to generate |
| `COVERAGE_GAP_SOFT_CAP_FACTOR` | 1.5 | Max benchmarks = target * factor for gap fill |
| `BENCHMARK_CATEGORIES` | 7 categories | aggregation, ranking, time-series, comparison, detail, list, threshold |

---

## Environment Variable Overrides

These environment variables are read at module import time:

| Env Variable | Config Parameter | Default |
|-------------|-----------------|---------|
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT` | `PROPAGATION_WAIT_SECONDS` | `30` |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING` | `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS` | `90` |
| `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` | Debug logging toggle | `true` |
| `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` | Max retry attempts per eval | `4` |
| `GENIE_SPACE_OPTIMIZER_CATALOG` | Catalog for state tables | `main` |
| `GENIE_SPACE_OPTIMIZER_SCHEMA` | Schema for state tables | `genie_optimization` |
| `GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID` | SQL Warehouse ID | (required) |
| `GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS` | Finalize heartbeat interval | `30` |
| `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` | Finalize soft timeout | `6600` |

---

## Delta Table Names

| Constant | Table Name | Purpose |
|----------|-----------|---------|
| `TABLE_RUNS` | `genie_opt_runs` | Run lifecycle |
| `TABLE_STAGES` | `genie_opt_stages` | Stage timeline |
| `TABLE_ITERATIONS` | `genie_opt_iterations` | Per-eval scores |
| `TABLE_PATCHES` | `genie_opt_patches` | Patch log |
| `TABLE_ASI` | `genie_eval_asi_results` | Judge feedback |
| `TABLE_PROVENANCE` | `genie_opt_provenance` | Provenance chain |
| `TABLE_DATA_ACCESS_GRANTS` | `genie_opt_data_access_grants` | UC grant ledger |

---

## Tuning Guidance

### Faster iterations (for testing)

```python
TARGET_BENCHMARK_COUNT = 10  # fewer benchmarks
MAX_ITERATIONS = 2           # fewer iterations
RATE_LIMIT_SECONDS = 8       # faster API calls (if not rate-limited)
```

### More thorough optimization

```python
TARGET_BENCHMARK_COUNT = 30  # more comprehensive evaluation
MAX_ITERATIONS = 5           # more iteration opportunities
DIMINISHING_RETURNS_EPSILON = 1.0  # smaller improvements still accepted
```

### Stricter quality gates

```python
REGRESSION_THRESHOLD = 3.0   # tighter rollback threshold
MAX_NOISE_FLOOR = 3.0        # reject smaller improvements as noise
```

---

Next: [Appendix B -- Troubleshooting](B-troubleshooting.md)
