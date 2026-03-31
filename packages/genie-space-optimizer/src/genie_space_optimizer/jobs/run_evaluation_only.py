# Databricks notebook source
# MAGIC %md
# MAGIC # Standalone Evaluation — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | Standalone — **Not part of the 6-task DAG** |
# MAGIC | **Entry point** | `run_evaluation_via_job()` in `optimization/harness.py`, or manual run |
# MAGIC | **Shares code with** | Task 2 (baseline_eval) — same `run_evaluation()`, `make_predict_fn()`, `make_all_scorers()` |
# MAGIC | **Results via** | `dbutils.notebook.exit(accuracy)` + MLflow experiment runs |
# MAGIC | **Typical duration** | 5–30 min |
# MAGIC | **Log label** | `[EVALUATION]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC This notebook runs evaluation **independently** — it is **not** part of the 6-task optimization DAG (preflight → baseline_eval → enrichment → lever_loop → finalize → deploy). It is a self-contained evaluation runner used when evaluation needs to run in isolation.
# MAGIC
# MAGIC ## 🏗️ Relationship to the DAG
# MAGIC
# MAGIC > **⚠️ Warning:** This notebook is **NOT** a DAG task. It runs as a separate Databricks Job.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  6-Task DAG (optimization pipeline)                                    │
# MAGIC │  preflight → baseline → enrichment → lever_loop → finalize → deploy  │
# MAGIC │  ↑ Uses run_evaluation() inline within _run_baseline()     │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  Standalone Evaluation (this notebook)                      │
# MAGIC │  NOT part of the DAG — runs as a separate Databricks Job   │
# MAGIC │  ↑ Submitted by run_evaluation_via_job() in harness.py     │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC This notebook shares the same `run_evaluation()` function, `make_predict_fn()`, and `make_all_scorers()` used by the DAG's baseline and lever_loop tasks. The only difference is the **execution context**: it runs as its own Databricks Job rather than as a task within the optimization DAG.
# MAGIC
# MAGIC ## Who Calls This Notebook
# MAGIC
# MAGIC | Caller | How | When |
# MAGIC |--------|-----|------|
# MAGIC | `run_evaluation_via_job()` | Submits this notebook as a `w.jobs.submit()` run with widget parameters | When evaluation is executed as a separate task (e.g., for Serverless compute or multi-task architecture) |
# MAGIC | **Manual runs** | Operators run directly in Databricks with widget parameters | Ad-hoc evaluation, debugging, or re-evaluation of a specific space |
# MAGIC
# MAGIC ## How Results Flow Back
# MAGIC
# MAGIC - The notebook calls `dbutils.notebook.exit(accuracy)` with the overall accuracy as a string.
# MAGIC - The caller (`run_evaluation_via_job`) polls the job status via `w.jobs.get_run()` and reads the exit value from the completed run.
# MAGIC - Evaluation results are also persisted in MLflow (experiment runs, metrics, artifacts) and are accessible independently of the notebook exit value.
# MAGIC
# MAGIC ## ⚙️ Widget Parameters
# MAGIC
# MAGIC | Parameter | Type | Default | Description |
# MAGIC |-----------|------|---------|-------------|
# MAGIC | `space_id` | text | `""` | Genie Space ID to evaluate |
# MAGIC | `experiment_name` | text | `""` | MLflow experiment name for logging |
# MAGIC | `iteration` | text | `"0"` | Iteration number (for MLflow tagging) |
# MAGIC | `domain` | text | `""` | Domain (e.g., for benchmark filtering) |
# MAGIC | `model_id` | text | `""` | Optional model ID to associate with this eval |
# MAGIC | `eval_scope` | text | `"full"` | Evaluation scope: `full`, `slice`, or `p0` |
# MAGIC | `catalog` | text | `""` | Unity Catalog catalog for benchmarks and state |
# MAGIC | `schema` | text | `""` | Unity Catalog schema for benchmarks and state |
# MAGIC
# MAGIC ## SQL Context Setup
# MAGIC
# MAGIC > **📝 Note:** Before creating the predict function and running evaluation, the notebook sets the Spark SQL context (`USE CATALOG` / `USE SCHEMA`) via `_ensure_sql_context()`. This is required for SQL Connect stability when querying Unity Catalog tables and running Genie predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Serialize payloads for structured logging |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `partial` | Bind `_TASK_LABEL` to shared `_banner` and `_log` helpers |
# MAGIC | `WorkspaceClient` | Databricks SDK — Genie API calls for predict function |
# MAGIC | `SparkSession` | SQL execution for predict function and SQL context setup |
# MAGIC | `_banner`, `_log` | Shared logging helpers from `_helpers.py` |
# MAGIC | `load_benchmarks_from_dataset` | Load benchmarks from UC table |
# MAGIC | `make_predict_fn` | Create the Genie predict closure for `mlflow.genai.evaluate()` |
# MAGIC | `run_evaluation` | Run the full evaluation with retry and threshold checking |
# MAGIC | `_ensure_sql_context` | Set Spark SQL catalog/schema context for SQL Connect stability |
# MAGIC | `make_all_scorers` | Create the 8-judge scorer list for `mlflow.genai.evaluate()` |
# MAGIC
# MAGIC ### Helper Functions
# MAGIC
# MAGIC | Function | What It Does |
# MAGIC |----------|---------------|
# MAGIC | `_banner(title)` | Prints a 120-char separator and `[EVALUATION] {title}` for visual section breaks |
# MAGIC | `_log(event, **payload)` | Logs `event` with optional JSON payload; uses `default=str` for non-JSON-serializable values |

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.evaluation import (
    load_benchmarks_from_dataset,
    make_predict_fn,
    run_evaluation,
)
from genie_space_optimizer.optimization.harness import _ensure_sql_context
from genie_space_optimizer.optimization.scorers import make_all_scorers

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "EVALUATION"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widget Parameter Resolution
# MAGIC
# MAGIC Widgets are read from `dbutils.widgets.get(...)`. When submitted via `run_evaluation_via_job()`, these are passed as `base_parameters` in the notebook task. When run manually, operators fill them in the Databricks notebook UI.

# COMMAND ----------

dbutils.widgets.text("space_id", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("iteration", "0")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("model_id", "")
dbutils.widgets.text("eval_scope", "full")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

space_id = dbutils.widgets.get("space_id")
experiment_name = dbutils.widgets.get("experiment_name")
iteration = int(dbutils.widgets.get("iteration") or "0")
domain = dbutils.widgets.get("domain")
model_id = dbutils.widgets.get("model_id") or None
eval_scope = dbutils.widgets.get("eval_scope") or "full"
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

_banner("Resolved Widget Inputs")
_log(
    "Parameters",
    space_id=space_id,
    experiment_name=experiment_name,
    iteration=iteration,
    domain=domain,
    model_id=model_id,
    eval_scope=eval_scope,
    catalog=catalog,
    schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Load Benchmarks
# MAGIC
# MAGIC Benchmarks are loaded from the UC table `{catalog}.{schema}.genie_benchmarks_{domain}`. The same benchmark dataset is used by the DAG's baseline and lever_loop tasks.
# MAGIC
# MAGIC > **⚠️ Warning:** If no benchmarks are found, the notebook raises immediately.

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

import mlflow
mlflow.openai.autolog()

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

_banner("Loaded Benchmarks")
_log("Benchmark dataset", uc_schema=uc_schema, domain=domain, benchmark_count=len(benchmarks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 Set SQL Context and Create Predict/Scorers
# MAGIC
# MAGIC - **`_ensure_sql_context()`** — Runs `USE CATALOG` and `USE SCHEMA` so Spark SQL resolves table references correctly.
# MAGIC - **`make_predict_fn()`** — Creates a closure that rate-limits, calls Genie API, executes both expected and generated SQLs, normalizes results, and compares hashes.
# MAGIC - **`make_all_scorers()`** — Creates the 8-judge scorer list (syntax_validity, schema_accuracy, logical_accuracy, semantic_equivalence, completeness, result_correctness, asset_routing, arbiter).

# COMMAND ----------

_ensure_sql_context(spark, catalog, schema)
predict_fn = make_predict_fn(
    w, space_id, spark, catalog, schema,
    warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
)
scorers = make_all_scorers(w, spark, catalog, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Run Evaluation
# MAGIC
# MAGIC `run_evaluation()` wraps `mlflow.genai.evaluate()` with:
# MAGIC
# MAGIC | Feature | Detail |
# MAGIC |---------|--------|
# MAGIC | Retry logic | `_run_evaluate_with_retries()` — up to 4 attempts with exponential backoff |
# MAGIC | Worker fallback | Single-worker mode on retries to reduce concurrency-related races |
# MAGIC | Benchmark quarantine | SQL/routine gating integrated in the evaluation path |
# MAGIC | Threshold checking | Against `MLFLOW_THRESHOLDS` |
# MAGIC | Infra error detection | Fail-fast controlled by `FAIL_ON_INFRA_EVAL_ERRORS` |
# MAGIC
# MAGIC **Returns:** `{overall_accuracy, thresholds_met, scores, ...}`

# COMMAND ----------

try:
    _banner("Running Evaluation")
    result = run_evaluation(
        space_id, experiment_name, iteration, benchmarks,
        domain, model_id, eval_scope,
        predict_fn, scorers,
        spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
    )
    _log(
        "Evaluation complete",
        overall_accuracy=result.get("overall_accuracy"),
        thresholds_met=result.get("thresholds_met"),
    )
except Exception as exc:
    _banner("Evaluation FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

_banner("Evaluation Completed")
_log(
    "Final result",
    overall_accuracy=result["overall_accuracy"],
    thresholds_met=result["thresholds_met"],
)
dbutils.notebook.exit(str(result.get("overall_accuracy", 0.0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Known Failure Modes
# MAGIC
# MAGIC This notebook uses the same evaluation engine as Task 2 (baseline_eval). The same failure modes apply:
# MAGIC
# MAGIC ### 🔴 CRITICAL: `AttributeError: 'NoneType' object has no attribute 'info'`
# MAGIC
# MAGIC **Cause:** Transient bug inside `mlflow.genai.evaluation.harness`.
# MAGIC
# MAGIC **Handling:** Retry logic in `_run_evaluate_with_retries()` — up to 4 attempts with exponential backoff and single-worker fallback.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 CRITICAL: Infrastructure SQL Errors
# MAGIC
# MAGIC **Cause:** Spark SQL context not set correctly, or missing permissions.
# MAGIC
# MAGIC **Handling:** `_ensure_sql_context()` is called before evaluation. If errors persist, check catalog/schema permissions for the service principal.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 WARNING: No Benchmarks Found
# MAGIC
# MAGIC **Cause:** The benchmark table `{catalog}.{schema}.genie_benchmarks_{domain}` is empty or missing.
# MAGIC
# MAGIC **Handling:** Raises immediately with a descriptive error. Ensure benchmarks are generated (via preflight) before running standalone evaluation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | `AttributeError` / `NoneType` / `info` | Retry; increase `EVAL_MAX_ATTEMPTS` |
# MAGIC | `not in the current catalog` | Verify catalog/schema exist and are accessible |
# MAGIC | `permission denied` | Grant `USE CATALOG`, `USE SCHEMA`, `SELECT` on benchmark tables |
# MAGIC | No benchmarks found | Run preflight to generate benchmarks, or check domain name |
# MAGIC | gRPC / timeout | Retry; check cluster/network health |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What Success Looks Like
# MAGIC
# MAGIC When this notebook completes successfully, you will see output similar to:
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [EVALUATION] Running Evaluation
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 14:05:00 UTC] [EVALUATION] Evaluation complete
# MAGIC   {"overall_accuracy": 82.5, "thresholds_met": true}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [EVALUATION] Evaluation Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 14:05:01 UTC] [EVALUATION] Final result
# MAGIC   {"overall_accuracy": 82.5, "thresholds_met": true}
# MAGIC ```
# MAGIC
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC - **Standalone Evaluation** runs the same 8-judge evaluation suite as the DAG's baseline task, but as an independent Databricks Job.
# MAGIC - **Not part of the DAG** — called by `run_evaluation_via_job()` in the harness or run manually by operators.
# MAGIC - **Results flow back** via `dbutils.notebook.exit(accuracy)` and are persisted in MLflow.
# MAGIC - **Shares all code** with the DAG evaluation path: `run_evaluation()`, `make_predict_fn()`, `make_all_scorers()`, `_ensure_sql_context()`.
# MAGIC - **Same failure modes** as Task 2 (baseline_eval) — see that notebook's Known Failure Modes for detailed guidance.
