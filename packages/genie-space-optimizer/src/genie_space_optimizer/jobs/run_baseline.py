# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Baseline Evaluation — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 2 of 6 — Baseline Evaluation |
# MAGIC | **Harness function** | `_run_baseline()` in `optimization/harness.py` |
# MAGIC | **Reads from** | `preflight` (task values) |
# MAGIC | **Publishes to** | `lever_loop` (scores, accuracy, thresholds_met, model_id) |
# MAGIC | **Typical duration** | 5–20 min |
# MAGIC | **Log label** | `[TASK-2 BASELINE]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC This task establishes the **quality baseline** for the Genie Space before any optimization. It runs the full 9-judge evaluation suite against the benchmark dataset and records scores that the downstream **lever_loop** task will use to measure improvement.
# MAGIC
# MAGIC ## 🏗️ DAG Position
# MAGIC
# MAGIC | Step | Task | Status | Reads From | Publishes To |
# MAGIC |:----:|------|:------:|------------|--------------|
# MAGIC | 1 | preflight | Done | widgets | all tasks |
# MAGIC | 2 | **baseline_eval** | **⬅️ THIS TASK** | preflight | enrichment |
# MAGIC | 3 | enrichment | Next | preflight + baseline | lever_loop |
# MAGIC | 4 | lever_loop | Pending | preflight + baseline + enrichment | finalize |
# MAGIC | 5 | finalize | Pending | lever_loop | deploy |
# MAGIC | 6 | deploy | Pending | preflight + finalize | *(terminal)* |
# MAGIC
# MAGIC ## 9-Judge Scoring System
# MAGIC
# MAGIC The evaluation uses 9 custom scorers passed to `mlflow.genai.evaluate(scorers=all_scorers)`:
# MAGIC
# MAGIC | Judge | Purpose |
# MAGIC |-------|---------|
# MAGIC | syntax_validity | SQL parses and executes without error |
# MAGIC | schema_accuracy | Correct tables, columns, joins |
# MAGIC | logical_accuracy | Correct aggregations, filters, GROUP BY, ORDER BY |
# MAGIC | semantic_equivalence | Same business metric even if written differently |
# MAGIC | completeness | No missing dimensions, measures, or filters |
# MAGIC | response_quality | Overall quality of Genie's natural-language analysis and SQL response |
# MAGIC | result_correctness | Query results match expected (hash/signature comparison) |
# MAGIC | asset_routing | Correct asset type (TABLE/MV/TVF) selected |
# MAGIC | arbiter | Tie-breaker when results disagree (LLM-based) |
# MAGIC
# MAGIC ## ⚠️ What Happens If This Task Fails
# MAGIC
# MAGIC > **⚠️ Warning:** If baseline fails, the job stops — lever_loop never runs.
# MAGIC
# MAGIC - Delta state is updated with `BASELINE_EVAL` = FAILED
# MAGIC - Run status is set to FAILED with `convergence_reason=error_in_BASELINE_EVAL`
# MAGIC - This task is expected to **fail loudly** when infrastructure-level evaluation issues are detected (see Known Failure Modes below)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 What `_run_baseline` Does Internally
# MAGIC
# MAGIC | Step | Action | Key Function | Output |
# MAGIC |:----:|--------|-------------|--------|
# MAGIC | 1 | Create predict function | `make_predict_fn(w, space_id, spark, catalog, schema)` | closure (rate-limits, calls Genie, executes SQL, normalizes, compares hashes) |
# MAGIC | 2 | Assemble 9 scorers | `make_all_scorers(w, spark, catalog, schema)` | scorer list for `mlflow.genai.evaluate()` |
# MAGIC | 3 | Run evaluation with retry | `run_evaluation()` → `_run_evaluate_with_retries()` | scores, accuracy (up to 4 attempts, exponential backoff, single-worker fallback) |
# MAGIC | 4 | Check thresholds | `all_thresholds_met(scores, MLFLOW_THRESHOLDS)` | `thresholds_met` boolean |
# MAGIC | 5 | Write state | Delta iteration 0, link scores to model, update run status | Delta records |
# MAGIC
# MAGIC > **📝 Note:** Benchmark quarantine (SQL/routine gating) is now integrated inside `run_evaluation()` — invalid benchmarks are quarantined within the shared evaluation path rather than as a separate pre-step.
# MAGIC
# MAGIC ### Labeling Session and Expectations
# MAGIC
# MAGIC After evaluation, `log_expectations_on_traces()` logs each benchmark's expected SQL as an MLflow expectation on the corresponding trace, giving human reviewers ground-truth context. If failures are detected, a **labeling session** is created via `create_review_session()` using the evaluation's MLflow run IDs for reliable trace population. The session URL is persisted to the `genie_opt_runs` Delta table and surfaced in the application UI as a "Human Review" link.
# MAGIC
# MAGIC Results flow to lever_loop via task values: `scores`, `overall_accuracy`, `thresholds_met`, `model_id` are consumed to decide whether to proceed and to compare against post-lever scores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Known Failure Modes (Read This If Baseline Fails Often)
# MAGIC
# MAGIC This task fails more often than others because it touches MLflow GenAI harness, Spark SQL, and Genie APIs. Below are the main failure modes and how to handle them.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 CRITICAL: `AttributeError: 'NoneType' object has no attribute 'info'`
# MAGIC
# MAGIC **Cause:** Transient bug inside `mlflow.genai.evaluation.harness`. The harness sometimes receives `eval_item.trace` or `eval_item.trace.info` as `None` and accesses `.info` or `.assessments` on it.
# MAGIC
# MAGIC **Handling:** The evaluation engine wraps `mlflow.genai.evaluate()` in `_run_evaluate_with_retries()`. Exceptions matching this pattern are classified as **retryable** via `_is_retryable_eval_exception()`. The engine will:
# MAGIC - Retry up to 4 times (configurable via `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS`)
# MAGIC - Sleep 10s × attempt between retries (configurable via `GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS`)
# MAGIC - Fall back to single-worker mode on retries (`GENIE_SPACE_OPTIMIZER_EVAL_RETRY_WORKERS=1`) to reduce concurrency-related races
# MAGIC
# MAGIC **Interpretation:** If you see this error in logs and the task still fails, it means all retries exhausted. Check:
# MAGIC - Cluster size and load (try a larger cluster or fewer parallel jobs)
# MAGIC - MLflow/GenAI service health
# MAGIC - Increase `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` (e.g. 6) and `GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS` (e.g. 15)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 CRITICAL: Infrastructure SQL Errors (Catalog/Schema Context)
# MAGIC
# MAGIC **Cause:** Spark SQL context not set correctly. Errors like:
# MAGIC - `"TABLE_OR_VIEW_NOT_FOUND"` / `"not in the current catalog"`
# MAGIC - `"please set the current catalog"`
# MAGIC - `"catalog does not exist"` / `"schema does not exist"`
# MAGIC - `"permission denied"` / `"insufficient_permissions"`
# MAGIC
# MAGIC These are detected by `_is_infrastructure_sql_error()` in the evaluation module.
# MAGIC
# MAGIC **Handling:** `_run_baseline()` calls `_ensure_sql_context(spark, catalog, schema)` before evaluation to set `USE CATALOG` and `USE SCHEMA`. If you still see these errors:
# MAGIC - Verify the job runs with a user/service principal that has `USE CATALOG` and `USE SCHEMA` on the target catalog/schema
# MAGIC - Ensure the catalog and schema exist and are accessible from the cluster
# MAGIC - Check that benchmark `expected_sql` uses `{catalog}.{schema}` template variables correctly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 WARNING: `FAIL_ON_INFRA_EVAL_ERRORS` Behavior
# MAGIC
# MAGIC **Env var:** `GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS` (default: `true`)
# MAGIC
# MAGIC When `true`, if **any** evaluation row contains an infrastructure SQL error (detected in `outputs/comparison/error` or scorer rationale/metadata), the evaluation **fails immediately** with:
# MAGIC
# MAGIC ```
# MAGIC RuntimeError: Infrastructure SQL errors detected during evaluation: <first 3 error messages>
# MAGIC ```
# MAGIC
# MAGIC This is intentional: we do not want to report "passing" scores when some rows failed due to catalog/schema/permission issues.
# MAGIC
# MAGIC **To allow evaluation to complete despite infra errors** (e.g. for debugging): set `GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS=false`. Scores will still be computed for rows that succeeded; infra errors will be logged but will not abort the run.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 WARNING: gRPC / Spark Connect Timeouts
# MAGIC
# MAGIC **Cause:** Transient network or Spark Connect issues during scorer execution.
# MAGIC
# MAGIC **Handling:** These are also classified as retryable. If `_multithreadedrendezvous` or `grpc` appears in the error message, the retry loop will attempt again.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Interpret Error Logs
# MAGIC
# MAGIC 1. Look for `[TASK-2 BASELINE] Failure details` — it logs `error_type`, `error_message`, and full `traceback`
# MAGIC 2. Check `_eval_attempts` on the exception (if present) — shows each attempt's status, workers, and error
# MAGIC 3. Check MLflow run artifacts: `evaluation_failure/infrastructure_sql_errors.json` if infra errors were detected
# MAGIC 4. Check Delta `genie_opt_stages` for `BASELINE_EVAL` = FAILED and the `error_message` column
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | `AttributeError` / `NoneType` / `info` | Retry; increase `EVAL_MAX_ATTEMPTS`; use single-worker fallback |
# MAGIC | `not in the current catalog` | Fix catalog/schema context; verify permissions |
# MAGIC | `permission denied` | Grant `USE CATALOG`, `USE SCHEMA`, `SELECT` on benchmark tables |
# MAGIC | gRPC / timeout | Retry; check cluster/network; reduce benchmark count for large runs |
# MAGIC | No benchmarks found | Ensure `genie_benchmarks_{domain}` exists and has rows for the domain |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports and Helpers
# MAGIC
# MAGIC - `_ts()` — UTC timestamp for log lines
# MAGIC - `_banner()` — Section separator with task label
# MAGIC - `_log()` — Structured logging with optional JSON payload

# COMMAND ----------

import json
import traceback
from functools import partial
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import (
    baseline_setup_scorers,
    baseline_run_evaluation,
    baseline_display_scorecard,
    baseline_persist_state,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-2 BASELINE"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Read Upstream Task Values from Preflight
# MAGIC
# MAGIC All values required for baseline evaluation are published by the preflight task.
# MAGIC
# MAGIC > **⚠️ Warning:** If any key is missing, `dbutils.jobs.taskValues.get()` returns `None` and downstream logic will fail with a clear error.

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

# Read task values from preflight
run_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")
space_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="space_id")
domain = dbutils.jobs.taskValues.get(taskKey="preflight", key="domain")
catalog = dbutils.jobs.taskValues.get(taskKey="preflight", key="catalog")
schema = dbutils.jobs.taskValues.get(taskKey="preflight", key="schema")
exp_name = dbutils.jobs.taskValues.get(taskKey="preflight", key="experiment_name")

import os as _os
_warehouse_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="warehouse_id", default="")
if _warehouse_id:
    _os.environ["GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID"] = _warehouse_id

import mlflow
mlflow.set_experiment(exp_name)

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Load Benchmarks
# MAGIC
# MAGIC Benchmarks are loaded from the UC table `{catalog}.{schema}.genie_benchmarks_{domain}`. Each row is a benchmark question with `question`, `expected_sql`, `expected_asset`, and metadata.
# MAGIC
# MAGIC > **⚠️ Warning:** If no benchmarks are found, the task raises immediately.

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
benchmarks = [b for b in benchmarks if b.get("split") != "held_out"]
_banner("Loaded Benchmarks")
_log("Benchmark dataset (train only)", uc_schema=uc_schema, domain=domain, benchmark_count=len(benchmarks))
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a: Evaluation Setup
# MAGIC
# MAGIC Create the Genie predict function (rate-limited, calls Genie API, executes SQL,
# MAGIC normalizes results, compares hashes) and assemble the 9 custom scorers for
# MAGIC `mlflow.genai.evaluate()`. This cell also sets the SQL context (`USE CATALOG`,
# MAGIC `USE SCHEMA`) and writes the `BASELINE_EVAL_STARTED` stage to Delta.

# COMMAND ----------

try:
    _banner("Evaluation Setup")
    setup_ctx = baseline_setup_scorers(
        w, spark, space_id, run_id, catalog, schema, exp_name, None, domain,
    )
    _log("Setup complete", scorers=len(setup_ctx["scorers"]))
except Exception as exc:
    _banner("Baseline Setup FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b: Run 9-Judge Evaluation
# MAGIC
# MAGIC Run the full evaluation suite against all benchmarks. The evaluation engine
# MAGIC wraps `mlflow.genai.evaluate()` with retry logic (up to 4 attempts with
# MAGIC exponential backoff and single-worker fallback). Benchmark quarantine
# MAGIC (SQL/routine gating) is integrated inside the evaluation path.

# COMMAND ----------

from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs
from genie_space_optimizer.optimization.preflight import preflight_collect_uc_metadata

_baseline_config = fetch_space_config(w, space_id)
_genie_table_refs = extract_genie_space_table_refs(_baseline_config)
_uc_ctx = preflight_collect_uc_metadata(
    w, spark, run_id, catalog, schema,
    _baseline_config, _baseline_config,
    _genie_table_refs,
)
_log(
    "Local config + UC metadata collected for model creation",
    table_refs=len(_genie_table_refs),
    uc_columns=len(_uc_ctx.get("uc_columns") or []),
    uc_tags=len(_uc_ctx.get("uc_tags") or []),
    uc_routines=len(_uc_ctx.get("uc_routines") or []),
)

_baseline_model_kwargs = {
    "w": w,
    "space_id": space_id,
    "config": _baseline_config,
    "iteration": 0,
    "domain": domain,
    "experiment_name": exp_name,
    "uc_schema": f"{catalog}.{schema}",
    "uc_columns": _uc_ctx.get("uc_columns"),
    "uc_tags": _uc_ctx.get("uc_tags"),
    "uc_routines": _uc_ctx.get("uc_routines"),
}

try:
    _banner("Running 9-Judge Evaluation")
    eval_result = baseline_run_evaluation(
        spark, run_id, catalog, schema, benchmarks, setup_ctx, w=w,
        model_creation_kwargs=_baseline_model_kwargs,
    )
    _log("Evaluation complete", overall_accuracy=eval_result.get("overall_accuracy", 0.0))
except Exception as exc:
    _banner("Baseline Evaluation FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2c: Baseline Scorecard
# MAGIC
# MAGIC Display the per-judge scorecard showing each judge's score against its
# MAGIC threshold. This gives a clear picture of where the Genie Space stands
# MAGIC before any optimization and which quality dimensions need improvement.

# COMMAND ----------

scorecard = baseline_display_scorecard(eval_result)
_log(
    "Scorecard",
    overall_accuracy=scorecard["overall_accuracy"],
    thresholds_met=scorecard["thresholds_met"],
    judge_count=len(scorecard["scores"]),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2d: State Persistence and Labeling
# MAGIC
# MAGIC Write the baseline iteration to Delta, link evaluation scores to the MLflow
# MAGIC model, update the run status with the best accuracy, write the
# MAGIC `BASELINE_EVAL_STARTED -> COMPLETE` stage record, and log expected SQL
# MAGIC and judge verdicts on evaluation traces for human reviewers.

# COMMAND ----------

model_id = eval_result.get("model_id", "")

try:
    _banner("Persisting Baseline State")
    baseline_out = baseline_persist_state(
        w, spark, run_id, model_id, catalog, schema, eval_result, scorecard,
    )
    _log(
        "Baseline finished",
        overall_accuracy=baseline_out["overall_accuracy"],
        thresholds_met=baseline_out["thresholds_met"],
        model_id=baseline_out["model_id"],
        judge_count=len(baseline_out.get("scores", {})),
    )
except Exception as exc:
    _banner("Baseline Persistence FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Publish Task Values for Downstream
# MAGIC
# MAGIC The lever_loop task reads these keys via `dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="...")`. All values must be published for the DAG to proceed correctly.

# COMMAND ----------

_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="scores", value=json.dumps(baseline_out["scores"]))
dbutils.jobs.taskValues.set(key="overall_accuracy", value=baseline_out["overall_accuracy"])
dbutils.jobs.taskValues.set(key="thresholds_met", value=baseline_out["thresholds_met"])
dbutils.jobs.taskValues.set(key="model_id", value=baseline_out["model_id"])
dbutils.jobs.taskValues.set(key="mlflow_run_id", value=eval_result.get("mlflow_run_id", ""))

_log(
    "Task values published",
    overall_accuracy=baseline_out["overall_accuracy"],
    thresholds_met=baseline_out["thresholds_met"],
    model_id=baseline_out["model_id"],
)
_banner("Task 2 Completed")
dbutils.notebook.exit(json.dumps({
    "overall_accuracy": baseline_out["overall_accuracy"],
    "thresholds_met": baseline_out["thresholds_met"],
}, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What Success Looks Like
# MAGIC
# MAGIC When this task completes successfully, you will see output similar to:
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-2 BASELINE] Running _run_baseline
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:15:12 UTC] [TASK-2 BASELINE] Baseline finished
# MAGIC   {"overall_accuracy": 72.5, "thresholds_met": false, "model_id": "mv-abc123", "judge_count": 9}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-2 BASELINE] Publishing Task Values
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:15:23 UTC] [TASK-2 BASELINE] Task values published
# MAGIC   {"overall_accuracy": 72.5, "thresholds_met": false, "model_id": "mv-abc123"}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-2 BASELINE] Task 2 Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC ```
# MAGIC
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC Task 2 (Baseline Evaluation) has completed successfully. The lever_loop task will use the published scores and thresholds to:
# MAGIC - Decide whether baseline already meets thresholds (early exit possible)
# MAGIC - Compare post-lever scores against baseline to detect regressions
# MAGIC - Track the best model and accuracy across iterations
