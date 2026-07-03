# Databricks notebook source
# MAGIC %md
# MAGIC # Task 02: Baseline Eval & Triage (GSO v2 — 5-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 3 of 5 — `02_baseline_eval_and_triage` |
# MAGIC | **Reads** | repaired/validated benchmark, attempt budget |
# MAGIC | **Writes** | `genie_opt_iterations` (iter 0), `genie_opt_stages` |
# MAGIC | **Hard stop** | none — the loop always runs at least the attempt-1 coverage pass |
# MAGIC | **Log label** | `[TASK-02 BASELINE]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §6)
# MAGIC
# MAGIC Freeze the baseline (iteration 0) before any repair choice is made and
# MAGIC seed the loop state for `03_optimize`. The eval/RCA logic is unchanged
# MAGIC from Phases 1–3 — this is a **rename + rewire** of the old `baseline_eval`
# MAGIC task into the new DAG. Bootstraps from job parameters + Delta
# MAGIC (no taskValues — D9).

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast


from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE, MAX_BENCHMARK_COUNT
from genie_space_optimizer.common.genie_client import (
    configure_connection_pool,
    configure_mlflow_connection_pool,
    fetch_space_config,
)
from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs
from genie_space_optimizer.common.warehouse import export_warehouse_id, resolve_warehouse_id
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.benchmarks import benchmark_corpus_for_optimization
from genie_space_optimizer.optimization.harness import (
    baseline_display_scorecard,
    baseline_persist_state,
    baseline_run_evaluation,
    baseline_setup_scorers,
)
from genie_space_optimizer.optimization.preflight import (
    _resolve_experiment_path,
    preflight_collect_uc_metadata,
)
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    write_stage,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-02 BASELINE"
_TASK_KEY = "02_baseline_eval_and_triage"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("max_attempts", "3")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
domain = dbutils.widgets.get("domain").strip() or "default"
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
max_attempts = int(dbutils.widgets.get("max_attempts") or "3")
target_accuracy = float(dbutils.widgets.get("target_accuracy") or "0.90")
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("02_baseline_eval_and_triage: run_id parameter is required")

exp_name = _resolve_experiment_path(space_id=space_id, domain=domain)

# COMMAND ----------

w = make_workspace_client()
spark = cast(Any, globals().get("spark"))
configure_connection_pool(w, CONNECTION_POOL_SIZE)
configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

warehouse_id = resolve_warehouse_id(dbutils.widgets.get("warehouse_id").strip())
if warehouse_id:
    export_warehouse_id(warehouse_id)
    os.environ["GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID"] = warehouse_id

ensure_optimization_tables(spark, catalog, schema)
if catalog:
    spark.sql(f"USE CATALOG `{catalog}`")
if schema:
    spark.sql(f"USE SCHEMA `{schema}`")

import mlflow
try:
    mlflow.set_experiment(exp_name)
    mlflow.openai.autolog()
except Exception as exc:  # MLflow tracing is optional in v2 (D7)
    _log("MLflow experiment unavailable (tracing disabled)", reason=str(exc))

write_stage(
    spark, run_id, "BASELINE_EVAL_AND_TRIAGE", "STARTED",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 02a — Load benchmarks (persisted by 01)

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
_all_benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
benchmarks = benchmark_corpus_for_optimization(_all_benchmarks)
_log("Loaded benchmarks", total=len(_all_benchmarks), assessed=len(benchmarks))
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 02b — Baseline evaluation (frozen reference for the loop)

# COMMAND ----------

try:
    _banner("Evaluation Setup")
    setup_ctx = baseline_setup_scorers(
        w, spark, space_id, run_id, catalog, schema, exp_name, None, domain,
    )

    _baseline_config = fetch_space_config(w, space_id)
    _genie_table_refs = extract_genie_space_table_refs(_baseline_config)
    preflight_collect_uc_metadata(
        w, spark, run_id, catalog, schema,
        _baseline_config, _baseline_config, _genie_table_refs,
    )

    _banner("Running Baseline Evaluation")
    eval_result = baseline_run_evaluation(
        spark, run_id, catalog, schema, benchmarks, setup_ctx, w=w,
        max_benchmark_count=MAX_BENCHMARK_COUNT,
    )
    scorecard = baseline_display_scorecard(eval_result)
    _log(
        "Baseline scored",
        overall_accuracy=scorecard["overall_accuracy"],
        thresholds_met=scorecard["thresholds_met"],
    )
except Exception as exc:
    _banner("Baseline Evaluation FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "BASELINE_EVAL_AND_TRIAGE", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 02c — Persist iteration 0

# COMMAND ----------

model_id = eval_result.get("model_id", "")
try:
    baseline_out = baseline_persist_state(
        w, spark, run_id, model_id, catalog, schema, eval_result, scorecard,
        config_snapshot=_baseline_config,
    )
    _baseline_accuracy = baseline_out["overall_accuracy"]
    _log("Baseline persisted (iteration 0)", overall_accuracy=_baseline_accuracy)
except Exception as exc:
    _banner("Baseline Persistence FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "BASELINE_EVAL_AND_TRIAGE", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

write_stage(
    spark, run_id, "BASELINE_EVAL_AND_TRIAGE", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={"baseline_accuracy": _baseline_accuracy},
)

_banner("Task 02 Completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "baseline_accuracy": _baseline_accuracy,
}, default=str))
