# Databricks notebook source
# MAGIC %md
# MAGIC # Optimize (GSO v2 — 4-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `optimize` |
# MAGIC | **Reads** | benchmarks, live Genie Space config |
# MAGIC | **Writes** | `genie_opt_iterations`, `genie_opt_patches`, `genie_opt_stages` |
# MAGIC | **Log label** | `[TASK OPTIMIZE]` |
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Run the native-only optimization loop:
# MAGIC
# MAGIC 1. Evaluate the current space through the Genie Benchmark API and persist
# MAGIC    iteration 0.
# MAGIC 2. If the target is not met, ask the LLM for one targeted patch set.
# MAGIC 3. Apply, evaluate the full benchmark set, accept only on improvement, and
# MAGIC    rollback non-improving candidates.
# MAGIC 4. Stamp the terminal reason on the champion row for `publish_and_audit`.
# MAGIC
# MAGIC Bootstraps from job parameters + Delta only (no taskValues).

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast

import mlflow

from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE
from genie_space_optimizer.common.genie_client import (
    configure_connection_pool,
    configure_mlflow_connection_pool,
)
from genie_space_optimizer.common.warehouse import export_warehouse_id, resolve_warehouse_id
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.benchmarks import benchmark_corpus_for_optimization
from genie_space_optimizer.optimization.benchmarking import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.preflight import _resolve_experiment_path
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    write_stage,
)
from genie_space_optimizer.optimization.unified_loop import (
    run_unified_optimization_loop,
    target_accuracy_percent,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK OPTIMIZE"
_TASK_KEY = "optimize"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("levers", "[1,2,3,4,5,6]")
dbutils.widgets.text("max_attempts", "3")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
domain = dbutils.widgets.get("domain").strip() or "default"
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
apply_mode = dbutils.widgets.get("apply_mode").strip() or "genie_config"
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5,6]")
max_attempts = int(dbutils.widgets.get("max_attempts") or "3")
target_accuracy = target_accuracy_percent(
    float(dbutils.widgets.get("target_accuracy") or "0.90")
)
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("optimize: run_id parameter is required")

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

try:
    mlflow.set_experiment(exp_name)
    mlflow.openai.autolog()
except Exception as exc:
    _log("MLflow experiment unavailable (tracing disabled)", reason=str(exc))

write_stage(
    spark,
    run_id,
    "OPTIMIZE",
    "STARTED",
    task_key=_TASK_KEY,
    catalog=catalog,
    schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Benchmarks

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
_all_benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
benchmarks = benchmark_corpus_for_optimization(_all_benchmarks)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

_log(
    "Optimize inputs",
    run_id=run_id,
    benchmarks=len(benchmarks),
    levers=levers,
    max_attempts=max_attempts,
    target_accuracy=target_accuracy,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Native-Only Optimization Loop

# COMMAND ----------

try:
    _banner("Running unified native-only optimization loop")
    loop_out = run_unified_optimization_loop(
        w,
        spark,
        run_id=run_id,
        space_id=space_id,
        benchmarks=benchmarks,
        catalog=catalog,
        schema=schema,
        levers=levers,
        max_attempts=max_attempts,
        target_accuracy=target_accuracy,
        apply_mode=apply_mode,
    )
    _log(
        "Optimize loop finished",
        accuracy=loop_out.get("accuracy"),
        iteration_counter=loop_out.get("iteration_counter"),
        best_iteration=loop_out.get("best_iteration"),
        terminal_reason=loop_out.get("terminal_reason"),
        surgical_attempts_used=loop_out.get("surgical_attempts_used"),
        levers_accepted=loop_out.get("levers_accepted"),
        levers_rolled_back=loop_out.get("levers_rolled_back"),
    )
except Exception as exc:
    _banner("Optimize loop FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    write_stage(
        spark,
        run_id,
        "OPTIMIZE",
        "FAILED",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
        error_message=str(exc),
    )
    raise

write_stage(
    spark,
    run_id,
    "OPTIMIZE",
    "COMPLETE",
    task_key=_TASK_KEY,
    catalog=catalog,
    schema=schema,
    detail={
        "accuracy": loop_out.get("accuracy"),
        "best_iteration": loop_out.get("best_iteration"),
        "iteration_counter": loop_out.get("iteration_counter"),
        "terminal_reason": loop_out.get("terminal_reason"),
        "surgical_attempts_used": loop_out.get("surgical_attempts_used"),
    },
)

_banner("Optimize completed")
dbutils.notebook.exit(
    json.dumps(
        {
            "run_id": run_id,
            "accuracy": loop_out.get("accuracy"),
            "best_iteration": loop_out.get("best_iteration"),
            "terminal_reason": loop_out.get("terminal_reason"),
        },
        default=str,
    )
)
