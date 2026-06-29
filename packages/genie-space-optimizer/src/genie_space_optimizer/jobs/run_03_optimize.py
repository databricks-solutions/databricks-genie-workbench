# Databricks notebook source
# MAGIC %md
# MAGIC # Task 03: Optimize (GSO v2 — 5-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 4 of 5 — `03_optimize` |
# MAGIC | **Reads** | triage artifact, snapshot artifact, baseline iter 0, benchmarks |
# MAGIC | **Writes** | `genie_opt_iterations`, `genie_opt_patches`, `genie_eval_lever_loop_decisions`, `genie_opt_provenance`, `genie_opt_stages` |
# MAGIC | **Log label** | `[TASK-03 OPTIMIZE]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §5 / §6.1)
# MAGIC
# MAGIC `03_optimize` is the controller task that will run the whole bounded
# MAGIC hill-climb as an in-process `while` loop (two-mode coverage/surgical).
# MAGIC
# MAGIC **Phase 7 is a SHELL:** the two-mode controller is Phase 8. For now `03`
# MAGIC temporarily delegates to the existing `_run_lever_loop` so the reshaped
# MAGIC job runs end-to-end and ships green. Because there is no longer a
# MAGIC standalone `enrichment` task, the loop is invoked with
# MAGIC `enrichment_done=False` so its internal proactive-enrichment Phase 1 runs
# MAGIC inside this single task — faithfully collapsing the old
# MAGIC `enrichment + lever_loop` pair into `03` without yet building the
# MAGIC measured/reversible coverage-mode controller (Phase 8).
# MAGIC
# MAGIC Bootstraps from job parameters + Delta only (no taskValues — D9).

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
)
from genie_space_optimizer.common.warehouse import export_warehouse_id, resolve_warehouse_id
from genie_space_optimizer.jobs._handoff import (
    assert_lever_loop_inputs_sane,
    get_baseline_eval_state,
)
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.benchmarks import benchmark_corpus_for_optimization
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_lever_loop
from genie_space_optimizer.optimization.preflight import _resolve_experiment_path
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    load_run,
    write_stage,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-03 OPTIMIZE"
_TASK_KEY = "03_optimize"
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
dbutils.widgets.text("max_iterations", "5")
dbutils.widgets.text("triggered_by", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
domain = dbutils.widgets.get("domain").strip() or "default"
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
apply_mode = dbutils.widgets.get("apply_mode").strip() or "genie_config"
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5,6]")
max_iterations = int(dbutils.widgets.get("max_iterations") or "5")
triggered_by = dbutils.widgets.get("triggered_by").strip()
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("03_optimize: run_id parameter is required")

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

# Production defaults to warn-and-degrade for the loop invariant suite.
os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")

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
    spark, run_id, "OPTIMIZE", "STARTED",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve baseline reference (Delta) + benchmarks

# COMMAND ----------

baseline = get_baseline_eval_state(
    spark, run_id=run_id, catalog=catalog, schema=schema, dbutils=dbutils,
)
_baseline_scores = baseline["scores"].value or {}
_baseline_accuracy = baseline["overall_accuracy"].value
baseline_model_id = baseline["model_id"].value or ""
assert_lever_loop_inputs_sane(
    {"overall_accuracy": baseline["overall_accuracy"], "scores": baseline["scores"]}
)

_run_row = load_run(spark, run_id, catalog, schema) or {}
_human_corrections_raw = _run_row.get("human_corrections_json")
try:
    human_corrections = json.loads(_human_corrections_raw) if _human_corrections_raw else []
except (ValueError, TypeError):
    human_corrections = []

uc_schema = f"{catalog}.{schema}"
_all_benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
benchmarks = benchmark_corpus_for_optimization(_all_benchmarks)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

_log(
    "Optimize inputs",
    run_id=run_id,
    baseline_accuracy=_baseline_accuracy,
    benchmarks=len(benchmarks),
    levers=levers,
    max_iterations=max_iterations,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the optimization loop (Phase 7 shell → existing `_run_lever_loop`)
# MAGIC
# MAGIC `enrichment_done=False`: the standalone `enrichment` task was removed, so
# MAGIC the loop runs its internal proactive-enrichment Phase 1 here. The Phase-8
# MAGIC rebuild replaces this with the two-mode (coverage/surgical) controller and
# MAGIC the per-attempt loop-state Delta commits.

# COMMAND ----------

try:
    _banner("Running _run_lever_loop (Phase 7 shell — pre-two-mode)")
    loop_out = _run_lever_loop(
        w, spark, run_id, space_id, domain, benchmarks, exp_name,
        _baseline_scores, _baseline_accuracy, baseline_model_id, {},
        catalog, schema, levers, max_iterations,
        apply_mode=apply_mode,
        triggered_by=triggered_by,
        human_corrections=human_corrections,
        enrichment_done=False,
        enrichment_model_id="",
        max_benchmark_count=MAX_BENCHMARK_COUNT,
    )
    _log(
        "Optimize loop finished",
        accuracy=loop_out.get("accuracy"),
        iteration_counter=loop_out.get("iteration_counter"),
        best_iteration=loop_out.get("best_iteration"),
        levers_accepted=loop_out.get("levers_accepted"),
        levers_rolled_back=loop_out.get("levers_rolled_back"),
    )
except Exception as exc:
    _banner("Optimize Loop FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "OPTIMIZE", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

_pretty = loop_out.get("pretty_print_transcript")
if _pretty:
    print()
    print(_pretty)
    print()

write_stage(
    spark, run_id, "OPTIMIZE", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={
        "accuracy": loop_out.get("accuracy"),
        "best_iteration": loop_out.get("best_iteration"),
        "iteration_counter": loop_out.get("iteration_counter"),
    },
)

_banner("Task 03 Completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "accuracy": loop_out.get("accuracy"),
    "best_iteration": loop_out.get("best_iteration"),
}, default=str))
