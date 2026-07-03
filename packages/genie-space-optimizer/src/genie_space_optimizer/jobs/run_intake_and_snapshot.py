# Databricks notebook source
# MAGIC %md
# MAGIC # Intake & Snapshot (GSO v2 — 4-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `intake_and_snapshot` |
# MAGIC | **Reads** | job parameters, request metadata, current space config (run-row snapshot) |
# MAGIC | **Writes** | `genie_opt_runs`, `genie_opt_artifacts` (`run_manifest`), `genie_opt_stages` |
# MAGIC | **Hard stop** | abort if snapshot capture fails |
# MAGIC | **Log label** | `[TASK INTAKE]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §6)
# MAGIC
# MAGIC The first task of the GSO v2 linear 4-task DAG splits the old `preflight`
# MAGIC stage in two. `intake_and_snapshot` captures the **rollback snapshot**
# MAGIC (the ORIGINAL `serialized_space`, the discard revert anchor) and writes the
# MAGIC **run manifest**. Benchmark QC + repair moves to `benchmark_qc_and_repair`.
# MAGIC
# MAGIC State handoff is via Delta only (D9): every task reads job parameters +
# MAGIC durable state by `run_id`. There is no `dbutils.jobs.taskValues` plumbing.

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast


from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.config import (
    MAX_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.common.warehouse import (
    export_warehouse_id,
    resolve_warehouse_id,
)
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.preflight import (
    compute_asset_fingerprint,
    preflight_fetch_config,
)
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    update_run_status,
    write_artifact,
    write_stage,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK INTAKE"
_TASK_KEY = "intake_and_snapshot"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# Job parameters arrive as widgets (base_parameters); defaults keep the task
# self-sufficient even if a param is omitted by the trigger.
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("levers", "[1,2,3,4,5,6]")
dbutils.widgets.text("max_attempts", "3")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("benchmark_repair_max_tries", "3")
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
max_attempts = int(dbutils.widgets.get("max_attempts") or "3")
target_accuracy = float(dbutils.widgets.get("target_accuracy") or "0.90")
benchmark_repair_max_tries = int(dbutils.widgets.get("benchmark_repair_max_tries") or "3")
triggered_by = dbutils.widgets.get("triggered_by").strip()
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("intake_and_snapshot: run_id parameter is required")

_banner("Resolved Job Parameters")
_log(
    "Parameters",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    apply_mode=apply_mode,
    levers=levers,
    max_attempts=max_attempts,
    target_accuracy=target_accuracy,
    benchmark_repair_max_tries=benchmark_repair_max_tries,
)

# COMMAND ----------

w = make_workspace_client()
spark = cast(Any, globals().get("spark"))

from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE
from genie_space_optimizer.common.genie_client import (
    configure_connection_pool,
    configure_mlflow_connection_pool,
)

configure_connection_pool(w, CONNECTION_POOL_SIZE)
configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

warehouse_id = resolve_warehouse_id(dbutils.widgets.get("warehouse_id").strip())
if warehouse_id:
    export_warehouse_id(warehouse_id)
_log("SQL warehouse", warehouse_id=warehouse_id or "(not set — using Spark SQL)")

_banner("Ensuring Delta State Tables")
# Creates the GSO v2 table set (incl. genie_opt_artifacts), runs additive
# column migrations (incl. the loop-state columns), and one-shot retires the
# dropped 6-notebook tables (rename-to-*_deprecated on existing installs).
ensure_optimization_tables(spark, catalog, schema)
_log("State tables verified", catalog=catalog, schema=schema)

if catalog:
    spark.sql(f"USE CATALOG `{catalog}`")
if schema:
    spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 00a — Capture the rollback snapshot (ORIGINAL serialized_space)
# MAGIC
# MAGIC `preflight_fetch_config` reads the config snapshot captured at trigger time
# MAGIC (the run-row `config_snapshot`), or fetches it from the Genie API as a
# MAGIC fallback. This ORIGINAL config is the discard revert anchor (§3.5): on
# MAGIC discard, rollback re-PATCHes it so GSO's additive mutations are removed.

# COMMAND ----------

try:
    write_stage(
        spark, run_id, "INTAKE_AND_SNAPSHOT", "STARTED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema,
    )
    _banner("Step 00a — Config Fetch + Snapshot Capture")
    ctx_config = preflight_fetch_config(
        w, spark, run_id, space_id, catalog, schema, domain, apply_mode,
    )
    _config = ctx_config["config"]
    _snapshot = ctx_config["snapshot"]
    _genie_table_refs = ctx_config["genie_table_refs"]
    _domain = ctx_config["domain"]
    _config_hash = compute_asset_fingerprint(_config)
    _log(
        "Config fetched + snapshot captured",
        tables=len(_genie_table_refs),
        config_hash=_config_hash,
    )
except Exception as exc:
    _banner("Snapshot Capture FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "INTAKE_AND_SNAPSHOT", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema,
        error_message=str(exc),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 00b — Write the run_manifest artifact (arch §7.3)

# COMMAND ----------

# run_manifest: the run's parameter envelope (arch §7.3).
write_artifact(
    spark, run_id, "run_manifest",
    {
        "run_id": run_id,
        "space_id": space_id,
        "domain": _domain,
        "catalog": catalog,
        "schema": schema,
        "apply_mode": apply_mode,
        "levers": levers,
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
        "benchmark_repair_max_tries": benchmark_repair_max_tries,
        "benchmark_target": TARGET_BENCHMARK_COUNT,
        "benchmark_max": MAX_BENCHMARK_COUNT,
        "baseline_config_hash": _config_hash,
        "triggered_by": triggered_by,
        "warehouse_id": warehouse_id,
    },
    catalog=catalog, schema=schema,
    stage_name=_TASK_KEY, source_notebook="run_intake_and_snapshot.py",
)

# Persist run-level handoff columns + mark IN_PROGRESS. The run row itself is
# created by the app trigger (integration/trigger.py) before the job starts.
update_run_status(
    spark, run_id, catalog, schema,
    status="IN_PROGRESS",
    warehouse_id=warehouse_id,
    llm_model=llm_model or None,
)

write_stage(
    spark, run_id, "INTAKE_AND_SNAPSHOT", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={"config_hash": _config_hash, "table_refs": len(_genie_table_refs)},
)

_banner("Intake and snapshot completed")
dbutils.notebook.exit(json.dumps({"run_id": run_id, "config_hash": _config_hash}, default=str))
