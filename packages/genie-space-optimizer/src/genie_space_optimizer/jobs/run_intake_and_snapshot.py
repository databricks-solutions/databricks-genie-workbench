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
    MIN_VALID_BENCHMARK_COUNT,
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
    load_latest_artifact_record,
    update_run_status,
    write_artifact,
    write_required_artifact,
    write_failure_stage_safely,
    write_stage,
)
from genie_space_optimizer.optimization.wide_schema import (
    build_local_evidence,
    collect_inventory,
    merge_query_history_evidence,
    project_full_inventory,
    validate_inventory,
)
from genie_space_optimizer.optimization.wide_schema_history import (
    collect_query_history_evidence,
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
dbutils.widgets.text("workload_warehouse_ids", "[]")

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
try:
    workload_warehouse_ids = [
        str(value).strip()
        for value in json.loads(dbutils.widgets.get("workload_warehouse_ids") or "[]")
        if str(value).strip()
    ]
except (TypeError, ValueError):
    workload_warehouse_ids = []
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("intake_and_snapshot: run_id parameter is required")
os.environ["GSO_RUN_ID"] = run_id

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
    write_failure_stage_safely(
        spark, run_id, "INTAKE_AND_SNAPSHOT",
        task_key=_TASK_KEY, catalog=catalog, schema=schema,
        error_message=str(exc),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 00b — Complete inventory and optional usage evidence

# COMMAND ----------

try:
    _existing_inventory_record = load_latest_artifact_record(
        spark, run_id, catalog, schema, "wide_schema_inventory",
    )
    if _existing_inventory_record is not None:
        _inventory = _existing_inventory_record["payload"]
        validate_inventory(_inventory)
        _inventory_uc_columns = project_full_inventory(_inventory)
        _inventory_foreign_keys = []
    else:
        _inventory, _inventory_uc_columns, _inventory_foreign_keys = collect_inventory(
            w,
            spark,
            _config,
            _genie_table_refs,
            prefetched=(
                _snapshot.get("_prefetched_uc_metadata", {})
                if isinstance(_snapshot, dict)
                else {}
            ),
            warehouse_id=warehouse_id,
        )
        write_required_artifact(
            spark,
            run_id,
            "wide_schema_inventory",
            _inventory,
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_intake_and_snapshot.py",
        )

    _local_evidence = build_local_evidence(_config, _inventory)
    try:
        from genie_space_optimizer.common.sp_permissions import get_sp_principal_aliases

        _sp_identities = get_sp_principal_aliases(w)
    except Exception:
        _sp_identities = set()
    _history_evidence = collect_query_history_evidence(
        w,
        _inventory,
        profiling_warehouse_id=warehouse_id,
        workload_warehouse_ids=workload_warehouse_ids,
        run_id=run_id,
        target_space_id=space_id,
        service_principal_identities=_sp_identities,
    )
    _wide_schema_evidence = merge_query_history_evidence(
        _local_evidence,
        _history_evidence,
    )
    write_artifact(
        spark,
        run_id,
        "wide_schema_evidence",
        _wide_schema_evidence,
        catalog=catalog,
        schema=schema,
        stage_name=_TASK_KEY,
        source_notebook="run_intake_and_snapshot.py",
    )
    _log(
        "Wide-schema inventory captured",
        assets=len(_inventory.get("assets") or []),
        columns=sum(len(asset.get("columns") or []) for asset in _inventory.get("assets") or []),
        history_source=_wide_schema_evidence.get("source_mode"),
        history_warnings=_wide_schema_evidence.get("warnings") or [],
    )
except Exception as exc:
    _banner("Wide-Schema Inventory FAILED")
    write_failure_stage_safely(
        spark,
        run_id,
        "INTAKE_AND_SNAPSHOT",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
        error_message=str(exc),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 00c — Write the run_manifest artifact (arch §7.3)

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
        "benchmark_min_valid": MIN_VALID_BENCHMARK_COUNT,
        "benchmark_target": TARGET_BENCHMARK_COUNT,
        "benchmark_max": MAX_BENCHMARK_COUNT,
        "baseline_config_hash": _config_hash,
        "triggered_by": triggered_by,
        "warehouse_id": warehouse_id,
        "workload_warehouse_ids": workload_warehouse_ids,
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
