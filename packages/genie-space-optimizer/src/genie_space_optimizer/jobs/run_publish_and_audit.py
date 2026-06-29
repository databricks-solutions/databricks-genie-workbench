# Databricks notebook source
# MAGIC %md
# MAGIC # Task publish_and_audit (GSO v2 — 5-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 5 of 5 — `publish_and_audit` |
# MAGIC | **Reads** | champion state, loop-state, patch history, attempt budget |
# MAGIC | **Writes** | `genie_opt_runs`, `genie_opt_artifacts` (`publish_record`), `genie_opt_stages` |
# MAGIC | **Log label** | `[TASK-PUBLISH]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §6 / §7.3)
# MAGIC
# MAGIC Publish the champion and write the run audit record once the target is hit
# MAGIC or the attempt budget is exhausted.
# MAGIC
# MAGIC **Phase 7 is a THIN SHELL:** the real body — the LLM-generated 1–2 paragraph
# MAGIC human-readable audit summary, improvement trajectory, and concerns
# MAGIC callout — is Phase 9. Phase 7 records a minimal `publish_record` (champion
# MAGIC pointer + terminal reason + run status) so the reshaped 5-task job runs
# MAGIC end-to-end. Bootstraps from job parameters + Delta only (no taskValues — D9).

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast


from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.warehouse import export_warehouse_id, resolve_warehouse_id
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    load_latest_full_iteration,
    load_run,
    update_run_status,
    write_artifact,
    write_stage,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-PUBLISH"
_TASK_KEY = "publish_and_audit"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("max_attempts", "3")
dbutils.widgets.text("warehouse_id", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
target_accuracy = float(dbutils.widgets.get("target_accuracy") or "0.90")
max_attempts = int(dbutils.widgets.get("max_attempts") or "3")

if not run_id:
    raise RuntimeError("publish_and_audit: run_id parameter is required")

# COMMAND ----------

w = make_workspace_client()
spark = cast(Any, globals().get("spark"))

warehouse_id = resolve_warehouse_id(dbutils.widgets.get("warehouse_id").strip())
if warehouse_id:
    export_warehouse_id(warehouse_id)
    os.environ["GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID"] = warehouse_id

ensure_optimization_tables(spark, catalog, schema)
if catalog:
    spark.sql(f"USE CATALOG `{catalog}`")
if schema:
    spark.sql(f"USE SCHEMA `{schema}`")

write_stage(
    spark, run_id, "PUBLISH_AND_AUDIT", "STARTED",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve champion + terminal reason, write the publish_record shell

# COMMAND ----------

try:
    _banner("Resolving champion")
    _run_row = load_run(spark, run_id, catalog, schema) or {}
    _best_iter_row = load_latest_full_iteration(spark, run_id, catalog, schema) or {}

    # best_accuracy / best_iteration are stamped on the run row by the loop
    # (promote_best_model). Fall back to the latest full iteration row.
    best_accuracy = _run_row.get("best_accuracy")
    if best_accuracy is None:
        best_accuracy = _best_iter_row.get("overall_accuracy")
    best_iteration = _run_row.get("best_iteration")
    if best_iteration is None:
        best_iteration = _best_iter_row.get("iteration")

    # Terminal reason (arch §5.1 vocabulary). The Phase-8 controller sets this
    # explicitly on the loop-state row; the Phase-7 shell derives it from the
    # champion accuracy vs. target.
    _acc_fraction = (float(best_accuracy) / 100.0) if best_accuracy is not None else 0.0
    if best_accuracy is not None and _acc_fraction >= target_accuracy:
        terminal_reason = "TARGET_REACHED"
        run_status = "CONVERGED"
    else:
        terminal_reason = "MAX_ATTEMPTS"
        run_status = "MAX_ITERATIONS"

    _log(
        "Champion resolved",
        best_accuracy=best_accuracy,
        best_iteration=best_iteration,
        terminal_reason=terminal_reason,
    )
except Exception as exc:
    _banner("Champion Resolution FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "PUBLISH_AND_AUDIT", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

# publish_record artifact (arch §7.3). Phase 7 shell — the LLM-generated
# human-readable audit summary + improvement trajectory + concerns callout is
# Phase 9. The fields are present (None / placeholder) so the contract shape is
# stable for the UI and Phase 9.
write_artifact(
    spark, run_id, "publish_record",
    {
        "run_id": run_id,
        "space_id": space_id,
        "final_status": run_status,
        "terminal_reason": terminal_reason,
        "champion_iteration": best_iteration,
        "champion_accuracy": best_accuracy,
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
        "published": True,
        "audit_summary": None,          # Phase 9: LLM-generated 1–2 paragraph summary
        "improvement_trajectory": None,  # Phase 9
        "concerns": [],                  # Phase 9
        "note": "Phase 7 shell — audit summary body is Phase 9.",
    },
    catalog=catalog, schema=schema,
    stage_name=_TASK_KEY, source_notebook="run_publish_and_audit.py",
)

update_run_status(
    spark, run_id, catalog, schema,
    status=run_status,
    best_iteration=int(best_iteration) if best_iteration is not None else None,
    best_accuracy=float(best_accuracy) if best_accuracy is not None else None,
    convergence_reason=terminal_reason,
    space_id=space_id or None,
)

write_stage(
    spark, run_id, "PUBLISH_AND_AUDIT", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={"terminal_reason": terminal_reason, "champion_accuracy": best_accuracy},
)

_banner("Task publish_and_audit Completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "terminal_reason": terminal_reason,
    "champion_accuracy": best_accuracy,
}, default=str))
