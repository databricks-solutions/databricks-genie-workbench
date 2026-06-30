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
# MAGIC ## 🎯 Purpose (arch §6 / §7.3 / §13.3)
# MAGIC
# MAGIC Publish the champion and write the run audit record, **gated on the STAMPED
# MAGIC `terminal_reason`** read off the champion iteration row (NOT re-derived from
# MAGIC accuracy):
# MAGIC
# MAGIC - `TARGET_REACHED` / `MAX_ATTEMPTS` ⇒ publish (idempotent Delta-only
# MAGIC   `promote_best_model`; NO live-space mutation) + write the full `publish_record`.
# MAGIC - anything else (`EVAL_INVALID`, `LOOP_STATE_INVALID`, `NO_NEW_HYPOTHESIS`,
# MAGIC   `EVAL_BUDGET_EXHAUSTED`) ⇒ do NOT publish, but still write a `publish_record`
# MAGIC   carrying the stop reason + residual failures as concerns (arch §7.3).
# MAGIC
# MAGIC The `publish_record` includes an **LLM-generated 1–2 paragraph audit summary**
# MAGIC (best-effort / non-fatal) over a LEAK-FREE structural context, the structured
# MAGIC improvement trajectory, the champion pointer, and concerns. The real body
# MAGIC lives in `optimization/publish.py`; this notebook is the thin shell. Run status
# MAGIC reuses the existing terminal statuses (`CONVERGED` / `MAX_ITERATIONS` /
# MAGIC `FAILED` / `STALLED`) — `PUBLISHED_AUDITED` (arch §13.3) is deferred to the
# MAGIC human. Bootstraps from job parameters + Delta only (no taskValues — D9).

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
from genie_space_optimizer.optimization.publish import publish_and_audit
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
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

# Normalize to the 0-100 accuracy scale (mirrors run_03_optimize.py) so the
# publish_record's target_accuracy lines up with the per-attempt accuracies.
if target_accuracy <= 1.0:
    target_accuracy *= 100.0

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
# MAGIC ## Publish (gated on the stamped terminal_reason) + write the publish_record

# COMMAND ----------

try:
    _banner("Publish + audit (stamped-reason gated)")
    result = publish_and_audit(
        spark, w, run_id,
        space_id=space_id,
        catalog=catalog,
        schema=schema,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        source_notebook="run_publish_and_audit.py",
    )
    _log(
        "Publish + audit complete",
        terminal_reason=result.get("terminal_reason"),
        final_status=result.get("final_status"),
        published=result.get("published"),
        publish_outcome=result.get("publish_outcome"),
        champion_iteration=result.get("champion_iteration"),
        champion_accuracy=result.get("champion_accuracy"),
        audit_summary_generated=result.get("audit_summary_generated"),
        concerns=result.get("concerns"),
    )
except Exception as exc:
    _banner("Publish + Audit FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_stage(
        spark, run_id, "PUBLISH_AND_AUDIT", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

write_stage(
    spark, run_id, "PUBLISH_AND_AUDIT", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={
        "terminal_reason": result.get("terminal_reason"),
        "final_status": result.get("final_status"),
        "published": result.get("published"),
        "champion_accuracy": result.get("champion_accuracy"),
    },
)

_banner("Task publish_and_audit Completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "terminal_reason": result.get("terminal_reason"),
    "final_status": result.get("final_status"),
    "published": result.get("published"),
    "champion_accuracy": result.get("champion_accuracy"),
}, default=str))
