# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Optimization (Single-Task)
# MAGIC
# MAGIC Dynamically submitted by the app backend via `ws.jobs.submit()`.
# MAGIC Runs the full optimization pipeline (preflight → baseline → lever loop → finalize → deploy).
# MAGIC
# MAGIC This notebook is a verbose, operator-friendly wrapper that logs each
# MAGIC parameter and emits structured completion/failure diagnostics.

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone
from typing import Any, cast

dbutils = cast(Any, globals().get("dbutils"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [SINGLE-TASK OPTIMIZATION] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [SINGLE-TASK OPTIMIZATION] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("domain", "default")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("levers", "[1,2,3,4,5]")
dbutils.widgets.text("max_iterations", "5")
dbutils.widgets.text("triggered_by", "")

run_id = dbutils.widgets.get("run_id")
space_id = dbutils.widgets.get("space_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain") or "default"
apply_mode = dbutils.widgets.get("apply_mode") or "genie_config"
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5]")
max_iterations = int(dbutils.widgets.get("max_iterations") or "5")
triggered_by = dbutils.widgets.get("triggered_by") or None

_banner("Resolved Widget Inputs")
_log(
    "Parameters",
    run_id=run_id,
    space_id=space_id,
    catalog=catalog,
    schema=schema,
    domain=domain,
    apply_mode=apply_mode,
    levers=levers,
    max_iterations=max_iterations,
    triggered_by=triggered_by,
)

# COMMAND ----------

from genie_space_optimizer.optimization.harness import optimize_genie_space

try:
    _banner("Running optimize_genie_space")
    result = optimize_genie_space(
        space_id=space_id,
        catalog=catalog,
        schema=schema,
        domain=domain,
        run_id=run_id or None,
        apply_mode=apply_mode,
        levers=levers,
        max_iterations=max_iterations,
        triggered_by=triggered_by,
    )
except Exception as exc:
    _banner("Optimization FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

_banner("Optimization Completed")
_log(
    "Result",
    status=result.status,
    best_accuracy=result.best_accuracy,
    best_iteration=result.best_iteration,
    best_repeatability=result.best_repeatability,
    best_model_id=result.best_model_id,
    convergence_reason=result.convergence_reason,
    total_iterations=result.total_iterations,
)
if result.error:
    raise RuntimeError(result.error)
