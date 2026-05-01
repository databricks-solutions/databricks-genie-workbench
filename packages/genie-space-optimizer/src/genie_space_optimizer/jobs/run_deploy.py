# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5: Deploy — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 6 of 6 — Deploy (Conditional) |
# MAGIC | **Harness function** | `_run_deploy()` in `optimization/harness.py` |
# MAGIC | **Reads from** | `preflight` (deploy_target, run context) + `lever_loop` or `baseline_eval` (model_id) |
# MAGIC | **Publishes to** | *(terminal — no downstream)* |
# MAGIC | **Typical duration** | 1–5 min |
# MAGIC | **Log label** | `[TASK-5 DEPLOY]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC Task 6 (Deploy) is the **final and conditional** step in the 6-task optimization DAG. It applies the optimized Genie Space configuration to a target environment (e.g., via DABs) after optimization has completed successfully.
# MAGIC
# MAGIC ## 🏗️ DAG Position
# MAGIC
# MAGIC | Step | Task | Status | Reads From | Publishes To |
# MAGIC |:----:|------|:------:|------------|--------------|
# MAGIC | 1 | preflight | Done | widgets | all tasks |
# MAGIC | 2 | baseline_eval | Done | preflight | enrichment |
# MAGIC | 3 | enrichment | Done | preflight + baseline | lever_loop |
# MAGIC | 4 | lever_loop | Done | preflight + baseline + enrichment | finalize |
# MAGIC | 5 | finalize | Done | lever_loop | deploy |
# MAGIC | 6 | **deploy** | **⬅️ THIS TASK** | preflight + finalize | *(terminal)* |
# MAGIC
# MAGIC ## When Does Deploy Run?
# MAGIC
# MAGIC > **📝 Note:** Deploy is **conditional** — it executes only when `deploy_target` is set. A **condition task** (`deploy_check`) gates the deploy step: it runs only when `deploy_target` is non-empty. If `deploy_target` is empty or unset, the deploy task is skipped and the pipeline completes after finalize.
# MAGIC
# MAGIC ## What Deploy Does
# MAGIC
# MAGIC When `deploy_target` is set:
# MAGIC
# MAGIC 1. **Writes** `DEPLOY_STARTED` stage record to Delta for audit
# MAGIC 2. **Applies** the optimized configuration to the target Genie Space (DABs integration — full implementation pending)
# MAGIC 3. **Writes** `DEPLOY_COMPLETE` stage record to Delta
# MAGIC 4. **Returns** `{"status": "DEPLOYED", "deploy_target": deploy_target}` on success
# MAGIC
# MAGIC When `deploy_target` is empty:
# MAGIC
# MAGIC 1. **Writes** `DEPLOY_SKIPPED` stage record to Delta
# MAGIC 2. **Returns** `{"status": "SKIPPED", "reason": "no_deploy_target"}`
# MAGIC
# MAGIC > **📝 Note:** DABs integration is pending full implementation. The harness currently writes stage records and returns status.
# MAGIC
# MAGIC ## MLflow Integration
# MAGIC
# MAGIC > **📝 Note:** Deploy does **not** mint a new MLflow run name. The pyfunc model snapshot is logged into the champion iteration's source `run_id` (see `register_uc_model` → `mlflow.start_run(run_id=source_run_id)` in `optimization/models.py`). The v2 run-naming scheme (`<run_short>/<stage>/<detail>`, see `common/mlflow_names.py`) governs every other task — baseline, enrichment, strategy, slice/p0/full evals, finalize/held_out, finalize/repeat_pass_k — so champion artefacts are reachable via the same `genie.run_id` tag (Tier 4).
# MAGIC
# MAGIC ## ⚠️ What Happens If This Task Fails
# MAGIC
# MAGIC > **📝 Note:** Optimization results are **not lost** — scores, model, and report from finalize are already persisted in Delta and MLflow.
# MAGIC
# MAGIC - Delta state is updated with `DEPLOY` = FAILED
# MAGIC
# MAGIC > **💡 Tip:** Check job run logs for `[TASK-5 DEPLOY] Failure details`, inspect `genie_opt_stages` for the DEPLOY stage record.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Serialize deploy result for `dbutils.notebook.exit()` |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `partial` | Bind `_TASK_LABEL` to shared `_banner` and `_log` helpers |
# MAGIC | `WorkspaceClient` | Databricks SDK — workspace operations, DABs integration |
# MAGIC | `SparkSession` | Delta state writes |
# MAGIC | `_banner`, `_log` | Shared logging helpers from `_helpers.py` |
# MAGIC | `_run_deploy` | Harness stage function: deploy to target, Delta state writes |
# MAGIC
# MAGIC ### Helper Functions
# MAGIC
# MAGIC | Function | What It Does |
# MAGIC |----------|---------------|
# MAGIC | `_banner(title)` | Prints a 120-char separator and `[TASK-5 DEPLOY] {title}` for visual section breaks |
# MAGIC | `_log(event, **payload)` | Logs `event` with optional JSON payload; uses `default=str` for non-JSON-serializable values |

# COMMAND ----------

import json
import traceback
from functools import partial
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.harness import deploy_check, deploy_execute

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-5 DEPLOY"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Reading Upstream Task Values
# MAGIC
# MAGIC Task 5 reads from two upstream tasks depending on whether lever_loop ran or was skipped:
# MAGIC
# MAGIC **From preflight (always):**
# MAGIC
# MAGIC | Key | Purpose |
# MAGIC |-----|---------|
# MAGIC | `run_id` | Optimization run identifier |
# MAGIC | `space_id` | Genie Space being optimized |
# MAGIC | `domain` | Domain context for deploy operations |
# MAGIC | `catalog`, `schema` | Unity Catalog location for state tables |
# MAGIC | `experiment_name` | MLflow experiment path |
# MAGIC | `deploy_target` | DABs target for deployment (empty = skip deploy) |
# MAGIC
# MAGIC **From lever_loop or baseline_eval (conditional):**
# MAGIC
# MAGIC | Key | Source when skipped | Source when ran | Purpose |
# MAGIC |-----|--------------------|-----------------|---------| 
# MAGIC | `model_id` | `baseline_eval` | `lever_loop` | Best model version ID to deploy |
# MAGIC | `iteration_counter` | `0` (hardcoded) | `lever_loop` | Number of lever iterations completed |
# MAGIC
# MAGIC > **📝 Note:** The `skipped` key from `lever_loop` determines which source to use. This mirrors the branching logic in Task 4 (finalize).

# COMMAND ----------

from genie_space_optimizer._workspace_client import make_workspace_client
w = make_workspace_client()
spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
_widget_run_id = dbutils.widgets.get("run_id").strip()
_widget_catalog = dbutils.widgets.get("catalog").strip()
_widget_schema = dbutils.widgets.get("schema").strip()

from genie_space_optimizer.jobs._handoff import (
    get_lever_loop_outputs,
    get_run_context,
)

ctx = get_run_context(
    spark,
    run_id_widget=_widget_run_id,
    catalog_widget=_widget_catalog,
    schema_widget=_widget_schema,
    dbutils=dbutils,
)
run_id = ctx["run_id"].value
space_id = ctx["space_id"].value
domain = ctx["domain"].value
catalog = ctx["catalog"].value
schema = ctx["schema"].value
exp_name = ctx["experiment_name"].value
# deploy_target is preflight-published but not yet in genie_opt_runs; fall
# back to the legacy taskValue read until a future plan widens the schema.
deploy_target = (
    dbutils.jobs.taskValues.get(taskKey="preflight", key="deploy_target", default="")
    or None
)

import os as _os
_warehouse_id = ctx["warehouse_id"].value or ""
if _warehouse_id:
    _os.environ["GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID"] = _warehouse_id

import mlflow
mlflow.set_experiment(exp_name)
mlflow.openai.autolog()

ll = get_lever_loop_outputs(
    spark, run_id=run_id, catalog=catalog, schema=schema, dbutils=dbutils,
)
prev_model_id = ll["model_id"].value
lever_skipped = ll["skipped"].value
iteration_counter = 0 if lever_skipped else (ll["iteration_counter"].value or 0)

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    deploy_target=deploy_target,
    lever_skipped=bool(lever_skipped),
    prev_model_id=prev_model_id,
    iteration_counter=iteration_counter,
)
_log(
    "Handoff sources",
    run_id_source=ctx["run_id"].source.value,
    lever_skipped_source=ll["skipped"].source.value,
    model_id_source=ll["model_id"].source.value,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5a: Deploy Gate Check
# MAGIC
# MAGIC Before attempting deployment, verify whether a `deploy_target` was configured.
# MAGIC If no target is set, the deploy step will be skipped gracefully.
# MAGIC This cell prints the deploy target, model ID, and iteration context so you
# MAGIC can confirm the correct configuration is being deployed.

# COMMAND ----------

_banner("Deploy Gate Check")
gate = deploy_check(deploy_target, prev_model_id, iteration_counter)
_log("Gate result", **gate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5b: Deploy Execution
# MAGIC
# MAGIC Execute the deployment to the configured target, or skip if no target is set.
# MAGIC On success, a `DEPLOY_COMPLETE` stage record is written to Delta.
# MAGIC On skip, a `DEPLOY_SKIPPED` record is written instead.
# MAGIC The deploy status is printed and published as the notebook exit value.

# COMMAND ----------

try:
    _banner("Running Deploy Execution")
    deploy_out = deploy_execute(
        w, spark, run_id, deploy_target, space_id, exp_name,
        domain, prev_model_id, iteration_counter,
        catalog, schema,
    )
    _log("Deploy result", **deploy_out)
except Exception as exc:
    _banner("Deploy FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

_banner("Task 5 Completed")
dbutils.notebook.exit(json.dumps(deploy_out, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Known Failure Modes
# MAGIC
# MAGIC ### 🔵 INFO: Empty `deploy_target`
# MAGIC
# MAGIC > **📝 Note:** Not a failure — the harness writes `DEPLOY_SKIPPED` to Delta and returns `{"status": "SKIPPED"}`. The condition task in the job definition should prevent this notebook from running at all when `deploy_target` is empty, but the harness handles it gracefully as a safety net.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 CRITICAL: Permission Errors
# MAGIC
# MAGIC **Cause:** The service principal lacks permissions to modify the target Genie Space or write to the deploy location.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Verify the service principal has appropriate permissions on the target workspace
# MAGIC - Check that the `deploy_target` path is valid and accessible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 WARNING: Stale Model ID
# MAGIC
# MAGIC **Cause:** The `model_id` from lever_loop/baseline may reference a model version that was deleted or moved between tasks.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Check MLflow experiment for the model version
# MAGIC - Re-run finalize to generate a fresh model if needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Interpret Error Logs
# MAGIC
# MAGIC 1. Look for `[TASK-5 DEPLOY] Failure details` — logs `error_type`, `error_message`, and full `traceback`
# MAGIC 2. Check Delta `genie_opt_stages` for `DEPLOY` stage records and `error_message` column
# MAGIC 3. Verify `deploy_target` is a valid, accessible path
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | Deploy skipped unexpectedly | Check `deploy_target` was set in preflight task values |
# MAGIC | Permission denied | Grant workspace permissions to the service principal |
# MAGIC | Model not found | Verify `model_id` exists in MLflow; re-run finalize if needed |
# MAGIC | Deploy task never ran | Check condition task (`deploy_check`) in job definition |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What Success Looks Like
# MAGIC
# MAGIC **When deployed:**
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-5 DEPLOY] Running _run_deploy
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 11:50:00 UTC] [TASK-5 DEPLOY] Deploy result
# MAGIC   {"status": "DEPLOYED", "deploy_target": "dabs://my-workspace/genie-spaces/revenue"}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-5 DEPLOY] Task 5 Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC ```
# MAGIC
# MAGIC **When skipped (no deploy target):**
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-5 DEPLOY] Running _run_deploy
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 11:50:00 UTC] [TASK-5 DEPLOY] Deploy result
# MAGIC   {"status": "SKIPPED", "reason": "no_deploy_target"}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-5 DEPLOY] Task 5 Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC ```
# MAGIC
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC - **Task 5 (Deploy)** is a conditional step that applies the optimized Genie Space configuration to a target environment.
# MAGIC - **Input branching:** Reads `model_id` and `iteration_counter` from `lever_loop` if it ran, or `baseline_eval` if it was skipped. This mirrors the same branching logic as Task 4 (finalize).
# MAGIC - **Condition task:** The job definition gates deploy on a non-empty `deploy_target`. If empty, the task is skipped.
# MAGIC - **On success:** The optimized configuration is deployed and `DEPLOY_COMPLETE` is written to Delta.
# MAGIC - **On skip:** `DEPLOY_SKIPPED` is written to Delta; no configuration changes are made.
# MAGIC - **On failure:** Optimization results are preserved in Delta and MLflow; only the deploy step needs retry.
