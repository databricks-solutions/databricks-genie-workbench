# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Proactive Enrichment — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 3 of 6 — Proactive Enrichment |
# MAGIC | **Harness function** | `_run_enrichment()` in `optimization/harness.py` |
# MAGIC | **Reads from** | `preflight` (run context) + `baseline_eval` (scores, thresholds_met, model_id) |
# MAGIC | **Publishes to** | `lever_loop` (enrichment_model_id, enrichment_skipped, total_enrichments) **plus** post-enrichment eval (Tier 1.3): `post_enrichment_accuracy`, `post_enrichment_scores`, `post_enrichment_model_id`, `post_enrichment_thresholds_met` — present only when enrichment actually ran and the eval succeeded |
# MAGIC | **Typical duration** | 2–10 min |
# MAGIC | **Log label** | `[TASK-3 ENRICHMENT]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC Task 3 proactively improves the Genie Space configuration *before* the adaptive lever loop begins. It applies enrichments that don't require iterative feedback: description generation, join discovery, metadata filling, instruction seeding, and example SQL mining.
# MAGIC
# MAGIC ## 🏗️ DAG Position
# MAGIC
# MAGIC | Step | Task | Status | Reads From | Publishes To |
# MAGIC |:----:|------|:------:|------------|--------------|
# MAGIC | 1 | preflight | Done | widgets | all tasks |
# MAGIC | 2 | baseline_eval | Done | preflight | enrichment |
# MAGIC | 3 | **enrichment** | **⬅️ THIS TASK** | preflight + baseline | lever_loop |
# MAGIC | 4 | lever_loop | Next | preflight + baseline + enrichment | finalize |
# MAGIC | 5 | finalize | Pending | lever_loop | deploy |
# MAGIC | 6 | deploy | Pending | preflight + finalize | *(terminal)* |
# MAGIC
# MAGIC ## Enrichment Sub-Steps
# MAGIC
# MAGIC | Sub-step | What It Does | Config Refresh After? |
# MAGIC |:--------:|-------------|:--------------------:|
# MAGIC | Description enrichment | LLM-generated column descriptions for blank columns | Yes (if any changed) |
# MAGIC | Join discovery | Cross-table join path detection from baseline failures | Yes (if any applied) |
# MAGIC | Space metadata | Auto-generate space description and sample questions | Yes (if generated) |
# MAGIC | Instruction seeding | Seed initial instructions for empty instruction sets | Yes (if seeded) |
# MAGIC | Example SQL mining | Extract reference queries from benchmarks | Yes (if applied) |
# MAGIC
# MAGIC After all sub-steps, an MLflow LoggedModel snapshot captures the enriched state.
# MAGIC
# MAGIC ### Post-Enrichment Eval Feedback (Tier 1.3)
# MAGIC
# MAGIC Enrichment **mutates** the Genie Space — descriptions, joins, instructions, example SQLs change. Without a fresh eval, Task 4 (lever loop) would gate against the *pre-enrichment* baseline scorecard while its clustering reads *post-enrichment* rows. Those two realities can disagree arbitrarily and produce the "ghost-ceiling" rollback loop where every iteration looks like a regression against a stale anchor.
# MAGIC
# MAGIC When enrichment runs successfully, `_run_enrichment` performs a single-pass evaluation against the post-enrichment LoggedModel and publishes:
# MAGIC
# MAGIC | Task value | Purpose |
# MAGIC |---|---|
# MAGIC | `post_enrichment_accuracy` | Float — current `overall_accuracy` of the enriched space |
# MAGIC | `post_enrichment_scores` | JSON — per-judge scores |
# MAGIC | `post_enrichment_model_id` | Genie model ID for the snapshot |
# MAGIC | `post_enrichment_thresholds_met` | Bool — whether the enriched space already meets all thresholds |
# MAGIC
# MAGIC Task 4 prefers these over `baseline_eval.*` when present (see the `prev_accuracy_source` log entry for the resolution decision).
# MAGIC
# MAGIC ### Skip Path
# MAGIC
# MAGIC > **📝 Note:** When `enrichment_skipped=True` (baseline already met thresholds, or the entire enrichment block failed and fell back to baseline) the `post_enrichment_*` task values are **absent**. Task 4's reader treats that as "fall back to `baseline_eval.*`" so the contract degrades gracefully.

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
from genie_space_optimizer.optimization.harness import _run_enrichment

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-3 ENRICHMENT"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Reading Upstream Task Values

# COMMAND ----------

from genie_space_optimizer._workspace_client import make_workspace_client
w = make_workspace_client()
spark = SparkSession.builder.getOrCreate()

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

thresholds_met_raw = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="thresholds_met")
thresholds_met = str(thresholds_met_raw).lower() in ("true", "1")
baseline_model_id = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="model_id")

import mlflow
mlflow.set_experiment(exp_name)
mlflow.openai.autolog()

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    baseline_model_id=baseline_model_id,
    thresholds_met=thresholds_met,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Loading Benchmarks

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
_all_benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
benchmarks = [b for b in _all_benchmarks if b.get("split") != "held_out"]
_held_out_n = len(_all_benchmarks) - len(benchmarks)
_banner("Loaded Benchmarks")
_log(
    "Benchmark dataset (train/held-out split)",
    uc_schema=uc_schema,
    domain=domain,
    total_loaded=len(_all_benchmarks),
    train_count=len(benchmarks),
    held_out_count=_held_out_n,
    note="held-out reserved for Finalize generalization check",
)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Running Proactive Enrichment
# MAGIC
# MAGIC Enrichment always runs regardless of whether the baseline meets thresholds.
# MAGIC On failure, the pipeline falls back to the baseline model ID so downstream
# MAGIC tasks are unaffected.

# COMMAND ----------

try:
    _banner("Running _run_enrichment")
    enrichment_out = _run_enrichment(
        w, spark, run_id, space_id, domain, benchmarks, exp_name,
        catalog, schema,
        baseline_model_id=baseline_model_id,
    )
    _log(
        "Enrichment finished",
        enrichment_model_id=enrichment_out["enrichment_model_id"],
        enrichment_skipped=enrichment_out["enrichment_skipped"],
        summary=enrichment_out["summary"],
    )
    if enrichment_out["enrichment_skipped"]:
        enrichment_out["enrichment_model_id"] = baseline_model_id
except Exception as exc:
    _banner("Enrichment FAILED — falling back to baseline model")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    enrichment_out = {
        "enrichment_model_id": baseline_model_id,
        "enrichment_skipped": True,
        "summary": {"total_enrichments": 0},
        "post_enrichment_accuracy": None,
        "post_enrichment_scores": {},
        "post_enrichment_model_id": baseline_model_id,
        "post_enrichment_thresholds_met": False,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Publishing Task Values

# COMMAND ----------

_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="enrichment_model_id", value=enrichment_out["enrichment_model_id"])
dbutils.jobs.taskValues.set(key="enrichment_skipped", value=enrichment_out["enrichment_skipped"])
dbutils.jobs.taskValues.set(key="total_enrichments", value=enrichment_out["summary"]["total_enrichments"])

# Tier 1.3: publish post-enrichment eval so Task 4 can gate against the
# current space state (not the stale baseline). Present only when
# enrichment actually applied and the eval succeeded.
_post_enr_acc = enrichment_out.get("post_enrichment_accuracy")
_post_enr_scores = enrichment_out.get("post_enrichment_scores") or {}
_post_enr_model_id = enrichment_out.get("post_enrichment_model_id") or enrichment_out["enrichment_model_id"]
_post_enr_thresholds_met = bool(enrichment_out.get("post_enrichment_thresholds_met", False))
if _post_enr_acc is not None:
    dbutils.jobs.taskValues.set(key="post_enrichment_accuracy", value=_post_enr_acc)
    dbutils.jobs.taskValues.set(key="post_enrichment_scores", value=json.dumps(_post_enr_scores))
    dbutils.jobs.taskValues.set(key="post_enrichment_model_id", value=_post_enr_model_id)
    dbutils.jobs.taskValues.set(key="post_enrichment_thresholds_met", value=_post_enr_thresholds_met)

_log(
    "Task values published",
    enrichment_model_id=enrichment_out["enrichment_model_id"],
    enrichment_skipped=enrichment_out["enrichment_skipped"],
    total_enrichments=enrichment_out["summary"]["total_enrichments"],
    post_enrichment_accuracy=_post_enr_acc,
    post_enrichment_thresholds_met=_post_enr_thresholds_met,
)
_banner("Task 3 Completed")
dbutils.notebook.exit(json.dumps(enrichment_out["summary"], default=str))
