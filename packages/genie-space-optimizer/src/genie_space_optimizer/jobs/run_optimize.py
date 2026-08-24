# Databricks notebook source
# MAGIC %md
# MAGIC # Optimize (GSO v2 — 4-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `optimize` |
# MAGIC | **Reads** | benchmarks, live Genie Agent config |
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
from genie_space_optimizer.jobs._helpers import _diagnostic as _diagnostic_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.benchmarks import (
    benchmark_corpus_for_optimization,
    load_benchmark_corpus,
)
from genie_space_optimizer.optimization.mv_advisor import run_mv_advisor_phase
from genie_space_optimizer.optimization.mv_scoring import FoundationModelEmbeddingClient
from genie_space_optimizer.optimization.preflight import _resolve_experiment_path
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    load_artifacts,
    load_latest_artifact_record,
    load_latest_artifact_payload,
    write_artifact,
    write_failure_stage_safely,
    write_stage,
)
from genie_space_optimizer.optimization.wide_schema import (
    validate_inventory,
    validate_selection_plan,
)
from genie_space_optimizer.optimization.wide_schema_profile import (
    build_profiling_budget,
    merge_profiling_budgets,
)
from genie_space_optimizer.optimization.unified_loop import (
    run_unified_optimization_loop,
    target_accuracy_percent,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK OPTIMIZE"
_TASK_KEY = "optimize"
_banner = partial(_banner_base, _TASK_LABEL)
_diagnostic = partial(_diagnostic_base, _TASK_LABEL)
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
dbutils.widgets.text("benchmark_policy", "repair_allowed")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")
dbutils.widgets.text("enable_metric_view_suggestions", "false")
dbutils.widgets.text("mv_attach_views", "")
dbutils.widgets.text("mv_consent_id", "")

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
benchmark_policy = dbutils.widgets.get("benchmark_policy").strip() or "repair_allowed"
# STRING compare, matching the other boolean-ish job parameters: the widget value
# arrives as text and anything other than "true" leaves the advisor phase off.
enable_metric_view_suggestions = (
    dbutils.widgets.get("enable_metric_view_suggestions").strip().lower() == "true"
)
# MV-D16: the attach phase's two inputs. ``mv_attach_views`` is a JSON list of
# fully-qualified metric view names the backend already created under OBO;
# ``mv_consent_id`` carries the consent's PROBE ID, because genie_opt_mv_consents
# is keyed on probe_id and has no consent_id column. Either one empty skips the
# phase. Registering these across databricks.yml / gso_job.py / job_launcher.py
# is Prompt 8's work, as it was for enable_metric_view_suggestions above.
mv_attach_views = dbutils.widgets.get("mv_attach_views").strip()
mv_consent_id = dbutils.widgets.get("mv_consent_id").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("optimize: run_id parameter is required")
os.environ["GSO_RUN_ID"] = run_id

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

# QC business skips keep the four-task job green while guaranteeing that no
# evaluation or live configuration mutation occurs in Optimize.
_benchmark_qc_record = load_latest_artifact_record(
    spark, run_id, catalog, schema, "benchmark_qc",
)
if _benchmark_qc_record is None:
    raise RuntimeError(
        f"Required benchmark_qc artifact is missing for run {run_id}"
    )
_benchmark_qc = _benchmark_qc_record["payload"]
if _benchmark_qc.get("optimization_eligible") is False:
    _skip_reason = str(
        _benchmark_qc.get("terminal_reason")
        or "INSUFFICIENT_VALID_BENCHMARKS"
    )
    write_stage(
        spark,
        run_id,
        "OPTIMIZE",
        "SKIPPED",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
        detail={
            "terminal_reason": _skip_reason,
            "valid_count": _benchmark_qc.get("valid_count"),
            "minimum_valid_count": _benchmark_qc.get("minimum_valid_count"),
            "benchmark_policy": benchmark_policy,
        },
    )
    dbutils.notebook.exit(json.dumps({
        "run_id": run_id,
        "status": "SKIPPED",
        "terminal_reason": _skip_reason,
    }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Benchmarks

# COMMAND ----------

def _load_optimize_inputs() -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    """Load and validate the durable Delta handoff for the Optimize task."""
    uc_schema = f"{catalog}.{schema}"
    all_benchmarks = load_benchmark_corpus(spark, uc_schema, domain)
    loaded_benchmarks = benchmark_corpus_for_optimization(all_benchmarks)
    if not loaded_benchmarks:
        raise RuntimeError(
            f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}"
        )

    loaded_prompt_context = load_latest_artifact_payload(
        spark, run_id, catalog, schema, "space_metadata",
    ) or {}

    loaded_inventory_record = load_latest_artifact_record(
        spark, run_id, catalog, schema, "wide_schema_inventory",
    )
    if loaded_inventory_record is None:
        raise RuntimeError("Required wide_schema_inventory artifact is missing")
    loaded_inventory = loaded_inventory_record["payload"]
    validate_inventory(loaded_inventory)

    loaded_plan_record = load_latest_artifact_record(
        spark, run_id, catalog, schema, "wide_schema_selection_plan",
    )
    if loaded_plan_record is None:
        raise RuntimeError("Required wide_schema_selection_plan artifact is missing")
    loaded_plan = loaded_plan_record["payload"]
    validate_selection_plan(
        loaded_plan,
        inventory_hash=loaded_inventory["inventory_hash"],
    )
    os.environ["GSO_WIDE_SCHEMA_INVENTORY_HASH"] = loaded_inventory[
        "inventory_hash"
    ]
    os.environ["GSO_WIDE_SCHEMA_PLAN_HASH"] = loaded_plan["plan_hash"]
    if loaded_prompt_context.get("inventory_hash") not in {
        None,
        loaded_inventory["inventory_hash"],
    }:
        raise RuntimeError(
            "space_metadata inventory hash does not match required inventory"
        )
    if loaded_prompt_context.get("plan_hash") not in {
        None,
        loaded_plan["plan_hash"],
    }:
        raise RuntimeError(
            "space_metadata plan hash does not match latest selection plan"
        )

    profile_artifact_rows = load_artifacts(
        spark,
        run_id,
        catalog,
        schema,
        artifact_kind="wide_schema_profile_telemetry",
    )
    persisted_profile_telemetry: list[dict[str, Any]] = []
    for raw_profile_telemetry in profile_artifact_rows.get("artifact_json", []):
        try:
            profile_payload = (
                json.loads(raw_profile_telemetry)
                if isinstance(raw_profile_telemetry, str)
                else raw_profile_telemetry
            )
        except (TypeError, ValueError):
            continue
        if isinstance(profile_payload, dict):
            persisted_profile_telemetry.append(profile_payload)
    profile_budget = merge_profiling_budgets(
        loaded_plan.get("profiling_budget") or {},
        build_profiling_budget(persisted_profile_telemetry),
    )
    return (
        loaded_benchmarks,
        loaded_prompt_context,
        loaded_inventory_record,
        loaded_plan_record,
        profile_budget,
    )


try:
    (
        benchmarks,
        prompt_matching_context,
        inventory_record,
        plan_record,
        wide_schema_profile_budget,
    ) = _load_optimize_inputs()
    wide_schema_inventory = inventory_record["payload"]
    wide_schema_plan = plan_record["payload"]
    _log(
        "Optimize inputs",
        run_id=run_id,
        benchmarks=len(benchmarks),
        levers=levers,
        max_attempts=max_attempts,
        target_accuracy=target_accuracy,
    )
except Exception as exc:
    _banner("Optimize Input Loading FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    write_failure_stage_safely(
        spark,
        run_id,
        "OPTIMIZE",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
        error_message=str(exc),
    )
    _diagnostic(
        "Input loading failed",
        run_id=run_id,
        log_schema=f"{catalog}.{schema}",
        error_type=type(exc).__name__,
        error_message=str(exc),
        next_sources=["genie_opt_artifacts", f"genie_benchmarks_{domain}"],
    )
    raise

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
        prompt_matching_context=prompt_matching_context,
        wide_schema_inventory=wide_schema_inventory,
        wide_schema_plan=wide_schema_plan,
        wide_schema_parent_artifact_id=plan_record.get("artifact_id"),
        wide_schema_profile_budget=wide_schema_profile_budget,
        diagnostic_callback=_diagnostic,
        mv_attach_views=mv_attach_views,
        mv_consent_id=mv_consent_id,
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
    write_failure_stage_safely(
        spark,
        run_id,
        "OPTIMIZE",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
        error_message=str(exc),
    )
    _diagnostic(
        "Task failed",
        run_id=run_id,
        log_schema=f"{catalog}.{schema}",
        error_type=type(exc).__name__,
        error_message=str(exc),
        next_source="genie_opt_stages",
    )
    raise

from genie_space_optimizer.optimization.wide_schema_prompt import (
    drain_prompt_telemetry,
)

_prompt_telemetry = drain_prompt_telemetry()
if _prompt_telemetry:
    write_artifact(
        spark,
        run_id,
        "wide_schema_prompt_telemetry",
        {"stage": _TASK_KEY, "requests": _prompt_telemetry},
        catalog=catalog,
        schema=schema,
        stage_name=_TASK_KEY,
        source_notebook="run_optimize.py",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View Advisor Phase (gated, proposal-only)

# COMMAND ----------

# A phase, not a task (MV-D3). It runs after the loop because it consumes
# iteration-0 generated SQL from Delta and has no reason to block optimization,
# and it only ever proposes — object creation is a backend/OBO surface under
# MV-D1, and this notebook runs as the service principal.
#
# run_mv_advisor_phase never raises: it writes its own stage row and returns an
# outcome either way, so nothing below this point can be reached differently
# because the advisor failed.
_mv_outcome = run_mv_advisor_phase(
    spark,
    run_id=run_id,
    space_id=space_id,
    catalog=catalog,
    schema=schema,
    enabled=enable_metric_view_suggestions,
    benchmarks=benchmarks,
    wide_schema_inventory=wide_schema_inventory,
    w=w,
    warehouse_id=warehouse_id,
    embedding_client=FoundationModelEmbeddingClient(w),
    # Benchmark question text is S's intent side. Passing it here is the one
    # sanctioned route for that text: it is consumed as vectors and never stored,
    # while the proposal's evidence carries question *ids* only.
    intent_texts=[
        str(b.get("question") or "") for b in (benchmarks or ()) if isinstance(b, dict)
    ],
    domain=domain,
)
_log("Metric view advisor", **_mv_outcome.detail())

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

_diagnostic(
    "Task outcome",
    run_id=run_id,
    log_schema=f"{catalog}.{schema}",
    terminal_reason=loop_out.get("terminal_reason"),
    champion_iteration=loop_out.get("best_iteration"),
    champion_accuracy=loop_out.get("accuracy"),
    attempts_used=loop_out.get("surgical_attempts_used"),
    primary_sources=["genie_opt_iterations", "genie_opt_patches"],
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
