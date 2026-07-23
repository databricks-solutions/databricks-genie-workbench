# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark QC & Repair (GSO v2 — 4-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `benchmark_qc_and_repair` |
# MAGIC | **Reads** | space metadata, run-row snapshot, benchmark set |
# MAGIC | **Writes** | `genie_opt_artifacts` (`space_metadata`, `benchmark_qc`), `genie_opt_benchmark_mutations`, `genie_opt_stages` |
# MAGIC | **Hard stop** | Invalid after repair budget, or fewer than 15 valid questions after QC |
# MAGIC | **Log label** | `[TASK BENCH_QC]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §5 / §6 / progress §5 K=3)
# MAGIC
# MAGIC Validate the benchmark set → **bounded inline repair/prune** (≤
# MAGIC `benchmark_repair_max_tries`, default 3) → re-validate → push the
# MAGIC quality-reviewed, SQL-valid set into the LIVE space (additive/merge-only) → flow
# MAGIC into `optimize` when at least 15 valid questions remain. A benchmark
# MAGIC still invalid after K tries hard-fails with `BENCHMARK_UNREPAIRABLE`;
# MAGIC a smaller final corpus hard-fails with `INSUFFICIENT_VALID_BENCHMARKS`.
# MAGIC
# MAGIC The bounded try-counting control loop lives in
# MAGIC `optimization.benchmark_repair.run_bounded_benchmark_repair`; this task
# MAGIC wires it to the canonical benchmark-quality reviewer + benchmark synthesis + the
# MAGIC Phase-2 live-space push (`preflight_push_benchmarks_to_space`) and the
# MAGIC `genie_opt_benchmark_mutations` ledger — no re-invention.

# COMMAND ----------

import copy
import json
import os
import traceback
from functools import partial
from typing import Any, cast


from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.config import (
    CONNECTION_POOL_SIZE,
    MAX_BENCHMARK_COUNT,
    MIN_VALID_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.common.genie_client import (
    configure_connection_pool,
    configure_mlflow_connection_pool,
)
from genie_space_optimizer.common.warehouse import (
    export_warehouse_id,
    resolve_warehouse_id,
)
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.iq_scan import collect_rls_audit
from genie_space_optimizer.optimization.benchmark_repair import (
    BenchmarkCorpusTooSmallError,
    BenchmarkUnrepairableError,
    default_id_of,
    require_minimum_valid_benchmarks,
    run_bounded_benchmark_repair,
)
from genie_space_optimizer.optimization.benchmark_quality import (
    QUALITY_REVIEW_VERSION,
    review_benchmark_quality,
)
from genie_space_optimizer.optimization.benchmarking import generate_benchmarks
from genie_space_optimizer.optimization.preflight import (
    preflight_collect_uc_metadata,
    preflight_fetch_config,
    preflight_generate_benchmarks,
    preflight_push_benchmarks_to_space,
    preflight_setup_experiment,
)
from genie_space_optimizer.optimization.space_quality_enrichment import (
    build_prompt_matching_context,
)
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    load_artifacts,
    load_latest_artifact_payload,
    load_latest_artifact_record,
    write_artifact,
    write_failure_stage_safely,
    write_required_artifact,
    write_stage,
)
from genie_space_optimizer.optimization.wide_schema import (
    active_column_keys,
    build_local_evidence,
    build_selection_plan,
    project_active_inventory,
    project_full_inventory,
    revise_plan_for_column,
    revise_plan_with_profile_outcomes,
    sql_column_evidence,
    validate_inventory,
    validate_selection_plan,
)
from genie_space_optimizer.optimization.wide_schema_profile import (
    build_profiling_budget,
    merge_profiling_budgets,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK BENCH_QC"
_TASK_KEY = "benchmark_qc_and_repair"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("benchmark_repair_max_tries", "3")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
domain = dbutils.widgets.get("domain").strip() or "default"
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
apply_mode = dbutils.widgets.get("apply_mode").strip() or "genie_config"
benchmark_repair_max_tries = int(dbutils.widgets.get("benchmark_repair_max_tries") or "3")
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model

if not run_id:
    raise RuntimeError("benchmark_qc_and_repair: run_id parameter is required")
os.environ["GSO_RUN_ID"] = run_id

effective_target = TARGET_BENCHMARK_COUNT
effective_max = MAX_BENCHMARK_COUNT

# COMMAND ----------

w = make_workspace_client()
spark = cast(Any, globals().get("spark"))
configure_connection_pool(w, CONNECTION_POOL_SIZE)
configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

warehouse_id = resolve_warehouse_id(dbutils.widgets.get("warehouse_id").strip())
if warehouse_id:
    export_warehouse_id(warehouse_id)

# Idempotent — intake already created the tables; safe to re-assert on Repair Run.
ensure_optimization_tables(spark, catalog, schema)
if catalog:
    spark.sql(f"USE CATALOG `{catalog}`")
if schema:
    spark.sql(f"USE SCHEMA `{schema}`")

write_stage(
    spark, run_id, "BENCHMARK_QC_AND_REPAIR", "STARTED",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config + UC metadata + initial benchmark generation

# COMMAND ----------

try:
    _banner("Step 01a — Config + UC Metadata + Benchmark Generation")
    ctx_config = preflight_fetch_config(
        w, spark, run_id, space_id, catalog, schema, domain, apply_mode,
    )
    _config = ctx_config["config"]
    _snapshot = ctx_config["snapshot"]
    _genie_table_refs = ctx_config["genie_table_refs"]
    _domain = ctx_config["domain"]

    _inventory_record = load_latest_artifact_record(
        spark, run_id, catalog, schema, "wide_schema_inventory",
    )
    if _inventory_record is None:
        raise RuntimeError("Required wide_schema_inventory artifact is missing")
    _wide_schema_inventory = _inventory_record["payload"]
    validate_inventory(_wide_schema_inventory)

    _wide_schema_evidence = load_latest_artifact_payload(
        spark, run_id, catalog, schema, "wide_schema_evidence",
    )
    if (
        not isinstance(_wide_schema_evidence, dict)
        or _wide_schema_evidence.get("inventory_hash")
        != _wide_schema_inventory["inventory_hash"]
    ):
        # Evidence is optional. Reconstruct the always-available local subset
        # and continue without query-history signal when its artifact is absent
        # or belongs to a different inventory.
        _wide_schema_evidence = build_local_evidence(
            _config, _wide_schema_inventory,
        )

    _plan_record = load_latest_artifact_record(
        spark, run_id, catalog, schema, "wide_schema_selection_plan",
    )
    if _plan_record is not None:
        _wide_schema_plan = _plan_record["payload"]
        validate_selection_plan(
            _wide_schema_plan,
            inventory_hash=_wide_schema_inventory["inventory_hash"],
        )
    else:
        _wide_schema_plan = build_selection_plan(
            _wide_schema_inventory,
            _wide_schema_evidence,
            run_id=run_id,
        )
        _plan_record = write_required_artifact(
            spark,
            run_id,
            "wide_schema_selection_plan",
            _wide_schema_plan,
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_benchmark_qc_and_repair.py",
            iteration=_wide_schema_plan["revision"],
            parent_artifact_id=_inventory_record.get("artifact_id"),
        )

    _existing_space_metadata = load_latest_artifact_payload(
        spark,
        run_id,
        catalog,
        schema,
        "space_metadata",
    )
    if (
        isinstance(_existing_space_metadata, dict)
        and _existing_space_metadata.get("inventory_hash")
        == _wide_schema_inventory["inventory_hash"]
        and _existing_space_metadata.get("plan_hash")
        == _wide_schema_plan["plan_hash"]
    ):
        _config["_data_profile"] = copy.deepcopy(
            _existing_space_metadata.get("data_profile") or {},
        )

    _profile_artifact_rows = load_artifacts(
        spark,
        run_id,
        catalog,
        schema,
        artifact_kind="wide_schema_profile_telemetry",
    )
    _persisted_profile_telemetry: list[dict[str, Any]] = []
    for _raw_profile_telemetry in _profile_artifact_rows.get(
        "artifact_json", [],
    ):
        try:
            _profile_payload = (
                json.loads(_raw_profile_telemetry)
                if isinstance(_raw_profile_telemetry, str)
                else _raw_profile_telemetry
            )
        except (TypeError, ValueError):
            continue
        if isinstance(_profile_payload, dict):
            _persisted_profile_telemetry.append(_profile_payload)
    _wide_schema_profile_budget = merge_profiling_budgets(
        _wide_schema_plan.get("profiling_budget") or {},
        build_profiling_budget(_persisted_profile_telemetry),
    )

    ctx_uc = preflight_collect_uc_metadata(
        w, spark, run_id, catalog, schema, _config, _snapshot,
        _genie_table_refs, apply_mode=apply_mode,
        configured_cols=ctx_config.get("configured_cols", 0),
        warehouse_id=warehouse_id,
        wide_schema_inventory=_wide_schema_inventory,
        wide_schema_plan=_wide_schema_plan,
        wide_schema_profile_budget=_wide_schema_profile_budget,
    )
    _uc_columns = ctx_uc["uc_columns"]
    _uc_tags = ctx_uc["uc_tags"]
    _uc_routines = ctx_uc["uc_routines"]
    _deterministic_uc_columns = project_full_inventory(_wide_schema_inventory)

    _initial_profile_outcomes = ctx_uc.get("wide_schema_profile_outcomes") or {}
    if ctx_uc.get("wide_schema_profile_telemetry"):
        write_artifact(
            spark,
            run_id,
            "wide_schema_profile_telemetry",
            {
                "stage": "initial",
                **ctx_uc["wide_schema_profile_telemetry"],
            },
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_benchmark_qc_and_repair.py",
        )
    if _initial_profile_outcomes:
        _wide_schema_plan = revise_plan_with_profile_outcomes(
            _wide_schema_plan,
            _wide_schema_inventory,
            _initial_profile_outcomes,
            profiling_budget=_wide_schema_profile_budget,
        )
        _plan_record = write_required_artifact(
            spark,
            run_id,
            "wide_schema_selection_plan",
            _wide_schema_plan,
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_benchmark_qc_and_repair.py",
            iteration=_wide_schema_plan["revision"],
            parent_artifact_id=_plan_record.get("artifact_id"),
        )
    validate_selection_plan(
        _wide_schema_plan,
        inventory_hash=_wide_schema_inventory["inventory_hash"],
    )
    os.environ["GSO_WIDE_SCHEMA_INVENTORY_HASH"] = _wide_schema_inventory[
        "inventory_hash"
    ]
    os.environ["GSO_WIDE_SCHEMA_PLAN_HASH"] = _wide_schema_plan["plan_hash"]
    _config["_wide_schema_inventory_hash"] = _wide_schema_inventory["inventory_hash"]
    _config["_wide_schema_plan_hash"] = _wide_schema_plan["plan_hash"]
    _config["_wide_schema_inventory_column_count"] = sum(
        len(asset.get("columns") or [])
        for asset in _wide_schema_inventory.get("assets") or []
    )
    _uc_columns = project_active_inventory(
        _wide_schema_inventory,
        _wide_schema_plan,
    )
    _config["_uc_columns"] = _uc_columns

    _parsed = _config.get("_parsed_space", {})
    _data_sources = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
    _space_sources = (
        list(_data_sources.get("tables") or [])
        + list(_data_sources.get("metric_views") or [])
    ) if isinstance(_data_sources, dict) else []
    try:
        _config["_rls_audit"] = collect_rls_audit(
            _space_sources,
            spark=spark,
            w=w,
            warehouse_id=warehouse_id,
        )
    except Exception as exc:
        _log(
            "RLS audit unavailable for prompt matching; continuing fail-open",
            reason=f"{type(exc).__name__}: {exc}",
        )
        _config["_rls_audit"] = {}
    write_artifact(
        spark,
        run_id,
        "space_metadata",
        build_prompt_matching_context(_config),
        catalog=catalog,
        schema=schema,
        stage_name=_TASK_KEY,
        source_notebook="run_benchmark_qc_and_repair.py",
    )

    ctx_bench = preflight_generate_benchmarks(
        w, spark, run_id, catalog, schema, _config,
        _uc_columns, _uc_tags, _uc_routines, _domain,
        space_id=space_id, experiment_name=None, warehouse_id=warehouse_id,
        target_benchmark_count=effective_target, max_benchmark_count=effective_max,
    )
    _benchmarks = ctx_bench["benchmarks"]
    _log("Initial benchmarks", count=len(_benchmarks), regenerated=ctx_bench["regenerated"])
except Exception as exc:
    _banner("Benchmark Generation FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    write_failure_stage_safely(
        spark, run_id, "BENCHMARK_QC_AND_REPAIR",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

# COMMAND ----------


def _activate_benchmark_columns(benchmarks: list[dict[str, Any]]) -> None:
    """Persist adaptive revisions for omitted columns used by this operation."""
    global _wide_schema_plan, _plan_record, _uc_columns

    referenced: set[tuple[str, str, str, str]] = set()
    for benchmark in benchmarks:
        sql = benchmark.get("expected_sql") or benchmark.get("sql") or ""
        if isinstance(sql, list):
            sql = " ".join(str(part) for part in sql)
        for item in sql_column_evidence(str(sql), _wide_schema_inventory):
            referenced.add(tuple(item["column_key"]))
    omitted = sorted(referenced - active_column_keys(_wide_schema_plan))
    if not omitted:
        return

    for column_key in omitted:
        try:
            _wide_schema_plan = revise_plan_for_column(
                _wide_schema_plan,
                _wide_schema_inventory,
                column_key,
                reason="REPAIR_FAILURE",
                protected_column_keys=referenced,
            )
        except ValueError as exc:
            _log(
                "Adaptive column activation skipped",
                column=".".join(column_key),
                reason=str(exc),
            )
            continue
        _plan_record = write_required_artifact(
            spark,
            run_id,
            "wide_schema_selection_plan",
            _wide_schema_plan,
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_benchmark_qc_and_repair.py",
            iteration=_wide_schema_plan["revision"],
            parent_artifact_id=_plan_record.get("artifact_id"),
        )

    pending = {
        tuple(row["column_key"])
        for asset in _wide_schema_plan.get("assets") or []
        for row in asset.get("columns") or []
        if row.get("active") and row.get("profile_status") == "pending"
    }
    if pending:
        profile_result: dict[str, Any] = {}
        if warehouse_id:
            from genie_space_optimizer.optimization.wide_schema_profile import (
                run_bounded_profile,
            )

            profile_result = run_bounded_profile(
                w,
                warehouse_id,
                _wide_schema_inventory,
                _wide_schema_plan,
                run_id=run_id,
                budget=_wide_schema_profile_budget,
            )
            outcomes = profile_result.get("outcomes") or {}
            write_artifact(
                spark,
                run_id,
                "wide_schema_profile_telemetry",
                {
                    "stage": "benchmark_adaptive",
                    **(profile_result.get("telemetry") or {}),
                    "asset_statement_counts": profile_result.get(
                        "asset_statement_counts"
                    ) or {},
                },
                catalog=catalog,
                schema=schema,
                stage_name=_TASK_KEY,
                source_notebook="run_benchmark_qc_and_repair.py",
                parent_artifact_id=_plan_record.get("artifact_id"),
            )
        else:
            outcomes = {
                key: {
                    "profile_status": "metadata_only",
                    "submitted": False,
                    "available_metrics": [],
                }
                for key in pending
            }
        _wide_schema_plan = revise_plan_with_profile_outcomes(
            _wide_schema_plan,
            _wide_schema_inventory,
            outcomes,
            profiling_budget=_wide_schema_profile_budget,
        )
        _plan_record = write_required_artifact(
            spark,
            run_id,
            "wide_schema_selection_plan",
            _wide_schema_plan,
            catalog=catalog,
            schema=schema,
            stage_name=_TASK_KEY,
            source_notebook="run_benchmark_qc_and_repair.py",
            iteration=_wide_schema_plan["revision"],
            parent_artifact_id=_plan_record.get("artifact_id"),
        )
        for asset_id, asset_profile in (profile_result.get("data_profile") or {}).items():
            current = _config.setdefault("_data_profile", {}).setdefault(
                asset_id,
                {"row_count": -1, "columns": {}, "kind": asset_profile.get("kind")},
            )
            if asset_profile.get("row_count", -1) >= 0:
                current["row_count"] = asset_profile["row_count"]
            current.setdefault("columns", {}).update(asset_profile.get("columns") or {})

    _uc_columns = project_active_inventory(
        _wide_schema_inventory,
        _wide_schema_plan,
    )
    _config["_uc_columns"] = _uc_columns
    _config["_wide_schema_plan_hash"] = _wide_schema_plan["plan_hash"]
    write_artifact(
        spark,
        run_id,
        "space_metadata",
        build_prompt_matching_context(_config),
        catalog=catalog,
        schema=schema,
        stage_name=_TASK_KEY,
        source_notebook="run_benchmark_qc_and_repair.py",
        parent_artifact_id=_plan_record.get("artifact_id"),
    )


_activate_benchmark_columns(_benchmarks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bounded inline repair/prune (≤ benchmark_repair_max_tries)
# MAGIC
# MAGIC Quality-review the set, regenerate/prune the invalid subset, and
# MAGIC re-review — bounded by `benchmark_repair_max_tries`. The quality review
# MAGIC includes SQL/data validity, question clarity, and question↔SQL alignment.
# MAGIC A try is consumed only when ≥1 question is still invalid after
# MAGIC re-validation (progress §5).

# COMMAND ----------


_quality_findings_by_key: dict[str, dict] = {}
_quality_results_by_id: dict[str, dict] = {}
_rejected_benchmarks_by_id: dict[str, dict] = {}
_quality_review_status = "complete"
_semantic_review_coverage = 1.0


def _validate_fn(bms: list[dict]) -> tuple[list[dict], list[dict]]:
    """Comprehensively review ``bms`` and partition into eligible/excluded."""
    global _quality_review_status, _semantic_review_coverage

    _activate_benchmark_columns(bms)
    review = review_benchmark_quality(
        bms,
        spark,
        catalog=catalog,
        schema=schema,
        w=w,
        warehouse_id=warehouse_id,
        config=_config,
        uc_columns=_uc_columns,
        deterministic_uc_columns=_deterministic_uc_columns,
        uc_routines=_uc_routines,
    )
    if review.get("review_status") != "complete":
        _quality_review_status = "degraded"
    _semantic_review_coverage = min(
        _semantic_review_coverage,
        float(review.get("semantic_review_coverage", 0.0) or 0.0),
    )

    for result in review.get("benchmark_results", []):
        qid = str(result.get("question_id") or result.get("question") or "")
        if qid:
            _quality_results_by_id[qid] = result
    for finding in review.get("findings", []):
        key = "|".join(
            [
                str(finding.get("question_id") or ""),
                str(finding.get("category") or ""),
                str(finding.get("code") or ""),
            ]
        )
        _quality_findings_by_key[key] = finding
    for benchmark, result in zip(bms, review.get("benchmark_results", [])):
        if result.get("disposition") != "excluded":
            continue
        qid = str(result.get("question_id") or default_id_of(benchmark) or result.get("question") or "")
        rejected = dict(benchmark)
        errors = [
            f for f in result.get("findings", []) if f.get("severity") == "error"
        ]
        if errors:
            rejected["validation_reason_code"] = str(errors[0].get("code") or "quality_rejected").lower()
            rejected["validation_error"] = str(errors[0].get("explanation") or "")[:200]
        _rejected_benchmarks_by_id[qid] = rejected

    return list(review.get("accepted", [])), list(review.get("excluded", []))


def _repair_fn(invalid: list[dict], valid: list[dict]) -> list[dict]:
    """One repair sweep: prune the invalid rows and synthesize replacements
    toward the 30–40 target, keeping the already-valid set as context.

    Returns ONLY the newly synthesized candidates (not the already-valid set),
    so the bounded loop accumulates valid rows correctly.
    """
    _activate_benchmark_columns(invalid)
    refilled = generate_benchmarks(
        w, _config, _uc_columns, _uc_tags, _uc_routines, _domain,
        catalog, schema, spark,
        target_count=effective_target,
        existing_benchmarks=valid,
        warehouse_id=warehouse_id,
        max_benchmark_count=effective_max,
    )
    valid_ids = {default_id_of(b) for b in valid}
    return [b for b in refilled if default_id_of(b) not in valid_ids]


_repair_failed = False
_repair_error: BenchmarkUnrepairableError | BenchmarkCorpusTooSmallError | None = None
try:
    _banner("Step 01b — Bounded Benchmark Repair")
    outcome = run_bounded_benchmark_repair(
        _benchmarks,
        validate_fn=_validate_fn,
        repair_fn=_repair_fn,
        max_tries=benchmark_repair_max_tries,
    )
    _benchmarks = outcome.benchmarks
    _repair_tries_used = outcome.tries_used
    _repaired_ids = outcome.repaired_ids
    _repair_sweeps = outcome.sweeps
    _final_validity = True
    require_minimum_valid_benchmarks(
        _benchmarks,
        minimum_count=MIN_VALID_BENCHMARK_COUNT,
        target_count=effective_target,
        context="bounded benchmark quality review and repair",
    )
    _log(
        "Benchmark repair complete",
        valid_count=len(_benchmarks),
        tries_used=_repair_tries_used,
        repaired=len(_repaired_ids),
    )
except BenchmarkUnrepairableError as exc:
    _repair_failed = True
    _repair_error = exc
    _benchmarks = exc.valid
    _repair_tries_used = exc.tries_used
    _repaired_ids = []
    _repair_sweeps = []
    _final_validity = False
    _log(
        "Benchmark UNREPAIRABLE",
        tries_used=exc.tries_used,
        still_invalid=[default_id_of(b) for b in exc.still_invalid],
    )
except BenchmarkCorpusTooSmallError as exc:
    _repair_failed = True
    _repair_error = exc
    _final_validity = False
    _log(
        "Benchmark corpus below minimum",
        valid_count=exc.valid_count,
        minimum_count=exc.minimum_count,
        target_count=exc.target_count,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Push validated set to the LIVE space (additive/merge-only)
# MAGIC
# MAGIC Reuses the Phase-2 publisher: additive push of the quality-reviewed set,
# MAGIC 30–40 window recommendation, the §3.6 leakage firewall (in the applier),
# MAGIC and the §3.5 `genie_opt_benchmark_mutations` ledger. Skipped on the
# MAGIC unrepairable path (we hard-fail below instead of mutating the live space).

# COMMAND ----------

_push = {}
_window: dict[str, Any] = {}
if not _repair_failed and _benchmarks:
    try:
        _banner("Step 01c — Push Benchmarks to Live Space")
        _push = preflight_push_benchmarks_to_space(
            w, spark, run_id, space_id, catalog, schema, _benchmarks,
            rejected_benchmarks=list(_rejected_benchmarks_by_id.values()),
        )
        _window = _push.get("window", {})
        _log(
            "Push complete",
            published=_push.get("published_count"),
            window_status=_window.get("status"),
            ledger_rows=_push.get("ledger_rows"),
        )
    except Exception as exc:
        _banner("Benchmark Push FAILED")
        _log("Push failure", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
        write_failure_stage_safely(
            spark, run_id, "BENCHMARK_QC_AND_REPAIR",
            task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the evaluation dataset + experiment

# COMMAND ----------

_experiment_name = None
_persisted_count = len(_benchmarks)
if not _repair_failed and _benchmarks:
    try:
        ctx_exp = preflight_setup_experiment(
            w, spark, run_id, space_id, catalog, schema, _domain,
            _config, _benchmarks, _uc_columns, _uc_tags, _uc_routines,
            _genie_table_refs, None, max_benchmark_count=effective_max,
        )
        _experiment_name = ctx_exp.get("experiment_name")
        _persisted_count = int(ctx_exp.get("benchmark_count", len(_benchmarks)))
    except Exception as exc:
        _banner("Experiment / Dataset Setup FAILED")
        _log("Failure", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
        write_failure_stage_safely(
            spark, run_id, "BENCHMARK_QC_AND_REPAIR",
            task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the benchmark_qc artifact + flow into optimize (or hard-fail)

# COMMAND ----------


def _final_quality_result(benchmark: dict) -> dict:
    qid = default_id_of(benchmark)
    if qid and qid in _quality_results_by_id:
        return _quality_results_by_id[qid]
    question = str(benchmark.get("question") or "").strip().lower()
    return next(
        (
            result
            for result in _quality_results_by_id.values()
            if str(result.get("question") or "").strip().lower() == question
        ),
        {},
    )


_final_quality_results = [_final_quality_result(b) for b in _benchmarks]

_qc_payload: dict[str, Any] = {
    "run_id": run_id,
    "valid_count": len(_benchmarks),
    "minimum_valid_count": MIN_VALID_BENCHMARK_COUNT,
    "target_count": effective_target,
    "persisted_count": _persisted_count,
    "repair_tries_used": _repair_tries_used,
    "repaired_ids": _repaired_ids,
    "repair_sweeps": _repair_sweeps,
    "benchmark_repair_max_tries": benchmark_repair_max_tries,
    "final_validity": _final_validity,
    "window": _window,
    "window_target_min": 30,
    "window_target_max": 40,
    "quality_review_version": QUALITY_REVIEW_VERSION,
    "quality_review_status": _quality_review_status,
    "semantic_review_coverage": _semantic_review_coverage,
    "quality_findings": list(_quality_findings_by_key.values()),
    "quality_counts": {
        "total": len(_benchmarks),
        "trusted": sum(
            1
            for result in _final_quality_results
            if result.get("disposition") == "passed"
        ),
        "warnings": sum(
            1
            for result in _final_quality_results
            if result.get("disposition") == "warning"
        ),
        "excluded": len(_rejected_benchmarks_by_id),
        "review_not_run": sum(
            1
            for f in _quality_findings_by_key.values()
            if f.get("code") == "REVIEW_NOT_RUN"
        ),
    },
    "proposed_changes": [
        {
            "question_id": f.get("question_id"),
            "question": f.get("question"),
            "proposed_question": f.get("proposed_question"),
            "proposed_sql": f.get("proposed_sql"),
            "reason": f.get("code"),
        }
        for f in _quality_findings_by_key.values()
        if f.get("proposed_question") or f.get("proposed_sql")
    ],
    # GT-correction candidates folded in here (retired
    # genie_eval_gt_correction_candidates table — §7 reconciliation).
    "gt_correction_candidates": [],
}
if _repair_failed and _repair_error is not None:
    _qc_payload["terminal_reason"] = _repair_error.terminal_reason
    if isinstance(_repair_error, BenchmarkUnrepairableError):
        _qc_payload["still_invalid_ids"] = [
            default_id_of(b) for b in _repair_error.still_invalid
        ]

write_artifact(
    spark, run_id, "benchmark_qc", _qc_payload,
    catalog=catalog, schema=schema,
    stage_name=_TASK_KEY, source_notebook="run_benchmark_qc_and_repair.py",
)

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
        source_notebook="run_benchmark_qc_and_repair.py",
        parent_artifact_id=_plan_record.get("artifact_id"),
    )

if _repair_failed and _repair_error is not None:
    write_failure_stage_safely(
        spark, run_id, "BENCHMARK_QC_AND_REPAIR",
        task_key=_TASK_KEY, catalog=catalog, schema=schema,
        detail={
            "terminal_reason": _repair_error.terminal_reason,
            "valid_count": len(_benchmarks),
            "minimum_valid_count": MIN_VALID_BENCHMARK_COUNT,
        },
        error_message=str(_repair_error),
    )
    # Hard stop — invalid questions or an undersized final corpus.
    raise _repair_error

write_stage(
    spark, run_id, "BENCHMARK_QC_AND_REPAIR", "COMPLETE",
    task_key=_TASK_KEY, catalog=catalog, schema=schema,
    detail={
        "valid_count": len(_benchmarks),
        "repair_tries_used": _repair_tries_used,
        "window_status": _window.get("status"),
    },
)

_banner("Benchmark QC and repair completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "valid_count": len(_benchmarks),
    "repair_tries_used": _repair_tries_used,
}, default=str))
