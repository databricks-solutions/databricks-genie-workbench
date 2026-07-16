# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark QC & Repair (GSO v2 — 4-task DAG)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `benchmark_qc_and_repair` |
# MAGIC | **Reads** | space metadata, run-row snapshot, benchmark set |
# MAGIC | **Writes** | `genie_opt_artifacts` (`benchmark_qc`), `genie_opt_benchmark_mutations`, `genie_opt_stages` |
# MAGIC | **Hard stop** | `BENCHMARK_UNREPAIRABLE` if still invalid after `benchmark_repair_max_tries` |
# MAGIC | **Log label** | `[TASK BENCH_QC]` |
# MAGIC
# MAGIC ## 🎯 Purpose (arch §5 / §6 / progress §5 K=3)
# MAGIC
# MAGIC Validate the benchmark set → **bounded inline repair/prune** (≤
# MAGIC `benchmark_repair_max_tries`, default 3) → re-validate → push the
# MAGIC quality-reviewed, SQL-valid set into the LIVE space (additive/merge-only) → flow
# MAGIC **unconditionally** into `optimize`. Only a benchmark still invalid after K
# MAGIC tries hard-fails with `BENCHMARK_UNREPAIRABLE`.
# MAGIC
# MAGIC The bounded try-counting control loop lives in
# MAGIC `optimization.benchmark_repair.run_bounded_benchmark_repair`; this task
# MAGIC wires it to the canonical benchmark-quality reviewer + benchmark synthesis + the
# MAGIC Phase-2 live-space push (`preflight_push_benchmarks_to_space`) and the
# MAGIC `genie_opt_benchmark_mutations` ledger — no re-invention.

# COMMAND ----------

import json
import os
import traceback
from functools import partial
from typing import Any, cast


from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.common.config import (
    CONNECTION_POOL_SIZE,
    MAX_BENCHMARK_COUNT,
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
from genie_space_optimizer.optimization.benchmark_repair import (
    BENCHMARK_UNREPAIRABLE,
    BenchmarkUnrepairableError,
    default_id_of,
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
from genie_space_optimizer.optimization.state import (
    ensure_optimization_tables,
    write_artifact,
    write_stage,
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

    ctx_uc = preflight_collect_uc_metadata(
        w, spark, run_id, catalog, schema, _config, _snapshot,
        _genie_table_refs, apply_mode=apply_mode,
        configured_cols=ctx_config.get("configured_cols", 0),
        warehouse_id=warehouse_id,
    )
    _uc_columns = ctx_uc["uc_columns"]
    _uc_tags = ctx_uc["uc_tags"]
    _uc_routines = ctx_uc["uc_routines"]

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
    write_stage(
        spark, run_id, "BENCHMARK_QC_AND_REPAIR", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema, error_message=str(exc),
    )
    raise

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

    review = review_benchmark_quality(
        bms,
        spark,
        catalog=catalog,
        schema=schema,
        w=w,
        warehouse_id=warehouse_id,
        config=_config,
        uc_columns=_uc_columns,
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
_repair_error: BenchmarkUnrepairableError | None = None
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
        write_stage(
            spark, run_id, "BENCHMARK_QC_AND_REPAIR", "FAILED",
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
        write_stage(
            spark, run_id, "BENCHMARK_QC_AND_REPAIR", "FAILED",
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
    _qc_payload["terminal_reason"] = BENCHMARK_UNREPAIRABLE
    _qc_payload["still_invalid_ids"] = [
        default_id_of(b) for b in _repair_error.still_invalid
    ]

write_artifact(
    spark, run_id, "benchmark_qc", _qc_payload,
    catalog=catalog, schema=schema,
    stage_name=_TASK_KEY, source_notebook="run_benchmark_qc_and_repair.py",
)

if _repair_failed and _repair_error is not None:
    write_stage(
        spark, run_id, "BENCHMARK_QC_AND_REPAIR", "FAILED",
        task_key=_TASK_KEY, catalog=catalog, schema=schema,
        detail={"terminal_reason": BENCHMARK_UNREPAIRABLE},
        error_message=str(_repair_error),
    )
    # Hard stop — the only failure mode of 01 (arch §5.1 / §6).
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
