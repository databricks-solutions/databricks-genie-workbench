"""
Optimization Harness — stage functions for the 6-task Databricks Job.

The canonical execution path is the **6-task DAG** launched via
``submit_optimization()`` in ``job_launcher.py``.  Each DAG notebook is a
thin wrapper that deserializes task values, calls a single harness function,
and publishes outputs.

Each ``_run_*`` / ``_prepare_*`` function encapsulates all business logic
for its stage so that both the DAG notebooks and the ``optimize_genie_space()``
convenience function (used for dev/test only) share identical code paths.

Architecture: ``preflight`` → ``baseline_eval`` → ``enrichment`` →
``lever_loop`` → ``finalize`` → ``deploy``.  Inter-task data flows via
``dbutils.jobs.taskValues``.  Detailed state goes to Delta.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from collections import Counter

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    ARBITER_CORRECTION_TRIGGER,
    CONSECUTIVE_ROLLBACK_LIMIT,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    DIMINISHING_RETURNS_EPSILON,
    DIMINISHING_RETURNS_LOOKBACK,
    ENABLE_PROMPT_MATCHING_AUTO_APPLY,
    ENABLE_SLICE_GATE,
    FINALIZE_REPEATABILITY_PASSES,
    GENIE_CORRECT_CONFIRMATION_THRESHOLD,
    GT_REPAIR_PROMPT,
    INLINE_EVAL_DELAY,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    LEVER_NAMES,
    MAX_ITERATIONS,
    MAX_NOISE_FLOOR,
    NEITHER_CORRECT_QUARANTINE_THRESHOLD,
    NEITHER_CORRECT_REPAIR_THRESHOLD,
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
    REGRESSION_THRESHOLD,
    SLICE_GATE_MIN_REDUCTION,
    SLICE_GATE_TOLERANCE,
    format_mlflow_template,
)
from genie_space_optimizer.optimization.applier import (
    _get_general_instructions,
    apply_patch_set,
    auto_apply_prompt_matching,
    proposals_to_patches,
    rollback,
)
from genie_space_optimizer.optimization.evaluation import (
    _extract_genie_sql_from_trace,
    all_thresholds_met,
    extract_reference_sqls,
    extract_reference_result_hashes,
    filter_benchmarks_by_scope,
    log_asi_feedback_on_traces,
    log_gate_feedback_on_traces,
    log_judge_verdicts_on_traces,
    log_persistence_context_on_traces,
    make_predict_fn,
    normalize_scores,
    register_instruction_version,
    run_evaluation,
    run_repeatability_evaluation,
)
from genie_space_optimizer.optimization.models import (
    create_genie_model_version,
    promote_best_model,
)
from genie_space_optimizer.optimization.optimizer import (
    _call_llm_for_adaptive_strategy,
    _enrich_blank_descriptions,
    _enrich_table_descriptions,
    _generate_holistic_strategy,
    _mine_benchmark_example_sqls,
    cluster_failures,
    detect_regressions,
    enrich_metadata_with_uc_types,
    format_reflection_buffer,
    generate_metadata_proposals,
    generate_proposals_from_strategy,
    rank_clusters,
)
from genie_space_optimizer.optimization.preflight import run_preflight
from genie_space_optimizer.optimization.repeatability import run_repeatability_test
from genie_space_optimizer.optimization.report import generate_report
from genie_space_optimizer.optimization.scorers import make_all_scorers
from genie_space_optimizer.optimization.state import (
    create_run,
    ensure_optimization_tables,
    load_all_full_iterations,
    load_latest_full_iteration,
    load_run,
    load_stages,
    mark_patches_rolled_back,
    update_iteration_reflection,
    update_provenance_gate,
    update_provenance_proposals,
    update_run_status,
    write_asi_results,
    write_iteration,
    write_patch,
    write_provenance,
    write_stage,
    write_suggestion,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

FINALIZE_TIMEOUT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS", "6600"),
)
FINALIZE_HEARTBEAT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS", "30"),
)


_W = 78

def _section(title: str, char: str = "=") -> str:
    pad = max(0, _W - len(title) - 4)
    return f"\n{char * 2} {title} {char * pad}"


def _kv(key: str, value: object, indent: int = 2) -> str:
    return f"{' ' * indent}{'| ' if indent == 0 else '|  '}{key + ':':<28s} {value}"


def _bar(char: str = "-") -> str:
    return char * _W


_PATCH_TYPE_LABELS: dict[str, str] = {
    "add_instruction": "Add Instruction",
    "update_instruction": "Update Instruction",
    "remove_instruction": "Remove Instruction",
    "rewrite_instruction": "Rewrite Instruction",
    "add_example_sql": "Add Example SQL",
    "update_example_sql": "Update Example SQL",
    "remove_example_sql": "Remove Example SQL",
    "add_description": "Add Table Description",
    "update_description": "Update Table Description",
    "add_column_description": "Add Column Description",
    "update_column_description": "Update Column Description",
    "add_column_synonym": "Add Column Synonym",
    "add_join_spec": "Add Join Spec",
    "update_join_spec": "Update Join Spec",
    "remove_join_spec": "Remove Join Spec",
    "update_tvf_sql": "Update TVF SQL",
}


def _fmt_patch(idx: int, patch: dict, action: dict) -> str:
    """Format a single applied patch into a readable multi-line string."""
    ptype = patch.get("type", action.get("action_type", "?"))
    label = _PATCH_TYPE_LABELS.get(ptype, ptype)
    table = patch.get("table") or patch.get("target") or ""
    column = patch.get("column", "")
    target = f"{table}.{column}" if column else table
    short_target = target.rsplit(".", 1)[-1] if "." in target else target

    lines = [f"|  [{idx}] {label}"]
    if target:
        lines.append(f"|      Target: {target}")

    struct = patch.get("structured_sections") or {}
    if struct and isinstance(struct, dict):
        for sk, sv in struct.items():
            sv_flat = str(sv).replace("\n", " ")[:100]
            lines.append(f"|      {sk}: {sv_flat}")
    else:
        new_text = patch.get("new_text", "")
        if new_text:
            lines.append(f"|      Value: {new_text.replace(chr(10), ' ')[:120]}")

    eq = patch.get("example_question", "")
    esql = patch.get("example_sql", "")
    if eq:
        lines.append(f"|      Question: {eq[:100]}")
    if esql:
        lines.append(f"|      SQL: {esql[:100]}")

    js = patch.get("join_spec")
    if js and isinstance(js, dict):
        left = js.get("left", {}).get("identifier", "?")
        right = js.get("right", {}).get("identifier", "?")
        sql_cond = (js.get("sql") or ["?"])[0][:80]
        lines.append(f"|      Join: {left} <-> {right}")
        lines.append(f"|        ON {sql_cond}")

    return "\n".join(lines)


def _scorecard(scores: dict[str, float], prefix: str = "|  ") -> str:
    parts = [f"{j}={v:.1f}" for j, v in sorted(scores.items())]
    line = "  ".join(parts)
    return f"{prefix}Scores:  {line}"


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_sql_context(spark: SparkSession, catalog: str, schema: str) -> None:
    """Set Spark SQL catalog/schema context explicitly for SQL Connect stability."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


# ── Result Dataclass ──────────────────────────────────────────────────


@dataclass
class OptimizationResult:
    """Outcome of an optimization run (used by convenience function)."""

    run_id: str
    space_id: str
    domain: str
    status: str  # CONVERGED | STALLED | MAX_ITERATIONS | FAILED
    best_iteration: int
    best_accuracy: float
    best_repeatability: float
    best_model_id: str | None
    convergence_reason: str | None
    total_iterations: int
    levers_attempted: list[int] = field(default_factory=list)
    levers_accepted: list[int] = field(default_factory=list)
    levers_rolled_back: list[int] = field(default_factory=list)
    final_scores: dict[str, float] = field(default_factory=dict)
    experiment_name: str = ""
    experiment_id: str = ""
    report_path: str | None = None
    error: str | None = None


# ── Error Handling ────────────────────────────────────────────────────


def _safe_stage(
    state_spark: Any,
    run_id: str,
    stage_name: str,
    fn: Any,
    state_catalog: str,
    state_schema: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Wrap a stage function — on exception write FAILED to Delta and re-raise."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Stage %s FAILED for run %s", stage_name, run_id)
        try:
            write_stage(
                state_spark, run_id, stage_name, "FAILED",
                error_message=err_msg[:500],
                catalog=state_catalog, schema=state_schema,
            )
            update_run_status(
                state_spark, run_id, state_catalog, state_schema,
                status="FAILED",
                convergence_reason=f"error_in_{stage_name}",
            )
        except Exception:
            logger.exception("Failed to write FAILED state for %s", run_id)
        raise


# ── Stage 1: PREFLIGHT ───────────────────────────────────────────────


def _run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
    apply_mode: str = "genie_config",
) -> dict:
    """Stage 1: Fetch config, UC metadata, generate/load benchmarks, create experiment.

    Returns a dict of task values to pass downstream.
    """
    config, benchmarks, model_id, exp_name, human_corrections = _safe_stage(
        spark, run_id, "PREFLIGHT", run_preflight,
        catalog, schema,
        w, spark, run_id, space_id, catalog, schema, domain, experiment_name,
        apply_mode,
    )

    import mlflow
    exp = mlflow.get_experiment_by_name(exp_name)
    experiment_id = exp.experiment_id if exp else ""

    update_run_status(
        spark, run_id, catalog, schema,
        status="IN_PROGRESS",
        experiment_name=exp_name,
        experiment_id=experiment_id,
    )

    return {
        "benchmarks": benchmarks,
        "config": config,
        "model_id": model_id,
        "experiment_name": exp_name,
        "experiment_id": experiment_id,
        "human_corrections": human_corrections,
    }


# ── Stage 2: BASELINE EVAL ──────────────────────────────────────────


def baseline_setup_scorers(
    w: WorkspaceClient,
    spark: SparkSession,
    space_id: str,
    run_id: str,
    catalog: str,
    schema: str,
    exp_name: str,
    model_id: str | None,
    domain: str = "",
) -> dict:
    """Sub-step 2a: Create predict function and scorers. Writes STARTED stage."""
    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "STARTED",
        task_key="baseline_eval", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    _instr_prompt = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE,
        uc_schema=f"{catalog}.{schema}", space_id=space_id,
    )
    predict_fn = make_predict_fn(
        w, space_id, spark, catalog, schema,
        warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        instruction_prompt_name=_instr_prompt,
    )
    scorers = make_all_scorers(w, spark, catalog, schema)

    _lines = [_section("BASELINE — EVALUATION SETUP", "-")]
    _lines.append(_kv("Space ID", space_id))
    _lines.append(_kv("Model ID", model_id))
    _lines.append(_kv("Experiment", exp_name))
    _lines.append(_kv("Scorers", len(scorers)))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "predict_fn": predict_fn,
        "scorers": scorers,
        "model_id": model_id,
        "exp_name": exp_name,
        "space_id": space_id,
        "domain": domain,
    }


def baseline_run_evaluation(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    benchmarks: list[dict],
    setup_ctx: dict,
    w: WorkspaceClient | None = None,
    model_creation_kwargs: dict | None = None,
) -> dict:
    """Sub-step 2b: Run 9-judge evaluation with retry."""
    _ensure_sql_context(spark, catalog, schema)
    eval_result = _safe_stage(
        spark, run_id, "BASELINE_EVAL", run_evaluation,
        catalog, schema,
        setup_ctx["space_id"], setup_ctx["exp_name"], 0, benchmarks,
        setup_ctx["domain"], setup_ctx.get("model_id"), "full",
        setup_ctx["predict_fn"], setup_ctx["scorers"],
        spark=spark, w=w, catalog=catalog, gold_schema=schema,
        uc_schema=f"{catalog}.{schema}",
        model_creation_kwargs=model_creation_kwargs,
    )
    return eval_result


def baseline_display_scorecard(
    eval_result: dict,
    thresholds: dict[str, float] | None = None,
) -> dict:
    """Sub-step 2c: Print per-judge scorecard and return scores summary."""
    _thresholds = thresholds or DEFAULT_THRESHOLDS
    scores = eval_result.get("scores", {})
    overall = eval_result.get("overall_accuracy", 0.0)
    thresholds_met = eval_result.get("thresholds_met", False)

    _lines = [_section("BASELINE EVALUATION — 9-JUDGE SCORECARD", "-")]
    _lines.append(_kv("Overall accuracy", f"{overall:.1f}%"))
    _lines.append("")
    _lines.append(f"  {'Judge':<28s} {'Score':>8s}  {'Threshold':>10s}  {'Status'}")
    for judge in sorted(_thresholds.keys()):
        score_val = scores.get(judge)
        threshold_val = _thresholds[judge]
        if score_val is not None:
            passed = score_val >= threshold_val if threshold_val > 0 else True
            status = "PASS" if passed else "FAIL  <--"
            t_str = f"{threshold_val:.1f}%" if threshold_val > 0 else "--"
            _lines.append(f"  {judge:<28s} {score_val:>7.1f}%  {t_str:>10s}  {status}")
    _lines.append("")
    _lines.append(_kv("Thresholds met", thresholds_met))
    _lines.append(_kv("Eval attempts", eval_result.get("harness_retry_count", 0) + 1))
    _lines.append(_kv("Quarantined questions", eval_result.get("invalid_benchmark_count", 0)))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "scores": scores,
        "overall_accuracy": overall,
        "thresholds_met": thresholds_met,
    }


def baseline_persist_state(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    model_id: str,
    catalog: str,
    schema: str,
    eval_result: dict,
    scorecard: dict,
) -> dict:
    """Sub-step 2d: Write iteration, link scores, log expectations."""
    scores = scorecard["scores"]
    thresholds_met = scorecard["thresholds_met"]

    write_iteration(
        spark, run_id, 0, eval_result,
        catalog=catalog, schema=schema,
        eval_scope="full", model_id=model_id,
    )

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=0,
        best_accuracy=eval_result.get("overall_accuracy", 0.0),
        best_model_id=model_id,
    )

    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "COMPLETE",
        task_key="baseline_eval",
        detail={
            "overall_accuracy": eval_result.get("overall_accuracy", 0.0),
            "thresholds_met": thresholds_met,
            "invalid_benchmark_count": eval_result.get("invalid_benchmark_count", 0),
            "permission_blocked_count": eval_result.get("permission_blocked_count", 0),
            "unresolved_column_count": eval_result.get("unresolved_column_count", 0),
            "harness_retry_count": eval_result.get("harness_retry_count", 0),
        },
        catalog=catalog, schema=schema,
    )

    try:
        from genie_space_optimizer.optimization.evaluation import log_expectations_on_traces
        log_expectations_on_traces(eval_result)
    except Exception:
        logger.debug("Failed to log expectations on baseline traces", exc_info=True)

    try:
        log_judge_verdicts_on_traces(eval_result)
    except Exception:
        logger.debug("Failed to log judge verdicts on baseline traces", exc_info=True)

    _lines = [_section("BASELINE — STATE PERSISTENCE", "-")]
    _lines.append(_kv("Iteration written", 0))
    _lines.append(_kv("Model linked", model_id))
    _lines.append(_kv("Stage", "BASELINE_EVAL_STARTED -> COMPLETE"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "scores": scores,
        "overall_accuracy": eval_result.get("overall_accuracy", 0.0),
        "thresholds_met": thresholds_met,
        "model_id": model_id,
        "eval_result": eval_result,
    }


def _run_baseline(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    benchmarks: list[dict],
    exp_name: str,
    model_id: str,
    catalog: str,
    schema: str,
    domain: str = "",
) -> dict:
    """Stage 2: Run full 8-judge evaluation, check thresholds.

    Wrapper that calls sub-steps in sequence. Returns a dict with scores,
    thresholds_met flag, and model_id.
    """
    try:
        setup_ctx = baseline_setup_scorers(
            w, spark, space_id, run_id, catalog, schema, exp_name, model_id, domain,
        )
        eval_result = baseline_run_evaluation(
            spark, run_id, catalog, schema, benchmarks, setup_ctx, w=w,
        )
        scorecard = baseline_display_scorecard(eval_result)
        return baseline_persist_state(
            w, spark, run_id, model_id, catalog, schema, eval_result, scorecard,
        )
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("BASELINE_EVAL FAILED for run %s", run_id)
        try:
            write_stage(
                spark, run_id, "BASELINE_EVAL", "FAILED",
                task_key="baseline_eval",
                error_message=err_msg[:500],
                catalog=catalog, schema=schema,
            )
            update_run_status(
                spark, run_id, catalog, schema,
                status="FAILED",
                convergence_reason="error_in_BASELINE_EVAL",
            )
        except Exception:
            logger.exception("Failed to write FAILED state for baseline %s", run_id)
        raise


# ── Stage 2.5: PROMPT MATCHING AUTO-CONFIG ──────────────────────────


def _run_prompt_matching_setup(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.5: Enable format assistance and entity matching as best practice.

    Runs between baseline eval and lever loop.  Deterministic (no LLM).
    Returns summary dict with counts of changes applied.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    write_stage(
        spark, run_id, "PROMPT_MATCHING_SETUP", "STARTED",
        task_key="prompt_matching_setup", catalog=catalog, schema=schema,
    )

    try:
        _parsed = config.get("_parsed_space", {})
        _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
        _tbl_count = len(_ds.get("tables", []))
        _mv_count = len(_ds.get("metric_views", []))
        print(
            f"\n[PROMPT MATCHING] Starting auto-config — "
            f"tables: {_tbl_count}, metric_views: {_mv_count}, "
            f"total data sources: {_tbl_count + _mv_count}"
        )

        apply_log = auto_apply_prompt_matching(w, space_id, config)

        applied = apply_log.get("applied", [])
        fa_count = apply_log.get("format_assistance_count", 0)
        em_count = apply_log.get("entity_matching_count", 0)

        for idx, entry in enumerate(applied):
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": entry.get("type", "unknown"),
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": f"{entry.get('table', '')}.{entry.get('column', '')}",
                    "patch": entry,
                    "command": None,
                    "rollback": None,
                    "proposal_id": "prompt_matching_auto_config",
                },
                catalog, schema,
            )

        if applied:
            refreshed = fetch_space_config(w, space_id)
            config["_parsed_space"] = refreshed.get("_parsed_space", refreshed)

        _pm_lines = [_section("PROMPT MATCHING", "-")]
        _pm_lines.append(_kv("Total changes", len(applied)))
        _pm_lines.append(_kv("Format assistance", f"{fa_count} columns"))
        _pm_lines.append(_kv("Entity matching", f"{em_count} columns"))
        _pm_lines.append(_kv("Tables patched", apply_log.get('patched_objects', [])))
        _pm_lines.append(_kv("Genie API PATCH sent", "YES" if applied else "NO"))
        _pm_lines.append(_kv("Config refreshed", "YES" if applied else "N/A"))
        _pm_lines.append(_bar("-"))
        print("\n".join(_pm_lines))

        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "COMPLETE",
            task_key="prompt_matching_setup",
            detail={
                "format_assistance_enabled": fa_count,
                "entity_matching_enabled": em_count,
                "total_changes": len(applied),
                "patched_objects": apply_log.get("patched_objects", []),
            },
            catalog=catalog, schema=schema,
        )

        return {
            "format_assistance_count": fa_count,
            "entity_matching_count": em_count,
            "total_changes": len(applied),
        }

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("PROMPT_MATCHING_SETUP FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "FAILED",
            task_key="prompt_matching_setup",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return {"format_assistance_count": 0, "entity_matching_count": 0, "total_changes": 0}


# ── Stage 2.75: PROACTIVE DESCRIPTION ENRICHMENT ───────────────────


def _run_description_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.75: Generate structured descriptions for columns and tables.

    Runs after UC type enrichment and before the strategist.  Targets
    columns and tables whose descriptions are insufficient (< 10 chars)
    in both the Genie Space and Unity Catalog.
    Applies patches via update_sections with lever=0 (pre-optimization).

    Returns summary dict with column and table enrichment counts.
    """
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )
    from genie_space_optimizer.optimization.structured_metadata import (
        entity_type_for_column,
        update_sections,
    )

    write_stage(
        spark, run_id, "DESCRIPTION_ENRICHMENT", "STARTED",
        task_key="description_enrichment", catalog=catalog, schema=schema,
    )

    result = {
        "total_eligible": 0, "total_enriched": 0, "total_skipped": 0,
        "tables_eligible": 0, "tables_enriched": 0, "tables_skipped": 0,
    }

    try:
        # ── Column description enrichment ────────────────────────────
        _data_profile = metadata_snapshot.get("_data_profile", {})
        col_patches = _enrich_blank_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["total_eligible"] = len(col_patches)

        ds = metadata_snapshot.get("data_sources", {})
        if not isinstance(ds, dict):
            ds = {}
        tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
        mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
        all_objects = list(tables) + list(mvs)
        tbl_lookup: dict[str, dict] = {}
        for tbl in all_objects:
            if isinstance(tbl, dict):
                ident = tbl.get("identifier", "") or tbl.get("name", "")
                tbl_lookup[ident] = tbl

        col_enriched = 0
        col_skipped = 0
        col_enriched_items: list[dict] = []

        for patch in col_patches:
            tbl_id = patch["table"]
            col_name = patch["column"]
            sections = patch.get("structured_sections", {})
            etype = patch.get("column_entity_type", "")

            tbl = tbl_lookup.get(tbl_id)
            if not tbl:
                logger.warning("Description enrichment: table %s not found — skipping", tbl_id)
                col_skipped += 1
                continue

            cols = tbl.get("column_configs", tbl.get("columns", []))
            cc = None
            for c in cols:
                if isinstance(c, dict) and (c.get("column_name", c.get("name", "")) == col_name):
                    cc = c
                    break

            if cc is None:
                logger.warning(
                    "Description enrichment: column %s.%s not found — skipping", tbl_id, col_name,
                )
                col_skipped += 1
                continue

            if not etype:
                data_type = cc.get("data_type", "")
                etype = entity_type_for_column(col_name, data_type)

            try:
                new_desc = update_sections(
                    cc.get("description"),
                    sections,
                    lever=0,
                    entity_type=etype,
                )
                cc["description"] = new_desc
                col_enriched += 1
                col_enriched_items.append(patch)
            except Exception:
                logger.warning(
                    "Description enrichment: failed to apply sections for %s.%s",
                    tbl_id, col_name, exc_info=True,
                )
                col_skipped += 1

        result["total_enriched"] = col_enriched
        result["total_skipped"] = col_skipped

        # ── Table description enrichment ─────────────────────────────
        tbl_patches = _enrich_table_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["tables_eligible"] = len(tbl_patches)

        tbl_enriched = 0
        tbl_skipped = 0
        tbl_enriched_items: list[dict] = []

        for patch in tbl_patches:
            tbl_id = patch["table"]
            sections = patch.get("structured_sections", {})
            entity_type = patch.get("table_entity_type", "table")

            tbl = tbl_lookup.get(tbl_id)
            if not tbl:
                logger.warning("Table description enrichment: table %s not found — skipping", tbl_id)
                tbl_skipped += 1
                continue

            try:
                new_desc = update_sections(
                    tbl.get("description"),
                    sections,
                    lever=0,
                    entity_type=entity_type,
                )
                tbl["description"] = new_desc
                tbl_enriched += 1
                tbl_enriched_items.append(patch)
            except Exception:
                logger.warning(
                    "Table description enrichment: failed to apply sections for %s",
                    tbl_id, exc_info=True,
                )
                tbl_skipped += 1

        result["tables_enriched"] = tbl_enriched
        result["tables_skipped"] = tbl_skipped

        # ── PATCH the Genie Space if anything changed ────────────────
        anything_enriched = col_enriched > 0 or tbl_enriched > 0
        if anything_enriched:
            parsed = config.get("_parsed_space", config)
            patch_space_config(w, space_id, parsed)

            patch_idx = 0
            for patch in col_enriched_items:
                write_patch(
                    spark, run_id, 0, 0, patch_idx,
                    {
                        "patch_type": "proactive_description_enrichment",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": f"{patch['table']}.{patch['column']}",
                        "patch": patch,
                        "command": None,
                        "rollback": None,
                        "proposal_id": "description_enrichment",
                    },
                    catalog, schema,
                )
                patch_idx += 1
            for patch in tbl_enriched_items:
                write_patch(
                    spark, run_id, 0, 0, patch_idx,
                    {
                        "patch_type": "proactive_table_description_enrichment",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": patch["table"],
                        "patch": patch,
                        "command": None,
                        "rollback": None,
                        "proposal_id": "table_description_enrichment",
                    },
                    catalog, schema,
                )
                patch_idx += 1

        # ── Logging ──────────────────────────────────────────────────
        _de_lines = [_section("DESCRIPTION ENRICHMENT", "-")]
        _de_lines.append(_kv("Eligible columns", len(col_patches)))
        _de_lines.append(_kv("Columns enriched", col_enriched))
        _de_lines.append(_kv("Columns skipped", col_skipped))
        if col_enriched_items:
            _de_lines.append("|")
            for ei, ep in enumerate(col_enriched_items, 1):
                _tbl_short = ep["table"].rsplit(".", 1)[-1]
                _col = ep["column"]
                _sects = ep.get("structured_sections", {})
                _defn = _sects.get("definition", "")[:80]
                _de_lines.append(f"|  [{ei}] {_tbl_short}.{_col}")
                if _defn:
                    _de_lines.append(f"|      definition: {_defn}")
        _de_lines.append(_kv("Eligible tables", len(tbl_patches)))
        _de_lines.append(_kv("Tables enriched", tbl_enriched))
        _de_lines.append(_kv("Tables skipped", tbl_skipped))
        if tbl_enriched_items:
            _de_lines.append("|")
            for ei, ep in enumerate(tbl_enriched_items, 1):
                _tbl_short = ep["table"].rsplit(".", 1)[-1]
                _sects = ep.get("structured_sections", {})
                _purpose = _sects.get("purpose", "")[:80]
                _de_lines.append(f"|  [{ei}] {_tbl_short}")
                if _purpose:
                    _de_lines.append(f"|      purpose: {_purpose}")
        _de_lines.append(_kv("Genie API PATCH sent", "YES" if anything_enriched else "NO"))
        _de_lines.append(_bar("-"))
        print("\n".join(_de_lines))

        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "COMPLETE",
            task_key="description_enrichment",
            detail=result, catalog=catalog, schema=schema,
        )

        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("DESCRIPTION_ENRICHMENT FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "FAILED",
            task_key="description_enrichment",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.85: PROACTIVE JOIN DISCOVERY ─────────────────────────────


def _run_proactive_join_discovery(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.85: Discover execution-proven joins from baseline eval.

    Parses JOIN clauses from successful baseline eval queries (arbiter =
    ``both_correct`` or ``genie_correct``), corroborates with UC column
    type metadata, and codifies them as Genie Space join specifications.

    Only proposes joins that have Tier 1 (execution-proven) evidence.
    """
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _build_join_specs_from_proven,
        _convert_fk_to_candidates,
        _corroborate_with_uc_metadata,
        _extract_proven_joins,
        _short_name,
    )

    write_stage(
        spark, run_id, "JOIN_DISCOVERY", "STARTED",
        task_key="join_discovery", catalog=catalog, schema=schema,
    )

    result: dict = {
        "existing_specs": 0,
        "fk_candidates": 0,
        "execution_candidates": 0,
        "candidates_found": 0,
        "already_defined": 0,
        "type_incompatible": 0,
        "total_applied": 0,
        "total_skipped": 0,
    }

    try:
        # 1. Load baseline eval rows
        baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
        if not baseline_iter:
            print(
                f"\n-- JOIN DISCOVERY " + "-" * 34 + "\n"
                f"  No baseline eval rows found — skipping.\n"
                + "-" * 52
            )
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        rows_json = baseline_iter.get("rows_json")
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                rows_json = []
        if not isinstance(rows_json, list):
            rows_json = []

        if not rows_json:
            print(
                f"\n-- JOIN DISCOVERY " + "-" * 34 + "\n"
                f"  Baseline eval has 0 rows — skipping.\n"
                + "-" * 52
            )
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        # 2. Gather existing join specs
        _inst = metadata_snapshot.get("instructions", {})
        if not isinstance(_inst, dict):
            _inst = {}
        existing_specs = _inst.get("join_specs", [])
        if not isinstance(existing_specs, list):
            existing_specs = []
        result["existing_specs"] = len(existing_specs)

        existing_pairs: set[tuple[str, str]] = set()
        for spec in existing_specs:
            if not isinstance(spec, dict):
                continue
            left_obj = spec.get("left", {})
            right_obj = spec.get("right", {})
            lt = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
            rt = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
            if lt and rt:
                _a, _b = sorted((lt, rt))
                existing_pairs.add((_a, _b))

        # 3a. Tier 0: FK constraint candidates (authoritative)
        fk_rows = config.get("_uc_foreign_keys") or []
        fk_candidates = _convert_fk_to_candidates(fk_rows) if fk_rows else []
        result["fk_candidates"] = len(fk_candidates)

        # 3b. Tier 1: Execution-proven joins from baseline eval
        exec_candidates = _extract_proven_joins(rows_json, metadata_snapshot)
        result["execution_candidates"] = len(exec_candidates)

        # 3c. Merge: FK candidates take precedence for shared table pairs.
        #     For pairs that appear in both, keep the FK candidate's ON
        #     condition (authoritative) but inherit frequency/agreed from
        #     the execution-proven candidate.
        fk_pairs: dict[tuple[str, str], dict] = {}
        for fc in fk_candidates:
            key = tuple(sorted((fc["left_table"], fc["right_table"])))
            fk_pairs[key] = fc

        merged: list[dict] = list(fk_candidates)
        for ec in exec_candidates:
            key = tuple(sorted((ec["left_table"], ec["right_table"])))
            if key in fk_pairs:
                fk_pairs[key]["frequency"] = ec.get("frequency", 0)
                fk_pairs[key]["agreed"] = ec.get("agreed", False)
                fk_pairs[key]["source_questions"] = ec.get("source_questions", [])
            else:
                merged.append(ec)

        candidates = merged
        result["candidates_found"] = len(candidates)

        # 4. Filter out already-defined pairs
        new_candidates = []
        for cand in candidates:
            pair_key = tuple(sorted((cand["left_table"], cand["right_table"])))
            if pair_key in existing_pairs:
                result["already_defined"] += 1
            else:
                new_candidates.append(cand)

        # 5. Corroborate with UC metadata (type check).
        #    FK-sourced candidates bypass this check — the database's own
        #    constraints are authoritative about type compatibility.
        fk_cands = [c for c in new_candidates if c.get("fk_constraint")]
        exec_cands = [c for c in new_candidates if not c.get("fk_constraint")]
        before_uc = len(exec_cands)
        exec_cands = _corroborate_with_uc_metadata(exec_cands, metadata_snapshot)
        result["type_incompatible"] = before_uc - len(exec_cands)
        new_candidates = fk_cands + exec_cands

        # 6. Build join specs
        new_specs = _build_join_specs_from_proven(new_candidates, metadata_snapshot)

        # 7. Gate: nothing to apply
        if not new_specs:
            _jd_lines = [_section("JOIN DISCOVERY", "-")]
            _jd_lines.append(_kv("Existing join specs", result['existing_specs']))
            _jd_lines.append(_kv("FK constraint candidates", result['fk_candidates']))
            _jd_lines.append(_kv("Execution-proven candidates", result['execution_candidates']))
            _jd_lines.append(_kv("Merged candidates", result['candidates_found']))
            _jd_lines.append(_kv("Already defined", result['already_defined']))
            _jd_lines.append(_kv("Type-incompatible", result['type_incompatible']))
            _jd_lines.append(_kv("New joins to apply", 0))
            _jd_lines.append(_bar("-"))
            print("\n".join(_jd_lines))
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        # 8. Apply join specs to config
        parsed = config.get("_parsed_space", config)
        inst_block = parsed.setdefault("instructions", {})
        spec_list = inst_block.setdefault("join_specs", [])

        applied_lines: list[str] = []
        for spec in new_specs:
            meta = spec.pop("_proactive_metadata", {})
            spec_list.append(spec)

            left_short = _short_name(spec["left"]["identifier"])
            right_short = _short_name(spec["right"]["identifier"])
            freq = meta.get("frequency", 0)
            agreed_tag = "agreed" if meta.get("agreed") else "single_source"
            applied_lines.append(
                f"    {left_short} <-> {right_short}"
                f" ON {spec['sql'][0][:60] if spec.get('sql') else '?'}"
                f" (freq={freq}, {agreed_tag})"
            )
            result["total_applied"] += 1

        # 9. PATCH Genie Space
        patch_space_config(w, space_id, parsed)

        # 10. Write patch provenance
        for idx, spec in enumerate(new_specs):
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": "proactive_join_discovery",
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": (
                        f"{spec['left']['identifier']}"
                        f" <-> {spec['right']['identifier']}"
                    ),
                    "patch": spec,
                    "command": None,
                },
                catalog=catalog, schema=schema,
            )

        # 11. Summary
        _jd_lines = [_section("JOIN DISCOVERY", "-")]
        _jd_lines.append(_kv("Existing join specs", result['existing_specs']))
        _jd_lines.append(_kv("FK constraint candidates", result['fk_candidates']))
        _jd_lines.append(_kv("Execution-proven candidates", result['execution_candidates']))
        _jd_lines.append(_kv("Merged candidates", result['candidates_found']))
        _jd_lines.append(_kv("Already defined", result['already_defined']))
        _jd_lines.append(_kv("Type-incompatible", result['type_incompatible']))
        _jd_lines.append(_kv("New joins applied", result['total_applied']))
        if applied_lines:
            _jd_lines.append("|")
            _jd_lines.extend(f"|  {al.strip()}" for al in applied_lines)
        _jd_lines.append(_kv("Genie API PATCH sent", "YES"))
        _jd_lines.append(_bar("-"))
        print("\n".join(_jd_lines))

        write_stage(
            spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
            task_key="join_discovery", detail=result,
            catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("JOIN_DISCOVERY FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "JOIN_DISCOVERY", "FAILED",
            task_key="join_discovery",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.9: PROACTIVE SPACE METADATA ENRICHMENT ──────────────────


def _run_space_metadata_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.9: Generate Space description and sample questions if empty.

    Runs after join discovery and before the lever loop.  Only fires when
    the top-level ``description`` or ``config.sample_questions`` are absent.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config,
        update_space_description,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _generate_sample_questions,
        _generate_space_description,
    )

    from genie_space_optimizer.optimization.optimizer import _MIN_DESCRIPTION_LENGTH
    _desc_text = (config.get("description") or "").strip()
    needs_description = len(_desc_text) < _MIN_DESCRIPTION_LENGTH
    parsed = config.get("_parsed_space", config)
    existing_sqs = (parsed.get("config") or {}).get("sample_questions")
    needs_questions = not existing_sqs

    result: dict = {
        "description_generated": False,
        "questions_generated": False,
        "questions_count": 0,
    }

    if not needs_description and not needs_questions:
        _sm_lines = [_section("SPACE METADATA ENRICHMENT", "-")]
        _sm_lines.append(_kv("Description", "already present"))
        _sm_lines.append(_kv("Sample questions", f"already present ({len(existing_sqs or [])})"))
        _sm_lines.append(_kv("Status", "No enrichment needed"))
        _sm_lines.append(_bar("-"))
        print("\n".join(_sm_lines))
        return result

    write_stage(
        spark, run_id, "SPACE_METADATA_ENRICHMENT", "STARTED",
        task_key="space_metadata_enrichment", catalog=catalog, schema=schema,
    )

    try:
        desc_text = ""
        if needs_description:
            desc_text = _generate_space_description(metadata_snapshot, w)
            if desc_text:
                try:
                    update_space_description(w, space_id, desc_text)
                    result["description_generated"] = True
                    write_patch(
                        spark, run_id, 0, 0, 0,
                        {
                            "patch_type": "proactive_space_description",
                            "scope": "genie_space",
                            "risk_level": "low",
                            "target_object": "space.description",
                            "patch": {"description": desc_text[:200] + "..."},
                            "command": None,
                            "rollback": None,
                            "proposal_id": "space_metadata_enrichment",
                        },
                        catalog, schema,
                    )
                except Exception:
                    logger.warning(
                        "Space metadata enrichment: description PATCH failed",
                        exc_info=True,
                    )

        effective_desc = desc_text or (config.get("description") or "")

        new_sqs: list[dict] = []
        if needs_questions:
            new_sqs = _generate_sample_questions(metadata_snapshot, effective_desc, w)
            if new_sqs:
                cfg_block = parsed.setdefault("config", {})
                cfg_block["sample_questions"] = new_sqs
                try:
                    patch_space_config(w, space_id, parsed)
                    result["questions_generated"] = True
                    result["questions_count"] = len(new_sqs)
                    for idx, sq in enumerate(new_sqs):
                        q_text = sq.get("question", [""])[0] if sq.get("question") else ""
                        write_patch(
                            spark, run_id, 0, 0, idx + 1,
                            {
                                "patch_type": "proactive_sample_question",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": f"config.sample_questions[{idx}]",
                                "patch": {"question": q_text[:100]},
                                "command": None,
                                "rollback": None,
                                "proposal_id": "space_metadata_enrichment",
                            },
                            catalog, schema,
                        )
                except Exception:
                    logger.warning(
                        "Space metadata enrichment: sample_questions PATCH failed",
                        exc_info=True,
                    )

        _desc_status = (
            f"generated ({len(desc_text)} chars)"
            if result['description_generated']
            else ("already present" if not needs_description else "FAILED")
        )
        _sq_status = (
            f"generated ({len(new_sqs)} questions)"
            if result['questions_generated']
            else ("already present" if not needs_questions else "FAILED")
        )
        _sm_lines = [_section("SPACE METADATA ENRICHMENT", "-")]
        _sm_lines.append(_kv("Description", _desc_status))
        if result['description_generated'] and desc_text:
            _sm_lines.append(f"|      {desc_text[:120]}...")
        _sm_lines.append(_kv("Sample questions", _sq_status))
        if result['questions_generated'] and new_sqs:
            for sq in new_sqs:
                q_text = sq.get("question", [""])[0] if sq.get("question") else ""
                if q_text:
                    _sm_lines.append(f"|      - {q_text[:100]}")
        _sm_lines.append(_bar("-"))
        print("\n".join(_sm_lines))

        write_stage(
            spark, run_id, "SPACE_METADATA_ENRICHMENT", "COMPLETE",
            task_key="space_metadata_enrichment",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("SPACE_METADATA_ENRICHMENT FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "SPACE_METADATA_ENRICHMENT", "FAILED",
            task_key="space_metadata_enrichment",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Proactive Benchmark Example SQL Application ─────────────────────


def _apply_proactive_example_sqls(
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    space_id: str,
    mined_proposals: list[dict],
    metadata_snapshot: dict,
    config: dict,
    catalog: str,
    schema: str,
) -> None:
    """Apply mined benchmark example SQLs proactively via the Genie API."""
    from genie_space_optimizer.optimization.applier import (
        proposals_to_patches,
        apply_patch_set,
    )

    patches = proposals_to_patches(mined_proposals)
    apply_log = apply_patch_set(w, space_id, patches, metadata_snapshot, apply_mode="api")

    applied = apply_log.get("applied", [])
    _lines = [_section("PROACTIVE BENCHMARK EXAMPLE SQLs", "-")]
    _lines.append(_kv("Mined proposals", len(mined_proposals)))
    _lines.append(_kv("Applied", len(applied)))
    if apply_log.get("patch_error"):
        _lines.append(_kv("Error", str(apply_log["patch_error"])[:200]))
    for idx, entry in enumerate(applied, 1):
        _ap = entry.get("patch", {})
        q = _ap.get("example_question", _ap.get("question", ""))
        if isinstance(q, list):
            q = q[0] if q else ""
        _lines.append(f"|  [{idx}] {q[:80]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    for idx, entry in enumerate(applied):
        write_patch(
            spark, run_id, 0, 0, idx,
            {
                "patch_type": "proactive_example_sql",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": f"example_question_sqls[{idx}]",
                "patch": {"question": str(entry.get("patch", {}).get("example_question", ""))[:100]},
                "command": None,
                "rollback": None,
                "proposal_id": "proactive_benchmark_mining",
            },
            catalog, schema,
        )


# ── Stage 2.95: PROACTIVE INSTRUCTION SEEDING ────────────────────────


def _run_proactive_instruction_seeding(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.95: Seed conservative routing instructions if the space has none.

    Only fires when existing instructions are empty or below 50 chars.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.optimization.optimizer import (
        _generate_proactive_instructions,
    )
    from genie_space_optimizer.optimization.applier import (
        _get_general_instructions,
        _set_general_instructions,
    )

    _INSTRUCTION_SEED_THRESHOLD = 50
    parsed = config.get("_parsed_space", config)
    current_instructions = _get_general_instructions(parsed)
    needs_seeding = len(current_instructions.strip()) < _INSTRUCTION_SEED_THRESHOLD

    result: dict = {"instructions_seeded": False, "instruction_chars": 0}

    if not needs_seeding:
        _lines = [_section("PROACTIVE INSTRUCTION SEEDING", "-")]
        _lines.append(_kv("Instructions", f"already present ({len(current_instructions)} chars)"))
        _lines.append(_kv("Status", "No seeding needed"))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        return result

    write_stage(
        spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "STARTED",
        task_key="instruction_seeding", catalog=catalog, schema=schema,
    )

    try:
        instruction_text = _generate_proactive_instructions(metadata_snapshot, w)
        if instruction_text:
            _set_general_instructions(parsed, instruction_text)
            try:
                patch_space_config(w, space_id, parsed)
                result["instructions_seeded"] = True
                result["instruction_chars"] = len(instruction_text)
                write_patch(
                    spark, run_id, 0, 0, 0,
                    {
                        "patch_type": "proactive_instruction_seeding",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": "instructions.text_instructions",
                        "patch": {"instructions": instruction_text[:200] + "..."},
                        "command": None,
                        "rollback": None,
                        "proposal_id": "proactive_instruction_seeding",
                    },
                    catalog, schema,
                )
            except Exception:
                logger.warning(
                    "Proactive instruction seeding: PATCH failed",
                    exc_info=True,
                )

        _status = (
            f"generated ({len(instruction_text)} chars)"
            if result["instructions_seeded"]
            else "FAILED"
        )
        _lines = [_section("PROACTIVE INSTRUCTION SEEDING", "-")]
        _lines.append(_kv("Instructions", _status))
        if result["instructions_seeded"] and instruction_text:
            _lines.append(f"|      {instruction_text[:200]}...")
        _lines.append(_bar("-"))
        print("\n".join(_lines))

        write_stage(
            spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "COMPLETE",
            task_key="instruction_seeding",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("PROACTIVE_INSTRUCTION_SEEDING FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "FAILED",
            task_key="instruction_seeding",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.5b: PREPARE LEVER LOOP ──────────────────────────────────


def _prepare_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
) -> dict:
    """Load Genie Space config, enrich UC metadata, run Stage 2.5 prompt matching.

    Consolidates all config preparation needed before the lever loop into a
    single function used by both the DAG notebook and the convenience wrapper.

    Steps:
      1. Load config from Delta snapshot (API fetch fallback).
      2. Fetch UC column metadata via REST API (for prompt matching + type
         enrichment in the lever loop).
      3. Print detailed inventory diagnostic (tables, cols, FA/VD stats).
      4. Run Stage 2.5 prompt matching auto-config (if enabled).
      5. If changes applied: entity-matching-aware propagation wait + diagnostic.
      6. Post-wait config refresh from API.
      7. If no changes or prompt matching disabled: skip wait entirely.

    Non-fatal: exceptions in UC fetch or prompt matching are logged and
    swallowed so the lever loop can still proceed.

    Returns the fully prepared config dict with ``_uc_columns`` populated.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.common.uc_metadata import (
        extract_genie_space_table_refs,
        get_columns_for_tables_rest,
    )

    # ── 1. Load config from Delta snapshot (API fetch fallback) ──────
    run_data = load_run(spark, run_id, catalog, schema) or {}
    snapshot = run_data.get("config_snapshot", {})
    config: dict
    if isinstance(snapshot, dict) and snapshot:
        config = snapshot
        logger.info("Lever loop: using config snapshot from run row for %s", run_id)
    else:
        logger.warning(
            "No config snapshot found in run row for %s — fetching from API.",
            run_id,
        )
        config = fetch_space_config(w, space_id)
        logger.info("Lever loop: fetched config for space %s", space_id)

    logger.info(
        "Lever loop config loaded: keys=%s",
        sorted(list(config.keys()))[:20] if isinstance(config, dict) else [],
    )

    # ── 2. Fetch UC column metadata via REST API ─────────────────────
    table_refs: list = []
    try:
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns
        logger.info(
            "Lever loop: fetched %d UC columns across %d tables",
            len(uc_columns), len(table_refs),
        )
    except Exception as exc:
        logger.warning(
            "UC column metadata fetch failed for %s (non-fatal): %s",
            run_id, exc,
        )
        uc_columns = config.get("_uc_columns", [])

    # ── 3. Print detailed inventory diagnostic ───────────────────────
    _parsed = config.get("_parsed_space", {})
    _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
    _tables = _ds.get("tables", []) + _ds.get("metric_views", [])
    _total_cols = sum(len(t.get("column_configs", [])) for t in _tables)
    _visible_cols = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if not c.get("hidden")
    )
    _hidden_cols = _total_cols - _visible_cols
    _string_cols = sum(
        1 for c in uc_columns
        if str(c.get("data_type", "")).upper() == "STRING"
    )
    _fa_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_format_assistance")
    )
    _vd_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )
    print(
        f"\n{'=' * 62}\n"
        f"  PREPARE LEVER LOOP — GENIE SPACE INVENTORY\n"
        f"{'=' * 62}\n"
        f"\n-- GENIE SPACE INVENTORY " + "-" * 27 + "\n"
        f"  Tables: {len(_tables)}"
        f" ({', '.join(t.get('name', t.get('identifier', '?')) for t in _tables[:10])})\n"
        f"  Total columns: {_total_cols}"
        f" (visible: {_visible_cols}, hidden: {_hidden_cols})\n"
        f"  UC column metadata: {len(uc_columns)} columns"
        f" fetched across {len(table_refs)} tables\n"
        f"  STRING columns eligible for entity matching: {_string_cols}\n"
        f"  Columns already with format assistance: {_fa_existing}\n"
        f"  Columns already with value dictionary: {_vd_existing} of 120 max slots\n"
        + "-" * 52
    )

    # ── 4–7. Stage 2.5 prompt matching + propagation wait ────────────
    if ENABLE_PROMPT_MATCHING_AUTO_APPLY:
        try:
            pm_result = _run_prompt_matching_setup(
                w, spark, run_id, space_id, config, catalog, schema,
            )
            logger.info(
                "Prompt matching complete: FA=%d, EM=%d, total=%d",
                pm_result.get("format_assistance_count", 0),
                pm_result.get("entity_matching_count", 0),
                pm_result.get("total_changes", 0),
            )

            if pm_result.get("total_changes", 0) > 0:
                has_entity_matching = pm_result.get("entity_matching_count", 0) > 0
                wait_time = (
                    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_entity_matching
                    else PROPAGATION_WAIT_SECONDS
                )
                print(
                    f"\n-- PROPAGATION WAIT " + "-" * 32 + "\n"
                    f"  Changes applied: {pm_result.get('total_changes', 0)}\n"
                    f"  Entity matching changes:"
                    f" {pm_result.get('entity_matching_count', 0)}\n"
                    f"  Wait time: {wait_time}s"
                    + (
                        " (extended for value dictionary rebuild)"
                        if has_entity_matching else ""
                    )
                    + "\n" + "-" * 52
                )
                time.sleep(wait_time)
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                logger.info("Config refreshed after prompt matching propagation wait")
            else:
                logger.info("Prompt matching: no changes applied, skipping wait")
        except Exception as exc:
            logger.warning(
                "Stage 2.5 prompt matching failed (non-fatal, continuing): %s: %s",
                type(exc).__name__, exc,
            )

    return config


def _run_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict],
    exp_name: str,
    catalog: str,
    schema: str,
    baseline_model_id: str = "",
    optimization_run_id: str = "",
) -> dict:
    """Stage 2.5: Config preparation + proactive enrichment + LoggedModel snapshot.

    Combines ``_prepare_lever_loop()`` (config loading, UC metadata, prompt
    matching) with the Phase 1 proactive enrichment steps previously embedded
    in ``_run_lever_loop()`` (descriptions, joins, metadata, instructions,
    example SQLs).

    Side effects:
      - Patches the Genie Space with enrichments via the API
      - Creates an MLflow LoggedModel snapshot of the enriched state
      - Writes Delta stage records (ENRICHMENT_STARTED, ENRICHMENT_COMPLETE)

    Returns dict with:
      - enrichment_model_id: str
      - enrichment_skipped: bool
      - config: dict (enriched config)
      - summary: dict (counts of each enrichment type)
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    write_stage(
        spark, run_id, "ENRICHMENT_STARTED", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    try:
        # ── 1. Load config (delegates to _prepare_lever_loop) ─────────────
        config = _prepare_lever_loop(w, spark, run_id, space_id, catalog, schema)

        uc_columns = config.get("_uc_columns", [])
        metadata_snapshot = config.get("_parsed_space", config)
        data_profile = metadata_snapshot.get("_data_profile", {})
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

        # ── 2. Proactive Enrichment sub-steps ─────────────────────────────
        import mlflow as _mlflow_enr

        uc_schema = f"{catalog}.{schema}"
        _enr_run_name = f"enrichment_snapshot_{(optimization_run_id or run_id)[:12]}"
        with _mlflow_enr.start_run(run_name=_enr_run_name):
            _mlflow_enr.set_tags({
                "genie.space_id": space_id,
                "genie.domain": domain,
                "genie.optimization_run_id": optimization_run_id or run_id,
                "genie.run_type": "enrichment_snapshot",
            })

            _pe_lines = [_section("ENRICHMENT — PROACTIVE ENRICHMENT", "-")]
            _pe_lines.append(_kv("Space ID", space_id))
            _pe_lines.append(_kv("UC columns", len(uc_columns)))
            _pe_lines.append(_bar("-"))
            print("\n".join(_pe_lines))

            enrichment_result = _run_description_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tables_enriched", 0) > 0:
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            join_result = _run_proactive_join_discovery(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if join_result.get("total_applied", 0) > 0:
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            meta_result = _run_space_metadata_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if meta_result.get("description_generated") or meta_result.get("questions_generated"):
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            instruction_result = _run_proactive_instruction_seeding(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if instruction_result.get("instructions_seeded"):
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            mined_example_proposals = _mine_benchmark_example_sqls(
                benchmarks, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=schema,
                w=w, warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            )
            if mined_example_proposals:
                _apply_proactive_example_sqls(
                    w, spark, run_id, space_id, mined_example_proposals,
                    metadata_snapshot, config, catalog, schema,
                )
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── Summary ───────────────────────────────────────────────────
            total_enrichments = (
                enrichment_result.get("total_enriched", 0)
                + join_result.get("total_applied", 0)
                + (1 if meta_result.get("description_generated") else 0)
                + (1 if meta_result.get("questions_generated") else 0)
                + (1 if instruction_result.get("instructions_seeded") else 0)
                + len(mined_example_proposals)
            )

            _enr_summary = [_section("PROACTIVE ENRICHMENT — SUMMARY", "-")]
            _enr_summary.append(_kv("Descriptions enriched", enrichment_result.get("total_enriched", 0)))
            _enr_summary.append(_kv("Joins discovered", join_result.get("total_applied", 0)))
            _enr_summary.append(_kv("Space metadata", "description=%s, questions=%s" % (
                "generated" if meta_result.get("description_generated") else "unchanged",
                "generated" if meta_result.get("questions_generated") else "unchanged",
            )))
            _enr_summary.append(_kv("Instructions seeded", "yes" if instruction_result.get("instructions_seeded") else "no"))
            _enr_summary.append(_kv("Example SQLs mined", len(mined_example_proposals)))
            _enr_summary.append(_kv("Total enrichments", total_enrichments))
            _enr_summary.append(_bar("-"))
            print("\n".join(_enr_summary))

            enrichment_skipped = total_enrichments == 0

            # ── Log enrichment metrics ────────────────────────────────────
            _mlflow_enr.log_metrics({
                "enrichment.columns_enriched": enrichment_result.get("total_enriched", 0),
                "enrichment.tables_enriched": enrichment_result.get("tables_enriched", 0),
                "enrichment.joins_discovered": join_result.get("total_applied", 0),
                "enrichment.examples_mined": len(mined_example_proposals),
                "enrichment.total": total_enrichments,
            })

            # ── 3. LoggedModel snapshot of enriched state ─────────────────
            enrichment_model_id = create_genie_model_version(
                w,
                space_id,
                config,
                iteration=-1,
                domain=domain,
                experiment_name=exp_name,
                uc_schema=uc_schema,
                uc_columns=uc_columns,
                parent_model_id=baseline_model_id or None,
                optimization_run_id=optimization_run_id or run_id,
            )

        write_stage(
            spark, run_id, "ENRICHMENT_COMPLETE", "COMPLETE",
            task_key="enrichment", catalog=catalog, schema=schema,
            detail={
                "enrichment_model_id": enrichment_model_id,
                "total_enrichments": total_enrichments,
                "enrichment_skipped": enrichment_skipped,
                "descriptions_enriched": enrichment_result.get("total_enriched", 0),
                "joins_discovered": join_result.get("total_applied", 0),
                "instructions_seeded": bool(instruction_result.get("instructions_seeded")),
                "examples_mined": len(mined_example_proposals),
            },
        )

        return {
            "enrichment_model_id": enrichment_model_id,
            "enrichment_skipped": enrichment_skipped,
            "config": config,
            "summary": {
                "descriptions_enriched": enrichment_result.get("total_enriched", 0),
                "joins_discovered": join_result.get("total_applied", 0),
                "description_generated": bool(meta_result.get("description_generated")),
                "questions_generated": bool(meta_result.get("questions_generated")),
                "instructions_seeded": bool(instruction_result.get("instructions_seeded")),
                "examples_mined": len(mined_example_proposals),
                "total_enrichments": total_enrichments,
            },
        }

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("ENRICHMENT FAILED for run %s", run_id)
        try:
            write_stage(
                spark, run_id, "ENRICHMENT_STARTED", "FAILED",
                task_key="enrichment",
                error_message=err_msg[:500],
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.warning("Failed to write ENRICHMENT FAILED stage", exc_info=True)
        raise


# ── Stage 3: LEVER LOOP ─────────────────────────────────────────────

# ── Adaptive loop helpers ───────────────────────────────────────────


def _build_reflection_entry(
    iteration: int,
    ag_id: str,
    accepted: bool,
    levers: list[int],
    target_objects: list[str],
    prev_scores: dict[str, float],
    new_scores: dict[str, float],
    rollback_reason: str | None,
    patches: list[dict],
    *,
    affected_question_ids: list[str] | None = None,
    prev_failure_qids: set[str] | None = None,
    new_failure_qids: set[str] | None = None,
    reflection_text: str = "",
    refinement_mode: str = "",
    escalation_handled: bool = False,
) -> dict:
    """Build a structured reflection dict for the adaptive loop memory.

    *reflection_text* is a 2-3 sentence verbal explanation of why the
    iteration succeeded or failed (Reflexion-style semantic gradient).

    *refinement_mode* is ``"in_plan"`` when the lever direction was correct
    but caused collateral regressions, or ``"out_of_plan"`` when the
    approach fundamentally did not work (AdaPlanner-style classification).
    """
    score_deltas = {
        k: new_scores.get(k, 0.0) - prev_scores.get(k, 0.0)
        for k in set(prev_scores) | set(new_scores)
    }
    prev_acc = sum(prev_scores.values()) / max(len(prev_scores), 1)
    new_acc = sum(new_scores.values()) / max(len(new_scores), 1)

    patch_summary_parts: list[str] = []
    do_not_retry: list[str] = []
    for p in patches:
        ptype = p.get("type", p.get("patch_type", "?"))
        target = p.get("target", p.get("target_object", "?"))
        patch_summary_parts.append(f"{ptype} on {target}")
        if not accepted:
            do_not_retry.append(f"{ptype} on {target}")

    action = ", ".join(patch_summary_parts[:8])
    if len(patch_summary_parts) > 8:
        action += f" (+{len(patch_summary_parts) - 8} more)"

    new_failure_parts: list[str] = []
    for k, delta in score_deltas.items():
        if delta < -1.0:
            new_failure_parts.append(f"{k} {delta:+.1f}%")
    new_failures = ", ".join(new_failure_parts) if new_failure_parts else None

    _prev = prev_failure_qids or set()
    _new = new_failure_qids or set()

    return {
        "iteration": iteration,
        "ag_id": ag_id,
        "accepted": accepted,
        "action": action,
        "levers": levers,
        "target_objects": target_objects[:15],
        "score_deltas": score_deltas,
        "accuracy_delta": new_acc - prev_acc,
        "new_failures": new_failures,
        "rollback_reason": rollback_reason,
        "do_not_retry": do_not_retry,
        "affected_question_ids": affected_question_ids or [],
        "fixed_questions": sorted(_prev - _new),
        "still_failing": sorted(_prev & _new),
        "new_regressions": sorted(_new - _prev),
        "reflection_text": reflection_text,
        "refinement_mode": refinement_mode,
        "escalation_handled": escalation_handled,
    }


def _diminishing_returns(
    reflection_buffer: list[dict],
    epsilon: float | None = None,
    lookback: int | None = None,
) -> bool:
    """Return True if none of the last *lookback* non-escalation iterations
    achieved a mean accuracy improvement >= *epsilon*.

    Rolled-back iterations count as zero improvement, which correctly
    signals the optimizer is stuck.  Escalation-handled entries are skipped
    so they don't pollute the lookback window.
    """
    if epsilon is None:
        epsilon = DIMINISHING_RETURNS_EPSILON
    if lookback is None:
        lookback = DIMINISHING_RETURNS_LOOKBACK

    non_escalation = [r for r in reflection_buffer if not r.get("escalation_handled")]
    recent = non_escalation[-lookback:]
    if len(recent) < lookback:
        return False

    for r in recent:
        if r.get("accepted") and r.get("accuracy_delta", 0.0) >= epsilon:
            return False
    return True


def _filter_tried_clusters(
    clusters: list[dict],
    tried_root_causes: set[tuple[str, str]],
) -> list[dict]:
    """Remove clusters whose (failure_type, blame_set) was already tried
    and rolled back.  Keeps clusters that were tried and *accepted* (they
    should no longer appear in fresh failure data anyway).

    *tried_root_causes* stores ``(asi_failure_type, asi_blame_set)`` tuples
    recorded at rollback time so the key space matches the cluster fields.
    """
    if not tried_root_causes:
        return clusters
    filtered: list[dict] = []
    for c in clusters:
        ft = c.get("asi_failure_type") or c.get("root_cause", "other")
        blame = c.get("asi_blame_set") or ""
        if (ft, blame) not in tried_root_causes:
            filtered.append(c)
    return filtered


def _extract_arbiter_actions_from_baseline(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Extract genie_correct arbiter actions from baseline iteration rows."""
    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not baseline_iter:
        return []

    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(rows_json, list):
        return []

    actions: list[dict] = []
    for row in rows_json:
        av = str(
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        if av != "genie_correct":
            continue
        genie_sql = (
            row.get("outputs/response")
            or (row.get("outputs") or {}).get("response", "")
        )
        question = (
            row.get("inputs/question")
            or (row.get("inputs") or {}).get("question", "")
        )
        if genie_sql and question:
            actions.append({
                "question": str(question),
                "new_expected_sql": str(genie_sql),
                "verdict": "genie_correct",
            })
    return actions


# ── Cross-Iteration Verdict History ────────────────────────────────────


def _get_arbiter_verdict(row: dict) -> str:
    """Extract the arbiter verdict string from an evaluation row."""
    return str(
        row.get("arbiter/value")
        or row.get("feedback/arbiter/value")
        or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
        or "skipped"
    ).lower()


def _get_question_id(row: dict) -> str:
    """Extract the question ID from an evaluation row."""
    _rq = row.get("request") or {}
    if isinstance(_rq, str):
        try:
            _rq = json.loads(_rq)
        except (json.JSONDecodeError, TypeError):
            _rq = {}
    _rqk = _rq.get("kwargs", {}) if isinstance(_rq, dict) else {}
    return str(
        row.get("inputs/question_id")
        or (row.get("inputs") or {}).get("question_id", "")
        or row.get("question_id")
        or _rqk.get("question_id")
        or (_rq.get("question_id") if isinstance(_rq, dict) else None)
        or "?"
    )


def _get_question_text(row: dict) -> str:
    """Extract the question text from an evaluation row."""
    return str(
        row.get("inputs/question")
        or (row.get("inputs") or {}).get("question", "")
    )


def _get_genie_sql(row: dict) -> str:
    """Extract Genie's generated SQL from an evaluation row."""
    return str(
        row.get("outputs/response")
        or (row.get("outputs") or {}).get("response", "")
    )


def _get_expected_sql(row: dict) -> str:
    """Extract the expected (ground truth) SQL from an evaluation row."""
    return str(
        row.get("inputs/expected_response")
        or (row.get("inputs") or {}).get("expected_response", "")
        or row.get("expected_response/value")
    )


def _get_arbiter_rationale(row: dict) -> str:
    """Extract the arbiter rationale from an evaluation row."""
    return str(
        row.get("arbiter/rationale")
        or row.get("feedback/arbiter/rationale")
        or ""
    )


@dataclass
class VerdictEntry:
    """A single arbiter observation for a question in one evaluation."""
    iteration: int
    verdict: str
    genie_sql: str
    expected_sql: str
    question_text: str
    rationale: str


def _build_verdict_history(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict[str, list[VerdictEntry]]:
    """Build per-question verdict history across all full-scope evaluations.

    Returns ``{question_id: [VerdictEntry, ...]}``, ordered by iteration.
    """
    all_iters = load_all_full_iterations(spark, run_id, catalog, schema)
    history: dict[str, list[VerdictEntry]] = {}

    for iteration_row in all_iters:
        iteration_num = int(iteration_row.get("iteration", 0))
        rows_json = iteration_row.get("rows_json")
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(rows_json, list):
            continue

        for row in rows_json:
            qid = _get_question_id(row)
            if qid == "?":
                continue
            entry = VerdictEntry(
                iteration=iteration_num,
                verdict=_get_arbiter_verdict(row),
                genie_sql=_get_genie_sql(row),
                expected_sql=_get_expected_sql(row),
                question_text=_get_question_text(row),
                rationale=_get_arbiter_rationale(row),
            )
            history.setdefault(qid, []).append(entry)

    return history


def _build_question_persistence_summary(
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    *,
    min_failures: int | None = None,
) -> tuple[str, dict[str, dict]]:
    """Render a per-question failure persistence summary for the strategist.

    Only includes questions that failed in >= *min_failures* iterations.
    For each, shows the question text, consecutive failure count, verdict
    breakdown, patches previously tried for that question, and an
    exhaustion classification.

    Returns ``(text, structured)`` where *text* feeds the strategist prompt
    and *structured* is ``{qid: {question_text, fail_count, max_consecutive,
    classification, patches_tried, fail_iterations}}`` for trace enrichment
    and hard-quarantine decisions.
    """
    from genie_space_optimizer.common.config import PERSISTENCE_MIN_FAILURES

    if min_failures is None:
        min_failures = PERSISTENCE_MIN_FAILURES
    if not verdict_history:
        return "(No cross-iteration verdict data available yet.)", {}

    _PASSING = {"both_correct"}
    _ADDITIVE_PATCH_TYPES = {"add_instruction", "add_example_sql"}

    q_patches: dict[str, list[tuple[int, str]]] = {}
    for entry in reflection_buffer:
        iter_n = entry.get("iteration", 0)
        affected = entry.get("affected_question_ids", [])
        patch_types = set()
        for part in entry.get("action", "").split(", "):
            pt = part.split(" on ")[0].strip() if " on " in part else ""
            if pt:
                patch_types.add(pt)
        for qid in affected:
            for pt in patch_types:
                q_patches.setdefault(qid, []).append((iter_n, pt))

    persistent: list[dict] = []
    for qid, entries in verdict_history.items():
        non_passing = [e for e in entries if e.verdict not in _PASSING]
        if len(non_passing) < min_failures:
            continue

        consecutive = 0
        max_consecutive = 0
        for e in entries:
            if e.verdict not in _PASSING:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        verdict_counts: dict[str, int] = {}
        for e in non_passing:
            verdict_counts[e.verdict] = verdict_counts.get(e.verdict, 0) + 1

        q_text = non_passing[-1].question_text if non_passing else "?"
        fail_iters = sorted({e.iteration for e in non_passing})

        tried = q_patches.get(qid, [])
        additive_counts: dict[str, int] = {}
        for _, pt in tried:
            if pt in _ADDITIVE_PATCH_TYPES:
                additive_counts[pt] = additive_counts.get(pt, 0) + 1

        exhausted = all(
            additive_counts.get(pt, 0) >= 2 for pt in _ADDITIVE_PATCH_TYPES
        )

        if max_consecutive < 2:
            classification = "INTERMITTENT"
        elif exhausted:
            classification = "ADDITIVE_LEVERS_EXHAUSTED"
        else:
            classification = "PERSISTENT"

        persistent.append({
            "qid": qid,
            "question_text": q_text,
            "fail_count": len(non_passing),
            "total_evals": len(entries),
            "max_consecutive": max_consecutive,
            "verdict_counts": verdict_counts,
            "fail_iterations": fail_iters,
            "patches_tried": tried,
            "classification": classification,
        })

    structured: dict[str, dict] = {}
    for p in persistent:
        structured[p["qid"]] = {
            "question_text": p["question_text"],
            "fail_count": p["fail_count"],
            "total_evals": p["total_evals"],
            "max_consecutive": p["max_consecutive"],
            "classification": p["classification"],
            "patches_tried": p["patches_tried"],
            "fail_iterations": p["fail_iterations"],
            "verdict_counts": p["verdict_counts"],
        }

    if not persistent:
        return "(No persistent failures detected across iterations.)", structured

    persistent.sort(key=lambda p: (-p["max_consecutive"], -p["fail_count"]))

    lines: list[str] = [
        "Questions failing across multiple iterations despite fix attempts:",
        "",
    ]
    for p in persistent:
        lines.append(f"### {p['qid']}: \"{p['question_text'][:120]}\"")
        lines.append(
            f"  Failed {p['fail_count']}/{p['total_evals']} evals "
            f"({p['max_consecutive']} consecutive)"
        )
        vstr = ", ".join(f"{v}={c}" for v, c in sorted(p["verdict_counts"].items()))
        lines.append(f"  Verdicts: {vstr}")
        lines.append(f"  Failed in iterations: {p['fail_iterations']}")
        if p["patches_tried"]:
            patch_lines = []
            for it, pt in p["patches_tried"]:
                patch_lines.append(f"iter{it}: {pt}")
            lines.append(f"  Patches tried: {'; '.join(patch_lines)}")
        lines.append(f"  ASSESSMENT: {p['classification']}")
        lines.append("")

    return "\n".join(lines), structured


def _validate_tvf_removal_coverage(
    tvf_identifier: str,
    benchmarks: list[dict],
    schema_overlap: dict,
    metadata_snapshot: dict,
) -> dict:
    """Hard gate: verify alternative assets exist before allowing TVF removal.

    Only TVFs can be removed through escalation — tables and MVs are rejected.
    If the TVF's output columns are not sufficiently covered by other assets
    in the Genie Space, the removal is rejected.

    Returns ``{"valid": bool, "reason": str, ...}``.
    """
    tvf_lower = tvf_identifier.lower()

    is_tvf = any(
        isinstance(fn, dict) and fn.get("identifier", "").lower() == tvf_lower
        for fn in (metadata_snapshot.get("instructions") or {}).get("sql_functions", [])
    )

    if not is_tvf:
        ds = metadata_snapshot.get("data_sources", {})
        if not isinstance(ds, dict):
            ds = {}
        is_table = any(
            (t.get("identifier", "").lower() == tvf_lower or t.get("name", "").lower() == tvf_lower)
            for t in ds.get("tables", [])
        )
        is_mv = any(
            (m.get("identifier", "").lower() == tvf_lower or m.get("name", "").lower() == tvf_lower)
            for m in ds.get("metric_views", [])
        )
        if is_table:
            return {
                "valid": False,
                "reason": f"Cannot remove table '{tvf_identifier}' — only TVFs may be removed via escalation",
                "affected_questions": [],
                "coverage_ratio": 0.0,
                "uncovered_columns": [],
            }
        if is_mv:
            return {
                "valid": False,
                "reason": f"Cannot remove metric view '{tvf_identifier}' — only TVFs may be removed via escalation",
                "affected_questions": [],
                "coverage_ratio": 0.0,
                "uncovered_columns": [],
            }
        return {
            "valid": False,
            "reason": f"Asset '{tvf_identifier}' not found as a TVF in the Genie Space",
            "affected_questions": [],
            "coverage_ratio": 0.0,
            "uncovered_columns": [],
        }

    affected_questions = [
        b.get("question_id", b.get("id", ""))
        for b in benchmarks
        if tvf_lower in (b.get("expected_asset") or "").lower()
    ]

    coverage_ratio = schema_overlap.get("coverage_ratio", 0.0)
    uncovered = schema_overlap.get("uncovered_columns", [])
    full_coverage = schema_overlap.get("full_coverage", False)

    if affected_questions and not full_coverage and coverage_ratio < 0.5:
        return {
            "valid": False,
            "reason": (
                f"Insufficient alternative coverage for TVF '{tvf_identifier}': "
                f"{coverage_ratio:.0%} of columns covered, {len(uncovered)} uncovered "
                f"({', '.join(uncovered[:5])}). {len(affected_questions)} benchmark "
                f"question(s) reference this TVF."
            ),
            "affected_questions": affected_questions,
            "coverage_ratio": coverage_ratio,
            "uncovered_columns": uncovered,
        }

    return {
        "valid": True,
        "reason": (
            f"TVF '{tvf_identifier}' coverage OK: {coverage_ratio:.0%} columns covered"
            + (f", {len(affected_questions)} benchmark question(s) affected" if affected_questions else "")
        ),
        "affected_questions": affected_questions,
        "coverage_ratio": coverage_ratio,
        "uncovered_columns": uncovered,
    }


def _score_tvf_removal_confidence(
    tvf_identifier: str,
    benchmarks: list[dict],
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    schema_overlap: dict,
    asi_provenance: list[dict],
    *,
    min_iterations: int | None = None,
) -> str | None:
    """Tiered confidence model for TVF removal.

    Returns ``"high"``, ``"medium"``, ``"low"``, or ``None`` if the
    iteration gate has not been met yet (too early to consider removal).
    """
    from genie_space_optimizer.common.config import (
        TVF_REMOVAL_BLAME_THRESHOLD,
        TVF_REMOVAL_MIN_ITERATIONS,
    )

    if min_iterations is None:
        min_iterations = TVF_REMOVAL_MIN_ITERATIONS

    _PASSING = {"both_correct"}
    tvf_lower = tvf_identifier.lower()

    blamed_qids: set[str] = set()
    for prov in asi_provenance:
        blame = prov.get("blame_set")
        if isinstance(blame, str):
            try:
                blame = json.loads(blame)
            except (json.JSONDecodeError, TypeError):
                blame = [blame]
        if not isinstance(blame, list):
            continue
        for b in blame:
            if tvf_lower in str(b).lower():
                qid = prov.get("question_id", "")
                if qid:
                    blamed_qids.add(qid)

    max_consecutive = 0
    for qid in blamed_qids:
        entries = verdict_history.get(qid, [])
        consec = 0
        for e in entries:
            if e.verdict not in _PASSING:
                consec += 1
                max_consecutive = max(max_consecutive, consec)
            else:
                consec = 0

    if max_consecutive < min_iterations:
        return None

    gt_refs = sum(
        1 for b in benchmarks
        if tvf_lower in (b.get("expected_asset") or "").lower()
        or tvf_lower in (b.get("expected_sql") or "").lower()
    )

    if gt_refs > 0:
        return "low"

    blame_iterations: set[int] = set()
    for prov in asi_provenance:
        blame = prov.get("blame_set")
        if isinstance(blame, str):
            try:
                blame = json.loads(blame)
            except (json.JSONDecodeError, TypeError):
                blame = [blame]
        if not isinstance(blame, list):
            continue
        for b in blame:
            if tvf_lower in str(b).lower():
                it = prov.get("iteration")
                if it is not None:
                    blame_iterations.add(int(it))

    full_coverage = schema_overlap.get("full_coverage", False)

    if full_coverage and len(blame_iterations) >= TVF_REMOVAL_BLAME_THRESHOLD:
        return "high"

    uncovered = schema_overlap.get("uncovered_columns", [])
    if schema_overlap.get("coverage_ratio", 0) > 0:
        uncov_in_benchmarks = False
        for col in uncovered:
            for b in benchmarks:
                if col.lower() in (b.get("expected_sql") or "").lower():
                    uncov_in_benchmarks = True
                    break
            if uncov_in_benchmarks:
                break
        if not uncov_in_benchmarks:
            return "medium"

    return "low"


def _handle_escalation(
    escalation: str,
    ag: dict,
    *,
    w: Any,
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    domain: str,
    iteration: int,
    benchmarks: list[dict],
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    metadata_snapshot: dict,
) -> dict:
    """Dispatch an escalation action from the strategist.

    Returns ``{"handled": True/False, "action": "...", "detail": {...}}``.
    """
    from genie_space_optimizer.common.uc_metadata import check_tvf_schema_overlap
    from genie_space_optimizer.optimization.labeling import flag_for_human_review
    from genie_space_optimizer.optimization.state import (
        load_provenance,
        write_queued_patch,
    )

    affected = ag.get("affected_questions", [])
    result: dict[str, Any] = {"handled": False, "action": escalation, "detail": {}}

    if escalation == "remove_tvf":
        lever3 = ag.get("lever_directives", {}).get("3", {})
        funcs = lever3.get("functions", [])
        tvf_id = ""
        for f in funcs:
            tvf_id = f.get("identifier") or f.get("function") or ""
            if tvf_id:
                break
        if not tvf_id:
            logger.warning("Escalation remove_tvf but no TVF identifier in lever 3")
            result["detail"] = {"error": "no_tvf_identifier"}
            return result

        schema_overlap = check_tvf_schema_overlap(spark, tvf_id, metadata_snapshot)

        coverage_validation = _validate_tvf_removal_coverage(
            tvf_id, benchmarks, schema_overlap, metadata_snapshot,
        )
        if not coverage_validation["valid"]:
            logger.warning(
                "TVF removal coverage check failed for %s: %s",
                tvf_id, coverage_validation["reason"],
            )
            result["detail"] = {
                "error": "coverage_check_failed",
                "reason": coverage_validation["reason"],
                "affected_questions": coverage_validation["affected_questions"],
                "coverage_ratio": coverage_validation["coverage_ratio"],
            }
            return result

        prov_df = load_provenance(spark, run_id, catalog, schema)
        prov_list = prov_df.to_dict("records") if not prov_df.empty else []

        confidence = _score_tvf_removal_confidence(
            tvf_id, benchmarks, verdict_history, reflection_buffer,
            schema_overlap, prov_list,
        )
        previous_tvf_asset: dict = {}
        for fn in (metadata_snapshot.get("instructions") or {}).get("sql_functions", []):
            if isinstance(fn, dict) and fn.get("identifier") == tvf_id:
                previous_tvf_asset = dict(fn)
                break

        result["detail"]["confidence"] = confidence
        result["detail"]["tvf_identifier"] = tvf_id
        result["detail"]["tvf_id"] = tvf_id
        result["detail"]["previous_tvf_asset"] = previous_tvf_asset
        result["detail"]["schema_overlap"] = schema_overlap

        if confidence is None:
            logger.info(
                "TVF removal of %s: iteration gate not met — deferring",
                tvf_id,
            )
            result["detail"]["deferred"] = True
            return result

        result["handled"] = True

        if confidence == "low":
            write_queued_patch(
                spark, run_id, iteration, "remove_tvf", tvf_id,
                catalog, schema,
                confidence_tier="low",
                coverage_analysis=schema_overlap,
                blame_iterations=0,
            )
            flag_for_human_review(
                spark, run_id, catalog, schema, domain,
                [{
                    "question_id": q,
                    "question_text": "",
                    "reason": f"Low-confidence TVF removal recommended: {tvf_id}",
                    "iterations_failed": 0,
                    "patches_tried": "remove_tvf",
                } for q in (affected or [tvf_id])],
            )
            result["detail"]["tier_action"] = "flagged_only"

        elif confidence == "medium":
            result["detail"]["tier_action"] = "apply_and_flag"
            flag_for_human_review(
                spark, run_id, catalog, schema, domain,
                [{
                    "question_id": q,
                    "question_text": "",
                    "reason": f"Medium-confidence TVF removal applied: {tvf_id} — please verify",
                    "iterations_failed": 0,
                    "patches_tried": "remove_tvf",
                } for q in (affected or [tvf_id])],
            )

        else:
            result["detail"]["tier_action"] = "auto_apply"

    elif escalation == "gt_repair":
        logger.info(
            "Escalation gt_repair for questions %s — running inline arbiter corrections",
            affected,
        )
        _corr_result = _run_arbiter_corrections(
            w, spark, run_id, catalog, schema, domain,
            force_adopt_qids=set(affected) if affected else None,
        )
        _total_corrections = (
            _corr_result.get("gc_applied", 0)
            + _corr_result.get("nc_repaired", 0)
        )
        result["handled"] = True
        result["detail"]["corrections_applied"] = _total_corrections
        result["detail"]["gc_applied"] = _corr_result.get("gc_applied", 0)
        result["detail"]["nc_repaired"] = _corr_result.get("nc_repaired", 0)
        result["detail"]["corrected_qids"] = sorted(_corr_result.get("corrected_qids", set()))
        result["detail"]["quarantined_qids"] = sorted(_corr_result.get("quarantined_qids", set()))

    elif escalation == "flag_for_review":
        flag_for_human_review(
            spark, run_id, catalog, schema, domain,
            [{
                "question_id": q,
                "question_text": "",
                "reason": ag.get("root_cause_summary", "Strategist flagged for review"),
                "iterations_failed": 0,
                "patches_tried": "",
            } for q in affected],
        )
        result["handled"] = True
        result["detail"]["flagged_count"] = len(affected)
    else:
        logger.warning("Unknown escalation type: %s", escalation)

    return result


def _extract_confirmed_corrections(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    already_corrected: set[str] | None = None,
    force_adopt_qids: set[str] | None = None,
) -> list[dict]:
    """Return benchmark corrections for questions with cross-iteration ``genie_correct`` confirmation.

    A question qualifies when it received ``genie_correct`` in at least
    ``GENIE_CORRECT_CONFIRMATION_THRESHOLD`` independent evaluations.
    Questions in *force_adopt_qids* bypass this threshold and are adopted
    with a single ``genie_correct`` evaluation.

    Uses the most recent Genie SQL as the corrected expected SQL.
    """
    history = _build_verdict_history(spark, run_id, catalog, schema)
    corrected = already_corrected or set()
    force = force_adopt_qids or set()
    actions: list[dict] = []

    for qid, entries in history.items():
        if qid in corrected:
            continue
        gc_entries = [e for e in entries if e.verdict == "genie_correct"]
        gc_iterations = {e.iteration for e in gc_entries}
        threshold = 1 if qid in force else GENIE_CORRECT_CONFIRMATION_THRESHOLD
        if len(gc_iterations) < threshold:
            continue
        latest = max(gc_entries, key=lambda e: e.iteration)
        if latest.genie_sql:
            actions.append({
                "question": latest.question_text,
                "question_id": qid,
                "new_expected_sql": latest.genie_sql,
                "verdict": "genie_correct",
                "confirmation_count": len(gc_iterations),
            })
    return actions


def _extract_neither_correct_repair_candidates(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    already_repaired: set[str] | None = None,
) -> list[dict]:
    """Return questions that need GT repair due to repeated ``neither_correct`` verdicts.

    A question qualifies when it received ``neither_correct`` in at least
    ``NEITHER_CORRECT_REPAIR_THRESHOLD`` independent evaluations.
    """
    history = _build_verdict_history(spark, run_id, catalog, schema)
    repaired = already_repaired or set()
    candidates: list[dict] = []

    for qid, entries in history.items():
        if qid in repaired:
            continue
        nc_entries = [e for e in entries if e.verdict == "neither_correct"]
        nc_iterations = {e.iteration for e in nc_entries}
        if len(nc_iterations) < NEITHER_CORRECT_REPAIR_THRESHOLD:
            continue

        consecutive_nc = 0
        for e in reversed(entries):
            if e.verdict == "neither_correct":
                consecutive_nc += 1
            else:
                break

        latest = max(nc_entries, key=lambda e: e.iteration)
        rationales = [e.rationale for e in nc_entries if e.rationale]

        candidates.append({
            "question": latest.question_text,
            "question_id": qid,
            "genie_sql": latest.genie_sql,
            "expected_sql": latest.expected_sql,
            "rationale": " | ".join(rationales[-3:]),
            "nc_count": len(nc_iterations),
            "consecutive_nc": consecutive_nc,
        })
    return candidates


def _should_quarantine(candidate: dict) -> bool:
    """Decide whether a ``neither_correct`` question should be quarantined."""
    return candidate.get("consecutive_nc", 0) >= NEITHER_CORRECT_QUARANTINE_THRESHOLD




def _attempt_gt_repair(
    w: WorkspaceClient,
    candidate: dict,
    spark: Any,
    *,
    warehouse_id: str = "",
) -> str | None:
    """Use LLM to produce a corrected ground-truth SQL for a ``neither_correct`` question.

    Returns the validated corrected SQL string, or ``None`` if repair fails.
    """
    from genie_space_optimizer.optimization.evaluation import _call_llm_for_scoring
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    prompt = format_mlflow_template(
        GT_REPAIR_PROMPT,
        question=candidate["question"],
        expected_sql=candidate.get("expected_sql", ""),
        genie_sql=candidate.get("genie_sql", ""),
        rationale=candidate.get("rationale", "No rationale available"),
    )

    try:
        result = _call_llm_for_scoring(w, prompt)
        repaired_sql = ""
        if isinstance(result, str):
            repaired_sql = result.strip()
        elif isinstance(result, dict):
            repaired_sql = (
                result.get("sql", "")
                or result.get("corrected_sql", "")
                or result.get("query", "")
            ).strip()

        if not repaired_sql:
            logger.warning("GT repair returned empty SQL for: %s", candidate["question"][:60])
            return None

        is_valid, val_err = validate_ground_truth_sql(
            repaired_sql, spark, execute=True,
            w=w, warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.warning(
                "GT repair SQL failed validation for '%s': %s",
                candidate["question"][:60], val_err[:200],
            )
            return None

        return repaired_sql
    except Exception:
        logger.warning("GT repair LLM call failed for: %s", candidate["question"][:60], exc_info=True)
        return None


def _run_arbiter_corrections(
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    domain: str,
    *,
    already_corrected: set[str] | None = None,
    already_repaired: set[str] | None = None,
    quarantined_qids: set[str] | None = None,
    force_adopt_qids: set[str] | None = None,
) -> dict:
    """Run the full cross-iteration arbiter correction pipeline.

    1. ``genie_correct`` confirmations → benchmark corrections
    2. ``neither_correct`` repairs → LLM-assisted GT repair or quarantine

    When *force_adopt_qids* is provided, questions in that set bypass the
    normal ``GENIE_CORRECT_CONFIRMATION_THRESHOLD`` and are adopted with a
    single ``genie_correct`` evaluation.  This is used when the strategist
    explicitly escalates ``gt_repair`` for specific questions.

    Returns ``{gc_applied, gc_skipped, nc_repaired, nc_quarantined, corrected_qids, quarantined_qids}``.
    """
    from genie_space_optimizer.optimization.benchmarks import (
        apply_benchmark_corrections,
        quarantine_benchmark_question,
    )

    uc_schema = f"{catalog}.{schema}"
    corrected = set(already_corrected or set())
    repaired = set(already_repaired or set())
    quarantined = set(quarantined_qids or set())

    gc_applied = 0
    gc_skipped = 0
    nc_repaired = 0
    nc_quarantined = 0

    # ── Phase 1: genie_correct confirmations ──────────────────────────
    gc_actions = _extract_confirmed_corrections(
        spark, run_id, catalog, schema,
        already_corrected=corrected,
        force_adopt_qids=force_adopt_qids,
    )

    if gc_actions:
        print(
            f"\n-- PER-QUESTION ARBITER CORRECTIONS " + "-" * 16 + "\n"
            f"  Confirmed genie_correct questions: {len(gc_actions)}"
        )
        for ac in gc_actions:
            print(
                f"    - [{ac.get('question_id', '?')}] "
                f"\"{ac['question'][:60]}\" "
                f"(confirmed in {ac.get('confirmation_count', '?')} evals)"
            )

        result = apply_benchmark_corrections(gc_actions, spark, uc_schema, domain)
        gc_applied = result["applied"]
        gc_skipped = result["skipped"]
        print(
            f"  Applied: {gc_applied}, Skipped: {gc_skipped}"
        )
        if result["errors"]:
            print(f"  Errors: {result['errors'][:3]}")
        print("-" * 52)

        for ac in gc_actions:
            qid = ac.get("question_id")
            if qid:
                corrected.add(qid)

    # ── Phase 2: neither_correct repair / quarantine ──────────────────
    nc_candidates = _extract_neither_correct_repair_candidates(
        spark, run_id, catalog, schema,
        already_repaired=repaired | quarantined,
    )

    if nc_candidates:
        print(
            f"\n-- NEITHER_CORRECT GT REPAIR " + "-" * 24 + "\n"
            f"  Candidates: {len(nc_candidates)}"
        )

        for cand in nc_candidates:
            qid = cand.get("question_id", "?")
            if _should_quarantine(cand):
                print(
                    f"    - [{qid}] QUARANTINE: \"{cand['question'][:60]}\" "
                    f"({cand['consecutive_nc']} consecutive neither_correct)"
                )
                try:
                    quarantine_benchmark_question(
                        spark, uc_schema, domain, cand["question"],
                        reason=(
                            f"Quarantined after {cand['consecutive_nc']} consecutive "
                            f"neither_correct verdicts across {cand['nc_count']} evaluations"
                        ),
                    )
                    quarantined.add(qid)
                    nc_quarantined += 1
                except Exception:
                    logger.warning("Failed to quarantine %s", qid, exc_info=True)
            else:
                print(
                    f"    - [{qid}] REPAIR ATTEMPT: \"{cand['question'][:60]}\" "
                    f"(neither_correct in {cand['nc_count']} evals)"
                )
                repaired_sql = _attempt_gt_repair(
                    w, cand, spark,
                    warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                )
                if repaired_sql:
                    repair_actions = [{
                        "question": cand["question"],
                        "question_id": qid,
                        "new_expected_sql": repaired_sql,
                        "verdict": "arbiter_repair",
                    }]
                    repair_result = apply_benchmark_corrections(
                        repair_actions, spark, uc_schema, domain,
                    )
                    if repair_result["applied"] > 0:
                        print(f"      -> Repair succeeded: {repaired_sql[:80]}")
                        repaired.add(qid)
                        nc_repaired += 1
                    else:
                        print(f"      -> Repair SQL rejected: {repair_result['errors'][:2]}")
                else:
                    print(f"      -> Repair failed (LLM returned no valid SQL)")

        print("-" * 52)

    return {
        "gc_applied": gc_applied,
        "gc_skipped": gc_skipped,
        "nc_repaired": nc_repaired,
        "nc_quarantined": nc_quarantined,
        "corrected_qids": corrected,
        "quarantined_qids": quarantined,
    }


def _analyze_and_distribute(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    metadata_snapshot: dict,
    iteration_counter: int,
    lever_label: int,
    *,
    verbose: bool = True,
    quarantined_qids: set[str] | None = None,
) -> dict:
    """Analyze failures once, cluster, and distribute clusters to levers.

    Returns a dict with:
      - ``lever_assignments``: ``{lever_int: [clusters]}``
      - ``all_clusters``: all hard-failure clusters (flat list)
      - ``soft_signal_clusters``: soft-signal clusters
      - ``summary``: printable summary lines
      - ``asi_rows``: ASI rows for Delta
      - ``prov_rows``: provenance rows for Delta
    """
    from genie_space_optimizer.optimization.optimizer import (
        _map_to_lever,
        cluster_failures,
    )

    failure_rows = _get_failure_rows(spark, run_id, catalog, schema)
    _quarantined = quarantined_qids or set()

    _NON_ACTIONABLE_VERDICTS = {"genie_correct", "both_correct"}
    arbiter_counts: dict[str, int] = {}
    arbiter_excluded: list[str] = []
    quarantine_excluded: list[str] = []
    soft_signal_qids: list[str] = []
    filtered_failure_rows: list[dict] = []
    soft_signal_rows: list[dict] = []
    for row in failure_rows:
        av = _get_arbiter_verdict(row)
        qid = _get_question_id(row)
        arbiter_counts[av] = arbiter_counts.get(av, 0) + 1

        if qid in _quarantined:
            quarantine_excluded.append(qid)
            continue

        if av in _NON_ACTIONABLE_VERDICTS:
            if _has_individual_judge_failure(row):
                soft_signal_rows.append(row)
                soft_signal_qids.append(qid)
            else:
                arbiter_excluded.append(qid)
        else:
            filtered_failure_rows.append(row)

    # ── Print failure analysis summary ─────────────────────────────
    _arbiter_summary = "  ".join(f"{k}={v}" for k, v in sorted(arbiter_counts.items()))
    _fa_lines = [
        _section("Failure Analysis", "-"),
        _kv("Total rows loaded", len(failure_rows)),
        _kv("Arbiter verdicts", _arbiter_summary),
    ]
    if quarantine_excluded:
        _fa_lines.append(_kv("Quarantined (excluded)", f"{len(quarantine_excluded)} question(s): {', '.join(quarantine_excluded[:5])}"))
    if arbiter_excluded:
        _fa_lines.append(_kv("Excluded (fully correct)", f"{len(arbiter_excluded)} question(s)"))
    if soft_signal_rows:
        _fa_lines.append(_kv("Soft signals (correct but judges failed)", f"{len(soft_signal_rows)} question(s)"))
        _fa_lines.append(_kv("  Soft signal question IDs", ", ".join(soft_signal_qids[:10])))
        for _ss_row, _ss_qid in zip(soft_signal_rows[:10], soft_signal_qids[:10]):
            _failed_judges = _get_failed_judges(_ss_row)
            _fa_lines.append(f"  |    {_ss_qid}: failed judges = {', '.join(_failed_judges) if _failed_judges else '(none detected)'}")
    _fa_lines.append(_kv("Hard failure rows for clustering", len(filtered_failure_rows)))
    _fa_lines.append(_bar("-"))
    print("\n".join(_fa_lines))

    # ── Cluster hard failures ──────────────────────────────────────
    eval_result_for_clustering = {"rows": filtered_failure_rows}
    clusters = cluster_failures(
        eval_result_for_clustering, metadata_snapshot,
        spark=spark, run_id=run_id, catalog=catalog, schema=schema,
    )

    # ── Cluster soft signals ───────────────────────────────────────
    soft_clusters: list[dict] = []
    if soft_signal_rows:
        soft_eval = {"rows": soft_signal_rows}
        soft_clusters = cluster_failures(
            soft_eval, metadata_snapshot,
            spark=spark, run_id=run_id, catalog=catalog, schema=schema,
            verbose=False,
        )
        for sc in soft_clusters:
            sc["signal_type"] = "soft"
        _soft_qids_total = sum(len(sc.get("question_ids", [])) for sc in soft_clusters)
        _soft_lines = [
            _section("Soft Signal Clusters (correct-but-suboptimal)", "-"),
            _kv("Soft signal rows", len(soft_signal_rows)),
            _kv("Soft clusters formed", len(soft_clusters)),
            _kv("Soft cluster questions", _soft_qids_total),
            "|",
        ]
        for si, sc in enumerate(soft_clusters, 1):
            sc_judge = sc.get("affected_judge", "?")
            sc_cause = sc.get("root_cause", "?")
            sc_asi = sc.get("asi_failure_type", "n/a")
            sc_qids = sc.get("question_ids", [])
            sc_blame = sc.get("asi_blame_set", sc.get("blame_set", []))
            blame_str = ", ".join(sc_blame) if isinstance(sc_blame, list) and sc_blame else str(sc_blame) if sc_blame else "(none)"
            _soft_lines.append(f"|  Soft cluster {si} / {len(soft_clusters)}")
            _soft_lines.append(f"|    {'Judge:':<24s} {sc_judge}")
            _soft_lines.append(f"|    {'Root cause:':<24s} {sc_cause}")
            _soft_lines.append(f"|    {'ASI failure type:':<24s} {sc_asi}")
            _soft_lines.append(f"|    {'Blame:':<24s} {blame_str}")
            _soft_lines.append(f"|    Questions ({len(sc_qids)}):")
            for qid in sc_qids:
                _soft_lines.append(f"|      {qid}")
            _soft_lines.append("|")
        _soft_lines.append(_bar("-"))
        print("\n".join(_soft_lines))

    # ── Map clusters to levers ─────────────────────────────────────
    lever_assignments: dict[int, list[dict]] = {}
    cluster_lines = [_section(f"Failure Clusters ({len(clusters)} total)", "-"), "|"]
    _root_cause_counter: Counter[str] = Counter()
    _lever_counter: Counter[int] = Counter()
    _all_cluster_qids: set[str] = set()
    _clusters_with_asi = 0
    _clusters_with_blame = 0
    for ci, c in enumerate(clusters, 1):
        mapped = _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        )
        c["_mapped_lever"] = mapped
        lever_assignments.setdefault(mapped, []).append(c)
        blame = c.get("asi_blame_set", c.get("blame_set", []))
        qids = c["question_ids"]
        asi_ft = c.get("asi_failure_type", "n/a")
        cluster_lines.append(f"|  Cluster {ci} / {len(clusters)}")
        cluster_lines.append(f"|    {'Judge:':<24s} {c['affected_judge']}")
        cluster_lines.append(f"|    {'Root cause:':<24s} {c['root_cause']}")
        cluster_lines.append(f"|    {'ASI failure type:':<24s} {asi_ft}")
        cluster_lines.append(f"|    {'Mapped lever:':<24s} {mapped}")
        blame_str = ", ".join(blame) if isinstance(blame, list) and blame else str(blame) if blame else "(none)"
        cluster_lines.append(f"|    {'Blame:':<24s} {blame_str}")
        cluster_lines.append(f"|    Questions ({len(qids)}):")
        for qid in qids:
            cluster_lines.append(f"|      {qid}")
        cluster_lines.append("|")
        _root_cause_counter[c["root_cause"]] += 1
        _lever_counter[mapped] += 1
        _all_cluster_qids.update(qids)
        if asi_ft and asi_ft != "n/a":
            _clusters_with_asi += 1
        if blame and blame != []:
            _clusters_with_blame += 1

    _lever_summary = ", ".join(f"lever {k} = {v}" for k, v in sorted(_lever_counter.items()))
    _top_causes = ", ".join(f"{k} ({v})" for k, v in _root_cause_counter.most_common(5))
    cluster_lines.append("|  --- Summary ---")
    cluster_lines.append(f"|    {'Clusters by lever:':<24s} {_lever_summary}")
    cluster_lines.append(f"|    {'Unique questions:':<24s} {len(_all_cluster_qids)}")
    cluster_lines.append(f"|    {'Top root causes:':<24s} {_top_causes}")
    cluster_lines.append(f"|    {'Clusters with ASI:':<24s} {_clusters_with_asi} of {len(clusters)}")
    cluster_lines.append(f"|    {'Clusters with blame:':<24s} {_clusters_with_blame} of {len(clusters)}")
    cluster_lines.append(_bar("-"))
    print("\n".join(cluster_lines))

    # ── ASI / provenance rows for Delta ────────────────────────────
    all_clusters_for_asi = clusters + soft_clusters
    _asi_rows: list[dict] = []
    _prov_rows: list[dict] = []
    for c in all_clusters_for_asi:
        sig_type = c.get("signal_type", "hard")
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            for jt in qt.get("failed_judges", []):
                _asi_rows.append({
                    "question_id": qid,
                    "judge": jt.get("judge", ""),
                    "value": "no",
                    "failure_type": jt.get("asi_failure_type_raw"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                })
                _prov_rows.append({
                    "question_id": qid,
                    "signal_type": sig_type,
                    "judge": jt.get("judge", ""),
                    "judge_verdict": jt.get("verdict", "FAIL"),
                    "asi_failure_type_raw": jt.get("asi_failure_type_raw"),
                    "resolved_root_cause": jt.get("resolved_root_cause", "other"),
                    "resolution_method": jt.get("resolution_method", "unknown"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                    "rationale_snippet": jt.get("rationale_snippet"),
                    "cluster_id": c.get("cluster_id", ""),
                })

    # ── Pipeline lineage summary ───────────────────────────────────
    _lineage_lines = ["\n== PIPELINE LINEAGE ==========================================================", "|"]
    for c in clusters:
        mapped = c.get("_mapped_lever", _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        ))
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            judges_info = ", ".join(
                f"{jt['judge']} ({jt.get('resolved_root_cause', '?')})"
                for jt in qt.get("failed_judges", [])
            )
            blame = c.get("asi_blame_set") or "(none)"
            cfix_list = c.get("asi_counterfactual_fixes", [])
            cfix = str(cfix_list[0])[:120] if cfix_list else "(none)"
            _lineage_lines.append(f"|  Q: {qid}")
            _lineage_lines.append(f"|    Failed judges:         {judges_info}")
            _lineage_lines.append(f"|    Dominant root cause:   {c['root_cause']}")
            _lineage_lines.append(f"|    Blame:                 {blame}")
            _lineage_lines.append(f"|    Counterfactual:        \"{cfix}\"")
            _lineage_lines.append(f"|    -> Cluster {c['cluster_id']} -> Lever {mapped} ({LEVER_NAMES.get(mapped, '?')})")
            _lineage_lines.append("|")
    _lineage_lines.append("=" * 78)
    print("\n".join(_lineage_lines))

    return {
        "lever_assignments": lever_assignments,
        "all_clusters": clusters,
        "soft_signal_clusters": soft_clusters,
        "asi_rows": _asi_rows,
        "prov_rows": _prov_rows,
        "lever_counter": dict(_lever_counter),
    }


def _run_gate_checks(
    *,
    spark: Any,
    w: WorkspaceClient,
    run_id: str,
    space_id: str,
    exp_name: str,
    domain: str,
    iteration_counter: int,
    ag_id: str,
    benchmarks: list[dict],
    proposals: list[dict],
    patches: list[dict],
    apply_log: dict,
    clusters: list[dict],
    metadata_snapshot: dict,
    predict_fn: Any,
    scorers: list,
    prev_model_id: str,
    best_scores: dict[str, float],
    best_accuracy: float,
    catalog: str,
    schema: str,
    reference_sqls: dict[str, str],
    noise_floor: float,
    affected_question_ids: set[str] | None = None,
    lever_keys: list[str] | None = None,
) -> dict:
    """Run slice → P0 → full eval gate sequence for an action group.

    Returns a dict with:
      - ``passed``: bool
      - ``full_scores``: dict (only if full eval ran)
      - ``full_accuracy``: float (only if full eval ran)
      - ``new_model_id``: str (only if full eval ran)
      - ``full_result``: dict (only if full eval ran)
      - ``rollback_reason``: str (only if failed)
    """
    import mlflow

    uc_schema = f"{catalog}.{schema}"

    has_dict_changes = any(
        (entry.get("patch", {}) or {}).get("enable_entity_matching")
        or (entry.get("action", {}) or {}).get("type") in (
            "enable_value_dictionary", "enable_entity_matching",
        )
        for entry in apply_log.get("applied", [])
    )
    wait_time = (
        PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_dict_changes
        else PROPAGATION_WAIT_SECONDS
    )
    _wait_note = " (extended for value dictionary rebuild)" if has_dict_changes else ""
    patched_objects = apply_log.get("patched_objects", [])
    print(
        _section("Propagation Wait", "-") + "\n"
        + _kv("AG", f"{ag_id}: {len(apply_log.get('applied', []))} patches applied") + "\n"
        + _kv("Patched objects", ", ".join(str(o) for o in patched_objects) if patched_objects else "(none)") + "\n"
        + _kv("Wait time", f"{wait_time}s{_wait_note}") + "\n"
        + _bar("-")
    )
    time.sleep(wait_time)

    # ── Slice gate ────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass

    _run_slice = False
    if ENABLE_SLICE_GATE:
        affected_qids: set[str] = affected_question_ids or set()
        slice_benchmarks = filter_benchmarks_by_scope(
            benchmarks, "slice", patched_objects,
            affected_question_ids=affected_qids,
        )
        _total = len(benchmarks)
        _sliced = len(slice_benchmarks) if slice_benchmarks else 0
        if slice_benchmarks and _sliced <= (1 - SLICE_GATE_MIN_REDUCTION) * _total:
            _run_slice = True
        else:
            print(
                _section(f"SLICE GATE [{ag_id}]: SKIPPED", "-") + "\n"
                + _kv("Reason", f"slice too broad ({_sliced}/{_total} benchmarks)") + "\n"
                + _bar("-")
            )
    else:
        print(
            _section(f"SLICE GATE [{ag_id}]: DISABLED", "-") + "\n"
            + _bar("-")
        )

    if _run_slice:
        _ensure_sql_context(spark, catalog, schema)
        write_stage(
            spark, run_id, f"AG_{ag_id}_SLICE_EVAL", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )
        slice_result = run_evaluation(
            space_id, exp_name, iteration_counter, slice_benchmarks,
            domain, prev_model_id, "slice",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            patched_objects=patched_objects,
            reference_sqls=reference_sqls if reference_sqls else None,
        )
        slice_scores = slice_result.get("scores", {})
        slice_accuracy = slice_result.get("overall_accuracy", 0.0)
        _slice_qw = 100.0 / max(len(benchmarks), 1)
        effective_slice_tol = max(SLICE_GATE_TOLERANCE, noise_floor + 2.0, _slice_qw + 0.5)
        _informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
        if slice_accuracy >= best_accuracy - 2 * noise_floor:
            _informational_judges.add("asset_routing")
        slice_drops = detect_regressions(
            slice_scores, best_scores, threshold=effective_slice_tol,
            skip_judges=_informational_judges,
        )

        try:
            write_iteration(
                spark, run_id, iteration_counter, slice_result,
                catalog=catalog, schema=schema,
                lever=int(lever_keys[0]) if lever_keys else 0,
                eval_scope="slice", model_id=prev_model_id,
            )
        except Exception:
            logger.debug("Failed to write slice iteration", exc_info=True)

        if slice_drops:
            _score_changes = ", ".join(
                f"{d['judge']} {best_scores.get(d['judge'], 0):.1f}->{slice_scores.get(d['judge'], 0):.1f} ({d['drop']:+.1f})"
                for d in slice_drops
            )
            print(
                _section(f"SLICE GATE [{ag_id}]: FAIL", "-") + "\n"
                + _kv("Regressions", _score_changes) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            try:
                update_provenance_gate(
                    spark, run_id, iteration_counter, 0,
                    "slice", "rollback",
                    {"regressions": [{"judge": d["judge"], "drop": d["drop"]} for d in slice_drops]},
                    catalog, schema,
                )
            except Exception:
                logger.debug("Failed to update provenance gate", exc_info=True)
            try:
                log_gate_feedback_on_traces(
                    slice_result, "slice", "rollback",
                    regressions=slice_drops, lever=0, iteration=iteration_counter,
                )
            except Exception:
                logger.debug("Failed to log gate feedback", exc_info=True)
            return {"passed": False, "rollback_reason": f"slice_gate: {slice_drops[0]['judge']}", "failed_eval_result": slice_result}
        else:
            _sc = ", ".join(
                f"{j} {best_scores.get(j, 0):.1f}->{slice_scores.get(j, 0):.1f}"
                for j in sorted(slice_scores)
            )
            print(
                _section(f"SLICE GATE [{ag_id}]: PASS", "-") + "\n"
                + _kv("Score changes", _sc) + "\n"
                + _bar("-")
            )

    # ── P0 gate ───────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    p0_benchmarks = filter_benchmarks_by_scope(benchmarks, "p0")
    if p0_benchmarks:
        _ensure_sql_context(spark, catalog, schema)
        p0_result = run_evaluation(
            space_id, exp_name, iteration_counter, p0_benchmarks,
            domain, prev_model_id, "p0",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
        )
        try:
            write_iteration(
                spark, run_id, iteration_counter, p0_result,
                catalog=catalog, schema=schema,
                lever=int(lever_keys[0]) if lever_keys else 0,
                eval_scope="p0", model_id=prev_model_id,
            )
        except Exception:
            logger.debug("Failed to write P0 iteration", exc_info=True)

        p0_failures = p0_result.get("failures", [])
        if p0_failures:
            print(
                _section(f"P0 GATE [{ag_id}]: FAIL", "-") + "\n"
                + _kv("P0 questions failing", len(p0_failures)) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            return {"passed": False, "rollback_reason": f"p0_gate: {len(p0_failures)} failures", "failed_eval_result": p0_result}
        else:
            print(
                _section(f"P0 GATE [{ag_id}]: PASS", "-") + "\n"
                + _kv("P0 benchmarks", len(p0_benchmarks)) + "\n"
                + _bar("-")
            )

    # ── Full evaluation ───────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    write_stage(
        spark, run_id, f"AG_{ag_id}_FULL_EVAL", "STARTED",
        task_key="lever_loop", iteration=iteration_counter,
        catalog=catalog, schema=schema,
    )

    _model_kwargs = {
        "w": w, "space_id": space_id, "config": metadata_snapshot,
        "iteration": iteration_counter, "domain": domain,
        "experiment_name": exp_name, "uc_schema": uc_schema,
        "patch_set": patches, "parent_model_id": prev_model_id,
    }

    _ensure_sql_context(spark, catalog, schema)
    full_result_1 = run_evaluation(
        space_id, exp_name, iteration_counter, benchmarks,
        domain, None, "full",
        predict_fn, scorers,
        spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
        reference_sqls=reference_sqls if reference_sqls else None,
        model_creation_kwargs=_model_kwargs,
    )
    new_model_id = full_result_1.get("model_id", "")

    scores_1 = full_result_1.get("scores", {})
    accuracy_1 = full_result_1.get("overall_accuracy", 0.0)

    # ── Confirmation eval (2nd run) to smooth Genie non-determinism ──
    if accuracy_1 > best_accuracy:
        print(_kv("Confirmation eval", f"SKIPPED (accuracy improved {best_accuracy:.1f}% -> {accuracy_1:.1f}%)"))
        full_scores = scores_1
        full_accuracy = accuracy_1
        full_result = full_result_1
    else:
        try:
            mlflow.end_run()
        except Exception:
            pass
        print(_kv("Confirmation eval", "running 2nd evaluation to average out variance"))
        _ensure_sql_context(spark, catalog, schema)
        full_result_2 = run_evaluation(
            space_id, exp_name, iteration_counter, benchmarks,
            domain, new_model_id, "full_confirm",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
        )
        scores_2 = full_result_2.get("scores", {})
        accuracy_2 = full_result_2.get("overall_accuracy", 0.0)

        all_judge_keys = set(scores_1) | set(scores_2)
        full_scores = {
            j: (scores_1.get(j, 0.0) + scores_2.get(j, 0.0)) / 2.0
            for j in all_judge_keys
        }
        full_accuracy = (accuracy_1 + accuracy_2) / 2.0
        full_result = full_result_1

        print(
            _kv("Eval run 1 accuracy", f"{accuracy_1:.1f}%") + "\n"
            + _kv("Eval run 2 accuracy", f"{accuracy_2:.1f}%") + "\n"
            + _kv("Averaged accuracy", f"{full_accuracy:.1f}%")
        )

    write_iteration(
        spark, run_id, iteration_counter, full_result,
        catalog=catalog, schema=schema,
        lever=int(lever_keys[0]) if lever_keys else 0,
        eval_scope="full", model_id=new_model_id,
    )

    effective_regression_tol = max(REGRESSION_THRESHOLD, noise_floor)
    _informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    if full_accuracy >= best_accuracy - 2 * noise_floor:
        _informational_judges.add("asset_routing")
    regressions = detect_regressions(
        full_scores, best_scores, threshold=effective_regression_tol,
        skip_judges=_informational_judges,
    )

    accuracy_drop = best_accuracy - full_accuracy
    question_weight = 100.0 / max(len(benchmarks), 1)
    accuracy_threshold = max(effective_regression_tol / 2, noise_floor, question_weight + 0.5)
    if accuracy_drop >= accuracy_threshold:
        regressions.append({
            "judge": "overall_accuracy",
            "previous": best_accuracy,
            "current": full_accuracy,
            "drop": accuracy_drop,
        })

    # ── Per-question noise filtering ──────────────────────────────
    # If all detected regressions are within a single question's weight,
    # they are likely Genie non-determinism, not a true patch-caused
    # regression.  Downgrade them to warnings and proceed.
    if regressions and patched_objects:
        _noise_limit = question_weight * 1.5
        _noise_regs = [r for r in regressions if r["drop"] <= _noise_limit]
        if len(_noise_regs) == len(regressions):
            _noise_details = ", ".join(
                f"{r['judge']} drop={r['drop']:.1f} (limit={_noise_limit:.1f})"
                for r in _noise_regs
            )
            logger.info(
                "Noise filter: %d regression(s) within single-question noise band — treating as pass: %s",
                len(_noise_regs), _noise_details,
            )
            print(
                _kv("Noise filter", f"APPLIED — {len(_noise_regs)} regression(s) within ±{_noise_limit:.1f}pp noise band") + "\n"
                + _kv("Details", _noise_details)
            )
            regressions = []

    # ── Hard guard: never accept an iteration that reduced overall accuracy ─
    # The noise filter above may have cleared per-judge regressions that
    # individually fall within one question's weight, but if accuracy
    # actually dropped, the iteration introduced a genuine regression
    # on a previously-passing question.
    if not regressions and full_accuracy < best_accuracy:
        regressions.append({
            "judge": "overall_accuracy_guard",
            "previous": best_accuracy,
            "current": full_accuracy,
            "drop": best_accuracy - full_accuracy,
        })
        logger.info(
            "Accuracy guard: noise filter cleared per-judge regressions but overall accuracy dropped %.1f%% -> %.1f%% — rejecting iteration",
            best_accuracy, full_accuracy,
        )
        print(
            _kv("Accuracy guard", f"TRIGGERED — accuracy dropped {best_accuracy:.1f}% -> {full_accuracy:.1f}% despite noise filter pass")
        )

    if regressions:
        _reg_details = ", ".join(
            f"{r['judge']} {best_scores.get(r['judge'], 0):.1f}->{full_scores.get(r['judge'], 0):.1f} ({r['drop']:+.1f})"
            for r in regressions
        )
        print(
            _section(f"FULL EVAL [{ag_id}]: FAIL (REGRESSION)", "-") + "\n"
            + _kv("Accuracy", f"{best_accuracy:.1f}% -> {full_accuracy:.1f}%") + "\n"
            + _kv("Regressions", _reg_details) + "\n"
            + _kv("Action", "ROLLBACK") + "\n"
            + _bar("-")
        )
        try:
            update_provenance_gate(
                spark, run_id, iteration_counter, 0,
                "full", "rollback",
                {"regressions": [{"judge": r["judge"], "drop": r["drop"]} for r in regressions]},
                catalog, schema,
            )
        except Exception:
            logger.debug("Failed to update provenance gate (full rollback)", exc_info=True)
        try:
            log_gate_feedback_on_traces(
                full_result, "full", "rollback",
                regressions=regressions, lever=0, iteration=iteration_counter,
            )
        except Exception:
            logger.debug("Failed to log full eval gate feedback", exc_info=True)
        return {"passed": False, "rollback_reason": f"full_eval: {regressions[0]['judge']}", "failed_eval_result": full_result, "regressions": regressions}

    # ── PASSED ────────────────────────────────────────────────────────
    _score_delta = ", ".join(
        f"{j} {best_scores.get(j, 0):.1f}->{full_scores.get(j, 0):.1f}"
        for j in sorted(full_scores)
    )
    print(
        _section(f"FULL EVAL [{ag_id}]: PASS -- ACCEPTED", "=") + "\n"
        + _kv("Accuracy", f"{best_accuracy:.1f}% -> {full_accuracy:.1f}% ({full_accuracy - best_accuracy:+.1f}%)") + "\n"
        + _kv("Score changes", _score_delta) + "\n"
        + _bar("=")
    )
    try:
        update_provenance_gate(
            spark, run_id, iteration_counter, 0,
            "full", "pass", None, catalog, schema,
        )
    except Exception:
        logger.debug("Failed to update provenance gate (full pass)", exc_info=True)
    try:
        log_gate_feedback_on_traces(
            full_result, "full", "pass",
            lever=0, iteration=iteration_counter,
        )
    except Exception:
        logger.debug("Failed to log full eval gate feedback", exc_info=True)

    return {
        "passed": True,
        "full_scores": full_scores,
        "full_accuracy": full_accuracy,
        "new_model_id": new_model_id,
        "full_result": full_result,
    }


def _run_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict],
    exp_name: str,
    prev_scores: dict[str, float],
    prev_accuracy: float,
    prev_model_id: str,
    config: dict,
    catalog: str,
    schema: str,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    apply_mode: str = APPLY_MODE,
    triggered_by: str = "",
    human_corrections: list[dict] | None = None,
    enrichment_done: bool = False,
    enrichment_model_id: str = "",
) -> dict:
    """Stage 3: Iterate levers with convergence checking.

    Internal Python loop over levers. Supports resume on task retry.

    When *enrichment_done* is True, Phase 1 (proactive enrichment) is skipped
    because it was already executed by the standalone enrichment task. The
    enriched config is loaded from the Genie Space API instead.

    Returns dict with best scores, model_id, iteration_counter, levers lists.
    """
    levers = levers or DEFAULT_LEVER_ORDER
    thresholds = thresholds or DEFAULT_THRESHOLDS

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "STARTED",
        task_key="lever_loop", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    resume_state = _resume_lever_loop(spark, run_id, catalog, schema)
    start_lever = resume_state.get("resume_from_lever", 0)
    iteration_counter = resume_state.get("iteration_counter", 0)
    if resume_state.get("prev_scores"):
        prev_scores = resume_state["prev_scores"]
    if resume_state.get("prev_model_id"):
        prev_model_id = resume_state["prev_model_id"]
    if resume_state.get("prev_accuracy"):
        prev_accuracy = resume_state["prev_accuracy"]

    baseline_accuracy = prev_accuracy
    best_scores = dict(prev_scores)
    best_accuracy = prev_accuracy
    best_model_id = prev_model_id
    best_iteration = iteration_counter

    levers_attempted: list[int] = []
    levers_accepted: list[int] = []
    levers_rolled_back: list[int] = []
    lever_changes: list[dict] = []
    all_failure_trace_ids: list[str] = []
    all_regression_trace_ids: list[str] = []
    all_eval_mlflow_run_ids: list[str] = []
    all_failure_question_ids: list[str] = []
    question_trace_map: dict[str, list[str]] = {}

    _human_sql_fixes = [
        {"question": c.get("question", ""), "new_expected_sql": c["corrected_sql"], "verdict": "genie_correct"}
        for c in (human_corrections or [])
        if c.get("type") == "benchmark_correction" and c.get("corrected_sql")
    ]

    _judge_overrides = [c for c in (human_corrections or []) if c.get("type") == "judge_override"]
    for ov in _judge_overrides:
        try:
            feedback = ov.get("feedback", "")
            if "Genie answer is actually fine" in feedback or "Correct" in feedback:
                genie_sql = _extract_genie_sql_from_trace(ov.get("trace_id", ""))
                if genie_sql:
                    _human_sql_fixes.append({
                        "question": ov.get("question", ""),
                        "new_expected_sql": genie_sql,
                        "verdict": "genie_correct",
                    })
            elif "both answers are wrong" in feedback or "Both Wrong" in feedback:
                from genie_space_optimizer.optimization.benchmarks import quarantine_benchmark_question
                quarantine_benchmark_question(
                    spark, f"{catalog}.{schema}", domain,
                    ov.get("question", "") or ov.get("question_id", ""),
                    reason="both_wrong",
                )
            elif "Ambiguous" in feedback:
                from genie_space_optimizer.optimization.benchmarks import quarantine_benchmark_question
                quarantine_benchmark_question(
                    spark, f"{catalog}.{schema}", domain,
                    ov.get("question", "") or ov.get("question_id", ""),
                    reason="ambiguous",
                )
        except Exception:
            logger.warning("Failed to process judge_override feedback", exc_info=True)

    if _human_sql_fixes:
        try:
            from genie_space_optimizer.optimization.benchmarks import apply_benchmark_corrections
            _hfix = apply_benchmark_corrections(_human_sql_fixes, spark, f"{catalog}.{schema}", domain)
            print(
                f"\n[Human Feedback] Applied {_hfix['applied']} benchmark corrections "
                f"from prior review (skipped {_hfix['skipped']})"
            )
        except Exception:
            logger.warning("Failed to apply human benchmark corrections", exc_info=True)

    _human_suggestions = [c for c in (human_corrections or []) if c.get("type") == "improvement"]

    _ensure_sql_context(spark, catalog, schema)
    from genie_space_optimizer.optimization.evaluation import build_metric_view_measures
    _mv_measures = build_metric_view_measures(config)
    _instr_prompt = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE,
        uc_schema=f"{catalog}.{schema}", space_id=space_id,
    )
    predict_fn = make_predict_fn(
        w, space_id, spark, catalog, schema,
        metric_view_measures=_mv_measures,
        warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        optimization_run_id=run_id,
        triggered_by=triggered_by,
        instruction_prompt_name=_instr_prompt,
    )
    scorers = make_all_scorers(w, spark, catalog, schema)
    uc_schema = f"{catalog}.{schema}"
    metadata_snapshot = config.get("_parsed_space", config)
    data_profile = (
        metadata_snapshot.get("_data_profile", {})
        or config.get("_data_profile", {})
    )

    uc_columns = config.get("_uc_columns", [])
    if uc_columns:
        enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    enrichment_result: dict = {}
    join_result: dict = {}
    meta_result: dict = {}
    instruction_result: dict = {}

    if not enrichment_done:
        # ── Phase 1: Proactive Enrichment (inline, legacy path) ──
        import mlflow as _mlflow_legacy_enr

        _legacy_enr_run_name = f"lever_loop_enrichment_{run_id[:12]}"
        with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):
            _mlflow_legacy_enr.set_tags({
                "genie.space_id": space_id,
                "genie.run_type": "inline_enrichment",
            })

            _pe_lines = [_section("LEVER LOOP — PROACTIVE ENRICHMENT", "-")]
            _pe_lines.append(_kv("Space ID", space_id))
            _pe_lines.append(_kv("UC columns", len(uc_columns)))
            _pe_lines.append(_bar("-"))
            print("\n".join(_pe_lines))

            enrichment_result = _run_description_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tables_enriched", 0) > 0:
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            join_result = _run_proactive_join_discovery(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if join_result.get("total_applied", 0) > 0:
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            meta_result = _run_space_metadata_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if meta_result.get("description_generated") or meta_result.get("questions_generated"):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            instruction_result = _run_proactive_instruction_seeding(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if instruction_result.get("instructions_seeded"):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            _enr_summary = [_section("PROACTIVE ENRICHMENT — SUMMARY", "-")]
            _enr_summary.append(_kv("Descriptions enriched", enrichment_result.get("total_enriched", 0)))
            _enr_summary.append(_kv("Joins discovered", join_result.get("total_applied", 0)))
            _enr_summary.append(_kv("Space metadata", "description=%s, questions=%s" % (
                "generated" if meta_result.get("description_generated") else "unchanged",
                "generated" if meta_result.get("questions_generated") else "unchanged",
            )))
            _enr_summary.append(_kv("Instructions seeded", "yes" if instruction_result.get("instructions_seeded") else "no"))
            _enr_summary.append(_bar("-"))
            print("\n".join(_enr_summary))

            _mlflow_legacy_enr.log_metrics({
                "enrichment.columns_enriched": enrichment_result.get("total_enriched", 0),
                "enrichment.tables_enriched": enrichment_result.get("tables_enriched", 0),
                "enrichment.joins_discovered": join_result.get("total_applied", 0),
            })
    else:
        # Enrichment already handled by the enrichment task -- reload fresh
        # config from API (enrichment patches are already applied).
        from genie_space_optimizer.common.genie_client import fetch_space_config
        from genie_space_optimizer.common.uc_metadata import (
            extract_genie_space_table_refs,
            get_columns_for_tables_rest,
        )
        config = fetch_space_config(w, space_id)
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns
        metadata_snapshot = config.get("_parsed_space", config)
        metadata_snapshot["_data_profile"] = data_profile
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
        if enrichment_model_id:
            prev_model_id = enrichment_model_id
        print("\n".join([
            _section("LEVER LOOP — ENRICHMENT ALREADY DONE", "-"),
            _kv("Enrichment model", enrichment_model_id or "(none)"),
            _kv("Config loaded from", "Genie Space API (post-enrichment)"),
            _bar("-"),
        ]))

    # ── Phase 2: Pre-Loop Setup ──
    _pls_lines = [_section("LEVER LOOP — PRE-LOOP SETUP", "-")]
    print("\n".join(_pls_lines))

    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    reference_sqls: dict[str, str] = {}
    reference_result_hashes: dict[str, str] = {}
    if baseline_iter:
        rows_json = baseline_iter.get("rows_json")
        if isinstance(rows_json, list):
            _rows_payload = {"rows": rows_json}
            reference_sqls = extract_reference_sqls(_rows_payload)
            reference_result_hashes = extract_reference_result_hashes(_rows_payload)
        elif isinstance(rows_json, str):
            try:
                _rows_payload = {"rows": json.loads(rows_json)}
                reference_sqls = extract_reference_sqls(_rows_payload)
                reference_result_hashes = extract_reference_result_hashes(_rows_payload)
            except (json.JSONDecodeError, TypeError):
                pass
    logger.info(
        "Lever loop: %d reference SQLs, %d result hashes from baseline",
        len(reference_sqls),
        len(reference_result_hashes),
    )

    # ── Per-question cross-iteration arbiter corrections ─────────────
    _correction_state: dict[str, set[str]] = {
        "corrected_qids": set(),
        "repaired_qids": set(),
        "quarantined_qids": set(),
    }
    _pre_loop_corr = _run_arbiter_corrections(
        w, spark, run_id, catalog, schema, domain,
        already_corrected=_correction_state["corrected_qids"],
        already_repaired=_correction_state["repaired_qids"],
        quarantined_qids=_correction_state["quarantined_qids"],
    )
    _correction_state["corrected_qids"] = _pre_loop_corr["corrected_qids"]
    _correction_state["quarantined_qids"] = _pre_loop_corr["quarantined_qids"]

    # ── Mine benchmark examples once (proactive, before loop) ──────────
    mined_example_proposals: list = []
    if not enrichment_done:
        mined_example_proposals = _mine_benchmark_example_sqls(
            benchmarks, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=schema,
            w=w, warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        )
        if mined_example_proposals:
            _apply_proactive_example_sqls(
                w, spark, run_id, space_id, mined_example_proposals,
                metadata_snapshot, config, catalog, schema,
            )
            from genie_space_optimizer.common.genie_client import fetch_space_config
            config = fetch_space_config(w, space_id)
            config["_uc_columns"] = uc_columns
            metadata_snapshot = config.get("_parsed_space", config)
            metadata_snapshot["_data_profile"] = data_profile
            if uc_columns:
                enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    _setup_lines = [_section("PRE-LOOP SETUP — COMPLETE", "-")]
    _setup_lines.append(_kv("Reference SQLs", len(reference_sqls)))
    _setup_lines.append(_kv("Reference hashes", len(reference_result_hashes)))
    _setup_lines.append(_kv("Arbiter corrections", len(_pre_loop_corr.get("corrected_qids", set()))))
    _setup_lines.append(_kv("Mined examples", len(mined_example_proposals)))
    _setup_lines.append(_kv("Starting lever", start_lever))
    _setup_lines.append(_kv("Iteration counter", iteration_counter))
    _setup_lines.append(_kv("Baseline accuracy", f"{baseline_accuracy:.1f}%"))
    _setup_lines.append(_bar("-"))
    print("\n".join(_setup_lines))

    # ── Phase 3: Adaptive Lever Loop ──
    _loop_lines = [_section("LEVER LOOP — ADAPTIVE ITERATION", "-")]
    _loop_lines.append(_kv("Max iterations", max_iterations))
    _loop_lines.append(_kv("Lever order", levers))
    _loop_lines.append(_bar("-"))
    print("\n".join(_loop_lines))

    # ═══════════════════════════════════════════════════════════════════
    # ADAPTIVE LEVER LOOP
    # Re-cluster → priority score → strategist (1 AG) → apply → gate
    # → accept/rollback → reflect … repeat
    # ═══════════════════════════════════════════════════════════════════
    ags_attempted: list[str] = []
    ags_accepted: list[str] = []
    ags_rolled_back: list[str] = []
    noise_floor = min(100.0 / max(len(benchmarks), 1), MAX_NOISE_FLOOR)

    reflection_buffer: list[dict] = resume_state.get("reflection_buffer", [])
    skill_exemplars: list[dict] = resume_state.get("skill_exemplars", [])
    tried_patches: set[tuple[str, str]] = resume_state.get("tried_patches", set())
    tried_root_causes: set[tuple[str, str]] = resume_state.get("tried_root_causes", set())
    prev_failure_qids: set[str] = set()
    _verdict_history: dict[str, list] = {}
    _last_full_mlflow_run_id: str = baseline_iter.get("mlflow_run_id", "") if baseline_iter else ""

    for _iter_num in range(1, max_iterations + 1):
        # ── Exit checks ──────────────────────────────────────────────
        if all_thresholds_met(best_scores, thresholds):
            logger.info("Convergence: all thresholds met before iteration %d", _iter_num)
            break
        if _diminishing_returns(reflection_buffer):
            logger.info("Diminishing returns detected — stopping at iteration %d", _iter_num)
            break
        _consecutive_rb = 0
        for _rb_entry in reversed(reflection_buffer):
            if _rb_entry.get("escalation_handled"):
                continue
            if not _rb_entry.get("accepted"):
                _consecutive_rb += 1
            else:
                break
        if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:
            logger.info(
                "Consecutive rollback limit (%d) reached — stopping at iteration %d",
                CONSECUTIVE_ROLLBACK_LIMIT, _iter_num,
            )
            break

        iteration_counter += 1

        # ── 3B.1b: Per-iteration arbiter corrections ─────────────────
        _iter_corr = _run_arbiter_corrections(
            w, spark, run_id, catalog, schema, domain,
            already_corrected=_correction_state["corrected_qids"],
            already_repaired=_correction_state["repaired_qids"],
            quarantined_qids=_correction_state["quarantined_qids"],
        )
        _correction_state["corrected_qids"] = _iter_corr["corrected_qids"]
        _correction_state["quarantined_qids"] = _iter_corr["quarantined_qids"]

        # ── 3B.2: Re-cluster from latest eval ────────────────────────
        _analysis = _analyze_and_distribute(
            spark, run_id, catalog, schema, metadata_snapshot,
            iteration_counter - 1, lever_label=0,
            quarantined_qids=_correction_state["quarantined_qids"],
        )
        clusters = _analysis["all_clusters"]
        soft_signal_clusters = _analysis["soft_signal_clusters"]

        try:
            write_asi_results(spark, run_id, iteration_counter - 1, _analysis["asi_rows"], catalog, schema, mlflow_run_id=_last_full_mlflow_run_id)
        except Exception:
            logger.debug("Failed to write ASI results", exc_info=True)
        try:
            write_provenance(spark, run_id, iteration_counter - 1, 0, _analysis["prov_rows"], catalog, schema)
        except Exception:
            logger.debug("Failed to write provenance rows", exc_info=True)

        clusters = _filter_tried_clusters(clusters, tried_root_causes)
        if not clusters and not soft_signal_clusters:
            logger.info("No actionable clusters remain — stopping at iteration %d", _iter_num)
            break

        # ── 3B.3: Priority scoring ───────────────────────────────────
        ranked = rank_clusters(clusters)

        # ── 3B.4: Adaptive strategist (1 LLM call → 1 AG) ───────────
        print(_section(f"ADAPTIVE STRATEGIST — Iteration {iteration_counter}", "="))

        _verdict_history = _build_verdict_history(spark, run_id, catalog, schema)

        # ── 3B.3b: Hard-quarantine exhausted questions ────────────────
        if reflection_buffer:
            _, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )
            _quarantine_qids: set[str] = set()
            for _pq_id, _pq_info in _persist_data.items():
                _pq_class = _pq_info.get("classification", "")
                _pq_consec = _pq_info.get("max_consecutive", 0)
                if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (
                    _pq_class == "PERSISTENT" and _pq_consec >= 3
                ):
                    _quarantine_qids.add(_pq_id)
            if _quarantine_qids:
                _newly_quarantined = _quarantine_qids - _correction_state["quarantined_qids"]
                if _newly_quarantined:
                    logger.info(
                        "Hard-quarantining %d exhausted question(s): %s",
                        len(_newly_quarantined), _newly_quarantined,
                    )
                    _correction_state["quarantined_qids"] |= _newly_quarantined
                    try:
                        from genie_space_optimizer.optimization.labeling import flag_for_human_review
                        _flag_items = []
                        for _hq_id in sorted(_newly_quarantined):
                            _hq_info = _persist_data[_hq_id]
                            _tried_str = "; ".join(
                                f"iter{it}: {pt}" for it, pt in _hq_info.get("patches_tried", [])
                            )
                            _flag_items.append({
                                "question_id": _hq_id,
                                "question_text": _hq_info.get("question_text", ""),
                                "reason": (
                                    f"{_hq_info['classification']}: "
                                    f"failed {_hq_info['fail_count']}/{_hq_info['total_evals']} evals, "
                                    f"{_hq_info['max_consecutive']} consecutive"
                                ),
                                "iterations_failed": _hq_info.get("fail_count", 0),
                                "patches_tried": _tried_str,
                            })
                        if _flag_items:
                            _flagged = flag_for_human_review(
                                spark, run_id, catalog, schema, domain, _flag_items,
                            )
                            print(
                                _section("PERSISTENCE QUARANTINE", "!") + "\n"
                                + _kv("Questions quarantined", len(_newly_quarantined)) + "\n"
                                + _kv("Flagged for human review", _flagged) + "\n"
                                + _bar("!")
                            )
                    except Exception:
                        logger.warning("Failed to flag quarantined questions for human review", exc_info=True)
                for c in clusters:
                    c_qids = c.get("question_ids", [])
                    c["question_ids"] = [q for q in c_qids if q not in _quarantine_qids]
                clusters = [c for c in clusters if c.get("question_ids")]
                if not clusters and not soft_signal_clusters:
                    logger.info("All clusters emptied after quarantine — stopping at iteration %d", _iter_num)
                    break

        _total_q = len(benchmarks)
        _passing_q = _total_q - sum(len(c.get("question_ids", [])) for c in clusters)

        # ── Open a strategy MLflow run for this iteration ──────────
        import mlflow as _mlflow
        from datetime import datetime as _dt

        try:
            _mlflow.end_run()
        except Exception:
            pass
        _strat_ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        _mlflow.start_run(
            run_name=f"strategy_{iteration_counter}_eval_{_strat_ts}",
        )
        try:
            _mlflow.set_tags({
                "genie.space_id": space_id,
                "genie.domain": domain,
                "genie.optimization_run_id": run_id,
                "genie.iteration": str(iteration_counter),
                "genie.run_type": "strategy",
            })

            strategy = _call_llm_for_adaptive_strategy(
                clusters=clusters,
                soft_signal_clusters=soft_signal_clusters,
                metadata_snapshot=metadata_snapshot,
                reflection_buffer=reflection_buffer,
                priority_ranking=ranked,
                tried_patches=tried_patches,
                w=w,
                total_benchmarks=_total_q,
                passing_benchmarks=max(0, _passing_q),
                verdict_history=_verdict_history,
                skill_exemplars=skill_exemplars or None,
                human_suggestions=_human_suggestions or None,
            )
            action_groups = strategy.get("action_groups", [])
            ag = action_groups[0] if action_groups else None

            _global_rewrite = strategy.get("global_instruction_rewrite")
            if isinstance(_global_rewrite, dict):
                non_empty = {k: v for k, v in _global_rewrite.items() if v is not None}
                if non_empty and ag is not None:
                    ld = ag.setdefault("lever_directives", {})
                    l5 = ld.setdefault("5", {})
                    l5["instruction_sections"] = non_empty
            elif isinstance(_global_rewrite, str) and _global_rewrite.strip():
                if ag is not None:
                    ld = ag.setdefault("lever_directives", {})
                    l5 = ld.setdefault("5", {})
                    l5["instruction_guidance"] = _global_rewrite.strip()

            if ag is None and _iter_num == 1:
                logger.info("Adaptive strategist returned 0 AGs on iter 1 — trying holistic fallback")
                fallback_strategy = _generate_holistic_strategy(
                    clusters=clusters,
                    soft_signal_clusters=soft_signal_clusters,
                    metadata_snapshot=metadata_snapshot,
                    w=w,
                )
                _fb_ags = fallback_strategy.get("action_groups", [])
                _fb_ags.sort(key=lambda a: a.get("priority", 999))
                if _fb_ags:
                    ag = _fb_ags[0]
                    strategy = fallback_strategy
        finally:
            _mlflow.end_run()

        if ag is None:
            logger.info("Strategist produced 0 action groups — ending lever loop")
            print(
                _section("Strategy produced 0 action groups — nothing to do", "-") + "\n"
                + _bar("-")
            )
            break

        ag_id = ag.get("id", f"AG{iteration_counter}")
        ags_attempted.append(ag_id)
        lever_keys = sorted(ag.get("lever_directives", {}).keys())

        print(
            _section(f"ACTION GROUP {ag_id} — Iteration {iteration_counter}") + "\n"
            + _kv("Root cause", ag.get("root_cause_summary", "?")[:120]) + "\n"
            + _kv("Levers", ", ".join(lever_keys)) + "\n"
            + _kv("Affected questions", len(ag.get("affected_questions", []))) + "\n"
            + _kv("Best accuracy", f"{best_accuracy:.1f}%") + "\n"
            + _scorecard(best_scores) + "\n"
            + _bar("=")
        )

        _ag_source_cids = set(ag.get("source_cluster_ids", []))
        _ag_cluster_info: dict = {}
        for _rc_idx, _rc in enumerate(ranked):
            _rc_cid = _rc.get("cluster_id", "")
            if _ag_source_cids and _rc_cid not in _ag_source_cids:
                continue
            _ag_cluster_info = {
                "cluster_id": _rc_cid,
                "impact_score": _rc.get("impact_score"),
                "rank": _rc_idx + 1,
                "question_count": len(_rc.get("question_ids", [])),
                "root_cause": _rc.get("root_cause") or _rc.get("asi_failure_type"),
                "affected_questions": _rc.get("question_ids", [])[:20],
            }
            break

        _ag_cluster_info["rationale"] = ag.get("rationale", strategy.get("rationale", "") if strategy else "")
        _ag_cluster_info["escalation"] = ag.get("escalation") or None
        _global_rewrite = strategy.get("global_instruction_rewrite", "") if strategy else ""
        _ag_cluster_info["instruction_rewrite_preview"] = str(_global_rewrite)[:500] if _global_rewrite else ""

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            detail=_ag_cluster_info if _ag_cluster_info else None,
            catalog=catalog, schema=schema,
        )

        # ── 3B.4a+: Persist improvement proposals from strategist ───
        _ag_proposals = ag.get("proposals", [])
        if _ag_proposals and isinstance(_ag_proposals, list):
            for _prop in _ag_proposals:
                if not isinstance(_prop, dict):
                    continue
                try:
                    write_suggestion(spark, catalog, schema, {
                        "run_id": run_id,
                        "space_id": space_id,
                        "iteration": iteration_counter,
                        "lever": None,
                        "type": _prop.get("type", "METRIC_VIEW"),
                        "title": _prop.get("title", "Untitled proposal"),
                        "rationale": _prop.get("rationale"),
                        "definition": _prop.get("definition"),
                        "affected_questions": _prop.get("affected_questions", []),
                        "estimated_impact": _prop.get("estimated_impact"),
                    })
                except Exception:
                    logger.debug("Failed to write suggestion from AG %s", ag_id, exc_info=True)
            if _ag_proposals:
                logger.info("Wrote %d improvement proposals from AG %s", len(_ag_proposals), ag_id)

        # ── 3B.4b: Handle escalation if present ─────────────────────
        _escalation = ag.get("escalation", "")
        if _escalation:
            print(
                _section(f"ESCALATION: {_escalation}", "!") + "\n"
                + _kv("Type", _escalation) + "\n"
                + _kv("Affected questions", ag.get("affected_questions", [])) + "\n"
                + _bar("!")
            )
            _esc_result = _handle_escalation(
                _escalation, ag,
                w=w, spark=spark, run_id=run_id,
                catalog=catalog, schema=schema, domain=domain,
                iteration=iteration_counter,
                benchmarks=benchmarks,
                verdict_history=_verdict_history,
                reflection_buffer=reflection_buffer,
                metadata_snapshot=metadata_snapshot,
            )
            logger.info("Escalation result: %s", _esc_result)

            write_stage(
                spark, run_id, f"AG_{ag_id}_ESCALATION", "COMPLETE",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "escalation_type": _escalation,
                    "handled": _esc_result.get("handled", False),
                    "detail": _esc_result.get("detail", {}),
                    "affected_questions": ag.get("affected_questions", [])[:20],
                },
                catalog=catalog, schema=schema,
            )

            _esc_tier = _esc_result.get("detail", {}).get("tier_action", "")

            if _escalation == "flag_for_review" or (
                _escalation == "remove_tvf" and _esc_tier == "flagged_only"
            ):
                reflection_buffer.append(_build_reflection_entry(
                    iteration=iteration_counter, ag_id=ag_id, accepted=False,
                    levers=[], target_objects=ag.get("affected_questions", []),
                    prev_scores=best_scores, new_scores=best_scores,
                    rollback_reason=f"escalation:{_escalation}",
                    patches=[],
                    affected_question_ids=ag.get("affected_questions", []),
                    prev_failure_qids=prev_failure_qids,
                    new_failure_qids=prev_failure_qids,
                    escalation_handled=True,
                ))
                continue

            if _escalation == "gt_repair":
                _gt_repair_corrections = _esc_result.get("detail", {}).get("corrections_applied", 0)
                if _gt_repair_corrections > 0:
                    reflection_buffer.append(_build_reflection_entry(
                        iteration=iteration_counter, ag_id=ag_id, accepted=True,
                        levers=[], target_objects=ag.get("affected_questions", []),
                        prev_scores=best_scores, new_scores=best_scores,
                        rollback_reason=None,
                        patches=[],
                        affected_question_ids=ag.get("affected_questions", []),
                        prev_failure_qids=prev_failure_qids,
                        new_failure_qids=prev_failure_qids,
                        reflection_text=f"GT repair applied {_gt_repair_corrections} benchmark correction(s)",
                        escalation_handled=True,
                    ))
                else:
                    reflection_buffer.append(_build_reflection_entry(
                        iteration=iteration_counter, ag_id=ag_id, accepted=False,
                        levers=[], target_objects=ag.get("affected_questions", []),
                        prev_scores=best_scores, new_scores=best_scores,
                        rollback_reason="escalation:gt_repair (delegated to arbiter)",
                        patches=[],
                        affected_question_ids=ag.get("affected_questions", []),
                        prev_failure_qids=prev_failure_qids,
                        new_failure_qids=prev_failure_qids,
                        escalation_handled=True,
                    ))
                continue

            if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag"):
                _tvf_id = _esc_result.get("detail", {}).get("tvf_id", "")
                _prev_asset = _esc_result.get("detail", {}).get("previous_tvf_asset", {})
                if _tvf_id:
                    _synthetic_patch = {
                        "type": "remove_tvf",
                        "target": _tvf_id,
                        "new_text": "",
                        "old_text": "",
                        "previous_tvf_asset": _prev_asset,
                        "lever": 3,
                        "risk_level": "high",
                        "predicted_affected_questions": len(ag.get("affected_questions", [])),
                        "rationale": (
                            f"TVF {_tvf_id} auto-removed by tiered confidence model "
                            f"(tier={_esc_tier})"
                        ),
                    }
                    _tvf_conf = _esc_result.get("detail", {}).get("confidence", "?")
                    print(
                        _section(f"[{ag_id}] SYNTHETIC remove_tvf PATCH", "!") + "\n"
                        + _kv("TVF", _tvf_id) + "\n"
                        + _kv("Confidence", _tvf_conf) + "\n"
                        + _kv("Tier action", _esc_tier) + "\n"
                        + _bar("!")
                    )
                    _tvf_apply_log = apply_patch_set(
                        w, space_id, [_synthetic_patch], metadata_snapshot,
                        apply_mode=apply_mode,
                        force_apply=True,
                    )
                    _tvf_lever = int(lever_keys[0]) if lever_keys else 3
                    for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):
                        write_patch(
                            spark, run_id, iteration_counter, _tvf_lever, idx,
                            _build_patch_record(entry, _tvf_lever, apply_mode),
                            catalog, schema,
                        )
                    if _tvf_apply_log.get("patch_deployed", False):
                        logger.info("TVF %s removed successfully (tier=%s)", _tvf_id, _esc_tier)
                        metadata_snapshot = _tvf_apply_log.get("post_snapshot", metadata_snapshot)
                    else:
                        logger.warning(
                            "TVF removal patch deploy failed: %s",
                            _tvf_apply_log.get("patch_error", "unknown"),
                        )
                else:
                    logger.warning(
                        "remove_tvf escalation with tier %s but no tvf_id — skipping",
                        _esc_tier,
                    )

        # ── 3B.5: Generate proposals + apply patches ─────────────────
        all_proposals: list[dict] = []
        for lever_key in lever_keys:
            lever_int = int(lever_key)
            levers_attempted.append(lever_int)
            lever_proposals = generate_proposals_from_strategy(
                strategy=strategy,
                action_group=ag,
                metadata_snapshot=metadata_snapshot,
                target_lever=lever_int,
                apply_mode=apply_mode,
                w=w,
                spark=spark,
                catalog=catalog,
                gold_schema=schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            )
            all_proposals.extend(lever_proposals)

        # ── Log proposals ────────────────────────────────────────────
        _n_valid = 0
        _n_failed = 0
        proposal_lines = [_section(f"[{ag_id}] Proposals ({len(all_proposals)} total)", "-"), "|"]
        for pi, p in enumerate(all_proposals, 1):
            cluster_id = p.get("cluster_id", "?")
            ptype = p.get("type", p.get("patch_type", "?"))
            rationale = str(p.get("rationale", ""))
            proposed_value = str(p.get("proposed_value", ""))
            table = p.get("table", "")
            column = p.get("column", "")
            is_failed = "not valid JSON" in rationale or "non-JSON" in rationale.lower()
            if is_failed:
                _n_failed += 1
            else:
                _n_valid += 1
            status = "FAILED (non-JSON)" if is_failed else "OK"

            proposal_lines.append(f"|  Proposal {pi} / {len(all_proposals)}  [{cluster_id}]")
            proposal_lines.append(f"|    {'Type:':<24s} {ptype}")
            proposal_lines.append(f"|    {'Lever:':<24s} {p.get('lever', '?')}")
            if table:
                proposal_lines.append(f"|    {'Table:':<24s} {table}")
            if column:
                proposal_lines.append(f"|    {'Column:':<24s} {column}")
            proposal_lines.append(f"|    {'Rationale:':<24s} {rationale[:200]}")
            _p_col_sect = p.get("column_sections")
            _p_tbl_sect = p.get("table_sections")
            if isinstance(_p_col_sect, dict) and _p_col_sect:
                proposal_lines.append(f"|    Sections proposed:")
                for _sk, _sv in _p_col_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:
                proposal_lines.append(f"|    Table sections proposed:")
                for _sk, _sv in _p_tbl_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif proposed_value:
                _val_preview = proposed_value.replace("\n", "\\n")
                proposal_lines.append(f"|    {'Value (preview):':<24s} {_val_preview[:300]}")
            proposal_lines.append(f"|    {'Status:':<24s} {status}")
            proposal_lines.append("|")

        proposal_lines.append("|  --- Summary ---")
        proposal_lines.append(f"|    {'Valid proposals:':<24s} {_n_valid} of {len(all_proposals)}")
        if _n_failed:
            proposal_lines.append(f"|    {'Failed (non-JSON):':<24s} {_n_failed}")
        proposal_lines.append(f"|    Proceeding with {_n_valid} patch(es)")
        proposal_lines.append(_bar("-"))
        print("\n".join(proposal_lines))

        # ── Provenance log ───────────────────────────────────────────
        _prov_patch_lines = ["\n-- Patch Provenance " + "-" * 58]
        for pi, p in enumerate(all_proposals, 1):
            prov = p.get("provenance", {})
            if not prov:
                continue
            cid = prov.get("cluster_id", "?")
            rc = prov.get("root_cause", "?")
            lv = prov.get("lever", "?")
            ln = prov.get("lever_name", "?")
            pt = prov.get("patch_type", "?")
            _prov_patch_lines.append(f"|  P{pi:03d} [{cid}] lever={lv} ({ln}) type={pt} root_cause={rc}")
        _prov_patch_lines.append("-" * 78)
        print("\n".join(_prov_patch_lines))

        _prop_mappings = [
            {"cluster_id": p.get("cluster_id"), "proposal_id": p.get("proposal_id"), "patch_type": p.get("patch_type")}
            for p in all_proposals if p.get("cluster_id")
        ]
        try:
            update_provenance_proposals(spark, run_id, iteration_counter, _prop_mappings, catalog, schema)
        except Exception:
            logger.debug("Failed to update provenance proposals", exc_info=True)

        if not all_proposals:
            print(_section(f"[{ag_id}] No proposals — SKIPPING iteration", "-"))
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "SKIPPED",
                task_key="lever_loop", iteration=iteration_counter,
                detail={"reason": "no_proposals", "levers": lever_keys},
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[], target_objects=[], prev_scores=best_scores,
                new_scores=best_scores, rollback_reason="no_proposals", patches=[],
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
            ))
            continue

        # ── Apply coordinated patch set ──────────────────────────────
        patches = proposals_to_patches(all_proposals)
        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )

        _primary_lever = int(lever_keys[0]) if lever_keys else 0
        for idx, entry in enumerate(apply_log.get("applied", [])):
            write_patch(
                spark, run_id, iteration_counter, _primary_lever, idx,
                _build_patch_record(entry, _primary_lever, apply_mode),
                catalog, schema,
            )

        _queued = apply_log.get("queued_high", [])
        if _queued:
            from genie_space_optimizer.optimization.state import write_queued_patch
            from genie_space_optimizer.optimization.labeling import flag_for_human_review
            for qentry in _queued:
                _qpatch = qentry.get("patch", {})
                write_queued_patch(
                    spark, run_id, iteration_counter,
                    _qpatch.get("type", ""),
                    _qpatch.get("target", ""),
                    catalog, schema,
                    confidence_tier="queued_high_risk",
                )
            _queued_flag_items = [
                {
                    "question_id": qentry.get("patch", {}).get("target", "unknown"),
                    "question_text": "",
                    "reason": (
                        f"High-risk patch queued for review: "
                        f"{qentry.get('patch', {}).get('type', '')} on "
                        f"{qentry.get('patch', {}).get('target', '')}"
                    ),
                    "iterations_failed": 0,
                    "patches_tried": qentry.get("patch", {}).get("type", ""),
                }
                for qentry in _queued
            ]
            flag_for_human_review(spark, run_id, catalog, schema, domain, _queued_flag_items)
            _qh_lines = [_section(f"[{ag_id}] Queued {len(_queued)} High-Risk Patch(es) for Human Review", "!")]
            for qi, qe in enumerate(_queued, 1):
                _qp = qe.get("patch", {})
                _qh_lines.append(
                    _kv(f"  [{qi}]", f"{_qp.get('type', '?')} \u2192 {_qp.get('target', '?')}")
                )
            _qh_lines.append(_bar("!"))
            print("\n".join(_qh_lines))

        if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):
            _pe = apply_log.get("patch_error", "unknown")
            print(
                _section(f"[{ag_id}] PATCH DEPLOY FAILED", "!") + "\n"
                + _kv("Error", str(_pe)[:300]) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_PATCH_FAILED", "ERROR",
                task_key="lever_loop", iteration=iteration_counter,
                error_message=str(_pe)[:500],
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys], target_objects=[],
                prev_scores=best_scores, new_scores=best_scores,
                rollback_reason=f"patch_deploy_failed: {str(_pe)[:100]}",
                patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
            ))
            continue

        # ── Applied Patches Detail ───────────────────────────────────
        _applied = apply_log.get("applied", [])
        if _applied:
            _ap_lines = [_section(f"[{ag_id}] Applied {len(_applied)} Patch(es)", "=")]
            for ai, aentry in enumerate(_applied, 1):
                _ap = aentry.get("patch", {})
                _aa = aentry.get("action", {})
                _ap_lines.append(_fmt_patch(ai, _ap, _aa))
            _ap_lines.append(_bar("="))
            print("\n".join(_ap_lines))

        _dropped = apply_log.get("dropped_patches", [])
        if _dropped:
            _dp_lines = [_section(f"[{ag_id}] Dropped {len(_dropped)} Join Spec Patch(es)", "!")]
            for di, dp in enumerate(_dropped, 1):
                _dp_lines.append(
                    f"|  [{di}] {dp.get('type', '?')}: "
                    f"{dp.get('left_table', '?')} <-> {dp.get('right_table', '?')}"
                )
            _dp_lines.append(
                "|  Reason: join spec PATCH failed; remaining patches deployed successfully"
            )
            _dp_lines.append(_bar("!"))
            print("\n".join(_dp_lines))

        # ── 3B.6: Three-gate eval ───────────────────────────────────
        gate_result = _run_gate_checks(
            spark=spark,
            w=w,
            run_id=run_id,
            space_id=space_id,
            exp_name=exp_name,
            domain=domain,
            iteration_counter=iteration_counter,
            ag_id=ag_id,
            benchmarks=benchmarks,
            proposals=all_proposals,
            patches=patches,
            apply_log=apply_log,
            clusters=clusters,
            metadata_snapshot=metadata_snapshot,
            predict_fn=predict_fn,
            scorers=scorers,
            prev_model_id=prev_model_id,
            best_scores=best_scores,
            best_accuracy=best_accuracy,
            catalog=catalog,
            schema=schema,
            reference_sqls=reference_sqls,
            noise_floor=noise_floor,
            affected_question_ids=set(ag.get("affected_questions", [])),
            lever_keys=lever_keys,
        )

        # ── 3B.7: Accept or rollback ────────────────────────────────
        _target_objects = [
            p.get("target_object", "") for p in patches if p.get("target_object")
        ]

        if not gate_result.get("passed"):
            reason = gate_result.get("rollback_reason", "unknown")
            rollback(apply_log, w, space_id, metadata_snapshot)
            mark_patches_rolled_back(
                spark, run_id, iteration_counter, reason, catalog, schema,
            )
            ags_rolled_back.append(ag_id)
            for lk in lever_keys:
                levers_rolled_back.append(int(lk))
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "ROLLED_BACK",
                task_key="lever_loop", iteration=iteration_counter,
                detail={"reason": reason, "levers": lever_keys},
                catalog=catalog, schema=schema,
            )
            _failed_eval = gate_result.get("failed_eval_result", {})
            _fail_tmap = _failed_eval.get("trace_map", {})
            _fail_qids = set(_failed_eval.get("failure_question_ids", []))
            all_failure_question_ids.extend(_fail_qids)
            for qid, tid in _fail_tmap.items():
                question_trace_map.setdefault(qid, []).append(tid)
                if qid in _fail_qids:
                    all_failure_trace_ids.append(tid)
                elif "regressions" in gate_result:
                    all_regression_trace_ids.append(tid)
            _fail_run_id = _failed_eval.get("mlflow_run_id") or _failed_eval.get("run_id", "")
            if _fail_run_id:
                all_eval_mlflow_run_ids.append(_fail_run_id)

            _failed_scores = gate_result.get("full_scores", best_scores)
            _rb_fail_qids = set(
                gate_result.get("failed_eval_result", {}).get("failure_question_ids", [])
            )
            _affected_set = set(ag.get("affected_questions", []))
            _any_target_improved = bool(
                _affected_set and prev_failure_qids
                and (_affected_set & prev_failure_qids) - (_rb_fail_qids or prev_failure_qids)
            )
            _rb_refinement = "in_plan" if _any_target_improved else "out_of_plan"
            _rb_acc_delta = (
                sum(_failed_scores.values()) / max(len(_failed_scores), 1)
                - sum(best_scores.values()) / max(len(best_scores), 1)
            )
            _regressions = gate_result.get("regressions", [])
            _rb_patch_types = sorted({p.get("patch_type", "?") for p in patches})
            if _any_target_improved and _regressions:
                _rb_reflection = (
                    f"Rollback ({_rb_refinement}): patches ({', '.join(_rb_patch_types)}) "
                    f"improved some target questions but caused regressions on "
                    f"{len(_regressions)} other(s). Narrower scope on the same lever may help."
                )
            else:
                _rb_reflection = (
                    f"Rollback ({_rb_refinement}): {ag.get('root_cause_summary', 'unknown root cause')} "
                    f"was not resolved by {', '.join(_rb_patch_types)} "
                    f"(accuracy delta {_rb_acc_delta:+.1f}%). "
                    f"A different lever or escalation is needed."
                )
            reflection = _build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys],
                target_objects=_target_objects,
                prev_scores=best_scores, new_scores=_failed_scores,
                rollback_reason=reason, patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=_rb_fail_qids or prev_failure_qids,
                reflection_text=_rb_reflection,
                refinement_mode=_rb_refinement,
            )
            reflection_buffer.append(reflection)
            try:
                update_iteration_reflection(
                    spark, run_id, iteration_counter, reflection,
                    catalog=catalog, schema=schema, eval_scope="full",
                )
            except Exception:
                logger.debug("Failed to persist reflection for rollback iter %d", iteration_counter, exc_info=True)
            for p in patches:
                ft = p.get("patch_type", "")
                tgt = p.get("target_object", "")
                if ft and tgt:
                    tried_patches.add((ft, tgt))
            source_cids = set(ag.get("source_cluster_ids", []))
            for c in clusters:
                cid = c.get("cluster_id", "")
                if source_cids and cid not in source_cids:
                    continue
                rc_ft = c.get("asi_failure_type") or c.get("root_cause", "other")
                rc_blame = c.get("asi_blame_set") or ""
                if rc_ft:
                    tried_root_causes.add((rc_ft, rc_blame))
            continue

        # ── Accept action group ──────────────────────────────────────
        ags_accepted.append(ag_id)
        for lk in lever_keys:
            levers_accepted.append(int(lk))

        full_scores = gate_result["full_scores"]
        full_accuracy = gate_result["full_accuracy"]
        new_model_id = gate_result["new_model_id"]
        full_result = gate_result["full_result"]
        _last_full_mlflow_run_id = full_result.get("mlflow_run_id") or full_result.get("run_id", "")

        _full_trace_map = full_result.get("trace_map", {})
        _full_failures = set(full_result.get("failure_question_ids", []))
        all_failure_question_ids.extend(_full_failures)
        for qid, tid in _full_trace_map.items():
            question_trace_map.setdefault(qid, []).append(tid)
            if qid in _full_failures:
                all_failure_trace_ids.append(tid)
        _full_run_id = full_result.get("mlflow_run_id") or full_result.get("run_id", "")
        if _full_run_id:
            all_eval_mlflow_run_ids.append(_full_run_id)

        try:
            from genie_space_optimizer.optimization.evaluation import log_expectations_on_traces
            log_expectations_on_traces(full_result)
        except Exception:
            logger.debug("Failed to log expectations on iter %d traces", iteration_counter, exc_info=True)

        try:
            log_judge_verdicts_on_traces(full_result)
        except Exception:
            logger.debug("Failed to log judge verdicts on iter %d traces", iteration_counter, exc_info=True)

        try:
            _persist_text, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )
            if _persist_data:
                log_persistence_context_on_traces(full_result, _persist_data)
        except Exception:
            logger.debug("Failed to log persistence context on iter %d traces", iteration_counter, exc_info=True)

        lever_changes.append({
            "lever": ag_id,
            "lever_name": f"AG {ag_id}: {ag.get('root_cause_summary', '')[:60]}",
            "patches": [
                {"change": p.get("change_description", ""), "patch_type": p.get("patch_type", "")}
                for p in all_proposals
            ],
            "accuracy_delta": full_accuracy - best_accuracy,
        })

        _accepted_fail_qids = set(full_result.get("failure_question_ids", []))
        _acc_delta = full_accuracy - best_accuracy
        _acc_patch_types = sorted({p.get("patch_type", "?") for p in patches})
        _acc_reflection = (
            f"Accepted: {ag.get('root_cause_summary', 'improvement')} resolved via "
            f"{', '.join(_acc_patch_types)}. "
            f"Accuracy improved by {_acc_delta:+.1f}% "
            f"affecting {len(ag.get('affected_questions', []))} question(s)."
        )
        reflection = _build_reflection_entry(
            iteration=iteration_counter, ag_id=ag_id, accepted=True,
            levers=[int(lk) for lk in lever_keys],
            target_objects=_target_objects,
            prev_scores=best_scores, new_scores=full_scores,
            rollback_reason=None, patches=patches,
            affected_question_ids=ag.get("affected_questions", []),
            prev_failure_qids=prev_failure_qids,
            new_failure_qids=_accepted_fail_qids,
            reflection_text=_acc_reflection,
        )
        reflection_buffer.append(reflection)
        try:
            update_iteration_reflection(
                spark, run_id, iteration_counter, reflection,
                catalog=catalog, schema=schema, eval_scope="full",
            )
        except Exception:
            logger.debug("Failed to persist reflection for accepted iter %d", iteration_counter, exc_info=True)
        prev_failure_qids = _accepted_fail_qids

        if _acc_delta >= 1.0:
            skill_exemplars.append({
                "root_cause": ag.get("root_cause_summary", ""),
                "lever_pattern": sorted(ag.get("lever_directives", {}).keys()),
                "patch_types": [p.get("patch_type") for p in patches[:5] if p.get("patch_type") is not None],
                "accuracy_gain": round(_acc_delta, 1),
            })

        best_scores = full_scores
        best_accuracy = full_accuracy
        best_model_id = new_model_id
        best_iteration = iteration_counter
        prev_scores = full_scores
        prev_model_id = new_model_id

        new_refs = extract_reference_sqls(full_result)
        if new_refs:
            reference_sqls.update(new_refs)
        new_hashes = extract_reference_result_hashes(full_result)
        if new_hashes:
            reference_result_hashes.update(new_hashes)

        update_run_status(
            spark, run_id, catalog, schema,
            best_iteration=best_iteration,
            best_accuracy=best_accuracy,
            best_model_id=best_model_id,
        )

        post_instructions = _get_general_instructions(
            apply_log.get("post_snapshot", metadata_snapshot)
        )
        if post_instructions:
            register_instruction_version(
                uc_schema=f"{catalog}.{schema}",
                space_id=space_id,
                instruction_text=post_instructions,
                run_id=run_id,
                lever=0,
                iteration=iteration_counter,
                accuracy=best_accuracy,
                domain=domain,
            )

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "COMPLETE",
            task_key="lever_loop", iteration=iteration_counter,
            detail={
                "accuracy": full_accuracy,
                "accepted": True,
                "patches_applied": len(apply_log.get("applied", [])),
                "levers": lever_keys,
            },
            catalog=catalog, schema=schema,
        )

        metadata_snapshot = apply_log.get("post_snapshot", metadata_snapshot)

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "COMPLETE",
        task_key="lever_loop",
        detail={
            "levers_attempted": levers_attempted,
            "levers_accepted": levers_accepted,
            "levers_rolled_back": levers_rolled_back,
            "reflection_buffer": reflection_buffer,
        },
        catalog=catalog, schema=schema,
    )

    # ── End-of-Run Summary ─────────────────────────────────────────
    _summary = [_section("OPTIMIZATION RUN SUMMARY", "=")]
    _summary.append(_kv("Space ID", space_id))
    _summary.append(_kv("Run ID", run_id))
    _summary.append(_kv("Baseline accuracy", f"{baseline_accuracy:.1f}%"))
    _summary.append(_kv("Final accuracy", f"{best_accuracy:.1f}%"))
    _delta = best_accuracy - baseline_accuracy
    _summary.append(_kv("Net improvement", f"{'+' if _delta >= 0 else ''}{_delta:.1f}%"))
    _summary.append(_kv("Iterations", iteration_counter))
    _summary.append(_kv("Best iteration", best_iteration))
    _summary.append("|")

    # Proactive changes
    _summary.append("|  --- Proactive Changes (pre-lever-loop) ---")
    _desc_enriched = enrichment_result.get("total_enriched", 0)
    _tbl_desc_enriched = enrichment_result.get("tables_enriched", 0)
    _joins_applied = join_result.get("total_applied", 0)
    _desc_gen = meta_result.get("description_generated", False)
    _sq_gen = meta_result.get("questions_generated", False)
    _sq_count = meta_result.get("questions_count", 0)
    _summary.append(_kv("Column descriptions added", _desc_enriched))
    _summary.append(_kv("Table descriptions added", _tbl_desc_enriched))
    _summary.append(_kv("Join specs discovered", _joins_applied))
    _summary.append(_kv("Space description", "generated" if _desc_gen else "unchanged"))
    _summary.append(_kv("Sample questions", f"generated ({_sq_count})" if _sq_gen else "unchanged"))
    _instr_seeded = instruction_result.get("instructions_seeded", False)
    _instr_chars = instruction_result.get("instruction_chars", 0)
    _summary.append(_kv("Instructions seeded", f"generated ({_instr_chars} chars)" if _instr_seeded else "unchanged"))
    _mined_count = len(mined_example_proposals) if mined_example_proposals else 0
    _summary.append(_kv("Benchmark examples mined", _mined_count))
    _summary.append("|")

    # Lever loop changes
    _summary.append("|  --- Lever Loop Changes ---")
    _summary.append(_kv("Action groups attempted", len(ags_attempted)))
    _summary.append(_kv("Action groups accepted", len(ags_accepted)))
    _summary.append(_kv("Action groups rolled back", len(ags_rolled_back)))
    _summary.append(_kv("Levers used", sorted(set(levers_attempted)) if levers_attempted else "none"))
    if lever_changes:
        _summary.append("|")
        for lc in lever_changes:
            _delta_str = f"{lc['accuracy_delta']:+.1f}%"
            _summary.append(f"|  {lc['lever_name']}")
            _summary.append(f"|      Accuracy delta: {_delta_str}")
            for p in lc.get("patches", []):
                _ptype = _PATCH_TYPE_LABELS.get(p.get("patch_type", ""), p.get("patch_type", ""))
                _change = p.get("change", "")[:80]
                _summary.append(f"|      - {_ptype}: {_change}")
    elif not ags_accepted:
        _summary.append(_kv("Status", "No lever loop changes were accepted"))

    _summary.append("|")
    _summary.append("|  --- Final Scores ---")
    for sname, sval in sorted(best_scores.items()):
        _summary.append(f"|  {sname + ':':<28s} {sval:.1f}")
    _summary.append(_bar("="))
    print("\n".join(_summary))

    return {
        "scores": best_scores,
        "accuracy": best_accuracy,
        "model_id": best_model_id,
        "iteration_counter": iteration_counter,
        "best_iteration": best_iteration,
        "levers_attempted": levers_attempted,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "question_trace_map": question_trace_map,
        "reflection_buffer": reflection_buffer,
        "all_eval_mlflow_run_ids": list(dict.fromkeys(all_eval_mlflow_run_ids)),
        "all_failure_trace_ids": list(dict.fromkeys(all_failure_trace_ids)),
        "all_regression_trace_ids": list(dict.fromkeys(all_regression_trace_ids)),
        "all_failure_question_ids": list(dict.fromkeys(all_failure_question_ids)),
        "_debug_ref_sqls_count": len(reference_sqls),
        "_debug_failure_rows_loaded": len(_get_failure_rows(spark, run_id, catalog, schema)),
    }


# ── Stage 4: FINALIZE ───────────────────────────────────────────────


def _run_finalize(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    exp_name: str,
    prev_scores: dict[str, float],
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
    run_repeatability: bool = True,
    benchmarks: list[dict] | None = None,
    thresholds: dict[str, float] | None = None,
    finalize_timeout_seconds: int = FINALIZE_TIMEOUT_SECONDS,
    heartbeat_interval_seconds: int = FINALIZE_HEARTBEAT_SECONDS,
    question_trace_map: dict[str, list[str]] | None = None,
    reflection_buffer: list[dict] | None = None,
    all_eval_mlflow_run_ids: list[str] | None = None,
    all_failure_trace_ids: list[str] | None = None,
    all_regression_trace_ids: list[str] | None = None,
    all_failure_question_ids: list[str] | None = None,
    max_iterations: int | None = None,
    deploy_target: str | None = None,
) -> dict:
    """Stage 4: Repeatability test, promote model, generate report.

    Adds heartbeat events + a soft timeout so long-running finalization
    remains observable and fails with an explicit terminal reason.
    """
    thresholds = thresholds or DEFAULT_THRESHOLDS
    max_iterations = max_iterations or MAX_ITERATIONS
    finalize_timeout_seconds = max(1, int(finalize_timeout_seconds))
    heartbeat_interval_seconds = max(5, int(heartbeat_interval_seconds))
    started_monotonic = time.monotonic()
    last_heartbeat = 0.0
    heartbeat_count = 0
    current_phase = "initializing"

    def _elapsed_seconds() -> float:
        return time.monotonic() - started_monotonic

    def _check_timeout(phase: str) -> None:
        elapsed = _elapsed_seconds()
        if elapsed > finalize_timeout_seconds:
            raise TimeoutError(
                f"Finalize exceeded timeout ({finalize_timeout_seconds}s) "
                f"during {phase} after {elapsed:.1f}s",
            )

    def _emit_heartbeat(
        phase: str,
        *,
        detail: dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        nonlocal last_heartbeat, heartbeat_count, current_phase
        current_phase = phase
        now = time.monotonic()
        if not force and (now - last_heartbeat) < heartbeat_interval_seconds:
            return
        last_heartbeat = now
        heartbeat_count += 1

        heartbeat_detail: dict[str, Any] = {
            "phase": phase,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
            "timeout_seconds": finalize_timeout_seconds,
        }
        if detail:
            heartbeat_detail.update(detail)

        try:
            # Touch run.updated_at so stale-state reconciliation doesn't mark finalize as dead.
            update_run_status(spark, run_id, catalog, schema)
            write_stage(
                spark, run_id, "FINALIZE_HEARTBEAT", "STARTED",
                task_key="finalize",
                detail=heartbeat_detail,
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.warning(
                "Failed to persist finalize heartbeat for run %s",
                run_id,
                exc_info=True,
            )

    write_stage(
        spark, run_id, "FINALIZE_STARTED", "STARTED",
        task_key="finalize",
        detail={
            "timeout_seconds": finalize_timeout_seconds,
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
        },
        catalog=catalog, schema=schema,
    )
    _emit_heartbeat("finalize_started", force=True)

    question_trace_map = question_trace_map or {}
    reflection_buffer = reflection_buffer or []
    all_eval_mlflow_run_ids = list(all_eval_mlflow_run_ids or [])
    all_failure_trace_ids = list(all_failure_trace_ids or [])
    all_regression_trace_ids = list(all_regression_trace_ids or [])
    all_failure_question_ids = list(all_failure_question_ids or [])

    # Pre-seed question_trace_map from ALL eval runs (incl. baseline) so that
    # persistent-failure questions always have trace IDs for tagging and session
    # population, even when the lever loop never re-evaluated them.
    if all_eval_mlflow_run_ids:
        try:
            import mlflow
            from genie_space_optimizer.optimization.labeling import _extract_question_id
            for _seed_rid in dict.fromkeys(all_eval_mlflow_run_ids):
                try:
                    _seed_traces = mlflow.search_traces(run_id=_seed_rid)
                    if _seed_traces is not None and len(_seed_traces) > 0 and "request" in _seed_traces.columns:
                        for _, row in _seed_traces.iterrows():
                            _qid = _extract_question_id(row.get("request"))
                            _tid = row.get("trace_id", "")
                            if _qid and _tid:
                                question_trace_map.setdefault(_qid, []).append(_tid)
                except Exception as _seed_exc:
                    logger.debug("Failed to seed trace map from eval run %s: %s", _seed_rid, _seed_exc)
            # Deduplicate trace IDs per question
            for _qid in question_trace_map:
                question_trace_map[_qid] = list(dict.fromkeys(question_trace_map[_qid]))
            logger.info(
                "Pre-seeded question_trace_map with %d questions from %d eval runs",
                len(question_trace_map), len(all_eval_mlflow_run_ids),
            )
        except Exception as _seed_err:
            logger.warning("Failed to pre-seed question_trace_map: %s", _seed_err, exc_info=True)

    terminal_reason = ""
    repeatability_pct = 0.0
    try:
        # ── Phase 0: Split benchmarks ──
        train_benchmarks = [b for b in (benchmarks or []) if b.get("split") != "held_out"]
        held_out_benchmarks = [b for b in (benchmarks or []) if b.get("split") == "held_out"]

        # ── Phase 1: Repeatability Testing ──
        _lines = [_section("FINALIZE — REPEATABILITY TESTING", "-")]
        _lines.append(_kv("Run repeatability", run_repeatability))
        _lines.append(_kv("Train benchmarks", len(train_benchmarks)))
        _lines.append(_kv("Held-out benchmarks", len(held_out_benchmarks)))
        _lines.append(_bar("-"))
        print("\n".join(_lines))

        rep_results: list[dict] = []
        held_out_accuracy: float | None = None
        _check_timeout("pre_repeatability")
        if run_repeatability and train_benchmarks:
            write_stage(
                spark, run_id, "REPEATABILITY_TEST", "STARTED",
                task_key="finalize", catalog=catalog, schema=schema,
            )
            _emit_heartbeat(
                "repeatability_test",
                force=True,
                detail={"benchmark_count": len(train_benchmarks), "runs": FINALIZE_REPEATABILITY_PASSES},
            )

            uc_schema = f"{catalog}.{schema}"
            latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
            reference_sqls: dict[str, str] = {}
            reference_result_hashes: dict[str, str] = {}
            if latest_iter:
                rows_json = latest_iter.get("rows_json")
                if isinstance(rows_json, list):
                    _rows_payload = {"rows": rows_json}
                    reference_sqls = extract_reference_sqls(_rows_payload)
                    reference_result_hashes = extract_reference_result_hashes(_rows_payload)
                elif isinstance(rows_json, str):
                    try:
                        _rows_payload = {"rows": json.loads(rows_json)}
                        reference_sqls = extract_reference_sqls(_rows_payload)
                        reference_result_hashes = extract_reference_result_hashes(_rows_payload)
                    except (json.JSONDecodeError, TypeError):
                        pass
            if not reference_sqls and train_benchmarks:
                logger.warning("No reference SQLs from iterations — extracting from benchmarks")
                for b in train_benchmarks:
                    qid = b.get("id", "")
                    sql = b.get("expected_sql", "")
                    if qid and sql:
                        reference_sqls[qid] = sql
            logger.info(
                "Repeatability: %d reference SQLs, %d result hashes loaded",
                len(reference_sqls),
                len(reference_result_hashes),
            )
            print(
                f"  Repeatability: {len(reference_sqls)} reference SQLs, "
                f"{len(reference_result_hashes)} result hashes loaded"
            )

            _ensure_sql_context(spark, catalog, schema)
            predict_fn = make_predict_fn(
                w, space_id, spark, catalog, schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            )

            rep_pcts: list[float] = []
            try:
                for rep_run_idx in range(1, FINALIZE_REPEATABILITY_PASSES + 1):
                    _check_timeout(f"repeatability_run_{rep_run_idx}")
                    _emit_heartbeat(
                        f"repeatability_run_{rep_run_idx}",
                        force=True,
                        detail={"run": rep_run_idx, "of": FINALIZE_REPEATABILITY_PASSES},
                    )
                    rep_result = run_repeatability_evaluation(
                        space_id=space_id,
                        experiment_name=exp_name,
                        iteration=iteration_counter,
                        benchmarks=train_benchmarks,
                        domain=domain,
                        reference_sqls=reference_sqls,
                        predict_fn=predict_fn,
                        spark=spark,
                        catalog=catalog,
                        gold_schema=schema,
                        uc_schema=uc_schema,
                        model_id=prev_model_id,
                        run_label=f"final_{rep_run_idx}",
                        reference_result_hashes=reference_result_hashes,
                    )
                    rep_results.append(rep_result)
                    rep_pcts.append(rep_result.get("repeatability_pct", 0.0))
                    logger.info(
                        "Repeatability run %d/%d: %.1f%%",
                        rep_run_idx,
                        FINALIZE_REPEATABILITY_PASSES,
                        rep_pcts[-1],
                    )

                _check_timeout("post_repeatability")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "average_pct": repeatability_pct,
                        "per_run_pcts": rep_pcts,
                        "total_questions": len(train_benchmarks),
                    },
                    catalog=catalog, schema=schema,
                )
            except TimeoutError as exc:
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    detail={"terminal_reason": "finalize_timeout"},
                    error_message=str(exc)[:500],
                    catalog=catalog, schema=schema,
                )
                raise
            except Exception:
                logger.exception("Repeatability evaluation failed")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    error_message="Repeatability evaluation exception",
                    detail={"partial_pcts": rep_pcts},
                    catalog=catalog, schema=schema,
                )
        else:
            _emit_heartbeat(
                "repeatability_skipped",
                force=True,
                detail={"reason": "disabled_or_no_benchmarks"},
            )

        # ── Phase 2: Human Review Session ──
        _rep_lines = [_section("FINALIZE — REPEATABILITY RESULTS", "-")]
        _rep_lines.append(_kv("Average repeatability", f"{repeatability_pct:.1f}%"))
        _rep_lines.append(_kv("Passes completed", len(rep_results)))
        _rep_lines.append(_bar("-"))
        print("\n".join(_rep_lines))

        # ── Phase 1b: Held-Out Generalization Check ──
        if held_out_benchmarks:
            try:
                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "STARTED",
                    task_key="finalize", catalog=catalog, schema=schema,
                )
                _emit_heartbeat(
                    "held_out_eval", force=True,
                    detail={"held_out_count": len(held_out_benchmarks)},
                )
                _check_timeout("held_out_eval")

                _ensure_sql_context(spark, catalog, schema)
                ho_predict_fn = make_predict_fn(
                    w, space_id, spark, catalog, schema,
                    warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                )
                ho_scorers = make_all_scorers(w, spark, catalog, schema)

                held_out_result = run_evaluation(
                    space_id, exp_name, iteration_counter, held_out_benchmarks,
                    domain, prev_model_id, "held_out",
                    ho_predict_fn, ho_scorers,
                    spark=spark, w=w, catalog=catalog, gold_schema=schema,
                    uc_schema=f"{catalog}.{schema}",
                )

                write_iteration(
                    spark, run_id, iteration_counter, held_out_result,
                    catalog=catalog, schema=schema,
                    eval_scope="held_out", model_id=prev_model_id,
                )

                held_out_accuracy = held_out_result.get("overall_accuracy", 0.0)
                train_accuracy = prev_scores.get(
                    "genie_correct",
                    prev_scores.get("overall_accuracy", 0.0),
                )
                delta = train_accuracy - held_out_accuracy
                _ho_lines = [_section("FINALIZE — HELD-OUT GENERALIZATION CHECK", "-")]
                _ho_lines.append(_kv("Train accuracy", f"{train_accuracy:.1f}%"))
                _ho_lines.append(_kv("Held-out accuracy", f"{held_out_accuracy:.1f}%"))
                _ho_lines.append(_kv("Delta", f"{delta:+.1f} pp"))
                _ho_lines.append(_kv("Held-out questions", len(held_out_benchmarks)))
                if delta > 15.0:
                    _ho_lines.append(_kv("Warning", "Possible instruction overfitting (>15pp gap)"))
                _ho_lines.append(_bar("-"))
                print("\n".join(_ho_lines))

                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "held_out_accuracy": held_out_accuracy,
                        "train_accuracy": train_accuracy,
                        "delta_pp": round(delta, 1),
                        "held_out_count": len(held_out_benchmarks),
                    },
                    catalog=catalog, schema=schema,
                )

                _best_eval_run_id = (latest_iter or {}).get("mlflow_run_id", "")
                if _best_eval_run_id:
                    try:
                        from mlflow.tracking import MlflowClient as _HoMlflowClient
                        _ho_client = _HoMlflowClient()
                        _ho_client.log_metric(_best_eval_run_id, "held_out_accuracy", held_out_accuracy)
                        _ho_client.log_metric(_best_eval_run_id, "held_out_count", float(len(held_out_benchmarks)))
                        logger.info(
                            "Logged held-out metrics to eval run %s: accuracy=%.1f%%, count=%d",
                            _best_eval_run_id, held_out_accuracy, len(held_out_benchmarks),
                        )
                    except Exception:
                        logger.debug("Failed to log held-out metrics to eval run", exc_info=True)
            except TimeoutError:
                raise
            except Exception:
                logger.exception("Held-out evaluation failed — continuing")
                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "FAILED",
                    task_key="finalize",
                    error_message="Held-out evaluation exception",
                    catalog=catalog, schema=schema,
                )
        else:
            write_stage(
                spark, run_id, "HELD_OUT_EVAL", "SKIPPED",
                task_key="finalize",
                detail={"reason": "no_held_out_benchmarks"},
                catalog=catalog, schema=schema,
            )

        _review_lines = [_section("FINALIZE — HUMAN REVIEW SESSION", "-")]
        print("\n".join(_review_lines))

        _check_timeout("human_review_session")
        _emit_heartbeat("human_review_session", force=True)
        session_info: dict = {}
        try:
            for _rr in (rep_results if run_repeatability and benchmarks else []):
                _rr_tmap = _rr.get("trace_map", {})
                for qid, tid in _rr_tmap.items():
                    question_trace_map.setdefault(qid, []).append(tid)
                _rr_run_id = _rr.get("mlflow_run_id") or _rr.get("run_id", "")
                if _rr_run_id:
                    all_eval_mlflow_run_ids.append(_rr_run_id)

            _verdict_history = _build_verdict_history(spark, run_id, catalog, schema)
            _persist_text, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )

            from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

            persistent_question_ids = [
                qid for qid, ctx in _persist_data.items()
                if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
            ]

            if _persist_data:
                log_persistence_context_on_traces(
                    {}, _persist_data, extra_trace_map=question_trace_map,
                )
            if reflection_buffer and question_trace_map:
                log_patch_history_on_traces(
                    question_trace_map, reflection_buffer,
                    persistent_question_ids=set(persistent_question_ids) if persistent_question_ids else None,
                )

            _persistent_items = [
                {
                    "question_id": qid,
                    "question_text": ctx.get("question_text", ""),
                    "reason": ctx["classification"],
                    "iterations_failed": ctx["fail_count"],
                    "patches_tried": str(ctx.get("patches_tried", [])),
                }
                for qid, ctx in _persist_data.items()
                if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
            ]
            if _persistent_items:
                from genie_space_optimizer.optimization.labeling import flag_for_human_review
                flag_for_human_review(spark, run_id, catalog, schema, domain, _persistent_items)
                _persistent_qids = {item["question_id"] for item in _persistent_items}
                all_failure_question_ids.extend(
                    qid for qid in _persistent_qids if qid not in set(all_failure_question_ids)
                )

            _session_trace_ids = [
                question_trace_map[qid][-1]
                for qid in persistent_question_ids
                if qid in question_trace_map
            ] if persistent_question_ids else []
            _session_question_ids = [
                qid for qid in persistent_question_ids if qid in question_trace_map
            ] if persistent_question_ids else []

            if _session_question_ids:
                from genie_space_optimizer.optimization.labeling import create_review_session
                session_info = create_review_session(
                    run_id=run_id,
                    domain=domain,
                    experiment_name=exp_name,
                    uc_schema=f"{catalog}.{schema}",
                    failure_trace_ids=list(dict.fromkeys(_session_trace_ids)),
                    regression_trace_ids=[],
                    eval_mlflow_run_ids=list(dict.fromkeys(all_eval_mlflow_run_ids)),
                    failure_question_ids=list(dict.fromkeys(_session_question_ids)),
                    flagged_trace_ids=list(dict.fromkeys(_session_trace_ids)),
                )
                _sname = session_info.get("session_name", "")
                _srun = session_info.get("session_run_id", "")
                _surl = session_info.get("session_url", "")
                if _sname:
                    print(
                        f"\n[MLflow Review] Labeling session created for human review:\n"
                        f"  Name: {_sname}\n"
                        f"  Traces: {session_info.get('trace_count', 0)}\n"
                        f"  Persistent questions: {len(_session_question_ids)}\n"
                    )
                    if _surl:
                        print(f"  URL: {_surl}\n")
                if _sname or _srun:
                    update_run_status(
                        spark, run_id, catalog, schema,
                        labeling_session_name=_sname,
                        labeling_session_run_id=_srun,
                        labeling_session_url=_surl,
                    )
            else:
                print("\n[MLflow Review] No persistent failures — skipping labeling session creation\n")
        except Exception as exc:
            print(f"[Labeling] Failed to create post-repeatability review session: {exc}")
            logger.warning("Failed to create post-repeatability review session", exc_info=True)

        # ── Phase 3: Model Promotion & Report ──
        _promo_lines = [_section("FINALIZE — MODEL PROMOTION & REPORT", "-")]
        print("\n".join(_promo_lines))

        _check_timeout("promote_best_model")
        _emit_heartbeat("promote_best_model", force=True)
        promoted_model = promote_best_model(spark, run_id, catalog, schema)

        from genie_space_optimizer.optimization.models import register_uc_model
        uc_result = register_uc_model(spark, run_id, catalog, schema, ws=w, deploy_target=deploy_target)

        if uc_result:
            _uc_lines = [_section("FINALIZE — UC MODEL REGISTRATION", "-")]
            _uc_lines.append(_kv("UC Model", uc_result["uc_model_name"]))
            _uc_lines.append(_kv("Version", uc_result["version"]))
            _uc_lines.append(_kv("Champion", "YES" if uc_result["promoted_to_champion"] else "NO (existing champion is better)"))
            if uc_result.get("comparison"):
                for _judge, _cmp in uc_result["comparison"].items():
                    _uc_lines.append(_kv(f"  {_judge}", f"new={_cmp['new']:.1f} vs existing={_cmp['existing']:.1f}"))
            _uc_lines.append(_bar("-"))
            print("\n".join(_uc_lines))

        _check_timeout("generate_report")
        _emit_heartbeat("generate_report", force=True)
        report_path = generate_report(spark, run_id, domain, catalog, schema)

        # Publish benchmarks to the Genie Space's native benchmarks section
        # so they appear in the Genie UI and can be used for UI-based eval runs.
        benchmark_publish_count = 0
        _check_timeout("publish_benchmarks")
        _emit_heartbeat("publish_benchmarks", force=True)
        if benchmarks:
            try:
                from genie_space_optimizer.common.genie_client import (
                    publish_benchmarks_to_genie_space,
                )

                benchmark_publish_count = publish_benchmarks_to_genie_space(
                    w, space_id, benchmarks,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "COMPLETE",
                    task_key="finalize",
                    detail={"published_count": benchmark_publish_count},
                    catalog=catalog, schema=schema,
                )
            except Exception:
                logger.warning(
                    "Failed to publish benchmarks to Genie space %s — "
                    "optimization results are still valid",
                    space_id,
                    exc_info=True,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "FAILED",
                    task_key="finalize",
                    error_message="Benchmark publish failed (non-fatal)",
                    catalog=catalog, schema=schema,
                )
        else:
            write_stage(
                spark, run_id, "BENCHMARK_PUBLISH", "SKIPPED",
                task_key="finalize",
                detail={"reason": "no_benchmarks_available"},
                catalog=catalog, schema=schema,
            )

        _promo_result_lines = [_section("FINALIZE — PROMOTION RESULTS", "-")]
        _promo_result_lines.append(_kv("Promoted model", promoted_model or "(none)"))
        _promo_result_lines.append(_kv("Report path", report_path or "(none)"))
        _promo_result_lines.append(_kv("Benchmarks published", benchmark_publish_count))
        _promo_result_lines.append(_bar("-"))
        print("\n".join(_promo_result_lines))

        # ── Phase 4: Terminal Status Resolution ──
        _term_lines = [_section("FINALIZE — TERMINAL STATUS RESOLUTION", "-")]
        print("\n".join(_term_lines))

        _check_timeout("resolve_terminal_status")
        converged = all_thresholds_met(prev_scores, thresholds)
        if converged:
            status = "CONVERGED"
            reason = "threshold_met"
        elif iteration_counter >= max_iterations:
            status = "MAX_ITERATIONS"
            reason = "max_iterations"
        else:
            status = "STALLED"
            reason = "no_further_improvement"

        terminal_reason = f"finalize_completed:{reason}"
        update_run_status(
            spark, run_id, catalog, schema,
            status=status,
            convergence_reason=reason,
            best_repeatability=repeatability_pct,
        )

        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "COMPLETE",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "status": status,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "COMPLETE",
            task_key="finalize",
            detail={
                "status": status,
                "report_path": report_path,
                "promoted_model": promoted_model,
                "repeatability_pct": repeatability_pct,
                "terminal_reason": terminal_reason,
                "heartbeat_count": heartbeat_count,
                "uc_model_name": (uc_result or {}).get("uc_model_name", ""),
                "uc_model_version": (uc_result or {}).get("version", ""),
                "uc_champion_promoted": (uc_result or {}).get("promoted_to_champion", False),
            },
            catalog=catalog, schema=schema,
        )

        _term_result_lines = [_section("FINALIZE — FINAL STATUS", "-")]
        _term_result_lines.append(_kv("Status", status))
        _term_result_lines.append(_kv("Convergence reason", reason))
        _term_result_lines.append(_kv("Repeatability", f"{repeatability_pct:.1f}%"))
        _term_result_lines.append(_kv("Promoted model", promoted_model or "(none)"))
        _term_result_lines.append(_kv("Report path", report_path or "(none)"))
        _term_result_lines.append(_kv("Elapsed", f"{_elapsed_seconds():.1f}s"))
        _term_result_lines.append(_kv("Heartbeats", heartbeat_count))
        _term_result_lines.append(_bar("-"))
        print("\n".join(_term_result_lines))

        return {
            "status": status,
            "convergence_reason": reason,
            "repeatability_pct": repeatability_pct,
            "report_path": report_path,
            "promoted_model": promoted_model,
            "terminal_reason": terminal_reason,
            "benchmark_publish_count": benchmark_publish_count,
            "labeling_session": session_info,
            "uc_registration": uc_result,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
            "held_out_accuracy": held_out_accuracy,
            "held_out_count": len(held_out_benchmarks) if held_out_benchmarks else 0,
        }

    except TimeoutError as exc:
        terminal_reason = "finalize_timeout"
        logger.exception("Finalize timeout for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        raise
    except Exception as exc:
        terminal_reason = "finalize_error"
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Finalize failure for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        raise


# ── Stage 5: DEPLOY ─────────────────────────────────────────────────


def deploy_check(
    deploy_target: str | None,
    prev_model_id: str,
    iteration_counter: int,
) -> dict:
    """Sub-step 5a: Check deploy eligibility and print target info."""
    _lines = [_section("DEPLOY — GATE CHECK", "-")]
    _lines.append(_kv("Deploy target", deploy_target or "(none — will skip)"))
    _lines.append(_kv("Model ID", prev_model_id))
    _lines.append(_kv("Iteration", iteration_counter))
    _lines.append(_kv("Decision", "PROCEED" if deploy_target else "SKIP"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))
    return {
        "should_deploy": bool(deploy_target),
        "deploy_target": deploy_target,
        "prev_model_id": prev_model_id,
        "iteration_counter": iteration_counter,
    }


def deploy_execute(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    deploy_target: str | None,
    space_id: str,
    exp_name: str,
    domain: str,
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
) -> dict:
    """Sub-step 5b: Execute deployment or skip. Writes Delta stages."""
    if not deploy_target:
        write_stage(
            spark, run_id, "DEPLOY_SKIPPED", "SKIPPED",
            task_key="deploy",
            detail={"reason": "no_deploy_target"},
            catalog=catalog, schema=schema,
        )
        _lines = [_section("DEPLOY — RESULT", "-")]
        _lines.append(_kv("Status", "SKIPPED"))
        _lines.append(_kv("Reason", "no deploy target configured"))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        return {"status": "SKIPPED", "reason": "no_deploy_target"}

    write_stage(
        spark, run_id, "DEPLOY_DELEGATED", "COMPLETE",
        task_key="deploy",
        detail={"deploy_target": deploy_target, "mechanism": "mlflow_deployment_job"},
        catalog=catalog, schema=schema,
    )
    _lines = [_section("DEPLOY — RESULT", "-")]
    _lines.append(_kv("Status", "PENDING_APPROVAL"))
    _lines.append(_kv("Target", deploy_target))
    _lines.append(_kv("Model ID", prev_model_id))
    _lines.append(_kv("Mechanism", "MLflow Deployment Job (Approval -> Cross-env Deploy)"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))
    return {"status": "PENDING_APPROVAL", "deploy_target": deploy_target}


def _run_deploy(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    deploy_target: str | None,
    space_id: str,
    exp_name: str,
    domain: str,
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 5: Deploy via DABs, held-out evaluation (optional).

    Wrapper that calls deploy_check() then deploy_execute() in sequence.
    """
    deploy_check(deploy_target, prev_model_id, iteration_counter)
    return deploy_execute(
        w, spark, run_id, deploy_target, space_id, exp_name,
        domain, prev_model_id, iteration_counter, catalog, schema,
    )


# ── Resume Helper ────────────────────────────────────────────────────


def _resume_lever_loop(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict:
    """Read Delta to find last completed lever for resume after task retry.

    Returns: resume_from_lever, iteration_counter, prev_scores, prev_model_id,
    reflection_buffer, tried_patches, tried_root_causes, skill_exemplars.
    """
    latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not latest_iter:
        return {"resume_from_lever": 0, "iteration_counter": 0}

    stages_df = load_stages(spark, run_id, catalog, schema)
    last_lever = 0
    if not stages_df.empty:
        lever_stages = stages_df[
            stages_df["stage"].str.startswith("LEVER_")
            & (stages_df["status"] == "COMPLETE")
        ]
        if not lever_stages.empty:
            lever_nums = lever_stages["lever"].dropna().astype(int)
            if not lever_nums.empty:
                last_lever = int(lever_nums.max())

    scores_json = latest_iter.get("scores_json", {})
    if isinstance(scores_json, str):
        try:
            scores_json = json.loads(scores_json)
        except (json.JSONDecodeError, TypeError):
            scores_json = {}

    all_iters = load_all_full_iterations(spark, run_id, catalog, schema)
    restored_reflections: list[dict] = []
    restored_tried_patches: set[tuple[str, str]] = set()
    restored_tried_root_causes: set[tuple[str, str]] = set()
    restored_skill_exemplars: list[dict] = []
    for it in all_iters:
        rj = it.get("reflection_json")
        if not isinstance(rj, dict):
            continue
        restored_reflections.append(rj)
        for dnr in rj.get("do_not_retry", []):
            parts = dnr.split(" on ", 1)
            if len(parts) == 2:
                restored_tried_patches.add((parts[0], parts[1]))
        if not rj.get("accepted"):
            root_cause = rj.get("root_cause", "")
            blame = rj.get("blame_set", "")
            if root_cause and blame:
                restored_tried_root_causes.add((root_cause, blame))
        if rj.get("accepted") and rj.get("accuracy_delta", 0.0) >= 1.0:
            restored_skill_exemplars.append({
                "root_cause": rj.get("root_cause", ""),
                "lever_pattern": rj.get("levers", []),
                "patch_types": [x for x in rj.get("patch_types", []) if x is not None],
                "accuracy_gain": rj.get("accuracy_delta", 0.0),
            })

    if restored_reflections:
        logger.info(
            "Restored %d reflection entries from Delta for resume",
            len(restored_reflections),
        )

    return {
        "resume_from_lever": last_lever,
        "iteration_counter": int(latest_iter.get("iteration", 0)),
        "prev_scores": scores_json if isinstance(scores_json, dict) else {},
        "prev_model_id": latest_iter.get("model_id", ""),
        "prev_accuracy": float(latest_iter.get("overall_accuracy", 0.0)),
        "reflection_buffer": restored_reflections,
        "tried_patches": restored_tried_patches,
        "tried_root_causes": restored_tried_root_causes,
        "skill_exemplars": restored_skill_exemplars,
    }


# ── Evaluation via Job (for multi-task architecture) ─────────────────


def run_evaluation_via_job(
    w: WorkspaceClient,
    space_id: str,
    experiment_name: str,
    iteration: int,
    domain: str,
    model_id: str,
    eval_scope: str,
    **kwargs: Any,
) -> dict:
    """Submit evaluation as a Databricks Job run and poll for results.

    Uses ``w.jobs.submit_run()`` with Serverless compute. Benchmarks
    are loaded from the MLflow evaluation dataset.

    This is the job-based alternative to inline ``run_evaluation()``.
    """
    from genie_space_optimizer.common.config import JOB_MAX_WAIT, JOB_POLL_INTERVAL

    notebook_path = kwargs.get(
        "notebook_path", "/Workspace/genie_space_optimizer/jobs/run_evaluation_only",
    )

    task_params = {
        "space_id": space_id,
        "experiment_name": experiment_name,
        "iteration": str(iteration),
        "domain": domain,
        "model_id": model_id or "",
        "eval_scope": eval_scope,
    }

    try:
        run = w.jobs.submit(
            run_name=f"genie_eval_{space_id}_{iteration}",
            tasks=cast(Any, [
                {
                    "task_key": "evaluation",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": task_params,
                    },
                    "new_cluster": {"spark_version": "auto", "num_workers": 0},
                }
            ]),
        )
        run_id = run.run_id
        logger.info("Submitted evaluation job: run_id=%s", run_id)

        elapsed = 0
        while elapsed < JOB_MAX_WAIT:
            time.sleep(JOB_POLL_INTERVAL)
            elapsed += JOB_POLL_INTERVAL
            status = w.jobs.get_run(run_id)
            state = str(status.state.life_cycle_state) if status.state else "UNKNOWN"
            if state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break

        return {
            "job_run_id": str(run_id),
            "status": state,
        }

    except Exception as exc:
        logger.exception("Evaluation job submission failed")
        return {"status": "FAILED", "error": str(exc)}


# ── Convenience Function ─────────────────────────────────────────────


def optimize_genie_space(
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    *,
    run_id: str | None = None,
    apply_mode: str = APPLY_MODE,
    experiment_name: str | None = None,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    deploy_target: str | None = None,
    run_repeat: bool = True,
    triggered_by: str | None = None,
) -> OptimizationResult:
    """Run all 6 stages in a single process.

    When *run_id* is supplied the caller has already created the Delta row
    (e.g. the backend ``start_optimization`` endpoint), so we skip
    ``create_run`` to avoid duplicating the row.
    """
    w = WorkspaceClient()
    from genie_space_optimizer.common.genie_client import (
        configure_connection_pool,
        configure_mlflow_connection_pool,
    )
    from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE
    configure_connection_pool(w, CONNECTION_POOL_SIZE)
    configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    _run_created_here = run_id is None
    if _run_created_here:
        run_id = str(uuid.uuid4())
    assert run_id is not None
    run_id_str = run_id

    ensure_optimization_tables(spark, catalog, schema)

    if _run_created_here:
        create_run(
            spark, run_id_str, space_id, domain, catalog, schema,
            max_iterations=max_iterations,
            levers=levers,
            apply_mode=apply_mode,
            deploy_target=deploy_target,
            triggered_by=triggered_by,
        )

    result = OptimizationResult(
        run_id=run_id_str,
        space_id=space_id,
        domain=domain,
        status="FAILED",
        best_iteration=0,
        best_accuracy=0.0,
        best_repeatability=0.0,
        best_model_id=None,
        convergence_reason=None,
        total_iterations=0,
    )

    try:
        # Stage 1: Preflight
        preflight_out = _run_preflight(
            w, spark, run_id_str, space_id, catalog, schema, domain, experiment_name,
        )
        config = cast(dict[str, Any], preflight_out["config"])
        benchmarks = cast(list[dict], preflight_out["benchmarks"])
        model_id = str(preflight_out["model_id"])
        human_corrections = cast(list[dict], preflight_out.get("human_corrections", []))
        exp_name = str(preflight_out["experiment_name"])
        result.experiment_name = exp_name
        result.experiment_id = str(preflight_out.get("experiment_id", ""))

        train_benchmarks = [b for b in benchmarks if b.get("split") != "held_out"]
        held_out_benchmarks = [b for b in benchmarks if b.get("split") == "held_out"]
        logger.info(
            "Benchmark split: %d train, %d held_out",
            len(train_benchmarks), len(held_out_benchmarks),
        )

        # Stage 2: Baseline
        baseline_out = _run_baseline(
            w, spark, run_id_str, space_id, train_benchmarks, exp_name,
            model_id, catalog, schema, domain,
        )
        prev_scores = cast(dict[str, float], baseline_out["scores"])
        prev_accuracy = float(baseline_out["overall_accuracy"])
        thresholds_met = bool(baseline_out["thresholds_met"])

        if thresholds_met:
            result.status = "CONVERGED"
            result.convergence_reason = "baseline_meets_thresholds"
            result.best_accuracy = prev_accuracy
            result.best_model_id = model_id
            result.final_scores = prev_scores
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason="baseline_meets_thresholds",
            )
        else:
            # Stage 2.5: Proactive Enrichment
            enrichment_out = _run_enrichment(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                catalog, schema,
                baseline_model_id=model_id,
            )

            # Stage 3: Lever Loop (enrichment already done)
            loop_out = _run_lever_loop(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                prev_scores, prev_accuracy, model_id,
                enrichment_out["config"],
                catalog, schema, levers, max_iterations, thresholds, apply_mode,
                triggered_by=triggered_by or "",
                human_corrections=human_corrections,
                enrichment_done=True,
                enrichment_model_id=enrichment_out["enrichment_model_id"],
            )
            result.levers_attempted = cast(list[int], loop_out["levers_attempted"])
            result.levers_accepted = cast(list[int], loop_out["levers_accepted"])
            result.levers_rolled_back = cast(list[int], loop_out["levers_rolled_back"])
            result.total_iterations = int(loop_out["iteration_counter"])
            result.best_accuracy = float(loop_out["accuracy"])
            result.best_model_id = str(loop_out["model_id"])
            result.best_iteration = int(loop_out["best_iteration"])
            result.final_scores = cast(dict[str, float], loop_out["scores"])

            prev_scores = cast(dict[str, float], loop_out["scores"])
            prev_model_id = str(loop_out["model_id"])

            # Stage 4: Finalize
            finalize_out = _run_finalize(
                w, spark, run_id_str, space_id, domain, exp_name,
                prev_scores, prev_model_id, int(loop_out["iteration_counter"]),
                catalog, schema, run_repeat, benchmarks, thresholds,
                question_trace_map=loop_out.get("question_trace_map"),
                reflection_buffer=loop_out.get("reflection_buffer"),
                all_eval_mlflow_run_ids=loop_out.get("all_eval_mlflow_run_ids"),
                all_failure_trace_ids=loop_out.get("all_failure_trace_ids"),
                all_regression_trace_ids=loop_out.get("all_regression_trace_ids"),
                all_failure_question_ids=loop_out.get("all_failure_question_ids"),
                max_iterations=max_iterations,
            )
            result.status = str(finalize_out["status"])
            result.convergence_reason = str(finalize_out["convergence_reason"])
            result.best_repeatability = float(finalize_out["repeatability_pct"])
            result.report_path = str(finalize_out["report_path"])

        # Stage 5: Deploy
        _run_deploy(
            w, spark, run_id_str, deploy_target, space_id, exp_name,
            domain, result.best_model_id or "", result.total_iterations,
            catalog, schema,
        )

    except Exception as exc:
        result.status = "FAILED"
        result.error = traceback.format_exc()
        logger.exception("optimize_genie_space failed for run %s", run_id_str)

    return result


# ── Private Helpers ──────────────────────────────────────────────────


def _build_patch_record(entry: dict, lever: int, apply_mode: str) -> dict:
    """Build a patch record dict for Delta from an apply_log entry."""
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    return {
        "patch_type": patch.get("type", action.get("action_type", "unknown")),
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", "medium"),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id", ""),
    }


def _get_failed_judges(row: dict) -> list[str]:
    """Return list of individual scorer judge names that failed (value contains 'no')."""
    _NON_JUDGE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")
    failed: list[str] = []
    for col, val in row.items():
        is_judge = False
        if col.startswith("feedback/") and col.endswith("/value"):
            is_judge = True
        elif col.startswith("feedback/") and not any(col.endswith(s) for s in _NON_JUDGE_SUFFIXES):
            if "/" not in col.removeprefix("feedback/"):
                is_judge = True
        elif col.endswith("/value") and not col.startswith("feedback/"):
            is_judge = True
        if is_judge and "no" in str(val).lower():
            judge_name = col.replace("feedback/", "").replace("/value", "")
            failed.append(judge_name)
    return failed


def _has_individual_judge_failure(row: dict) -> bool:
    """Return True if at least one individual scorer judge failed (value contains 'no').

    Used to detect rows where the arbiter said 'both_correct' or 'genie_correct'
    but individual judges flagged suboptimal patterns worth learning from.
    """
    return len(_get_failed_judges(row)) > 0


def _get_failure_rows(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Load the latest iteration's per-question rows for failure clustering."""
    latest = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not latest:
        return []
    rows_json = latest.get("rows_json")
    if isinstance(rows_json, str):
        try:
            return json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(rows_json, list):
        return rows_json
    return []


def _compute_category_performance(
    iteration_rows: list[dict],
    benchmarks: list[dict],
) -> dict[str, dict]:
    """Compute per-category benchmark accuracy from an evaluation iteration.

    Maps each evaluation row to its benchmark's ``category`` and computes
    ``{category: {"total": N, "correct": M}}``.  Used to identify weak
    categories for targeted gap-filling.
    """
    qid_to_category: dict[str, str] = {}
    for b in benchmarks:
        qid = b.get("question_id") or b.get("id") or ""
        cat = b.get("category", "")
        question = b.get("question", "")
        if qid and cat:
            qid_to_category[str(qid)] = cat
        if question and cat:
            qid_to_category[question.lower().strip()] = cat

    category_stats: dict[str, dict] = {}
    for row in iteration_rows:
        req = row.get("request") or row.get("inputs") or {}
        if isinstance(req, dict):
            question = str(req.get("question", "")).lower().strip()
            qid = str(req.get("question_id", ""))
        else:
            question = ""
            qid = ""

        cat = qid_to_category.get(qid) or qid_to_category.get(question) or "unknown"
        if cat not in category_stats:
            category_stats[cat] = {"total": 0, "correct": 0}

        category_stats[cat]["total"] += 1

        arbiter = (
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        if arbiter in ("both_correct", "genie_correct"):
            category_stats[cat]["correct"] += 1

    return category_stats
