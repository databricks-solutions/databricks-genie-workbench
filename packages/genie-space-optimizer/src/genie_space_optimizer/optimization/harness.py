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

import copy
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
    CONSECUTIVE_ESCALATION_LIMIT,
    CONSECUTIVE_ROLLBACK_LIMIT,
    INFRA_RETRY_BUDGET,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    DIMINISHING_RETURNS_EPSILON,
    DIMINISHING_RETURNS_LOOKBACK,
    ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS,
    ENABLE_PROMPT_MATCHING_AUTO_APPLY,
    ENABLE_SLICE_GATE,
    SLICE_GATE_SMALL_CORPUS_ROWS,
    SLICE_GATE_TOLERANCE_SMALL_CORPUS,
    FINALIZE_REPEATABILITY_PASSES,
    GENIE_CORRECT_CONFIRMATION_THRESHOLD,
    GT_REPAIR_PROMPT,
    INLINE_EVAL_DELAY,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    LEVER_NAMES,
    MAX_AG_PATCHES,
    MAX_BENCHMARK_COUNT,
    MAX_ITERATIONS,
    MAX_NOISE_FLOOR,
    NEITHER_CORRECT_QUARANTINE_THRESHOLD,
    NEITHER_CORRECT_REPAIR_THRESHOLD,
    OPTIMIZATION_OBJECTIVE,
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
    REGRESSION_THRESHOLD,
    SHADOW_APPLY,
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
    _collect_blank_columns,
    _collect_insufficient_tables,
    _enrich_blank_descriptions,
    _enrich_table_descriptions,
    _generate_holistic_strategy,
    _iq_scan_strategist_enabled,
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
    write_gt_correction_candidates,
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


def _merge_bug4_counters(eval_result: dict) -> dict:
    """Inject Bug #4 (benchmark leakage) counters into an eval_result before
    it is written via ``write_iteration`` for the 'full' scope.

    Reads the module-level counters from ``optimizer._BUG4_COUNTERS`` and
    resets them. Intended to be called once per iteration, at the
    ``eval_scope="full"`` write site, so each iteration row carries the
    counters observed since the prior iteration.

    For 'slice' / 'p0' intermediate evals we deliberately leave counters
    untouched so the 'full' write aggregates them. If leakage is suppressed
    on a slice attempt and the iteration gets rolled back, the counts still
    surface on the full write.
    """
    try:
        from genie_space_optimizer.optimization.optimizer import (
            get_bug4_counters, reset_bug4_counters,
        )
    except ImportError:
        return eval_result
    snapshot = get_bug4_counters()
    eval_result = dict(eval_result)
    # The shape-by-type maps are collected elsewhere (see
    # count_example_sql_leaks and firewall_rejection_count_by_type); here we
    # fold in the flat counter for secondary-mining blocks and a summary
    # entry for firewall rejections.
    eval_result.setdefault("secondary_mining_blocked", 0)
    eval_result["secondary_mining_blocked"] = (
        int(eval_result.get("secondary_mining_blocked") or 0)
        + int(snapshot.get("secondary_mining_blocked", 0) or 0)
    )
    existing_fw = eval_result.get("firewall_rejection_count_by_type") or {}
    if not isinstance(existing_fw, dict):
        existing_fw = {}
    flat_rejections = int(snapshot.get("firewall_rejections", 0) or 0)
    if flat_rejections:
        existing_fw = dict(existing_fw)
        existing_fw["_total"] = int(existing_fw.get("_total", 0)) + flat_rejections
    eval_result["firewall_rejection_count_by_type"] = existing_fw
    reset_bug4_counters()
    return eval_result


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


def _compute_lever_efficacy_prior(
    reflection_buffer: list[dict],
) -> dict:
    """T2.3: Summarise lever efficacy from the current run's history.

    Returns a dict keyed on ``"lever_N"`` with stats:
      - ``attempts``:        number of times this lever was tried
      - ``accepted``:        number of attempts that passed the gate
      - ``acceptance_rate``: accepted / attempts
      - ``mean_delta``:      mean accuracy_delta across all attempts
      - ``examples``:        up to 3 recent (iteration, accepted, delta,
                             root_cause) tuples for the strategist prompt

    The summary is computed in-memory from the reflection buffer; a
    future iteration can persist this across runs as
    ``genie_opt_lever_efficacy`` (Delta table) and seed the strategist
    with cross-run priors. For now it makes the current-run evidence
    visible which is already more signal than the strategist has today.
    """
    stats: dict[str, dict] = {}
    for entry in reflection_buffer:
        _levers = entry.get("lever_set") or entry.get("levers") or []
        if not _levers:
            continue
        _accepted = bool(entry.get("accepted"))
        _delta = float(entry.get("accuracy_delta", 0.0) or 0.0)
        _rc = str(entry.get("root_cause", ""))[:40]
        _iter = entry.get("iteration")
        for _l in _levers:
            try:
                _key = f"lever_{int(_l)}"
            except (TypeError, ValueError):
                continue
            bucket = stats.setdefault(
                _key,
                {"attempts": 0, "accepted": 0, "delta_sum": 0.0, "examples": []},
            )
            bucket["attempts"] += 1
            if _accepted:
                bucket["accepted"] += 1
            bucket["delta_sum"] += _delta
            bucket["examples"].append({
                "iteration": _iter,
                "accepted": _accepted,
                "accuracy_delta": round(_delta, 1),
                "root_cause": _rc,
            })
    # Post-process: compute rates + trim examples.
    out: dict[str, dict] = {}
    for _key, b in stats.items():
        _att = max(b["attempts"], 1)
        out[_key] = {
            "attempts": b["attempts"],
            "accepted": b["accepted"],
            "acceptance_rate": round(b["accepted"] / _att, 3),
            "mean_delta": round(b["delta_sum"] / _att, 2),
            "examples": b["examples"][-3:],
        }
    return out


def _compute_eval_variance(
    full_result_1: dict,
    full_result_2: dict | None,
) -> dict:
    """T0.2: Estimate run-to-run variance of two confirmation evals.

    Compares the per-question pre-arbiter verdicts from two evaluation
    runs against the same Genie space. Returns a dict with:

    - ``disagreed_qids``: list[str] — questions that flipped pass<->fail.
    - ``disagreement_ratio``: float in [0,1] — |disagreed| / total_scored.
    - ``total_scored``: int — questions scored in both runs.
    - ``mean_pre_arbiter_acc``: float — average pre-arbiter rc accuracy.

    When ``full_result_2`` is ``None`` (confirmation skipped because the
    first run already improved) the helper returns zeros — variance
    cannot be estimated from a single run and the gate falls back to the
    legacy single-run comparison.
    """
    if not full_result_2:
        return {
            "disagreed_qids": [],
            "disagreement_ratio": 0.0,
            "total_scored": 0,
            "mean_pre_arbiter_acc": float(
                full_result_1.get("pre_arbiter_accuracy",
                                  full_result_1.get("overall_accuracy", 0.0))
            ),
        }

    def _pre_arbiter_verdicts(fr: dict) -> dict[str, bool]:
        """Build ``qid -> pass`` map using raw result_correctness only."""
        out: dict[str, bool] = {}
        for row in fr.get("rows", []) or []:
            rq = row.get("request") or {}
            if isinstance(rq, str):
                try:
                    rq = json.loads(rq)
                except (json.JSONDecodeError, TypeError):
                    rq = {}
            rqk = rq.get("kwargs", {}) if isinstance(rq, dict) else {}
            qid = str(
                row.get("inputs/question_id")
                or (row.get("inputs") or {}).get("question_id", "")
                or row.get("question_id")
                or rqk.get("question_id")
                or (rq.get("question_id") if isinstance(rq, dict) else None)
                or ""
            )
            if not qid:
                continue
            rc = str(
                row.get("result_correctness/value", row.get("result_correctness", ""))
            ).lower()
            if rc == "excluded":
                continue
            err_type = str(
                row.get("outputs/comparison/error_type")
                or row.get("comparison/error_type")
                or row.get("comparison.error_type")
                or ""
            ).lower()
            if err_type in ("both_empty", "genie_result_unavailable"):
                continue
            out[qid] = rc in ("yes", "true", "1", "1.0")
        return out

    v1 = _pre_arbiter_verdicts(full_result_1)
    v2 = _pre_arbiter_verdicts(full_result_2)
    shared = set(v1) & set(v2)
    disagreed = sorted(qid for qid in shared if v1[qid] != v2[qid])
    total_scored = len(shared)
    ratio = len(disagreed) / total_scored if total_scored else 0.0
    mean_pre = (
        float(full_result_1.get("pre_arbiter_accuracy", 0.0))
        + float(full_result_2.get("pre_arbiter_accuracy", 0.0))
    ) / 2.0
    return {
        "disagreed_qids": disagreed,
        "disagreement_ratio": ratio,
        "total_scored": total_scored,
        "mean_pre_arbiter_acc": mean_pre,
    }


def _paired_question_test(
    prev_failures: set[str],
    new_failures: set[str],
) -> dict:
    """T0.2: Paired per-question sign test on pre-arbiter verdicts.

    Given the set of failing question ids before and after a patch
    application, compute:
      - ``flipped_to_pass``:  questions failing before but passing after
      - ``flipped_to_fail``:  questions passing before but failing after
      - ``stable_fail``:      failing in both
      - ``net_improvement``:  flipped_to_pass - flipped_to_fail
      - ``significant``:      True iff the sign of the difference is
        unambiguous given the minimum-effect threshold. For our typical
        20–30 row corpus we accept ``net_improvement >= 2`` as
        significant, and ``net_improvement >= max(3, K * 0.15)`` for
        larger corpora. This is a cheap stand-in for a real binomial
        test; once we're off the tiny-corpus regime we can tighten it
        to a proper McNemar computation.
    """
    _flipped_to_pass = prev_failures - new_failures
    _flipped_to_fail = new_failures - prev_failures
    _stable_fail = prev_failures & new_failures
    net = len(_flipped_to_pass) - len(_flipped_to_fail)
    _total_touched = len(_flipped_to_pass) + len(_flipped_to_fail)
    # Minimum-effect threshold — on a 22-row corpus even a 2-question
    # swing (~9%) is outside the run-to-run variance we've observed, so
    # require net >= 2 or net <= -2. On larger corpora demand a larger
    # effect.
    _min_effect = max(2, int(round(_total_touched * 0.15)))
    significant = abs(net) >= _min_effect
    return {
        "flipped_to_pass": sorted(_flipped_to_pass),
        "flipped_to_fail": sorted(_flipped_to_fail),
        "stable_fail": sorted(_stable_fail),
        "net_improvement": net,
        "significant": significant,
        "min_effect": _min_effect,
    }


def _iteration_label(counter: int) -> str:
    """T3.17: Unified label for iteration_counter in log banners.

    The lever-loop's ``iteration_counter`` is 0-indexed (0 is the first
    loop body iteration, attempt #1). Historically some banners printed
    the raw counter ("Iteration 0", "Iteration 1") while others printed
    ``counter + 1`` ("Iteration 1", "Iteration 2"), and operators had
    no reliable way to tell which one they were reading. This helper
    renders a single canonical form: ``index 0 / attempt 1``.
    """
    try:
        idx = int(counter)
    except (TypeError, ValueError):
        idx = 0
    attempt = idx + 1
    return f"index {idx} / attempt {attempt}"


def _fmt_patch(idx: int, patch: dict, action: dict, entry: dict | None = None) -> str:
    """Format a single applied patch into a readable multi-line string.

    T2.13: when ``entry`` is provided and carries a distinct
    ``applied_patch_type`` (i.e. the applier transformed the proposal,
    as with the rewrite_instruction downgrade splitter), the header
    prints both types so the log is honest about what actually ran.
    """
    ptype = patch.get("type", action.get("action_type", "?"))
    applied_ptype = None
    applied_detail = None
    if entry:
        applied_ptype = entry.get("applied_patch_type")
        applied_detail = entry.get("applied_patch_detail")
    label = _PATCH_TYPE_LABELS.get(ptype, ptype)
    table = patch.get("table") or patch.get("target") or ""
    column = patch.get("column", "")
    target = f"{table}.{column}" if column else table

    if applied_ptype and applied_ptype != ptype:
        applied_label = _PATCH_TYPE_LABELS.get(applied_ptype, applied_ptype)
        lines = [f"|  [{idx}] {label} -> {applied_label} (T2.13 applier-side transform)"]
    else:
        lines = [f"|  [{idx}] {label}"]
    if target:
        lines.append(f"|      Target: {target}")
    if applied_detail:
        lines.append(f"|      Applied: {applied_detail}")

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

    iq_scan_recommended_levers = (
        config.get("_gso_iq_scan_recommended_levers", [])
        if isinstance(config, dict) else []
    )
    iq_scan_summary = (
        config.get("_gso_iq_scan_summary")
        if isinstance(config, dict) else None
    )

    return {
        "benchmarks": benchmarks,
        "config": config,
        "model_id": model_id,
        "experiment_name": exp_name,
        "experiment_id": experiment_id,
        "human_corrections": human_corrections,
        "iq_scan_recommended_levers": iq_scan_recommended_levers,
        "iq_scan_summary": iq_scan_summary,
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

    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg
    try:
        _bl_config = _fetch_cfg(w, space_id)
        _bl_parsed = _bl_config.get("_parsed_space", _bl_config)
        _bl_instr = _bl_parsed.get("instructions", {}) if isinstance(_bl_parsed, dict) else {}
        _bl_instr_text = _bl_instr.get("text_instructions", "") if isinstance(_bl_instr, dict) else ""
    except Exception:
        _bl_instr_text = ""
    scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_bl_instr_text)

    _lines = [_section("BASELINE — EVALUATION SETUP", "-")]
    _lines.append(_kv("Space ID", space_id))
    _lines.append(_kv("Model ID", model_id))
    _lines.append(_kv("Experiment", exp_name))
    _lines.append(_kv("Scorers", len(scorers)))
    _lines.append(_kv("Instruction context", f"{len(_bl_instr_text)} chars" if _bl_instr_text else "(none)"))
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
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> dict:
    """Sub-step 2b: Run 9-judge evaluation with retry."""
    _ensure_sql_context(spark, catalog, schema)
    # Tier 4: v2 name — ``<run_short>/baseline``.
    from genie_space_optimizer.common.mlflow_names import (
        baseline_run_name,
        default_tags as _v2_tags_baseline,
    )
    eval_result = _safe_stage(
        spark, run_id, "BASELINE_EVAL", run_evaluation,
        catalog, schema,
        setup_ctx["space_id"], setup_ctx["exp_name"], 0, benchmarks,
        setup_ctx["domain"], setup_ctx.get("model_id"), "full",
        setup_ctx["predict_fn"], setup_ctx["scorers"],
        spark=spark, w=w, catalog=catalog, gold_schema=schema,
        uc_schema=f"{catalog}.{schema}",
        model_creation_kwargs=model_creation_kwargs,
        max_benchmark_count=max_benchmark_count,
        run_name=baseline_run_name(run_id),
        extra_tags=_v2_tags_baseline(
            run_id,
            space_id=str(setup_ctx.get("space_id", "")),
            stage="baseline", iteration=0,
        ),
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

    eval_result = _merge_bug4_counters(eval_result)
    write_iteration(
        spark, run_id, 0, eval_result,
        catalog=catalog, schema=schema,
        eval_scope="full", model_id=model_id,
    )

    # Tier 1.7: anchor ``best_accuracy`` to the stricter of
    # ``overall_accuracy`` and ``both_correct_rate``. ``overall_accuracy``
    # can be inflated when Genie's SQL is semantically wrong but happens
    # to return the same row set as GT (rc=yes); the arbiter flags those
    # rows as ``ground_truth_correct``. Anchoring to ``both_correct_rate``
    # ensures later iterations can't be rejected by an artificially high
    # ceiling (the "ghost ceiling" regression loop).
    _overall_acc = float(eval_result.get("overall_accuracy", 0.0) or 0.0)
    _both_correct_rate = eval_result.get("both_correct_rate")
    if _both_correct_rate is not None:
        _anchored_best = min(_overall_acc, float(_both_correct_rate))
    else:
        _anchored_best = _overall_acc

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=0,
        best_accuracy=_anchored_best,
        best_model_id=model_id,
    )

    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "COMPLETE",
        task_key="baseline_eval",
        detail={
            "overall_accuracy": _overall_acc,
            "both_correct_rate": _both_correct_rate,
            "anchored_best_accuracy": _anchored_best,
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
    *,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Stage 2.5: Enable format assistance and entity matching as best practice.

    Runs between baseline eval and lever loop.  Deterministic (no LLM).
    Returns summary dict with counts of changes applied.

    ``benchmarks`` (optional) is forwarded to the scorer for the
    benchmark-column-reference boost. Callers that don't have the
    benchmark corpus yet can pass ``None`` — the scorer falls back to
    name-based scoring in that case. The first run where benchmarks
    aren't available still produces sensible results, just without the
    benchmark boost.
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

        apply_log = auto_apply_prompt_matching(
            w, space_id, config, benchmarks=benchmarks,
        )

        applied = apply_log.get("applied", [])
        fa_count = apply_log.get("format_assistance_count", 0)
        em_count = apply_log.get("entity_matching_count", 0)
        em_disabled_count = apply_log.get("entity_matching_disabled_count", 0)

        # Map entry type to the inverse operation so standard rollback can
        # restore disabled slots (and conversely, undo enables). Format
        # assistance doesn't need a paired inverse in this table — operator
        # rollbacks of FA go through the standard genie_config path.
        _INVERSE_BY_TYPE = {
            "enable_value_dictionary": {"enable_entity_matching": False},
            "disable_value_dictionary": {"enable_entity_matching": True},
            "enable_example_values": {"enable_format_assistance": False},
        }
        for idx, entry in enumerate(applied):
            etype = entry.get("type", "unknown")
            tbl = entry.get("table", "")
            col = entry.get("column", "")
            inverse = _INVERSE_BY_TYPE.get(etype)
            rollback_payload = None
            if inverse is not None:
                rollback_payload = json.dumps({
                    "op": "update",
                    "section": "column_configs",
                    "table": tbl,
                    "column": col,
                    **inverse,
                })
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": etype,
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": f"{tbl}.{col}",
                    "patch": entry,
                    "command": None,
                    "rollback": rollback_payload,
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
        _pm_lines.append(_kv("Entity matching enabled", f"{em_count} columns"))
        _pm_lines.append(_kv("Entity matching disabled", f"{em_disabled_count} columns"))
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
                "entity_matching_disabled": em_disabled_count,
                "total_changes": len(applied),
                "patched_objects": apply_log.get("patched_objects", []),
            },
            catalog=catalog, schema=schema,
        )

        return {
            "format_assistance_count": fa_count,
            "entity_matching_count": em_count,
            "entity_matching_disabled_count": em_disabled_count,
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
        return {
            "format_assistance_count": 0,
            "entity_matching_count": 0,
            "entity_matching_disabled_count": 0,
            "total_changes": 0,
        }


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
        "total_eligible": 0,
        "total_patches_generated": 0,
        "total_failed_llm": 0,
        "total_enriched": 0,
        "total_skipped": 0,
        "tables_eligible": 0,
        "tables_patches_generated": 0,
        "tables_failed_llm": 0,
        "tables_enriched": 0,
        "tables_skipped": 0,
    }

    try:
        # ── Column description enrichment ────────────────────────────
        _data_profile = metadata_snapshot.get("_data_profile", {})
        # Compute ORIGINAL eligibility before calling the LLM so silent
        # batch drops (e.g. unparseable JSON after validator retries)
        # become visible in the stage summary.
        _blank_columns = _collect_blank_columns(metadata_snapshot)
        col_patches = _enrich_blank_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["total_eligible"] = len(_blank_columns)
        result["total_patches_generated"] = len(col_patches)
        result["total_failed_llm"] = max(
            0, len(_blank_columns) - len(col_patches),
        )

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
                synonym_value = sections.pop("synonyms", "")

                new_desc = update_sections(
                    cc.get("description"),
                    sections,
                    lever=0,
                    entity_type=etype,
                )
                cc["description"] = new_desc

                if synonym_value:
                    new_syns = [s.strip() for s in str(synonym_value).split(",") if s.strip()]
                    existing = cc.get("synonyms") or []
                    for s in new_syns:
                        if s not in existing:
                            existing.append(s)
                    cc["synonyms"] = existing

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
        _insufficient_tables = _collect_insufficient_tables(metadata_snapshot)
        tbl_patches = _enrich_table_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["tables_eligible"] = len(_insufficient_tables)
        result["tables_patches_generated"] = len(tbl_patches)
        result["tables_failed_llm"] = max(
            0, len(_insufficient_tables) - len(tbl_patches),
        )

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
        _de_lines.append(_kv("Eligible columns", len(_blank_columns)))
        _de_lines.append(_kv("LLM patches generated", len(col_patches)))
        if result["total_failed_llm"]:
            _de_lines.append(_kv(
                "LLM batch failures",
                result["total_failed_llm"],
            ))
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
        _de_lines.append(_kv("Eligible tables", len(_insufficient_tables)))
        _de_lines.append(_kv("Table patches generated", len(tbl_patches)))
        if result["tables_failed_llm"]:
            _de_lines.append(_kv(
                "Table LLM batch failures",
                result["tables_failed_llm"],
            ))
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
        exec_candidates, exec_diagnostics = _extract_proven_joins(rows_json, metadata_snapshot)
        result["execution_candidates"] = len(exec_candidates)
        result["extraction_diagnostics"] = exec_diagnostics

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
            _diag = result.get("extraction_diagnostics", {})
            if _diag:
                _jd_lines.append(_kv("  Eval rows scanned", _diag.get("total_rows", "?")))
                _jd_lines.append(_kv("  Positive verdicts", _diag.get("positive_verdicts", "?")))
                _jd_lines.append(_kv("  SQL with JOIN", _diag.get("sql_with_join", "?")))
                _jd_lines.append(_kv("  FROM unresolved", _diag.get("no_from_resolved", "?")))
                _jd_lines.append(_kv("  Joined unresolved", _diag.get("no_joined_resolved", "?")))
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
        _diag = result.get("extraction_diagnostics", {})
        if _diag:
            _jd_lines.append(_kv("  Eval rows scanned", _diag.get("total_rows", "?")))
            _jd_lines.append(_kv("  Positive verdicts", _diag.get("positive_verdicts", "?")))
            _jd_lines.append(_kv("  SQL with JOIN", _diag.get("sql_with_join", "?")))
            _jd_lines.append(_kv("  FROM unresolved", _diag.get("no_from_resolved", "?")))
            _jd_lines.append(_kv("  Joined unresolved", _diag.get("no_joined_resolved", "?")))
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


# ── Iterative join mining (runs after each accepted iteration) ────────


def _mine_and_apply_proven_joins(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    metadata_snapshot: dict,
    eval_rows: list[dict],
    catalog: str,
    schema: str,
    *,
    iteration: int = 0,
) -> dict:
    """Mine execution-proven joins from eval rows and apply new ones.

    Lightweight wrapper around ``_extract_proven_joins`` →
    ``_corroborate_with_uc_metadata`` → ``_build_join_specs_from_proven``
    that can be called after any accepted iteration.

    Returns a result dict with ``total_applied`` count and ``new_specs``.
    Does NOT re-read from Delta — operates on the in-memory *eval_rows*.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _build_join_specs_from_proven,
        _corroborate_with_uc_metadata,
        _extract_proven_joins,
        _short_name,
    )

    result: dict = {"total_applied": 0, "new_specs": [], "extraction_diagnostics": {}}
    if not eval_rows:
        return result

    exec_candidates, exec_diagnostics = _extract_proven_joins(eval_rows, metadata_snapshot)
    result["extraction_diagnostics"] = exec_diagnostics
    if not exec_candidates:
        return result

    _inst = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    existing_specs = _inst.get("join_specs", [])
    if not isinstance(existing_specs, list):
        existing_specs = []

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

    new_candidates = []
    for cand in exec_candidates:
        pair_key = tuple(sorted((cand["left_table"], cand["right_table"])))
        if pair_key not in existing_pairs:
            new_candidates.append(cand)

    new_candidates = _corroborate_with_uc_metadata(new_candidates, metadata_snapshot)
    if not new_candidates:
        return result

    new_specs = _build_join_specs_from_proven(new_candidates, metadata_snapshot)
    if not new_specs:
        return result

    parsed = metadata_snapshot
    inst_block = parsed.setdefault("instructions", {})
    spec_list = inst_block.setdefault("join_specs", [])

    # Defence-in-depth EXPLAIN: these specs come from eval rows that
    # already executed successfully on the warehouse, so EXPLAIN rarely
    # fires. Keeping it here for symmetry with
    # :func:`_apply_instruction_join_specs` and to catch corner cases
    # (FQ-resolution drift, column renames between eval and PATCH).
    warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "")
    validated_specs: list[dict] = []
    explain_rejected = 0
    for spec in new_specs:
        left_id = (spec.get("left") or {}).get("identifier", "")
        right_id = (spec.get("right") or {}).get("identifier", "")
        sql_field = spec.get("sql", [])
        join_cond = (
            sql_field[0] if isinstance(sql_field, list) and sql_field
            else str(sql_field or "")
        )
        is_valid, err = _explain_join_candidate(
            w, spark, left_id, right_id, join_cond,
            catalog=catalog, gold_schema=schema,
            warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.info(
                "Proven-join EXPLAIN rejected: %s <-> %s ON %s — %s",
                left_id, right_id, (join_cond or "")[:80], err,
            )
            explain_rejected += 1
            continue
        validated_specs.append(spec)
    new_specs = validated_specs
    if explain_rejected:
        result["explain_rejected"] = explain_rejected

    applied_lines: list[str] = []
    for spec in new_specs:
        meta = spec.pop("_proactive_metadata", {})
        spec_list.append(spec)
        left_short = _short_name(spec["left"]["identifier"])
        right_short = _short_name(spec["right"]["identifier"])
        freq = meta.get("frequency", 0)
        agreed_tag = "agreed" if meta.get("agreed") else "single_source"
        applied_lines.append(
            f"{left_short} <-> {right_short}"
            f" ON {spec['sql'][0][:60] if spec.get('sql') else '?'}"
            f" (freq={freq}, {agreed_tag})"
        )
        result["total_applied"] += 1

    patch_space_config(w, space_id, parsed)
    result["new_specs"] = new_specs

    for idx, spec in enumerate(new_specs):
        write_patch(
            spark, run_id, iteration, 4, idx,
            {
                "patch_type": "iterative_join_mining",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": (
                    f"{spec['left']['identifier']}"
                    f" <-> {spec['right']['identifier']}"
                ),
                "patch": spec,
                "command": None,
            },
            catalog, schema,
        )

    _jm_lines = [_section(f"ITERATIVE JOIN MINING (iter {iteration})", "-")]
    _diag = result.get("extraction_diagnostics", {})
    _jm_lines.append(_kv("Eval rows scanned", _diag.get("total_rows", "?")))
    _jm_lines.append(_kv("Positive verdicts", _diag.get("positive_verdicts", "?")))
    _jm_lines.append(_kv("SQL with JOIN", _diag.get("sql_with_join", "?")))
    _jm_lines.append(_kv("Existing join specs", len(existing_specs)))
    _jm_lines.append(_kv("New candidates", len(new_candidates)))
    _jm_lines.append(_kv("New joins applied", result["total_applied"]))
    for al in applied_lines:
        _jm_lines.append(f"|    {al}")
    _jm_lines.append(_bar("-"))
    print("\n".join(_jm_lines))

    write_stage(
        spark, run_id, f"JOIN_MINING_ITER_{iteration}", "COMPLETE",
        task_key="lever_loop", iteration=iteration,
        detail=result,
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


# ── Unified example-SQL generation (Phase 4.R4) ─────────────────────


def _run_unified_example_sql_generation(
    *,
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    uc_columns: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    full_firewall_corpus: list[dict],
    data_profile: dict | None = None,
) -> dict:
    """Run the unified (benchmark-engine-based) example-SQL generator.

    Builds the leakage oracle from the benchmark corpus + the space's
    already-installed ``example_question_sqls``, calls
    ``generate_example_sqls``, and applies survivors via the shared
    ``_apply_proactive_example_sqls`` (which runs the last-mile
    firewall one more time, by design).

    Returns a dict with ``applied``, ``rejection_counters``, and a
    short list of applied examples for the pretty summary.
    """
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SQL_TARGET,
    )
    from genie_space_optimizer.optimization.evaluation import (
        generate_example_sqls,
    )
    from genie_space_optimizer.optimization.leakage import (
        BenchmarkCorpus,
        LeakageOracle,
    )

    out: dict = {
        "applied": 0,
        "rejection_counters": {},
        "unified_generated": 0,
        "archetype_fallback": False,
    }

    try:
        # ── Firewall: benchmark corpus ∪ existing example_sqls ──────
        bench_corpus = BenchmarkCorpus.from_benchmarks(full_firewall_corpus)

        existing_sqls = (
            (metadata_snapshot.get("instructions") or {})
            .get("example_question_sqls") or []
        )
        existing_corpus_rows = [
            {
                "id": f"existing_{i}",
                "question": (e or {}).get("question", "") or "",
                "expected_sql": (e or {}).get("sql", "") or "",
            }
            for i, e in enumerate(existing_sqls)
            if isinstance(e, dict)
        ]
        existing_corpus = BenchmarkCorpus.from_benchmarks(existing_corpus_rows)
        oracle = LeakageOracle(bench_corpus, existing_corpus)

        candidates, rejection_counters = generate_example_sqls(
            w=w, spark=spark,
            config=config, uc_columns=uc_columns,
            uc_tags=[], uc_routines=[],
            domain=domain, catalog=catalog, schema=schema,
            warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            target_count=PREFLIGHT_EXAMPLE_SQL_TARGET,
            existing_example_sqls=existing_sqls,
            leakage_oracle=oracle,
        )
    except Exception:
        logger.warning(
            "unified example-sql generation raised; "
            "archetype fallback will run",
            exc_info=True,
        )
        out["rejection_counters"] = {"pipeline_exception": 1}
        return out

    out["unified_generated"] = len(candidates)
    out["rejection_counters"] = rejection_counters or {}

    if not candidates:
        _print_unified_example_summary(
            run_id=run_id,
            target=PREFLIGHT_EXAMPLE_SQL_TARGET,
            existing=len(existing_sqls),
            applied_examples=[],
            rejection_counters=rejection_counters or {},
            config=config,
        )
        return out

    # ── Apply via the shared pipeline (runs last-mile firewall) ─────
    proposals = [
        {
            "patch_type": "add_example_sql",
            "example_question": c.get("question", ""),
            "example_sql": c.get("expected_sql", ""),
            "usage_guidance": c.get("usage_guidance", ""),
            "risk_level": "low",
            "provenance": c.get("provenance", "synthetic_example_sql"),
        }
        for c in candidates
    ]

    try:
        _apply_proactive_example_sqls(
            w, spark, run_id, space_id, proposals,
            metadata_snapshot, config, catalog, schema,
            benchmarks=full_firewall_corpus,
        )
        out["applied"] = len(proposals)
    except Exception:
        logger.warning(
            "unified applier raised; proposals not deployed",
            exc_info=True,
        )

    _print_unified_example_summary(
        run_id=run_id,
        target=PREFLIGHT_EXAMPLE_SQL_TARGET,
        existing=len(existing_sqls),
        applied_examples=candidates,
        rejection_counters=rejection_counters or {},
        config=config,
    )

    return out


def _print_unified_example_summary(
    *,
    run_id: str,
    target: int,
    existing: int,
    applied_examples: list[dict],
    rejection_counters: dict[str, int],
    config: dict | None = None,
) -> None:
    """Pretty-print the unified example-SQL generator outcome.

    Seven distinct rejection classes are surfaced so operators can
    attribute yield shortfall to a specific stage (metadata vs MV vs
    execute vs arbiter vs firewall vs dedup) rather than one bucket.

    PR 21 — when ``config`` is provided, the banner also surfaces a
    one-line MV-detection summary (``MVs detected: N (config: a,
    column-flags: b, catalog: c)``) and the adaptive-overdraw
    short-circuit reason, so log readers see at a glance whether a
    cluster of ``mv_missing_measure_function`` rejections is rooted in
    "no MVs at all" vs "MVs detected but not by the path the rewriter
    consults".
    """
    _lines = [_section("EXAMPLE SQL GENERATION (unified)", "=")]
    _lines.append(_kv("Target", target))
    _lines.append(_kv("Existing examples", existing))
    _lines.append(_kv("Applied (new)", len(applied_examples)))
    _mv_total_for_hint = 0
    if isinstance(config, dict):
        try:
            from genie_space_optimizer.optimization.evaluation import (
                _count_mv_detection_sources,
            )
            _mv_counts = _count_mv_detection_sources(config)
            _mv_total = (
                _mv_counts.get("config", 0)
                + _mv_counts.get("column_flags", 0)
                + _mv_counts.get("catalog", 0)
            )
            _mv_total_for_hint = _mv_total
            _lines.append(_kv(
                "MVs detected",
                f"{_mv_total} (config: {_mv_counts.get('config', 0)}, "
                f"column-flags: {_mv_counts.get('column_flags', 0)}, "
                f"catalog: {_mv_counts.get('catalog', 0)})",
            ))
        except Exception:
            pass
    # PR 23 — when zero MVs are detected but the run produced any
    # ``mv_*`` rejection bucket, the catalog-detection helper either
    # silently no-op'd (DBR < 16.2, permission failure, JSON envelope
    # mismatch) or the unified pipeline has a stale cache. Surface a
    # one-line hint so operators reading the banner are pointed at
    # the catalog-detection summary log line for the underlying cause.
    rc_for_hint = rejection_counters or {}
    _has_mv_reject = False
    _exec_subbuckets = rc_for_hint.get("explain_or_execute_subbuckets") or {}
    if isinstance(_exec_subbuckets, dict):
        for _reason in _exec_subbuckets:
            if isinstance(_reason, str) and _reason.startswith("mv_"):
                _has_mv_reject = True
                break
    if (
        _mv_total_for_hint == 0
        and _has_mv_reject
        and isinstance(config, dict)
    ):
        _lines.append(
            "|  hint: 0 MVs detected but mv_* rejections present — "
            "see catalog-detection summary log line",
        )
    _lines.append("|")
    rc = rejection_counters or {}
    _lines.append(_kv("Metadata rejected", rc.get("metadata", 0)))
    _lines.append(_kv("MV guard rejected", rc.get("mv_select_star", 0)))
    _lines.append(_kv(
        "EXPLAIN/execute rejected", rc.get("explain_or_execute", 0),
    ))
    # PR 18 — split EXPLAIN/execute rejected into sub-buckets keyed by
    # the validation reason code so operators can see at a glance
    # whether the dominant failure class is unknown columns vs
    # missing-MEASURE() vs alias collisions vs join issues. Also
    # surface up to 3 example questions per bucket so the log is
    # immediately actionable.
    _subbuckets = rc.get("explain_or_execute_subbuckets") or {}
    _examples = rc.get("explain_or_execute_examples") or {}
    if isinstance(_subbuckets, dict) and _subbuckets:
        _ordered = sorted(
            _subbuckets.items(), key=lambda kv: (-kv[1], kv[0]),
        )
        for _reason, _count in _ordered:
            _lines.append(_kv(f"  {_reason}", _count))
            _ex_list = _examples.get(_reason) or []
            if isinstance(_ex_list, list):
                for _ex in _ex_list[:3]:
                    if not isinstance(_ex, dict):
                        continue
                    _q = str(_ex.get("question", ""))[:80]
                    _err = str(_ex.get("error", ""))[:120]
                    _lines.append(f"|     [{_reason}] {_q} — {_err}")
    _lines.append(_kv(
        "Arbiter verdict=no", rc.get("arbiter_no", 0),
    ))
    # F12 — row-capture-side failures get differentiated counters so
    # the banner distinguishes "judge said no" from "judge never saw
    # rows because capture raised". The subquery-unsupported bucket
    # is the diagnostic signal for PR 13's metric-view-safe fallback;
    # the exec-failed bucket flags timeouts / permissions / other
    # infrastructure issues. Both are emitted unconditionally so a
    # zero count is itself a reassurance signal in the log.
    _lines.append(_kv(
        "Arbiter row-capture: subquery unsupported",
        rc.get("arbiter_row_capture_subquery_unsupported", 0),
    ))
    _lines.append(_kv(
        "Arbiter row-capture: exec failed",
        rc.get("arbiter_row_capture_exec_failed", 0),
    ))
    _lines.append(_kv(
        "Firewall: SQL fingerprint match", rc.get("firewall_fingerprint", 0),
    ))
    _lines.append(_kv(
        "Firewall: question echo", rc.get("firewall_question_echo", 0),
    ))
    _lines.append(_kv("Dedup (in-corpus)", rc.get("dedup_in_corpus", 0)))
    # F8 — deterministic repairs applied inside the correction loop.
    # Shown only when non-zero so the banner stays terse when the LLM
    # returns clean output. These mirror the F4/F5 counters the
    # preflight banner surfaces.
    _stem_repairs = rc.get("repaired_stemmed_identifiers", 0)
    _measure_repairs = rc.get("repaired_measure_refs", 0)
    _alias_repairs = rc.get("measure_alias_collisions_repaired", 0)
    _overdraw_rounds = rc.get("adaptive_overdraw_rounds_used", 0)
    if (
        _stem_repairs or _measure_repairs or _alias_repairs
        or (isinstance(_overdraw_rounds, int) and _overdraw_rounds > 1)
    ):
        _lines.append("|")
        if _stem_repairs:
            _lines.append(_kv(
                "Stemmed identifiers repaired", _stem_repairs,
            ))
        if _measure_repairs:
            _lines.append(_kv(
                "MEASURE() refs repaired", _measure_repairs,
            ))
        # PR 15 — alias collisions deterministically repaired via
        # ``_repair_measure_alias_collisions``.
        if _alias_repairs:
            _lines.append(_kv(
                "MEASURE() alias collisions repaired", _alias_repairs,
            ))
        # PR 17 — number of adaptive overdraw rounds we used. Show
        # only when > 1 (i.e. the first round under-produced and we
        # had to re-ask the LLM).
        if isinstance(_overdraw_rounds, int) and _overdraw_rounds > 1:
            _lines.append(_kv(
                "Adaptive overdraw rounds used", _overdraw_rounds,
            ))
    # PR 21 — surface the short-circuit reason whenever it is set,
    # independent of whether any deterministic-repair counters
    # fired. Operators reading the banner should always see the
    # signal that we cut overdraw short and why.
    _short_circuit = rc.get("adaptive_overdraw_short_circuited")
    if _short_circuit:
        _lines.append(_kv(
            "Adaptive overdraw short-circuited", str(_short_circuit),
        ))
    if applied_examples:
        _lines.append("|")
        for idx, c in enumerate(applied_examples[:10], 1):
            q = str(c.get("question", ""))[:80]
            _lines.append(f"|  [{idx}] {q}")
        if len(applied_examples) > 10:
            _lines.append(f"|  … and {len(applied_examples) - 10} more")
    _lines.append(_bar("="))
    print("\n".join(_lines))


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
    benchmarks: list[dict] | None = None,
) -> None:
    """Apply mined benchmark example SQLs proactively via the Genie API.

    Bug #4 firewall — every proposal is passed through ``is_benchmark_leak``
    before ``proposals_to_patches`` when ``benchmarks`` is provided. Leaky
    proposals are dropped with a counter increment. Callers are expected to
    pass the benchmark corpus so the firewall can run; older call sites
    that omit it degrade gracefully (no firewall, logs a warning).
    """
    from genie_space_optimizer.optimization.applier import (
        proposals_to_patches,
        apply_patch_set,
    )

    if benchmarks is None:
        logger.warning(
            "_apply_proactive_example_sqls called without benchmarks — "
            "Bug #4 firewall skipped. Caller should pass benchmarks.",
        )
    else:
        from genie_space_optimizer.optimization.leakage import (
            BenchmarkCorpus, is_benchmark_leak,
        )
        from genie_space_optimizer.optimization.optimizer import _incr_bug4_counter

        corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
        filtered: list[dict] = []
        for p in mined_proposals:
            is_leak, reason = is_benchmark_leak(
                p, p.get("patch_type", "add_example_sql"), corpus,
            )
            if is_leak:
                _incr_bug4_counter("firewall_rejections")
                logger.info(
                    "Bug #4 firewall: dropped proactive proposal %s (%s) - %s",
                    p.get("proposal_id", "?"),
                    str(p.get("example_question", ""))[:80],
                    reason,
                )
                continue
            filtered.append(p)
        mined_proposals = filtered

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
        _ap = entry.get("patch", {})
        _action = entry.get("action", {})
        write_patch(
            spark, run_id, 0, 0, idx,
            {
                "patch_type": "proactive_example_sql",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": f"example_question_sqls[{idx}]",
                "patch": {
                    "question": str(_ap.get("example_question", ""))[:200],
                    "sql": str(_ap.get("example_sql", "")),
                },
                "command": _action.get("command"),
                "rollback": _action.get("rollback_command"),
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
    """Two-phase proactive instruction management (Task B.5).

    **Seed phase** — only runs when existing instructions are empty or
    below ``_INSTRUCTION_SEED_THRESHOLD`` chars. Calls
    :func:`_generate_proactive_instructions` (5-section schema),
    validates strictly, writes on pass.

    **Expand phase** — always runs after seed. Parses the (possibly
    freshly seeded) instructions to determine which canonical sections
    are missing, calls :func:`_expand_instructions`, validates the
    merged result strictly, writes on pass. Never overwrites existing
    canonical sections — expansion only *fills gaps*.

    Compared to the pre-B.5 behaviour this solves the "instructions not
    comprehensive" bug: a space whose existing prose was ≥50 chars
    (but only covered PURPOSE) previously got no enrichment at all.
    Now the expand phase fills the other four canonical sections.
    """
    from genie_space_optimizer.common.config import (
        CANONICAL_SECTION_HEADERS, MAX_TEXT_INSTRUCTIONS_CHARS,
        MIN_EXPAND_BUDGET,
    )
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.optimization.applier import (
        _get_general_instructions, _set_general_instructions,
        _trim_bullets_to_budget, _trim_rendered_to_cap,
        parse_canonical_sections, render_canonical_sections,
        validate_instruction_text,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _expand_instructions, _generate_proactive_instructions,
    )

    _INSTRUCTION_SEED_THRESHOLD = 50
    parsed = config.get("_parsed_space", config)
    current_instructions = _get_general_instructions(parsed)

    result: dict = {
        "instructions_seeded": False,
        "instructions_expanded": False,
        "instruction_chars": 0,
        "seeded_sections": [],
        "expanded_sections": [],
        "skipped_reason": None,
        # C3 decline-log UX: track disposition of each phase so the summary
        # line surfaces the actual reason instead of the misleading "no-op".
        "seed_outcome": "skipped",
        "expand_outcome": "not_attempted",
    }

    write_stage(
        spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "STARTED",
        task_key="instruction_seeding", catalog=catalog, schema=schema,
    )

    try:
        # ── Seed phase ──────────────────────────────────────────────
        needs_seeding = (
            not current_instructions
            or len(current_instructions.strip()) < _INSTRUCTION_SEED_THRESHOLD
        )
        if needs_seeding:
            instruction_text = _generate_proactive_instructions(metadata_snapshot, w)
            if instruction_text:
                _set_general_instructions(parsed, instruction_text)
                try:
                    patch_space_config(w, space_id, parsed)
                    result["instructions_seeded"] = True
                    result["seed_outcome"] = "wrote"
                    result["instruction_chars"] = len(instruction_text)
                    canonical_secs, _, _ = parse_canonical_sections(instruction_text)
                    result["seeded_sections"] = [
                        h for h in CANONICAL_SECTION_HEADERS if h in canonical_secs
                    ]
                    current_instructions = instruction_text
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
                    result["seed_outcome"] = "patch_failed"
            else:
                # LLM returned "" — either validation failed after repair
                # or the LLM call raised. Specific reason was logged by
                # _generate_proactive_instructions.
                result["seed_outcome"] = "declined_llm_or_validation"
        else:
            # Seed is only for empty-or-thin prose; nothing to do otherwise.
            result["seed_outcome"] = "skipped_existing_prose"

        # ── Expand phase ────────────────────────────────────────────
        # Always runs after seed. Works on whatever prose exists after
        # the seed phase (if any). Declines rather than writes malformed
        # prose — Fix Agent contract parity.
        canonical_secs, _legacy_secs, _preamble = parse_canonical_sections(
            current_instructions or "",
        )
        present_headers = {h for h in CANONICAL_SECTION_HEADERS if h in canonical_secs}
        missing = [h for h in CANONICAL_SECTION_HEADERS if h not in present_headers]

        # Budget for Layer 1 pre-render trim — re-derived to match what
        # _expand_instructions used when calling the LLM.
        existing_length = len(current_instructions or "")
        remaining_budget = max(MAX_TEXT_INSTRUCTIONS_CHARS - existing_length, 0)
        per_section_budget = (
            remaining_budget // len(missing) if missing else remaining_budget
        )

        if not missing:
            result["expand_outcome"] = "skipped_all_sections_present"
        elif not current_instructions:
            result["expand_outcome"] = "skipped_no_existing_prose"
        else:
            try:
                new_sections = _expand_instructions(
                    metadata_snapshot, current_instructions, missing, w=w,
                )
            except Exception:
                logger.warning("Expand instructions call failed", exc_info=True)
                new_sections = {}
                result["expand_outcome"] = "llm_error"

            # ``__skip_reason__`` sentinel — LLM wasn't called because the
            # remaining char budget was below MIN_EXPAND_BUDGET.
            _expand_skip_reason = new_sections.pop("__skip_reason__", None)
            if _expand_skip_reason:
                logger.info(
                    "Expand no-op: skip_reason=%s", _expand_skip_reason,
                )
                result["expand_outcome"] = f"skipped_{_expand_skip_reason}"

            if new_sections:
                # ── Layer 1: pre-render per-section clip ────────────
                # Hard-enforce per_section_budget so the sum of section
                # bodies can never exceed remaining_budget.
                merged = dict(canonical_secs)
                for header, body in new_sections.items():
                    if header in merged:
                        continue  # never overwrite existing content
                    clipped = _trim_bullets_to_budget(body, per_section_budget)
                    if not clipped.strip():
                        continue
                    merged[header] = [
                        ln for ln in clipped.splitlines() if ln.strip()
                    ]

                rendered = render_canonical_sections(merged)

                # ── Layer 2: post-render global clip ────────────────
                # Handles rendering overhead (headers + blank lines add
                # ~15 chars per section) that Layer 1 can't see. This
                # makes over-cap merge structurally impossible.
                rendered = _trim_rendered_to_cap(
                    rendered, MAX_TEXT_INSTRUCTIONS_CHARS,
                )
                new_text = "".join(rendered).rstrip() + "\n"

                ok, errs = validate_instruction_text(new_text, strict=True)
                if not ok:
                    # Categorise the decline for the summary line.
                    err_codes: list[str] = []
                    for e in errs:
                        le = e.lower()
                        if "length" in le:
                            err_codes.append("length")
                        elif "sql detected" in le:
                            err_codes.append("sql_in_prose")
                        elif "verbatim" in le or "non-canonical" in le:
                            err_codes.append("header")
                        elif "order" in le:
                            err_codes.append("order")
                        else:
                            err_codes.append("other")
                    result["expand_outcome"] = (
                        "declined_" + "+".join(sorted(set(err_codes)))
                    )
                    logger.warning(
                        "Expand instructions: strict validation failed "
                        "(outcome=%s) — keeping existing prose. errors=%s",
                        result["expand_outcome"], errs,
                    )
                else:
                    _set_general_instructions(parsed, new_text)
                    try:
                        patch_space_config(w, space_id, parsed)
                        result["instructions_expanded"] = True
                        result["expand_outcome"] = "wrote"
                        result["expanded_sections"] = list(new_sections.keys())
                        result["instruction_chars"] = len(new_text)
                        write_patch(
                            spark, run_id, 0, 0, 1,
                            {
                                "patch_type": "proactive_instruction_expand",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": "instructions.text_instructions",
                                "patch": {
                                    "added_sections": list(new_sections.keys()),
                                    "chars": len(new_text),
                                },
                                "command": None,
                                "rollback": None,
                                "proposal_id": "proactive_instruction_expand",
                            },
                            catalog, schema,
                        )
                    except Exception:
                        logger.warning(
                            "Proactive instruction expand: PATCH failed",
                            exc_info=True,
                        )
                        result["expand_outcome"] = "patch_failed"
            elif result["expand_outcome"] == "not_attempted":
                # No sections generated AND no earlier outcome set.
                result["expand_outcome"] = "llm_returned_empty"

        if not result["instructions_seeded"] and not result["instructions_expanded"]:
            result["skipped_reason"] = (
                "already_comprehensive"
                if not needs_seeding and not missing
                else "llm_failed_or_validation"
            )

        # ── Print summary with explicit outcomes (C3) ───────────────
        # Surfaces the real disposition of each phase. "declined_*" lines
        # tell operators exactly what failed validation without having to
        # correlate with an earlier WARNING.
        _lines = [_section("PROACTIVE INSTRUCTION SEEDING", "-")]
        seed_outcome = result.get("seed_outcome", "?")
        expand_outcome = result.get("expand_outcome", "?")
        if result["instructions_seeded"]:
            _lines.append(_kv("Seed", f"WROTE ({result['instruction_chars']} chars)"))
            _lines.append(_kv("  Sections", ", ".join(result["seeded_sections"]) or "(none)"))
        elif seed_outcome == "skipped_existing_prose":
            _lines.append(_kv("Seed", f"SKIPPED (existing {len(current_instructions)} chars, threshold={_INSTRUCTION_SEED_THRESHOLD})"))
        elif seed_outcome.startswith("declined"):
            _lines.append(_kv("Seed", f"DECLINED — {seed_outcome.removeprefix('declined_')}"))
        elif seed_outcome == "patch_failed":
            _lines.append(_kv("Seed", "PATCH_FAILED"))
        else:
            _lines.append(_kv("Seed", seed_outcome.upper()))
        if result["instructions_expanded"]:
            _lines.append(_kv(
                "Expand",
                f"WROTE ({len(result['expanded_sections'])} sections, {result['instruction_chars']} chars)",
            ))
            _lines.append(_kv("  Sections", ", ".join(result["expanded_sections"])))
        elif expand_outcome.startswith("declined"):
            _lines.append(_kv(
                "Expand",
                f"DECLINED — {expand_outcome.removeprefix('declined_')}",
            ))
            if missing:
                _lines.append(_kv("  Unfilled", ", ".join(missing)))
        elif expand_outcome.startswith("skipped"):
            _lines.append(_kv(
                "Expand",
                f"SKIPPED — {expand_outcome.removeprefix('skipped_')}",
            ))
        else:
            _lines.append(_kv("Expand", expand_outcome.upper()))
            if missing:
                _lines.append(_kv("  Unfilled", ", ".join(missing)))
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


# ── Stage 2.96: INSTRUCTION-TO-SQL-EXPRESSION CONVERSION ─────────────


def _apply_instruction_sql_expressions(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Apply validated instruction-derived SQL expression candidates to the Genie Space.

    Returns the count of expressions successfully applied.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.common.genie_schema import generate_genie_id

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_SQL_EXPRESSION_CONVERSION", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    instr = metadata_snapshot.get("instructions", {})
    if not isinstance(instr, dict):
        instr = {}
    original_snippets = instr.get("sql_snippets", {})
    working_snippets = copy.deepcopy(original_snippets) if original_snippets else {}

    applied = 0
    for c in candidates:
        stype = c["snippet_type"]
        category = f"{stype}s"
        if category not in working_snippets:
            working_snippets[category] = []

        _sql_val = c["sql"]
        entry: dict = {
            "id": generate_genie_id(),
            "sql": [_sql_val] if isinstance(_sql_val, str) else _sql_val,
        }
        if c.get("display_name"):
            entry["display_name"] = c["display_name"]
        if c.get("alias") and stype != "filter":
            entry["alias"] = c["alias"]
        if c.get("synonyms"):
            entry["synonyms"] = c["synonyms"]

        working_snippets[category].append(entry)
        applied += 1

    if applied:
        patch_copy = copy.deepcopy(metadata_snapshot)
        patch_copy.setdefault("instructions", {})["sql_snippets"] = working_snippets
        try:
            patch_space_config(w, space_id, patch_copy)
            metadata_snapshot.setdefault("instructions", {})["sql_snippets"] = working_snippets
            logger.info(
                "Applied %d instruction-derived SQL expressions to space %s",
                applied, space_id,
            )
        except Exception:
            logger.warning(
                "Failed to PATCH instruction-derived SQL expressions",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_SQL_EXPRESSION_CONVERSION", "COMPLETE",
        task_key="enrichment",
        detail={
            "candidates_total": len(candidates),
            "applied": applied,
        },
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → SQL EXPRESSION CONVERSION", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for c in candidates[:5]:
        _lines.append(f"  |  {c['snippet_type']}: {c['sql'][:60]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


# ── Stage 2.96b: INSTRUCTION-DERIVED JOIN SPECS / TABLE DESC / SYNONYMS ─


def _explain_join_candidate(
    w: WorkspaceClient,
    spark: SparkSession,
    left_identifier: str,
    right_identifier: str,
    join_sql_cond: str,
    *,
    catalog: str,
    gold_schema: str,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Run ``EXPLAIN SELECT 1 FROM <l> JOIN <r> ON <cond> LIMIT 1``.

    Returns ``(is_valid, error_message)``. When no execution backend is
    available (both ``spark`` and ``warehouse_id`` are absent/empty), this
    short-circuits to ``(True, "")`` so unit-test runs without a Spark
    session still succeed — production runs always have one.

    The EXPLAIN guard catches re-wrap failures the arbiter-approval pre-
    filter cannot: wrong FQ prefix, type-incompatible join columns, and
    references to columns that don't exist on the named table.
    """
    from genie_space_optimizer.optimization.benchmarks import (
        _resolve_primary_table_fqn,
    )

    if not (left_identifier and right_identifier and join_sql_cond):
        return False, "missing identifier or join condition"

    left_fq = _resolve_primary_table_fqn(
        left_identifier, catalog=catalog, gold_schema=gold_schema,
    )
    right_fq = _resolve_primary_table_fqn(
        right_identifier, catalog=catalog, gold_schema=gold_schema,
    )
    if not (left_fq and right_fq):
        return False, "could not resolve FQ identifiers"

    explain_sql = (
        f"EXPLAIN SELECT 1 FROM {left_fq} JOIN {right_fq} "
        f"ON {join_sql_cond} LIMIT 1"
    )
    try:
        if w is not None and warehouse_id:
            from genie_space_optimizer.optimization.evaluation import (
                _execute_sql_via_warehouse,
            )
            _execute_sql_via_warehouse(
                w, warehouse_id, explain_sql,
                catalog=catalog, schema=gold_schema,
            )
            return True, ""
        if spark is not None:
            try:
                spark.sql(f"USE CATALOG `{catalog}`") if catalog else None
                spark.sql(f"USE SCHEMA `{gold_schema}`") if gold_schema else None
            except Exception:
                pass  # best-effort context setup; EXPLAIN error surfaces below
            spark.sql(explain_sql)
            return True, ""
        # No backend — cannot validate. Accept optimistically; the Genie
        # API PATCH will reject malformed join specs at persist time.
        return True, ""
    except Exception as exc:
        return False, f"EXPLAIN failed: {str(exc)[:200]}"


def _apply_instruction_join_specs(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> int:
    """Append instruction-derived join_spec candidates to the Genie Space.

    Mirrors the patch pattern used by :func:`_mine_and_apply_proven_joins`:
    mutate ``metadata_snapshot["instructions"]["join_specs"]`` in place and
    PATCH the whole config. Dedups against existing pairs by sorted
    (left_identifier, right_identifier).

    Runs ``EXPLAIN`` validation (via :func:`_explain_join_candidate`) on
    each candidate before persisting. The prose miner's source is user-
    asserted text (not arbiter-backed), so EXPLAIN is the critical
    persistence gate — it catches re-wrap failures such as FQ mismatch,
    type-incompatible join columns, and references to columns that don't
    exist. Candidates that fail EXPLAIN are dropped with an INFO log.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_JOIN_SPECS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    instr = metadata_snapshot.setdefault("instructions", {})
    specs = instr.setdefault("join_specs", [])

    existing_pairs: set[tuple[str, str]] = set()
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        left = spec.get("left", {}) if isinstance(spec.get("left"), dict) else {}
        right = spec.get("right", {}) if isinstance(spec.get("right"), dict) else {}
        lt, rt = left.get("identifier", ""), right.get("identifier", "")
        if lt and rt:
            existing_pairs.add(tuple(sorted((lt, rt))))

    applied = 0
    explain_rejected = 0
    for c in candidates:
        left = c.get("left", {})
        right = c.get("right", {})
        pair = tuple(sorted((left.get("identifier", ""), right.get("identifier", ""))))
        if not all(pair) or pair in existing_pairs:
            continue

        # EXPLAIN-based exec validation — see _explain_join_candidate.
        sql_field = c.get("sql", [])
        join_cond = (
            sql_field[0] if isinstance(sql_field, list) and sql_field
            else str(sql_field or "")
        )
        is_valid, err = _explain_join_candidate(
            w, spark,
            left.get("identifier", ""), right.get("identifier", ""),
            join_cond,
            catalog=catalog, gold_schema=schema,
            warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.info(
                "Instruction join_spec rejected by EXPLAIN: %s <-> %s ON %s — %s",
                left.get("identifier", "?"), right.get("identifier", "?"),
                (join_cond or "")[:80], err,
            )
            explain_rejected += 1
            continue

        spec_entry = {
            "left": left,
            "right": right,
            "sql": c.get("sql", []),
        }
        if c.get("instruction"):
            spec_entry["instruction"] = c["instruction"]
        specs.append(spec_entry)
        existing_pairs.add(pair)
        applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived join_specs PATCH failed", exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_JOIN_SPECS", "COMPLETE",
        task_key="enrichment",
        detail={
            "candidates_total": len(candidates),
            "applied": applied,
            "explain_rejected": explain_rejected,
        },
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → JOIN SPECS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    if explain_rejected:
        _lines.append(_kv("Rejected (EXPLAIN)", explain_rejected))
    for c in candidates[:5]:
        left_id = c.get("left", {}).get("identifier", "?").split(".")[-1]
        right_id = c.get("right", {}).get("identifier", "?").split(".")[-1]
        _lines.append(f"  |  {left_id} ↔ {right_id}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


def _apply_instruction_table_descriptions(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Append instruction-derived descriptions to matching tables / metric views.

    Idempotent: if ``description_append`` already appears in the target
    description (case-insensitive), the append is skipped.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_TABLE_DESCRIPTIONS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    ds = metadata_snapshot.setdefault("data_sources", {})
    all_sources: list[dict] = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])

    # Index by lower(identifier) AND lower(short name) so candidates that
    # reference a table by short name still resolve.
    by_full: dict[str, dict] = {}
    by_short: dict[str, dict] = {}
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        ident = (t.get("identifier") or t.get("name") or "").strip().lower()
        if ident:
            by_full[ident] = t
            by_short.setdefault(ident.split(".")[-1], t)

    def _desc_as_text(value: Any) -> str:
        # The Genie API stores ``description`` as ``list[str]`` for tables
        # and metric views, but some legacy snapshots / proactive seeds
        # write a plain string. Normalise both shapes to one comparison
        # string so dedupe / contains checks work uniformly. Mirrors the
        # ``_section_text`` helper in :mod:`applier`.
        if isinstance(value, list):
            return "\n".join(str(x) for x in value)
        if value is None:
            return ""
        return str(value)

    applied = 0
    updated_ids: list[str] = []
    for c in candidates:
        tid_lower = str(c.get("table_identifier", "")).strip().lower()
        desc_append = str(c.get("description_append", "")).strip()
        if not tid_lower or not desc_append:
            continue
        tbl = by_full.get(tid_lower) or by_short.get(tid_lower.split(".")[-1])
        if not tbl:
            continue
        existing_value = tbl.get("description", "") or ""
        existing_text = _desc_as_text(existing_value)
        if desc_append.lower() in existing_text.lower():
            continue  # idempotent — already present
        # Preserve the input shape: list-shaped descriptions stay
        # ``list[str]`` (the Genie API contract), string-shaped stay str.
        # Writing back a Python ``repr`` of a list (the prior bug) caused
        # PATCH to reject with "Expected an array for description".
        if isinstance(existing_value, list):
            tbl["description"] = list(existing_value) + [desc_append]
        else:
            new_desc = (
                existing_text
                + ("\n" if existing_text and not existing_text.endswith("\n") else "")
                + desc_append
            ).strip()
            tbl["description"] = new_desc
        updated_ids.append(tbl.get("identifier", tbl.get("name", "?")))
        applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived table descriptions PATCH failed",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_TABLE_DESCRIPTIONS", "COMPLETE",
        task_key="enrichment",
        detail={"candidates_total": len(candidates), "applied": applied},
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → TABLE DESCRIPTIONS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for tid in updated_ids[:5]:
        _lines.append(f"  |  {tid}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


def _apply_instruction_column_synonyms(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Merge instruction-derived column synonyms into ``column_configs``.

    Dedup is case-insensitive on synonym strings. Missing column_configs
    rows are created on the fly so the synonym survives the PATCH round-trip.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_COLUMN_SYNONYMS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    ds = metadata_snapshot.setdefault("data_sources", {})
    all_sources: list[dict] = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])

    by_full: dict[str, dict] = {}
    by_short: dict[str, dict] = {}
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        ident = (t.get("identifier") or t.get("name") or "").strip().lower()
        if ident:
            by_full[ident] = t
            by_short.setdefault(ident.split(".")[-1], t)

    applied = 0
    for c in candidates:
        tid_lower = str(c.get("table_identifier", "")).strip().lower()
        col_name = str(c.get("column_name", "")).strip()
        synonyms = c.get("synonyms", []) or []
        if not tid_lower or not col_name or not synonyms:
            continue
        tbl = by_full.get(tid_lower) or by_short.get(tid_lower.split(".")[-1])
        if not tbl:
            continue
        cc_list = tbl.setdefault("column_configs", [])
        if not isinstance(cc_list, list):
            cc_list = []
            tbl["column_configs"] = cc_list
        col_entry: dict | None = None
        for cc in cc_list:
            if isinstance(cc, dict) and (cc.get("column_name", "") or "").strip().lower() == col_name.lower():
                col_entry = cc
                break
        if col_entry is None:
            col_entry = {"column_name": col_name, "synonyms": []}
            cc_list.append(col_entry)
        existing_syns = col_entry.setdefault("synonyms", []) or []
        if not isinstance(existing_syns, list):
            existing_syns = list(existing_syns)
        existing_lower = {str(s).strip().lower() for s in existing_syns}
        changed = False
        for syn in synonyms:
            s = str(syn).strip()
            if s and s.lower() not in existing_lower:
                existing_syns.append(s)
                existing_lower.add(s.lower())
                changed = True
        col_entry["synonyms"] = existing_syns
        if changed:
            applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived column synonyms PATCH failed",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_COLUMN_SYNONYMS", "COMPLETE",
        task_key="enrichment",
        detail={"candidates_total": len(candidates), "applied": applied},
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → COLUMN SYNONYMS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for c in candidates[:5]:
        _lines.append(f"  |  {c.get('table_identifier', '?').split('.')[-1]}.{c.get('column_name')}: {c.get('synonyms', [])[:3]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


# ── Stage 2.96c: Instruction prose mining orchestrator ─────────────


def _run_instruction_prose_mining(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> dict:
    """Stage 5a (Task C.5): multi-target prose miner + rewrite pipeline.

    Runs BEFORE the proactive seed/expand phase so legacy ALL-CAPS prose is
    promoted into structured config and normalised into the canonical 5-
    section form before seed/expand run. Idempotent: a second pass on
    already-normalised prose finds nothing to promote and the rewrite
    step returns ``SKIP_NO_CHANGE``.

    Source-specific invariant:
        This path persists sql_snippets / join_specs / example_qsqls from
        USER-ASSERTED prose (``text_instructions``). User prose is its
        own authority — no arbiter gate is available (or possible) at
        the source. Persistence gates:

        - sql_snippet: ``validate_sql_snippet`` EXPLAIN+execute.
        - join_spec: ``_explain_join_candidate`` EXPLAIN-only.
        - example_qsql: benchmark-leakage firewall (answer-shape path)
          + example-SQL ground-truth validator.
        - table_desc / column_synonym: shape + dedup only.
        - keep_in_prose: schema-validator only (stays in prose).

        The Bug #4 firewall is intentionally not applied to sql_snippet
        or join_spec outputs (see scoping comment in
        ``leakage._PATCH_TEXT_FIELDS``). Structural primitives are not
        answers, so firewall fingerprint-matching is inappropriate here.

    Pipeline:

    1. ``_convert_instructions_to_sql_expressions`` (multi-target miner).
    2. Per-target appliers (``_apply_instruction_*``) for every non-empty
       bucket.
    3. ``rewrite_instructions_from_miner_output`` — span-based removal +
       canonical regrouping + strict validation; on ``DECLINE_MALFORMED``
       the original prose is preserved (Fix Agent contract parity).
    4. Emit ``set_text_instructions`` via ``_set_general_instructions``
       + ``patch_space_config`` when ``RewriteResult.WRITE``.

    Returns a dict with per-target applied counts + the raw miner stats so
    the caller can decide whether to refetch config. Never raises — any
    downstream failure is logged and treated as a soft miss.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config as _patch,
    )
    from genie_space_optimizer.optimization.applier import (
        RewriteResult, _get_general_instructions,
        _set_general_instructions,
        rewrite_instructions_from_miner_output,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _convert_instructions_to_sql_expressions,
    )

    miner_result = _convert_instructions_to_sql_expressions(
        metadata_snapshot, w=w,
        spark=spark, catalog=catalog, gold_schema=schema,
        warehouse_id=warehouse_id,
    )
    miner_stats = miner_result.get("stats", {})

    sql_applied = join_applied = example_applied = desc_applied = synonym_applied = 0
    applied_spans: list[str] = []

    def _collect_spans(bucket_name: str) -> None:
        applied_spans.extend(
            c.get("source_span", "")
            for c in miner_result.get(bucket_name, [])
            if c.get("source_span")
        )

    if miner_result.get("sql_snippet"):
        sql_applied = _apply_instruction_sql_expressions(
            w, spark, run_id, space_id, miner_result["sql_snippet"],
            metadata_snapshot, catalog, schema,
        )
        if sql_applied:
            _collect_spans("sql_snippet")

    if miner_result.get("join_spec"):
        join_applied = _apply_instruction_join_specs(
            w, spark, run_id, space_id, miner_result["join_spec"],
            metadata_snapshot, catalog, schema,
            warehouse_id=warehouse_id,
        )
        if join_applied:
            _collect_spans("join_spec")

    if miner_result.get("example_qsql"):
        # Reuse the existing proactive-example-sql applier shape.
        _example_proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": c["question"],
                "example_sql": c["sql"],
                "usage_guidance": c.get("usage_guidance", ""),
                "risk_level": "low",
            }
            for c in miner_result["example_qsql"]
        ]
        if _example_proposals:
            try:
                _apply_proactive_example_sqls(
                    w, spark, run_id, space_id, _example_proposals,
                    metadata_snapshot, config, catalog, schema,
                    benchmarks=benchmarks,
                )
                example_applied = len(_example_proposals)
                _collect_spans("example_qsql")
            except Exception:
                logger.warning(
                    "Instruction-derived example SQLs: apply failed",
                    exc_info=True,
                )

    if miner_result.get("table_desc"):
        desc_applied = _apply_instruction_table_descriptions(
            w, spark, run_id, space_id, miner_result["table_desc"],
            metadata_snapshot, catalog, schema,
        )
        if desc_applied:
            _collect_spans("table_desc")

    if miner_result.get("column_synonym"):
        synonym_applied = _apply_instruction_column_synonyms(
            w, spark, run_id, space_id, miner_result["column_synonym"],
            metadata_snapshot, catalog, schema,
        )
        if synonym_applied:
            _collect_spans("column_synonym")

    # Span-based prose rewrite: remove promoted spans, regroup keep_in_prose
    # spans under canonical headers, validate strictly, emit the op or decline.
    total_applied = (
        sql_applied + join_applied + example_applied
        + desc_applied + synonym_applied
    )
    keep_in_prose_spans = miner_result.get("keep_in_prose", []) or []
    rewrite_outcome = "not_run"
    if total_applied or keep_in_prose_spans:
        _original_instr = _get_general_instructions(metadata_snapshot)
        outcome, new_instr, rewrite_errors = rewrite_instructions_from_miner_output(
            _original_instr, applied_spans, keep_in_prose_spans,
        )
        rewrite_outcome = outcome
        if outcome == RewriteResult.WRITE and new_instr:
            try:
                _set_general_instructions(metadata_snapshot, new_instr)
                _patch(w, space_id, metadata_snapshot)
                logger.info(
                    "miner.rewrite.applied chars_before=%d chars_after=%d space_id=%s",
                    len(_original_instr), len(new_instr), space_id,
                )
            except Exception:
                logger.warning("miner.rewrite.patch_failed", exc_info=True)
                rewrite_outcome = "patch_failed"
        elif outcome == RewriteResult.DECLINE_MALFORMED:
            # Fix Agent decline contract: keep original prose, emit a
            # decline-shaped log line for triage.
            logger.warning(
                "miner.rewrite.declined reason=malformed errors=%s space_id=%s",
                rewrite_errors[:5], space_id,
            )
        else:  # SKIP_NO_CHANGE
            logger.info(
                "miner.rewrite.skipped reason=no_change space_id=%s",
                space_id,
            )

    _miner_lines = [_section("INSTRUCTION PROSE MINING & PROMOTION", "-")]
    _miner_lines.append(_kv("Candidates total", miner_stats.get("candidates_total", 0)))
    _miner_lines.append(_kv("SQL snippets", sql_applied))
    _miner_lines.append(_kv("Join specs", join_applied))
    _miner_lines.append(_kv("Example SQLs", example_applied))
    _miner_lines.append(_kv("Table descriptions", desc_applied))
    _miner_lines.append(_kv("Column synonyms", synonym_applied))
    _miner_lines.append(_kv("Kept in prose", len(keep_in_prose_spans)))
    _miner_lines.append(_kv("Rewrite", rewrite_outcome))
    if miner_stats.get("rejected_by_reason"):
        _miner_lines.append(_kv(
            "Rejected",
            ", ".join(
                f"{k}={v}" for k, v in miner_stats["rejected_by_reason"].items()
            ),
        ))
    _miner_lines.append(_bar("-"))
    print("\n".join(_miner_lines))

    return {
        "sql_applied": sql_applied,
        "join_applied": join_applied,
        "example_applied": example_applied,
        "desc_applied": desc_applied,
        "synonym_applied": synonym_applied,
        "total_applied": total_applied,
        "keep_in_prose_count": len(keep_in_prose_spans),
        "rewrite_outcome": rewrite_outcome,
        "stats": miner_stats,
    }


# ── Stage 2.97: PROACTIVE SQL EXPRESSION SEEDING ─────────────────────


_SNIPPET_TYPE_FROM_KEY = {
    "measures": "measure",
    "filters": "filter",
    "expressions": "expression",
}


def _repair_existing_sql_snippets(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Normalize every existing SQL snippet to fully-qualified form.

    Runs unconditionally — unlike :func:`_seed_new_sql_snippets`, which is
    gated by the remaining SQL-snippet headroom (see that function). This
    is the remediation
    path for spaces whose stored snippets were produced by an older
    GSO version (or by the lever loop before A.1) and still use
    short-form prefixes. The Genie serving path rejects such snippets; the
    failing run in the bug report is exactly this scenario.

    Algorithm:

    - Iterate ``instructions.sql_snippets.{measures,filters,expressions}``.
    - For each snippet, call :func:`normalize_sql_snippet` (no execution
      check; EXPLAIN only). If the normalized form differs from the
      stored form, replace the snippet's ``sql`` field in-place.
    - PATCH the space config once at the end if anything changed.

    Returns ``{"scanned": N, "rewritten": M, "rejected": K}``.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.optimization.benchmarks import normalize_sql_snippet

    result: dict = {"scanned": 0, "rewritten": 0, "rejected": 0}

    parsed = config.get("_parsed_space", config)
    existing_snippets = parsed.get("instructions", {}).get("sql_snippets", {})
    if not isinstance(existing_snippets, dict):
        existing_snippets = {}

    if not any(existing_snippets.get(k) for k in _SNIPPET_TYPE_FROM_KEY):
        # Nothing to repair — print a terse line so the run log stays tidy.
        _lines = [_section("SQL EXPRESSION REPAIR", "-")]
        _lines.append(_kv("Scanned", 0))
        _lines.append(_kv("Status", "No existing snippets"))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        return result

    write_stage(
        spark, run_id, "SQL_EXPRESSION_REPAIR", "STARTED",
        task_key="sql_expression_repair", catalog=catalog, schema=schema,
    )

    working = copy.deepcopy(existing_snippets)
    any_change = False
    sample_warnings: list[str] = []

    for type_key, snippet_type in _SNIPPET_TYPE_FROM_KEY.items():
        for snippet in (working.get(type_key) or []):
            sql_val = snippet.get("sql", [])
            if isinstance(sql_val, list) and sql_val:
                original = str(sql_val[0])
            else:
                original = str(sql_val) if sql_val else ""
            if not original:
                continue
            result["scanned"] += 1
            try:
                normalized, warnings = normalize_sql_snippet(
                    original, snippet_type, metadata_snapshot,
                    catalog=catalog, gold_schema=schema,
                    spark=spark, w=w, warehouse_id=warehouse_id,
                )
            except Exception as exc:
                logger.warning(
                    "SQL expression repair: normalize failed for %s: %s",
                    original[:80], exc,
                )
                result["rejected"] += 1
                continue
            explain_failed = any(
                warn.startswith("EXPLAIN failed:") for warn in warnings
            )
            if explain_failed:
                # Don't rewrite one invalid form into another — leave it
                # alone so the lever loop can later propose a real fix.
                result["rejected"] += 1
                if warnings and len(sample_warnings) < 5:
                    sample_warnings.extend(warnings[:2])
                continue
            if normalized != original:
                snippet["sql"] = [normalized]
                any_change = True
                result["rewritten"] += 1
            elif warnings and len(sample_warnings) < 5:
                # Informational warnings (ambiguity, unknown table) — keep
                # a small sample for debugging in the run log.
                sample_warnings.extend(warnings[:1])

    if any_change:
        patch_copy = copy.deepcopy(parsed)
        patch_copy.setdefault("instructions", {})["sql_snippets"] = working
        try:
            patch_space_config(w, space_id, patch_copy)
            parsed.setdefault("instructions", {})["sql_snippets"] = working
        except Exception:
            logger.warning("SQL expression repair: PATCH failed", exc_info=True)
            result["rewritten"] = 0
            write_stage(
                spark, run_id, "SQL_EXPRESSION_REPAIR", "FAILED",
                task_key="sql_expression_repair",
                error_message="PATCH failed", catalog=catalog, schema=schema,
            )
            return result

    _lines = [_section("SQL EXPRESSION REPAIR", "-")]
    _lines.append(_kv("Scanned", result["scanned"]))
    _lines.append(_kv("Rewritten", result["rewritten"]))
    _lines.append(_kv("Rejected", result["rejected"]))
    for warning in sample_warnings[:5]:
        _lines.append(_kv("  warn", warning[:120]))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    write_stage(
        spark, run_id, "SQL_EXPRESSION_REPAIR", "COMPLETE",
        task_key="sql_expression_repair",
        detail=result, catalog=catalog, schema=schema,
    )
    return result


def _seed_new_sql_snippets(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Mine and apply new SQL Expressions (measures, filters, dimensions).

    Gated by the remaining per-space SQL-snippet headroom:

        headroom = MAX_SQL_SNIPPETS - existing_count - LEVER_RESERVE

    When ``headroom == 0`` this step skips entirely (repair still runs,
    see :func:`_repair_existing_sql_snippets`). ``LEVER_RESERVE`` holds
    back ~25% of the 200-snippet budget for the lever loop.

    Source-specific invariant:
        This path persists sql_snippets from ARBITER-APPROVED benchmark
        rows only (verdict == ``both_correct``, pre-filtered by the
        caller via ``_extract_arbiter_approved_benchmarks``), plus
        schema-discovery heuristics. Every candidate is EXPLAIN+execute
        validated via ``validate_sql_snippet`` before being applied.

        The Bug #4 benchmark-leakage firewall is NOT applied here —
        the arbiter pre-filter is the source gate; adding the firewall
        would double-gate and reject legitimate structural primitives
        whose fingerprints happen to match the benchmark corpus (see
        scoping comment above ``_PATCH_TEXT_FIELDS`` in leakage.py).
    """
    import json as _json

    from genie_space_optimizer.common.config import (
        SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
        SQL_EXPRESSION_SEEDING_MAX_CANDIDATES,
    )
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.common.genie_schema import (
        MAX_SQL_SNIPPETS,
        count_sql_snippets,
        generate_genie_id,
    )
    from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet
    from genie_space_optimizer.optimization.optimizer import (
        _discover_schema_sql_expressions,
        _enrich_candidates_with_llm,
        _format_existing_sql_snippets,
        _mine_sql_expression_candidates,
        _ngram_similarity,
    )

    # ── Source-specific invariant ───────────────────────────────────────
    # This path persists sql_snippets from arbiter-approved benchmarks
    # only (``both_correct`` verdict, filtered by the caller via
    # ``_extract_arbiter_approved_benchmarks``) OR from schema-discovery
    # heuristics. Every candidate still goes through EXPLAIN+execute via
    # ``validate_sql_snippet`` below.
    #
    # The Bug #4 benchmark-leakage firewall is deliberately NOT run here
    # (see leakage.py ``_PATCH_TEXT_FIELDS`` scoping comment). The pre-
    # mining arbiter filter is the source gate; the firewall would
    # double-gate and reject legitimate structural primitives like
    # ``SUM(revenue)`` whose fingerprint happens to match a benchmark.

    result: dict = {
        "total_candidates": 0,
        "total_seeded": 0,
        "total_rejected": 0,
        # ``firewall_rejected`` retained as a zero for back-compat with
        # observability consumers reading this shape; the firewall no
        # longer runs in this path.
        "firewall_rejected": 0,
        "validation_rejected": 0,         # EXPLAIN / execution validator
        "ngram_rejected": 0,              # duplicate of an already-seeded snippet
        "measures_seeded": 0,
        "filters_seeded": 0,
        "expressions_seeded": 0,
        "skipped_reason": None,
        # Per-candidate rejection diagnostics — bounded list so the
        # pretty summary block can explain WHY candidates died without
        # operators having to grep INFO logs. Shape: list of
        # ``{sql_prefix, snippet_type, gate, reason}``.
        "rejected_examples": [],
    }

    # Bounded so a pathological pool doesn't balloon the result dict /
    # write_stage payload. 10 is plenty for diagnostics; the summary
    # renders at most 3.
    _MAX_REJECTED_EXAMPLES = 10

    def _record_rejection(
        sql_raw: str, snippet_type: str, gate: str, reason: str,
    ) -> None:
        if len(result["rejected_examples"]) >= _MAX_REJECTED_EXAMPLES:
            return
        result["rejected_examples"].append({
            "sql_prefix": (sql_raw or "")[:120],
            "snippet_type": snippet_type,
            "gate": gate,
            "reason": (reason or "")[:200],
        })

    parsed = config.get("_parsed_space", config)
    existing_snippets = parsed.get("instructions", {}).get("sql_snippets", {})
    if not isinstance(existing_snippets, dict):
        existing_snippets = {}

    # Headroom-based gate (replaces the old 5-snippet skip threshold).
    #
    #   headroom = MAX_SQL_SNIPPETS (200) - existing_sql_snippets - LEVER_RESERVE
    #
    # The ``LEVER_RESERVE`` (default 50) holds back budget for the lever
    # loop's iterative additions later in the optimisation run. Seeding
    # only contributes up to ``headroom``; when headroom hits zero we
    # skip seeding entirely so the lever loop retains its runway.
    #
    # ``count_sql_snippets`` intentionally counts only the three
    # sql_snippet buckets (measures + filters + expressions). The
    # Databricks docs' 200-limit also covers join_specs + table
    # descriptions, but the code's validator enforces a different
    # split today; reconciling that is tracked as a separate issue.
    existing_count = count_sql_snippets(parsed)
    headroom = max(
        0,
        MAX_SQL_SNIPPETS - existing_count - SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
    )
    remaining_snippet_budget = headroom  # loop-level cap (was: MAX - existing)

    if headroom == 0:
        _lines = [_section("SQL EXPRESSION SEEDING", "-")]
        _lines.append(_kv("Existing snippets", existing_count))
        _lines.append(_kv(
            "Status",
            f"Skipped (insufficient headroom: "
            f"existing={existing_count}, "
            f"reserve={SQL_EXPRESSION_SEEDING_LEVER_RESERVE}, "
            f"cap={MAX_SQL_SNIPPETS})",
        ))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        result["skipped_reason"] = "insufficient_headroom_for_seeding"
        return result

    write_stage(
        spark, run_id, "SQL_EXPRESSION_SEEDING", "STARTED",
        task_key="sql_expression_seeding", catalog=catalog, schema=schema,
    )

    try:
        benchmark_candidates = _mine_sql_expression_candidates(benchmarks, metadata_snapshot)
        # Phase 3.R7: expose the alias-rebind drop counts on ``result``
        # so the pretty summary can attribute why candidate N dropped to
        # N-k (undeclared alias in the source benchmark query).
        result["rebind_dropped"] = getattr(
            _mine_sql_expression_candidates, "last_rebind_dropped", 0,
        )
        result["rebind_dropped_examples"] = getattr(
            _mine_sql_expression_candidates, "last_rebind_dropped_examples", [],
        )
        schema_candidates = _discover_schema_sql_expressions(metadata_snapshot)

        all_candidates = benchmark_candidates + schema_candidates

        seen_sqls: set[str] = set()
        deduped: list[dict] = []
        for c in all_candidates:
            key = c["sql"].lower()
            if any(_ngram_similarity(key, s) > 0.85 for s in seen_sqls):
                continue
            seen_sqls.add(key)
            deduped.append(c)

        deduped = deduped[:SQL_EXPRESSION_SEEDING_MAX_CANDIDATES]
        result["total_candidates"] = len(deduped)

        if not deduped:
            _lines = [_section("SQL EXPRESSION SEEDING", "-")]
            _lines.append(_kv("Candidates found", 0))
            _lines.append(_kv("Status", "No candidates"))
            _lines.append(_bar("-"))
            print("\n".join(_lines))
            write_stage(
                spark, run_id, "SQL_EXPRESSION_SEEDING", "COMPLETE",
                task_key="sql_expression_seeding",
                detail=result, catalog=catalog, schema=schema,
            )
            return result

        deduped = _enrich_candidates_with_llm(deduped, metadata_snapshot, w=w)

        existing_sql_set: set[str] = set()
        for snippet_type in ("measures", "filters", "expressions"):
            for item in (existing_snippets.get(snippet_type, []) or []):
                sql_val = item.get("sql", [])
                sql_str = sql_val[0] if isinstance(sql_val, list) and sql_val else str(sql_val)
                existing_sql_set.add(sql_str.lower())

        applied_snippets: list[dict] = []
        type_key_map = {"measure": "measures", "filter": "filters", "expression": "expressions"}
        working_snippets_seed = copy.deepcopy(existing_snippets) if existing_snippets else {}

        for candidate in deduped:
            sql_raw = candidate["sql"]
            snippet_type = candidate["snippet_type"]

            if len(applied_snippets) >= remaining_snippet_budget:
                logger.info(
                    "SQL expression seeding: stopping — snippet budget exhausted (%d/%d)",
                    existing_count + len(applied_snippets), MAX_SQL_SNIPPETS,
                )
                break

            if any(_ngram_similarity(sql_raw.lower(), e) > 0.85 for e in existing_sql_set):
                result["ngram_rejected"] += 1
                result["total_rejected"] += 1
                _record_rejection(
                    sql_raw, snippet_type, "ngram",
                    "duplicate of an already-seeded snippet (>0.85 similarity)",
                )
                continue

            # Bug #4 firewall REMOVED at this call site — the mining source
            # has already been filtered to arbiter-approved baseline rows
            # (verdict == ``both_correct``, see
            # ``_extract_arbiter_approved_benchmarks``) which guarantees
            # the benchmark's ``expected_sql`` is gold-standard, and
            # ``validate_sql_snippet`` below still runs EXPLAIN+execute
            # against the warehouse. Adding the firewall here would
            # double-gate and reject legitimate structural patterns
            # (``SUM(revenue)`` is STRUCTURE, not an ANSWER — see
            # docstring above ``_PATCH_TEXT_FIELDS`` in leakage.py).
            _valid_result = validate_sql_snippet(
                sql_raw, snippet_type, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=schema,
                w=w, warehouse_id=warehouse_id,
            )
            is_valid = _valid_result[0]
            err = _valid_result[1]
            prefixed_sql = _valid_result[2] if len(_valid_result) > 2 else sql_raw
            if not is_valid:
                logger.info("SQL expression candidate rejected: %s — %s", sql_raw[:80], err)
                result["validation_rejected"] += 1
                result["total_rejected"] += 1
                _record_rejection(sql_raw, snippet_type, "validation", err or "")
                continue

            snippet_entry = {
                "id": generate_genie_id(),
                "sql": [prefixed_sql],
                "display_name": candidate.get("display_name", ""),
                "synonyms": candidate.get("synonyms", []),
                "instruction": [candidate.get("instruction", "")] if candidate.get("instruction") else [],
            }
            if candidate.get("alias") and snippet_type != "filter":
                snippet_entry["alias"] = candidate["alias"]

            type_key = type_key_map[snippet_type]
            items = working_snippets_seed.setdefault(type_key, [])
            items.append(snippet_entry)

            applied_snippets.append({
                "snippet_type": snippet_type,
                "type_key": type_key,
                "sql": sql_raw,
                "display_name": candidate.get("display_name", ""),
                "target_table": candidate.get("target_table", "sql_snippet"),
                "snippet_id": snippet_entry["id"],
            })

            count_key = f"{type_key}_seeded"
            result[count_key] = result.get(count_key, 0) + 1
            existing_sql_set.add(sql_raw.lower())

        if applied_snippets:
            patch_copy = copy.deepcopy(parsed)
            patch_copy.setdefault("instructions", {})["sql_snippets"] = working_snippets_seed
            try:
                patch_space_config(w, space_id, patch_copy)
                parsed.setdefault("instructions", {})["sql_snippets"] = working_snippets_seed
            except Exception:
                logger.warning("SQL expression seeding: PATCH failed", exc_info=True)
                result["total_seeded"] = 0
                result["measures_seeded"] = 0
                result["filters_seeded"] = 0
                result["expressions_seeded"] = 0
                write_stage(
                    spark, run_id, "SQL_EXPRESSION_SEEDING", "FAILED",
                    task_key="sql_expression_seeding",
                    error_message="PATCH failed",
                    catalog=catalog, schema=schema,
                )
                return result

            for idx, snippet_entry in enumerate(applied_snippets):
                write_patch(
                    spark, run_id, 0, 0, idx,
                    {
                        "patch_type": "proactive_sql_expression",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": snippet_entry.get("target_table", "sql_snippet"),
                        "patch": {
                            "snippet_type": snippet_entry["snippet_type"],
                            "sql": snippet_entry["sql"],
                            "display_name": snippet_entry["display_name"],
                        },
                        "command": None,
                        "rollback": None,
                        "proposal_id": f"proactive_sql_expr_{idx}",
                    },
                    catalog, schema,
                )

        result["total_seeded"] = len(applied_snippets)

        _lines = [_section("SQL EXPRESSION SEEDING", "-")]
        _lines.append(_kv("Candidates evaluated", result["total_candidates"]))
        # Phase 3.R7: alias-rebind diagnostics. Shown only when something
        # was dropped so clean runs stay compact.
        rebind_dropped = result.get("rebind_dropped", 0) or 0
        if rebind_dropped:
            _lines.append(_kv(
                "  Alias-rebind dropped",
                rebind_dropped,
                indent=4,
            ))
            for ex in (result.get("rebind_dropped_examples") or [])[:3]:
                _lines.append(_kv(f"    e.g. {ex}", "", indent=6))
        _lines.append(_kv("Seeded", result["total_seeded"]))
        _lines.append(_kv("  Measures", result["measures_seeded"]))
        _lines.append(_kv("  Filters", result["filters_seeded"]))
        _lines.append(_kv("  Expressions", result["expressions_seeded"]))
        _lines.append(_kv("Rejected", result["total_rejected"]))
        _lines.append(_kv("  Firewall (leakage)", result["firewall_rejected"]))
        _lines.append(_kv("  Validation (EXPLAIN)", result["validation_rejected"]))
        _lines.append(_kv("  Ngram duplicate", result["ngram_rejected"]))
        # Per-candidate rejection reasons — cheap observability so the next
        # diagnosis doesn't require grepping the job log. We print up to
        # 3 examples; the full bounded list stays on the result dict.
        _rejected_examples = result.get("rejected_examples") or []
        if _rejected_examples:
            _lines.append(_kv("Rejection examples (up to 3)", ""))
            for _ex in _rejected_examples[:3]:
                _sql_prefix = (_ex.get("sql_prefix") or "").strip()
                _reason = (_ex.get("reason") or "").strip()
                _gate = _ex.get("gate") or ""
                _lines.append(_kv(
                    f"  [{_gate}] {_sql_prefix[:60]}",
                    _reason[:140],
                    indent=4,
                ))
        _lines.append(_bar("-"))
        print("\n".join(_lines))

        write_stage(
            spark, run_id, "SQL_EXPRESSION_SEEDING", "COMPLETE",
            task_key="sql_expression_seeding",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("SQL_EXPRESSION_SEEDING FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "SQL_EXPRESSION_SEEDING", "FAILED",
            task_key="sql_expression_seeding",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


def _run_sql_expression_seeding(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Decoupled repair + seed: always repair existing, seed only when thin.

    This function intentionally keeps its name and call signature to stay
    compatible with all existing callers. The two sub-steps:

    1. :func:`_repair_existing_sql_snippets` runs unconditionally and
       normalizes every stored snippet to fully-qualified form. This
       fixes the bug that caused the failing run — snippets stored with
       short-form prefixes were rejected by Genie's serving path.
    2. :func:`_seed_new_sql_snippets` runs the legacy threshold-gated
       seeding path. No behaviour change.

    Returns a backward-compatible dict that carries both old flat keys
    (``total_candidates`` / ``total_seeded`` / …, used by callers that
    branch on whether new snippets were added) **and** a nested
    ``"repair"`` / ``"seed"`` structure for observability.
    """
    repair_result = _repair_existing_sql_snippets(
        w, spark, run_id, space_id, config=config,
        metadata_snapshot=metadata_snapshot,
        catalog=catalog, schema=schema,
        warehouse_id=warehouse_id,
    )
    # If repair rewrote any snippets, downstream stages need the fresh
    # snapshot. The harness-level caller already refetches after this
    # whole stage when new snippets are seeded; repair feeds the same
    # path because we mutated ``parsed`` in place inside the repair.
    seed_result = _seed_new_sql_snippets(
        w, spark, run_id, space_id, config=config,
        metadata_snapshot=metadata_snapshot,
        benchmarks=benchmarks,
        catalog=catalog, schema=schema,
        warehouse_id=warehouse_id,
    )
    # Preserve legacy flat shape for callers that look at total_seeded /
    # total_candidates; attach the structured summary alongside.
    return {
        **seed_result,
        "repair": repair_result,
        "seed": seed_result,
    }


# ── Stage 2.5b: PREPARE LEVER LOOP ──────────────────────────────────


def _prepare_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    *,
    benchmarks: list[dict] | None = None,
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

    # ── 2b. Catalog-level metric-view detection (PR 19) ──────────────
    # Genie's serialized space sometimes omits the ``column_type='measure'``
    # / ``is_measure`` flags on metric-view columns and lists MVs under
    # ``data_sources.tables`` with no ``data_sources.metric_views`` entry,
    # which makes the entire MV machinery (effective identifier set,
    # ``build_metric_view_measures`` map, ``has_metric_view`` trait,
    # MEASURE() auto-wrap) blind. Probing the catalog with
    # ``DESCRIBE TABLE EXTENDED ... AS JSON`` is the only reliable way
    # to recover; we cache the parsed YAML on the config so every
    # downstream stage sees the same answer without re-running DESCRIBE.
    # Idempotent: skipped when the cache is already populated (e.g. the
    # snapshot was warmed by a prior preflight run).
    if not (
        isinstance(config.get("_metric_view_yaml"), dict)
        and config["_metric_view_yaml"]
    ):
        _yamls: dict[str, dict] = {}
        _outcomes: dict[str, str] = {}
        try:
            from genie_space_optimizer.common.metric_view_catalog import (
                detect_metric_views_via_catalog_with_outcomes,
                summarize_outcomes,
            )
            _warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "")
            _, _yamls, _outcomes = detect_metric_views_via_catalog_with_outcomes(
                spark,
                table_refs,
                w=w,
                warehouse_id=_warehouse_id,
                catalog=catalog,
                schema=schema,
            )
        except Exception as exc:
            logger.warning(
                "Catalog metric-view detection failed for %s "
                "(non-fatal, MV-aware features may be degraded): %s: %s",
                run_id, type(exc).__name__, exc,
            )
        if _yamls:
            config["_metric_view_yaml"] = _yamls
            _ps = config.get("_parsed_space")
            if isinstance(_ps, dict):
                _ps["_metric_view_yaml"] = _yamls

        # PR 23 — Always emit a one-line outcome summary, even when
        # zero MVs were detected, so log readers can distinguish
        # "no MVs in this space" from "DESCRIBE silently failed on
        # every ref". Counts are stable column-by-column so an alert
        # rule keyed on ``describe_error`` rising will fire correctly.
        if table_refs:
            try:
                _counts = summarize_outcomes(_outcomes)
                logger.info(
                    "Catalog metric-view detection summary for %s: "
                    "refs=%d, detected=%d, describe_error=%d, empty_result=%d, "
                    "no_envelope=%d, no_view_text=%d, yaml_parse_error=%d, "
                    "not_mv_shape=%d",
                    run_id, len(table_refs),
                    _counts["detected"],
                    _counts["describe_error"],
                    _counts["empty_result"],
                    _counts["no_envelope"],
                    _counts["no_view_text"],
                    _counts["yaml_parse_error"],
                    _counts["not_mv_shape"],
                )
            except Exception:
                logger.debug(
                    "MV detection summary aggregation failed", exc_info=True,
                )

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

    # ── 3b. RLS audit via information_schema (best-effort) ───────────
    # Populate config["_rls_audit"] so auto_apply_prompt_matching can use
    # the view-aware RLS verdict (inherited RLS, dynamic views) that the
    # serialized_space field check alone can't see. Fail-open: any
    # probe/query failure logs a WARNING and leaves verdicts as
    # "unknown", which the scorer treats as clean by default
    # (STRICT_RLS_MODE flips this to tainted).
    try:
        from genie_space_optimizer.iq_scan import collect_rls_audit
        space_tables = (_ds.get("tables") or []) + (_ds.get("metric_views") or [])
        config["_rls_audit"] = collect_rls_audit(
            space_tables,
            spark=spark, w=w,
            warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        )
        _tainted_count = sum(
            1 for v in config["_rls_audit"].values()
            if v.get("verdict") == "tainted"
        )
        _unknown_count = sum(
            1 for v in config["_rls_audit"].values()
            if v.get("verdict") == "unknown"
        )
        print(
            f"  [RLS AUDIT] {len(config['_rls_audit'])} tables scanned; "
            f"{_tainted_count} tainted, {_unknown_count} unknown"
        )
    except Exception as exc:
        logger.warning(
            "RLS audit failed (non-fatal, proceeding without view-aware "
            "RLS detection): %s: %s",
            type(exc).__name__, exc,
        )
        config["_rls_audit"] = {}

    # ── 4–7. Stage 2.5 prompt matching + propagation wait ────────────
    if ENABLE_PROMPT_MATCHING_AUTO_APPLY:
        try:
            pm_result = _run_prompt_matching_setup(
                w, spark, run_id, space_id, config, catalog, schema,
                benchmarks=benchmarks,
            )
            logger.info(
                "Prompt matching complete: FA=%d, EM=%d, total=%d",
                pm_result.get("format_assistance_count", 0),
                pm_result.get("entity_matching_count", 0),
                pm_result.get("total_changes", 0),
            )

            if pm_result.get("total_changes", 0) > 0:
                em_enabled = pm_result.get("entity_matching_count", 0)
                em_disabled = pm_result.get("entity_matching_disabled_count", 0)
                has_entity_matching = (em_enabled + em_disabled) > 0
                wait_time = (
                    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_entity_matching
                    else PROPAGATION_WAIT_SECONDS
                )
                print(
                    f"\n-- PROPAGATION WAIT " + "-" * 32 + "\n"
                    f"  Changes applied: {pm_result.get('total_changes', 0)}\n"
                    f"  Entity matching changes:"
                    f" +{em_enabled} / -{em_disabled}\n"
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
    *,
    held_out_benchmarks: list[dict] | None = None,
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
        config = _prepare_lever_loop(
            w, spark, run_id, space_id, catalog, schema,
            benchmarks=benchmarks,
        )

        uc_columns = config.get("_uc_columns", [])
        metadata_snapshot = config.get("_parsed_space", config)
        data_profile = metadata_snapshot.get("_data_profile", {})
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

        # ── 2. Proactive Enrichment sub-steps ─────────────────────────────
        import mlflow as _mlflow_enr

        uc_schema = f"{catalog}.{schema}"
        # Tier 4: MLflow run naming v2 — ``<run_short>/enrichment/snapshot``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags,
            enrichment_run_name,
        )
        _effective_run_id = optimization_run_id or run_id
        _enr_run_name = enrichment_run_name(_effective_run_id, detail="snapshot")
        with _mlflow_enr.start_run(run_name=_enr_run_name):
            _mlflow_enr.set_tags({
                **_v2_tags(
                    _effective_run_id,
                    space_id=space_id,
                    stage="enrichment_snapshot",
                ),
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

            # ── 5a. Instruction prose mining & promotion (miner-first) ────
            # Runs BEFORE proactive seed/expand (per Task C.5 ordering) so
            # legacy ALL-CAPS prose is normalised into the canonical 5-
            # section form before seed/expand see it.
            #
            # ``benchmarks`` here is used ONLY as the firewall corpus for
            # the prose miner's example_qsql target (it does not mine
            # from benchmarks). Pass the full train+held_out corpus so
            # the example-SQL firewall can catch held-out leakage.
            _miner_out = _run_instruction_prose_mining(
                w, spark, run_id, space_id, config, metadata_snapshot,
                catalog, schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                benchmarks=list(benchmarks) + list(held_out_benchmarks or []),
            )
            if _miner_out["total_applied"] or _miner_out["keep_in_prose_count"]:
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── 5b. Proactive instruction seeding + expand ────────────────
            instruction_result = _run_proactive_instruction_seeding(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if (
                instruction_result.get("instructions_seeded")
                or instruction_result.get("instructions_expanded")
            ):
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── 5c. Pre-flight example_sql synthesis (fills to 20) ────────
            # Two paths behind GSO_UNIFIED_EXAMPLE_SQL_GENERATION (default ON):
            #
            #   UNIFIED (Phase 4.R4): calls ``generate_example_sqls``, which
            #   uses the mature benchmark engine with all its quality
            #   features (MEASURE auto-wrap, field-drift correction,
            #   correction LLM retry, arbiter approval, leakage firewall).
            #   Option A fallback: if unified yields < GSO_UNIFIED_MIN_SURVIVORS,
            #   the archetype-templated preflight path runs to fill the gap.
            #
            #   LEGACY: archetype-templated per-candidate synthesis. Retained
            #   as a rollback lever; flip GSO_UNIFIED_EXAMPLE_SQL_GENERATION
            #   to false to restore the pre-unification behaviour.
            #
            # Both paths share the same last-mile firewall at
            # ``_apply_proactive_example_sqls``, which MUST see the full
            # benchmark corpus (train + held_out) so the SQL fingerprint
            # check catches held-out leakage. See
            # ``docs/example-sql-isolation.md``.
            _full_firewall_corpus = list(benchmarks) + list(held_out_benchmarks or [])
            preflight_example_result: dict = {}
            unified_example_result: dict = {}
            if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:
                _unified_enabled = os.environ.get(
                    "GSO_UNIFIED_EXAMPLE_SQL_GENERATION", "true",
                ).lower() in {"1", "true", "yes", "on"}
                _unified_min_survivors = int(
                    os.environ.get("GSO_UNIFIED_MIN_SURVIVORS", "5") or "5",
                )
                if _unified_enabled:
                    unified_example_result = _run_unified_example_sql_generation(
                        w=w, spark=spark, run_id=run_id, space_id=space_id,
                        config=config, metadata_snapshot=metadata_snapshot,
                        uc_columns=uc_columns, domain=domain,
                        catalog=catalog, schema=schema,
                        full_firewall_corpus=_full_firewall_corpus,
                        data_profile=data_profile,
                    )
                    if unified_example_result.get("applied", 0) > 0:
                        config = fetch_space_config(w, space_id)
                        config["_uc_columns"] = uc_columns
                        metadata_snapshot = config.get("_parsed_space", config)
                        metadata_snapshot["_data_profile"] = data_profile
                        if uc_columns:
                            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

                # Archetype fallback (Option A) OR legacy path.
                _need_fallback = (
                    not _unified_enabled
                    or unified_example_result.get("applied", 0) < _unified_min_survivors
                )
                if _need_fallback:
                    try:
                        from genie_space_optimizer.optimization.preflight_synthesis import (
                            run_preflight_example_synthesis,
                        )
                        if _unified_enabled and unified_example_result.get("applied", 0) > 0:
                            logger.info(
                                "unified generator yielded %d < %d — running "
                                "archetype fallback to fill the gap",
                                unified_example_result.get("applied", 0),
                                _unified_min_survivors,
                            )
                        preflight_example_result = run_preflight_example_synthesis(
                            w, spark, run_id, space_id, config, metadata_snapshot,
                            benchmarks=_full_firewall_corpus,
                            catalog=catalog, schema=schema,
                            warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                        )
                        if preflight_example_result.get("applied", 0) > 0:
                            config = fetch_space_config(w, space_id)
                            config["_uc_columns"] = uc_columns
                            metadata_snapshot = config.get("_parsed_space", config)
                            metadata_snapshot["_data_profile"] = data_profile
                            if uc_columns:
                                enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
                    except Exception:
                        logger.warning(
                            "preflight example synthesis (fallback) raised; "
                            "continuing without it",
                            exc_info=True,
                        )

            # ── 5d. SQL Expression REPAIR + SEEDING ───────────────────────
            # Repair runs unconditionally; seeding is headroom-gated.
            #
            # Mining source gate: only baseline benchmarks whose arbiter
            # verdict is ``both_correct`` qualify — the ``expected_sql``
            # on those rows is proven correct (Genie and GT agreed).
            # ``genie_correct`` rows are explicitly EXCLUDED because
            # their ``expected_sql`` may be wrong (that verdict triggers
            # the separate ground-truth-repair path). First run / no
            # baseline -> empty subset -> only schema-discovery proposes.
            approved_benchmarks, _arbiter_verdict_counts = (
                _extract_arbiter_approved_benchmarks(
                    spark, run_id, catalog, schema, benchmarks,
                )
            )
            logger.info(
                "miner.arbiter_filter total=%d approved=%d verdicts=%s",
                len(benchmarks), len(approved_benchmarks),
                _arbiter_verdict_counts,
            )
            sql_expr_result = _run_sql_expression_seeding(
                w, spark, run_id, space_id, config=config,
                metadata_snapshot=metadata_snapshot,
                benchmarks=approved_benchmarks,
                catalog=catalog, schema=schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            )
            if (
                sql_expr_result.get("total_candidates", 0) > 0
                or sql_expr_result.get("repair", {}).get("rewritten", 0) > 0
            ):
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # Bug #4 — benchmark verbatim mining removed. Proposals for
            # example_sqls now come exclusively from AFS-gated structural
            # synthesis (Phase 3), never from copying benchmark expected_sql.
            mined_example_proposals: list = []

            # ── Summary ───────────────────────────────────────────────────
            # _miner_out carries per-target applied counts from the prose
            # mining & promotion step (see _run_instruction_prose_mining).
            # The earlier single `_instr_sql_applied` local was removed when
            # the miner grew multi-target support; sum every promoted target
            # so the summary reflects reality.
            total_enrichments = (
                enrichment_result.get("total_enriched", 0)
                + join_result.get("total_applied", 0)
                + (1 if meta_result.get("description_generated") else 0)
                + (1 if meta_result.get("questions_generated") else 0)
                + (1 if instruction_result.get("instructions_seeded") else 0)
                + _miner_out.get("sql_applied", 0)
                + _miner_out.get("join_applied", 0)
                + _miner_out.get("example_applied", 0)
                + _miner_out.get("desc_applied", 0)
                + _miner_out.get("synonym_applied", 0)
                + sql_expr_result.get("total_seeded", 0)
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
            _enr_summary.append(_kv("Instruction-derived SQL expressions", _miner_out.get("sql_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived join specs", _miner_out.get("join_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived example SQLs", _miner_out.get("example_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived table descriptions", _miner_out.get("desc_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived column synonyms", _miner_out.get("synonym_applied", 0)))
            _enr_summary.append(_kv("SQL expressions seeded", sql_expr_result.get("total_seeded", 0)))
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
                "enrichment.sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
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

        # ── 4. Post-enrichment evaluation (Tier 1.3) ──────────────────────
        # Enrichment mutates the Genie Space (descriptions, joins,
        # instructions, example SQLs). Without a fresh eval, Task 4 (lever
        # loop) would gate against the stale baseline accuracy while its
        # clustering reads post-enrichment rows — those two realities can
        # disagree arbitrarily. We run a single-pass eval (no confirmation)
        # and publish the result so Task 4 can consume it. Skipped when
        # nothing changed.
        post_enrichment_accuracy: float | None = None
        post_enrichment_scores: dict[str, float] = {}
        post_enrichment_evaluated_count: int | None = None
        post_enrichment_thresholds_met: bool = False
        if not enrichment_skipped and enrichment_model_id:
            try:
                _pe_setup = baseline_setup_scorers(
                    w, spark, space_id, run_id, catalog, schema, exp_name,
                    enrichment_model_id, domain,
                )
                # Tier 4: v2 name — ``<run_short>/enrichment/post_eval``.
                from genie_space_optimizer.common.mlflow_names import (
                    default_tags as _v2_tags_pe,
                    enrichment_run_name as _enrichment_run_name_pe,
                )
                _effective_run_id_pe = optimization_run_id or run_id
                _pe_eval = run_evaluation(
                    space_id, exp_name, 0, benchmarks,
                    domain, enrichment_model_id, "full",
                    _pe_setup["predict_fn"], _pe_setup["scorers"],
                    spark=spark, w=w,
                    catalog=catalog, gold_schema=schema,
                    uc_schema=f"{catalog}.{schema}",
                    run_name=_enrichment_run_name_pe(
                        _effective_run_id_pe, detail="post_eval",
                    ),
                    extra_tags=_v2_tags_pe(
                        _effective_run_id_pe,
                        space_id=space_id, stage="enrichment_post_eval",
                    ),
                )
                post_enrichment_accuracy = float(
                    _pe_eval.get("overall_accuracy", 0.0) or 0.0
                )
                _raw_scores = _pe_eval.get("scores", {})
                if isinstance(_raw_scores, dict):
                    post_enrichment_scores = {
                        str(k): float(v) for k, v in _raw_scores.items()
                        if v is not None
                    }
                _ec = _pe_eval.get("evaluated_count")
                if isinstance(_ec, (int, float)):
                    post_enrichment_evaluated_count = int(_ec)
                post_enrichment_thresholds_met = bool(
                    _pe_eval.get("thresholds_met", False)
                )
                _pe_lines = [_section("ENRICHMENT — POST-ENRICHMENT EVAL", "-")]
                _pe_lines.append(
                    _kv("Accuracy", f"{post_enrichment_accuracy:.1f}%")
                )
                _pe_lines.append(
                    _kv("Thresholds met", post_enrichment_thresholds_met)
                )
                _pe_lines.append(_bar("-"))
                print("\n".join(_pe_lines))
            except Exception:
                logger.warning(
                    "Post-enrichment eval failed — Task 4 will fall back to "
                    "baseline accuracy",
                    exc_info=True,
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
                "sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
                "examples_mined": len(mined_example_proposals),
                "post_enrichment_accuracy": post_enrichment_accuracy,
                "post_enrichment_thresholds_met": post_enrichment_thresholds_met,
            },
        )

        return {
            "enrichment_model_id": enrichment_model_id,
            "enrichment_skipped": enrichment_skipped,
            "config": config,
            "post_enrichment_accuracy": post_enrichment_accuracy,
            "post_enrichment_scores": post_enrichment_scores,
            "post_enrichment_model_id": enrichment_model_id,
            "post_enrichment_evaluated_count": post_enrichment_evaluated_count,
            "post_enrichment_thresholds_met": post_enrichment_thresholds_met,
            "summary": {
                "descriptions_enriched": enrichment_result.get("total_enriched", 0),
                "joins_discovered": join_result.get("total_applied", 0),
                "description_generated": bool(meta_result.get("description_generated")),
                "questions_generated": bool(meta_result.get("questions_generated")),
                "instructions_seeded": bool(instruction_result.get("instructions_seeded")),
                "sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
                "examples_mined": len(mined_example_proposals),
                "total_enrichments": total_enrichments,
                "post_enrichment_accuracy": post_enrichment_accuracy,
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
    root_cause: str = "",
    blame_set: Any = None,
    source_cluster_ids: list[str] | None = None,
    source_cluster_signatures: list[str] | None = None,
) -> dict:
    """Build a structured reflection dict for the adaptive loop memory.

    *reflection_text* is a 2-3 sentence verbal explanation of why the
    iteration succeeded or failed (Reflexion-style semantic gradient).

    *refinement_mode* is ``"in_plan"`` when the lever direction was correct
    but caused collateral regressions, or ``"out_of_plan"`` when the
    approach fundamentally did not work (AdaPlanner-style classification).

    Phase C2 added the identity fields (``root_cause``, ``blame_set``,
    ``source_cluster_ids``, ``lever_set``, ``rollback_class``) that
    Phases D1-D3 use to build the DO-NOT-RETRY forbidden set without
    parsing the free-form ``action`` string. Callers that didn't supply
    identity fields still produce a valid entry; the fields are simply
    empty and won't match anything in the collision guard.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        classify_rollback_reason,
    )

    score_deltas = {
        k: new_scores.get(k, 0.0) - prev_scores.get(k, 0.0)
        for k in set(prev_scores) | set(new_scores)
    }
    prev_acc = sum(prev_scores.values()) / max(len(prev_scores), 1)
    new_acc = sum(new_scores.values()) / max(len(new_scores), 1)

    patch_summary_parts: list[str] = []
    do_not_retry: list[str] = []
    # T3.1: minimum-viable leave-one-out attribution. Without running
    # actual re-evals (expensive, would need full harness wiring), we
    # heuristically rank patch suspicion by the T2.4 collateral-risk
    # flag. Patches that touched assets depended on by many passing
    # questions are the prime suspects when a rollback is needed;
    # low-risk patches are kept out of the DO-NOT-RETRY set so the
    # strategist can re-propose them in a different combination.
    #
    # When every patch is low-risk, fall back to blanket DO-NOT-RETRY
    # (the historical behaviour) — at least one of them regressed.
    _high_risk = [p for p in patches if p.get("high_collateral_risk")]
    _suspicious = _high_risk if _high_risk else patches
    _suspicious_keys = {
        (
            p.get("type", p.get("patch_type", "?")),
            p.get("target", p.get("target_object", "?")),
        )
        for p in _suspicious
    }
    for p in patches:
        ptype = p.get("type", p.get("patch_type", "?"))
        target = p.get("target", p.get("target_object", "?"))
        patch_summary_parts.append(f"{ptype} on {target}")
        if not accepted and (ptype, target) in _suspicious_keys:
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

    # Normalise blame_set so reflection entries share the same shape as
    # :func:`_filter_tried_clusters` expects — this keeps the DO-NOT-RETRY
    # forbidden set symmetric with the tried-clusters bookkeeping.
    _blame_norm = _normalise_blame(blame_set)

    _lever_set = sorted({int(l) for l in levers}) if levers else []

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
        "rollback_class": classify_rollback_reason(rollback_reason).value,
        "do_not_retry": do_not_retry,
        "affected_question_ids": affected_question_ids or [],
        "fixed_questions": sorted(_prev - _new),
        "still_failing": sorted(_prev & _new),
        "new_regressions": sorted(_new - _prev),
        "reflection_text": reflection_text,
        "refinement_mode": refinement_mode,
        "escalation_handled": escalation_handled,
        # Phase C2 identity fields.
        "root_cause": root_cause or "",
        "blame_set": _blame_norm,
        "source_cluster_ids": list(source_cluster_ids or []),
        "lever_set": _lever_set,
        # T2.1: iteration-independent cluster identity. Unlike the
        # pretty ``source_cluster_ids`` (which churn as H001→H001
        # between iterations for unrelated clusters), each signature
        # is a sha1 of ``base_question_ids + root_cause + blame`` so
        # "the same cluster" joins across iterations.
        "source_cluster_signatures": list(source_cluster_signatures or []),
    }


# Phase C1 retired the earlier ``_is_schema_fatal_patch_error`` shim in
# favour of :class:`RollbackClass` and :func:`classify_rollback_reason`
# from ``optimization/rollback_class``. A deterministic schema rejection
# now surfaces as ``RollbackClass.SCHEMA_FAILURE`` so all gating code
# (infra retry budget, diminishing-returns filter, DO-NOT-RETRY guard)
# reads one source of truth.


def _is_schema_fatal_patch_error(error: Any) -> bool:
    """Backwards-compatible alias for tests and any legacy call sites.

    Returns True when ``error`` classifies as
    :attr:`RollbackClass.SCHEMA_FAILURE`. New code should call
    :func:`classify_rollback_reason` directly.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
        classify_rollback_reason,
    )

    if not error:
        return False
    return classify_rollback_reason(str(error)) == RollbackClass.SCHEMA_FAILURE


def _diminishing_returns(
    reflection_buffer: list[dict],
    epsilon: float | None = None,
    lookback: int | None = None,
) -> bool:
    """Return True if none of the last *lookback* actionable iterations
    achieved a mean accuracy improvement >= *epsilon*.

    Rolled-back iterations count as zero improvement, which correctly
    signals the optimizer is stuck. Phase C3: only entries whose
    ``rollback_class`` is ``CONTENT_REGRESSION`` (or which were
    accepted — those carry an ``accuracy_delta``) participate. Infra,
    schema, escalation, and other non-content rollbacks carry no
    content signal and are skipped so they don't artificially trip
    diminishing-returns before the loop has had a real chance to try
    a strategy.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    if epsilon is None:
        epsilon = DIMINISHING_RETURNS_EPSILON
    if lookback is None:
        lookback = DIMINISHING_RETURNS_LOOKBACK

    def _is_content_signal(r: dict) -> bool:
        if r.get("escalation_handled"):
            return False
        if r.get("accepted"):
            return True
        return r.get("rollback_class") == RollbackClass.CONTENT_REGRESSION.value

    content_signal = [r for r in reflection_buffer if _is_content_signal(r)]
    recent = content_signal[-lookback:]
    if len(recent) < lookback:
        return False

    for r in recent:
        if r.get("accepted") and r.get("accuracy_delta", 0.0) >= epsilon:
            return False
    return True


def _detect_divergence(
    reflection_buffer: list[dict],
    lookback: int = 4,
    min_sign_flips: int = 2,
) -> tuple[bool, str]:
    """T4.2: Detect accuracy *divergence* (thrashing).

    A loop that alternates accept → rollback → accept → rollback is
    making negative progress per unit cost — every accepted iteration
    is followed by another that takes it back. Stop the loop rather
    than burn more budget.

    Returns ``(diverging, rationale)``. ``diverging=True`` when the
    sign of ``accuracy_delta`` flips at least ``min_sign_flips`` times
    in the last ``lookback`` content-signal iterations. Rationale is a
    human-readable explanation for logs.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    def _is_content_signal(r: dict) -> bool:
        if r.get("escalation_handled"):
            return False
        if r.get("accepted"):
            return True
        return r.get("rollback_class") == RollbackClass.CONTENT_REGRESSION.value

    content_signal = [r for r in reflection_buffer if _is_content_signal(r)]
    recent = content_signal[-lookback:]
    if len(recent) < lookback:
        return False, ""

    # Compute sign series on accuracy_delta. Rolled-back iterations
    # don't carry a real accuracy_delta (they revert the space), but
    # the recorded delta is what the eval produced and is the right
    # signal here — we're measuring "did the space keep trying both
    # directions?" not "what did we end up at?".
    deltas = [r.get("accuracy_delta", 0.0) for r in recent]
    signs = [1 if d > 0.5 else (-1 if d < -0.5 else 0) for d in deltas]
    # Count sign flips (ignore zeros as non-signal).
    prev_sign = 0
    flips = 0
    for s in signs:
        if s == 0:
            continue
        if prev_sign != 0 and s != prev_sign:
            flips += 1
        prev_sign = s
    if flips >= min_sign_flips:
        return (
            True,
            f"{flips} accuracy-sign flips across last {lookback} "
            f"content-signal iterations (deltas={[round(d, 1) for d in deltas]})",
        )
    return False, ""


# Phase D1: which lever sets is the router allowed to try for a given
# root cause? If every one of them has been tried and rolled back, the
# cluster is truly dead and we can drop it. Keep this conservative —
# narrower than the strategist's "also Lever N" hints so a single
# lever-set miss doesn't suppress the whole cluster prematurely.
_FEASIBLE_LEVER_SETS_BY_ROOT_CAUSE: dict[str, tuple[frozenset[int], ...]] = {
    # SQL-shape causes: primary Lever 6 (sql_snippet), secondary Lever 5
    # (example_sql). These are the only two ways to actually install a
    # missing filter / aggregation / measure without touching benchmarks.
    "missing_filter":           (frozenset({6}), frozenset({5})),
    "missing_scd_filter":       (frozenset({6}), frozenset({5})),
    "missing_temporal_filter":  (frozenset({6}), frozenset({5})),
    "wrong_filter_condition":   (frozenset({6}), frozenset({5})),
    "wrong_aggregation":        (frozenset({6}), frozenset({5})),
    "wrong_measure":            (frozenset({6}), frozenset({5})),
    "missing_aggregation":      (frozenset({6}), frozenset({5})),
    "missing_dimension":        (frozenset({6}), frozenset({5})),
    "wrong_grouping":           (frozenset({6}), frozenset({5})),
    # Descriptive causes: Lever 1 is primary; Lever 5 instructions also fit.
    "wrong_column":             (frozenset({1}), frozenset({5})),
    "wrong_table":              (frozenset({1}), frozenset({5})),
    "description_mismatch":     (frozenset({1}),),
    "missing_synonym":          (frozenset({1}),),
    # Join causes: Lever 4 primary; Lever 5 also suggested by the prompt.
    "wrong_join":               (frozenset({4}), frozenset({5})),
    "wrong_join_spec":          (frozenset({4}), frozenset({5})),
    "missing_join_spec":        (frozenset({4}), frozenset({5})),
    "wrong_join_type":          (frozenset({5}),),
    # TVF causes: Lever 3.
    "tvf_parameter_error":      (frozenset({3}), frozenset({5})),
    # Routing / instruction causes: Lever 5 only.
    "asset_routing_error":      (frozenset({5}),),
    "missing_instruction":      (frozenset({5}),),
    "ambiguous_question":       (frozenset({5}),),
}


def _feasible_lever_sets(ft: str) -> tuple[frozenset[int], ...]:
    """Return the tuple of lever sets the router could plausibly try for
    root cause *ft*. Unknown root causes fall back to an empty tuple,
    which means the 3-tuple suppression in :func:`_filter_tried_clusters`
    can never fire — safe default (only the explicit legacy 2-tuple
    will suppress the cluster).
    """
    return _FEASIBLE_LEVER_SETS_BY_ROOT_CAUSE.get(ft, ())


def _compute_forbidden_ag_set(
    reflection_buffer: list[dict],
) -> set[tuple[str, Any, frozenset[int]]]:
    """Build the DO-NOT-RETRY forbidden set from the reflection buffer.

    Only CONTENT_REGRESSION rollbacks contribute — infra / schema / other
    classes don't count as evidence that the strategy was wrong. Returns
    a set of ``(root_cause, blame_set_norm, frozenset(lever_set))`` tuples.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    forbidden: set[tuple[str, Any, frozenset[int]]] = set()
    for r in reflection_buffer:
        if r.get("accepted"):
            continue
        if r.get("escalation_handled"):
            continue
        if r.get("rollback_class") != RollbackClass.CONTENT_REGRESSION.value:
            continue
        rc = r.get("root_cause") or ""
        if not rc:
            continue
        blame = r.get("blame_set") or ""
        lever_set = r.get("lever_set") or []
        if not lever_set:
            continue
        forbidden.add((rc, blame, frozenset(int(l) for l in lever_set)))
    return forbidden


def _ag_collision_key(
    ag: dict,
    ag_root_cause: str,
    ag_blame_set: Any,
    lever_keys: list[str],
) -> tuple[str, Any, frozenset[int]] | None:
    """Build the collision key for an action group.

    Returns ``None`` when the AG lacks enough identity to meaningfully
    collide (no root cause or no lever set). Normalises blame the same
    way :func:`_build_reflection_entry` does so keys can be compared.
    """
    if not ag_root_cause:
        return None
    if not lever_keys:
        return None
    return (
        ag_root_cause,
        _normalise_blame(ag_blame_set),
        frozenset(int(lk) for lk in lever_keys),
    )


def _filter_tried_clusters(
    clusters: list[dict],
    tried_root_causes: set[tuple],
) -> list[dict]:
    """Remove clusters whose root cause was already tried and rolled back.

    *tried_root_causes* stores tuples recorded at rollback time. Supports
    both legacy 2-tuples ``(ft, blame)`` and new 3-tuples
    ``(ft, blame, frozenset_of_levers)``. A cluster is suppressed when
    EITHER:

    * the legacy ``(ft, blame)`` 2-tuple is present (truly-dead cluster
      — rolled back across multiple distinct lever sets per Phase D3), OR
    * every feasible lever set for the cluster's root cause has a matching
      3-tuple entry (lever-aware suppression — we've exhausted the router's
      options for this cluster).

    Phase D1 is what makes the 3-tuple bookkeeping actually effective;
    before D1 the function silently ignored 3-tuples entirely.
    """
    if not tried_root_causes:
        return clusters
    legacy_keys: set[tuple[str, Any]] = set()
    lever_keys: set[tuple[str, Any, frozenset]] = set()
    for entry in tried_root_causes:
        if len(entry) == 2:
            legacy_keys.add(entry)
        elif len(entry) >= 3:
            lever_keys.add((entry[0], entry[1], entry[2]))

    filtered: list[dict] = []
    for c in clusters:
        ft = c.get("asi_failure_type") or c.get("root_cause", "other")
        blame = _normalise_blame(c.get("asi_blame_set"))
        if (ft, blame) in legacy_keys:
            continue
        feasible = _feasible_lever_sets(ft)
        if feasible and all(
            (ft, blame, frozenset(ls)) in lever_keys for ls in feasible
        ):
            continue
        filtered.append(c)
    return filtered


def _normalise_blame(blame_raw: Any) -> tuple[str, ...] | str:
    """Canonical blame representation used by reflection entries and the
    tried-clusters filter. Empty collections normalise to ``""`` (same
    shape as ``None``) so legacy 2-tuple keys written with an empty
    blame will match a cluster whose ``asi_blame_set`` is also empty.
    """
    if blame_raw is None:
        return ""
    if isinstance(blame_raw, (list, tuple, set, frozenset)):
        items = tuple(sorted(str(b) for b in blame_raw if str(b)))
        return items or ""
    return str(blame_raw)


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


def _extract_arbiter_approved_benchmarks(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    benchmarks: list[dict],
) -> tuple[list[dict], dict[str, int]]:
    """Return the subset of ``benchmarks`` whose baseline arbiter verdict
    was ``both_correct`` ONLY.

    Per docs/gsl-instruction-schema.md design decision:

    * ``both_correct`` — Genie and the benchmark's ``expected_sql`` both
      produce the same correct result. The ``expected_sql`` is gold
      standard and its patterns (aggregations, filters, derived
      expressions) are safe to mine into structured sql_snippets or
      join_specs.

    * ``genie_correct`` — Genie is right and the benchmark's
      ``expected_sql`` is WRONG (hence the separate repair path at
      :func:`_extract_arbiter_actions_from_baseline`). Mining from
      ``expected_sql`` here would ingest bad SQL. **EXCLUDED.**

    * ``ground_truth_correct``, ``neither_correct``, ``skipped``,
      anything else — EXCLUDED (conservative; only ``both_correct``
      qualifies as unambiguously safe source content).

    Empty-baseline fallback: if no baseline iteration is persisted yet
    (first GSO run on a fresh space, or baseline crashed), returns
    ``([], {})`` — caller falls back to schema-discovery-only mining.

    Returns
    -------
    (approved_benchmarks, verdict_counts)
        ``approved_benchmarks`` is the filtered subset.
        ``verdict_counts`` is a per-verdict tally across all matched
        rows (for the single-line diagnostic log).
    """
    approved: list[dict] = []
    verdict_counts: dict[str, int] = {}

    if not benchmarks:
        return approved, verdict_counts

    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not baseline_iter:
        return approved, verdict_counts

    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return approved, verdict_counts
    if not isinstance(rows_json, list):
        return approved, verdict_counts

    # Build {question_id: verdict} from baseline rows.
    verdict_by_qid: dict[str, str] = {}
    for row in rows_json:
        av = _get_arbiter_verdict(row)
        qid = _get_question_id(row)
        verdict_counts[av] = verdict_counts.get(av, 0) + 1
        if qid:
            verdict_by_qid[qid] = av

    for b in benchmarks:
        qid = str(b.get("id") or b.get("question_id") or "").strip()
        if not qid:
            continue
        if verdict_by_qid.get(qid) == "both_correct":
            approved.append(b)

    return approved, verdict_counts


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


def _is_quarantined_qid(qid: str, quarantined: set[str]) -> bool:
    """Return True when *qid* (or its base form) is in the quarantine set.

    The benchmark-suffix scheme produces qids like ``retail_..._002:v2``
    and ``retail_..._002:v3`` whose base qid is ``retail_..._002``.
    Exact-equality match misses these, leaking quarantined questions
    through the row filter and the cluster prune. Strip the ``:vN``
    suffix on either side so the base qid is the canonical key.

    Defensive on both directions: a quarantined entry may itself be
    a base qid (the common case) or an already-suffixed string (when
    a previous iteration recorded the suffixed variant directly).
    """
    if not qid or not quarantined:
        return False
    if qid in quarantined:
        return True
    if ":" in qid:
        base = qid.split(":", 1)[0]
        if base in quarantined:
            return True
    for q in quarantined:
        if ":" in q and q.split(":", 1)[0] == qid:
            return True
    return False


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

    T1.10: duplicate benchmark rows (same question_id appearing multiple
    times in the corpus) previously inflated the per-iteration entry count
    — e.g. `_003` appearing 3× produced 3 VerdictEntry rows with
    ``iteration=1`` and the persistence counter then reported
    "Failed 3/3 evals (3 consecutive)" after a single iteration. Group by
    ``(base_qid, iteration)`` and roll up the verdict: passing iff ALL
    trials in that iteration passed, otherwise the most common
    non-passing verdict wins. This ensures per-iteration count caps at 1.
    """
    _PASSING = {"both_correct"}
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

        trials_by_qid: dict[str, list[VerdictEntry]] = {}
        for row in rows_json:
            qid = _get_question_id(row)
            if qid == "?":
                continue
            base_qid = str(qid).split(":v")[0]
            trials_by_qid.setdefault(base_qid, []).append(
                VerdictEntry(
                    iteration=iteration_num,
                    verdict=_get_arbiter_verdict(row),
                    genie_sql=_get_genie_sql(row),
                    expected_sql=_get_expected_sql(row),
                    question_text=_get_question_text(row),
                    rationale=_get_arbiter_rationale(row),
                )
            )

        for base_qid, trial_entries in trials_by_qid.items():
            if all(t.verdict in _PASSING for t in trial_entries):
                rolled = trial_entries[0]
            else:
                from collections import Counter as _Counter
                _non_pass = [t for t in trial_entries if t.verdict not in _PASSING]
                _dominant_verdict = _Counter(t.verdict for t in _non_pass).most_common(1)[0][0]
                rolled = next(t for t in _non_pass if t.verdict == _dominant_verdict)
            history.setdefault(base_qid, []).append(rolled)

    return history


def _compute_convergence_state(
    entries: list,
    *,
    lookback: int = 4,
) -> tuple[str, str]:
    """T4.1: Return ``(state, rationale)`` for a question's recent history.

    States (in priority order):
      - ``fixed``:       currently passing and passed in the last 2+ iters
      - ``worsening``:   was passing in early window, failing in recent
      - ``improving``:   was failing in early window, passing in recent
      - ``oscillating``: alternating pass/fail in the lookback window
      - ``stuck``:       failing in all ``lookback`` most recent iterations
      - ``intermittent``: some failures but not stuck/oscillating/trending
      - ``new``:         too few evals to classify

    *entries* must be the list of ``VerdictEntry`` for the question in
    chronological order. Only the last ``lookback`` entries are
    considered. A verdict is treated as passing when its arbiter value
    is ``both_correct``; anything else (including ``genie_correct``,
    ``ground_truth_correct``, ``neither_correct``) counts as failing —
    same rule the existing persistence summary applies.
    """
    _PASSING = {"both_correct"}
    if not entries:
        return "new", "no evaluations"
    window = entries[-lookback:]
    if len(window) < 2:
        return "new", f"only {len(window)} eval(s) in history"

    passes = [e.verdict in _PASSING for e in window]
    last = passes[-1]
    n = len(passes)
    n_pass = sum(passes)
    n_fail = n - n_pass

    if last and n_pass >= 2 and all(passes[-2:]):
        return (
            "fixed",
            f"passed last 2+ iters ({n_pass}/{n} in window)",
        )
    if n_fail == n:
        return (
            "stuck",
            f"failed all {n} iterations in window",
        )
    flips = sum(1 for i in range(1, n) if passes[i] != passes[i - 1])
    # Require *more than half* of adjacent-pair transitions to be flips
    # so that [P,P,F,P] (2 flips in 4 items, half) reads as
    # ``intermittent``, while [P,F,P,F] (3 flips in 4 items) reads as
    # ``oscillating``. Minimum of 2 flips keeps short windows honest.
    if flips >= max(2, n // 2 + 1):
        return (
            "oscillating",
            f"{flips} pass/fail flips across {n} iters",
        )
    half = max(1, n // 2)
    early_pass = sum(passes[:half])
    recent_pass = sum(passes[half:])
    if recent_pass > early_pass:
        return (
            "improving",
            f"{early_pass}/{half} -> {recent_pass}/{n - half} passes",
        )
    if early_pass > recent_pass:
        return (
            "worsening",
            f"{early_pass}/{half} -> {recent_pass}/{n - half} passes",
        )
    return (
        "intermittent",
        f"{n_pass}/{n} passes, no clear trend",
    )


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

        # T4.1: compute a second-axis trajectory label — convergence
        # state. Unlike ``classification`` (which is about exhaustion),
        # this describes the *direction*: fixed / improving /
        # oscillating / stuck / worsening / intermittent. The strategist
        # sees both so "oscillating" signals different action than
        # "worsening" even if classification is the same.
        _conv_state, _conv_rationale = _compute_convergence_state(entries)

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
            "convergence_state": _conv_state,
            "convergence_rationale": _conv_rationale,
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
            # T4.1: carry trajectory state so the strategist and any
            # temporary-quarantine logic (T4.3) can differentiate
            # "oscillating" from "worsening" from plain "stuck".
            "convergence_state": p["convergence_state"],
            "convergence_rationale": p["convergence_rationale"],
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
        lines.append(
            f"  CONVERGENCE: {p['convergence_state']} "
            f"({p['convergence_rationale']})"
        )
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
            data_profile=metadata_snapshot.get("_data_profile"),
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

        if _total_corrections == 0 and affected:
            _unfixed_qids = [
                q for q in affected
                if q not in _corr_result.get("corrected_qids", set())
                and q not in _corr_result.get("quarantined_qids", set())
            ]
            if _unfixed_qids:
                _root_cause = ag.get("root_cause_summary", "Ground truth SQL may be incorrect")
                flag_for_human_review(
                    spark, run_id, catalog, schema, domain,
                    [{
                        "question_id": q,
                        "question_text": "",
                        "reason": f"GT_REPAIR_UNRESOLVED: {_root_cause[:200]}",
                        "iterations_failed": 0,
                        "patches_tried": "gt_repair (arbiter corrections failed)",
                    } for q in _unfixed_qids],
                )
                result["detail"]["flagged_for_review"] = len(_unfixed_qids)
                logger.info(
                    "gt_repair: flagged %d questions for human review "
                    "(arbiter could not fix)",
                    len(_unfixed_qids),
                )

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
    data_profile: dict | None = None,
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

        result = apply_benchmark_corrections(
            gc_actions, spark, uc_schema, domain,
            data_profile=data_profile,
        )
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
                        data_profile=data_profile,
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
    exclude_qids: set[str] | None = None,
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
    from genie_space_optimizer.optimization.evaluation import row_is_hard_failure
    from genie_space_optimizer.optimization.ground_truth_corrections import (
        build_gt_correction_candidate,
        is_gt_correction_candidate,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _map_to_lever,
        cluster_failures,
    )

    failure_rows = _get_failure_rows(spark, run_id, catalog, schema)
    _quarantined = quarantined_qids or set()
    _exclude = exclude_qids or set()

    # Tier 1.4: ``row_is_hard_failure`` is the single predicate shared by
    # accuracy and clustering. A row is a hard failure only when rc=no AND
    # arbiter didn't override. Previously we used arbiter alone, so any row
    # where Genie's SQL was semantically wrong (arbiter=ground_truth_correct)
    # but happened to return a matching result set (rc=yes) went into the
    # hard cluster — producing phantom clusters even when the accept gate
    # saw 100% accuracy.
    arbiter_counts: dict[str, int] = {}
    arbiter_excluded: list[str] = []
    quarantine_excluded: list[str] = []
    soft_signal_qids: list[str] = []
    filtered_failure_rows: list[dict] = []
    soft_signal_rows: list[dict] = []
    gt_correction_candidates: list[dict] = []
    for row in failure_rows:
        av = _get_arbiter_verdict(row)
        qid = _get_question_id(row)
        arbiter_counts[av] = arbiter_counts.get(av, 0) + 1

        # B3.2 — match base qid AND ``:vN`` suffix variants so a
        # quarantined ``_002`` excludes ``_002:v2`` and ``_002:v3``
        # rows at the source. Without this, the soft-signal cluster
        # below picks them up and the strategist re-targets them.
        if _is_quarantined_qid(qid, _quarantined):
            quarantine_excluded.append(qid)
            continue

        if qid in _exclude:
            continue

        # Task 1: divert ``arbiter=genie_correct`` rows to the corpus-
        # review queue BEFORE either clustering branch. These are
        # corpus-quality signals (the GT itself is wrong or under-
        # specified), not Genie failures, and must not drive patch
        # generation. The queue persists them for reviewer triage; the
        # state machine then governs whether the qid re-enters the loop
        # (rejected_keep_gt) or feeds Task 9's proactive mining
        # (accepted_corpus_fix).
        if is_gt_correction_candidate(row):
            gt_correction_candidates.append(
                build_gt_correction_candidate(
                    row, run_id=run_id, iteration=iteration_counter
                )
            )
            continue

        if row_is_hard_failure(row):
            filtered_failure_rows.append(row)
        elif _has_individual_judge_failure(row):
            soft_signal_rows.append(row)
            soft_signal_qids.append(qid)
        else:
            arbiter_excluded.append(qid)

    # ── Print failure analysis summary ─────────────────────────────
    # Tier 3.5: labels count ROWS not questions (each benchmark trial is a
    # separate row). Show row counts plus unique-question counts so
    # operators don't misread "5 question(s)" when it's really 5 rows from
    # 3 distinct questions (some run twice).
    _arbiter_summary = "  ".join(f"{k}={v}" for k, v in sorted(arbiter_counts.items()))
    _arbiter_unique_qids = len({
        _get_question_id(row) for row in failure_rows if _get_question_id(row)
    })
    _fa_lines = [
        _section("Failure Analysis", "-"),
        _kv(
            "Total rows loaded",
            f"{len(failure_rows)} row(s) across {_arbiter_unique_qids} unique question(s)",
        ),
        _kv("Arbiter verdicts (row counts)", _arbiter_summary),
    ]
    if quarantine_excluded:
        _qu_unique = len(set(quarantine_excluded))
        _fa_lines.append(_kv(
            "Quarantined (excluded)",
            f"{len(quarantine_excluded)} row(s) across {_qu_unique} unique "
            f"question(s): {', '.join(list(dict.fromkeys(quarantine_excluded))[:5])}",
        ))
    if gt_correction_candidates:
        _gtc_qids = [c.get("question_id", "") for c in gt_correction_candidates]
        _gtc_unique = len({q for q in _gtc_qids if q})
        _fa_lines.append(_kv(
            "GT correction candidates (genie_correct → corpus review)",
            f"{len(gt_correction_candidates)} row(s) across {_gtc_unique} unique "
            f"question(s): {', '.join([q for q in _gtc_qids if q][:5])}",
        ))
    if arbiter_excluded:
        _ae_unique = len(set(arbiter_excluded))
        _fa_lines.append(_kv(
            "Excluded (fully correct)",
            f"{len(arbiter_excluded)} row(s) across {_ae_unique} unique question(s)",
        ))
    if soft_signal_rows:
        _ss_unique = len(set(soft_signal_qids))
        _fa_lines.append(_kv(
            "Soft signals (correct but judges failed)",
            f"{len(soft_signal_rows)} row(s) across {_ss_unique} unique question(s)",
        ))
        # Tier 3.4: show "+N more" suffix when truncated so operators know
        # the list is incomplete.
        # T3.15: when soft_signal_qids contains duplicates (two rows with
        # the same base qid), the display was printing ``_004, _004`` —
        # visually identical tokens that readers can't tell apart.
        # Annotate duplicates inline: the first occurrence carries a
        # ``(base)`` tag, subsequent occurrences get ``:v2``, ``:v3`` so
        # the display mirrors the suffix the dedup stage will apply in
        # cluster_failures.
        def _annotate_dups(qids: list[str]) -> list[str]:
            seen: dict[str, int] = {}
            _has_dups = len(set(qids)) != len(qids)
            out: list[str] = []
            for q in qids:
                n = seen.get(q, 0) + 1
                seen[q] = n
                if n == 1:
                    out.append(f"{q} (base)" if _has_dups and qids.count(q) > 1 else q)
                else:
                    out.append(f"{q}:v{n}")
            return out

        _preview_qids = _annotate_dups(soft_signal_qids[:10])
        _suffix = (
            "" if len(soft_signal_qids) <= 10
            else f" (+{len(soft_signal_qids) - 10} more)"
        )
        _fa_lines.append(_kv(
            "  Soft signal question IDs",
            ", ".join(_preview_qids) + _suffix,
        ))
        for _ss_row, (_ss_qid, _ss_label) in zip(
            soft_signal_rows[:10],
            zip(soft_signal_qids[:10], _preview_qids),
        ):
            _failed_judges = _get_failed_judges(_ss_row)
            _fa_lines.append(f"  |    {_ss_label}: failed judges = {', '.join(_failed_judges) if _failed_judges else '(none detected)'}")
    _hf_unique = len({
        _get_question_id(row) for row in filtered_failure_rows if _get_question_id(row)
    })
    _fa_lines.append(_kv(
        "Hard failure rows for clustering",
        f"{len(filtered_failure_rows)} row(s) across {_hf_unique} unique question(s)",
    ))
    _fa_lines.append(_bar("-"))
    print("\n".join(_fa_lines))

    # Tier 2.11: share qid dedup state across hard+soft clustering so a
    # physical row that appears in both pathways gets a single stable qid
    # (possibly with :vN suffix) and therefore a single dominant root
    # cause. Without this, Q001 would be ``missing_aggregation`` in the
    # hard cluster and ``wrong_filter_condition`` in the soft cluster —
    # the DO-NOT-RETRY bookkeeping cannot reconcile those.
    _shared_qid_state: dict = {}

    # ── Cluster hard failures ──────────────────────────────────────
    # T1.9: explicit ``namespace="H"`` so hard clusters mint H001, H002 …
    # and cannot collide with soft cluster IDs in the shared priority
    # ranking / ``source_cluster_ids`` namespace.
    eval_result_for_clustering = {"rows": filtered_failure_rows}
    clusters = cluster_failures(
        eval_result_for_clustering, metadata_snapshot,
        spark=spark, run_id=run_id, catalog=catalog, schema=schema,
        qid_state=_shared_qid_state,
        signal_type="hard",
        namespace="H",
    )

    # ── Cluster soft signals ───────────────────────────────────────
    # T1.9: explicit ``namespace="S"`` so soft clusters mint S001, S002 …
    soft_clusters: list[dict] = []
    if soft_signal_rows:
        soft_eval = {"rows": soft_signal_rows}
        soft_clusters = cluster_failures(
            soft_eval, metadata_snapshot,
            spark=spark, run_id=run_id, catalog=catalog, schema=schema,
            verbose=False,
            qid_state=_shared_qid_state,
            signal_type="soft",
            namespace="S",
        )
        for sc in soft_clusters:
            sc.setdefault("signal_type", "soft")
        _soft_qids_total = sum(len(sc.get("question_ids", [])) for sc in soft_clusters)
        _soft_lines = [
            _section("Soft Signal Clusters (correct-but-suboptimal)", "-"),
            _kv("Soft signal rows", len(soft_signal_rows)),
            _kv("Soft clusters formed", len(soft_clusters)),
            _kv("Soft cluster questions", _soft_qids_total),
            "|",
        ]
        # Phase E1: annotate each soft cluster with its aggregate signal
        # class so operators can see at a glance whether a cluster is
        # driven by NL-text judges (response_quality) or SQL-shape judges.
        from genie_space_optimizer.optimization.judge_classes import (
            aggregate_cluster_signal_class,
        )

        for si, sc in enumerate(soft_clusters, 1):
            sc_judge = sc.get("affected_judge", "?")
            sc_cause = sc.get("root_cause", "?")
            sc_asi = sc.get("asi_failure_type", "n/a")
            sc_qids = sc.get("question_ids", [])
            sc_blame = sc.get("asi_blame_set", sc.get("blame_set", []))
            blame_str = ", ".join(sc_blame) if isinstance(sc_blame, list) and sc_blame else str(sc_blame) if sc_blame else "(none)"
            sc_signal = aggregate_cluster_signal_class(sc.get("affected_judges", []))
            _soft_lines.append(f"|  Soft cluster {si} / {len(soft_clusters)}")
            _soft_lines.append(f"|    {'Judge:':<24s} {sc_judge}")
            _soft_lines.append(f"|    {'Signal class:':<24s} {sc_signal}")
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
    from genie_space_optimizer.optimization.judge_classes import (
        aggregate_cluster_signal_class,
    )
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
        c_signal = aggregate_cluster_signal_class(c.get("affected_judges", []))
        cluster_lines.append(f"|  Cluster {ci} / {len(clusters)}")
        cluster_lines.append(f"|    {'Judge:':<24s} {c['affected_judge']}")
        cluster_lines.append(f"|    {'Signal class:':<24s} {c_signal}")
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
                    "mapped_lever": c.get("_mapped_lever"),
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
        # Task 1: corpus-review queue payloads (Delta-shaped). The caller
        # is responsible for persisting via state.write_gt_correction_candidates.
        "gt_correction_candidates": gt_correction_candidates,
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
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
    prev_failure_qids: set[str] | None = None,
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
    _primary_lever = int(lever_keys[0]) if lever_keys else 0

    # Task 3: collect decision audit rows as the gates run, persist them
    # in one shot before every return so the audit trail survives even
    # an early rollback. ``_audit_emit`` accepts plain Python lists/dicts;
    # the state writer JSON-serializes.
    _decision_rows: list[dict] = []
    _decision_order = [0]

    def _audit_emit(
        *,
        gate_name: str,
        decision: str,
        stage_letter: str | None = None,
        reason_code: str | None = None,
        reason_detail: str | None = None,
        affected_qids: list[str] | None = None,
        source_cluster_ids: list[str] | None = None,
        proposal_ids: list[str] | None = None,
        proposal_to_patch_map: dict[str, str] | None = None,
        metrics: dict | None = None,
    ) -> None:
        _decision_order[0] += 1
        _decision_rows.append({
            "run_id": run_id,
            "iteration": iteration_counter,
            "ag_id": ag_id,
            "decision_order": _decision_order[0],
            "stage_letter": stage_letter,
            "gate_name": gate_name,
            "decision": decision,
            "reason_code": reason_code,
            "reason_detail": reason_detail,
            "affected_qids": affected_qids,
            "source_cluster_ids": source_cluster_ids,
            "proposal_ids": proposal_ids,
            "proposal_to_patch_map": proposal_to_patch_map,
            "metrics": metrics,
        })

    def _audit_persist() -> None:
        if not _decision_rows:
            return
        try:
            from genie_space_optimizer.optimization.state import (
                write_lever_loop_decisions,
            )
            write_lever_loop_decisions(
                spark, list(_decision_rows), catalog=catalog, schema=schema,
            )
        except Exception:
            logger.debug(
                "Failed to persist lever-loop decision rows", exc_info=True,
            )

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

    # Tier 3.11: bounded polling against fetch_space_config instead of a
    # fixed sleep. Terminates on the first fetch where the applied
    # instruction text / section is visible, falling back to the full
    # wait only when the fetch can't confirm propagation. Median wall-
    # clock drops; slow-API runs still get the full budget.
    _poll_interval = 2.0
    _elapsed = 0.0
    _propagated = False
    _applied_entries = apply_log.get("applied") or []
    _instruction_snippets = []
    for _entry in _applied_entries:
        _patch = _entry.get("patch", {}) if isinstance(_entry, dict) else {}
        for _field in ("new_text", "proposed_value", "text_instructions"):
            _v = _patch.get(_field)
            if isinstance(_v, str) and _v.strip():
                _instruction_snippets.append(_v.strip()[:80])
                break
    _expected_snippets = {s for s in _instruction_snippets if s}

    print(
        _section("Propagation Wait", "-") + "\n"
        + _kv("AG", f"{ag_id}: {len(_applied_entries)} patches applied") + "\n"
        + _kv("Patched objects", ", ".join(str(o) for o in patched_objects) if patched_objects else "(none)") + "\n"
        + _kv("Max wait", f"{wait_time}s{_wait_note}") + "\n"
        + _kv("Mode", "polling fetch_space_config (Tier 3.11)") + "\n"
        + _bar("-")
    )

    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg

    while _elapsed < float(wait_time):
        time.sleep(_poll_interval)
        _elapsed += _poll_interval
        if not _expected_snippets:
            # No verifiable instruction-text snippet; fall back to the
            # full budget (this is the case for non-instruction-only
            # patches like snippet / join_spec changes).
            continue
        try:
            _cfg_probe = _fetch_cfg(w, space_id)
        except Exception:
            continue
        _parsed_probe = _cfg_probe.get("_parsed_space", _cfg_probe) if isinstance(_cfg_probe, dict) else {}
        _instr_probe = _parsed_probe.get("instructions", {}) if isinstance(_parsed_probe, dict) else {}
        _txt_probe = _instr_probe.get("text_instructions", "") if isinstance(_instr_probe, dict) else ""
        if not isinstance(_txt_probe, str) or not _txt_probe:
            continue
        if any(_snip in _txt_probe for _snip in _expected_snippets):
            _propagated = True
            break

    if _expected_snippets and _propagated:
        logger.info(
            "Propagation confirmed after %.1fs (< max %ds) for AG %s",
            _elapsed, wait_time, ag_id,
        )
        _audit_emit(
            stage_letter="K",
            gate_name="propagation_wait",
            decision="confirmed",
            metrics={
                "elapsed_seconds": round(_elapsed, 1),
                "max_wait_seconds": int(wait_time),
                "patches_applied": len(_applied_entries),
            },
        )
    else:
        remaining = max(0.0, float(wait_time) - _elapsed)
        if remaining > 0:
            time.sleep(remaining)
            logger.info(
                "Propagation not confirmed for AG %s — waited full %ds budget",
                ag_id, wait_time,
            )
        _audit_emit(
            stage_letter="K",
            gate_name="propagation_wait",
            decision="waited_full_budget",
            reason_code=(
                "no_verifiable_snippet" if not _expected_snippets
                else "snippet_not_observed"
            ),
            metrics={
                "elapsed_seconds": round(_elapsed, 1),
                "max_wait_seconds": int(wait_time),
                "patches_applied": len(_applied_entries),
            },
        )

    # ── Slice gate ────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass

    _run_slice = False
    # Task 2: slice gate is a legacy approval gate. By default, the new
    # strict full-eval acceptance policy supersedes it. Operators who
    # want the old behaviour set ``GSO_ENABLE_LEGACY_SLICE_P0_GATES=true``.
    from genie_space_optimizer.common.config import ENABLE_LEGACY_SLICE_P0_GATES
    if not ENABLE_LEGACY_SLICE_P0_GATES:
        print(
            _section(f"SLICE GATE [{ag_id}]: SKIPPED (Task 2)", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_LEGACY_SLICE_P0_GATES=False — strict full-eval "
                "acceptance is the only gate; opt back in via "
                "GSO_ENABLE_LEGACY_SLICE_P0_GATES=true",
            ) + "\n"
            + _bar("-")
        )
    elif ENABLE_SLICE_GATE:
        affected_qids: set[str] = affected_question_ids or set()
        # Phase 5.1: stratified slice. Compute the set of
        # baseline-passing qids (benchmarks minus prior-iteration
        # failures) so the slice can include a regression-detector
        # subset alongside the targeted rows. When ``prev_failure_qids``
        # is unavailable (iteration 0 + no resume), fall back to the
        # legacy targeted-only slice.
        _all_qids = {b.get("id") for b in benchmarks if b.get("id")}
        _baseline_passing = (
            _all_qids - set(prev_failure_qids or set())
            if prev_failure_qids is not None else None
        )
        slice_benchmarks = filter_benchmarks_by_scope(
            benchmarks, "slice", patched_objects,
            affected_question_ids=affected_qids,
            baseline_passing_qids=_baseline_passing,
        )
        _total = len(benchmarks)
        _sliced = len(slice_benchmarks) if slice_benchmarks else 0
        # Tier 3.12: small-corpus bypass. For suites with ≤ 30 benchmarks,
        # SLICE_GATE_MIN_REDUCTION=0.5 usually rejects the slice as "too
        # broad" because affected_qids covers most of the set. Relax the
        # threshold in that regime so the slice gate still catches the
        # early-warning regressions (e.g. the Q4 infra flake) before
        # the full eval spends 15+ minutes.
        _small_corpus = _total <= 30
        _broadness_ratio = _sliced / _total if _total else 1.0
        _slice_threshold = 0.9 if _small_corpus else (1.0 - SLICE_GATE_MIN_REDUCTION)
        if slice_benchmarks and _broadness_ratio <= _slice_threshold:
            _run_slice = True
        else:
            print(
                _section(f"SLICE GATE [{ag_id}]: SKIPPED", "-") + "\n"
                + _kv(
                    "Reason",
                    f"slice too broad ({_sliced}/{_total} benchmarks, "
                    f"ratio {_broadness_ratio:.2f} > {_slice_threshold:.2f})",
                ) + "\n"
                + _bar("-")
            )
    else:
        print(
            _section(f"SLICE GATE [{ag_id}]: DISABLED", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_SLICE_GATE=False in common/config.py — set True to enable",
            ) + "\n"
            + _bar("-")
        )

    if _run_slice:
        _ensure_sql_context(spark, catalog, schema)
        write_stage(
            spark, run_id, f"AG_{ag_id}_SLICE_EVAL", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )
        # Tier 4: v2 run name — ``<run_short>/iter_NN_slice_eval``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_slice,
            slice_eval_run_name,
        )
        slice_result = run_evaluation(
            space_id, exp_name, iteration_counter, slice_benchmarks,
            domain, prev_model_id, "slice",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            patched_objects=patched_objects,
            reference_sqls=reference_sqls if reference_sqls else None,
            max_benchmark_count=max_benchmark_count,
            run_name=slice_eval_run_name(run_id, iteration_counter),
            extra_tags=_v2_tags_slice(
                run_id, space_id=space_id, stage="slice_eval",
                iteration=iteration_counter, ag_id=ag_id,
            ),
        )
        slice_scores = slice_result.get("scores", {})
        slice_accuracy = slice_result.get("overall_accuracy", 0.0)
        _slice_qw = 100.0 / max(len(benchmarks), 1)
        # T2.15: use wider small-corpus tolerance when the full-scope
        # corpus is below ``SLICE_GATE_SMALL_CORPUS_ROWS`` so a single-row
        # swing doesn't spuriously fail the gate (e.g. 22-row retail
        # corpus where one flip is ~4.5%). Emit an INFO line with the
        # effective tolerance and corpus size so operators can see which
        # branch ran.
        _full_corpus = len(benchmarks)
        _is_small_corpus = _full_corpus < SLICE_GATE_SMALL_CORPUS_ROWS
        if _is_small_corpus:
            _base_tol = SLICE_GATE_TOLERANCE_SMALL_CORPUS
            _tol_source = "small_corpus"
        else:
            _base_tol = SLICE_GATE_TOLERANCE
            _tol_source = "standard"
        effective_slice_tol = max(_base_tol, noise_floor + 2.0, _slice_qw + 0.5)
        logger.info(
            "SLICE GATE [%s]: tolerance=%.1f%% (source=%s, base=%.1f, "
            "noise_floor+2=%.1f, qw+0.5=%.1f, corpus=%d)",
            ag_id, effective_slice_tol, _tol_source, _base_tol,
            noise_floor + 2.0, _slice_qw + 0.5, _full_corpus,
        )
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
                _section(
                    f"SLICE GATE [{ag_id}]: FAILED "
                    f"(tolerance={effective_slice_tol:.1f}%, corpus={_full_corpus})",
                    "-",
                ) + "\n"
                + _kv("Regressions", _score_changes) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            try:
                update_provenance_gate(
                    spark, run_id, iteration_counter - 1, _primary_lever,
                    "slice", "rollback",
                    {"regressions": [{"judge": d["judge"], "drop": d["drop"]} for d in slice_drops]},
                    catalog, schema,
                )
            except Exception:
                logger.debug("Failed to update provenance gate", exc_info=True)
            try:
                log_gate_feedback_on_traces(
                    slice_result, "slice", "rollback",
                    regressions=slice_drops, lever=_primary_lever, iteration=iteration_counter,
                )
            except Exception:
                logger.debug("Failed to log gate feedback", exc_info=True)
            _audit_emit(
                stage_letter="K",
                gate_name="slice_gate",
                decision="rolled_back",
                reason_detail=f"slice_gate: {slice_drops[0]['judge']}",
                metrics={"regressions": len(slice_drops)},
            )
            _audit_persist()
            return {"passed": False, "rollback_reason": f"slice_gate: {slice_drops[0]['judge']}", "failed_eval_result": slice_result}
        else:
            _sc = ", ".join(
                f"{j} {best_scores.get(j, 0):.1f}->{slice_scores.get(j, 0):.1f}"
                for j in sorted(slice_scores)
            )
            print(
                _section(
                    f"SLICE GATE [{ag_id}]: PASSED "
                    f"(tolerance={effective_slice_tol:.1f}%, corpus={_full_corpus})",
                    "-",
                ) + "\n"
                + _kv("Score changes", _sc) + "\n"
                + _bar("-")
            )

    # ── P0 gate ───────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    # Task 2: P0 gate is a legacy approval gate alongside the slice gate.
    # The new strict full-eval acceptance policy supersedes both. P0
    # only runs when ENABLE_LEGACY_SLICE_P0_GATES=True.
    p0_benchmarks = (
        filter_benchmarks_by_scope(benchmarks, "p0")
        if ENABLE_LEGACY_SLICE_P0_GATES
        else []
    )
    if not ENABLE_LEGACY_SLICE_P0_GATES:
        print(
            _section(f"P0 GATE [{ag_id}]: SKIPPED (Task 2)", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_LEGACY_SLICE_P0_GATES=False — full-eval acceptance "
                "is the only gate",
            ) + "\n"
            + _bar("-")
        )
    if p0_benchmarks:
        _ensure_sql_context(spark, catalog, schema)
        # Tier 4: v2 run name — ``<run_short>/iter_NN_p0_eval``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_p0,
            p0_eval_run_name,
        )
        p0_result = run_evaluation(
            space_id, exp_name, iteration_counter, p0_benchmarks,
            domain, prev_model_id, "p0",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
            max_benchmark_count=max_benchmark_count,
            run_name=p0_eval_run_name(run_id, iteration_counter),
            extra_tags=_v2_tags_p0(
                run_id, space_id=space_id, stage="p0_eval",
                iteration=iteration_counter, ag_id=ag_id,
            ),
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
            _audit_emit(
                stage_letter="K",
                gate_name="p0_gate",
                decision="rolled_back",
                reason_detail=f"p0_gate: {len(p0_failures)} failures",
                metrics={"p0_failures": len(p0_failures)},
            )
            _audit_persist()
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
    # Tier 4: v2 run name — ``<run_short>/iter_NN_full_eval/run_1``.
    from genie_space_optimizer.common.mlflow_names import (
        default_tags as _v2_tags_full,
        full_eval_run_name,
    )
    full_result_1 = run_evaluation(
        space_id, exp_name, iteration_counter, benchmarks,
        domain, None, "full",
        predict_fn, scorers,
        spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
        reference_sqls=reference_sqls if reference_sqls else None,
        model_creation_kwargs=_model_kwargs,
        max_benchmark_count=max_benchmark_count,
        run_name=full_eval_run_name(run_id, iteration_counter, pass_index=1),
        extra_tags=_v2_tags_full(
            run_id, space_id=space_id, stage="full_eval",
            iteration=iteration_counter, ag_id=ag_id,
        ),
    )
    new_model_id = full_result_1.get("model_id", "")

    # Task 0 → Task 3: forward the ASI extraction audit row that
    # ``run_evaluation`` stamped on the result dict. This makes a
    # zero-trace eval visible in the lever-loop decision audit instead
    # of silent.
    _asi_audit_1 = full_result_1.get("asi_extraction_audit")
    if isinstance(_asi_audit_1, dict):
        _asi_metrics = _asi_audit_1.get("metrics_json")
        if isinstance(_asi_metrics, str):
            try:
                _asi_metrics = json.loads(_asi_metrics)
            except (TypeError, ValueError):
                _asi_metrics = None
        _audit_emit(
            stage_letter=_asi_audit_1.get("stage_letter") or "C",
            gate_name=_asi_audit_1.get("gate_name") or "asi_extraction",
            decision=_asi_audit_1.get("decision") or "ok",
            reason_code=_asi_audit_1.get("reason_code"),
            metrics=_asi_metrics if isinstance(_asi_metrics, dict) else None,
        )

    scores_1 = dict(full_result_1.get("scores", {}))
    accuracy_1 = full_result_1.get("overall_accuracy", 0.0)
    pre_arbiter_accuracy_1 = float(
        full_result_1.get("pre_arbiter_accuracy", accuracy_1)
    )
    # Tier 1.8: thread both_correct_rate into the full_scores dict so
    # detect_regressions + the arbiter-override suppression at the top of
    # this block can reference it via ``full_scores['_both_correct_rate']``
    # without an extra RPC.
    _bcr_1 = full_result_1.get("both_correct_rate")
    if _bcr_1 is not None:
        scores_1["_both_correct_rate"] = float(_bcr_1)

    # T0.3: pick the gate's primary and guardrail signals based on the
    # configured objective. ``primary_*`` is what the paired test / delta
    # compares; ``guardrail_*`` is a separate check that prevents a
    # pathological pre-arbiter win from regressing post-arbiter
    # accuracy by more than OPTIMIZATION_OBJECTIVE_POST_ARBITER_GUARDRAIL_PP.
    _objective = str(OPTIMIZATION_OBJECTIVE or "post_arbiter").lower()
    if _objective not in ("pre_arbiter", "post_arbiter", "blended"):
        logger.warning(
            "OPTIMIZATION_OBJECTIVE=%r is not recognised; falling back to "
            "'post_arbiter'.",
            OPTIMIZATION_OBJECTIVE,
        )
        _objective = "post_arbiter"

    # ── Confirmation eval (2nd run) to smooth Genie non-determinism ──
    # T0.3: decide whether to skip the confirm pass based on the chosen
    # objective (pre-arbiter vs post-arbiter accuracy). Under the default
    # ``pre_arbiter`` objective we compare ``pre_arbiter_accuracy_1`` to
    # baseline; a clean pre-arbiter improvement is strong enough signal
    # to skip the confirmation pass.
    if _objective == "pre_arbiter":
        _accuracy_for_skip = pre_arbiter_accuracy_1
    else:
        _accuracy_for_skip = accuracy_1

    if _accuracy_for_skip > best_accuracy:
        print(
            _kv(
                "Confirmation eval",
                f"SKIPPED ({_objective} accuracy improved "
                f"{best_accuracy:.1f}% -> {_accuracy_for_skip:.1f}%)",
            )
        )
        full_scores = scores_1
        full_accuracy = accuracy_1
        full_result = full_result_1
        full_pre_arbiter_accuracy = pre_arbiter_accuracy_1
    else:
        try:
            mlflow.end_run()
        except Exception:
            pass
        print(_kv("Confirmation eval", "running 2nd evaluation to average out variance"))
        _ensure_sql_context(spark, catalog, schema)
        # Tier 4: v2 name — ``<run_short>/iter_NN_full_eval/run_2_confirm``.
        full_result_2 = run_evaluation(
            space_id, exp_name, iteration_counter, benchmarks,
            domain, new_model_id, "full_confirm",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
            max_benchmark_count=max_benchmark_count,
            run_name=full_eval_run_name(run_id, iteration_counter, pass_index=2),
            extra_tags=_v2_tags_full(
                run_id, space_id=space_id, stage="full_eval_confirm",
                iteration=iteration_counter, ag_id=ag_id,
            ),
        )
        # Forward run 2's ASI extraction audit too.
        _asi_audit_2 = full_result_2.get("asi_extraction_audit")
        if isinstance(_asi_audit_2, dict):
            _asi_metrics2 = _asi_audit_2.get("metrics_json")
            if isinstance(_asi_metrics2, str):
                try:
                    _asi_metrics2 = json.loads(_asi_metrics2)
                except (TypeError, ValueError):
                    _asi_metrics2 = None
            _audit_emit(
                stage_letter=_asi_audit_2.get("stage_letter") or "C",
                gate_name=_asi_audit_2.get("gate_name") or "asi_extraction",
                decision=_asi_audit_2.get("decision") or "ok",
                reason_code=_asi_audit_2.get("reason_code"),
                reason_detail="confirmation_run",
                metrics=_asi_metrics2 if isinstance(_asi_metrics2, dict) else None,
            )

        scores_2 = dict(full_result_2.get("scores", {}))
        accuracy_2 = full_result_2.get("overall_accuracy", 0.0)
        pre_arbiter_accuracy_2 = float(
            full_result_2.get("pre_arbiter_accuracy", accuracy_2)
        )
        _bcr_2 = full_result_2.get("both_correct_rate")
        if _bcr_2 is not None:
            scores_2["_both_correct_rate"] = float(_bcr_2)

        all_judge_keys = set(scores_1) | set(scores_2)
        full_scores = {
            j: (scores_1.get(j, 0.0) + scores_2.get(j, 0.0)) / 2.0
            for j in all_judge_keys
        }
        full_accuracy = (accuracy_1 + accuracy_2) / 2.0
        full_pre_arbiter_accuracy = (
            pre_arbiter_accuracy_1 + pre_arbiter_accuracy_2
        ) / 2.0
        full_result = full_result_1

        print(
            _kv("Eval run 1 accuracy", f"{accuracy_1:.1f}%") + "\n"
            + _kv("Eval run 2 accuracy", f"{accuracy_2:.1f}%") + "\n"
            + _kv("Averaged accuracy", f"{full_accuracy:.1f}%") + "\n"
            + _kv(
                "Pre-arbiter accuracy",
                f"run1={pre_arbiter_accuracy_1:.1f}%  "
                f"run2={pre_arbiter_accuracy_2:.1f}%  "
                f"avg={full_pre_arbiter_accuracy:.1f}%",
            )
        )

    full_result = _merge_bug4_counters(full_result)
    # T0.3: stamp the pre-arbiter accuracy on full_scores so gate code
    # below can reference it through the standard ``full_scores`` dict
    # without an extra parameter plumbing pass.
    full_scores["_pre_arbiter/overall_accuracy"] = float(full_pre_arbiter_accuracy)

    # T0.2: compute run-to-run variance on the two confirmation passes.
    # When variance exceeds the baseline regression threshold, the
    # gate's effective tolerance is widened so noise-only flips don't
    # spuriously trigger rollback. The variance number is also logged
    # and stamped on full_scores so downstream readers can tell apart
    # "Genie is deterministic here" from "Genie oscillates; demand a
    # larger effect before accepting or rejecting".
    _variance_full_result_2 = locals().get("full_result_2", None)
    _variance_info = _compute_eval_variance(full_result_1, _variance_full_result_2)
    full_scores["_eval_variance_ratio"] = float(_variance_info["disagreement_ratio"])
    if _variance_info["total_scored"] > 0:
        print(
            _section(f"EVAL VARIANCE [{ag_id}]", "-") + "\n"
            + _kv(
                "Disagreed between runs",
                f"{len(_variance_info['disagreed_qids'])}/"
                f"{_variance_info['total_scored']} "
                f"({_variance_info['disagreement_ratio'] * 100:.1f}%)",
            ) + "\n"
            + _kv(
                "Disagreed qids (sample)",
                ", ".join(_variance_info["disagreed_qids"][:6])
                or "(none)",
            ) + "\n"
            + _bar("-")
        )

    write_iteration(
        spark, run_id, iteration_counter, full_result,
        catalog=catalog, schema=schema,
        lever=int(lever_keys[0]) if lever_keys else 0,
        eval_scope="full", model_id=new_model_id,
    )

    # T0.2: widen the regression tolerance when run-to-run variance
    # exceeds the baseline threshold. Formula: effective_tol =
    # max(REGRESSION_THRESHOLD, noise_floor, 100 * variance_ratio).
    # For a 22-row corpus with 3 questions disagreeing between runs
    # (13.6% variance), tolerance becomes max(5, noise_floor, 13.6) =
    # 13.6pp, preventing noise-only rollbacks.
    _variance_tol_bump = float(_variance_info["disagreement_ratio"]) * 100.0
    effective_regression_tol = max(
        REGRESSION_THRESHOLD, noise_floor, _variance_tol_bump,
    )
    if _variance_tol_bump > REGRESSION_THRESHOLD:
        logger.info(
            "GATE [%s]: regression threshold widened from %.1fpp -> %.1fpp "
            "due to run-to-run variance (%.1f%% disagreement).",
            ag_id, REGRESSION_THRESHOLD, effective_regression_tol,
            _variance_info["disagreement_ratio"] * 100.0,
        )
    # T0.3: pick the primary accuracy for gate comparison. Under
    # ``pre_arbiter`` we compare ``full_pre_arbiter_accuracy`` against
    # ``best_pre_arbiter_accuracy`` (pulled from ``best_scores`` if
    # present, falling back to ``best_accuracy`` for back-compat when
    # best_scores was produced by a pre-T0.3 run). Post-arbiter accuracy
    # is still checked as a catastrophic-regression guardrail.
    _best_pre_arbiter = float(
        best_scores.get("_pre_arbiter/overall_accuracy", best_accuracy)
    )
    if _objective == "pre_arbiter":
        _primary_prev = _best_pre_arbiter
        _primary_cur = full_pre_arbiter_accuracy
        _primary_label = "pre-arbiter result_correctness"
        _secondary_prev = best_accuracy
        _secondary_cur = full_accuracy
        _secondary_label = "post-arbiter overall accuracy"
    else:
        _primary_prev = best_accuracy
        _primary_cur = full_accuracy
        _primary_label = "post-arbiter overall accuracy"
        _secondary_prev = _best_pre_arbiter
        _secondary_cur = full_pre_arbiter_accuracy
        _secondary_label = "pre-arbiter result_correctness"

    logger.info(
        "GATE OBJECTIVE [%s]: mode=%s  primary=%s %.1f%% -> %.1f%% (Δ=%+.1fpp)  "
        "secondary=%s %.1f%% -> %.1f%% (Δ=%+.1fpp)",
        ag_id, _objective, _primary_label, _primary_prev, _primary_cur,
        _primary_cur - _primary_prev,
        _secondary_label, _secondary_prev, _secondary_cur,
        _secondary_cur - _secondary_prev,
    )
    _informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    if full_accuracy >= best_accuracy - 2 * noise_floor:
        _informational_judges.add("asset_routing")
    regressions = detect_regressions(
        full_scores, best_scores, threshold=effective_regression_tol,
        skip_judges=_informational_judges,
    )

    # Tier 1.8: if result_correctness regressed but the arbiter-adjusted
    # both_correct_rate stayed flat or improved, the drop is hash-noise
    # (column reordering, row reordering, alias renames) rather than a
    # semantic regression. Drop that specific regression entry so the
    # iteration isn't rolled back for an equivalence-class difference.
    _prev_bcr = best_scores.get("_both_correct_rate")
    _full_bcr = full_scores.get("_both_correct_rate")
    if _prev_bcr is not None and _full_bcr is not None:
        _bcr_held = _full_bcr >= _prev_bcr - effective_regression_tol
        if _bcr_held:
            _filtered_regressions = [
                r for r in regressions if r.get("judge") != "result_correctness"
            ]
            if len(_filtered_regressions) != len(regressions):
                logger.info(
                    "result_correctness regression suppressed: both_correct_rate held "
                    "(prev=%.1f, current=%.1f, tol=%.1fpp). Likely hash-noise, not "
                    "semantic regression.",
                    _prev_bcr, _full_bcr, effective_regression_tol,
                )
                regressions = _filtered_regressions

    # T0.3: run the overall-accuracy regression check against the
    # *primary* signal (pre-arbiter under the default objective). The
    # post-arbiter accuracy is checked separately below as a guardrail.
    #
    # B0.3 — use the FULL variance-widened tolerance, not tol/2. The
    # halving was a vestige from the legacy per-judge code path that
    # made the gate undershoot run-to-run noise by 2x and rolled back
    # legitimate iterations whose drop fit comfortably inside the
    # variance band T0.2 already widened. The other two arms
    # (noise_floor, question_weight + 0.5) remain as floors so we
    # never go below the per-question discreteness guard.
    accuracy_drop = _primary_prev - _primary_cur
    question_weight = 100.0 / max(len(benchmarks), 1)
    accuracy_threshold = max(
        effective_regression_tol,
        noise_floor,
        question_weight + 0.5,
    )
    if accuracy_drop >= accuracy_threshold:
        regressions.append({
            "judge": f"overall_accuracy ({_primary_label})",
            "previous": _primary_prev,
            "current": _primary_cur,
            "drop": accuracy_drop,
        })

    # Task 2: strict full-eval acceptance. Replaces the legacy
    # ``OPTIMIZATION_OBJECTIVE_POST_ARBITER_GUARDRAIL_PP`` guardrail
    # (which let AG2 through with a -4.6pp post-arbiter regression).
    # The new policy:
    #   - is K-of-N strict: every confirmation run must clear the
    #     primary-gain floor and the post-arbiter guardrail,
    #   - composes with variance widening as a one-sided protection
    #     (variance can only TIGHTEN the guardrail, never relax it),
    #   - applies the post-arbiter guardrail in all three objective
    #     modes, including ``blended``.
    # Per-judge regressions detected above are still in ``regressions``;
    # the new policy adds the typed acceptance verdict on top.
    from genie_space_optimizer.common.config import (
        MAX_POST_ARBITER_DROP_PP_SMALL_CORPUS,
        MIN_PRIMARY_GAIN_PP,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        decide_full_eval_acceptance,
    )

    _run_pre_arbiter = [pre_arbiter_accuracy_1]
    _run_post_arbiter = [accuracy_1]
    if locals().get("full_result_2") is not None:
        _run_pre_arbiter.append(pre_arbiter_accuracy_2)
        _run_post_arbiter.append(accuracy_2)

    _strict_decision = decide_full_eval_acceptance(
        objective=_objective,
        previous_pre_arbiter=_best_pre_arbiter,
        previous_post_arbiter=best_accuracy,
        run_pre_arbiter=_run_pre_arbiter,
        run_post_arbiter=_run_post_arbiter,
        min_primary_gain_pp=MIN_PRIMARY_GAIN_PP,
        max_post_arbiter_drop_pp=MAX_POST_ARBITER_DROP_PP_SMALL_CORPUS,
        variance_widened_tol_pp=effective_regression_tol,
    )
    logger.info(
        "STRICT ACCEPTANCE [%s]: accepted=%s reason=%s "
        "primary_Δ=%+.1fpp secondary_Δ=%+.1fpp "
        "min_run_primary=%.1f min_run_post=%.1f guardrail=%.2fpp",
        ag_id, _strict_decision.accepted, _strict_decision.reason_code,
        _strict_decision.primary_delta_pp, _strict_decision.secondary_delta_pp,
        _strict_decision.min_run_primary, _strict_decision.min_run_post_arbiter,
        _strict_decision.effective_guardrail_pp,
    )
    # Task 3: emit a typed audit row for the strict acceptance verdict
    # regardless of pass/fail. Reason_code lets a single SQL query
    # answer "did we get rejected because primary didn't improve, or
    # because post-arbiter dropped too far?" without parsing logs.
    _audit_emit(
        stage_letter="N",
        gate_name=(
            "post_arbiter_guardrail"
            if _strict_decision.reason_code == "post_arbiter_guardrail"
            else "full_eval_acceptance"
        ),
        decision=("pass" if _strict_decision.accepted else "fail"),
        reason_code=_strict_decision.reason_code,
        metrics={
            "primary_delta_pp": _strict_decision.primary_delta_pp,
            "secondary_delta_pp": _strict_decision.secondary_delta_pp,
            "min_run_primary": _strict_decision.min_run_primary,
            "min_run_post_arbiter": _strict_decision.min_run_post_arbiter,
            "effective_guardrail_pp": _strict_decision.effective_guardrail_pp,
            "previous_pre_arbiter": _best_pre_arbiter,
            "previous_post_arbiter": best_accuracy,
            "objective": _objective,
        },
    )

    if not _strict_decision.accepted:
        # Map the typed reason to a regression entry the rest of the
        # gate already knows how to roll back on.
        if _strict_decision.reason_code == "post_arbiter_guardrail":
            _judge_label = f"post_arbiter_guardrail ({_secondary_label})"
            _prev = _secondary_prev
            _cur = _strict_decision.min_run_post_arbiter
            _drop = _prev - _cur
        else:  # primary_not_improved_in_every_run / missing_confirmation_runs
            _judge_label = f"strict_acceptance ({_strict_decision.reason_code})"
            _prev = _primary_prev
            _cur = _strict_decision.min_run_primary
            _drop = _prev - _cur
        regressions.append({
            "judge": _judge_label,
            "previous": _prev,
            "current": _cur,
            "drop": _drop,
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
    # actually dropped more than the noise floor, the iteration introduced
    # a genuine regression on a previously-passing question.
    #
    # Tier 1.5: the previous strict ``<`` check made any drop below baseline
    # unacceptable — including Genie non-determinism of < 1 pp. Against a
    # 100% baseline this is catastrophic (every iteration rolls back). The
    # tolerance-aware version mirrors the per-judge threshold arithmetic so
    # small drops are classed as noise not regressions.
    # B0.3 — hard guard uses the same variance-widened tolerance as
    # the primary regression check above. Anything tighter than the
    # variance estimate defeats the purpose of T0.2 (we'd reject
    # iterations whose drop sits inside the run-to-run noise band).
    # ``noise_floor`` remains as a lower bound for deterministic
    # corpora where ``effective_regression_tol`` collapses to zero.
    _guard_tolerance = max(noise_floor, effective_regression_tol)
    # T0.3: run the hard-guard on the *primary* signal (pre-arbiter under
    # the default objective). Post-arbiter noise shouldn't block a real
    # pre-arbiter improvement.
    if not regressions and _primary_cur < _primary_prev - _guard_tolerance:
        regressions.append({
            "judge": f"overall_accuracy_guard ({_primary_label})",
            "previous": _primary_prev,
            "current": _primary_cur,
            "drop": _primary_prev - _primary_cur,
        })
        logger.info(
            "Accuracy guard: noise filter cleared per-judge regressions but "
            "%s dropped %.1f%% -> %.1f%% (tolerance %.1fpp) — "
            "rejecting iteration",
            _primary_label, _primary_prev, _primary_cur, _guard_tolerance,
        )
        print(
            _kv(
                "Accuracy guard",
                f"TRIGGERED — {_primary_label} dropped {_primary_prev:.1f}% -> {_primary_cur:.1f}% "
                f"(drop > tolerance {_guard_tolerance:.1f}pp, despite noise filter pass)",
            )
        )

    if regressions:
        # Tier 3.2: prefer r['previous']/r['current'] (populated by
        # detect_regressions and by the overall_accuracy synthetic entry)
        # so synthetic judges like ``overall_accuracy_guard`` show real
        # numbers instead of ``0.0->0.0``. Render the delta as a signed
        # pp value (negative = regression) so operators don't misread
        # ``+15.0`` as an improvement.
        def _fmt_reg(r: dict) -> str:
            _prev = r.get("previous")
            if _prev is None:
                _prev = best_scores.get(r.get("judge", ""), 0.0)
            _cur = r.get("current")
            if _cur is None:
                _cur = full_scores.get(r.get("judge", ""), 0.0)
            _delta = float(_cur) - float(_prev)
            return (
                f"{r.get('judge', '?')} {float(_prev):.1f}->{float(_cur):.1f} "
                f"({_delta:+.1f}pp)"
            )

        _reg_details = ", ".join(_fmt_reg(r) for r in regressions)
        print(
            _section(f"FULL EVAL [{ag_id}]: FAIL (REGRESSION)", "-") + "\n"
            + _kv(
                "Objective",
                f"{_objective}  (primary={_primary_label})",
            ) + "\n"
            + _kv(
                "Primary accuracy",
                f"{_primary_prev:.1f}% -> {_primary_cur:.1f}% "
                f"({_primary_cur - _primary_prev:+.1f}pp)",
            ) + "\n"
            + _kv(
                "Secondary accuracy",
                f"{_secondary_prev:.1f}% -> {_secondary_cur:.1f}% "
                f"({_secondary_cur - _secondary_prev:+.1f}pp)",
            ) + "\n"
            + _kv("Regressions", _reg_details) + "\n"
            + _kv("Action", "ROLLBACK") + "\n"
            + _bar("-")
        )
        try:
            update_provenance_gate(
                spark, run_id, iteration_counter - 1, _primary_lever,
                "full", "rollback",
                {"regressions": [{"judge": r["judge"], "drop": r["drop"]} for r in regressions]},
                catalog, schema,
            )
        except Exception:
            logger.debug("Failed to update provenance gate (full rollback)", exc_info=True)
        try:
            log_gate_feedback_on_traces(
                full_result, "full", "rollback",
                regressions=regressions, lever=_primary_lever, iteration=iteration_counter,
            )
        except Exception:
            logger.debug("Failed to log full eval gate feedback", exc_info=True)
        _audit_emit(
            stage_letter="N",
            gate_name="full_eval_acceptance",
            decision="rolled_back",
            reason_code=_strict_decision.reason_code,
            reason_detail=f"full_eval: {regressions[0]['judge']}",
            metrics={
                "regression_count": len(regressions),
                "primary_delta_pp": _primary_cur - _primary_prev,
                "secondary_delta_pp": _secondary_cur - _secondary_prev,
            },
        )
        _audit_persist()
        return {"passed": False, "rollback_reason": f"full_eval: {regressions[0]['judge']}", "failed_eval_result": full_result, "regressions": regressions}

    # ── PASSED ────────────────────────────────────────────────────────
    _score_delta = ", ".join(
        f"{j} {best_scores.get(j, 0):.1f}->{full_scores.get(j, 0):.1f}"
        for j in sorted(full_scores)
    )
    print(
        _section(f"FULL EVAL [{ag_id}]: PASS -- ACCEPTED", "=") + "\n"
        + _kv(
            "Objective",
            f"{_objective}  (primary={_primary_label})",
        ) + "\n"
        + _kv(
            "Primary accuracy",
            f"{_primary_prev:.1f}% -> {_primary_cur:.1f}% "
            f"({_primary_cur - _primary_prev:+.1f}pp)",
        ) + "\n"
        + _kv(
            "Secondary accuracy",
            f"{_secondary_prev:.1f}% -> {_secondary_cur:.1f}% "
            f"({_secondary_cur - _secondary_prev:+.1f}pp)",
        ) + "\n"
        + _kv("Score changes", _score_delta) + "\n"
        + _bar("=")
    )
    try:
        update_provenance_gate(
            spark, run_id, iteration_counter - 1, _primary_lever,
            "full", "pass", None, catalog, schema,
        )
    except Exception:
        logger.debug("Failed to update provenance gate (full pass)", exc_info=True)
    try:
        log_gate_feedback_on_traces(
            full_result, "full", "pass",
            lever=_primary_lever, iteration=iteration_counter,
        )
    except Exception:
        logger.debug("Failed to log full eval gate feedback", exc_info=True)

    _audit_emit(
        stage_letter="N",
        gate_name="full_eval_acceptance",
        decision="accepted",
        reason_code="accepted",
        metrics={
            "primary_delta_pp": _primary_cur - _primary_prev,
            "secondary_delta_pp": _secondary_cur - _secondary_prev,
            "min_run_post_arbiter": _strict_decision.min_run_post_arbiter,
            "effective_guardrail_pp": _strict_decision.effective_guardrail_pp,
        },
    )
    _audit_persist()
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
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
    iq_scan_recommended_levers: list[int] | None = None,
    iq_scan_summary: dict | None = None,
) -> dict:
    """Stage 3: Iterate levers with convergence checking.

    Internal Python loop over levers. Supports resume on task retry.

    When *enrichment_done* is True, Phase 1 (proactive enrichment) is skipped
    because it was already executed by the standalone enrichment task. The
    enriched config is loaded from the Genie Space API instead.

    Returns dict with best scores, model_id, iteration_counter, levers lists.

    Source-specific invariant (lever-loop-proposed sql_snippets / join_specs):
        The lever loop proposes structural SQL via the LLM (Lever 6 for
        sql_snippets; join-lever for join_specs). Persistence gates:

        - Exec-validation at propose time via ``validate_sql_snippet`` /
          ``_explain_join_candidate``.
        - Post-iteration full-eval arbiter gate (``detect_regressions``
          + accuracy-drop check below). Iterations whose patches cause
          arbiter-detected regression roll back, removing the offending
          snippet.

        This is the functional equivalent of proactive enrichment's
        pre-mining arbiter filter: both mechanisms guarantee that
        persisted sql_snippets / join_specs are arbiter-backed. The
        Bug #4 firewall is therefore no longer wired for these patch
        types — see scoping comment above ``_PATCH_TEXT_FIELDS`` in
        leakage.py.
    """
    levers = levers or DEFAULT_LEVER_ORDER
    thresholds = thresholds or DEFAULT_THRESHOLDS

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "STARTED",
        task_key="lever_loop", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    resume_state = _resume_lever_loop(spark, run_id, catalog, schema)
    # S10 — ``start_lever`` is informational only: the loop below always
    # begins at Lever 1 and iterates the full ``levers`` sequence per
    # iteration. ``None`` means "no completed lever in Delta", i.e. a
    # fresh run; an int means "the last COMPLETE lever_stage from a
    # prior task attempt" and is surfaced in the setup block so on-call
    # can distinguish a retried task from a cold start.
    start_lever = resume_state.get("resume_from_lever")
    iteration_counter = resume_state.get("iteration_counter", 0)
    if resume_state.get("prev_scores"):
        prev_scores = resume_state["prev_scores"]
    if resume_state.get("prev_model_id"):
        prev_model_id = resume_state["prev_model_id"]
    if resume_state.get("prev_accuracy"):
        prev_accuracy = resume_state["prev_accuracy"]

    baseline_accuracy = prev_accuracy
    best_scores = dict(prev_scores)
    # B0.2 — defensive fallback for resumed/older runs that pre-date
    # B0.1's ``_pre_arbiter/overall_accuracy`` stamping. When the prior
    # eval emitted only ``_pre_arbiter/result_correctness`` (the
    # canonical primary signal), promote it so the gate's
    # baseline-pre-arbiter lookup returns a real pre-arbiter number
    # rather than silently falling back to post-arbiter accuracy.
    if "_pre_arbiter/overall_accuracy" not in best_scores:
        if "_pre_arbiter/result_correctness" in best_scores:
            best_scores["_pre_arbiter/overall_accuracy"] = best_scores[
                "_pre_arbiter/result_correctness"
            ]
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

    # ── SQL snippet normalize safety net ──────────────────────────────
    # Defense-in-depth: the enrichment stage already ran the repair path,
    # but any intermediate step (instruction prose miner, benchmark miner,
    # manual edits between runs) could have added a snippet with a short-
    # form prefix. This pass is cheap (no-op when everything is already
    # fully-qualified) and guarantees the lever loop only sees
    # ``catalog.schema.table.col`` references — Genie's serving path
    # rejects any other form. If repair rewrote anything, refetch config
    # so the lever loop works against the post-repair state.
    try:
        _parsed_pre_repair = config.get("_parsed_space", config) or {}
        _snippet_repair_result = _repair_existing_sql_snippets(
            w, spark, run_id, space_id, config=config,
            metadata_snapshot=_parsed_pre_repair,
            catalog=catalog, schema=schema,
            warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        )
        if _snippet_repair_result.get("rewritten", 0) > 0:
            from genie_space_optimizer.common.genie_client import fetch_space_config
            config = fetch_space_config(w, space_id)
    except Exception:
        # Non-fatal: the lever loop will still function against the
        # current config; EXPLAIN errors on individual snippets surface
        # later via the lever-specific validators.
        logger.warning("SQL snippet normalize safety net failed", exc_info=True)

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
    _parsed_space = config.get("_parsed_space", config)
    _instr_section = _parsed_space.get("instructions", {}) if isinstance(_parsed_space, dict) else {}
    _instr_text_for_scorers = _instr_section.get("text_instructions", "") if isinstance(_instr_section, dict) else ""
    scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_instr_text_for_scorers)
    uc_schema = f"{catalog}.{schema}"
    metadata_snapshot = _parsed_space
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

        # Tier 4: v2 naming — ``<run_short>/enrichment/inline``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_legacy,
            enrichment_run_name as _enrichment_run_name_legacy,
        )
        _legacy_enr_run_name = _enrichment_run_name_legacy(run_id, detail="inline")
        with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):
            _mlflow_legacy_enr.set_tags({
                **_v2_tags_legacy(run_id, space_id=space_id, stage="enrichment_inline"),
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

            # ── Instruction prose mining & promotion (miner-first) ───────
            # Mirrors the primary ``_run_enrichment`` path (Task C.5) so
            # direct-lever-loop invocations with ``enrichment_done=False``
            # see the same prose-normalisation behaviour as the job-level
            # entry point. The miner runs BEFORE proactive seed/expand.
            _legacy_miner_out = _run_instruction_prose_mining(
                w, spark, run_id, space_id, config, metadata_snapshot,
                catalog, schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                benchmarks=benchmarks,
            )
            if _legacy_miner_out["total_applied"] or _legacy_miner_out["keep_in_prose_count"]:
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
            if (
                instruction_result.get("instructions_seeded")
                or instruction_result.get("instructions_expanded")
            ):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── Pre-flight example_sql synthesis (legacy path) ───────────
            # Fills example_question_sqls to target. Idempotent on re-runs.
            if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:
                try:
                    from genie_space_optimizer.optimization.preflight_synthesis import (
                        run_preflight_example_synthesis,
                    )
                    legacy_preflight_result = run_preflight_example_synthesis(
                        w, spark, run_id, space_id, config, metadata_snapshot,
                        benchmarks=benchmarks,
                        catalog=catalog, schema=schema,
                        warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
                    )
                    if legacy_preflight_result.get("applied", 0) > 0:
                        from genie_space_optimizer.common.genie_client import fetch_space_config
                        config = fetch_space_config(w, space_id)
                        config["_uc_columns"] = uc_columns
                        metadata_snapshot = config.get("_parsed_space", config)
                        metadata_snapshot["_data_profile"] = data_profile
                        if uc_columns:
                            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
                except Exception:
                    logger.warning(
                        "preflight example synthesis (legacy path) raised; continuing",
                        exc_info=True,
                    )

            # ── SQL Expression REPAIR + SEEDING (legacy path) ────────────
            # Repair runs unconditionally; seeding is headroom-gated.
            # Mining source gate mirrors the primary path
            # (_run_enrichment): only arbiter-approved (``both_correct``)
            # baseline rows contribute. See _extract_arbiter_approved_benchmarks
            # for the verdict-scoping rationale.
            _legacy_approved, _legacy_verdicts = _extract_arbiter_approved_benchmarks(
                spark, run_id, catalog, schema, benchmarks,
            )
            logger.info(
                "miner.arbiter_filter path=legacy total=%d approved=%d verdicts=%s",
                len(benchmarks), len(_legacy_approved), _legacy_verdicts,
            )
            sql_expr_result = _run_sql_expression_seeding(
                w, spark, run_id, space_id, config=config,
                metadata_snapshot=metadata_snapshot,
                benchmarks=_legacy_approved,
                catalog=catalog, schema=schema,
                warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
            )
            if (
                sql_expr_result.get("total_seeded", 0) > 0
                or sql_expr_result.get("repair", {}).get("rewritten", 0) > 0
            ):
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
            _enr_summary.append(_kv("SQL expressions seeded", sql_expr_result.get("total_seeded", 0)))
            _enr_summary.append(_bar("-"))
            print("\n".join(_enr_summary))

            _mlflow_legacy_enr.log_metrics({
                "enrichment.columns_enriched": enrichment_result.get("total_enriched", 0),
                "enrichment.tables_enriched": enrichment_result.get("tables_enriched", 0),
                "enrichment.joins_discovered": join_result.get("total_applied", 0),
                "enrichment.sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
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

    # ── Phase 1.5: Restructure unstructured instructions ──────────
    # If existing instructions lack ALL-CAPS section headers, classify
    # them into canonical sections via LLM so downstream section-level
    # merges are safe.  The restructured text is persisted to the Genie
    # Space so all lever iterations operate on structured input.
    try:
        from genie_space_optimizer.optimization.applier import (
            _get_general_instructions,
            _set_general_instructions,
        )
        from genie_space_optimizer.optimization.optimizer import (
            _is_unstructured,
            _pre_structure_instructions,
            normalize_instructions,
        )

        _current_instr = _get_general_instructions(metadata_snapshot)
        if _current_instr and _current_instr.strip() and _is_unstructured(_current_instr):
            logger.info(
                "Existing instructions are unstructured (%d chars) "
                "— restructuring into canonical sections",
                len(_current_instr),
            )
            _restructured_secs = _pre_structure_instructions(
                _current_instr, metadata_snapshot, w=w,
            )
            if _restructured_secs:
                parts: list[str] = []
                from genie_space_optimizer.common.config import INSTRUCTION_SECTION_ORDER
                for _sec in INSTRUCTION_SECTION_ORDER:
                    _lines_list = _restructured_secs.get(_sec, [])
                    if not _lines_list:
                        continue
                    parts.append(f"{_sec}:")
                    for _ln in _lines_list:
                        _s = _ln.strip()
                        if not _s:
                            continue
                        if not _s.startswith("- "):
                            _s = f"- {_s}"
                        parts.append(_s)
                    parts.append("")
                _restructured_text = normalize_instructions("\n".join(parts).strip())

                if len(_restructured_text.strip()) >= len(_current_instr.strip()) * 0.5:
                    _set_general_instructions(metadata_snapshot, _restructured_text)
                    try:
                        from genie_space_optimizer.common.genie_client import (
                            fetch_space_config,
                            patch_space_config,
                        )
                        patch_space_config(w, space_id, metadata_snapshot)
                        # Re-fetch to get server-canonical state after PATCH.
                        # Without this, metadata_snapshot diverges from the API's
                        # version and all subsequent lever PATCHes fail with
                        # "Space configuration has been modified since this export".
                        config = fetch_space_config(w, space_id)
                        config["_uc_columns"] = uc_columns
                        metadata_snapshot = config.get("_parsed_space", config)
                        metadata_snapshot["_data_profile"] = data_profile
                        if uc_columns:
                            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
                        logger.info(
                            "Persisted restructured instructions (%d chars, %d sections)",
                            len(_restructured_text), len(_restructured_secs),
                        )
                        write_patch(
                            spark, run_id, 0, 0, 0,
                            {
                                "patch_type": "instruction_restructure",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": "instructions.text_instructions",
                                "patch": {"instructions": _restructured_text[:200] + "..."},
                                "command": None,
                                "rollback": None,
                                "proposal_id": "instruction_restructure",
                            },
                            catalog, schema,
                        )
                    except Exception:
                        logger.warning(
                            "Instruction restructure: PATCH to Genie Space failed",
                            exc_info=True,
                        )
                else:
                    logger.warning(
                        "Restructured text too short (%d chars vs %d original) "
                        "— skipping persist",
                        len(_restructured_text), len(_current_instr),
                    )

            print("\n".join([
                _section("INSTRUCTION RESTRUCTURING", "-"),
                _kv("Original format", "unstructured"),
                _kv("Sections detected", ", ".join(_restructured_secs.keys()) if _restructured_secs else "none"),
                _bar("-"),
            ]))
        else:
            logger.debug("Instructions already structured or empty — no restructuring needed")
    except Exception:
        logger.warning("Instruction restructuring failed — continuing with existing format", exc_info=True)

    # ── Phase 1.6: Snapshot user-authored instruction sections ────────
    # Capture the instruction sections AFTER restructuring but BEFORE any
    # lever patches.  This snapshot is the user's ground truth — the
    # optimizer must never generate content that contradicts it.
    _original_instruction_sections: dict[str, list[str]] = {}
    try:
        from genie_space_optimizer.optimization.applier import _get_general_instructions
        from genie_space_optimizer.optimization.optimizer import _ensure_structured
        _pre_loop_instr = _get_general_instructions(metadata_snapshot)
        if _pre_loop_instr and _pre_loop_instr.strip():
            _original_instruction_sections = _ensure_structured(
                _pre_loop_instr, metadata_snapshot, w=w,
            )
            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections
            logger.info(
                "Snapshotted %d user-authored instruction section(s): %s",
                len(_original_instruction_sections),
                list(_original_instruction_sections.keys()),
            )
    except Exception:
        logger.warning("Could not snapshot original instruction sections", exc_info=True)

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
        data_profile=metadata_snapshot.get("_data_profile"),
    )
    _correction_state["corrected_qids"] = _pre_loop_corr["corrected_qids"]
    _correction_state["quarantined_qids"] = _pre_loop_corr["quarantined_qids"]

    # Bug #4 — benchmark verbatim mining removed from pre-loop setup.
    # Example SQLs are now proposed only via AFS-gated structural synthesis
    # during the lever loop, never by copying benchmark expected_sql.
    mined_example_proposals: list = []

    _setup_lines = [_section("PRE-LOOP SETUP — COMPLETE", "-")]
    _setup_lines.append(_kv("Reference SQLs", len(reference_sqls)))
    _setup_lines.append(_kv("Reference hashes", len(reference_result_hashes)))
    _setup_lines.append(_kv("Arbiter corrections", len(_pre_loop_corr.get("corrected_qids", set()))))
    _setup_lines.append(_kv("Mined examples", len(mined_example_proposals)))
    # S10 — the old ``Starting lever: 0`` label implied the loop would
    # skip to some lever, which is false (see comment at start_lever
    # initialisation). Replace with a human resume state.
    _resume_display = (
        f"Resuming after lever {start_lever}" if start_lever else "Starting fresh"
    )
    _setup_lines.append(_kv("Resume state", _resume_display))
    _setup_lines.append(_kv("Iteration counter", _iteration_label(iteration_counter)))
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
    escalated_gt_repair_qids: set[str] = set()
    noise_floor = min(100.0 / max(len(benchmarks), 1), MAX_NOISE_FLOOR)

    reflection_buffer: list[dict] = resume_state.get("reflection_buffer", [])
    skill_exemplars: list[dict] = resume_state.get("skill_exemplars", [])
    tried_patches: set[tuple[str, str]] = resume_state.get("tried_patches", set())
    tried_root_causes: set[tuple[str, str]] = resume_state.get("tried_root_causes", set())
    prev_failure_qids: set[str] = set()
    _verdict_history: dict[str, list] = {}
    _last_full_mlflow_run_id: str = baseline_iter.get("mlflow_run_id", "") if baseline_iter else ""

    # Phase 1.3: pending action groups buffer.  When the strategist
    # returns multiple action_groups, only AG[0] is consumed in the
    # current iteration; remaining AGs are buffered for the next
    # iteration so we don't need a fresh (and expensive) strategist
    # round-trip to address the next-priority cluster.  Each buffered
    # AG is re-validated against the current cluster set before reuse;
    # if its source clusters have been resolved or the schema state has
    # diverged, the buffer is drained and the strategist is re-called.
    # Toggle via ``GSO_PROCESS_ALL_AGS=0`` to fall back to the legacy
    # AG[0]-only behavior.
    _process_all_ags = os.getenv(
        "GSO_PROCESS_ALL_AGS", "1",
    ).strip().lower() not in ("0", "false", "no", "off")
    _MAX_AGS_PER_STRATEGIST_CALL = int(
        os.getenv("GSO_MAX_AGS_PER_STRATEGIST_CALL", "3"),
    )
    pending_action_groups: list[dict] = []
    pending_strategy: dict | None = None

    for _iter_num in range(1, max_iterations + 1):
        # ── Exit checks ──────────────────────────────────────────────
        if all_thresholds_met(best_scores, thresholds):
            logger.info("Convergence: all thresholds met before iteration %d", _iter_num)
            break
        if _diminishing_returns(reflection_buffer):
            logger.info("Diminishing returns detected — stopping at iteration %d", _iter_num)
            print(
                _section("LEVER LOOP — TERMINATION: plateau", "!") + "\n"
                + _kv("Reason", "diminishing returns (no improvement >= epsilon)") + "\n"
                + _kv("Iteration", _iteration_label(_iter_num)) + "\n"
                + _bar("!")
            )
            break
        _diverging, _div_rationale = _detect_divergence(reflection_buffer)
        if _diverging:
            logger.info(
                "Divergence detected at iteration %d: %s",
                _iter_num, _div_rationale,
            )
            print(
                _section("LEVER LOOP — TERMINATION: divergence", "!") + "\n"
                + _kv("Reason", _div_rationale) + "\n"
                + _kv("Iteration", _iteration_label(_iter_num)) + "\n"
                + _bar("!")
            )
            break
        # Phase C3: only CONTENT_REGRESSION rollbacks count toward the
        # consecutive-rollback limit. INFRA / SCHEMA / escalation / OTHER
        # rollbacks carry no content signal and have their own handling
        # (infra retry budget, schema fatal exit, escalation_handled).
        from genie_space_optimizer.optimization.rollback_class import (
            RollbackClass as _RC,
        )
        _consecutive_rb = 0
        for _rb_entry in reversed(reflection_buffer):
            if _rb_entry.get("escalation_handled"):
                continue
            if _rb_entry.get("accepted"):
                break
            if _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value:
                _consecutive_rb += 1
            else:
                # Non-content rollback (infra/schema/other). Skip without
                # counting; don't break, so we can still see a
                # CONTENT_REGRESSION further back in the buffer.
                continue
        if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:
            logger.info(
                "Consecutive content-rollback limit (%d) reached — stopping at iteration %d",
                CONSECUTIVE_ROLLBACK_LIMIT, _iter_num,
            )
            break

        _consecutive_esc = 0
        _last_esc_type: str | None = None
        for _esc_entry in reversed(reflection_buffer):
            if not _esc_entry.get("escalation_handled"):
                break
            _esc_reason = _esc_entry.get("rollback_reason", "")
            if _last_esc_type is None:
                _last_esc_type = _esc_reason
            if _esc_reason == _last_esc_type:
                _consecutive_esc += 1
            else:
                break
        if _consecutive_esc >= CONSECUTIVE_ESCALATION_LIMIT:
            logger.info(
                "Consecutive escalation limit (%d) reached for '%s' — "
                "stopping at iteration %d",
                CONSECUTIVE_ESCALATION_LIMIT, _last_esc_type, _iter_num,
            )
            write_stage(
                spark, run_id, "LEVER_LOOP_ESCALATION_EXIT", "COMPLETE",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "consecutive_escalations": _consecutive_esc,
                    "escalation_type": _last_esc_type,
                },
                catalog=catalog, schema=schema,
            )
            break

        iteration_counter += 1

        # ── 3B.1b: Per-iteration arbiter corrections ─────────────────
        _iter_corr = _run_arbiter_corrections(
            w, spark, run_id, catalog, schema, domain,
            already_corrected=_correction_state["corrected_qids"],
            already_repaired=_correction_state["repaired_qids"],
            quarantined_qids=_correction_state["quarantined_qids"],
            data_profile=metadata_snapshot.get("_data_profile"),
        )
        _correction_state["corrected_qids"] = _iter_corr["corrected_qids"]
        _correction_state["quarantined_qids"] = _iter_corr["quarantined_qids"]

        # ── 3B.2: Re-cluster from latest eval ────────────────────────
        _analysis = _analyze_and_distribute(
            spark, run_id, catalog, schema, metadata_snapshot,
            iteration_counter - 1, lever_label=0,
            quarantined_qids=_correction_state["quarantined_qids"],
            exclude_qids=escalated_gt_repair_qids,
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
        try:
            # Task 1: persist GT correction queue payloads. Empty list
            # is a no-op inside the helper.
            write_gt_correction_candidates(
                spark,
                _analysis.get("gt_correction_candidates") or [],
                catalog=catalog,
                schema=schema,
            )
        except Exception:
            logger.debug("Failed to write GT correction candidates", exc_info=True)

        clusters = _filter_tried_clusters(clusters, tried_root_causes)
        if not clusters and not soft_signal_clusters:
            logger.info("No actionable clusters remain — stopping at iteration %d", _iter_num)
            break

        # ── Cluster-driven synthesis iteration-scoped state ──────────
        # Stamp clusters on the snapshot so
        # ``_resolve_source_cluster_for_ag`` (optimizer.py) can look up
        # source clusters by id for Lever 5 intercept. Reset the shared
        # per-iteration budget counter + stamp the active space_id so
        # the P2 arbiter gate can call Genie (both per Bug #4 Phase 3
        # Invariants B and C).
        metadata_snapshot["_failure_clusters"] = clusters
        metadata_snapshot["_cluster_synthesis_count"] = 0
        metadata_snapshot["_space_id"] = space_id

        # ── 3B.3: Priority scoring ───────────────────────────────────
        _scan_levers = (
            set(iq_scan_recommended_levers)
            if iq_scan_recommended_levers and _iq_scan_strategist_enabled()
            else None
        )
        # Tier 2.3: include soft clusters in ranking. cluster_impact applies a
        # 0.5 dampen for signal_type=="soft" so hard clusters still win at
        # equal q_count, while large soft clusters (the response_quality=63%
        # case) can out-rank tiny hard clusters and earn strategist attention.
        for _sc in soft_signal_clusters or []:
            if isinstance(_sc, dict):
                _sc.setdefault("signal_type", "soft")
        ranked = rank_clusters(
            list(clusters) + list(soft_signal_clusters or []),
            recommended_levers=_scan_levers,
            # T2.1: pass reflection buffer so each cluster gains a
            # ``history`` block with prior attempts against its
            # iteration-independent ``cluster_signature``. The
            # strategist can then reason about "we've tried this
            # cluster twice and rolled back both times; consider
            # escalating or picking a different lever".
            reflection_buffer=reflection_buffer,
        )

        # ── 3B.4: Adaptive strategist (1 LLM call → 1 AG) ───────────
        print(_section(f"ADAPTIVE STRATEGIST — Iteration ({_iteration_label(iteration_counter)})", "="))

        _verdict_history = _build_verdict_history(spark, run_id, catalog, schema)

        # ── 3B.3b: Hard-quarantine exhausted questions ────────────────
        if reflection_buffer:
            _, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )
            _quarantine_qids: set[str] = set()
            # T4.3: temporary quarantine for stuck/worsening questions
            # that haven't hit the hard-quarantine threshold yet. These
            # are excluded from cluster formation for the *current*
            # iteration only — they re-enter next iteration automatically.
            # Prevents a couple of stuck questions from dominating every
            # cluster and blinding the loop to patchable failures.
            _soft_skip_qids: set[str] = set()
            for _pq_id, _pq_info in _persist_data.items():
                _pq_class = _pq_info.get("classification", "")
                _pq_consec = _pq_info.get("max_consecutive", 0)
                _pq_conv = _pq_info.get("convergence_state", "")
                if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (
                    _pq_class == "PERSISTENT" and _pq_consec >= 3
                ):
                    _quarantine_qids.add(_pq_id)
                elif _pq_conv in ("stuck", "worsening") and _pq_consec >= 2:
                    # Not bad enough for hard-quarantine, but bad enough
                    # to not dominate cluster-formation this pass.
                    _soft_skip_qids.add(_pq_id)

            if _soft_skip_qids:
                logger.info(
                    "T4.3: soft-skipping %d stuck/worsening question(s) "
                    "from this iteration's cluster formation: %s",
                    len(_soft_skip_qids), sorted(_soft_skip_qids),
                )
                print(
                    _section(
                        f"T4.3 CONVERGENCE QUARANTINE — "
                        f"{len(_soft_skip_qids)} qid(s) soft-skipped",
                        "-",
                    ) + "\n"
                    + _kv(
                        "Questions",
                        ", ".join(sorted(_soft_skip_qids))[:200],
                    ) + "\n"
                    + _kv(
                        "Rationale",
                        "stuck/worsening convergence state; re-entered next iter",
                    ) + "\n"
                    + _bar("-")
                )
                # Merge into quarantine_qids only for this iteration's
                # cluster-formation call — the _correction_state store
                # below is for hard quarantine, which persists.
                _quarantine_qids = _quarantine_qids | _soft_skip_qids
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
                # B3.3 — prune both hard and soft clusters using the
                # shared base-qid helper so a quarantined ``_002``
                # excludes ``_002:v2`` / ``_002:v3`` from S001 too.
                # Soft clusters were previously left untouched, which
                # is what let iter-2's S001 still contain the suffixed
                # variants of quarantined base qids.
                for c in list(clusters) + list(soft_signal_clusters or []):
                    c_qids = c.get("question_ids", [])
                    c["question_ids"] = [
                        q for q in c_qids
                        if not _is_quarantined_qid(q, _quarantine_qids)
                    ]
                clusters = [c for c in clusters if c.get("question_ids")]
                soft_signal_clusters = [
                    c for c in (soft_signal_clusters or [])
                    if c.get("question_ids")
                ]
                if not clusters and not soft_signal_clusters:
                    logger.info("All clusters emptied after quarantine — stopping at iteration %d", _iter_num)
                    break

        _total_q = len(benchmarks)
        # Tier 2.4: include soft-cluster questions in the "failing" set when
        # computing passing_q. Previously this only subtracted hard-cluster
        # qids, so the success-summary line fed to the strategist read
        # "N of M pass all judges" while the prompt's own soft_signal_clusters
        # block showed judge failures — the two statements contradicted each
        # other and the strategist would under-prioritise soft-cluster work.
        _hard_qids = {
            q for c in clusters for q in c.get("question_ids", []) if q
        }
        _soft_qids = {
            q for c in (soft_signal_clusters or [])
            for q in c.get("question_ids", []) if q
        }
        _passing_q = _total_q - len(_hard_qids | _soft_qids)

        # ── Open a strategy MLflow run for this iteration ──────────
        # Tier 4: v2 naming — ``<run_short>/iter_NN_strategy/<pending>``.
        # We don't know the AG id yet (strategist is called next); use a
        # ``pending`` detail until the strategist returns, then update
        # tags with the concrete ag_id once known.
        import mlflow as _mlflow

        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_strat,
            strategy_run_name,
        )

        try:
            _mlflow.end_run()
        except Exception:
            pass
        _mlflow.start_run(
            run_name=strategy_run_name(run_id, iteration_counter, "pending"),
        )
        try:
            _mlflow.set_tags({
                **_v2_tags_strat(
                    run_id,
                    space_id=space_id,
                    stage="strategy",
                    iteration=iteration_counter,
                ),
                "genie.domain": domain,
                "genie.optimization_run_id": run_id,
                "genie.run_type": "strategy",
            })

            # Phase 1.3: try buffered AG first.  We re-validate against
            # the current cluster set so a buffered AG whose source
            # clusters have been resolved (or split) by a prior
            # iteration is dropped and the strategist is re-called.
            ag = None
            strategy = pending_strategy if _process_all_ags else None
            if _process_all_ags and pending_action_groups:
                _live_cluster_ids = {
                    c.get("cluster_id", "") for c in clusters + (soft_signal_clusters or [])
                }
                while pending_action_groups:
                    _candidate = pending_action_groups.pop(0)
                    _src_ids = set(_candidate.get("source_cluster_ids", []) or [])
                    if not _src_ids or (_src_ids & _live_cluster_ids):
                        ag = _candidate
                        break
                if ag is not None:
                    print(
                        _section(
                            f"REUSING BUFFERED AG (skipping strategist call) — "
                            f"{len(pending_action_groups)} more queued",
                            "-",
                        ) + "\n"
                        + _kv("AG id", ag.get("id", "?")) + "\n"
                        + _kv("Source clusters", sorted(_src_ids)) + "\n"
                        + _bar("-")
                    )
                else:
                    pending_strategy = None
                    strategy = None

            if ag is None:
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
                    iq_scan_summary=(
                        iq_scan_summary if _iq_scan_strategist_enabled() else None
                    ),
                )
                strategy["_source_clusters"] = clusters + soft_signal_clusters
                action_groups = strategy.get("action_groups", [])
                # Sort by priority (lower = higher priority); fall back
                # to source-cluster impact_score when priority is
                # missing or tied.
                _impact_by_cid = {
                    c.get("cluster_id", ""): float(c.get("impact_score", 0.0))
                    for c in clusters + (soft_signal_clusters or [])
                }

                def _ag_sort_key(_ag: dict) -> tuple:
                    _pri = _ag.get("priority")
                    _pri_v = float(_pri) if isinstance(_pri, (int, float)) else 999.0
                    _src = _ag.get("source_cluster_ids", []) or []
                    _impact = max(
                        (_impact_by_cid.get(_cid, 0.0) for _cid in _src),
                        default=0.0,
                    )
                    return (_pri_v, -_impact)

                action_groups = sorted(action_groups, key=_ag_sort_key)
                ag = action_groups[0] if action_groups else None
                if _process_all_ags and len(action_groups) > 1:
                    pending_action_groups = list(
                        action_groups[1:_MAX_AGS_PER_STRATEGIST_CALL]
                    )
                    pending_strategy = strategy
                    print(
                        _section(
                            f"BUFFERING {len(pending_action_groups)} ADDITIONAL AG(S) "
                            f"FOR LATER ITERATION(S)",
                            "-",
                        ) + "\n"
                        + _kv(
                            "Buffered AGs",
                            ", ".join(
                                _a.get("id", "?") for _a in pending_action_groups
                            ),
                        ) + "\n"
                        + _bar("-")
                    )
                else:
                    pending_action_groups = []
                    pending_strategy = None

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

        if ag is None and clusters:
            _remaining_qids = set()
            for c in clusters:
                _remaining_qids.update(c.get("question_ids", []))
            if _remaining_qids and _iter_num <= max_iterations - 1:
                logger.info(
                    "Strategist returned 0 AGs but %d clusters with %d questions remain — "
                    "constructing diagnostic fallback AG",
                    len(clusters), len(_remaining_qids),
                )
                _top_cluster = ranked[0] if ranked else clusters[0]
                ag = {
                    "id": f"AG{iteration_counter}_fallback",
                    "root_cause_summary": _top_cluster.get("root_cause", "unresolved_failures"),
                    "affected_questions": _top_cluster.get("question_ids", []),
                    "source_cluster_ids": [_top_cluster.get("cluster_id", "")],
                    "lever_directives": {
                        "5": {"instruction_guidance": "Add example SQLs and routing instructions for remaining failure patterns"},
                        "6": {"generate_expressions": True},
                    },
                    "rationale": (
                        f"Diagnostic fallback: {len(_remaining_qids)} question(s) still failing. "
                        f"Trying Lever 5 (instructions/examples) + Lever 6 (SQL expressions) "
                        f"as a broad-spectrum fix."
                    ),
                    "coordination_notes": "Fallback AG — strategist returned empty, applying broad-spectrum lever 5+6",
                }
                strategy = strategy or {}
                strategy["action_groups"] = [ag]
                print(
                    _section(f"DIAGNOSTIC FALLBACK AG — {len(_remaining_qids)} questions remain", "!") + "\n"
                    + _kv("Cluster", _top_cluster.get("cluster_id", "?")) + "\n"
                    + _kv("Root cause", _top_cluster.get("root_cause", "?")) + "\n"
                    + _kv("Questions", len(_top_cluster.get("question_ids", []))) + "\n"
                    + _bar("!")
                )

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

        # Tier 3.3: relabel header so the scorecard is clearly identified
        # as "best (post last accepted iter)" rather than conflated with
        # the latest eval. After Tier 1.3, ``prev_accuracy`` is refreshed
        # by the post-enrichment eval, so the two blocks now represent
        # the same reality (current space state), but the label clarifies
        # intent for operators reading stale runs.
        print(
            _section(f"ACTION GROUP {ag_id} — Iteration ({_iteration_label(iteration_counter)})") + "\n"
            + _kv("Root cause", ag.get("root_cause_summary", "?")[:120]) + "\n"
            + _kv("Levers", ", ".join(lever_keys)) + "\n"
            + _kv("Affected questions", len(ag.get("affected_questions", []))) + "\n"
            + _kv("Best accuracy (post last accepted)", f"{best_accuracy:.1f}%") + "\n"
            + _scorecard(best_scores) + "\n"
            + _kv(
                "Failure analysis source",
                f"iter {iteration_counter - 1} full eval (current space state)",
            ) + "\n"
            + _bar("=")
        )

        _ag_source_cids = list(ag.get("source_cluster_ids", []))
        _ag_cluster_info: dict = {}
        # Phase C2: derive identity fields used by DO-NOT-RETRY (D1-D3)
        # from the first source cluster. Action groups can span multiple
        # clusters but in practice they share a root cause (the strategist
        # is instructed to merge clusters with the same blame set). The
        # first cluster's root_cause / blame_set is representative enough
        # for collision detection.
        _ag_root_cause: str = ""
        _ag_blame_set: Any = None
        # T2.1: collect iteration-independent cluster signatures for
        # every source cluster this AG targets. Reflection buffer stamps
        # them so the next iteration can detect "this signature has
        # been tried before" even if the pretty cluster_id changed.
        _ag_source_signatures: list[str] = []
        for _rc_idx, _rc in enumerate(ranked):
            _rc_cid = _rc.get("cluster_id", "")
            if _ag_source_cids and _rc_cid not in set(_ag_source_cids):
                continue
            _rc_sig = _rc.get("cluster_signature")
            if _rc_sig and _rc_sig not in _ag_source_signatures:
                _ag_source_signatures.append(_rc_sig)
            if not _ag_cluster_info:
                _ag_cluster_info = {
                    "cluster_id": _rc_cid,
                    "impact_score": _rc.get("impact_score"),
                    "rank": _rc_idx + 1,
                    "question_count": len(_rc.get("question_ids", [])),
                    "root_cause": _rc.get("root_cause") or _rc.get("asi_failure_type"),
                    "affected_questions": _rc.get("question_ids", [])[:20],
                    "cluster_signature": _rc_sig,
                }
                _ag_root_cause = (
                    _rc.get("asi_failure_type")
                    or _rc.get("root_cause")
                    or ""
                )
                _ag_blame_set = _rc.get("asi_blame_set")

        # Phase C2: reusable identity kwargs for every _build_reflection_entry
        # call in this AG iteration. Keeps call sites DRY while guaranteeing
        # the forbidden-set / tried-cluster bookkeeping downstream always
        # sees the same root_cause / blame_set / source_cluster_ids.
        _ag_identity_kwargs = {
            "root_cause": _ag_root_cause,
            "blame_set": _ag_blame_set,
            "source_cluster_ids": list(_ag_source_cids),
            "source_cluster_signatures": list(_ag_source_signatures),
        }

        # Phase D2: collision guard. The strategist occasionally re-proposes
        # a previously-rejected (root_cause, blame_set, lever_set) tuple
        # despite the DO NOT RETRY hint in its prompt (see Q004 regression).
        # When that happens, skip this AG rather than deploying the same
        # patch again. The reflection entry is logged with rollback_class
        # OTHER so this skip doesn't count against any budget — it's
        # purely a routing correction.
        _forbidden = _compute_forbidden_ag_set(reflection_buffer)
        _collision_key = _ag_collision_key(
            ag, _ag_root_cause, _ag_blame_set, lever_keys,
        )
        if _collision_key is not None and _collision_key in _forbidden:
            _rc_k, _blame_k, _lever_k = _collision_key
            print(
                _section(f"[{ag_id}] AG COLLISION — skipping", "!") + "\n"
                + _kv("Root cause", _rc_k) + "\n"
                + _kv("Blame", _blame_k) + "\n"
                + _kv("Lever set", sorted(_lever_k)) + "\n"
                + _kv(
                    "Reason",
                    "strategist re-proposed a (root_cause, blame, lever_set) "
                    "tuple previously rolled back for content regression",
                ) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_COLLISION_SKIPPED", "SKIPPED",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "root_cause": _rc_k,
                    "blame_set": list(_blame_k) if isinstance(_blame_k, tuple) else _blame_k,
                    "lever_set": sorted(_lever_k),
                },
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys], target_objects=[],
                prev_scores=best_scores, new_scores=best_scores,
                rollback_reason="ag_collision_with_forbidden_set",
                patches=[],
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                **_ag_identity_kwargs,
            ))
            continue

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
                    **_ag_identity_kwargs,
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
                        **_ag_identity_kwargs,
                    ))
                else:
                    _unfixed = set(ag.get("affected_questions", [])) - set(
                        _esc_result.get("detail", {}).get("corrected_qids", [])
                    ) - set(
                        _esc_result.get("detail", {}).get("quarantined_qids", [])
                    )
                    escalated_gt_repair_qids.update(_unfixed)
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
                        **_ag_identity_kwargs,
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
                    _tvf_lever = 3
                    for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):
                        write_patch(
                            spark, run_id, iteration_counter, _tvf_lever, idx,
                            _build_patch_record(entry, _tvf_lever, apply_mode),
                            catalog, schema,
                        )
                    if _tvf_apply_log.get("patch_deployed", False):
                        logger.info("TVF %s removed successfully (tier=%s)", _tvf_id, _esc_tier)
                        metadata_snapshot = _tvf_apply_log.get("post_snapshot", metadata_snapshot)
                        if _original_instruction_sections:
                            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections
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
                benchmarks=benchmarks,
            )
            all_proposals.extend(lever_proposals)

        # ── T2.2: Reflection-as-validator ────────────────────────────
        # Build a per-patch forbidden set from prior rolled-back iterations
        # and drop any proposal whose (patch_type, target) signature was
        # already rolled back. Without this the strategist routinely
        # re-proposes the same patch type against the same table after a
        # content regression (iter-1 + iter-3 of the retail corpus both
        # patched mv_7now_fact_sales.description → rolled back → iter-3
        # re-proposed update_description on mv_7now_fact_sales).
        #
        # Escape hatch: a proposal carrying
        # ``escalation_justification: <non-empty>`` bypasses the filter,
        # and every rejection is logged so operators can see what was
        # dropped and why. The existing cluster-level DO-NOT-RETRY
        # (_compute_forbidden_ag_set) covers lever/root-cause combos;
        # this new per-patch guard covers patch-type/target combos.
        _patch_forbidden: set[tuple[str, str]] = set()
        for _rb in reflection_buffer:
            if _rb.get("accepted"):
                continue
            # CONTENT_REGRESSION rollbacks are the ones that signal "the
            # patch made things worse". Other classes (infra, schema)
            # don't indicate the patch was wrong, only that applying it
            # blew up.
            from genie_space_optimizer.optimization.rollback_class import (
                RollbackClass as _RC,
            )
            if _rb.get("rollback_class") != _RC.CONTENT_REGRESSION.value:
                continue
            for _dnr in _rb.get("do_not_retry", []):
                _s = str(_dnr).strip()
                if " on " not in _s:
                    continue
                _ptype, _target = _s.split(" on ", 1)
                _patch_forbidden.add((_ptype.strip(), _target.strip()))

        # B1.3 — diagnostics so an empty ``_patch_forbidden`` is
        # debuggable: distinguish (a) no rollbacks yet, (b) all
        # rollbacks classified non-CONTENT_REGRESSION, (c)
        # CONTENT_REGRESSION rollbacks but empty ``do_not_retry``.
        from genie_space_optimizer.optimization.rollback_class import (
            RollbackClass as _RC_DIAG,
        )
        _total_rb = sum(
            1 for r in reflection_buffer if not r.get("accepted")
        )
        _content_rb = sum(
            1 for r in reflection_buffer
            if not r.get("accepted")
            and r.get("rollback_class") == _RC_DIAG.CONTENT_REGRESSION.value
        )
        _content_rb_with_dnr = sum(
            1 for r in reflection_buffer
            if not r.get("accepted")
            and r.get("rollback_class") == _RC_DIAG.CONTENT_REGRESSION.value
            and r.get("do_not_retry")
        )
        logger.info(
            "[%s] T2.2 forbidden set: size=%d  rollbacks_total=%d  "
            "content_rollbacks=%d  with_do_not_retry=%d",
            ag_id, len(_patch_forbidden), _total_rb, _content_rb,
            _content_rb_with_dnr,
        )

        if _patch_forbidden:
            _kept: list[dict] = []
            _dropped: list[tuple[str, str, str]] = []  # (ptype, target, reason)
            for _p in all_proposals:
                _ptype = str(_p.get("type") or _p.get("patch_type") or "")
                # B1.1 — raw proposals carry ``table`` (column-level
                # patches) before ``proposals_to_patches`` populates
                # ``target``. Without reading ``table`` here, T2.2
                # extracts ``"?"`` and never matches the forbidden
                # set entries (which use the FQN target string).
                _target = str(
                    _p.get("target") or _p.get("target_object")
                    or _p.get("target_table")
                    or _p.get("table")
                    or "?"
                )
                _key = (_ptype, _target)
                _justification = str(_p.get("escalation_justification") or "").strip()
                if _key in _patch_forbidden and not _justification:
                    _dropped.append((_ptype, _target,
                                     "rolled back previously (no escalation_justification)"))
                else:
                    _kept.append(_p)
            if _dropped:
                logger.warning(
                    "[%s] T2.2 reflection-as-validator dropped %d proposal(s) "
                    "that were rolled back in prior iterations without an "
                    "escalation_justification:",
                    ag_id, len(_dropped),
                )
                for _ptype, _target, _reason in _dropped:
                    logger.warning("  - %s on %s (%s)", _ptype, _target, _reason)
                print(
                    _section(
                        f"[{ag_id}] T2.2 Reflection validator: dropped "
                        f"{len(_dropped)} re-proposal(s)",
                        "-",
                    )
                )
                for _ptype, _target, _reason in _dropped:
                    print(f"|  - {_ptype} on {_target} ({_reason})")
                print(_bar("-"))
            all_proposals = _kept

        # ── T2.4: Counterfactual asset-impact scan ───────────────────
        # Before applying, identify passing benchmarks that reference
        # each patch's target asset. If a patch touches an asset that
        # many passing questions depend on, we're rolling the dice on
        # those questions — stamp ``high_collateral_risk`` on the
        # proposal and warn prominently. Downstream the slice-gate
        # (once T3.1 lands) should prioritise the at-risk set.
        _passing_qids = set(b.get("id") for b in benchmarks if b.get("id")) - (prev_failure_qids or set())
        _affected_qids = set(ag.get("affected_questions", []) or [])
        _affected_n = max(len(_affected_qids), 1)
        _collateral_details: list[tuple[str, str, list[str]]] = []

        # B2 — SQL-text fallback for benchmarks that don't carry
        # ``required_tables`` / ``required_columns`` metadata. The
        # original scan was silent in that case; substring-matching
        # against the benchmark's expected/ground-truth SQL keeps
        # detection working even when auto-synthesis skipped the
        # asset metadata.
        def _sql_text_for_benchmark(_b: dict) -> str:
            return " ".join(
                str(_b.get(k, "")) for k in
                ("expected_response", "expected_sql", "ground_truth_sql")
            ).lower()

        for _p in all_proposals:
            _ptype = str(_p.get("type") or _p.get("patch_type") or "")
            # B2 — read ``table`` for raw column-level proposals
            # (parallel to B1.1). Without this, every column-level
            # proposal had ``_target == ""`` and ``continue``d
            # silently, so the scan never ran for any column patch.
            _target = str(
                _p.get("target") or _p.get("target_object")
                or _p.get("target_table") or _p.get("table") or ""
            ).lower()
            _target_column = str(_p.get("column") or "").lower()
            if not _target:
                continue
            # B2 — FQN normalisation. Patch targets are usually
            # ``catalog.schema.table`` while benchmarks may carry
            # only ``table`` (unqualified) in their SQL or asset
            # lists. Match on the unqualified tail too.
            _target_tail = _target.split(".")[-1] if "." in _target else _target
            _target_candidates = {_target, _target_tail}
            if _target_column:
                _target_candidates.add(f"{_target_tail}.{_target_column}")
            _dependents: list[str] = []
            for _b in benchmarks:
                _bid = _b.get("id", "")
                if not _bid or _bid not in _passing_qids:
                    continue
                _bench_assets = [
                    str(t).lower() for t in
                    (_b.get("required_tables") or []) + (_b.get("required_columns") or [])
                ]
                _matched = any(
                    any(c == _ba or c in _ba or _ba in c for c in _target_candidates)
                    for _ba in _bench_assets
                )
                if not _matched:
                    _sql_text = _sql_text_for_benchmark(_b)
                    if _sql_text and any(
                        c and c in _sql_text for c in _target_candidates
                    ):
                        _matched = True
                if _matched:
                    _dependents.append(_bid)
            if _dependents:
                _p["passing_dependents"] = _dependents[:50]
                if len(_dependents) >= 2 * _affected_n:
                    _p["high_collateral_risk"] = True
                    _collateral_details.append(
                        (_ptype, _target, _dependents[:10])
                    )
        # B2 — ALWAYS print a summary so operators can distinguish
        # "scan ran, nothing flagged" from "scan didn't run".
        _total_with_metadata = sum(
            1 for _b in benchmarks if _b.get("required_tables")
        )
        logger.info(
            "[%s] T2.4 counterfactual scan: %d/%d proposal(s) high-risk; "
            "benchmarks_with_required_tables=%d/%d (SQL-text fallback used otherwise)",
            ag_id, len(_collateral_details), len(all_proposals),
            _total_with_metadata, len(benchmarks),
        )
        print(
            _section(
                f"[{ag_id}] T2.4 Counterfactual scan: "
                f"{len(_collateral_details)} high-risk proposal(s)",
                "-",
            )
        )
        print(
            _kv(
                "Benchmarks with required_tables",
                f"{_total_with_metadata}/{len(benchmarks)}",
            )
        )
        if _collateral_details:
            for _ptype, _target, _deps in _collateral_details:
                print(
                    f"|  - {_ptype} on {_target}  "
                    f"(passing dependents: {len(_deps)}+, "
                    f"affected: {_affected_n})"
                )
                print(f"|    sample deps: {', '.join(_deps[:5])}")
        print(_bar("-"))

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
            {"cluster_id": p.get("cluster_id"), "proposal_id": p.get("proposal_id"), "patch_type": p.get("patch_type"), "lever": p.get("lever")}
            for p in all_proposals if p.get("cluster_id")
        ]
        try:
            update_provenance_proposals(spark, run_id, iteration_counter - 1, _prop_mappings, catalog, schema)
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
                **_ag_identity_kwargs,
            ))
            continue

        # ── Apply coordinated patch set ──────────────────────────────
        patches = proposals_to_patches(all_proposals)

        # Phase 4.3: expand ``rewrite_instruction`` proposals into
        # ``update_instruction_section`` children BEFORE the cap so a
        # single rewrite_instruction proposal does not balloon past the
        # cap inside ``apply_patch_set``. Idempotent — applier will not
        # re-split children that already carry ``_split_from``.
        try:
            from genie_space_optimizer.optimization.applier import (
                _expand_rewrite_splits as _harness_expand_splits,
            )
            _pre_split_count = len(patches)
            patches = _harness_expand_splits(patches)
            if len(patches) > _pre_split_count:
                logger.info(
                    "Phase 4.3: rewrite splits expanded patch list "
                    "%d -> %d before diversity-aware cap",
                    _pre_split_count, len(patches),
                )
        except Exception:
            logger.debug(
                "Phase 4.3: pre-cap rewrite split expansion failed (non-fatal)",
                exc_info=True,
            )

        # Tier 2.6: cap AG patch-set size. A single failing patch in a
        # large batch rolls back everything — including the patches that
        # would have helped. If the cap is exceeded, keep the highest-
        # confidence / highest-impact patches first (sorted by the
        # caller); extras are dropped with a clear warning.
        if len(patches) > MAX_AG_PATCHES:
            # B4 — diversity-aware cap. The previous slice-only cap
            # consumed the budget with low-risk Lever-1 column
            # patches and dropped Lever-5 / Lever-6 patches at the
            # tail — exactly the patches that address pattern-level
            # failures. Pass 1 keeps at least one patch per distinct
            # lever; pass 2 fills the remaining budget by the
            # public ``risk_level`` field, preserving stable
            # original order within each risk group.
            #
            # Uses each patch's existing ``risk_level`` (set by
            # ``proposals_to_patches`` via ``classify_risk``) — no
            # private import from applier.
            _LOCAL_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}
            _known_levers = [1, 2, 3, 4, 5, 6]
            _seen_levers: list[int] = []
            _by_lever: dict[int, list[dict]] = {}
            # Phase 4.3: also bucket by ``(lever, section_name)`` so a
            # single ``rewrite_instruction`` that expanded into N
            # section patches can't consume the whole Lever-5 budget.
            _by_lever_section: dict[tuple[int, str], list[dict]] = {}
            for p in patches:
                try:
                    L = int(p.get("lever", 5))
                except (TypeError, ValueError):
                    L = 5
                if L not in _by_lever:
                    _seen_levers.append(L)
                _by_lever.setdefault(L, []).append(p)
                _section_key = str(p.get("section_name") or p.get("section") or "")
                _by_lever_section.setdefault((L, _section_key), []).append(p)
            _lever_order = (
                [L for L in _known_levers if L in _by_lever]
                + [L for L in _seen_levers if L not in _known_levers]
            )

            kept: list[dict] = []
            remaining: dict[int, list[dict]] = {
                k: list(v) for k, v in _by_lever.items()
            }
            # Phase 4.3 pass-1: take one patch per ``(lever, section)``
            # pair so a single rewrite-instruction split can't dominate
            # the entire Lever-5 budget. After this pass, every distinct
            # section that the strategist proposed has at least one slot
            # in the cap.
            _seen_section_keys: set[tuple[int, str]] = set()
            for L in _lever_order:
                _bucket = remaining.get(L) or []
                # Pull the first patch for each unique section in this
                # lever's bucket, preserving original order.
                _new_bucket: list[dict] = []
                for _p in _bucket:
                    _sec = str(_p.get("section_name") or _p.get("section") or "")
                    _key = (L, _sec)
                    if _key not in _seen_section_keys:
                        kept.append(_p)
                        _seen_section_keys.add(_key)
                        if len(kept) >= MAX_AG_PATCHES:
                            break
                    else:
                        _new_bucket.append(_p)
                remaining[L] = _new_bucket + [
                    _p for _p in _bucket
                    if (L, str(_p.get("section_name") or _p.get("section") or ""))
                    in _seen_section_keys and _p not in kept
                ]
                if len(kept) >= MAX_AG_PATCHES:
                    break

            # Pass 2: keep at least one patch per remaining lever (any
            # section).  Preserves the original "diverse levers" goal.
            for L in _lever_order:
                if remaining.get(L) and L not in {int(p.get("lever", 5)) for p in kept}:
                    kept.append(remaining[L].pop(0))
                    if len(kept) >= MAX_AG_PATCHES:
                        break

            if len(kept) < MAX_AG_PATCHES:
                _remaining_flat = [
                    p for L in _lever_order for p in remaining.get(L, [])
                ]
                _remaining_flat.sort(
                    key=lambda p: _LOCAL_RISK_ORDER.get(
                        str(p.get("risk_level", "low")).lower(), 1
                    ),
                )
                kept.extend(_remaining_flat[: MAX_AG_PATCHES - len(kept)])

            _levers_kept = sorted({int(p.get("lever", 5)) for p in kept})
            _levers_dropped = sorted(
                {int(p.get("lever", 5)) for p in patches}
                - set(_levers_kept)
            )
            logger.warning(
                "AG %s patch cap (diversity-aware): kept %d of %d. "
                "Levers kept: %s; levers fully dropped: %s.",
                ag_id, len(kept), len(patches),
                _levers_kept, _levers_dropped,
            )
            print(
                _section(f"[{ag_id}] PATCH CAP APPLIED (diversity-aware)", "-") + "\n"
                + _kv("Original size", len(patches)) + "\n"
                + _kv("Kept", len(kept)) + "\n"
                + _kv("Levers kept", _levers_kept) + "\n"
                + _kv(
                    "Levers fully dropped",
                    _levers_dropped if _levers_dropped else "(none)",
                ) + "\n"
                + _kv(
                    "Reason",
                    "Diversity-aware cap: preserve one patch per distinct "
                    "lever before filling by risk_level.",
                ) + "\n"
                + _bar("-")
            )
            patches = kept

        # T3.3: shadow apply. When enabled, the intent is to clone the
        # space, apply patches to the clone, eval, and promote only on
        # pass. The Genie SDK fork/promote primitives aren't wired yet,
        # so for now we log the intent and fall back to in-place apply;
        # the rollback path below still covers us. Leaving this here so
        # operators can see the flag is respected by code and we have a
        # single commit to wire the actual fork when available.
        if SHADOW_APPLY:
            logger.warning(
                "[%s] T3.3 SHADOW_APPLY=True but the Genie fork/promote "
                "API is not yet integrated in this harness; falling back "
                "to in-place apply with rollback-on-regression.",
                ag_id,
            )
            print(
                _section(f"[{ag_id}] SHADOW_APPLY: fallback", "-") + "\n"
                + _kv(
                    "Reason",
                    "Genie space-clone API not yet wired — rollback path "
                    "still covers content regressions.",
                ) + "\n"
                + _bar("-")
            )

        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )

        _fallback_lever = int(lever_keys[0]) if lever_keys else 0
        for idx, entry in enumerate(apply_log.get("applied", [])):
            _patch_lever = int(entry.get("patch", {}).get("lever", _fallback_lever))
            write_patch(
                spark, run_id, iteration_counter, _patch_lever, idx,
                _build_patch_record(entry, _patch_lever, apply_mode),
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
            from genie_space_optimizer.optimization.rollback_class import (
                RollbackClass,
                classify_rollback_reason,
            )
            _pe_class = classify_rollback_reason(f"patch_deploy_failed: {_pe}")
            print(
                _section(f"[{ag_id}] PATCH DEPLOY FAILED", "!") + "\n"
                + _kv("Error", str(_pe)[:300]) + "\n"
                + _kv("Rollback class", _pe_class.value) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_PATCH_FAILED", "ERROR",
                task_key="lever_loop", iteration=iteration_counter,
                error_message=str(_pe)[:500],
                detail={"rollback_class": _pe_class.value},
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
                **_ag_identity_kwargs,
            ))
            if _pe_class == RollbackClass.SCHEMA_FAILURE:
                print(
                    _section("LEVER LOOP — SCHEMA-FATAL PATCH ERROR", "!") + "\n"
                    + _kv("Error", str(_pe)[:300]) + "\n"
                    + _kv("Rollback class", RollbackClass.SCHEMA_FAILURE.value) + "\n"
                    + _kv("Reason", "Genie API rejected the PATCH payload structure; retrying would deterministically fail.") + "\n"
                    + _bar("!")
                )
                write_stage(
                    spark, run_id, "LEVER_LOOP_SCHEMA_FATAL", "ERROR",
                    task_key="lever_loop", iteration=iteration_counter,
                    error_message=str(_pe)[:500],
                    detail={"rollback_class": RollbackClass.SCHEMA_FAILURE.value},
                    catalog=catalog, schema=schema,
                )
                break
            # Phase C3: INFRA_FAILURE retry budget. Unlike CONTENT_REGRESSION
            # rollbacks, infra failures don't tell us anything about the
            # strategy's quality. We don't want them to count against
            # ``_diminishing_returns`` or the content rollback counter,
            # but an unbounded loop of infra flakes would spin forever —
            # hence the separate budget. Exit cleanly when the budget is
            # exhausted with a dedicated terminal reason.
            if _pe_class == RollbackClass.INFRA_FAILURE:
                _consecutive_infra = 0
                for _rb_entry in reversed(reflection_buffer):
                    if _rb_entry.get("rollback_class") == RollbackClass.INFRA_FAILURE.value:
                        _consecutive_infra += 1
                    else:
                        break
                if _consecutive_infra >= INFRA_RETRY_BUDGET:
                    print(
                        _section("LEVER LOOP — INFRA RETRY BUDGET EXHAUSTED", "!") + "\n"
                        + _kv("Consecutive infra rollbacks", _consecutive_infra) + "\n"
                        + _kv("Budget", INFRA_RETRY_BUDGET) + "\n"
                        + _kv("Last error", str(_pe)[:300]) + "\n"
                        + _bar("!")
                    )
                    write_stage(
                        spark, run_id, "LEVER_LOOP_INFRA_EXHAUSTED", "ERROR",
                        task_key="lever_loop", iteration=iteration_counter,
                        error_message=str(_pe)[:500],
                        detail={
                            "consecutive_infra": _consecutive_infra,
                            "budget": INFRA_RETRY_BUDGET,
                        },
                        catalog=catalog, schema=schema,
                    )
                    break
            continue

        # ── Applied Patches Detail ───────────────────────────────────
        _applied = apply_log.get("applied", [])
        if _applied:
            _ap_lines = [_section(f"[{ag_id}] Applied {len(_applied)} Patch(es)", "=")]
            for ai, aentry in enumerate(_applied, 1):
                _ap = aentry.get("patch", {})
                _aa = aentry.get("action", {})
                _ap_lines.append(_fmt_patch(ai, _ap, _aa, aentry))
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
            max_benchmark_count=max_benchmark_count,
            prev_failure_qids=prev_failure_qids,
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
            # Phase 1.3: drop the AG buffer when an AG rolls back.  The
            # buffered AGs were produced from the same strategist call
            # and tend to share the same flawed root-cause hypothesis;
            # forcing a fresh strategist call gives the next iteration
            # a clean slate informed by the rollback in reflection_buffer.
            if pending_action_groups:
                logger.info(
                    "Dropping %d buffered AG(s) after rollback of %s",
                    len(pending_action_groups), ag_id,
                )
                pending_action_groups = []
                pending_strategy = None
            for lk in lever_keys:
                levers_rolled_back.append(int(lk))
            # Phase E2: include rollback_class in stage detail so the
            # run summary can break rollbacks down by class.
            from genie_space_optimizer.optimization.rollback_class import (
                classify_rollback_reason as _classify_rb,
            )
            _rb_class = _classify_rb(reason).value
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "ROLLED_BACK",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "reason": reason,
                    "levers": lever_keys,
                    "rollback_class": _rb_class,
                },
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
                **_ag_identity_kwargs,
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
                # B1.2 — converted patches use ``type`` / ``target``;
                # the legacy fields ``patch_type`` / ``target_object``
                # are absent on the conversion path so the previous
                # extractor returned ``("", "")`` and the guard
                # silently rejected every entry. Read both for
                # backward compatibility with any pre-conversion
                # fixtures.
                ft = str(p.get("type") or p.get("patch_type") or "").strip()
                tgt = str(p.get("target") or p.get("target_object") or "").strip()
                if ft and tgt:
                    tried_patches.add((ft, tgt))
            _lever_frozenset = frozenset(int(lk) for lk in lever_keys)
            # Phase C3 + D3: only CONTENT_REGRESSION rollbacks contribute
            # to the tried-cluster bookkeeping. Phase D3 also lowers the
            # threshold so the (root_cause, blame, lever_set) 3-tuple is
            # marked after the FIRST content rollback, while the legacy
            # (root_cause, blame) 2-tuple (which suppresses across ALL
            # levers) is only written when we've exhausted ``>= 2``
            # distinct lever sets on the same cluster.
            _content_rb_count = sum(
                1 for _rb_entry in reflection_buffer
                if not _rb_entry.get("accepted")
                and not _rb_entry.get("escalation_handled")
                and _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value
            )
            _should_mark_tried_lever_aware = _content_rb_count >= 1
            source_cids = set(ag.get("source_cluster_ids", []))
            for c in clusters:
                cid = c.get("cluster_id", "")
                if source_cids and cid not in source_cids:
                    continue
                rc_ft = c.get("asi_failure_type") or c.get("root_cause", "other")
                rc_blame = _normalise_blame(c.get("asi_blame_set"))
                if not rc_ft or not _should_mark_tried_lever_aware:
                    continue
                # Always add the 3-tuple (lever-aware) immediately.
                tried_root_causes.add((rc_ft, rc_blame, _lever_frozenset))
                # Legacy 2-tuple: only when the same cluster has failed
                # across >= 2 distinct lever sets (truly-dead cluster).
                _distinct_lever_sets = {
                    frozenset(e.get("lever_set") or [])
                    for e in reflection_buffer
                    if (
                        not e.get("accepted")
                        and not e.get("escalation_handled")
                        and e.get("rollback_class") == _RC.CONTENT_REGRESSION.value
                        and e.get("root_cause") == rc_ft
                        and (e.get("blame_set") or "") == rc_blame
                    )
                }
                # Include the current lever set we're about to add.
                _distinct_lever_sets.add(_lever_frozenset)
                if len(_distinct_lever_sets) >= 2:
                    tried_root_causes.add((rc_ft, rc_blame))
            if not _should_mark_tried_lever_aware:
                logger.info(
                    "No CONTENT_REGRESSION rollbacks yet — keeping cluster "
                    "available for retry (root causes NOT marked as tried)",
                )
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
            **_ag_identity_kwargs,
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
        if _original_instruction_sections:
            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections

        # Phase 7: end-of-iteration diagnostics. Single block summarizing
        # ASI source quality, cluster processing, patch capping, and
        # pre-arbiter accuracy so operators can spot degenerate values
        # at a glance. Any field at a danger threshold (ASI=none > 50%,
        # arbiter rescue > 30%) emits an inline SEVERITY:HIGH banner.
        try:
            _diag_lines = [_section("LEVER_LOOP_DIAGNOSTICS", "=")]
            _diag_lines.append(_kv("Iteration", iteration_counter))
            _diag_lines.append(_kv("AG id", ag_id))

            # ASI source histogram from the latest eval rows. The
            # ``_eval_rows`` variable is populated below in the join-
            # mining block but doesn't exist yet at this point in the
            # iteration; pull rows directly from ``full_result``.
            _asi_source_counts: dict[str, int] = {}
            _asi_total = 0
            _diag_rows = full_result.get("rows", []) if isinstance(full_result, dict) else []
            if not _diag_rows and isinstance(full_result, dict):
                _rows_json = full_result.get("rows_json")
                if isinstance(_rows_json, list):
                    _diag_rows = _rows_json
                elif isinstance(_rows_json, str):
                    try:
                        import json as _json_diag
                        _diag_rows = _json_diag.loads(_rows_json)
                    except (ValueError, TypeError):
                        _diag_rows = []
            for _r in _diag_rows:
                if not isinstance(_r, dict):
                    continue
                for _log in (_r.get("_asi_extraction_log") or []):
                    if not isinstance(_log, dict):
                        continue
                    _src = str(_log.get("source") or "none")
                    _asi_source_counts[_src] = _asi_source_counts.get(_src, 0) + 1
                    _asi_total += 1
            if _asi_total:
                _none_pct = (
                    100 * _asi_source_counts.get("none", 0) / _asi_total
                )
                _hist = ", ".join(
                    f"{k}={v}"
                    for k, v in sorted(
                        _asi_source_counts.items(), key=lambda kv: -kv[1],
                    )
                )
                _diag_lines.append(_kv("ASI source histogram", _hist))
                if _none_pct > 50.0:
                    _diag_lines.append(
                        f"|   [SEVERITY:HIGH] ASI source none={_none_pct:.0f}% > 50% "
                        f"— strategist is reasoning blind on heuristic root_causes"
                    )
            else:
                _diag_lines.append(_kv("ASI source histogram", "(no rows)"))

            # Cluster processing.
            _diag_lines.append(_kv(
                "Hard clusters formed", len(clusters or []),
            ))
            _diag_lines.append(_kv(
                "Soft clusters formed", len(soft_signal_clusters or []),
            ))
            _diag_lines.append(_kv("AGs attempted (cumulative)", len(ags_attempted)))
            _diag_lines.append(_kv("AGs accepted (cumulative)", len(ags_accepted)))
            _diag_lines.append(_kv("AGs rolled back (cumulative)", len(ags_rolled_back)))
            _diag_lines.append(_kv("Pending AGs in buffer", len(pending_action_groups)))

            # Patch cap.
            _diag_lines.append(_kv("Patches applied this AG", len(apply_log.get("applied", []))))

            # Pre-arbiter accuracy + arbiter rescue rate.
            _full_scores = (
                full_result.get("scores", {})
                if isinstance(full_result, dict) else {}
            )
            _pre_arb = _full_scores.get(
                "_pre_arbiter/overall_accuracy",
                _full_scores.get("_pre_arbiter/result_correctness"),
            )
            _adj = (
                full_result.get("overall_accuracy")
                if isinstance(full_result, dict) else None
            )
            if _pre_arb is not None:
                _diag_lines.append(
                    _kv("Pre-arbiter accuracy", f"{_pre_arb:.1f}%"),
                )
            if _adj is not None:
                _diag_lines.append(
                    _kv("Arbiter-adjusted accuracy", f"{_adj:.1f}%"),
                )
            _bcr = (
                full_result.get("both_correct_rate")
                if isinstance(full_result, dict) else None
            )
            if isinstance(_bcr, (int, float)) and _bcr is not None:
                _alljudge = float(
                    _full_scores.get("_pre_arbiter/result_correctness", 0.0)
                ) / 100.0
                _rescue = max(0.0, float(_bcr) - _alljudge)
                _diag_lines.append(
                    _kv("Arbiter rescue rate", f"{_rescue*100:.1f}%"),
                )
                if _rescue > 0.30:
                    _diag_lines.append(
                        f"|   [SEVERITY:HIGH] Arbiter rescue rate "
                        f"{_rescue*100:.1f}% > 30% — pre-arbiter is the "
                        f"truthful signal"
                    )

            _diag_lines.append(_bar("="))
            print("\n".join(_diag_lines))
        except Exception:
            logger.debug(
                "Phase 7: LEVER_LOOP_DIAGNOSTICS block failed (non-fatal)",
                exc_info=True,
            )

        # ── Mine execution-proven joins from latest eval rows ────────
        try:
            _eval_rows = full_result.get("rows", [])
            if not _eval_rows:
                _eval_rows_json = full_result.get("rows_json")
                if isinstance(_eval_rows_json, str):
                    import json as _json_mod
                    try:
                        _eval_rows = _json_mod.loads(_eval_rows_json)
                    except (ValueError, TypeError):
                        _eval_rows = []
                elif isinstance(_eval_rows_json, list):
                    _eval_rows = _eval_rows_json
            if _eval_rows:
                _mine_result = _mine_and_apply_proven_joins(
                    w, spark, run_id, space_id, metadata_snapshot, _eval_rows,
                    catalog, schema, iteration=iteration_counter,
                )
                if _mine_result.get("total_applied", 0) > 0:
                    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg
                    config = _fetch_cfg(w, space_id)
                    metadata_snapshot = config.get("_parsed_space", config)
        except Exception:
            logger.debug(
                "Iterative join mining failed at iter %d (non-fatal)",
                iteration_counter, exc_info=True,
            )

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
    # Phase E3: break down rolled-back AGs by rollback_class so operators
    # can tell "3 rolled back (1 content, 2 infra)" from "3 rolled back
    # (3 content)" at a glance.
    _rb_class_counter: Counter[str] = Counter()
    for _rb_entry in reflection_buffer:
        if _rb_entry.get("accepted"):
            continue
        if _rb_entry.get("escalation_handled"):
            continue
        _rb_class_counter[_rb_entry.get("rollback_class", "other")] += 1
    if len(ags_rolled_back) and _rb_class_counter:
        _rb_class_str = ", ".join(
            f"{v} {k}" for k, v in sorted(_rb_class_counter.items())
        )
        _summary.append(_kv(
            "Action groups rolled back",
            f"{len(ags_rolled_back)} ({_rb_class_str})",
        ))
    else:
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
                    # Tier 4: v2 name — ``<run_short>/finalize/repeat_pass_{k}``.
                    from genie_space_optimizer.common.mlflow_names import (
                        default_tags as _v2_tags_rep,
                        finalize_run_name as _finalize_run_name_rep,
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
                        run_name=_finalize_run_name_rep(
                            run_id,
                            detail=f"repeat_pass_{rep_run_idx}",
                            iteration=iteration_counter,
                        ),
                        extra_tags=_v2_tags_rep(
                            run_id, space_id=space_id, stage="finalize_repeatability",
                            iteration=iteration_counter,
                        ),
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
                try:
                    from genie_space_optimizer.common.genie_client import fetch_space_config as _ho_fetch
                    _ho_cfg = _ho_fetch(w, space_id)
                    _ho_parsed = _ho_cfg.get("_parsed_space", _ho_cfg)
                    _ho_instr = _ho_parsed.get("instructions", {}) if isinstance(_ho_parsed, dict) else {}
                    _ho_instr_text = _ho_instr.get("text_instructions", "") if isinstance(_ho_instr, dict) else ""
                except Exception:
                    _ho_instr_text = ""
                ho_scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_ho_instr_text)

                # Tier 4: v2 name — ``<run_short>/finalize/held_out``.
                from genie_space_optimizer.common.mlflow_names import (
                    default_tags as _v2_tags_ho,
                    finalize_run_name as _finalize_run_name_ho,
                )
                held_out_result = run_evaluation(
                    space_id, exp_name, iteration_counter, held_out_benchmarks,
                    domain, prev_model_id, "held_out",
                    ho_predict_fn, ho_scorers,
                    spark=spark, w=w, catalog=catalog, gold_schema=schema,
                    uc_schema=f"{catalog}.{schema}",
                    run_name=_finalize_run_name_ho(
                        run_id, detail="held_out", iteration=iteration_counter,
                    ),
                    extra_tags=_v2_tags_ho(
                        run_id, space_id=space_id, stage="finalize_held_out",
                        iteration=iteration_counter,
                    ),
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

            # Resolve stale flags for questions that now pass in the latest evaluation
            _PASSING_VERDICTS = {"both_correct", "genie_correct"}
            _now_passing: set[str] = set()
            for _qid, _entries in _verdict_history.items():
                if _entries and _entries[-1].verdict in _PASSING_VERDICTS:
                    _now_passing.add(_qid)
            _still_failing = {item["question_id"] for item in _persistent_items} if _persistent_items else set()
            _resolve_candidates = _now_passing - _still_failing
            if _resolve_candidates:
                try:
                    from genie_space_optimizer.optimization.labeling import resolve_stale_flags
                    _resolved = resolve_stale_flags(spark, catalog, schema, domain, _resolve_candidates)
                    if _resolved:
                        logger.info("Resolved %d stale flag(s) for now-passing questions", _resolved)
                except Exception:
                    logger.debug("Failed to resolve stale flags", exc_info=True)

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
        # Merge-not-overwrite with existing user-authored benchmarks; tagged
        # with [auto-optimize] + source metadata. Opt-out via
        # GSO_PUBLISH_BENCHMARKS_TO_SPACE=0.
        benchmark_publish_count = 0
        _check_timeout("publish_benchmarks")
        _emit_heartbeat("publish_benchmarks", force=True)
        from genie_space_optimizer.common.config import PUBLISH_BENCHMARKS_TO_SPACE
        if benchmarks and PUBLISH_BENCHMARKS_TO_SPACE:
            try:
                from genie_space_optimizer.common.genie_client import (
                    publish_benchmarks_to_genie_space,
                )

                benchmark_publish_count = publish_benchmarks_to_genie_space(
                    w, space_id, benchmarks, run_id=run_id,
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

        # Postflight IQ scan (soft-fail). Flag-gated via scan_snapshots; runs
        # before the terminal status write so the phase='postflight' row is
        # committed even if update_run_status subsequently fails.
        try:
            from genie_space_optimizer.optimization.scan_snapshots import (
                run_postflight_scan,
            )
            run_postflight_scan(
                w, spark, run_id, space_id, catalog, schema,
                best_accuracy=prev_scores.get("accuracy") if isinstance(prev_scores, dict) else None,
            )
        except Exception:
            logger.warning(
                "Postflight scan hook raised for run=%s — continuing",
                run_id, exc_info=True,
            )

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
    _lines.append(_kv("Iteration", _iteration_label(iteration_counter)))
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
        # S10 — ``None`` signals "no prior lever". Previously we returned
        # ``0`` which collides with a legitimate "loop starts at index 0"
        # reading. The display label ``Starting fresh`` makes the intent
        # explicit; see the _resume_display block in ``run_lever_loop``.
        return {"resume_from_lever": None, "iteration_counter": 0}

    stages_df = load_stages(spark, run_id, catalog, schema)
    last_lever: int | None = None
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
    from genie_space_optimizer._workspace_client import make_workspace_client
    w = make_workspace_client()
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

        # Stage 2.5: Proactive Enrichment (always runs)
        _enrichment_out = None
        _effective_model_id = model_id
        try:
            _enrichment_out = _run_enrichment(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                catalog, schema,
                baseline_model_id=model_id,
                held_out_benchmarks=held_out_benchmarks,
            )
            if not _enrichment_out["enrichment_skipped"]:
                _effective_model_id = _enrichment_out["enrichment_model_id"]
        except Exception:
            logger.exception(
                "Enrichment failed for run %s — continuing with baseline model",
                run_id_str,
            )

        if thresholds_met:
            result.status = "CONVERGED"
            result.convergence_reason = "baseline_meets_thresholds"
            result.best_accuracy = prev_accuracy
            result.best_model_id = _effective_model_id
            result.final_scores = prev_scores
            try:
                from genie_space_optimizer.optimization.scan_snapshots import (
                    run_postflight_scan,
                )
                run_postflight_scan(
                    w, spark, run_id_str, space_id, catalog, schema,
                    best_accuracy=prev_accuracy,
                )
            except Exception:
                logger.warning(
                    "Postflight scan hook raised for run=%s — continuing",
                    run_id_str, exc_info=True,
                )
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason="baseline_meets_thresholds",
            )
        elif prev_accuracy >= 99.0 and not thresholds_met:
            _ao_qids = baseline_out.get("arbiter_overridden_qids", [])
            _ss_qids = baseline_out.get("soft_signal_qids", [])

            # Tier 1.6: if baseline is arbiter-saturated AND the unified
            # hard-failure predicate finds zero hard rows, the loop has
            # literally nothing to cluster on. Previously the loop would
            # still produce ghost clusters (rows where arbiter disagreed
            # despite rc=yes) and attack them, causing the ghost-ceiling
            # rollback cycle. We explicitly tag this as
            # ``arbiter_saturated_no_clusters`` so the convergence reason
            # is diagnosable without log-mining.
            _hard_failure_count = 0
            try:
                from genie_space_optimizer.optimization.evaluation import (
                    row_is_hard_failure,
                )
                _baseline_rows = baseline_out.get("eval_result", {}).get("rows") or []
                _hard_failure_count = sum(
                    1 for r in _baseline_rows if isinstance(r, dict) and row_is_hard_failure(r)
                )
            except Exception:
                logger.debug("Could not compute unified hard-failure count", exc_info=True)

            _saturation_reason = (
                "arbiter_saturated_no_clusters"
                if _hard_failure_count == 0
                else "arbiter_saturated"
            )

            logger.warning(
                "ARBITER-SATURATED (%s): accuracy=%.1f%% but thresholds not met. "
                "%d arbiter-overridden, %d soft-signal questions, %d unified hard failures. "
                "The lever loop cannot improve further — flagging for human review.",
                _saturation_reason, prev_accuracy,
                len(_ao_qids), len(_ss_qids), _hard_failure_count,
            )
            _review_items = []
            for _aq in _ao_qids:
                _review_items.append({
                    "question_id": _aq,
                    "question_text": "",
                    "reason": "ARBITER_SATURATED: arbiter overrode judge failures; "
                              "ground truth may need manual review",
                    "iterations_failed": 0,
                    "patches_tried": "none (arbiter-saturated baseline)",
                })
            if _review_items:
                from genie_space_optimizer.optimization.labeling import flag_for_human_review
                flag_for_human_review(
                    spark, run_id_str, catalog, schema, domain, _review_items,
                )
            result.status = "CONVERGED"
            result.convergence_reason = _saturation_reason
            result.best_accuracy = prev_accuracy
            result.best_model_id = _effective_model_id
            result.final_scores = prev_scores
            write_stage(
                spark, run_id_str, "ARBITER_SATURATED_EXIT", "COMPLETE",
                task_key="lever_loop", catalog=catalog, schema=schema,
                detail={
                    "baseline_accuracy": prev_accuracy,
                    "arbiter_overridden_count": len(_ao_qids),
                    "soft_signal_count": len(_ss_qids),
                    "hard_failure_count": _hard_failure_count,
                    "convergence_reason": _saturation_reason,
                    "thresholds_met": False,
                    "scores": prev_scores,
                },
            )
            try:
                from genie_space_optimizer.optimization.scan_snapshots import (
                    run_postflight_scan,
                )
                run_postflight_scan(
                    w, spark, run_id_str, space_id, catalog, schema,
                    best_accuracy=prev_accuracy,
                )
            except Exception:
                logger.warning(
                    "Postflight scan hook raised for run=%s — continuing",
                    run_id_str, exc_info=True,
                )
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason=_saturation_reason,
            )
        else:
            # Stage 3: Lever Loop (enrichment already done)
            loop_out = _run_lever_loop(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                prev_scores, prev_accuracy, model_id,
                _enrichment_out["config"] if _enrichment_out else {},
                catalog, schema, levers, max_iterations, thresholds, apply_mode,
                triggered_by=triggered_by or "",
                human_corrections=human_corrections,
                enrichment_done=bool(_enrichment_out),
                enrichment_model_id=_effective_model_id,
                iq_scan_recommended_levers=cast(
                    list[int],
                    preflight_out.get("iq_scan_recommended_levers") or [],
                ),
                iq_scan_summary=cast(
                    dict | None, preflight_out.get("iq_scan_summary"),
                ),
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
    """Build a patch record dict for Delta from an apply_log entry.

    T2.13: preserves the original proposal-side patch_type in
    ``patch_type`` (so downstream readers that track proposal stats keep
    working) while adding ``applied_patch_type`` / ``applied_patch_detail``
    columns that reflect what the applier actually executed. For a
    downgraded ``rewrite_instruction`` that was split into per-section
    ``update_instruction_section`` children, the original proposal row
    loses its applied record but each child gets its own row with
    ``patch_type=rewrite_instruction, applied_patch_type=update_instruction_section``
    and ``applied_patch_detail="section=ASSET ROUTING; lever=5; split_from=rewrite_instruction"``.
    """
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    applied_type = entry.get("applied_patch_type") or patch.get("type", action.get("action_type", "unknown"))
    proposal_type = entry.get("proposal_patch_type") or patch.get("_proposal_patch_type") or patch.get("type", applied_type)
    return {
        "patch_type": proposal_type,
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", "medium"),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id", patch.get("proposal_id", "")),
        "applied_patch_type": applied_type,
        "applied_patch_detail": entry.get("applied_patch_detail"),
    }


# The judge-failure predicates moved to ``evaluation.py`` so the
# ``ground_truth_corrections`` module (Task 1) can import them without
# pulling in ``harness``. The aliases below preserve the legacy
# underscore-prefixed names for existing call sites in this module.
from genie_space_optimizer.optimization.evaluation import (
    get_failed_judges as _get_failed_judges,
    has_individual_judge_failure as _has_individual_judge_failure,
)


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
