"""Auto-Optimize router — thin proxy bridging Workbench auth to the GSO engine."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from backend.routers._validators import RunId, SpaceId

from backend.services.auth import get_workspace_client, get_service_principal_client, get_databricks_host
from backend.services import gso_lakebase
from genie_space_optimizer.backend.utils import safe_int, safe_float, safe_finite, safe_json_parse
from genie_space_optimizer.integration import (
    trigger_optimization,
    apply_optimization,
    discard_optimization,
    get_lever_info,
    IntegrationConfig,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auto-optimize")

# Lightweight column list for iterations queries — excludes rows_json (megabytes per row).
# Bug #2: evaluated_count / excluded_count / quarantined_benchmarks_json MUST be
# included so the frontend can compute `accuracy = correct / evaluated` without
# falling back to total_questions (the original Bug #2 regression).
#
# The V2 list is what we WANT. The LEGACY list is what pre-migration Delta tables
# actually have. `_select_iterations_delta` tries V2 first, then degrades to
# LEGACY when the table is behind the GSO job's _migrate_add_columns. This keeps
# the Workbench UI rendering scores when the job bundle and the app are on
# slightly different deploy versions.
_ITER_COLS = _ITER_COLS_V2 = (
    "iteration, eval_scope, overall_accuracy, total_questions, correct_count, "
    "evaluated_count, excluded_count, quarantined_benchmarks_json, "
    "scores_json, failures_json, thresholds_met, lever, repeatability_pct, "
    "reflection_json, mlflow_run_id"
)
_ITER_COLS_LEGACY = (
    "iteration, eval_scope, overall_accuracy, total_questions, correct_count, "
    "scores_json, failures_json, thresholds_met, lever, repeatability_pct, "
    "reflection_json, mlflow_run_id"
)

# Lever names — matches GSO common/config.py
LEVER_NAMES: dict[int, str] = {
    0: "Proactive Enrichment",
    1: "Tables & Columns",
    2: "Metric Views",
    3: "SQL Queries & Functions",
    4: "Join Specifications",
    5: "Text Instructions",
    6: "SQL Expressions",
}

_TERMINAL_RUN_STATUSES = {
    "CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED",
    "APPLIED", "DISCARDED",
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TriggerRequest(BaseModel):
    space_id: str = Field(..., pattern=r"^[0-9a-zA-Z_-]{1,128}$")
    apply_mode: str = "genie_config"
    levers: list[int] | None = None
    deploy_target: str | None = None


class SchemaAccessStatus(BaseModel):
    catalog: str
    schema_name: str
    read_granted: bool
    grant_sql: str | None = None


class PermissionCheckResponse(BaseModel):
    sp_display_name: str
    sp_application_id: str = ""
    sp_has_manage: bool
    schemas: list[SchemaAccessStatus]
    # Fail-closed default: availability must be proven by the probe, not assumed.
    prompt_registry_available: bool = False
    prompt_registry_error: str | None = None
    # Stable reason code for UI/alerting; paired with prompt_registry_error.
    # One of: ok | feature_not_enabled | missing_uc_permissions |
    # registry_path_not_found | missing_sp_scope | vendor_bug |
    # unknown (legacy) | probe_error.
    prompt_registry_reason_code: str | None = None
    # Raw vendor error code (e.g. ENDPOINT_NOT_FOUND). Surfaced verbatim in
    # the UI mono block so the next unmapped code is visible without a log
    # dive. May be None when the probe succeeded or raised a non-SDK error.
    prompt_registry_error_code: str | None = None
    # Two-axis actionability: "customer" (admin flips toggle / grants perms)
    # vs. "platform" (our bug or Databricks' bug). Drives UI chip color and
    # alert routing. None = unknown (treated as platform by the UI).
    prompt_registry_actionable_by: str | None = None
    can_start: bool
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    return bool(os.environ.get("GSO_CATALOG")) and bool(os.environ.get("GSO_JOB_ID"))


def _delta_query(sql: str, *, strict: bool = False) -> list[dict]:
    """Execute a query against the Delta table via SQL Warehouse.

    Returns a list of dicts (rows).

    By default, any error is swallowed and `[]` is returned (legacy behavior —
    most callers only need best-effort reads). Pass ``strict=True`` to re-raise
    the underlying exception so the caller can distinguish "query failed" from
    "table is empty" — required for `_select_iterations_delta` which needs to
    detect the pre-migration schema-drift case.
    """
    config = _build_gso_config()
    if not config.warehouse_id:
        return []
    try:
        from genie_space_optimizer.common.warehouse import sql_warehouse_query
        ws = get_service_principal_client()
        df = sql_warehouse_query(ws, config.warehouse_id, sql)
        if df.empty:
            return []
        return df.to_dict(orient="records")
    except Exception as exc:
        if strict:
            raise
        logger.warning("Delta query failed: %s", exc, exc_info=True)
        return []


def _delta_table(name: str) -> str:
    """Return fully-qualified Delta table name for a GSO table."""
    config = _build_gso_config()
    return f"{config.catalog}.{config.schema_name}.{name}"


# Bug #2 regression (April 2026): `_ITER_COLS_V2` requires three columns that
# only land on the Delta table when the GSO job's `_migrate_add_columns`
# runs (see `packages/genie-space-optimizer/.../optimization/state.py`). If
# the app wheel and the job wheel are on different deploy versions — e.g. the
# app was redeployed with the new SELECT before the bundle-deployed job ran
# its first migrated run — the V2 SELECT fails with UNRESOLVED_COLUMN and the
# UI goes blank. We disambiguate "real error / legacy schema" from "empty
# table" with the module-level flag below so the second SELECT doesn't run on
# every empty-iterations run (first-time CONVERGED, brand-new runs, etc.).
_iterations_schema_legacy: bool | None = None  # None = unknown, True = pre-migration, False = migrated


def _reset_iterations_schema_cache() -> None:
    """Test helper — resets the process-wide schema state."""
    global _iterations_schema_legacy
    _iterations_schema_legacy = None


def probe_iterations_schema() -> str:
    """Check the genie_opt_iterations Delta table schema at app startup.

    Returns one of: "ok", "legacy", "unconfigured", "unreachable". Emits an
    ERROR log in the "legacy" case so oncall sees the schema-drift warning
    when the app boots on an un-migrated workspace (Bug #2 regression).
    Designed to be called once from FastAPI startup — all errors are
    swallowed so a probe failure never blocks boot.
    """
    global _iterations_schema_legacy
    if not _is_configured():
        return "unconfigured"
    table = _delta_table("genie_opt_iterations")
    try:
        _delta_query(
            f"SELECT evaluated_count, excluded_count, quarantined_benchmarks_json "
            f"FROM {table} LIMIT 0",
            strict=True,
        )
    except Exception as exc:
        if _looks_like_legacy_schema_error(exc):
            logger.error(
                "gso.runs.schema_drift_startup %s is missing Bug #2 denominator "
                "columns. The UI will fall back to stored overall_accuracy but "
                "accuracy may appear stale until the GSO job bundle redeploys "
                "and _migrate_add_columns adds evaluated_count / excluded_count "
                "/ quarantined_benchmarks_json. err=%s",
                table,
                str(exc)[:200],
            )
            _iterations_schema_legacy = True
            return "legacy"
        logger.warning("Schema probe failed: %s", str(exc)[:200])
        return "unreachable"
    _iterations_schema_legacy = False
    logger.info("gso.runs.schema_ok %s has all Bug #2 denominator columns", table)
    return "ok"


_LEGACY_COL_ERROR_MARKERS = (
    "UNRESOLVED_COLUMN",
    "cannot resolve",
    "evaluated_count",
    "excluded_count",
    "quarantined_benchmarks_json",
)


def _looks_like_legacy_schema_error(exc: BaseException) -> bool:
    msg = str(exc)
    # Cheap: if the error mentions any of our new columns by name, or uses
    # Databricks' canonical "UNRESOLVED_COLUMN" error code, we treat it as
    # schema drift and retry with the legacy SELECT.
    return any(marker in msg for marker in _LEGACY_COL_ERROR_MARKERS)


def _select_iterations_delta(run_id: str) -> list[dict]:
    """Load iteration rows from Delta, tolerating the pre-migration schema.

    Tries `_ITER_COLS_V2` first. If the query raises what looks like a
    missing-column error (Databricks' `UNRESOLVED_COLUMN` or the column name
    echoed verbatim), retries with `_ITER_COLS_LEGACY` and flips the module
    flag so subsequent reads skip the first probe until the process restarts.
    `_derived_accuracy` handles the legacy shape transparently (falls back to
    stored `overall_accuracy` when `evaluated_count` is absent).
    """
    global _iterations_schema_legacy
    table = _delta_table("genie_opt_iterations")
    order = f"WHERE run_id = '{run_id}' ORDER BY iteration ASC"

    if _iterations_schema_legacy is True:
        return _delta_query(f"SELECT {_ITER_COLS_LEGACY} FROM {table} {order}")

    try:
        rows = _delta_query(f"SELECT {_ITER_COLS_V2} FROM {table} {order}", strict=True)
        _iterations_schema_legacy = False
        return rows
    except Exception as exc:
        if not _looks_like_legacy_schema_error(exc):
            logger.warning("Delta iterations query failed: %s", exc, exc_info=True)
            return []
        logger.warning(
            "gso.runs.schema_drift genie_opt_iterations is missing Bug #2 columns "
            "(evaluated_count / excluded_count / quarantined_benchmarks_json). "
            "Falling back to the legacy SELECT — scores render from stored "
            "overall_accuracy. Redeploy the GSO job bundle so "
            "_migrate_add_columns can ALTER TABLE ADD COLUMN. err=%s",
            str(exc)[:200],
        )
        _iterations_schema_legacy = True
        return _delta_query(f"SELECT {_ITER_COLS_LEGACY} FROM {table} {order}")


def _build_gso_config() -> IntegrationConfig:
    return IntegrationConfig(
        catalog=os.environ.get("GSO_CATALOG", ""),
        schema_name=os.environ.get("GSO_SCHEMA", "genie_space_optimizer"),
        warehouse_id=os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", ""),
        job_id=int(os.environ["GSO_JOB_ID"]) if os.environ.get("GSO_JOB_ID", "").isdigit() else None,
    )


# Type coercion helpers — imported from genie_space_optimizer.backend.utils
# Aliases preserve call-site compatibility with the underscore-prefixed names
# that were used throughout this file before the import was added.
_safe_int = safe_int
_safe_float = safe_float
_finite = safe_finite
_safe_json_parse = safe_json_parse


def _derived_accuracy(
    iter_row: dict | None,
    *,
    run_id: str | None = None,
    iteration: int | None = None,
) -> float | None:
    """Bug #2 — canonical per-iteration accuracy percentage.

    Prefers `correct_count / evaluated_count * 100` (the same math the frontend
    uses for tab labels via `lib/eval-counts.ts`) so KPI cards and tab labels
    agree to the decimal. Falls back to the stored `overall_accuracy` only
    when the count columns are absent (legacy rows written before the
    `evaluated_count` / `excluded_count` migration).

    When both derived and stored exist and differ by more than 0.5pp, emit an
    INFO-level drift log so oncall can spot stale `overall_accuracy` rows
    without page noise. Derived wins — stored is effectively a back-pointer.
    """
    if not iter_row:
        return None

    total = _safe_int(iter_row.get("total_questions")) or 0
    correct = _safe_int(iter_row.get("correct_count")) or 0
    excluded = _safe_int(iter_row.get("excluded_count")) or 0
    evaluated_raw = iter_row.get("evaluated_count")
    evaluated = _safe_int(evaluated_raw) if evaluated_raw is not None else None
    if evaluated is None:
        derived_denom = total - excluded
        evaluated = derived_denom if derived_denom >= 0 else total

    stored = _safe_float(iter_row.get("overall_accuracy"))

    if evaluated > 0 and evaluated_raw is not None:
        # Only trust the derived value when we actually have evaluated_count
        # from the row — otherwise we're just dividing by total_questions
        # which IS the original Bug #2 regression.
        derived = round(100.0 * correct / evaluated, 2)
        if stored is not None and abs(derived - stored) > 0.5:
            logger.info(
                "gso.runs.accuracy_drift run_id=%s iteration=%s "
                "stored_overall_accuracy=%.2f derived=%.2f correct=%d evaluated=%d "
                "(Bug #2 drift — reading derived; row may need backfill)",
                run_id, iteration, stored, derived, correct, evaluated,
            )
        return derived

    return stored


def _parse_detail(stage: dict) -> dict:
    """Parse detail_json column from a stage row into a dict."""
    raw = stage.get("detail_json")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


# ---------------------------------------------------------------------------
# Step summary & IO builders (ported from GSO routes/runs.py)
# ---------------------------------------------------------------------------


def _collect_all_preflight_detail(stages_rows: list[dict]) -> dict:
    """Merge detail_json from ALL PREFLIGHT stages."""
    merged: dict[str, Any] = {}
    for s in stages_rows:
        if str(s.get("stage", "")).startswith("PREFLIGHT"):
            merged.update(_parse_detail(s))
    return merged


def _resolve_parsed_space(config_snapshot: dict) -> dict:
    """Extract the parsed space config for table/column counts."""
    if not isinstance(config_snapshot, dict) or not config_snapshot:
        return {}
    parsed = config_snapshot.get("_parsed_space")
    if isinstance(parsed, dict) and parsed:
        return parsed
    ss = config_snapshot.get("serialized_space")
    if isinstance(ss, str):
        try:
            ss = json.loads(ss)
        except (json.JSONDecodeError, TypeError):
            ss = None
    if isinstance(ss, dict) and ss:
        return ss
    if "data_sources" in config_snapshot:
        return config_snapshot
    return {}


def _extract_proactive_changes(matching: list[dict]) -> dict:
    """Scan matched stages for proactive enrichment results."""
    proactive: dict = {}
    for s in matching:
        stage_name = str(s.get("stage", ""))
        d = _parse_detail(s)
        if not d:
            continue
        if "DESCRIPTION_ENRICHMENT" in stage_name:
            proactive["descriptionsEnriched"] = d.get("total_enriched", 0)
            proactive["tablesEnriched"] = d.get("tables_enriched", 0)
        elif "JOIN_DISCOVERY" in stage_name:
            proactive["joinSpecsDiscovered"] = d.get("total_applied", 0)
        elif "SPACE_METADATA" in stage_name:
            proactive["spaceDescriptionGenerated"] = d.get("description_generated", False)
            proactive["sampleQuestionsGenerated"] = d.get("questions_count", 0)
        elif "INSTRUCTION_SEED" in stage_name:
            proactive["instructionsSeeded"] = d.get("instructions_seeded", False)
        elif "PROMPT_MATCH" in stage_name:
            proactive["promptsMatched"] = d.get("total_matched", 0)
        elif "EXAMPLE_SQL" in stage_name:
            proactive["exampleSqlsMined"] = d.get("total_mined", 0)
    return proactive


def _build_stage_timeline(matching: list[dict]) -> list[dict[str, Any]]:
    """Compact stage timeline for UI drill-down."""
    events: list[dict[str, Any]] = []
    for s in matching:
        events.append({
            "stage": s.get("stage"),
            "status": str(s.get("status", "")).lower(),
            "startedAt": _isoformat(s.get("started_at")),
            "completedAt": _isoformat(s.get("completed_at")),
            "durationSeconds": _safe_float(s.get("duration_seconds")),
            "errorMessage": s.get("error_message"),
        })
    return events


def _build_step_summary(
    defn: dict, matching: list[dict], iterations_rows: list[dict], run_data: dict,
    *, stages_rows: list[dict] | None = None,
) -> str | None:
    """Build human-readable summary for a pipeline step."""
    if not matching:
        return None
    step_name = defn["name"]
    detail: dict = {}
    for s in matching:
        detail.update(_parse_detail(s))

    if step_name == "Preflight":
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        tables_val = _safe_int(detail.get("table_count")) or _safe_int(all_pf.get("table_count")) or _safe_int(all_pf.get("table_ref_count"))
        columns_val = _safe_int(detail.get("columns_collected")) or _safe_int(detail.get("columnsCollected")) or _safe_int(all_pf.get("columns_collected"))
        instr_val = _safe_int(detail.get("instruction_count")) or _safe_int(all_pf.get("instruction_count"))
        bench_val = _safe_int(detail.get("benchmark_count")) or _safe_int(all_pf.get("benchmark_count")) or _safe_int(run_data.get("benchmark_count"))
        return f"Analyzed {tables_val or '?'} tables, {columns_val or '?'} columns, {instr_val or '?'} instructions, {bench_val or '?'} sample questions"
    if step_name == "Baseline Evaluation":
        baseline_iter = next((r for r in iterations_rows if _safe_int(r.get("iteration")) == 0), None)
        score = f"{_finite(baseline_iter.get('overall_accuracy', 0)):.1f}" if baseline_iter else "?"
        total = (_safe_int(baseline_iter.get("total_questions")) or 0) if baseline_iter else 0
        correct = (_safe_int(baseline_iter.get("correct_count")) or 0) if baseline_iter else 0
        # overall_accuracy = correct / (total - excluded) * 100, so effective denominator = correct / (accuracy/100)
        accuracy_val = _finite(baseline_iter.get("overall_accuracy", 0)) if baseline_iter else 0
        effective_denom = round(correct / (accuracy_val / 100)) if accuracy_val > 0 and correct > 0 else correct
        excluded = total - effective_denom if total > effective_denom else 0
        correct_str = str(correct) if correct else "?"
        denom_str = str(effective_denom) if effective_denom else str(total or "?")
        excluded_note = f" ({excluded} excluded)" if excluded > 0 else ""
        return f"Evaluated {total or '?'} benchmark questions with 9 evaluation judges. Baseline score: {score}% ({correct_str}/{denom_str} correct{excluded_note})"
    if step_name == "Proactive Enrichment":
        descriptions = _safe_int(detail.get("descriptions_enriched")) or 0
        joins = _safe_int(detail.get("joins_discovered")) or 0
        examples = _safe_int(detail.get("examples_mined")) or 0
        instructions = 1 if detail.get("instructions_seeded") else 0
        sql_expressions = _safe_int(detail.get("sql_expressions_seeded")) or 0
        total = _safe_int(detail.get("total_enrichments")) or (descriptions + joins + instructions + examples + sql_expressions)
        parts: list[str] = []
        if descriptions:
            parts.append(f"{descriptions} descriptions")
        if joins:
            parts.append(f"{joins} joins")
        if instructions:
            parts.append(f"{instructions} instructions")
        if examples:
            parts.append(f"{examples} example SQLs")
        if sql_expressions:
            parts.append(f"{sql_expressions} SQL expressions")
        breakdown = ", ".join(parts) if parts else "no changes"
        return f"Applied {total} proactive enrichments: {breakdown}"
    if step_name == "Adaptive Optimization":
        patches = detail.get("patches_applied", 0)
        levers_accepted = detail.get("levers_accepted", [])
        before = f"{_finite(run_data.get('baseline_accuracy', 0)):.1f}" if run_data.get("baseline_accuracy") else "?"
        after = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return f"Applied {patches} optimizations across {len(levers_accepted) if isinstance(levers_accepted, list) else '?'} categories. Score improved from {before}% to {after}%"
    if step_name == "Finalization":
        score = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        rep = f"{_finite(run_data.get('best_repeatability', 0)):.1f}" if run_data.get("best_repeatability") else "?"
        summary = f"Final evaluation complete. Optimized score: {score}%. Repeatability: {rep}%"
        ho_acc = _safe_float(detail.get("held_out_accuracy"))
        if ho_acc is not None:
            summary += f" Held-out: {ho_acc:.1f}%"
        return summary
    if step_name == "Deploy":
        return f"Deployment {detail.get('status', 'pending')}"
    return None


def _build_step_io(
    defn: dict, matching: list[dict], iterations_rows: list[dict], run_data: dict,
    *, stages_rows: list[dict] | None = None,
) -> tuple[dict | None, dict | None]:
    """Build rich inputs/outputs for pipeline step drill-down."""
    if not matching:
        return None, None
    step_name = defn["name"]
    detail: dict[str, Any] = {}
    for s in matching:
        detail.update(_parse_detail(s))
    timeline = _build_stage_timeline(matching)

    raw_snap = run_data.get("config_snapshot")
    config_snapshot: dict = {}
    if isinstance(raw_snap, dict):
        config_snapshot = raw_snap
    elif isinstance(raw_snap, str):
        parsed = _safe_json_parse(raw_snap)
        config_snapshot = parsed if isinstance(parsed, dict) else {}

    if step_name == "Preflight":
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        parsed_space = _resolve_parsed_space(config_snapshot)
        ds = parsed_space.get("data_sources", {}) if isinstance(parsed_space, dict) else {}
        tables = ds.get("tables", []) if isinstance(ds, dict) else []
        functions = ds.get("functions", []) if isinstance(ds, dict) else []
        instr_node = parsed_space.get("instructions", {}) if isinstance(parsed_space, dict) else {}
        text_instructions = instr_node.get("text_instructions", []) if isinstance(instr_node, dict) else []
        examples = instr_node.get("example_question_sqls", []) if isinstance(instr_node, dict) else []
        sample_questions: list[str] = []
        for ex in examples:
            q = str(ex.get("question") or "").strip() if isinstance(ex, dict) else ""
            if q:
                sample_questions.append(q)
        table_count = _safe_int(detail.get("table_count")) or _safe_int(all_pf.get("table_count")) or _safe_int(all_pf.get("table_ref_count")) or len(tables)
        function_count = _safe_int(detail.get("function_count")) or _safe_int(all_pf.get("function_count")) or len(functions)
        instruction_count = _safe_int(detail.get("instruction_count")) or _safe_int(all_pf.get("instruction_count")) or len(text_instructions)
        sample_q_count = _safe_int(detail.get("benchmark_count")) or _safe_int(all_pf.get("benchmark_count")) or _safe_int(detail.get("sample_question_count")) or len(sample_questions)
        prefetched = config_snapshot.get("_prefetched_uc_metadata", {}) if isinstance(config_snapshot, dict) else {}
        uc_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
        uc_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
        column_samples: list[str] = []
        for col in (uc_columns[:12] if isinstance(uc_columns, list) else []):
            if not isinstance(col, dict):
                continue
            t_name = str(col.get("table_name") or col.get("table") or "").strip()
            c_name = str(col.get("column_name") or col.get("column") or "").strip()
            if t_name and c_name:
                column_samples.append(f"{t_name}.{c_name}")
            elif c_name:
                column_samples.append(c_name)
        columns_collected = _safe_int(detail.get("columns_collected")) or _safe_int(detail.get("columnsCollected"))
        if columns_collected is None:
            columns_collected = len(uc_columns) if isinstance(uc_columns, list) else 0
        tags_collected = _safe_int(detail.get("tags_collected")) or _safe_int(detail.get("tagsCollected"))
        if tags_collected is None:
            tags_collected = len(uc_tags) if isinstance(uc_tags, list) else 0
        return (
            {"spaceId": run_data.get("space_id"), "domain": run_data.get("domain"), "catalog": run_data.get("catalog"), "schema": run_data.get("uc_schema")},
            {"tableCount": table_count, "functionCount": function_count, "instructionCount": instruction_count, "sampleQuestionCount": sample_q_count, "columnsCollected": columns_collected, "tagsCollected": tags_collected, "columnSamples": column_samples, "stageEvents": timeline},
        )

    if step_name == "Baseline Evaluation":
        baseline_iter = next((r for r in iterations_rows if _safe_int(r.get("iteration")) == 0 and str(r.get("eval_scope", "")).lower() == "full"), None)
        if not baseline_iter:
            return None, {"stageEvents": timeline}
        scores = baseline_iter.get("scores_json", {})
        if isinstance(scores, str):
            try:
                scores = json.loads(scores)
            except (json.JSONDecodeError, TypeError):
                scores = {}
        if not isinstance(scores, dict):
            scores = {}
        rows_json = baseline_iter.get("rows_json", [])
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                rows_json = []
        if not isinstance(rows_json, list):
            rows_json = []
        sample_rows: list[dict[str, Any]] = []
        for row in rows_json[:5]:
            if not isinstance(row, dict):
                continue
            question = ""
            if isinstance(row.get("inputs"), dict):
                question = str(row.get("inputs", {}).get("question") or "").strip()
            if not question:
                question = str(row.get("inputs/question") or "").strip()
            sample_rows.append({
                "question": question,
                "resultCorrectness": row.get("result_correctness/value", row.get("result_correctness")),
                "syntaxValidity": row.get("syntax_validity/value", row.get("syntax_validity")),
                "assetRouting": row.get("asset_routing/value", row.get("asset_routing")),
                "matchType": row.get("outputs", {}).get("comparison", {}).get("match_type") if isinstance(row.get("outputs"), dict) else None,
                "error": row.get("outputs", {}).get("comparison", {}).get("error") if isinstance(row.get("outputs"), dict) else None,
            })
        return (
            {"benchmarkCount": baseline_iter.get("total_questions"), "iteration": 0},
            {"judgeScores": {k: _safe_float(v) for k, v in scores.items()}, "totalQuestions": baseline_iter.get("total_questions"), "correctCount": baseline_iter.get("correct_count"), "failedCount": int(_finite(baseline_iter.get("total_questions", 0)) - _finite(baseline_iter.get("correct_count", 0))), "mlflowRunId": baseline_iter.get("mlflow_run_id"), "invalidBenchmarkCount": _safe_int(detail.get("invalid_benchmark_count")), "permissionBlockedCount": _safe_int(detail.get("permission_blocked_count")), "unresolvedColumnCount": _safe_int(detail.get("unresolved_column_count")), "harnessRetryCount": _safe_int(detail.get("harness_retry_count")), "sampleQuestions": sample_rows, "stageEvents": timeline},
        )

    if step_name == "Proactive Enrichment":
        proactive = _extract_proactive_changes(matching)
        return (
            {"spaceId": run_data.get("space_id")},
            {"proactiveChanges": proactive if proactive else None, "enrichmentModelId": detail.get("enrichment_model_id"), "totalEnrichments": detail.get("total_enrichments", 0), "enrichmentSkipped": detail.get("enrichment_skipped", False), "stageEvents": timeline},
        )

    if step_name == "Adaptive Optimization":
        patches_applied = detail.get("patches_applied") or detail.get("patches_count")
        iteration_counter = detail.get("iteration_counter") or run_data.get("best_iteration")
        return (
            {"leverCountConfigured": len(run_data.get("levers", [])) if isinstance(run_data.get("levers"), list) else None, "maxIterations": run_data.get("max_iterations")},
            {"patchesApplied": patches_applied, "leversAccepted": detail.get("levers_accepted", []), "leversRolledBack": detail.get("levers_rolled_back", []), "iterationCounter": iteration_counter, "baselineAccuracy": run_data.get("baseline_accuracy"), "bestAccuracy": _safe_float(run_data.get("best_accuracy")), "stageEvents": timeline},
        )

    if step_name == "Finalization":
        return (
            {"bestIteration": run_data.get("best_iteration")},
            {"bestAccuracy": _safe_float(run_data.get("best_accuracy")), "repeatability": _safe_float(run_data.get("best_repeatability")), "convergenceReason": run_data.get("convergence_reason"), "ucModelName": detail.get("uc_model_name") or None, "ucModelVersion": detail.get("uc_model_version") or None, "ucChampionPromoted": detail.get("uc_champion_promoted", False), "heldOutAccuracy": _safe_float(detail.get("held_out_accuracy")), "heldOutCount": _safe_int(detail.get("held_out_count")), "trainAccuracy": _safe_float(detail.get("train_accuracy")), "heldOutDeltaPp": _safe_float(detail.get("delta_pp")), "stageEvents": timeline},
        )

    if step_name == "Deploy":
        return (
            {"deployTarget": run_data.get("deploy_target")},
            {"deployStatus": detail.get("status"), "stageEvents": timeline},
        )

    return None, {"stageEvents": timeline}


# ---------------------------------------------------------------------------
# Lever builders (ported from GSO routes/runs.py)
# ---------------------------------------------------------------------------


def _iteration_scores(iter_row: dict | None) -> dict[str, float | None]:
    """Parse per-judge scores from scores_json."""
    if not iter_row:
        return {}
    scores = iter_row.get("scores_json", {})
    if isinstance(scores, str):
        try:
            scores = json.loads(scores)
        except (json.JSONDecodeError, TypeError):
            scores = {}
    if not isinstance(scores, dict):
        return {}
    return {str(k): _safe_float(v) for k, v in scores.items()}


def _patch_for_ui(row: dict) -> dict[str, Any]:
    """Convert patch table row to compact UI object."""
    return {
        "patchType": row.get("patch_type"),
        "scope": row.get("scope"),
        "riskLevel": row.get("risk_level"),
        "targetObject": row.get("target_object"),
        "rolledBack": bool(row.get("rolled_back")) if row.get("rolled_back") is not None else False,
        "rollbackReason": row.get("rollback_reason"),
        "command": _safe_json_parse(row.get("command_json")),
        "patch": _safe_json_parse(row.get("patch_json")),
        "appliedAt": str(row.get("applied_at")) if row.get("applied_at") is not None else None,
    }


def _derive_lever_status(stages: list[dict]) -> str:
    """Derive lever status from its stages."""
    statuses = {str(s.get("status", "")).upper() for s in stages}
    if "ROLLED_BACK" in statuses:
        return "rolled_back"
    if "FAILED" in statuses:
        return "failed"
    if "SKIPPED" in statuses:
        return "skipped"
    if "COMPLETE" in statuses:
        return "accepted"
    if "STARTED" in statuses:
        has_eval = any("EVAL" in str(s.get("stage", "")) for s in stages)
        return "evaluating" if has_eval else "running"
    return "pending"


def _normalize_lever_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale active lever states after the run is terminal."""
    if status not in {"running", "evaluating"}:
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _build_lever_iterations(
    *, lever_num: int, lever_stages: list[dict], iterations_rows: list[dict],
    patches_rows: list[dict], run_status: str, all_stages_rows: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Build iteration-by-iteration transparency payload for one lever."""
    by_iter: dict[int, dict[str, Any]] = {}
    lever_iterations: set[int] = set()
    for row in iterations_rows:
        if _safe_int(row.get("lever")) == lever_num:
            it = _safe_int(row.get("iteration"))
            if it is not None:
                lever_iterations.add(it)
    _non_zero_lever_iters: set[int] = set()
    if lever_num == 0:
        for row in iterations_rows:
            lv, it = _safe_int(row.get("lever")), _safe_int(row.get("iteration"))
            if lv is not None and lv != 0 and it is not None:
                _non_zero_lever_iters.add(it)
    for p in patches_rows:
        if _safe_int(p.get("lever")) == lever_num:
            it = _safe_int(p.get("iteration"))
            if it is not None and it not in _non_zero_lever_iters:
                lever_iterations.add(it)
    for stage in lever_stages:
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        entry["detail"].update(_parse_detail(stage))
    for stage in all_stages_rows or []:
        if not str(stage.get("stage", "")).startswith("AG_"):
            continue
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None or iteration not in lever_iterations:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        d = _parse_detail(stage)
        if d:
            entry["detail"].update(d)
    for row in iterations_rows:
        iteration = _safe_int(row.get("iteration"))
        if iteration is None:
            continue
        if _safe_int(row.get("lever")) != lever_num and iteration not in lever_iterations:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["rows"].append(row)
    for patch_row in patches_rows:
        if _safe_int(patch_row.get("lever")) != lever_num:
            continue
        iteration = _safe_int(patch_row.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["patches"].append(_patch_for_ui(patch_row))

    payloads: list[dict[str, Any]] = []
    for iteration in sorted(by_iter.keys()):
        entry = by_iter[iteration]
        status = _normalize_lever_status_for_terminal_run(status=_derive_lever_status(entry["stages"]), run_status=run_status)
        d = entry["detail"]
        full_row = next((r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "full"), None)
        score_after = _safe_float(full_row.get("overall_accuracy")) if full_row else _safe_float(d.get("accuracy"))
        score_before = _safe_float(d.get("score_before"))
        score_delta = _safe_float(d.get("score_delta"))
        if score_delta is None and score_before is not None and score_after is not None:
            score_delta = round(score_after - score_before, 2)
        rollback_reason = d.get("reason")
        if not rollback_reason and status == "rolled_back":
            rollback_reason = "regression"
        payloads.append({
            "iteration": iteration, "status": status, "patchCount": len(entry["patches"]),
            "patchTypes": [str(p.get("patchType") or "") for p in entry["patches"] if p.get("patchType")],
            "scoreBefore": score_before, "scoreAfter": score_after, "scoreDelta": score_delta,
            "judgeScores": _iteration_scores(full_row),
            "mlflowRunId": full_row.get("mlflow_run_id") if full_row else None,
            "rollbackReason": rollback_reason, "patches": entry["patches"],
        })
    return payloads


def _build_levers(
    stages_rows: list[dict], *, run_status: str = "",
    configured_levers: list[int] | None = None,
    patches_rows: list[dict] | None = None, iterations_rows: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Build lever detail from LEVER_* and AG_* stage rows."""
    lever_data: dict[int, dict] = {}
    for configured in configured_levers or []:
        try:
            lever_data[int(configured)] = {"stages": [], "detail": {}, "patches": []}
        except (TypeError, ValueError):
            continue
    iter_to_levers: dict[int, set[int]] = {}
    for p in patches_rows or []:
        it, lv = _safe_int(p.get("iteration")), _safe_int(p.get("lever"))
        if it is not None and lv is not None:
            iter_to_levers.setdefault(it, set()).add(lv)
    for row in iterations_rows or []:
        it, lv = _safe_int(row.get("iteration")), _safe_int(row.get("lever"))
        if it is not None and lv is not None and lv != 0:
            iter_to_levers.setdefault(it, set()).add(lv)
    for s in stages_rows:
        stage_name = str(s.get("stage", ""))
        if stage_name.startswith("LEVER_"):
            lever_num = s.get("lever")
            if lever_num is None:
                try:
                    lever_num = int(stage_name.split("_")[1])
                except (IndexError, ValueError):
                    continue
            try:
                lever_num = int(float(lever_num))
            except (TypeError, ValueError):
                continue
            if lever_num not in lever_data:
                lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
            lever_data[lever_num]["stages"].append(s)
            lever_data[lever_num]["detail"].update(_parse_detail(s))
            continue
        if stage_name.startswith("AG_"):
            iteration = _safe_int(s.get("iteration"))
            if iteration is None:
                continue
            ag_detail = _parse_detail(s)
            target_levers: set[int] = set(iter_to_levers.get(iteration, set()))
            if ag_detail and "levers" in ag_detail:
                for lk in ag_detail["levers"]:
                    try:
                        target_levers.add(int(lk))
                    except (TypeError, ValueError):
                        pass
            for lever_num in target_levers:
                if lever_num not in lever_data:
                    lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
                lever_data[lever_num]["stages"].append(s)
                if ag_detail:
                    lever_data[lever_num]["detail"].update(ag_detail)
    for p in patches_rows or []:
        lever_num = _safe_int(p.get("lever"))
        if lever_num is None:
            continue
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
        lever_data[lever_num]["patches"].append(_patch_for_ui(p))

    # For lever 0 (Proactive Enrichment), match enrichment stages since they
    # don't use LEVER_0_* naming — they use ENRICHMENT, DESCRIPTION_ENRICHMENT, etc.
    _ENRICHMENT_PREFIXES = ("ENRICHMENT", "DESCRIPTION_ENRICHMENT", "JOIN_DISCOVERY",
                            "SPACE_METADATA", "INSTRUCTION_SEED", "PROACTIVE_INSTRUCTION",
                            "EXAMPLE_SQL", "PROMPT_MATCH")
    if 0 in lever_data:
        for s in stages_rows:
            stage_name = str(s.get("stage", ""))
            if any(stage_name.startswith(pfx) for pfx in _ENRICHMENT_PREFIXES):
                lever_data[0]["stages"].append(s)
                lever_data[0]["detail"].update(_parse_detail(s))

    levers: list[dict[str, Any]] = []
    for lever_num in sorted(lever_data.keys()):
        data = lever_data[lever_num]
        ld = data["detail"]
        status = _normalize_lever_status_for_terminal_run(status=_derive_lever_status(data["stages"]), run_status=run_status)
        lever_patches = data.get("patches", [])
        rollback_reason = ld.get("reason", "regression") if status == "rolled_back" else None
        lever_iterations = _build_lever_iterations(
            lever_num=lever_num, lever_stages=data.get("stages", []),
            iterations_rows=iterations_rows or [], patches_rows=patches_rows or [],
            run_status=run_status, all_stages_rows=stages_rows,
        )
        # patchCount: prefer actual patches array length, fall back to stage detail
        patches_total = len(lever_patches)
        if patches_total == 0:
            # Also count patches aggregated across iterations
            iter_patch_total = sum(it.get("patchCount", 0) for it in lever_iterations)
            if iter_patch_total > 0:
                patches_total = iter_patch_total
            else:
                patches_total = _safe_int(ld.get("patches_applied")) or 0
        levers.append({
            "lever": lever_num, "name": LEVER_NAMES.get(lever_num, f"Lever {lever_num}"),
            "status": status, "patchCount": patches_total,
            "scoreBefore": _safe_float(ld.get("score_before")), "scoreAfter": _safe_float(ld.get("accuracy")),
            "scoreDelta": _safe_float(ld.get("score_delta")), "rollbackReason": rollback_reason,
            "patches": lever_patches, "iterations": lever_iterations,
        })
    return levers


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/health")
async def health():
    """Check if GSO is configured and operational for this deployment."""
    if not _is_configured():
        return {"configured": False, "issues": []}

    issues: list[str] = []
    config = _build_gso_config()

    # Validate job exists and SP can access it
    if config.job_id:
        try:
            sp_ws = get_service_principal_client()
            sp_ws.jobs.get(config.job_id)
        except Exception as exc:
            issues.append(f"Job {config.job_id} not accessible: {exc}")
    else:
        issues.append("GSO_JOB_ID not set")

    # Validate warehouse is configured
    if not config.warehouse_id:
        issues.append("No SQL warehouse configured (GSO_WAREHOUSE_ID or SQL_WAREHOUSE_ID)")

    return {"configured": True, "issues": issues}


@router.get("/permissions/{space_id}")
async def check_permissions(
    space_id: SpaceId,
    refresh: bool = Query(False, description="Bypass the Prompt Registry probe cache."),
):
    """Pre-check SP permissions for a Genie Space before optimization.

    ``?refresh=true`` bypasses the in-process TTL cache of the Prompt
    Registry probe. The UI's Re-check button should pass it; regular
    page loads should not.
    """
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured.")

    sp_ws = get_service_principal_client()
    errors: list[str] = []

    # Resolve SP identity — config.client_id is the UUID that UC SQL accepts
    sp_display_name = ""
    sp_application_id = sp_ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    try:
        me = sp_ws.current_user.me()
        sp_display_name = me.display_name or me.user_name or ""
        if not sp_application_id:
            sp_application_id = getattr(me, "application_id", "") or ""
    except Exception as exc:
        errors.append(f"Could not resolve SP identity: {exc}")
        logger.warning("Could not resolve SP identity", exc_info=True)

    # Validate SP UUID format
    if sp_application_id and not re.match(r'^[a-f0-9-]{36}$', sp_application_id):
        errors.append(
            f"SP identifier '{sp_application_id}' is not a UUID. "
            "Grant SQL may not work. Set DATABRICKS_CLIENT_ID to the SP's application_id."
        )
        logger.warning("SP application_id %r doesn't look like a UUID", sp_application_id)

    # Check SP CAN_MANAGE on the space
    sp_has_manage = False
    try:
        from genie_space_optimizer.common.sp_permissions import get_sp_principal_aliases
        from genie_space_optimizer.common.genie_client import sp_can_manage_space

        sp_aliases = get_sp_principal_aliases(sp_ws)
        # Try ACL fetch with user's OBO client (has access-management scope);
        # fall back to serialized-space fetch via SP if ACL read fails.
        obo_ws = get_workspace_client()
        sp_has_manage = sp_can_manage_space(obo_ws, space_id, sp_aliases, sp_client=sp_ws)
    except Exception as exc:
        errors.append(f"Could not check Genie Space access: {exc}")
        logger.warning("Could not check SP space access for %s", space_id, exc_info=True)

    # Extract table refs from space config and probe data access
    schemas: list[SchemaAccessStatus] = []
    try:
        from genie_space_optimizer.common.genie_client import fetch_space_config
        from genie_space_optimizer.common.uc_metadata import (
            extract_genie_space_table_refs,
            get_unique_schemas,
        )
        from genie_space_optimizer.common.sp_permissions import probe_sp_required_access

        ws = get_workspace_client()
        try:
            config = fetch_space_config(ws, space_id)
        except Exception:
            config = fetch_space_config(sp_ws, space_id)
        refs = extract_genie_space_table_refs(config)
        unique_schemas = set(get_unique_schemas(refs))

        if unique_schemas:
            read_granted, _write_granted = probe_sp_required_access(sp_ws, unique_schemas)
            # UC SQL requires the application_id (UUID), not the display name
            sp_name_for_grant = sp_application_id or sp_display_name or "<service-principal>"

            for cat, sch in sorted(unique_schemas):
                granted = (cat, sch) in read_granted
                grant_sql = None
                if not granted:
                    grant_sql = (
                        f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT SELECT ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;\n"
                        f"GRANT EXECUTE ON SCHEMA `{cat}`.`{sch}` TO `{sp_name_for_grant}`;"
                    )
                schemas.append(SchemaAccessStatus(
                    catalog=cat,
                    schema_name=sch,
                    read_granted=granted,
                    grant_sql=grant_sql,
                ))
    except Exception as exc:
        errors.append(f"Could not probe data access: {exc}")
        logger.warning("Could not probe data access for space %s", space_id, exc_info=True)

    # Probe MLflow Prompt Registry availability (fail-closed; structured codes).
    # Scope the probe to the GSO target schema so permission errors bind to
    # the exact catalog.schema the job will write to — probe-workload parity.
    from backend.services.prompt_registry import check_prompt_registry

    gso_config = _build_gso_config()
    gso_uc_schema = (
        f"{gso_config.catalog}.{gso_config.schema_name}"
        if gso_config.catalog and gso_config.schema_name
        else None
    )

    probe = check_prompt_registry(
        sp_ws,
        mode="read",
        uc_schema=gso_uc_schema,
        bypass_cache=refresh,
    )
    prompt_registry_available = probe.available
    prompt_registry_error = None if probe.available else probe.user_message
    prompt_registry_reason_code = probe.reason_code
    prompt_registry_error_code = probe.vendor_error_code
    prompt_registry_actionable_by = probe.actionable_by
    if not probe.available:
        errors.append(probe.user_message)

    all_read = all(s.read_granted for s in schemas) if schemas else True
    can_start = sp_has_manage and all_read and prompt_registry_available

    return PermissionCheckResponse(
        sp_display_name=sp_display_name,
        sp_application_id=sp_application_id,
        sp_has_manage=sp_has_manage,
        schemas=schemas,
        prompt_registry_available=prompt_registry_available,
        prompt_registry_error=prompt_registry_error,
        prompt_registry_reason_code=prompt_registry_reason_code,
        prompt_registry_error_code=prompt_registry_error_code,
        prompt_registry_actionable_by=prompt_registry_actionable_by,
        can_start=can_start,
        errors=errors,
    )


@router.post("/trigger")
async def trigger(body: TriggerRequest, request: Request):
    """Trigger an optimization run for a Genie Space."""
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured. Set GSO_CATALOG and GSO_JOB_ID.")

    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()

    # Server-side gate: re-verify Prompt Registry is available under the same
    # identity (sp_ws) the job will use. The UI also checks via /permissions,
    # but that is advisory — clients can skip it. This closes the bypass.
    # Always bypass the TTL cache here: /trigger is low-frequency and must
    # decide on a fresh probe.
    from backend.services.prompt_registry import (
        ACTIONABLE_BY_PLATFORM,
        check_prompt_registry,
    )

    trigger_uc_schema = (
        f"{config.catalog}.{config.schema_name}"
        if config.catalog and config.schema_name
        else None
    )
    probe = check_prompt_registry(
        sp_ws,
        mode="read",
        uc_schema=trigger_uc_schema,
        bypass_cache=True,
    )
    if not probe.available:
        logger.warning(
            "Trigger blocked: Prompt Registry unavailable (code=%s actionable_by=%s raw=%s)",
            probe.reason_code,
            probe.actionable_by,
            (probe.raw_error or "")[:200],
        )
        # Two different HTTP semantics so the UI and on-call routing can
        # distinguish "admin fix" from "our outage":
        #   412 Precondition Failed — customer must grant/enable something.
        #   503 Service Unavailable — Databricks/our platform is broken;
        #     the customer cannot fix it from the workspace.
        status_code = 503 if probe.actionable_by == ACTIONABLE_BY_PLATFORM else 412
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": probe.user_message,
                "reason_code": probe.reason_code,
                "error_code": probe.vendor_error_code,
                "actionable_by": probe.actionable_by,
                "prompt_registry_available": False,
            },
        )

    try:
        result = trigger_optimization(
            space_id=body.space_id,
            ws=ws,
            sp_ws=sp_ws,
            config=config,
            user_email=request.headers.get("x-forwarded-email"),
            user_name=request.headers.get("x-forwarded-preferred-username"),
            apply_mode=body.apply_mode,
            levers=body.levers,
            deploy_target=body.deploy_target,
        )
        return {
            "runId": result.run_id,
            "jobRunId": result.job_run_id,
            "jobUrl": result.job_url,
            "status": result.status,
        }
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        msg = str(e)
        if "already in progress" in msg:
            raise HTTPException(status_code=409, detail=msg)
        logger.exception("Trigger optimization failed: %s", e)
        raise HTTPException(status_code=500, detail=msg)
    except Exception as e:
        logger.exception("Failed to trigger optimization: %s", e)
        raise HTTPException(status_code=500, detail="Failed to start optimization job.")


# ---------------------------------------------------------------------------
# Pipeline step definitions — group raw sub-stages into 6 logical steps
# (Ported from Genie Space Optimizer's map_stages_to_steps)
# ---------------------------------------------------------------------------

_STEP_DEFINITIONS = [
    {"stepNumber": 1, "name": "Preflight",             "stage_prefixes": ["PREFLIGHT"]},
    {"stepNumber": 2, "name": "Baseline Evaluation",   "stage_prefixes": ["BASELINE_EVAL"]},
    {"stepNumber": 3, "name": "Proactive Enrichment",  "stage_prefixes": ["ENRICHMENT", "PROMPT_MATCH", "DESCRIPTION_ENRICHMENT", "JOIN_DISCOVERY", "SPACE_METADATA", "INSTRUCTION_SEED", "PROACTIVE_INSTRUCTION", "EXAMPLE_SQL"]},
    {"stepNumber": 4, "name": "Adaptive Optimization", "stage_prefixes": ["LEVER_", "AG_"]},
    {"stepNumber": 5, "name": "Finalization",          "stage_prefixes": ["FINALIZE", "REPEATABILITY", "HELD_OUT", "COMPLETE"]},
    {"stepNumber": 6, "name": "Deploy",                "stage_prefixes": ["DEPLOY", "UC_OBO_WRITE"]},
]


def _derive_step_status(matching_stages: list[dict]) -> str:
    """Derive a single step status from its matching raw stages."""
    if not matching_stages:
        return "pending"
    latest = matching_stages[-1]
    status = str(latest.get("status", "")).upper()
    if status == "FAILED":
        return "failed"
    if status in {"COMPLETE", "SKIPPED", "ROLLED_BACK"}:
        return "completed"
    if status == "STARTED":
        return "running"
    return "pending"


def _total_duration(matching_stages: list[dict]) -> float | None:
    """Sum durations of all matching stages; None if no positive total."""
    total = 0.0
    for s in matching_stages:
        val = s.get("duration_seconds")
        if val is not None:
            try:
                total += float(val)
            except (TypeError, ValueError):
                pass
    return total if total > 0 else None


def _last_summary(matching_stages: list[dict]) -> str | None:
    """Return the last non-empty summary from matching stages."""
    for s in reversed(matching_stages):
        if s.get("summary"):
            return s["summary"]
    return None


def _normalize_step_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Normalize step status when the overall run is already terminal."""
    normalized = run_status.upper()
    if status == "running":
        if normalized == "FAILED":
            return "failed"
        if normalized in {"CANCELLED", "DISCARDED"}:
            return "pending"
        if normalized in _TERMINAL_RUN_STATUSES:
            return "completed"
    if status == "pending" and normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _map_stages_to_steps(
    stages: list[dict], run: dict, iterations: list[dict],
) -> list[dict]:
    """Group raw stages by prefix into 6 logical pipeline steps with rich IO."""
    run_status = str(run.get("status", "")).upper()

    steps = []
    for step_def in _STEP_DEFINITIONS:
        matching = [
            s for s in stages
            if any(
                str(s.get("stage", "")).upper().startswith(prefix)
                for prefix in step_def["stage_prefixes"]
            )
        ]

        status = _derive_step_status(matching)
        status = _normalize_step_status_for_terminal_run(status=status, run_status=run_status)

        summary = _build_step_summary(step_def, matching, iterations, run, stages_rows=stages)
        inputs, outputs = _build_step_io(step_def, matching, iterations, run, stages_rows=stages)

        # Fall back to legacy summary if the new builder returned None
        if not summary:
            summary = _last_summary(matching)

        steps.append({
            "stepNumber": step_def["stepNumber"],
            "name": step_def["name"],
            "status": status,
            "durationSeconds": _total_duration(matching),
            "summary": summary,
            "inputs": inputs,
            "outputs": outputs,
        })

    return steps


@router.get("/runs/{run_id}")
async def get_run(run_id: RunId):
    """Get full run detail including stages, iterations, levers, and patches."""
    run = await gso_lakebase.load_gso_run(run_id)
    if not run and _is_configured():
        rows = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_runs')} WHERE run_id = '{run_id}'"
        )
        run = rows[0] if rows else None
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stages = await gso_lakebase.load_gso_stages(run_id)
    if not stages and _is_configured():
        stages = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_stages')} "
            f"WHERE run_id = '{run_id}' ORDER BY started_at ASC"
        )

    # Fetch iterations (lightweight — no rows_json)
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = _select_iterations_delta(run_id)

    # Fetch patches for lever detail
    patches = await gso_lakebase.load_gso_patches(run_id)
    if not patches and _is_configured():
        patches = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_patches')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration, lever, patch_index"
        )

    # Inject baseline_accuracy into run_data for step summary builders
    baseline_iter = next(
        (r for r in iterations if _safe_int(r.get("iteration")) == 0 and str(r.get("eval_scope", "")).lower() == "full"),
        None,
    )
    if not baseline_iter:
        baseline_iter = next((r for r in iterations if _safe_int(r.get("iteration")) == 0), None)
    # Bug #2: derive from correct/evaluated so the `baselineScore` we return
    # agrees with what the frontend computes for the Baseline Evaluation tab
    # label from the same iteration row. Falls back to stored overall_accuracy
    # for legacy rows without evaluated_count.
    baseline_accuracy = _derived_accuracy(baseline_iter, run_id=run_id, iteration=0)
    run["baseline_accuracy"] = baseline_accuracy

    # Build pipeline steps with rich IO
    steps = _map_stages_to_steps(stages, run, iterations)

    # Build levers with patches and iteration detail
    raw_levers = run.get("levers", [])
    if isinstance(raw_levers, str):
        try:
            raw_levers = json.loads(raw_levers)
        except (json.JSONDecodeError, TypeError):
            raw_levers = []
    if not isinstance(raw_levers, list):
        raw_levers = []
    configured_lever_ints: list[int] = []
    for lev in raw_levers:
        try:
            configured_lever_ints.append(int(lev))
        except (TypeError, ValueError):
            continue
    levers = _build_levers(
        stages, run_status=str(run.get("status", "")),
        configured_levers=configured_lever_ints,
        patches_rows=patches, iterations_rows=iterations,
    )

    # Build full stage event list (for Activity tab & Stage Timeline)
    stage_events = [
        {
            "stage": s.get("stage", ""),
            "status": s.get("status", "pending"),
            "durationSeconds": s.get("duration_seconds"),
            "startedAt": _isoformat(s.get("started_at")),
            "completedAt": _isoformat(s.get("completed_at")),
            "summary": s.get("summary"),
        }
        for s in stages
    ]

    # Find baseline and best scores from full-scope iterations only.
    # Bug #2: optimized_score is derived from correct_count/evaluated_count
    # per iteration so the optimized card in ScoreSummary agrees to the
    # decimal with what RunDetailView computes for the Final Evaluation tab.
    baseline_score = baseline_accuracy
    baseline_iteration = 0 if baseline_accuracy is not None else None
    optimized_score = None
    best_iteration = None
    for it in iterations:
        it_num = int(it.get("iteration", -1))
        if it.get("eval_scope") == "full" and it_num > 0:
            accuracy = _derived_accuracy(it, run_id=run_id, iteration=it_num)
            if accuracy is not None and (optimized_score is None or accuracy > optimized_score):
                optimized_score = accuracy
                best_iteration = it_num

    if optimized_score is None:
        for it in iterations:
            it_num = int(it.get("iteration", -1))
            if it_num > 0:
                accuracy = _derived_accuracy(it, run_id=run_id, iteration=it_num)
                if accuracy is not None and (optimized_score is None or accuracy > optimized_score):
                    optimized_score = accuracy
                    best_iteration = it_num

    # Build resource links (absolute URLs)
    config = _build_gso_config()
    host = get_databricks_host()
    space_id = run.get("space_id", "")
    links = []

    if host and space_id:
        links.append({"label": "Genie Space", "url": f"{host}/genie/rooms/{space_id}", "category": "genie"})

    # Resolve workspace_id once for ?o= parameter on deep links
    workspace_id = None
    if host:
        try:
            ws = get_workspace_client()
            workspace_id = ws.get_workspace_id()
        except Exception:
            pass

    job_run_id = run.get("job_run_id")
    job_id = run.get("job_id") or config.job_id
    if host and job_id and job_run_id:
        job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}"
        if workspace_id:
            job_url += f"?o={workspace_id}"
        links.append({"label": "Optimization Job Run", "url": job_url, "category": "job"})
    elif host and job_id:
        links.append({"label": "Optimization Job", "url": f"{host}/jobs/{job_id}", "category": "job"})

    # MLflow Experiment — deep link with experiment_id, fallback to searchFilter
    experiment_id = run.get("experiment_id")
    experiment_name = run.get("experiment_name")
    if host and experiment_id:
        mlflow_url = f"{host}/ml/experiments/{experiment_id}"
        if workspace_id:
            mlflow_url += f"?o={workspace_id}"
        links.append({"label": "MLflow Experiment", "url": mlflow_url, "category": "mlflow"})
    elif host and experiment_name:
        from urllib.parse import quote
        links.append({"label": "MLflow Experiment", "url": f"{host}/ml/experiments?searchFilter={quote(experiment_name)}", "category": "mlflow"})

    # Per-iteration MLflow eval run links
    if host and experiment_id:
        for it in iterations:
            mlflow_run_id = it.get("mlflow_run_id")
            if not mlflow_run_id:
                continue
            it_num = _safe_int(it.get("iteration"))
            if it_num is None:
                continue
            label = "Baseline Evaluation" if it_num == 0 else f"Iteration {it_num} Evaluation"
            links.append({"label": label, "url": f"{host}/ml/experiments/{experiment_id}/runs/{mlflow_run_id}", "category": "mlflow"})

    if host and config.catalog and config.schema_name:
        links.append({"label": "Runs Table", "url": f"{host}/explore/data/{config.catalog}/{config.schema_name}/genie_opt_runs", "category": "data"})
        links.append({"label": "Iterations Table", "url": f"{host}/explore/data/{config.catalog}/{config.schema_name}/genie_opt_iterations", "category": "data"})

    return {
        "runId": run.get("run_id"),
        "spaceId": run.get("space_id"),
        "spaceName": run.get("space_name", run.get("domain", "")),
        "status": run.get("status"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "initiatedBy": run.get("triggered_by") or "system",
        "baselineScore": baseline_score,
        "optimizedScore": optimized_score,
        "baselineIteration": baseline_iteration,
        "bestIteration": best_iteration,
        "steps": steps,
        "stages": stage_events,
        "levers": levers,
        "links": links,
        "convergenceReason": run.get("convergence_reason"),
        "deploymentStatus": run.get("deploy_status"),
        "labelingSessionUrl": run.get("labeling_session_url") or None,
        "labelingSessionName": run.get("labeling_session_name") or None,
    }


@router.get("/runs/{run_id}/status")
async def get_run_status(run_id: RunId):
    """Lightweight status poll endpoint."""
    run = await gso_lakebase.load_gso_run(run_id)
    if not run and _is_configured():
        rows = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_runs')} WHERE run_id = '{run_id}'"
        )
        run = rows[0] if rows else None
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    # Compute lightweight step progress from stages
    stages = await gso_lakebase.load_gso_stages(run_id)
    if not stages and _is_configured():
        stages = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_stages')} "
            f"WHERE run_id = '{run_id}' ORDER BY started_at ASC"
        )
    run_status_str = str(run.get("status", "")).upper()
    steps_completed = 0
    current_step_name = None
    for step_def in _STEP_DEFINITIONS:
        matching = [
            s for s in (stages or [])
            if any(
                str(s.get("stage", "")).upper().startswith(p)
                for p in step_def["stage_prefixes"]
            )
        ]
        status = _derive_step_status(matching)
        status = _normalize_step_status_for_terminal_run(
            status=status, run_status=run_status_str,
        )
        if status in ("completed", "skipped"):
            steps_completed += 1
        elif status == "running" and current_step_name is None:
            current_step_name = step_def["name"]
    if current_step_name is None and steps_completed < 6:
        # Next pending step
        current_step_name = _STEP_DEFINITIONS[steps_completed]["name"]

    # Compute baseline vs optimized scores from iterations (lightweight query)
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = _select_iterations_delta(run_id) or []

    # Bug #2: derive from correct/evaluated. The monitoring ScoreSummary
    # card in AutoOptimizeTab consumes these directly and must agree with
    # RunDetailView's tab labels to the decimal.
    baseline_score = None
    optimized_score = None
    for it in iterations:
        it_num = _safe_int(it.get("iteration"))
        scope = str(it.get("eval_scope", "")).lower()
        if it_num == 0 and scope == "full":
            acc = _derived_accuracy(it, run_id=run_id, iteration=0)
            if acc is not None:
                baseline_score = acc
        elif it_num is not None and it_num > 0 and scope == "full":
            acc = _derived_accuracy(it, run_id=run_id, iteration=it_num)
            if acc is not None and (optimized_score is None or acc > optimized_score):
                optimized_score = acc
    if baseline_score is None:
        for it in iterations:
            if _safe_int(it.get("iteration")) == 0:
                baseline_score = _derived_accuracy(it, run_id=run_id, iteration=0)
                break
    if optimized_score is None:
        for it in iterations:
            it_num = _safe_int(it.get("iteration"))
            if it_num is not None and it_num > 0:
                acc = _derived_accuracy(it, run_id=run_id, iteration=it_num)
                if acc is not None and (optimized_score is None or acc > optimized_score):
                    optimized_score = acc

    return {
        "runId": run.get("run_id"),
        "status": run.get("status"),
        "spaceId": run.get("space_id"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "baselineScore": baseline_score,
        "optimizedScore": optimized_score if optimized_score is not None else baseline_score,
        "convergenceReason": run.get("convergence_reason"),
        "stepsCompleted": steps_completed,
        "totalSteps": 6,
        "currentStepName": current_step_name,
    }


@router.get("/levers")
async def list_levers():
    """List available optimization levers (1-5, excludes lever 0)."""
    all_levers = get_lever_info()
    return [lev for lev in all_levers if lev.get("id", 0) != 0]


@router.post("/runs/{run_id}/apply")
async def apply_run(run_id: RunId):
    """Apply an optimization run's results to the Genie Space."""
    ws = get_workspace_client()
    config = _build_gso_config()

    try:
        result = apply_optimization(run_id, ws, config)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Failed to apply optimization %s: %s", run_id, e)
        raise HTTPException(status_code=500, detail="Failed to apply optimization.")


@router.post("/runs/{run_id}/discard")
async def discard_run(run_id: RunId):
    """Discard an optimization run and rollback to pre-optimization state."""
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()

    try:
        result = discard_optimization(run_id, ws, sp_ws, config)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.exception("Failed to discard optimization %s: %s", run_id, e)
        raise HTTPException(status_code=500, detail="Failed to discard optimization.")


@router.get("/spaces/{space_id}/active-run")
async def get_active_run(space_id: SpaceId):
    """Check for an active optimization run by querying the authoritative Delta table.

    Reconciles zombie runs first (same as trigger.py), then returns active run info.
    Falls back gracefully when GSO is not configured or the warehouse is unavailable.
    """
    if not _is_configured():
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    config = _build_gso_config()
    if not config.warehouse_id:
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    try:
        from genie_space_optimizer.common.warehouse import (
            sql_warehouse_query,
            wh_reconcile_active_runs,
        )

        ws = get_workspace_client()
        sp_ws = get_service_principal_client()

        runs_df = sql_warehouse_query(
            ws,
            config.warehouse_id,
            f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
            f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
        )

        # Reconcile zombie runs (stale QUEUED/IN_PROGRESS with terminated jobs)
        if not runs_df.empty:
            if wh_reconcile_active_runs(
                ws, sp_ws, config.warehouse_id, runs_df,
                config.catalog, config.schema_name,
            ):
                # Re-query after reconciliation updated rows
                runs_df = sql_warehouse_query(
                    ws,
                    config.warehouse_id,
                    f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
                    f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
                )

        # Check for active runs
        _ACTIVE = {"QUEUED", "IN_PROGRESS"}
        if not runs_df.empty:
            active = runs_df[runs_df["status"].isin(list(_ACTIVE))]
            if not active.empty:
                row = active.iloc[0]
                return {
                    "hasActiveRun": True,
                    "activeRunId": str(row.get("run_id", "")),
                    "activeRunStatus": str(row.get("status", "")),
                }

        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}

    except Exception as exc:
        logger.warning("Active-run check failed for space %s: %s", space_id, exc, exc_info=True)
        # Fail open — don't block the UI if the check fails
        return {"hasActiveRun": False, "activeRunId": None, "activeRunStatus": None}


async def load_runs_with_fallback(space_id: str) -> list[dict]:
    """Load optimization runs — Lakebase primary, Delta table fallback.

    Shared by the runs endpoint and the history endpoint.
    """
    runs = await gso_lakebase.load_gso_runs_for_space(space_id)
    if runs:
        return runs

    if not _is_configured():
        return []

    return _delta_query(
        f"SELECT run_id, space_id, status, started_at, completed_at, "
        f"best_accuracy, best_iteration, convergence_reason, triggered_by "
        f"FROM {_delta_table('genie_opt_runs')} "
        f"WHERE space_id = '{space_id}' ORDER BY started_at DESC"
    )


@router.get("/spaces/{space_id}/runs")
async def list_runs_for_space(space_id: SpaceId):
    """List past optimization runs for a space."""
    return await load_runs_with_fallback(space_id)


@router.get("/runs/{run_id}/iterations")
async def list_iterations(run_id: RunId):
    """Get per-iteration evaluation details for a run (excludes rows_json for performance)."""
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = _select_iterations_delta(run_id)
    # Coerce key numeric fields — Delta fallback may return strings.
    # Bug #2: evaluated_count / excluded_count must round-trip as ints so the
    # frontend divides by the same denominator the backend uses.
    for it in iterations:
        it["overall_accuracy"] = _safe_float(it.get("overall_accuracy"))
        it["total_questions"] = _safe_int(it.get("total_questions")) or 0
        it["correct_count"] = _safe_int(it.get("correct_count")) or 0
        if it.get("evaluated_count") is not None:
            it["evaluated_count"] = _safe_int(it.get("evaluated_count"))
        if it.get("excluded_count") is not None:
            it["excluded_count"] = _safe_int(it.get("excluded_count")) or 0
        it["iteration"] = _safe_int(it.get("iteration")) or 0
        it["lever"] = _safe_int(it.get("lever"))
    return iterations


@router.get("/runs/{run_id}/debug-data")
async def debug_data(run_id: RunId):
    """Diagnostic: inspect raw data sources for patches and iterations."""
    config = _build_gso_config()
    diag: dict = {
        "is_configured": _is_configured(),
        "catalog": config.catalog,
        "schema_name": config.schema_name,
        "warehouse_id": config.warehouse_id[:8] + "..." if config.warehouse_id else None,
    }

    # Test Lakebase
    lb_patches = await gso_lakebase.load_gso_patches(run_id)
    lb_iterations = await gso_lakebase.load_gso_iterations(run_id)
    lb_stages = await gso_lakebase.load_gso_stages(run_id)
    lb_run = await gso_lakebase.load_gso_run(run_id)
    diag["lakebase"] = {
        "run_found": bool(lb_run),
        "stages_count": len(lb_stages),
        "iterations_count": len(lb_iterations),
        "patches_count": len(lb_patches),
    }

    # Test Delta fallback
    delta_diag: dict = {"attempted": False}
    if _is_configured():
        delta_diag["attempted"] = True
        try:
            delta_patches = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_patches')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["patches_count"] = delta_patches[0]["cnt"] if delta_patches else "query_returned_empty"
        except Exception as e:
            delta_diag["patches_error"] = str(e)[:200]
        try:
            delta_iters = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["iterations_count"] = delta_iters[0]["cnt"] if delta_iters else "query_returned_empty"
        except Exception as e:
            delta_diag["iterations_error"] = str(e)[:200]
        try:
            delta_stages = _delta_query(
                f"SELECT count(*) as cnt FROM {_delta_table('genie_opt_stages')} "
                f"WHERE run_id = '{run_id}'"
            )
            delta_diag["stages_count"] = delta_stages[0]["cnt"] if delta_stages else "query_returned_empty"
        except Exception as e:
            delta_diag["stages_error"] = str(e)[:200]
    diag["delta"] = delta_diag

    # Show what get_run actually loaded (stages come from somewhere)
    stages = lb_stages
    if not stages and _is_configured():
        stages = _delta_query(
            f"SELECT stage, status, lever, iteration FROM {_delta_table('genie_opt_stages')} "
            f"WHERE run_id = '{run_id}' ORDER BY started_at ASC LIMIT 10"
        )
    diag["stage_samples"] = stages[:5] if stages else []

    # Sample patches if any exist in Delta
    if _is_configured():
        try:
            raw_patches = _delta_query(
                f"SELECT iteration, lever, patch_type, scope, risk_level FROM {_delta_table('genie_opt_patches')} "
                f"WHERE run_id = '{run_id}' LIMIT 3"
            )
            diag["delta_patch_samples"] = raw_patches
        except Exception as e:
            diag["delta_patch_samples_error"] = str(e)[:200]

    # Sample iteration 0 from Delta
    if _is_configured():
        try:
            raw_iter0 = _delta_query(
                f"SELECT iteration, eval_scope, overall_accuracy, scores_json "
                f"FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}' AND iteration = 0 LIMIT 1"
            )
            if raw_iter0:
                r = raw_iter0[0]
                scores = r.get("scores_json")
                diag["delta_iter0"] = {
                    "iteration": r.get("iteration"),
                    "eval_scope": r.get("eval_scope"),
                    "overall_accuracy": r.get("overall_accuracy"),
                    "scores_json_type": type(scores).__name__,
                    "scores_json_preview": str(scores)[:300] if scores else None,
                }
            else:
                diag["delta_iter0"] = "not_found"
        except Exception as e:
            diag["delta_iter0_error"] = str(e)[:200]

    return diag


@router.get("/runs/{run_id}/asi-results")
async def list_asi_results(run_id: RunId, iteration: int = Query(..., description="Iteration number")):
    """Get per-judge ASI failure analysis for a specific iteration."""
    results = await gso_lakebase.load_gso_asi_results(run_id, iteration)
    if not results and _is_configured():
        results = _delta_query(
            f"SELECT * FROM {_delta_table('genie_eval_asi_results')} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration}"
        )
    return results


@router.get("/runs/{run_id}/question-results")
async def list_question_results(run_id: RunId, iteration: int = Query(..., description="Iteration number")):
    """Get per-question results (question text + SQL) for a specific iteration."""

    # Try full-scope first, then fall back to any scope
    rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, iteration, "full")
    if not rows_json_str:
        rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, iteration, None)

    # Always try Delta if Lakebase returned nothing usable (None, empty string, etc.)
    if not rows_json_str and _is_configured():
        logger.info("Lakebase returned no rows_json for run=%s iter=%s, trying Delta", run_id, iteration)
        delta_rows = _delta_query(
            f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration} AND eval_scope = 'full' LIMIT 1"
        )
        if not delta_rows:
            delta_rows = _delta_query(
                f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
                f"WHERE run_id = '{run_id}' AND iteration = {iteration} "
                f"AND rows_json IS NOT NULL LIMIT 1"
            )
        rows_json_str = delta_rows[0]["rows_json"] if delta_rows else None

    return _parse_question_rows(rows_json_str)


@router.get("/runs/{run_id}/patches")
async def list_patches(run_id: RunId):
    """Get all optimization patches for a run."""
    patches = await gso_lakebase.load_gso_patches(run_id)
    if not patches and _is_configured():
        patches = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_patches')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration, lever, patch_index"
        )
    return patches


@router.get("/runs/{run_id}/suggestions")
async def list_suggestions(run_id: RunId):
    """Get strategist improvement suggestions for a run."""
    suggestions = await gso_lakebase.load_gso_suggestions(run_id)
    if not suggestions and _is_configured():
        suggestions = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_suggestions')} "
            f"WHERE run_id = '{run_id}' ORDER BY created_at ASC"
        )
    results = []
    for s in suggestions:
        aff = s.get("affected_questions", "[]")
        if isinstance(aff, str):
            try:
                aff = json.loads(aff)
            except (json.JSONDecodeError, TypeError):
                aff = []
        if not isinstance(aff, list):
            aff = []
        results.append({
            "suggestionId": s.get("suggestion_id"),
            "runId": s.get("run_id"),
            "spaceId": s.get("space_id"),
            "iteration": s.get("iteration"),
            "suggestionType": s.get("type", ""),
            "title": s.get("title", ""),
            "rationale": s.get("rationale"),
            "definition": s.get("definition"),
            "affectedQuestions": aff,
            "estimatedImpact": s.get("estimated_impact"),
            "status": s.get("status", "PROPOSED"),
        })
    return results


def _parse_question_rows(rows_json_str: str | None) -> list[dict]:
    """Parse rows_json from genie_opt_iterations into per-question results."""
    if not rows_json_str:
        return []

    try:
        rows = json.loads(rows_json_str) if isinstance(rows_json_str, str) else rows_json_str
    except Exception:
        return []

    if not isinstance(rows, list):
        return []

    results = []
    for row in rows:
        # --------------- Parse nested request/response dicts ---------------
        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        if not isinstance(_req, dict):
            _req = {}
        _req_kw = _req.get("kwargs", {})
        if not isinstance(_req_kw, dict):
            _req_kw = {}

        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}
        if not isinstance(_resp, dict):
            _resp = {}

        # Fallback: legacy inputs/outputs dicts
        inputs = row.get("inputs") or {}
        if not isinstance(inputs, dict):
            inputs = {}
        outputs = row.get("outputs") or {}
        if not isinstance(outputs, dict):
            outputs = {}

        def _str_or_none(val: object) -> str | None:
            """Return val as string if it's a scalar, None if it's a dict/list/None."""
            if val is None or isinstance(val, (dict, list)):
                return None
            return str(val)

        # --------------- Question text ---------------
        # Primary: request.question  |  Fallback: inputs/question, flat keys
        question = str(
            _req.get("question")
            or row.get("inputs/question")
            or inputs.get("question")
            or row.get("question")
            or row.get("question_text")
            or _req_kw.get("question")
            or ""
        ).strip()

        # --------------- Question ID ---------------
        # Primary: request.kwargs.question_id  |  Fallback: inputs/question_id, flat keys
        question_id = str(
            _req_kw.get("question_id")
            or row.get("inputs/question_id")
            or inputs.get("question_id")
            or row.get("question_id")
            or row.get("request_id")
            or _req.get("question_id")
            or ""
        ).strip()
        if not question_id and question:
            question_id = question[:80]

        if not question_id and not question:
            continue

        # --------------- Generated SQL (Genie's response) ---------------
        # Primary: response.response  |  Fallback: outputs/response, outputs/generated_sql
        generated_sql = (
            _str_or_none(_resp.get("response"))
            or _str_or_none(outputs.get("generated_sql"))
            or _str_or_none(row.get("outputs/generated_sql"))
            or _str_or_none(row.get("outputs/response"))
            or _str_or_none(outputs.get("response"))
            or _str_or_none(row.get("generated_sql"))
        )

        # --------------- Expected SQL (ground truth) ---------------
        # Primary: request.expected_sql  |  Fallback: inputs/expected_sql, outputs/expected_sql
        expected_sql = (
            _str_or_none(_req.get("expected_sql"))
            or _str_or_none(outputs.get("expected_sql"))
            or _str_or_none(row.get("outputs/expected_sql"))
            or _str_or_none(row.get("inputs/expected_sql"))
            or _str_or_none(inputs.get("expected_sql"))
            or _str_or_none(row.get("expected_sql"))
        )

        # --------------- Comparison metadata ---------------
        # Primary: response.comparison  |  Fallback: outputs.comparison
        comparison = _resp.get("comparison") or {}
        if not isinstance(comparison, dict):
            comparison = {}
        if not comparison:
            comparison = outputs.get("comparison") or {}
            if not isinstance(comparison, dict):
                comparison = {}
        if not comparison:
            cmp_raw = row.get("outputs/comparison")
            if isinstance(cmp_raw, dict):
                comparison = cmp_raw
        match_type = (
            comparison.get("match_type")
            or row.get("outputs/comparison/match_type")
            or row.get("match_type")
        )

        # Extract judge verdicts
        rc = str(
            row.get("result_correctness/value")
            or row.get("outputs/result_correctness/value")
            or outputs.get("result_correctness/value")
            or ""
        ).lower()
        arbiter = str(
            row.get("arbiter/value")
            or row.get("arbiter")
            or ""
        ).lower()

        # Collect all judge verdicts for the response
        judge_verdicts: dict[str, str] = {}
        for judge in (
            "syntax_validity", "schema_accuracy", "logical_accuracy",
            "semantic_equivalence", "completeness", "response_quality",
            "asset_routing", "result_correctness", "arbiter",
        ):
            val = str(
                row.get(f"{judge}/value") or row.get(judge) or ""
            ).strip()
            if val:
                judge_verdicts[judge] = val

        # Determine pass/fail using arbiter-adjusted accuracy logic
        # (matches GSO engine's _compute_arbiter_adjusted_accuracy)
        #
        # Exclusions: result_correctness=="excluded", both_empty, genie_result_unavailable
        error_type = str(
            comparison.get("error_type")
            or row.get("outputs/comparison/error_type")
            or ""
        ).lower()
        excluded = (
            rc == "excluded"
            or error_type in ("both_empty", "genie_result_unavailable")
        )

        if excluded:
            passed = None  # neither pass nor fail — excluded from accuracy
        else:
            # Arbiter-based pass/fail:
            # - both_correct / genie_correct → pass (overrides individual judge failures)
            # - ground_truth_correct / neither_correct → fail
            # - skipped / empty → fall back to result_correctness
            rc_pass = rc in ("yes", "true", "1", "1.0")
            arbiter_pass = arbiter in ("genie_correct", "both_correct")
            arbiter_fail = arbiter in ("ground_truth_correct", "neither_correct")

            if arbiter_pass:
                passed = True
            elif arbiter_fail:
                passed = False
            else:
                # No arbiter verdict — use result_correctness
                passed = rc_pass

        results.append({
            "question_id": question_id,
            "question": question,
            "generated_sql": generated_sql,
            "expected_sql": expected_sql,
            "passed": passed,
            "match_type": match_type,
            "judge_verdicts": judge_verdicts,
            "excluded": excluded,
            "genie_sample": comparison.get("genie_sample"),
            "gt_sample": comparison.get("gt_sample"),
            "genie_columns": comparison.get("genie_columns"),
            "gt_columns": comparison.get("gt_columns"),
            "genie_rows": comparison.get("genie_rows"),
            "gt_rows": comparison.get("gt_rows"),
        })

    return results


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _isoformat(val) -> str | None:
    """Safely convert a datetime to ISO format string."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return val.isoformat()
