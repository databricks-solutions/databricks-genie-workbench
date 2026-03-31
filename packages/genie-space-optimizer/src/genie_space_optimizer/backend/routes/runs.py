"""Run endpoints: status, comparison, apply, discard."""

from __future__ import annotations

import json
import logging
import math
from typing import Any, TypedDict

from databricks.sdk import WorkspaceClient
from fastapi import HTTPException

from ..constants import ACTIVE_RUN_STATUSES, TERMINAL_JOB_STATES
from ..core import Dependencies, create_router
from ..utils import ensure_utc_iso, safe_finite, safe_float, safe_int, safe_json_parse, scrub_nan_inf
from .spaces import _genie_client
from ..models import (
    ActionResponse,
    AsiResult,
    AsiSummary,
    ComparisonData,
    DimensionScore,
    GateResult,
    IterationDetail,
    IterationDetailResponse,
    IterationSummary,
    LeverStatus,
    PipelineLink,
    PipelineRun,
    PipelineStep,
    ProvenanceRecord,
    ProvenanceSummary,
    QuestionResult,
    ReflectionEntry,
    SpaceConfiguration,
    TableDescription,
)
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)
_DEFERRED_UC_MODES = {"uc_artifact", "both"}
_UC_WRITE_STAGE = "UC_OBO_WRITE"
_UC_WRITE_TASK_KEY = "obo_uc_write"
_UC_WRITE_READY_STATUSES = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}
_UC_WRITE_TERMINAL_STAGE_STATUSES = {"COMPLETE", "SKIPPED", "FAILED"}
_TERMINAL_RUN_STATUSES = {
    "CONVERGED",
    "STALLED",
    "MAX_ITERATIONS",
    "FAILED",
    "CANCELLED",
    "APPLIED",
    "DISCARDED",
}


class _StepDefinition(TypedDict):
    stepNumber: int
    name: str
    stage_prefixes: list[str]
    summary_template: str


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _parse_patch_command(raw: object) -> dict:
    """Parse a patch command JSON column into a dict."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        first = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if isinstance(first, dict):
        return first
    if isinstance(first, str):
        try:
            second = json.loads(first)
        except (json.JSONDecodeError, TypeError):
            return {}
        return second if isinstance(second, dict) else {}
    return {}


def _uc_statement_from_patch(patch_type: str, command: dict) -> str | None:
    """Build a UC SQL statement for supported deferred patch types."""
    if patch_type in {"add_column_description", "update_column_description"}:
        table = str(command.get("table") or "").strip()
        column = str(command.get("column") or "").strip()
        text = str(command.get("new_text") or command.get("value") or "").strip()
        if table and column and text:
            escaped = text.replace("'", "''")
            return f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'"

    if patch_type in {"add_description", "update_description"}:
        table = str(command.get("target") or "").strip()
        text = str(command.get("new_text") or command.get("value") or "").strip()
        if table and text:
            escaped = text.replace("'", "''")
            return f"COMMENT ON TABLE {table} IS '{escaped}'"

    if patch_type == "update_tvf_sql":
        stmt = str(command.get("new_sql") or "").strip()
        if stmt:
            return stmt

    return None


def _uc_stage_already_terminal(stages_rows: list[dict]) -> bool:
    for stage in stages_rows:
        stage_name = str(stage.get("stage", "")).upper()
        status = str(stage.get("status", "")).upper()
        if stage_name.startswith(_UC_WRITE_STAGE) and status in _UC_WRITE_TERMINAL_STAGE_STATUSES:
            return True
    return False


def _apply_deferred_uc_writes_obo(
    *,
    spark,
    run_id: str,
    run_data: dict,
    stages_rows: list[dict],
    ws,
    config,
) -> str:
    """Replay UC write SQL via OBO after optimization reaches terminal state."""
    from genie_space_optimizer.optimization.state import load_patches, write_stage

    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()
    run_status = str(run_data.get("status") or "").upper()

    if requested_mode not in _DEFERRED_UC_MODES:
        return "not_required"
    if run_status not in _UC_WRITE_READY_STATUSES:
        raise RuntimeError(f"Run status {run_status} is not ready for deferred UC writes")
    if _uc_stage_already_terminal(stages_rows):
        return "already_recorded"

    try:
        write_stage(
            spark,
            run_id,
            _UC_WRITE_STAGE,
            "STARTED",
            task_key=_UC_WRITE_TASK_KEY,
            detail={"mode": requested_mode},
            catalog=config.catalog,
            schema=config.schema_name,
        )

        if not config.warehouse_id:
            raise RuntimeError("Missing warehouse_id for OBO SQL execution")

        patches_df = load_patches(spark, run_id, config.catalog, config.schema_name)
        if patches_df.empty:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "SKIPPED",
                task_key=_UC_WRITE_TASK_KEY,
                detail={"reason": "no_patches"},
                catalog=config.catalog,
                schema=config.schema_name,
            )
            return "skipped_no_patches"

        statements: list[str] = []
        skipped = 0
        for row in patches_df.to_dict("records"):
            if _is_truthy(row.get("rolled_back")):
                continue
            patch_type = str(row.get("patch_type") or "")
            command = _parse_patch_command(row.get("command_json"))
            stmt = _uc_statement_from_patch(patch_type, command)
            if stmt:
                statements.append(stmt)
            else:
                skipped += 1

        if not statements:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "SKIPPED",
                task_key=_UC_WRITE_TASK_KEY,
                detail={
                    "reason": "no_supported_uc_commands",
                    "skipped_count": skipped,
                },
                catalog=config.catalog,
                schema=config.schema_name,
            )
            return "skipped_no_supported_commands"

        for statement in statements:
            ws.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=statement,
                wait_timeout="60s",
            )

        write_stage(
            spark,
            run_id,
            _UC_WRITE_STAGE,
            "COMPLETE",
            task_key=_UC_WRITE_TASK_KEY,
            detail={
                "executed_count": len(statements),
                "skipped_count": skipped,
            },
            catalog=config.catalog,
            schema=config.schema_name,
        )
        return "applied"
    except Exception as exc:
        logger.exception("Deferred OBO UC write failed for run %s", run_id)
        try:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "FAILED",
                task_key=_UC_WRITE_TASK_KEY,
                error_message=str(exc)[:500],
                detail={"error": str(exc)[:500]},
                catalog=config.catalog,
                schema=config.schema_name,
            )
        except Exception:
            logger.exception("Failed to persist UC write failure stage for run %s", run_id)
        raise RuntimeError(f"Deferred OBO UC write failed: {exc}") from exc


# ── Stage-to-Step Mapping ───────────────────────────────────────────────


_STEP_DEFINITIONS: list[_StepDefinition] = [
    {
        "stepNumber": 1,
        "name": "Preflight",
        "stage_prefixes": ["PREFLIGHT"],
        "summary_template": "Analyzed {tables} tables, {columns} columns, {instructions} instructions, {questions} sample questions",
    },
    {
        "stepNumber": 2,
        "name": "Baseline Evaluation",
        "stage_prefixes": ["BASELINE_EVAL"],
        "summary_template": "Evaluated {questions} benchmark questions with 9 judges. Baseline score: {score}% ({correct}/{questions} correct)",
    },
    {
        "stepNumber": 3,
        "name": "Proactive Enrichment",
        "stage_prefixes": ["ENRICHMENT", "PROMPT_MATCH", "DESCRIPTION_ENRICHMENT", "JOIN_DISCOVERY", "SPACE_METADATA", "INSTRUCTION_SEED", "PROACTIVE_INSTRUCTION", "EXAMPLE_SQL"],
        "summary_template": "Applied {total} proactive enrichments: {descriptions} descriptions, {joins} joins, {instructions} instructions, {examples} example SQLs",
    },
    {
        "stepNumber": 4,
        "name": "Adaptive Optimization",
        "stage_prefixes": ["LEVER_", "AG_"],
        "summary_template": "Applied {patches} optimizations across {levers} categories. Score improved from {before}% to {after}%",
    },
    {
        "stepNumber": 5,
        "name": "Finalization",
        "stage_prefixes": ["FINALIZE", "REPEATABILITY", "HELD_OUT"],
        "summary_template": "Final evaluation complete. Optimized score: {score}%. Repeatability: {repeatability}%",
    },
    {
        "stepNumber": 6,
        "name": "Deploy",
        "stage_prefixes": ["DEPLOY", "COMPLETE", _UC_WRITE_STAGE],
        "summary_template": "Deployment {status}",
    },
]


def _derive_step_status(matching_stages: list[dict]) -> str:
    """Derive user-facing step status from the latest matching stage row."""
    if not matching_stages:
        return "pending"
    latest = matching_stages[-1]
    latest_status = str(latest.get("status", "")).upper()
    if latest_status == "FAILED":
        return "failed"
    if latest_status in {"COMPLETE", "SKIPPED", "ROLLED_BACK"}:
        return "completed"
    if latest_status == "STARTED":
        return "running"
    return "pending"


def _total_duration(matching_stages: list[dict]) -> float | None:
    """Sum duration_seconds across matching stages."""
    total = 0.0
    has_any = False
    for s in matching_stages:
        d = s.get("duration_seconds")
        if d is not None:
            total += _finite(d)
            has_any = True
    return total if has_any else None


def _parse_detail(stage: dict) -> dict:
    """Parse detail_json from a stage row."""
    parsed = safe_json_parse(stage.get("detail_json"))
    return parsed if isinstance(parsed, dict) else {}


def _resolve_parsed_space(config_snapshot: dict) -> dict:
    """Extract the parsed Genie Space structure from config_snapshot.

    Handles multiple storage formats:
    1. ``_parsed_space`` key (normal fetch_space_config output)
    2. ``serialized_space`` as a JSON string (raw API response)
    3. ``data_sources`` at top level (the parsed space itself)
    """
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


def _collect_all_preflight_detail(stages_rows: list[dict]) -> dict:
    """Merge detail_json from ALL PREFLIGHT stages into a single dict.

    This provides a comprehensive fallback — if PREFLIGHT_STARTED COMPLETE
    wasn't written by an older harness, PREFLIGHT_METADATA_COLLECTION detail
    (which has ``table_ref_count``) is still available.
    """
    merged: dict[str, Any] = {}
    for s in stages_rows:
        stage_name = str(s.get("stage", ""))
        if stage_name.startswith("PREFLIGHT"):
            merged.update(_parse_detail(s))
    return merged


def map_stages_to_steps(
    stages_rows: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
    *,
    patches_rows: list[dict] | None = None,
) -> list[PipelineStep]:
    """Map internal harness stages to 6 user-facing pipeline steps."""
    steps: list[PipelineStep] = []

    for defn in _STEP_DEFINITIONS:
        prefixes = defn["stage_prefixes"]

        matching = []
        for s in stages_rows:
            stage_name = str(s.get("stage", ""))
            for prefix in prefixes:
                if stage_name.startswith(prefix):
                    matching.append(s)
                    break

        status = _derive_step_status(matching)
        status = _normalize_step_status_for_terminal_run(
            status=status,
            run_status=str(run_data.get("status", "")),
        )
        duration = _total_duration(matching)

        summary = _build_step_summary(defn, matching, iterations_rows, run_data, stages_rows=stages_rows)
        inputs, outputs = _build_step_io(
            defn, matching, iterations_rows, run_data,
            stages_rows=stages_rows, patches_rows=patches_rows,
        )

        steps.append(
            PipelineStep(
                stepNumber=defn["stepNumber"],
                name=defn["name"],
                status=status,
                durationSeconds=duration,
                summary=summary,
                inputs=inputs,
                outputs=outputs,
            )
        )

    return steps


def _extract_proactive_changes(matching: list[dict]) -> dict:
    """Scan matched stages for proactive pre-loop enrichment results."""
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


def _build_step_io(
    defn: _StepDefinition,
    matching: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
    *,
    stages_rows: list[dict] | None = None,
    patches_rows: list[dict] | None = None,
) -> tuple[dict | None, dict | None]:
    """Build rich inputs/outputs payload for pipeline step drill-down."""
    if not matching:
        return None, None

    step_name = defn["name"]
    detail: dict[str, Any] = {}
    for s in matching:
        detail.update(_parse_detail(s))
    timeline = _build_stage_timeline(matching)
    raw_snap = run_data.get("config_snapshot")
    if isinstance(raw_snap, dict):
        config_snapshot = raw_snap
    elif isinstance(raw_snap, str):
        config_snapshot = safe_json_parse(raw_snap)
        if not isinstance(config_snapshot, dict):
            config_snapshot = {}
    else:
        config_snapshot = {}

    if step_name == "Preflight":
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        parsed = _resolve_parsed_space(config_snapshot)
        ds = parsed.get("data_sources", {}) if isinstance(parsed, dict) else {}
        tables = ds.get("tables", []) if isinstance(ds, dict) else []
        functions = ds.get("functions", []) if isinstance(ds, dict) else []
        instructions_node = parsed.get("instructions", {}) if isinstance(parsed, dict) else {}
        text_instructions = (
            instructions_node.get("text_instructions", [])
            if isinstance(instructions_node, dict)
            else []
        )
        examples = (
            instructions_node.get("example_question_sqls", [])
            if isinstance(instructions_node, dict)
            else []
        )
        sample_questions: list[str] = []
        for ex in examples:
            q = str(ex.get("question") or "").strip() if isinstance(ex, dict) else ""
            if q:
                sample_questions.append(q)

        table_count = (
            _safe_int(detail.get("table_count"))
            or _safe_int(all_pf.get("table_count"))
            or _safe_int(all_pf.get("table_ref_count"))
            or len(tables)
        )
        function_count = (
            _safe_int(detail.get("function_count"))
            or _safe_int(all_pf.get("function_count"))
            or len(functions)
        )
        instruction_count = (
            _safe_int(detail.get("instruction_count"))
            or _safe_int(all_pf.get("instruction_count"))
            or len(text_instructions)
        )
        sample_q_count = (
            _safe_int(detail.get("benchmark_count"))
            or _safe_int(all_pf.get("benchmark_count"))
            or _safe_int(detail.get("sample_question_count"))
            or len(sample_questions)
        )

        def _to_int(value: Any) -> int | None:
            if value is None:
                return None
            try:
                p = int(value)
            except (TypeError, ValueError):
                return None
            return p if p >= 0 else None

        def _to_list_of_str(value: Any) -> list[str]:
            if not isinstance(value, list):
                return []
            out: list[str] = []
            for item in value:
                text = str(item).strip()
                if text:
                    out.append(text)
            return out

        prefetched = (
            config_snapshot.get("_prefetched_uc_metadata", {})
            if isinstance(config_snapshot, dict)
            else {}
        )
        uc_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
        uc_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
        uc_routines = prefetched.get("uc_routines", []) if isinstance(prefetched, dict) else []

        column_samples: list[str] = []
        for col in uc_columns[:12]:
            if not isinstance(col, dict):
                continue
            t_name = str(col.get("table_name") or col.get("table") or "").strip()
            c_name = str(col.get("column_name") or col.get("column") or "").strip()
            if t_name and c_name:
                column_samples.append(f"{t_name}.{c_name}")
            elif c_name:
                column_samples.append(c_name)

        columns_collected = _to_int(detail.get("columns_collected"))
        if columns_collected is None:
            columns_collected = _to_int(detail.get("columnsCollected"))
        if columns_collected is None:
            columns_collected = len(uc_columns) if isinstance(uc_columns, list) else 0

        tags_collected = _to_int(detail.get("tags_collected"))
        if tags_collected is None:
            tags_collected = _to_int(detail.get("tagsCollected"))
        if tags_collected is None:
            tags_collected = len(uc_tags) if isinstance(uc_tags, list) else 0

        return (
            {
                "spaceId": run_data.get("space_id"),
                "domain": run_data.get("domain"),
                "catalog": run_data.get("catalog"),
                "schema": run_data.get("uc_schema"),
            },
            {
                "tableCount": table_count,
                "tables": [str(t.get("identifier") or "") for t in tables[:12] if isinstance(t, dict)],
                "functionCount": function_count,
                "instructionCount": instruction_count,
                "sampleQuestionCount": sample_q_count,
                "sampleQuestionsPreview": sample_questions[:5],
                "columnsCollected": columns_collected,
                "tagsCollected": tags_collected,
                "columnSamples": _to_list_of_str(detail.get("column_samples")) or _to_list_of_str(detail.get("columnSamples")) or [s for s in column_samples if s],
                "stageEvents": timeline,
            },
        )

    if step_name == "Baseline Evaluation":
        baseline_iter = next(
            (
                r for r in iterations_rows
                if _safe_int(r.get("iteration")) == 0
                and str(r.get("eval_scope", "")).lower() == "full"
            ),
            None,
        )
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
            sample_rows.append(
                {
                    "question": question,
                    "resultCorrectness": row.get("result_correctness/value", row.get("result_correctness")),
                    "syntaxValidity": row.get("syntax_validity/value", row.get("syntax_validity")),
                    "assetRouting": row.get("asset_routing/value", row.get("asset_routing")),
                    "matchType": (
                        row.get("outputs", {}).get("comparison", {}).get("match_type")
                        if isinstance(row.get("outputs"), dict)
                        else None
                    ),
                    "error": (
                        row.get("outputs", {}).get("comparison", {}).get("error")
                        if isinstance(row.get("outputs"), dict)
                        else None
                    ),
                }
            )

        return (
            {
                "benchmarkCount": baseline_iter.get("total_questions"),
                "iteration": 0,
            },
            {
                "judgeScores": {k: _safe_float(v) for k, v in scores.items()},
                "totalQuestions": baseline_iter.get("total_questions"),
                "correctCount": baseline_iter.get("correct_count"),
                "failedCount": int(
                    _finite(baseline_iter.get("total_questions", 0))
                    - _finite(baseline_iter.get("correct_count", 0))
                ),
                "mlflowRunId": baseline_iter.get("mlflow_run_id"),
                "invalidBenchmarkCount": _safe_int(detail.get("invalid_benchmark_count")),
                "permissionBlockedCount": _safe_int(detail.get("permission_blocked_count")),
                "unresolvedColumnCount": _safe_int(detail.get("unresolved_column_count")),
                "harnessRetryCount": _safe_int(detail.get("harness_retry_count")),
                "sampleQuestions": sample_rows,
                "stageEvents": timeline,
            },
        )

    if step_name == "Proactive Enrichment":
        proactive = _extract_proactive_changes(matching)
        lever_0_count = sum(
            1 for p in (patches_rows or [])
            if _safe_int(p.get("lever")) == 0
        )
        return (
            {
                "spaceId": run_data.get("space_id"),
            },
            {
                "proactiveChanges": proactive if proactive else None,
                "enrichmentModelId": detail.get("enrichment_model_id"),
                "totalEnrichments": detail.get("total_enrichments", 0),
                "totalConfigChanges": lever_0_count,
                "enrichmentSkipped": detail.get("enrichment_skipped", False),
                "stageEvents": timeline,
            },
        )

    if step_name == "Adaptive Optimization":
        patches_applied = detail.get("patches_applied")
        if patches_applied is None:
            patches_applied = detail.get("patches_count")
        iteration_counter = detail.get("iteration_counter")
        if iteration_counter is None:
            iteration_counter = run_data.get("best_iteration")
        levers_accepted = detail.get("levers_accepted", [])
        levers_rolled_back = detail.get("levers_rolled_back", [])

        return (
            {
                "leverCountConfigured": len(run_data.get("levers", []))
                if isinstance(run_data.get("levers"), list)
                else None,
                "maxIterations": run_data.get("max_iterations"),
            },
            {
                "patchesApplied": patches_applied,
                "leversAccepted": levers_accepted,
                "leversRolledBack": levers_rolled_back,
                "iterationCounter": iteration_counter,
                "baselineAccuracy": run_data.get("baseline_accuracy"),
                "bestAccuracy": _safe_float(run_data.get("best_accuracy")),
                "stageEvents": timeline,
            },
        )

    if step_name == "Finalization":
        return (
            {
                "bestIteration": run_data.get("best_iteration"),
            },
            {
                "bestAccuracy": _safe_float(run_data.get("best_accuracy")),
                "repeatability": _safe_float(run_data.get("best_repeatability")),
                "convergenceReason": run_data.get("convergence_reason"),
                "ucModelName": detail.get("uc_model_name") or None,
                "ucModelVersion": detail.get("uc_model_version") or None,
                "ucChampionPromoted": detail.get("uc_champion_promoted", False),
                "heldOutAccuracy": _safe_float(detail.get("held_out_accuracy")),
                "heldOutCount": _safe_int(detail.get("held_out_count")),
                "trainAccuracy": _safe_float(detail.get("train_accuracy")),
                "heldOutDeltaPp": _safe_float(detail.get("delta_pp")),
                "stageEvents": timeline,
            },
        )

    if step_name == "Deploy":
        return (
            {
                "deployTarget": run_data.get("deploy_target"),
            },
            {
                "deployStatus": detail.get("status"),
                "stageEvents": timeline,
            },
        )

    return None, {"stageEvents": timeline}


def _build_stage_timeline(matching: list[dict]) -> list[dict[str, Any]]:
    """Compact stage timeline for UI drill-down."""
    events: list[dict[str, Any]] = []
    for s in matching:
        events.append(
            {
                "stage": s.get("stage"),
                "status": str(s.get("status", "")).lower(),
                "startedAt": ensure_utc_iso(s.get("started_at")),
                "completedAt": ensure_utc_iso(s.get("completed_at")),
                "durationSeconds": _safe_float(s.get("duration_seconds")),
                "errorMessage": s.get("error_message"),
            }
        )
    return events


def _build_step_summary(
    defn: _StepDefinition,
    matching: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
    *,
    stages_rows: list[dict] | None = None,
) -> str | None:
    """Build a human-readable summary for a pipeline step."""
    if not matching:
        return None

    step_name = defn["name"]
    detail = {}
    for s in matching:
        detail.update(_parse_detail(s))

    if step_name == "Preflight":
        all_pf = _collect_all_preflight_detail(stages_rows or [])
        tables_val = (
            _safe_int(detail.get("table_count"))
            or _safe_int(all_pf.get("table_count"))
            or _safe_int(all_pf.get("table_ref_count"))
        )
        columns_val = (
            _safe_int(detail.get("columns_collected"))
            or _safe_int(detail.get("columnsCollected"))
            or _safe_int(all_pf.get("columns_collected"))
        )
        instr_val = (
            _safe_int(detail.get("instruction_count"))
            or _safe_int(all_pf.get("instruction_count"))
        )
        bench_val = (
            _safe_int(detail.get("benchmark_count"))
            or _safe_int(all_pf.get("benchmark_count"))
            or _safe_int(run_data.get("benchmark_count"))
        )
        return defn["summary_template"].format(
            tables=tables_val if tables_val else "?",
            columns=columns_val if columns_val else "?",
            instructions=instr_val if instr_val else "?",
            questions=bench_val if bench_val else "?",
        )
    if step_name == "Baseline Evaluation":
        baseline_iter = next(
            (r for r in iterations_rows if r.get("iteration") == 0),
            None,
        )
        score = "?"
        questions = "?"
        correct = "?"
        if baseline_iter:
            score = f"{_finite(baseline_iter.get('overall_accuracy', 0)):.1f}"
            questions = str(baseline_iter.get("total_questions", "?"))
            correct = str(_safe_int(baseline_iter.get("correct_count")) or "?")
        return defn["summary_template"].format(questions=questions, score=score, correct=correct)
    if step_name == "Proactive Enrichment":
        descriptions = _safe_int(detail.get("descriptions_enriched")) or 0
        joins = _safe_int(detail.get("joins_discovered")) or 0
        examples = _safe_int(detail.get("examples_mined")) or 0
        instructions = 1 if detail.get("instructions_seeded") else 0
        fa_enabled = _safe_int(detail.get("format_assistance_enabled")) or 0
        sql_exprs = _safe_int(detail.get("sql_expressions_seeded")) or 0
        total = _safe_int(detail.get("total_enrichments")) or (descriptions + joins + instructions + examples)
        parts: list[str] = []
        if descriptions:
            parts.append(f"{descriptions} descriptions")
        if joins:
            parts.append(f"{joins} joins")
        if instructions:
            parts.append(f"{instructions} instructions")
        if examples:
            parts.append(f"{examples} example SQLs")
        if sql_exprs:
            parts.append(f"{sql_exprs} SQL expressions")
        if fa_enabled:
            parts.append(f"{fa_enabled} format assistance columns")
        if parts:
            return f"Applied {total} proactive enrichments: {', '.join(parts)}"
        if total:
            return f"Applied {total} proactive enrichments"
        return "Proactive enrichment complete"
    if step_name == "Adaptive Optimization":
        patches = detail.get("patches_applied", 0)
        levers_accepted = detail.get("levers_accepted", [])
        before = f"{_finite(run_data.get('baseline_accuracy', 0)):.1f}" if run_data.get("baseline_accuracy") else "?"
        after = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return defn["summary_template"].format(
            patches=patches,
            levers=len(levers_accepted) if isinstance(levers_accepted, list) else "?",
            before=before,
            after=after,
        )
    if step_name == "Finalization":
        score = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        rep = f"{_finite(run_data.get('best_repeatability', 0)):.1f}" if run_data.get("best_repeatability") else "?"
        summary = defn["summary_template"].format(score=score, repeatability=rep)
        ho_acc = _safe_float(detail.get("held_out_accuracy"))
        if ho_acc is not None:
            summary += f" Held-out: {ho_acc:.1f}%"
        return summary
    if step_name == "Deploy":
        status = detail.get("status", "pending")
        return defn["summary_template"].format(status=status)

    return None


def _build_levers(
    stages_rows: list[dict],
    *,
    run_status: str = "",
    configured_levers: list[int] | None = None,
    patches_rows: list[dict] | None = None,
    iterations_rows: list[dict] | None = None,
) -> list[LeverStatus]:
    """Build lever detail from LEVER_* and AG_* stage rows."""
    from genie_space_optimizer.common.config import LEVER_NAMES

    lever_data: dict[int, dict] = {}
    for configured in configured_levers or []:
        try:
            lever_data[int(configured)] = {"stages": [], "detail": {}, "patches": []}
        except (TypeError, ValueError):
            continue

    # Build iteration → levers mapping from multiple sources so AG_* stages
    # get assigned to the correct lever(s) even when genie_opt_iterations
    # has lever=0 (hardcoded by older harness versions).
    iter_to_levers: dict[int, set[int]] = {}

    # Source 1: patches table (always has the correct lever column)
    for p in patches_rows or []:
        it = _safe_int(p.get("iteration"))
        lv = _safe_int(p.get("lever"))
        if it is not None and lv is not None:
            iter_to_levers.setdefault(it, set()).add(lv)

    # Source 2: iterations table (correct after forward harness fix)
    for row in iterations_rows or []:
        it = _safe_int(row.get("iteration"))
        lv = _safe_int(row.get("lever"))
        if it is not None and lv is not None and lv != 0:
            iter_to_levers.setdefault(it, set()).add(lv)

    for s in stages_rows:
        stage_name = str(s.get("stage", ""))

        # Match LEVER_* stages (existing logic)
        if stage_name.startswith("LEVER_"):
            lever_num = s.get("lever")
            if lever_num is None:
                try:
                    parts = stage_name.split("_")
                    lever_num = int(parts[1])
                except (IndexError, ValueError):
                    continue
            try:
                lever_num_float = float(lever_num)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(lever_num_float):
                continue
            lever_num = int(lever_num_float)
            if lever_num not in lever_data:
                lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
            lever_data[lever_num]["stages"].append(s)
            lever_data[lever_num]["detail"].update(_parse_detail(s))
            continue

        # Match AG_* stages — map to lever via multi-source mapping
        if stage_name.startswith("AG_"):
            iteration = _safe_int(s.get("iteration"))
            if iteration is None:
                continue
            # Source 3: stage detail "levers" field (written by harness)
            detail = _parse_detail(s)
            target_levers: set[int] = set(iter_to_levers.get(iteration, set()))
            if detail and "levers" in detail:
                for lk in detail["levers"]:
                    try:
                        target_levers.add(int(lk))
                    except (TypeError, ValueError):
                        pass
            if not target_levers:
                continue
            for lever_num in target_levers:
                if lever_num not in lever_data:
                    lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
                lever_data[lever_num]["stages"].append(s)
                if detail:
                    lever_data[lever_num]["detail"].update(detail)

    for p in patches_rows or []:
        lever_num = _safe_int(p.get("lever"))
        if lever_num is None:
            continue
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
        lever_data[lever_num]["patches"].append(_patch_for_ui(p))

    levers: list[LeverStatus] = []
    for lever_num in sorted(lever_data.keys()):
        data = lever_data[lever_num]
        ld = data["detail"]
        status = _derive_lever_status(data["stages"])
        status = _normalize_lever_status_for_terminal_run(
            status=status,
            run_status=run_status,
        )
        patches_total = len(data.get("patches", []))
        detail_patch_count = _safe_int(ld.get("patches_applied"))
        patches_applied = patches_total if patches_total > 0 else (detail_patch_count or 0)

        rollback_reason = None
        if status == "rolled_back":
            rollback_reason = ld.get("reason", "regression")

        lever_iterations = _build_lever_iterations(
            lever_num=lever_num,
            lever_stages=data.get("stages", []),
            iterations_rows=iterations_rows or [],
            patches_rows=patches_rows or [],
            run_status=run_status,
            all_stages_rows=stages_rows,
        )

        levers.append(
            LeverStatus(
                lever=lever_num,
                name=LEVER_NAMES.get(lever_num, f"Lever {lever_num}"),
                status=status,
                patchCount=patches_applied,
                scoreBefore=_safe_float(ld.get("score_before")),
                scoreAfter=_safe_float(ld.get("accuracy")),
                scoreDelta=_safe_float(ld.get("score_delta")),
                rollbackReason=rollback_reason,
                patches=data.get("patches", []),
                iterations=lever_iterations,
            )
        )
    return levers


def _patch_for_ui(row: dict) -> dict[str, Any]:
    """Convert patch table row to compact UI object."""
    patch = _maybe_json(row.get("patch_json"))
    command = _maybe_json(row.get("command_json"))
    return {
        "patchType": row.get("patch_type"),
        "scope": row.get("scope"),
        "riskLevel": row.get("risk_level"),
        "targetObject": row.get("target_object"),
        "rolledBack": bool(row.get("rolled_back")) if row.get("rolled_back") is not None else False,
        "rollbackReason": row.get("rollback_reason"),
        "command": command if isinstance(command, dict) else command,
        "patch": patch if isinstance(patch, dict) else patch,
        "appliedAt": str(row.get("applied_at")) if row.get("applied_at") is not None else None,
    }


_maybe_json = safe_json_parse


def _build_lever_iterations(
    *,
    lever_num: int,
    lever_stages: list[dict],
    iterations_rows: list[dict],
    patches_rows: list[dict],
    run_status: str,
    all_stages_rows: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Build iteration-by-iteration transparency payload for one lever."""
    by_iter: dict[int, dict[str, Any]] = {}

    # Collect iterations that belong to this lever from multiple sources.
    # The iterations table may have lever=0 (hardcoded by older harness),
    # so also check the patches table which always has the correct lever.
    lever_iterations: set[int] = set()
    for row in iterations_rows:
        if _safe_int(row.get("lever")) == lever_num:
            it = _safe_int(row.get("iteration"))
            if it is not None:
                lever_iterations.add(it)

    # Guard: when building lever 0 (Proactive Enrichment), skip iterations
    # that are already assigned to a non-zero lever in the iterations table.
    # This handles old data where write_patch hardcoded lever=0 for all patches.
    _non_zero_lever_iters: set[int] = set()
    if lever_num == 0:
        for row in iterations_rows:
            lv = _safe_int(row.get("lever"))
            it = _safe_int(row.get("iteration"))
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

    # Also include AG_* stages for iterations belonging to this lever
    for stage in all_stages_rows or []:
        stage_name = str(stage.get("stage", ""))
        if not stage_name.startswith("AG_"):
            continue
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None or iteration not in lever_iterations:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        detail = _parse_detail(stage)
        if detail:
            entry["detail"].update(detail)

    # Match iteration rows — check both lever column and patches-based set
    for row in iterations_rows:
        row_lever = _safe_int(row.get("lever"))
        iteration = _safe_int(row.get("iteration"))
        if iteration is None:
            continue
        if row_lever != lever_num and iteration not in lever_iterations:
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

    iteration_payloads: list[dict[str, Any]] = []
    for iteration in sorted(by_iter.keys()):
        entry = by_iter[iteration]
        stage_status = _derive_lever_status(entry["stages"])
        status = _normalize_lever_status_for_terminal_run(status=stage_status, run_status=run_status)
        detail = entry["detail"]

        full_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "full"),
            None,
        )
        slice_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "slice"),
            None,
        )
        p0_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "p0"),
            None,
        )
        score_after = (
            _safe_float(full_row.get("overall_accuracy")) if full_row else _safe_float(detail.get("accuracy"))
        )
        score_before = _safe_float(detail.get("score_before"))
        score_delta = _safe_float(detail.get("score_delta"))
        if score_delta is None and score_before is not None and score_after is not None:
            score_delta = round(score_after - score_before, 2)

        judge_scores = _iteration_scores(full_row)
        patch_types = [str(p.get("patchType") or "") for p in entry["patches"] if p.get("patchType")]
        rollback_reason = detail.get("reason")
        if not rollback_reason and status == "rolled_back":
            rollback_reason = "regression"

        iteration_payloads.append(
            {
                "iteration": iteration,
                "status": status,
                "patchCount": len(entry["patches"]),
                "patchTypes": patch_types,
                "scoreBefore": score_before,
                "scoreAfter": score_after,
                "scoreDelta": score_delta,
                "sliceAccuracy": _safe_float(slice_row.get("overall_accuracy")) if slice_row else None,
                "p0Accuracy": _safe_float(p0_row.get("overall_accuracy")) if p0_row else None,
                "fullAccuracy": _safe_float(full_row.get("overall_accuracy")) if full_row else None,
                "totalQuestions": _safe_int(full_row.get("total_questions")) if full_row else None,
                "correctCount": _safe_int(full_row.get("correct_count")) if full_row else None,
                "judgeScores": judge_scores,
                "mlflowRunId": full_row.get("mlflow_run_id") if full_row else None,
                "rollbackReason": rollback_reason,
                "patches": entry["patches"],
                "stageEvents": _build_stage_timeline(entry["stages"]),
            }
        )

    return iteration_payloads


def _iteration_scores(iter_row: dict | None) -> dict[str, float | None]:
    """Parse and normalize per-judge score payload for one evaluation row."""
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


def _derive_lever_status(stages: list[dict]) -> str:
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


def _normalize_step_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale 'running' steps when the overall run is already terminal."""
    if status != "running":
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in {"CANCELLED", "DISCARDED"}:
        return "pending"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "completed"
    return status


def _normalize_lever_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale active lever states after the run is terminal."""
    if status not in {"running", "evaluating"}:
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in {"CANCELLED", "DISCARDED"}:
        return "skipped"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _get_baseline_and_best_accuracy(iters_rows: list[dict]) -> tuple[float | None, float | None]:
    """Return baseline(full iteration 0) and best full-eval accuracy."""
    full_rows = [r for r in iters_rows if str(r.get("eval_scope", "")).lower() == "full"]
    if not full_rows:
        return None, None

    baseline_row = next((r for r in full_rows if _safe_int(r.get("iteration")) == 0), None)
    baseline = _safe_float(baseline_row.get("overall_accuracy")) if baseline_row else None

    scores = [_safe_float(r.get("overall_accuracy")) for r in full_rows]
    finite_scores = [s for s in scores if s is not None]
    if not finite_scores:
        return baseline, None
    return baseline, max(finite_scores)


# ── Route Handlers ──────────────────────────────────────────────────────


_ACTIVE_RUN_STATUSES_LOCAL = ACTIVE_RUN_STATUSES
_TERMINAL_JOB_STATES_LOCAL = TERMINAL_JOB_STATES


def _reconcile_single_run(
    spark,
    run_data: dict,
    sp_ws: WorkspaceClient,
    catalog: str,
    schema_name: str,
) -> dict:
    """If the Delta status is active but the Databricks job has terminated,
    update Delta and return refreshed run_data so the caller sees the real state."""
    from genie_space_optimizer.optimization.state import load_run, update_run_status

    status = str(run_data.get("status") or "")
    if status not in _ACTIVE_RUN_STATUSES_LOCAL:
        return run_data

    job_run_id = run_data.get("job_run_id")
    if not job_run_id:
        return run_data

    try:
        run_obj = sp_ws.jobs.get_run(run_id=int(str(job_run_id)))
        life_cycle = (
            str(run_obj.state.life_cycle_state).split(".")[-1]
            if run_obj.state else ""
        )
        if life_cycle not in _TERMINAL_JOB_STATES_LOCAL:
            return run_data

        result_state = (
            str(run_obj.state.result_state).split(".")[-1].lower()
            if run_obj.state else ""
        )
        new_status = "CANCELLED" if result_state == "canceled" else "FAILED"
        suffix = f":{result_state}" if result_state and result_state != "none" else ""
        update_run_status(
            spark,
            str(run_data["run_id"]),
            catalog,
            schema_name,
            status=new_status,
            convergence_reason=f"job_{life_cycle.lower()}_detected_on_poll{suffix}",
        )
        logger.info(
            "Reconciled run %s → %s (job lifecycle=%s, result=%s)",
            run_data["run_id"], new_status, life_cycle, result_state,
        )
        refreshed = load_run(spark, str(run_data["run_id"]), catalog, schema_name)
        return refreshed if refreshed else run_data
    except Exception:
        logger.debug("Could not reconcile run %s with job API", run_data.get("run_id"), exc_info=True)
        return run_data


def _resolve_deployment_status(
    stages_rows: list[dict],
    run_data: dict,
    host: str,
) -> tuple[str | None, str | None]:
    """Derive deployment status and URL from DEPLOY stages.

    Returns (status, url) where status is one of:
    None — no deploy configured, SKIPPED, RUNNING, DEPLOYED,
    PENDING_APPROVAL, FAILED.
    """
    deploy_target = run_data.get("deploy_target")
    deploy_stages = [
        s for s in stages_rows
        if str(s.get("stage", "")).startswith("DEPLOY")
    ]
    if not deploy_stages:
        return (None, None) if not deploy_target else (None, None)

    latest = deploy_stages[-1]
    stage_name = str(latest.get("stage", ""))
    stage_status = str(latest.get("status", "")).upper()

    if "SKIPPED" in stage_name or stage_status == "SKIPPED":
        return ("SKIPPED", None)
    if stage_status == "FAILED":
        return ("FAILED", None)

    if "DELEGATED" in stage_name:
        uc_model = ""
        uc_version = ""
        for s in stages_rows:
            if str(s.get("stage", "")).startswith("FINALIZE") and str(s.get("status", "")).upper() == "COMPLETE":
                fin_detail = safe_json_parse(s.get("detail_json"))
                if isinstance(fin_detail, dict):
                    uc_model = fin_detail.get("uc_model_name", "")
                    uc_version = fin_detail.get("uc_model_version", "")
                break
        if uc_model and uc_version:
            parts = uc_model.split(".")
            if len(parts) == 3:
                model_url = (
                    f"{host.rstrip('/')}/explore/data/models/"
                    f"{parts[0]}/{parts[1]}/{parts[2]}/version/{uc_version}"
                )
                return ("PENDING_APPROVAL", model_url)
        return ("PENDING_APPROVAL", None)

    if stage_status == "COMPLETE":
        detail = safe_json_parse(latest.get("detail_json"))
        target = (
            detail.get("deploy_target") if isinstance(detail, dict) else None
        ) or deploy_target
        url = str(target).rstrip("/") if target else None
        return ("DEPLOYED", url)
    if stage_status == "STARTED":
        return ("RUNNING", None)

    return (None, None)


@router.get("/runs/{run_id}", response_model=PipelineRun, operation_id="getRun")
def get_run(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    session: Dependencies.Session,
):
    """Run status with 5 user-facing pipeline steps and lever detail."""
    from genie_space_optimizer.optimization.state import load_run
    from ..gso_read_through import (
        load_iterations_with_fallback,
        load_patches_with_fallback,
        load_stages_with_fallback,
    )

    spark = get_spark()
    # Always load run from Delta (authoritative for status)
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    run_data = _reconcile_single_run(spark, run_data, sp_ws, config.catalog, config.schema_name)

    # For completed runs, try Lakebase first for stages/iterations/patches
    is_active = run_data.get("status", "") in ("QUEUED", "IN_PROGRESS")

    stages_df = load_stages_with_fallback(
        session, spark, run_id, config.catalog, config.schema_name, is_active=is_active,
    )
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []

    iters_df = load_iterations_with_fallback(
        session, spark, run_id, config.catalog, config.schema_name, is_active=is_active,
    )
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []
    patches_df = load_patches_with_fallback(
        session, spark, run_id, config.catalog, config.schema_name, is_active=is_active,
    )
    patches_rows = patches_df.to_dict("records") if not patches_df.empty else []

    baseline_iter = next((r for r in iters_rows if r.get("iteration") == 0), None)
    baseline_accuracy = _safe_float(baseline_iter.get("overall_accuracy")) if baseline_iter else None
    run_data["baseline_accuracy"] = baseline_accuracy

    steps = map_stages_to_steps(stages_rows, iters_rows, run_data, patches_rows=patches_rows)
    configured_levers = run_data.get("levers", []) if isinstance(run_data.get("levers"), list) else []
    configured_lever_ints: list[int] = []
    for lever in configured_levers:
        try:
            configured_lever_ints.append(int(lever))
        except (TypeError, ValueError):
            continue
    levers = _build_levers(
        stages_rows,
        run_status=str(run_data.get("status", "")),
        configured_levers=configured_lever_ints,
        patches_rows=patches_rows,
        iterations_rows=iters_rows,
    )

    status = run_data.get("status", "QUEUED")
    display_status = status
    if status == "IN_PROGRESS":
        display_status = "RUNNING"

    host = (ws.config.host or "").rstrip("/")
    links = _build_links(host, run_data, iters_rows, config, ws, stages_rows=stages_rows)
    baseline_eval_link = next(
        (
            l.url for l in links
            if l.category == "mlflow" and "baseline" in l.label.lower()
        ),
        None,
    )
    for step in steps:
        if step.name == "Baseline Evaluation" and isinstance(step.outputs, dict) and baseline_eval_link:
            step.outputs.setdefault("evaluationRunUrl", baseline_eval_link)

    deploy_status, deploy_url = _resolve_deployment_status(
        stages_rows, run_data, host,
    )

    result = PipelineRun(
        runId=run_id,
        spaceId=run_data.get("space_id", ""),
        spaceName=run_data.get("space_name", run_data.get("domain", "")),
        status=display_status,
        startedAt=ensure_utc_iso(run_data.get("started_at")) or "",
        completedAt=ensure_utc_iso(run_data.get("completed_at")),
        initiatedBy=run_data.get("triggered_by") or "system",
        baselineScore=baseline_accuracy,
        optimizedScore=_safe_float(run_data.get("best_accuracy")),
        steps=steps,
        levers=levers,
        convergenceReason=run_data.get("convergence_reason"),
        links=links,
        deploymentJobStatus=deploy_status,
        deploymentJobUrl=deploy_url,
    )

    try:
        dumped = result.model_dump(mode="json")
        import json as _json
        _json.dumps(dumped, allow_nan=False)
        logger.info("get_run serialization OK for %s", run_id)
    except (ValueError, TypeError) as exc:
        logger.error("get_run NaN detected in model_dump for %s: %s", run_id, exc)
        logger.error("Dumped data: %s", dumped)
    return result


@router.get(
    "/runs/{run_id}/comparison",
    response_model=ComparisonData,
    operation_id="getComparison",
)
def get_comparison(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    session: Dependencies.Session,
):
    """Side-by-side original vs optimized config with per-dimension scores."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import load_run
    from ..gso_read_through import load_iterations_with_fallback

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    space_id = run_data.get("space_id", "")
    is_active = run_data.get("status", "") in ("QUEUED", "IN_PROGRESS")

    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        try:
            original_snapshot = json.loads(original_snapshot)
        except (json.JSONDecodeError, TypeError):
            original_snapshot = {}
    if not isinstance(original_snapshot, dict):
        original_snapshot = {}

    try:
        client, _ = _genie_client(ws, sp_ws)
        current_config = fetch_space_config(client, space_id)
        current_ss = current_config.get("_parsed_space", {})
    except Exception:
        current_ss = {}

    iters_df = load_iterations_with_fallback(
        session, spark, run_id, config.catalog, config.schema_name, is_active=is_active,
    )
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []

    baseline_scores = {}
    optimized_scores = {}
    baseline_accuracy = 0.0
    optimized_accuracy = 0.0

    for row in iters_rows:
        if row.get("eval_scope") != "full":
            continue
        scores_raw = row.get("scores_json", {})
        if isinstance(scores_raw, str):
            try:
                scores_raw = json.loads(scores_raw)
            except (json.JSONDecodeError, TypeError):
                scores_raw = {}
        if row.get("iteration") == 0:
            baseline_scores = scores_raw if isinstance(scores_raw, dict) else {}
            baseline_accuracy = _finite(row.get("overall_accuracy", 0))
        else:
            optimized_scores = scores_raw if isinstance(scores_raw, dict) else {}
            optimized_accuracy = _finite(row.get("overall_accuracy", 0))

    if not optimized_scores:
        optimized_scores = baseline_scores
        optimized_accuracy = baseline_accuracy

    dimensions: list[DimensionScore] = []
    all_dims = set(baseline_scores.keys()) | set(optimized_scores.keys())
    for dim in sorted(all_dims):
        b = baseline_scores.get(dim, 0.0)
        o = optimized_scores.get(dim, 0.0)
        dimensions.append(
            DimensionScore(
                dimension=dim,
                baseline=_finite(b),
                optimized=_finite(o),
                delta=_finite(o) - _finite(b),
            )
        )

    improvement_pct = 0.0
    if baseline_accuracy > 0:
        improvement_pct = ((optimized_accuracy - baseline_accuracy) / baseline_accuracy) * 100

    return ComparisonData(
        runId=run_id,
        spaceId=space_id,
        spaceName=run_data.get("domain", ""),
        baselineScore=baseline_accuracy,
        optimizedScore=optimized_accuracy,
        improvementPct=round(improvement_pct, 2),
        perDimensionScores=dimensions,
        original=_extract_space_configuration(original_snapshot),
        optimized=_extract_space_configuration(current_ss),
    )


@router.post(
    "/runs/{run_id}/apply",
    response_model=ActionResponse,
    operation_id="applyOptimization",
)
def apply_optimization(
    run_id: str,
    ws: Dependencies.UserClient,
    config: Dependencies.Config,
):
    """Confirm optimized config.

    For runs requested with deferred UC writes (apply_mode=uc_artifact|both),
    execute those writes via OBO only after:
    1) optimization is terminal, and
    2) evaluation shows an improvement over baseline.
    """
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_run,
        load_stages,
        update_run_status,
    )

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    status = run_data.get("status", "")
    terminal = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}
    if status not in terminal:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot apply run in status {status}. Must be one of: {terminal}",
        )

    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()
    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []
    baseline_score, best_score = _get_baseline_and_best_accuracy(iters_rows)

    if requested_mode in _DEFERRED_UC_MODES:
        if baseline_score is None or best_score is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Deferred UC writes require completed full-evaluation results "
                    "(baseline and optimized scores)."
                ),
            )
        if best_score <= baseline_score:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Deferred UC writes require a proven improvement in evaluation "
                    f"(baseline={baseline_score:.1f}%, optimized={best_score:.1f}%)."
                ),
            )

    stages_df = load_stages(spark, run_id, config.catalog, config.schema_name)
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []
    uc_apply_result = "not_required"
    if requested_mode in _DEFERRED_UC_MODES:
        try:
            uc_apply_result = _apply_deferred_uc_writes_obo(
                spark=spark,
                run_id=run_id,
                run_data=run_data,
                stages_rows=stages_rows,
                ws=ws,
                config=config,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    update_run_status(
        spark, run_id, config.catalog, config.schema_name,
        status="APPLIED",
    )
    message = "Optimization applied successfully."
    if requested_mode in _DEFERRED_UC_MODES:
        if uc_apply_result == "applied":
            message = "Optimization applied and deferred UC writes were executed via OBO."
        elif uc_apply_result == "already_recorded":
            message = "Optimization applied. Deferred UC write stage was already recorded."
        elif uc_apply_result.startswith("skipped_"):
            message = (
                "Optimization applied. Deferred UC writes were not needed for this run "
                f"({uc_apply_result})."
            )
    return ActionResponse(status="applied", runId=run_id, message=message)


@router.post(
    "/runs/{run_id}/discard",
    response_model=ActionResponse,
    operation_id="discardOptimization",
)
def discard_optimization(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """Discard optimization results and rollback patches to original config."""
    from genie_space_optimizer.optimization.applier import rollback
    from genie_space_optimizer.optimization.state import load_run, update_run_status

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    status = run_data.get("status", "")
    if status in ("DISCARDED", "APPLIED"):
        raise HTTPException(
            status_code=409,
            detail=f"Run already {status.lower()}.",
        )

    space_id = run_data.get("space_id", "")
    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        try:
            original_snapshot = json.loads(original_snapshot)
        except (json.JSONDecodeError, TypeError):
            original_snapshot = {}

    if original_snapshot and isinstance(original_snapshot, dict):
        apply_log = {"pre_snapshot": original_snapshot}
        client, _ = _genie_client(ws, sp_ws)
        rollback(apply_log, client, space_id)

    update_run_status(
        spark, run_id, config.catalog, config.schema_name,
        status="DISCARDED",
        convergence_reason="user_discarded",
    )

    return ActionResponse(
        status="discarded",
        runId=run_id,
        message="Optimization discarded and patches rolled back.",
    )


# ── Transparency Endpoints ──────────────────────────────────────────────


@router.get(
    "/runs/{run_id}/iterations",
    response_model=list[IterationSummary],
    operation_id="getIterations",
)
def get_iterations(run_id: str, config: Dependencies.Config):
    """All evaluation iterations for a run (baseline + lever evals)."""
    from genie_space_optimizer.optimization.state import load_iterations, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    if iters_df.empty:
        return []

    results: list[IterationSummary] = []
    for row in iters_df.to_dict("records"):
        scores = row.get("scores_json", {})
        if isinstance(scores, str):
            try:
                scores = json.loads(scores)
            except (json.JSONDecodeError, TypeError):
                scores = {}
        if not isinstance(scores, dict):
            scores = {}

        results.append(
            IterationSummary(
                iteration=_safe_int(row.get("iteration")) or 0,
                lever=_safe_int(row.get("lever")),
                evalScope=str(row.get("eval_scope", "full")).lower(),
                overallAccuracy=_finite(row.get("overall_accuracy", 0)),
                totalQuestions=_safe_int(row.get("total_questions")) or 0,
                correctCount=_safe_int(row.get("correct_count")) or 0,
                repeatabilityPct=_safe_float(row.get("repeatability_pct")),
                thresholdsMet=bool(row.get("thresholds_met", False)),
                judgeScores={str(k): _safe_float(v) for k, v in scores.items()},
            )
        )
    return results


@router.get(
    "/runs/{run_id}/asi-results",
    response_model=AsiSummary,
    operation_id="getAsiResults",
)
def get_asi_results(
    run_id: str,
    config: Dependencies.Config,
    iteration: int | None = None,
):
    """ASI judge feedback for a run, with summary statistics."""
    from genie_space_optimizer.optimization.state import load_asi_results, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    resolved_iteration = iteration if iteration is not None else 0
    df = load_asi_results(
        spark, run_id, config.catalog, config.schema_name, iteration=resolved_iteration,
    )
    if df.empty:
        return AsiSummary(
            runId=run_id, iteration=resolved_iteration,
            totalResults=0, passCount=0, failCount=0,
        )

    records = df.to_dict("records")
    results: list[AsiResult] = []
    pass_count = 0
    fail_count = 0
    failure_types: dict[str, int] = {}
    blame_counts: dict[str, int] = {}
    judge_totals: dict[str, int] = {}
    judge_passes: dict[str, int] = {}

    _PASS_VALUES = {"yes", "genie_correct", "pass", "true"}

    for row in records:
        value = str(row.get("value", "")).lower().strip()
        is_pass = value in _PASS_VALUES
        if is_pass:
            pass_count += 1
        else:
            fail_count += 1

        judge = str(row.get("judge", ""))
        judge_totals[judge] = judge_totals.get(judge, 0) + 1
        if is_pass:
            judge_passes[judge] = judge_passes.get(judge, 0) + 1

        ft = row.get("failure_type")
        if ft and not is_pass:
            ft_str = str(ft)
            failure_types[ft_str] = failure_types.get(ft_str, 0) + 1

        blame_raw = row.get("blame_set")
        blame_list: list[str] = []
        if blame_raw:
            if isinstance(blame_raw, str):
                try:
                    blame_list = json.loads(blame_raw)
                except (json.JSONDecodeError, TypeError):
                    blame_list = []
            elif isinstance(blame_raw, list):
                blame_list = [str(x) for x in blame_raw]

        results.append(
            AsiResult(
                questionId=str(row.get("question_id", "")),
                judge=judge,
                value=str(row.get("value", "")),
                failureType=row.get("failure_type"),
                severity=row.get("severity"),
                confidence=_safe_float(row.get("confidence")),
                blameSet=blame_list,
                counterfactualFix=row.get("counterfactual_fix"),
                wrongClause=row.get("wrong_clause"),
                expectedValue=row.get("expected_value"),
                actualValue=row.get("actual_value"),
            )
        )

    judge_pass_rates = {
        j: round((judge_passes.get(j, 0) / judge_totals[j]) * 100, 1)
        for j in sorted(judge_totals)
        if judge_totals[j] > 0
    }

    return AsiSummary(
        runId=run_id,
        iteration=resolved_iteration,
        totalResults=len(records),
        passCount=pass_count,
        failCount=fail_count,
        failureTypeDistribution=failure_types,
        blameDistribution=blame_counts,
        judgePassRates=judge_pass_rates,
        results=results,
    )


@router.get(
    "/runs/{run_id}/provenance",
    response_model=list[ProvenanceSummary],
    operation_id="getProvenance",
)
def get_provenance(
    run_id: str,
    config: Dependencies.Config,
    iteration: int | None = None,
    lever: int | None = None,
):
    """Provenance lineage for a run, grouped by iteration+lever."""
    from genie_space_optimizer.optimization.state import load_provenance, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    df = load_provenance(
        spark, run_id, config.catalog, config.schema_name,
        iteration=iteration, lever=lever,
    )
    if df.empty:
        return []

    groups: dict[tuple[int, int], list[dict]] = {}
    for row in df.to_dict("records"):
        key = (_safe_int(row.get("iteration")) or 0, _safe_int(row.get("lever")) or 0)
        groups.setdefault(key, []).append(row)

    summaries: list[ProvenanceSummary] = []
    for (iter_num, lever_num), rows in sorted(groups.items()):
        prov_records: list[ProvenanceRecord] = []
        root_causes: dict[str, int] = {}
        gate_results: dict[str, int] = {}
        clusters: set[str] = set()
        proposals: set[str] = set()

        for row in rows:
            rc = str(row.get("resolved_root_cause", ""))
            if rc:
                root_causes[rc] = root_causes.get(rc, 0) + 1

            gr = row.get("gate_result")
            if gr:
                gr_str = str(gr)
                gate_results[gr_str] = gate_results.get(gr_str, 0) + 1

            cid = str(row.get("cluster_id", ""))
            if cid:
                clusters.add(cid)

            pid = row.get("proposal_id")
            if pid:
                proposals.add(str(pid))

            blame_raw = row.get("blame_set")
            blame_list: list[str] = []
            if blame_raw:
                if isinstance(blame_raw, str):
                    try:
                        blame_list = json.loads(blame_raw)
                    except (json.JSONDecodeError, TypeError):
                        blame_list = []
                elif isinstance(blame_raw, list):
                    blame_list = [str(x) for x in blame_raw]

            prov_records.append(
                ProvenanceRecord(
                    questionId=str(row.get("question_id", "")),
                    signalType=str(row.get("signal_type", "")),
                    judge=str(row.get("judge", "")),
                    judgeVerdict=str(row.get("judge_verdict", "")),
                    resolvedRootCause=rc,
                    resolutionMethod=str(row.get("resolution_method", "")),
                    blameSet=blame_list,
                    counterfactualFix=row.get("counterfactual_fix"),
                    clusterId=cid,
                    proposalId=row.get("proposal_id"),
                    patchType=row.get("patch_type"),
                    gateType=row.get("gate_type"),
                    gateResult=row.get("gate_result"),
                )
            )

        summaries.append(
            ProvenanceSummary(
                runId=run_id,
                iteration=iter_num,
                lever=lever_num,
                totalRecords=len(prov_records),
                clusterCount=len(clusters),
                proposalCount=len(proposals),
                rootCauseDistribution=root_causes,
                gateResults=gate_results,
                records=prov_records,
            )
        )

    return summaries


@router.get(
    "/runs/{run_id}/iteration-detail",
    response_model=IterationDetailResponse,
    operation_id="getIterationDetail",
)
def get_iteration_detail(run_id: str, config: Dependencies.Config):
    """Comprehensive per-iteration breakdown for the transparency pane."""
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_patches,
        load_run,
        load_stages,
    )

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []

    stages_df = load_stages(spark, run_id, config.catalog, config.schema_name)
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []

    patches_df = load_patches(spark, run_id, config.catalog, config.schema_name)
    patches_rows = patches_df.to_dict("records") if not patches_df.empty else []

    ag_stages: dict[str, dict] = {}
    for s in stages_rows:
        stage_name = str(s.get("stage", ""))
        if stage_name.startswith("AG_") and "_STARTED" in stage_name:
            ag_id = stage_name.replace("AG_", "").replace("_STARTED", "")
            iteration = s.get("iteration")
            detail = safe_json_parse(s.get("detail_json"))
            status_val = str(s.get("status", "")).upper()
            entry = ag_stages.setdefault(ag_id, {})
            if status_val == "STARTED":
                entry["cluster_info"] = detail if isinstance(detail, dict) else {}
                entry["iteration"] = iteration
            if status_val == "COMPLETE":
                entry["accepted"] = True
                entry.setdefault("iteration", iteration)
                if isinstance(detail, dict):
                    entry.setdefault("cluster_info", {}).update(detail)
            if status_val == "ROLLED_BACK":
                entry["accepted"] = False
                entry.setdefault("iteration", iteration)
                if isinstance(detail, dict):
                    entry["rollback_detail"] = detail
        elif stage_name.startswith("AG_") and "_PATCH_FAILED" in stage_name:
            ag_id = stage_name.replace("AG_", "").replace("_PATCH_FAILED", "")
            entry = ag_stages.setdefault(ag_id, {})
            entry.setdefault("iteration", s.get("iteration"))
            entry["accepted"] = False
            entry["patch_failed"] = True
            entry["patch_error"] = str(s.get("error_message", ""))[:500]
        elif stage_name.startswith("AG_") and "_ESCALATION" in stage_name:
            ag_id = stage_name.replace("AG_", "").replace("_ESCALATION", "")
            detail = safe_json_parse(s.get("detail_json"))
            entry = ag_stages.setdefault(ag_id, {})
            entry.setdefault("iteration", s.get("iteration"))
            if isinstance(detail, dict):
                entry["escalation"] = {
                    "type": detail.get("escalation_type", ""),
                    "handled": detail.get("handled", False),
                    "detail": detail.get("detail", {}),
                    "affectedQuestions": detail.get("affected_questions", []),
                }

    full_rows = [r for r in iters_rows if str(r.get("eval_scope", "")).lower() == "full"]
    slice_rows = [r for r in iters_rows if str(r.get("eval_scope", "")).lower() == "slice"]
    p0_rows = [r for r in iters_rows if str(r.get("eval_scope", "")).lower() == "p0"]

    by_iter: dict[int, dict] = {}
    for row in full_rows:
        it = _safe_int(row.get("iteration")) or 0
        by_iter.setdefault(it, {"full": row})
        by_iter[it]["full"] = row
    for row in slice_rows:
        it = _safe_int(row.get("iteration")) or 0
        by_iter.setdefault(it, {})
        by_iter[it]["slice"] = row
    for row in p0_rows:
        it = _safe_int(row.get("iteration")) or 0
        by_iter.setdefault(it, {})
        by_iter[it]["p0"] = row

    patches_by_iter: dict[int, list[dict]] = {}
    for p in patches_rows:
        it = _safe_int(p.get("iteration"))
        if it is not None:
            patches_by_iter.setdefault(it, []).append(p)

    ag_iter_map: dict[int, str] = {}
    for ag_id, ag_data in ag_stages.items():
        it = _safe_int(ag_data.get("iteration"))
        if it is not None:
            ag_iter_map[it] = ag_id

    iterations: list[IterationDetail] = []
    for it_num in sorted(by_iter.keys()):
        data = by_iter[it_num]
        full_row = data.get("full")
        if not full_row:
            continue

        scores = _iteration_scores(full_row)
        accuracy = _finite(full_row.get("overall_accuracy", 0))
        total_q = _safe_int(full_row.get("total_questions")) or 0
        correct_c = _safe_int(full_row.get("correct_count")) or 0
        mlflow_rid = full_row.get("mlflow_run_id")
        model_id = full_row.get("model_id")
        timestamp = str(full_row.get("timestamp", ""))

        ag_id = ag_iter_map.get(it_num)
        ag_info = ag_stages.get(ag_id, {}) if ag_id else {}
        cluster_info = ag_info.get("cluster_info") if ag_info else None
        if cluster_info and ag_info.get("escalation"):
            cluster_info["escalation"] = ag_info["escalation"]

        if it_num == 0:
            status = "baseline"
        elif ag_info.get("accepted") is True:
            status = "accepted"
        elif ag_info.get("accepted") is False:
            status = "rolled_back"
        else:
            status = "completed"

        gates: list[GateResult] = []
        slice_row = data.get("slice")
        if slice_row:
            gates.append(GateResult(
                gateName="slice",
                accuracy=_safe_float(slice_row.get("overall_accuracy")),
                totalQuestions=_safe_int(slice_row.get("total_questions")),
                passed=status != "rolled_back" or not str(ag_info.get("rollback_detail", {}).get("reason", "")).startswith("slice"),
                mlflowRunId=slice_row.get("mlflow_run_id"),
            ))
        p0_row = data.get("p0")
        if p0_row:
            gates.append(GateResult(
                gateName="p0",
                accuracy=_safe_float(p0_row.get("overall_accuracy")),
                totalQuestions=_safe_int(p0_row.get("total_questions")),
                passed=status != "rolled_back" or not str(ag_info.get("rollback_detail", {}).get("reason", "")).startswith("p0"),
                mlflowRunId=p0_row.get("mlflow_run_id"),
            ))
        if full_row and it_num > 0:
            gates.append(GateResult(
                gateName="full",
                accuracy=_safe_float(full_row.get("overall_accuracy")),
                totalQuestions=_safe_int(full_row.get("total_questions")),
                passed=status == "accepted",
                mlflowRunId=mlflow_rid,
            ))

        iter_patches = [_patch_for_ui(p) for p in patches_by_iter.get(it_num, [])]

        reflection: ReflectionEntry | None = None
        refl_raw = safe_json_parse(full_row.get("reflection_json"))
        if isinstance(refl_raw, dict) and refl_raw:
            reflection = ReflectionEntry(
                iteration=refl_raw.get("iteration", it_num),
                agId=refl_raw.get("ag_id", ag_id or ""),
                accepted=refl_raw.get("accepted", status == "accepted"),
                action=refl_raw.get("action", ""),
                levers=refl_raw.get("levers", []),
                targetObjects=refl_raw.get("target_objects", []),
                scoreDeltas=refl_raw.get("score_deltas", {}),
                accuracyDelta=refl_raw.get("accuracy_delta", 0),
                newFailures=refl_raw.get("new_failures"),
                rollbackReason=refl_raw.get("rollback_reason"),
                doNotRetry=refl_raw.get("do_not_retry", []),
                affectedQuestionIds=refl_raw.get("affected_question_ids", []),
                fixedQuestions=refl_raw.get("fixed_questions", []),
                stillFailing=refl_raw.get("still_failing", []),
                newRegressions=refl_raw.get("new_regressions", []),
                reflectionText=refl_raw.get("reflection_text", ""),
                refinementMode=refl_raw.get("refinement_mode", ""),
            )

        questions: list[QuestionResult] = []
        rows_json = safe_json_parse(full_row.get("rows_json"))
        if isinstance(rows_json, list):
            for row in rows_json:
                if not isinstance(row, dict):
                    continue
                q_text = str(
                    row.get("inputs/question")
                    or (row.get("inputs") or {}).get("question", "")
                    or row.get("question")
                    or row.get("question_text")
                    or ""
                ).strip()

                _rq = row.get("request") or {}
                if isinstance(_rq, str):
                    try:
                        _rq = json.loads(_rq)
                    except (json.JSONDecodeError, TypeError):
                        _rq = {}
                _rqk = _rq.get("kwargs", {}) if isinstance(_rq, dict) else {}

                if not q_text:
                    q_text = str(
                        _rqk.get("question")
                        or (_rq.get("question") if isinstance(_rq, dict) else None)
                        or ""
                    ).strip()

                q_id = str(
                    row.get("inputs/question_id")
                    or (row.get("inputs") or {}).get("question_id", "")
                    or row.get("question_id")
                    or row.get("request_id")
                    or _rqk.get("question_id")
                    or (_rq.get("question_id") if isinstance(_rq, dict) else None)
                    or ""
                )
                if not q_id and q_text:
                    q_id = q_text[:80]

                verdicts: dict[str, str] = {}
                failure_types: list[str] = []
                for key, val in row.items():
                    if key.endswith("/value") and "/" in key:
                        judge_name = key.rsplit("/value", 1)[0]
                        verdicts[judge_name] = str(val)

                rc = row.get("result_correctness/value", row.get("result_correctness"))
                if rc:
                    verdicts.setdefault("result_correctness", str(rc))

                match_type = None
                gen_sql = None
                expected_sql = None
                if isinstance(row.get("outputs"), dict):
                    comp = row["outputs"].get("comparison", {})
                    if isinstance(comp, dict):
                        match_type = comp.get("match_type")
                    gen_sql = str(row["outputs"].get("generated_sql", ""))[:500] or None
                    expected_sql = str(row["outputs"].get("expected_sql", ""))[:500] or None

                questions.append(QuestionResult(
                    questionId=q_id,
                    question=q_text,
                    resultCorrectness=verdicts.get("result_correctness"),
                    judgeVerdicts=verdicts,
                    failureTypes=failure_types,
                    matchType=match_type,
                    expectedSql=expected_sql,
                    generatedSql=gen_sql,
                ))

        iterations.append(IterationDetail(
            iteration=it_num,
            agId=ag_id,
            status=status,
            overallAccuracy=accuracy,
            judgeScores=scores,
            totalQuestions=total_q,
            correctCount=correct_c,
            mlflowRunId=mlflow_rid,
            modelId=model_id,
            gates=gates,
            patches=iter_patches,
            reflection=reflection,
            questions=questions,
            clusterInfo=cluster_info if isinstance(cluster_info, dict) else None,
            timestamp=timestamp,
        ))

    existing_iter_nums = {it.iteration for it in iterations}
    for ag_id, ag_data in ag_stages.items():
        if not ag_data.get("patch_failed"):
            continue
        it_num = _safe_int(ag_data.get("iteration"))
        if it_num is None or it_num in existing_iter_nums:
            continue
        cluster_info = dict(ag_data.get("cluster_info") or {})
        cluster_info["patch_error"] = ag_data.get("patch_error", "")
        iterations.append(IterationDetail(
            iteration=it_num,
            agId=ag_id,
            status="patch_failed",
            overallAccuracy=0,
            totalQuestions=0,
            correctCount=0,
            clusterInfo=cluster_info,
            patches=[_patch_for_ui(p) for p in patches_by_iter.get(it_num, [])],
        ))
    iterations.sort(key=lambda it: it.iteration)

    baseline_score, optimized_score = _get_baseline_and_best_accuracy(iters_rows)

    flagged: list[dict] = []
    domain = run_data.get("domain", run_data.get("space_id", ""))
    try:
        from genie_space_optimizer.optimization.labeling import get_flagged_questions
        flagged = get_flagged_questions(
            spark, config.catalog, config.schema_name, domain,
            run_id=run_id,
        )
    except Exception:
        logger.debug("Could not load flagged questions for iteration detail", exc_info=True)

    labeling_url = run_data.get("labeling_session_url")

    proactive = _extract_proactive_changes(stages_rows)

    return IterationDetailResponse(
        runId=run_id,
        spaceId=run_data.get("space_id", ""),
        baselineScore=baseline_score,
        optimizedScore=optimized_score,
        totalIterations=len(iterations),
        iterations=iterations,
        flaggedQuestions=flagged,
        labelingSessionUrl=labeling_url,
        proactiveChanges=proactive if proactive else None,
    )


# ── Helpers ─────────────────────────────────────────────────────────────


def _build_links(
    host: str,
    run_data: dict,
    iters_rows: list[dict],
    config,
    ws: WorkspaceClient,
    stages_rows: list[dict] | None = None,
) -> list[PipelineLink]:
    """Build external Databricks links from run metadata."""
    links: list[PipelineLink] = []
    if not host:
        return links

    space_id = run_data.get("space_id")
    if space_id:
        links.append(PipelineLink(
            label="Genie Space",
            url=f"{host}/genie/rooms/{space_id}",
            category="genie",
        ))

    job_run_id = run_data.get("job_run_id")
    if job_run_id and job_run_id not in ("pending", ""):
        stored_job_id = run_data.get("job_id")
        resolved_job_id: int | None = int(str(stored_job_id)) if stored_job_id else None

        if resolved_job_id is None:
            try:
                run = ws.jobs.get_run(run_id=int(str(job_run_id)))
                if run.job_id is not None:
                    resolved_job_id = int(run.job_id)
            except Exception:
                pass

        if resolved_job_id is not None:
            workspace_id = ws.get_workspace_id()
            job_url = (
                f"{host}/jobs/{resolved_job_id}/runs/{int(str(job_run_id))}"
                f"?o={workspace_id}"
            )
            links.append(PipelineLink(
                label="Optimization Job Run",
                url=job_url,
                category="job",
            ))

    experiment_id = run_data.get("experiment_id")
    experiment_name = run_data.get("experiment_name")
    _has_mlflow_runs = any(row.get("mlflow_run_id") for row in iters_rows)
    if experiment_id and _has_mlflow_runs:
        links.append(PipelineLink(
            label="MLflow Experiment",
            url=f"{host}/ml/experiments/{experiment_id}",
            category="mlflow",
        ))
    elif experiment_name and _has_mlflow_runs:
        from urllib.parse import quote
        links.append(PipelineLink(
            label="MLflow Experiment",
            url=f"{host}/ml/experiments?searchFilter={quote(experiment_name)}",
            category="mlflow",
        ))

    for row in iters_rows:
        mlflow_run_id = row.get("mlflow_run_id")
        iteration = row.get("iteration")
        if mlflow_run_id and experiment_id:
            label = "Baseline Evaluation" if iteration == 0 else f"Iteration {iteration} Evaluation"
            links.append(PipelineLink(
                label=label,
                url=f"{host}/ml/experiments/{experiment_id}/runs/{mlflow_run_id}",
                category="mlflow",
            ))

    uc_model = ""
    uc_version = ""
    for s in (stages_rows or []):
        if str(s.get("stage", "")).startswith("FINALIZE") and str(s.get("status", "")).upper() == "COMPLETE":
            fin_detail = safe_json_parse(s.get("detail_json"))
            if isinstance(fin_detail, dict):
                uc_model = fin_detail.get("uc_model_name", "")
                uc_version = fin_detail.get("uc_model_version", "")
            break
    if uc_model and uc_version:
        parts = uc_model.split(".")
        if len(parts) == 3:
            links.append(PipelineLink(
                label="Best Model",
                url=f"{host}/explore/data/models/{parts[0]}/{parts[1]}/{parts[2]}/version/{uc_version}",
                category="mlflow",
            ))

    labeling_url = run_data.get("labeling_session_url")
    labeling_name = run_data.get("labeling_session_name")
    if labeling_url:
        links.append(PipelineLink(
            label=f"Human Review: {labeling_name}" if labeling_name else "Human Review Session",
            url=labeling_url,
            category="review",
        ))
    elif labeling_name and experiment_id:
        links.append(PipelineLink(
            label=f"Human Review: {labeling_name}",
            url=f"{host}/ml/experiments/{experiment_id}",
            category="review",
        ))

    catalog = config.catalog
    schema = config.schema_name
    if catalog and schema:
        links.append(PipelineLink(
            label="Runs Table",
            url=f"{host}/explore/data/{catalog}/{schema}/genie_opt_runs",
            category="data",
        ))
        links.append(PipelineLink(
            label="Iterations Table",
            url=f"{host}/explore/data/{catalog}/{schema}/genie_opt_iterations",
            category="data",
        ))

    return links


def _extract_space_configuration(ss: dict) -> SpaceConfiguration:
    """Extract SpaceConfiguration from a serialized_space dict."""
    if not ss or not isinstance(ss, dict):
        return SpaceConfiguration(instructions="", sampleQuestions=[], tableDescriptions=[])

    def _to_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return "\n".join(part for part in (_to_text(v) for v in value) if part)
        if isinstance(value, dict):
            if "question" in value:
                return _to_text(value.get("question"))
            if "content" in value:
                return _to_text(value.get("content"))
            return _to_text(list(value.values()))
        return str(value)

    instr = ss.get("instructions", {})
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instructions_str = ""
    if text_instr and isinstance(text_instr, list) and text_instr:
        content = text_instr[0].get("content", [])
        instructions_str = _to_text(content)

    example_qs = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    questions: list[str] = []
    for q in example_qs:
        q_val = q.get("question", "") if isinstance(q, dict) else q
        q_text = _to_text(q_val).strip()
        if q_text:
            questions.append(q_text)

    ds = ss.get("data_sources", {})
    tables = ds.get("tables", []) if isinstance(ds, dict) else []
    table_descs: list[TableDescription] = []
    for t in tables:
        ident = t.get("identifier", "")
        desc_str = _to_text(t.get("description", [])).strip()
        table_descs.append(TableDescription(tableName=ident, description=desc_str))

    return SpaceConfiguration(
        instructions=instructions_str,
        sampleQuestions=questions,
        tableDescriptions=table_descs,
    )


_safe_float = safe_float
_safe_int = safe_int
_finite = safe_finite


# ═══════════════════════════════════════════════════════════════════════
# Pending Reviews
# ═══════════════════════════════════════════════════════════════════════

from ..models import PendingReviewItem, PendingReviewsOut


@router.get(
    "/pending-reviews/{space_id}",
    response_model=PendingReviewsOut,
    operation_id="getPendingReviews",
)
def get_pending_reviews(space_id: str, config: Dependencies.Config) -> PendingReviewsOut:
    """Return counts and details of items awaiting human review for a space."""
    spark = get_spark()
    if not spark:
        return PendingReviewsOut()

    catalog = config.catalog
    schema = config.schema_name

    items: list[PendingReviewItem] = []
    flagged_count = 0
    queued_count = 0
    labeling_url: str | None = None

    try:
        from genie_space_optimizer.optimization.labeling import get_flagged_questions
        flagged = get_flagged_questions(spark, catalog, schema, space_id)
        flagged_count = len(flagged)
        for f in flagged[:5]:
            items.append(PendingReviewItem(
                questionId=f.get("question_id", ""),
                questionText=f.get("question_text", "")[:200],
                reason=f.get("flag_reason", ""),
                itemType="flagged_question",
            ))
    except Exception:
        logger.debug("Could not load flagged questions", exc_info=True)

    try:
        from genie_space_optimizer.optimization.state import get_queued_patches
        queued = get_queued_patches(spark, catalog, schema)
        queued_count = len(queued)
        for q in queued[:5 - len(items)]:
            items.append(PendingReviewItem(
                questionId=q.get("target_identifier", ""),
                reason=f"Queued {q.get('patch_type', '')}",
                confidenceTier=q.get("confidence_tier", ""),
                itemType="queued_patch",
            ))
    except Exception:
        logger.debug("Could not load queued patches", exc_info=True)

    try:
        from genie_space_optimizer.optimization.state import run_query
        fqn = f"{catalog}.{schema}.genie_opt_runs"
        df = run_query(
            spark,
            f"SELECT labeling_session_url FROM {fqn} "
            f"WHERE space_id = '{space_id}' AND labeling_session_url IS NOT NULL "
            f"ORDER BY started_at DESC LIMIT 1",
        )
        if not df.empty:
            labeling_url = df.iloc[0].get("labeling_session_url")
    except Exception:
        logger.debug("Could not load labeling session URL", exc_info=True)

    return PendingReviewsOut(
        flaggedQuestions=flagged_count,
        queuedPatches=queued_count,
        totalPending=flagged_count + queued_count,
        labelingSessionUrl=labeling_url,
        items=items,
    )
