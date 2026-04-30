"""
Delta-backed state machine for Genie Space optimization runs.

Persists every stage transition, iteration score, and patch record
across 5 Delta tables. Both the optimization harness (writer) and
the FastAPI backend (reader) depend on this module.

All functions accept ``spark``, ``catalog``, and ``schema`` as explicit
arguments — no globals.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pandas as pd

from genie_space_optimizer.common.config import (
    TABLE_ASI,
    TABLE_FINALIZE_ATTESTATION,
    TABLE_ITERATIONS,
    TABLE_PATCHES,
    TABLE_PROVENANCE,
    TABLE_RUNS,
    TABLE_STAGES,
    TABLE_SUGGESTIONS,
)
from genie_space_optimizer.common.delta_helpers import (
    _fqn,
    execute_delta_write_with_retry,
    insert_row,
    is_retryable_delta_write_conflict,
    read_table,
    run_query,
    update_row,
)
from genie_space_optimizer.optimization.ddl import (
    TABLE_DATA_ACCESS_GRANTS,
    TABLE_GT_CORRECTION_CANDIDATES,
    TABLE_HUMAN_REQUIRED,
    TABLE_LEVER_LOOP_DECISIONS,
    TABLE_PROACTIVE_CORPUS_PROFILE,
    TABLE_PROACTIVE_PATCHES,
    TABLE_QUESTION_REGRESSIONS,
    _ALL_DDL,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ── Table Bootstrapping ─────────────────────────────────────────────────


def ensure_optimization_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create all optimization Delta tables if they don't exist (idempotent)."""
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception as exc:
        exc_str = str(exc)
        if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
            logger.warning(
                "Cannot CREATE SCHEMA %s.%s (permission denied) — "
                "assuming it already exists and continuing with table creation.",
                catalog, schema,
            )
        else:
            raise

    for name, ddl in _ALL_DDL.items():
        resolved = ddl.replace("{catalog}", catalog).replace("{schema}", schema)
        try:
            spark.sql(resolved)
            logger.info("  [OK] %s.%s.%s", catalog, schema, name)
        except Exception as exc:
            exc_str = str(exc)
            if "PERMISSION_DENIED" in exc_str or "ACCESS_DENIED" in exc_str:
                logger.warning(
                    "Cannot create table %s.%s.%s (permission denied) — "
                    "it may already exist or SP lacks CREATE_TABLE.",
                    catalog, schema, name,
                )
            elif "SCHEMA_NOT_FOUND" in exc_str:
                logger.error(
                    "Schema %s.%s does not exist and could not be created. "
                    "Create it manually: CREATE SCHEMA %s.%s",
                    catalog, schema, catalog, schema,
                )
                raise
            else:
                raise

    _migrate_add_columns(spark, catalog, schema)


def _try_enable_column_defaults(spark: SparkSession, fqn: str) -> None:
    """Best-effort upgrade of the table to support inline ``DEFAULT`` values.

    Required so subsequent ``ALTER TABLE … ALTER COLUMN … SET DEFAULT``
    statements (issued by ``_apply_one_migration``) actually stick on
    existing tables created before the DDL opted into the feature.
    Failures are non-fatal: the DEFAULT-stripping fallback in
    ``_apply_one_migration`` already handles tables without the feature,
    and writers pass values explicitly.
    """
    try:
        spark.sql(
            f"ALTER TABLE {fqn} SET TBLPROPERTIES "
            "('delta.feature.allowColumnDefaults' = 'supported')"
        )
        logger.debug("Enabled allowColumnDefaults on %s", fqn)
    except Exception as exc:
        logger.debug(
            "Could not enable allowColumnDefaults on %s "
            "(continuing — DEFAULT-stripping fallback will be used): %s",
            fqn, exc,
        )


def _migrate_add_columns(spark: SparkSession, catalog: str, schema: str) -> None:
    """Add columns introduced after initial DDL (safe to run repeatedly)."""
    _try_enable_column_defaults(spark, _fqn(catalog, schema, TABLE_ITERATIONS))

    migrations = [
        (TABLE_RUNS, "job_id", "STRING COMMENT 'Databricks Job definition ID'"),
        (TABLE_PATCHES, "provenance_json", "STRING COMMENT 'JSON: full provenance chain from judge verdicts to this patch'"),
        (TABLE_ASI, "mlflow_run_id", "STRING COMMENT 'MLflow run ID from the evaluation that produced this ASI row'"),
        (TABLE_RUNS, "labeling_session_name", "STRING COMMENT 'MLflow labeling session name for human review'"),
        (TABLE_RUNS, "labeling_session_run_id", "STRING COMMENT 'MLflow run ID associated with the labeling session'"),
        (TABLE_RUNS, "labeling_session_url", "STRING COMMENT 'URL to the MLflow Review App labeling session'"),
        (TABLE_ITERATIONS, "reflection_json", "STRING COMMENT 'JSON: adaptive loop reflection entry for this iteration'"),
        (TABLE_DATA_ACCESS_GRANTS, "grant_type", "STRING DEFAULT 'read' COMMENT 'read|write — read grants SELECT/EXECUTE, write adds MODIFY'"),
        (TABLE_ITERATIONS, "evaluated_count", "INT COMMENT 'Denominator of overall_accuracy (total_questions minus runtime exclusions; see Bug #2 denominator contract)'"),
        (TABLE_ITERATIONS, "excluded_count", "INT COMMENT 'Number of rows removed from the denominator at runtime (ground-truth excluded, both empty, Genie unavailable, temporally stale, etc.)'"),
        (TABLE_ITERATIONS, "quarantined_benchmarks_json", "STRING COMMENT 'JSON: array of benchmarks removed by pre-evaluation quarantine ({question_id, reason_code, reason_detail, question})'"),
        (TABLE_ITERATIONS, "leakage_count_by_type", "STRING COMMENT 'JSON MAP<STRING,BIGINT>: Bug #4 - persisted leak count grouped by patch_type, measured by post-apply audit'"),
        (TABLE_ITERATIONS, "firewall_rejection_count_by_type", "STRING COMMENT 'JSON MAP<STRING,BIGINT>: Bug #4 - firewall rejections during this iteration grouped by patch_type'"),
        (TABLE_ITERATIONS, "secondary_mining_blocked", "BIGINT COMMENT 'Bug #4 - count of times the _resolve_lever5_llm_result secondary mining path was blocked this iteration'"),
        (TABLE_ITERATIONS, "synthesis_slots_persisted", "BIGINT COMMENT 'Bug #4 (Phase 3) - structurally-synthesized example_sqls persisted this iteration'"),
        (TABLE_ITERATIONS, "arbiter_rejection_count", "BIGINT COMMENT 'Bug #4 (Phase 3) - synthesis proposals rejected by the arbiter gate this iteration'"),
        (TABLE_ITERATIONS, "cluster_fallback_to_instruction_count", "BIGINT COMMENT 'Bug #4 (Phase 3) - clusters that fell back to instruction-only after synthesis failed repeatedly'"),
        (TABLE_ITERATIONS, "synthesis_archetype_distribution", "STRING COMMENT 'JSON MAP<STRING,BIGINT>: Bug #4 (Phase 3) - count of persisted synthesized example_sqls per archetype this iteration'"),
        (TABLE_ITERATIONS, "rolled_back", "BOOLEAN DEFAULT false COMMENT 'Tier 1.1: true if this iteration was rolled back by the accept/rollback gate. Readers that represent current state must filter this out (see _get_baseline_and_best_accuracy, promote_best_model, load_latest_full_iteration).'"),
        (TABLE_ITERATIONS, "rolled_back_at", "TIMESTAMP COMMENT 'Tier 1.1: timestamp of rollback'"),
        (TABLE_ITERATIONS, "rollback_reason", "STRING COMMENT 'Tier 1.1: human-readable rollback reason (mirrors genie_opt_patches.rollback_reason)'"),
        (TABLE_ITERATIONS, "both_correct_count", "INT COMMENT 'Tier 1.7: count of rows with arbiter verdict == both_correct. Used to anchor best_accuracy to both_correct_rate when rc=yes overrides inflate overall_accuracy.'"),
        (TABLE_ITERATIONS, "both_correct_rate", "DOUBLE COMMENT 'Tier 1.7: both_correct_count / evaluated_count * 100. Stricter than overall_accuracy (which counts arbiter override rows as correct). Lever loop anchors acceptance to this to avoid ghost-ceiling rejections.'"),
        (TABLE_PATCHES, "applied_patch_type", "STRING COMMENT 'T2.13: actual patch_type that was applied after any applier-side transformations (e.g. update_instruction_section emitted by the rewrite_instruction downgrade splitter). May differ from patch_type (the proposal type).'"),
        (TABLE_PATCHES, "applied_patch_detail", "STRING COMMENT 'T2.13: human-readable detail describing the applied transformation (e.g. section_name for update_instruction_section, or a note when a rewrite_instruction was split into children).'"),
    ]
    for table, col, col_def in migrations:
        fqn = _fqn(catalog, schema, table)
        try:
            existing = {
                row["col_name"].lower()
                for row in spark.sql(f"DESCRIBE TABLE {fqn}").collect()
            }
        except Exception:
            existing = set()

        if col.lower() in existing:
            print(f"  [SKIP] {fqn}.{col} already exists")
            continue

        _apply_one_migration(spark, fqn=fqn, col=col, col_def=col_def)

    _verify_required_columns(spark, catalog, schema)


# Match ``DEFAULT '<string>'`` (single-quoted literal) OR
# ``DEFAULT <bare-literal>`` (e.g. ``false``, ``0``, ``NULL``, ``1.5``,
# ``CURRENT_TIMESTAMP``). The DEFAULT must be stripped from the
# ``ADD COLUMN`` statement so ADD succeeds even on Delta tables that
# do not advertise the ``allowColumnDefaults`` table feature — once the
# column is created, we apply the DEFAULT in a separate
# ``ALTER COLUMN … SET DEFAULT`` that is allowed to fail without
# leaving the schema in a broken state (writers pass the value
# explicitly anyway; see ``write_iteration``).
_DEFAULT_RE_PATTERN = r"\bDEFAULT\s+(?:'[^']*'|[A-Za-z0-9_\-.+]+)"

import re as _re

_default_re = _re.compile(_DEFAULT_RE_PATTERN, _re.IGNORECASE)


def _apply_one_migration(spark, *, fqn: str, col: str, col_def: str) -> None:
    """Add a single column to an existing table, handling DEFAULTs safely.

    Splits the migration into two steps so that ``ADD COLUMN`` does not
    fail on Delta tables that reject inline DEFAULT values. The DEFAULT
    (if any) is applied in a separate ``ALTER COLUMN … SET DEFAULT``;
    failures there are warnings, not errors — the column still exists
    and writers that provide a value explicitly (e.g. ``write_iteration``)
    continue to work.
    """
    default_match = _default_re.search(col_def)
    add_def = _default_re.sub("", col_def).strip() if default_match else col_def

    try:
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMN {col} {add_def}")
        print(f"  [MIGRATED] Added {fqn}.{col}")
    except Exception as exc:
        msg = str(exc).lower()
        if "already exists" in msg:
            print(f"  [SKIP] {fqn}.{col} already exists")
            return
        logger.error(
            "  [MIGRATION FAILED] Could not ADD COLUMN %s.%s: %s",
            fqn, col, exc,
        )
        return

    if default_match:
        try:
            spark.sql(
                f"ALTER TABLE {fqn} ALTER COLUMN {col} SET {default_match.group()}"
            )
        except Exception as exc:
            logger.warning(
                "  [WARN] Column %s.%s added, but SET DEFAULT was rejected "
                "(continuing — writers set the value explicitly): %s",
                fqn, col, exc,
            )


# Columns that writers reference by name in their ``INSERT`` statements.
# If any of these are missing after the migration loop, subsequent writes
# will fail deep in the call stack with ``UNRESOLVED_COLUMN`` — which is
# hard to diagnose. Validate up front and log a clear, loud error.
_REQUIRED_ITERATION_COLUMNS = (
    "rolled_back",
    "rolled_back_at",
    "rollback_reason",
    "both_correct_count",
    "both_correct_rate",
    "evaluated_count",
    "excluded_count",
    "quarantined_benchmarks_json",
    "leakage_count_by_type",
    "firewall_rejection_count_by_type",
    "secondary_mining_blocked",
    "synthesis_slots_persisted",
    "arbiter_rejection_count",
    "cluster_fallback_to_instruction_count",
    "synthesis_archetype_distribution",
    "reflection_json",
)


def _verify_required_columns(spark, catalog: str, schema: str) -> None:
    """Verify columns that writers rely on are actually present.

    Called at the end of ``_migrate_add_columns`` so schema drift is
    surfaced immediately instead of causing ``UNRESOLVED_COLUMN`` later
    during ``write_iteration``. Logs a loud ERROR (not WARNING) listing
    the missing columns so the operator sees a concrete remediation
    target.
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    try:
        present = {
            row["col_name"].lower()
            for row in spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        }
    except Exception as exc:
        logger.warning(
            "  [VERIFY] Could not DESCRIBE %s to verify migration: %s",
            fqn, exc,
        )
        return

    missing = [c for c in _REQUIRED_ITERATION_COLUMNS if c.lower() not in present]
    if missing:
        logger.error(
            "  [MIGRATION INCOMPLETE] %s is missing columns required by "
            "write_iteration: %s. Subsequent INSERTs will fail with "
            "UNRESOLVED_COLUMN. Remediation: run "
            "`ALTER TABLE %s ADD COLUMNS (<col> <type>, …)` for each "
            "missing column (see genie_space_optimizer.optimization.state."
            "_migrate_add_columns for the intended types/comments).",
            fqn, missing, fqn,
        )


# ── Write Functions ──────────────────────────────────────────────────────


def create_run(
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    catalog: str,
    schema: str,
    *,
    uc_schema: str | None = None,
    max_iterations: int | None = None,
    levers: list[int] | None = None,
    apply_mode: str = "genie_config",
    deploy_target: str | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    config_snapshot: dict | None = None,
    triggered_by: str | None = None,
) -> None:
    """Insert a new row into ``genie_opt_runs`` with status QUEUED."""
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER, MAX_ITERATIONS

    now = datetime.now(timezone.utc).isoformat()
    row: dict[str, Any] = {
        "run_id": run_id,
        "space_id": space_id,
        "domain": domain,
        "catalog": catalog,
        "uc_schema": uc_schema or f"{catalog}.{schema}",
        "status": "QUEUED",
        "started_at": now,
        "max_iterations": max_iterations or MAX_ITERATIONS,
        "levers": json.dumps(levers or DEFAULT_LEVER_ORDER),
        "apply_mode": apply_mode,
        "updated_at": now,
    }
    if deploy_target is not None:
        row["deploy_target"] = deploy_target
    if experiment_name is not None:
        row["experiment_name"] = experiment_name
    if experiment_id is not None:
        row["experiment_id"] = experiment_id
    if config_snapshot is not None:
        row["config_snapshot"] = json.dumps(config_snapshot)
    if triggered_by is not None:
        row["triggered_by"] = triggered_by

    insert_row(spark, catalog, schema, TABLE_RUNS, row)
    logger.info("Created run %s for space %s", run_id, space_id)


def _update_row_with_delta_retry(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    keys: dict[str, Any],
    updates: dict[str, Any],
    *,
    attempts: int = 3,
) -> None:
    last_exc: BaseException | None = None
    for attempt in range(attempts):
        try:
            update_row(spark, catalog, schema, table, keys, updates)
            return
        except Exception as exc:
            if not is_retryable_delta_write_conflict(exc) or attempt == attempts - 1:
                raise
            last_exc = exc
            time.sleep(0.25 * (attempt + 1))
    if last_exc is not None:
        raise last_exc


def _lookup_run_space_id(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> str:
    """Return the run's space_id for partition-pruned updates when possible."""
    try:
        df = read_table(
            spark,
            catalog,
            schema,
            TABLE_RUNS,
            filters={"run_id": run_id},
        )
        if not df.empty and "space_id" in df.columns:
            value = df.iloc[0]["space_id"]
            if value is not None:
                return str(value)
    except Exception:
        logger.debug(
            "Could not look up space_id for run %s; falling back to run_id-only update",
            run_id,
            exc_info=True,
        )
    return ""


def update_run_status(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    status: str | None = None,
    best_iteration: int | None = None,
    best_accuracy: float | None = None,
    best_repeatability: float | None = None,
    best_model_id: str | None = None,
    convergence_reason: str | None = None,
    job_run_id: str | None = None,
    job_id: str | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    labeling_session_name: str | None = None,
    labeling_session_run_id: str | None = None,
    labeling_session_url: str | None = None,
    config_snapshot: dict | None = None,
    space_id: str | None = None,
) -> None:
    """Update ``genie_opt_runs`` — only sets non-None fields."""
    now = datetime.now(timezone.utc).isoformat()

    updates: dict[str, Any] = {"updated_at": now}
    terminal_statuses = {"CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED"}

    if status is not None:
        updates["status"] = status
        if status in terminal_statuses:
            updates["completed_at"] = now
    if best_iteration is not None:
        updates["best_iteration"] = best_iteration
    if best_accuracy is not None:
        updates["best_accuracy"] = best_accuracy
    if best_repeatability is not None:
        updates["best_repeatability"] = best_repeatability
    if best_model_id is not None:
        updates["best_model_id"] = best_model_id
    if convergence_reason is not None:
        updates["convergence_reason"] = convergence_reason
    if job_run_id is not None:
        updates["job_run_id"] = job_run_id
    if job_id is not None:
        updates["job_id"] = job_id
    if experiment_name is not None:
        updates["experiment_name"] = experiment_name
    if experiment_id is not None:
        updates["experiment_id"] = experiment_id
    if labeling_session_name is not None:
        updates["labeling_session_name"] = labeling_session_name
    if labeling_session_run_id is not None:
        updates["labeling_session_run_id"] = labeling_session_run_id
    if labeling_session_url is not None:
        updates["labeling_session_url"] = labeling_session_url
    if config_snapshot is not None:
        updates["config_snapshot"] = json.dumps(config_snapshot)

    resolved_space_id = space_id or _lookup_run_space_id(spark, run_id, catalog, schema)
    keys: dict[str, Any] = {"run_id": run_id}
    if resolved_space_id:
        keys["space_id"] = resolved_space_id

    _update_row_with_delta_retry(
        spark,
        catalog,
        schema,
        TABLE_RUNS,
        keys,
        updates,
    )


def write_stage(
    spark: SparkSession,
    run_id: str,
    stage: str,
    status: str,
    *,
    task_key: str | None = None,
    lever: int | None = None,
    iteration: int | None = None,
    detail: dict | None = None,
    error_message: str | None = None,
    catalog: str,
    schema: str,
) -> None:
    """Insert into ``genie_opt_stages``.

    ``task_key`` identifies which Databricks Job task wrote this row.
    ``detail`` dict is JSON-serialized into ``detail_json``.
    For COMPLETE/FAILED/SKIPPED/ROLLED_BACK, computes ``duration_seconds``
    by diffing the matching STARTED row's ``started_at``.
    """
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    fqn = _fqn(catalog, schema, TABLE_STAGES)

    completed_at: str | None = None
    duration_seconds: float | None = None

    if status in ("COMPLETE", "FAILED", "SKIPPED", "ROLLED_BACK"):
        completed_at = now_iso
        started_df = run_query(
            spark,
            f"SELECT started_at FROM {fqn} "
            f"WHERE run_id = '{run_id}' AND stage = '{stage}' AND status = 'STARTED' "
            f"ORDER BY started_at DESC LIMIT 1",
        )
        if not started_df.empty:
            started_ts = pd.Timestamp(started_df.iloc[0]["started_at"])
            if started_ts.tzinfo is None:
                started_ts = started_ts.tz_localize("UTC")
            duration_seconds = (now - started_ts.to_pydatetime()).total_seconds()

    detail_json = json.dumps(detail) if detail else None
    _safe_err = error_message.replace("'", "''") if error_message else None

    col_names = (
        "run_id, task_key, stage, status, started_at, completed_at, "
        "duration_seconds, lever, iteration, detail_json, error_message"
    )

    def _sql_val(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return str(val).lower()
        if isinstance(val, (int, float)):
            return str(val)
        return f"'{val}'"

    vals = ", ".join(
        [
            _sql_val(run_id),
            _sql_val(task_key),
            _sql_val(stage),
            _sql_val(status),
            f"TIMESTAMP '{now_iso}'",
            f"TIMESTAMP '{completed_at}'" if completed_at else "NULL",
            _sql_val(duration_seconds),
            _sql_val(lever),
            _sql_val(iteration),
            _sql_val(detail_json.replace("'", "''") if detail_json else None),
            _sql_val(_safe_err),
        ]
    )

    execute_delta_write_with_retry(
        spark,
        f"INSERT INTO {fqn} ({col_names}) VALUES ({vals})",
        operation_name="write_stage",
        table_name=fqn,
    )
    logger.info("Stage %s/%s for run %s", stage, status, run_id)


def write_eval_heartbeat(
    spark: SparkSession,
    run_id: str,
    *,
    phase: str,
    detail: dict,
    catalog: str,
    schema: str,
    task_key: str = "baseline_eval",
) -> None:
    """Append a lightweight heartbeat row for long-running evaluation."""
    write_stage(
        spark,
        run_id,
        "EVAL_HEARTBEAT",
        "STARTED",
        task_key=task_key,
        detail={"phase": phase, **detail},
        catalog=catalog,
        schema=schema,
    )


def write_iteration(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    eval_result: dict,
    *,
    catalog: str,
    schema: str,
    lever: int | None = None,
    eval_scope: str = "full",
    model_id: str | None = None,
    reflection_json: dict | None = None,
) -> None:
    """Insert into ``genie_opt_iterations`` with scores, failures, etc."""
    now = datetime.now(timezone.utc).isoformat()
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)

    scores = eval_result.get("scores", {})
    failures = eval_result.get("failures", [])
    remaining = eval_result.get("remaining_failures", failures)
    arbiter_actions = eval_result.get("arbiter_actions", [])
    thresholds_met = eval_result.get("thresholds_met", False)
    if isinstance(thresholds_met, (int, float)):
        thresholds_met = thresholds_met == 1.0

    repeatability_pct = eval_result.get("repeatability_pct")
    repeatability_details = eval_result.get("repeatability_details")
    rows_data = eval_result.get("rows")
    if isinstance(rows_data, list):
        _STRIP_COLS = {"trace", "trace_id"}
        rows_data = [{k: v for k, v in r.items() if k not in _STRIP_COLS} for r in rows_data if isinstance(r, dict)]

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    def _opt_json(val: Any) -> str:
        if val is None:
            return "NULL"
        return f"'{_esc(json.dumps(val))}'"

    mlflow_run_id = eval_result.get("mlflow_run_id")

    # Bug #2 denominator contract fields.
    # Read from eval_result but fall back sensibly:
    #   evaluated_count defaults to total_questions (matches pre-Bug#2 behavior
    #   where no rows were excluded at runtime).
    #   excluded_count / quarantined default to 0 / empty list.
    # This keeps the write safe against eval_results emitted by older call
    # sites (e.g. repeatability-only paths) that don't populate the new keys.
    _total_questions = int(eval_result.get("total_questions", 0) or 0)
    _evaluated_count = eval_result.get("evaluated_count")
    if _evaluated_count is None:
        _evaluated_count = _total_questions
    _excluded_count = int(eval_result.get("excluded_count", 0) or 0)
    _quarantined = eval_result.get("quarantined_benchmarks")
    if not isinstance(_quarantined, list):
        _quarantined = []

    # Bug #4 leakage observability. Callers pass the metrics through
    # eval_result so the write stays back-compat for call sites that don't
    # track them (older repeatability-only paths). Defaults: empty maps, 0.
    _leakage_count_by_type = eval_result.get("leakage_count_by_type") or {}
    if not isinstance(_leakage_count_by_type, dict):
        _leakage_count_by_type = {}
    _firewall_rejection_count_by_type = eval_result.get("firewall_rejection_count_by_type") or {}
    if not isinstance(_firewall_rejection_count_by_type, dict):
        _firewall_rejection_count_by_type = {}
    _secondary_mining_blocked = int(eval_result.get("secondary_mining_blocked", 0) or 0)

    # Bug #4 Phase 3 synthesis observability.
    _synthesis_slots_persisted = int(eval_result.get("synthesis_slots_persisted", 0) or 0)
    _arbiter_rejection_count = int(eval_result.get("arbiter_rejection_count", 0) or 0)
    _cluster_fallback_to_instruction_count = int(
        eval_result.get("cluster_fallback_to_instruction_count", 0) or 0
    )
    _synthesis_archetype_distribution = eval_result.get("synthesis_archetype_distribution") or {}
    if not isinstance(_synthesis_archetype_distribution, dict):
        _synthesis_archetype_distribution = {}

    # Tier 1.7: both_correct_count / both_correct_rate. These are
    # strictly stricter than overall_accuracy (which counts arbiter
    # overrides of rc=yes). Defaults preserve back-compat with eval
    # results emitted before the migration landed.
    _both_correct_count = int(eval_result.get("both_correct_count", 0) or 0)
    _both_correct_rate_val = eval_result.get("both_correct_rate")
    if _both_correct_rate_val is None and _evaluated_count:
        _both_correct_rate_val = round(
            100.0 * _both_correct_count / int(_evaluated_count), 2
        ) if int(_evaluated_count) > 0 else 0.0

    col_names = (
        "run_id, iteration, lever, eval_scope, timestamp, mlflow_run_id, model_id, "
        "overall_accuracy, total_questions, correct_count, scores_json, failures_json, "
        "remaining_failures, arbiter_actions_json, repeatability_pct, repeatability_json, "
        "thresholds_met, rows_json, reflection_json, "
        "evaluated_count, excluded_count, quarantined_benchmarks_json, "
        "leakage_count_by_type, firewall_rejection_count_by_type, secondary_mining_blocked, "
        "synthesis_slots_persisted, arbiter_rejection_count, "
        "cluster_fallback_to_instruction_count, synthesis_archetype_distribution, "
        "rolled_back, both_correct_count, both_correct_rate"
    )
    vals = ", ".join(
        [
            f"'{run_id}'",
            str(iteration),
            str(lever) if lever is not None else "NULL",
            f"'{eval_scope}'",
            f"TIMESTAMP '{now}'",
            f"'{mlflow_run_id}'" if mlflow_run_id else "NULL",
            f"'{model_id}'" if model_id else "NULL",
            str(eval_result.get("overall_accuracy", 0.0)),
            str(_total_questions),
            str(eval_result.get("correct_count", 0)),
            f"'{_esc(json.dumps(scores))}'",
            _opt_json(failures),
            _opt_json(remaining),
            _opt_json(arbiter_actions),
            str(repeatability_pct) if repeatability_pct is not None else "NULL",
            _opt_json(repeatability_details),
            str(thresholds_met).lower(),
            _opt_json(rows_data),
            _opt_json(reflection_json),
            str(int(_evaluated_count)),
            str(_excluded_count),
            _opt_json(_quarantined) if _quarantined else "NULL",
            _opt_json(_leakage_count_by_type) if _leakage_count_by_type else "NULL",
            _opt_json(_firewall_rejection_count_by_type) if _firewall_rejection_count_by_type else "NULL",
            str(_secondary_mining_blocked),
            str(_synthesis_slots_persisted),
            str(_arbiter_rejection_count),
            str(_cluster_fallback_to_instruction_count),
            _opt_json(_synthesis_archetype_distribution) if _synthesis_archetype_distribution else "NULL",
            "false",
            str(_both_correct_count),
            str(_both_correct_rate_val) if _both_correct_rate_val is not None else "NULL",
        ]
    )

    execute_delta_write_with_retry(
        spark,
        f"INSERT INTO {fqn} ({col_names}) VALUES ({vals})",
        operation_name="write_iteration",
        table_name=fqn,
    )
    logger.info(
        "Iteration %d (lever=%s, scope=%s) for run %s: accuracy=%.1f%%",
        iteration,
        lever,
        eval_scope,
        run_id,
        eval_result.get("overall_accuracy", 0.0),
    )


def update_iteration_reflection(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    reflection_json: dict,
    *,
    catalog: str,
    schema: str,
    eval_scope: str = "full",
) -> None:
    """Update ``reflection_json`` on an existing iteration row."""
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    payload = _esc(json.dumps(reflection_json))
    stmt = (
        f"UPDATE {fqn} SET reflection_json = '{payload}' "
        f"WHERE run_id = '{run_id}' AND iteration = {iteration} "
        f"AND eval_scope = '{eval_scope}'"
    )
    execute_delta_write_with_retry(
        spark,
        stmt,
        operation_name="update_iteration_reflection",
        table_name=fqn,
    )
    logger.info(
        "Updated reflection_json for run %s iteration %d scope=%s",
        run_id, iteration, eval_scope,
    )


def write_patch(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    lever: int,
    patch_index: int,
    patch_record: dict,
    catalog: str,
    schema: str,
) -> None:
    """Insert into ``genie_opt_patches``."""
    now = datetime.now(timezone.utc).isoformat()

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    row: dict[str, Any] = {
        "run_id": run_id,
        "iteration": iteration,
        "lever": lever,
        "patch_index": patch_index,
        "patch_type": patch_record.get("patch_type", "unknown"),
        "scope": patch_record.get("scope", "genie_config"),
        "risk_level": patch_record.get("risk_level", "low"),
        "applied_at": now,
    }

    target_object = patch_record.get("target_object")
    if target_object is not None:
        row["target_object"] = target_object

    row["patch_json"] = json.dumps(patch_record.get("patch", patch_record))

    command = patch_record.get("command")
    if command is not None:
        row["command_json"] = command if isinstance(command, str) else json.dumps(command)

    rollback = patch_record.get("rollback")
    if rollback is not None:
        row["rollback_json"] = rollback if isinstance(rollback, str) else json.dumps(rollback)

    proposal_id = patch_record.get("proposal_id")
    if proposal_id is not None:
        row["proposal_id"] = proposal_id

    cluster_id = patch_record.get("cluster_id")
    if cluster_id is not None:
        row["cluster_id"] = cluster_id

    provenance = patch_record.get("provenance")
    if provenance is not None:
        row["provenance_json"] = json.dumps(provenance, default=str)

    applied_patch_type = patch_record.get("applied_patch_type")
    if applied_patch_type is not None:
        row["applied_patch_type"] = applied_patch_type

    applied_patch_detail = patch_record.get("applied_patch_detail")
    if applied_patch_detail is not None:
        row["applied_patch_detail"] = applied_patch_detail

    insert_row(spark, catalog, schema, TABLE_PATCHES, row)
    logger.info(
        "Patch %d (lever %d, iter %d) for run %s: %s on %s",
        patch_index,
        lever,
        iteration,
        run_id,
        row["patch_type"],
        target_object,
    )


def mark_patches_rolled_back(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    reason: str,
    catalog: str,
    schema: str,
) -> None:
    """Set ``rolled_back=true`` on all patches AND the iteration row for a given run + iteration.

    Tier 1.1: also stamps ``genie_opt_iterations`` so downstream readers
    (``load_latest_full_iteration``, ``_get_baseline_and_best_accuracy``,
    ``promote_best_model``) can filter rolled-back iterations out of
    "current state" computations. Without this, iteration N's clustering
    would re-read iteration N-1's rolled-back eval data (ghost-cluster
    feedback loop), and the UI would show a rolled-back iteration's
    accuracy as ``optimizedScore``.
    """
    now = datetime.now(timezone.utc).isoformat()
    patches_fqn = _fqn(catalog, schema, TABLE_PATCHES)
    iters_fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    safe_reason = reason.replace("'", "''")
    patches_stmt = (
        f"UPDATE {patches_fqn} SET rolled_back = true, "
        f"rolled_back_at = TIMESTAMP '{now}', "
        f"rollback_reason = '{safe_reason}' "
        f"WHERE run_id = '{run_id}' AND iteration = {iteration}"
    )
    execute_delta_write_with_retry(
        spark,
        patches_stmt,
        operation_name="mark_patches_rolled_back.patches",
        table_name=patches_fqn,
    )
    try:
        iterations_stmt = (
            f"UPDATE {iters_fqn} SET rolled_back = true, "
            f"rolled_back_at = TIMESTAMP '{now}', "
            f"rollback_reason = '{safe_reason}' "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration}"
        )
        execute_delta_write_with_retry(
            spark,
            iterations_stmt,
            operation_name="mark_patches_rolled_back.iterations",
            table_name=iters_fqn,
        )
    except Exception:
        # Non-fatal: the patches-table stamp is still correct, so the
        # deployed Genie Space state is accurate. Only the iteration
        # filter downstream is affected; readers fall back to reading
        # ``rolled_back`` from patches via a join if needed.
        logger.warning(
            "Failed to stamp rolled_back on iterations row run=%s iter=%d",
            run_id, iteration, exc_info=True,
        )
    logger.info("Rolled back patches + iteration row for run %s iteration %d: %s", run_id, iteration, reason)


# ── ASI & Provenance Write Functions ─────────────────────────────────────


def write_asi_results(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    asi_rows: list[dict],
    catalog: str,
    schema: str,
    *,
    mlflow_run_id: str = "",
) -> None:
    """Write per-question per-judge ASI feedback to ``genie_eval_asi_results``."""
    if not asi_rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    for a in asi_rows:
        blame = a.get("blame_set")
        if isinstance(blame, list):
            blame = json.dumps(blame)
        row: dict[str, Any] = {
            "run_id": run_id,
            "mlflow_run_id": mlflow_run_id or a.get("mlflow_run_id", ""),
            "iteration": iteration,
            "question_id": a.get("question_id", ""),
            "judge": a.get("judge", ""),
            "value": a.get("value", "no"),
            "failure_type": a.get("failure_type"),
            "severity": a.get("severity"),
            "confidence": a.get("confidence"),
            "blame_set": blame,
            "counterfactual_fix": a.get("counterfactual_fix"),
            "wrong_clause": a.get("wrong_clause"),
            "expected_value": a.get("expected_value"),
            "actual_value": a.get("actual_value"),
            "missing_metadata": a.get("missing_metadata"),
            "ambiguity_detected": a.get("ambiguity_detected", False),
            "logged_at": now,
        }
        row = {k: v for k, v in row.items() if v is not None}
        try:
            insert_row(spark, catalog, schema, TABLE_ASI, row)
        except Exception:
            logger.debug("Failed to write ASI row for %s/%s", a.get("question_id"), a.get("judge"), exc_info=True)
    logger.info("Wrote %d ASI results for run %s iter %d", len(asi_rows), run_id, iteration)


def write_provenance(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    lever: int,
    provenance_rows: list[dict],
    catalog: str,
    schema: str,
) -> None:
    """Write provenance rows linking questions/judges to clusters."""
    if not provenance_rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    for p in provenance_rows:
        blame = p.get("blame_set")
        if isinstance(blame, list):
            blame = json.dumps(blame)
        row: dict[str, Any] = {
            "run_id": run_id,
            "iteration": iteration,
            "lever": lever,
            "question_id": p.get("question_id", ""),
            "signal_type": p.get("signal_type", "hard"),
            "arbiter_verdict": p.get("arbiter_verdict"),
            "judge": p.get("judge", ""),
            "judge_verdict": p.get("judge_verdict", "FAIL"),
            "asi_failure_type_raw": p.get("asi_failure_type_raw"),
            "resolved_root_cause": p.get("resolved_root_cause", "other"),
            "resolution_method": p.get("resolution_method", "unknown"),
            "blame_set": blame,
            "counterfactual_fix": p.get("counterfactual_fix"),
            "wrong_clause": p.get("wrong_clause"),
            "rationale_snippet": (p.get("rationale_snippet") or "")[:500],
            "expected_sql": (p.get("expected_sql") or "")[:2000],
            "generated_sql": (p.get("generated_sql") or "")[:2000],
            "cluster_id": p.get("cluster_id", ""),
            "logged_at": now,
        }
        row = {k: v for k, v in row.items() if v is not None}
        try:
            insert_row(spark, catalog, schema, TABLE_PROVENANCE, row)
        except Exception:
            logger.debug("Failed to write provenance row for %s/%s", p.get("question_id"), p.get("judge"), exc_info=True)
    logger.info("Wrote %d provenance rows for run %s iter %d lever %d", len(provenance_rows), run_id, iteration, lever)


def write_lever_loop_decisions(
    spark: SparkSession,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist Task 3 decision audit rows.

    Each input row may carry plain Python lists/dicts under
    ``affected_qids``, ``source_cluster_ids``, ``proposal_ids``,
    ``proposal_to_patch_map``, and ``metrics``. We JSON-serialize them
    here so callers don't have to round-trip through ``json.dumps``.
    No-op when ``rows`` is empty.
    """
    if not rows:
        return

    def _as_json(val: Any) -> Any:
        if val is None:
            return None
        if isinstance(val, str):
            return val  # already serialized
        try:
            return json.dumps(val, sort_keys=True, default=str)
        except (TypeError, ValueError):
            return None

    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for r in rows:
        payload: dict[str, Any] = {
            "run_id": r.get("run_id", ""),
            "iteration": int(r.get("iteration") or 0),
            "ag_id": r.get("ag_id"),
            "decision_order": int(r.get("decision_order") or 0),
            "stage_letter": r.get("stage_letter"),
            "gate_name": r.get("gate_name", ""),
            "decision": r.get("decision", ""),
            "reason_code": r.get("reason_code"),
            "reason_detail": (r.get("reason_detail") or None) if not isinstance(
                r.get("reason_detail"), str,
            ) else r["reason_detail"][:2000],
            "affected_qids_json": _as_json(
                r.get("affected_qids_json", r.get("affected_qids")),
            ),
            "source_cluster_ids_json": _as_json(
                r.get("source_cluster_ids_json", r.get("source_cluster_ids")),
            ),
            "proposal_ids_json": _as_json(
                r.get("proposal_ids_json", r.get("proposal_ids")),
            ),
            "proposal_to_patch_map_json": _as_json(
                r.get(
                    "proposal_to_patch_map_json", r.get("proposal_to_patch_map"),
                ),
            ),
            "metrics_json": _as_json(r.get("metrics_json", r.get("metrics"))),
            "created_at": now,
        }
        if not payload["run_id"] or not payload["gate_name"] or not payload["decision"]:
            continue
        try:
            insert_row(
                spark, catalog, schema, TABLE_LEVER_LOOP_DECISIONS, payload,
            )
            written += 1
        except Exception:
            logger.debug(
                "Failed to write lever-loop decision row %s/%s",
                payload["gate_name"],
                payload["reason_code"],
                exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d lever-loop decision row(s) for run %s",
            written,
            rows[0].get("run_id", "?"),
        )


def write_question_regressions(
    spark: SparkSession,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist Task 4 per-question pass/fail transitions.

    Empty list is a no-op. JSON columns are serialized here so callers
    can pass plain Python lists.
    """
    if not rows:
        return

    def _as_json(val: Any) -> Any:
        if val is None:
            return None
        if isinstance(val, str):
            return val
        try:
            return json.dumps(val, sort_keys=True, default=str)
        except (TypeError, ValueError):
            return None

    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for r in rows:
        payload: dict[str, Any] = {
            "run_id": r.get("run_id", ""),
            "iteration": int(r.get("iteration") or 0),
            "ag_id": r.get("ag_id", ""),
            "question_id": r.get("question_id", ""),
            "was_passing": bool(r.get("was_passing")) if r.get("was_passing") is not None else None,
            "is_passing": bool(r.get("is_passing")) if r.get("is_passing") is not None else None,
            "transition": r.get("transition"),
            "pre_arbiter_before": r.get("pre_arbiter_before"),
            "pre_arbiter_after": r.get("pre_arbiter_after"),
            "post_arbiter_before": r.get("post_arbiter_before"),
            "post_arbiter_after": r.get("post_arbiter_after"),
            "source_cluster_ids_json": _as_json(
                r.get("source_cluster_ids_json", r.get("source_cluster_ids")),
            ),
            "source_proposal_ids_json": _as_json(
                r.get(
                    "source_proposal_ids_json", r.get("source_proposal_ids"),
                ),
            ),
            "applied_patch_ids_json": _as_json(
                r.get("applied_patch_ids_json", r.get("applied_patch_ids")),
            ),
            "suppressed": bool(r.get("suppressed", False)),
            "created_at": now,
        }
        if not payload["run_id"] or not payload["question_id"]:
            continue
        try:
            insert_row(
                spark, catalog, schema, TABLE_QUESTION_REGRESSIONS, payload,
            )
            written += 1
        except Exception:
            logger.debug(
                "Failed to write question regression row %s",
                payload["question_id"],
                exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d question regression row(s) for run %s",
            written,
            rows[0].get("run_id", "?"),
        )


def write_proactive_corpus_profile(
    spark: SparkSession,
    *,
    run_id: str,
    iteration: int,
    table_id: str | None,
    profile_blob: dict | str | None,
    eligible_row_count: int,
    catalog: str,
    schema: str,
) -> None:
    """Persist a Task 9 proactive corpus profile snapshot.

    ``profile_blob`` may be a Python dict (will be JSON-serialized) or
    a pre-serialized string. No-op when both ``profile_blob`` and
    ``eligible_row_count`` are empty.
    """
    if not profile_blob and not eligible_row_count:
        return
    if isinstance(profile_blob, dict):
        try:
            profile_blob_str = json.dumps(profile_blob, sort_keys=True, default=str)
        except (TypeError, ValueError):
            profile_blob_str = None
    else:
        profile_blob_str = profile_blob
    payload: dict[str, Any] = {
        "run_id": run_id,
        "iteration": int(iteration),
        "table_id": table_id,
        "profile_blob": profile_blob_str,
        "eligible_row_count": int(eligible_row_count or 0),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        insert_row(
            spark, catalog, schema, TABLE_PROACTIVE_CORPUS_PROFILE, payload,
        )
    except Exception:
        logger.debug(
            "Failed to write proactive corpus profile row for run %s iter %d",
            run_id, iteration, exc_info=True,
        )


def write_proactive_patches(
    spark: SparkSession,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist Task 9 proactive enrichment patches.

    Each input row is the patch dict emitted by
    ``feature_mining.synthesize_proactive_patches`` plus an
    ``applied`` boolean and optional ``patch_id`` / ``run_id`` /
    ``iteration``. No-op when ``rows`` is empty.
    """
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for r in rows:
        run_id = str(r.get("run_id") or "")
        if not run_id:
            continue
        target = (
            r.get("target")
            or r.get("column")
            or r.get("snippet_name")
            or (
                f"{r.get('left_table', '')}|{r.get('join_kind', '')}|"
                f"{r.get('right_table', '')}"
                if r.get("type") == "add_join_spec" else None
            )
        )
        payload: dict[str, Any] = {
            "run_id": run_id,
            "iteration": int(r.get("iteration") or 0),
            "patch_id": str(r.get("patch_id") or f"proactive_{written + 1}"),
            "patch_type": r.get("type"),
            "target": str(target) if target is not None else None,
            "table_id": r.get("table_id"),
            "source_signal": r.get("source_signal"),
            "frequency": int(r.get("frequency") or 0),
            "dedup_route": r.get("dedup_route"),
            "dedup_dropped_reason": r.get("dedup_dropped_reason"),
            "applied": bool(r.get("applied", False)),
            "created_at": now,
        }
        try:
            insert_row(
                spark, catalog, schema, TABLE_PROACTIVE_PATCHES, payload,
            )
            written += 1
        except Exception:
            logger.debug(
                "Failed to write proactive patch row for run %s",
                run_id, exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d proactive patch row(s) for run %s",
            written, rows[0].get("run_id", "?"),
        )


def write_human_required_escalations(
    spark: SparkSession,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist Task 8 escalation records to ``genie_eval_human_required``.

    Each input row may carry plain Python lists/dicts under
    ``evidence`` / ``evidence_json``; we JSON-serialize defensively.
    Empty list is a no-op. Skips rows missing run_id or
    cluster_signature so orphan rows do not pollute the queue.
    """
    if not rows:
        return

    def _as_json(val: Any) -> Any:
        if val is None:
            return None
        if isinstance(val, str):
            return val
        try:
            return json.dumps(val, sort_keys=True, default=str)
        except (TypeError, ValueError):
            return None

    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for r in rows:
        sig = str(r.get("cluster_signature") or "").strip()
        run_id = str(r.get("run_id") or "").strip()
        if not sig or not run_id:
            continue
        payload: dict[str, Any] = {
            "run_id": run_id,
            "cluster_signature": sig,
            "question_id": r.get("question_id") or "",
            "root_cause": r.get("root_cause"),
            "attempt_count": int(r.get("attempt_count") or 0),
            "last_iteration": int(r.get("last_iteration") or 0),
            "reason_code": r.get("reason_code"),
            "evidence_json": _as_json(r.get("evidence_json", r.get("evidence"))),
            "created_at": now,
        }
        try:
            insert_row(
                spark, catalog, schema, TABLE_HUMAN_REQUIRED, payload,
            )
            written += 1
        except Exception:
            logger.debug(
                "Failed to write human-required row for sig=%s qid=%s",
                sig,
                payload["question_id"],
                exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d human-required escalation row(s) for run %s",
            written,
            rows[0].get("run_id", "?"),
        )


def write_gt_correction_candidates(
    spark: SparkSession,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist GT correction queue payloads from Task 1.

    Each row already carries the Delta-shaped fields built by
    ``ground_truth_corrections.build_gt_correction_candidate``. Status
    starts at ``pending_review``; the four-state machine
    (``pending_review`` / ``accepted_corpus_fix`` / ``rejected_keep_gt``
    / ``superseded``) is documented in the helper module. No-op when
    ``rows`` is empty.
    """
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for r in rows:
        payload: dict[str, Any] = {
            "run_id": r.get("run_id", ""),
            "iteration": int(r.get("iteration") or 0),
            "question_id": r.get("question_id", ""),
            "question": (r.get("question") or "")[:5000],
            "expected_sql": (r.get("expected_sql") or "")[:5000],
            "genie_sql": (r.get("genie_sql") or "")[:5000],
            "arbiter_verdict": r.get("arbiter_verdict", ""),
            "arbiter_rationale": (r.get("arbiter_rationale") or "")[:2000],
            "status": r.get("status") or "pending_review",
            "created_at": now,
        }
        if not payload["question_id"]:
            # Skip orphan rows; downstream consumers key on question_id.
            continue
        try:
            insert_row(
                spark, catalog, schema, TABLE_GT_CORRECTION_CANDIDATES, payload,
            )
            written += 1
        except Exception:
            logger.debug(
                "Failed to write GT correction candidate for %s",
                payload["question_id"],
                exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d GT correction candidate(s) for run %s",
            written,
            rows[0].get("run_id", "?"),
        )


def update_provenance_proposals(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    proposal_mappings: list[dict],
    catalog: str,
    schema: str,
) -> None:
    """Backfill ``proposal_id`` and ``patch_type`` into provenance rows."""
    fqn = _fqn(catalog, schema, TABLE_PROVENANCE)
    for m in proposal_mappings:
        cid = (m.get("cluster_id") or "").replace("'", "''")
        pid = (m.get("proposal_id") or "").replace("'", "''")
        pt = (m.get("patch_type") or "").replace("'", "''")
        if not cid:
            continue
        try:
            stmt = (
                f"UPDATE {fqn} SET proposal_id = '{pid}', patch_type = '{pt}' "
                f"WHERE run_id = '{run_id}' AND iteration = {iteration} AND cluster_id = '{cid}'"
            )
            execute_delta_write_with_retry(
                spark,
                stmt,
                operation_name="update_provenance_proposals",
                table_name=fqn,
            )
        except Exception:
            logger.debug("Failed to update provenance proposals for cluster %s", cid, exc_info=True)


def update_provenance_gate(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    lever: int,
    gate_type: str,
    gate_result: str,
    gate_regression: dict | None,
    catalog: str,
    schema: str,
) -> None:
    """Backfill gate outcome into provenance rows."""
    fqn = _fqn(catalog, schema, TABLE_PROVENANCE)
    gt = gate_type.replace("'", "''")
    gr = gate_result.replace("'", "''")
    reg_json = json.dumps(gate_regression, default=str) if gate_regression else None
    reg_str = f"'{reg_json.replace(chr(39), chr(39)+chr(39))}'" if reg_json else "NULL"
    try:
        stmt = (
            f"UPDATE {fqn} SET gate_type = '{gt}', gate_result = '{gr}', gate_regression = {reg_str} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration} AND lever = {lever}"
        )
        execute_delta_write_with_retry(
            spark,
            stmt,
            operation_name="update_provenance_gate",
            table_name=fqn,
        )
    except Exception:
        logger.debug("Failed to update provenance gate for run %s iter %d lever %d", run_id, iteration, lever, exc_info=True)


# ── Read Functions ───────────────────────────────────────────────────────


def load_run(spark: SparkSession, run_id: str, catalog: str, schema: str) -> dict | None:
    """Return a plain Python dict for a run, or ``None`` if not found."""
    df = read_table(spark, catalog, schema, TABLE_RUNS, filters={"run_id": run_id})
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for col in ("levers", "config_snapshot"):
        if row.get(col) and isinstance(row[col], str):
            try:
                row[col] = json.loads(row[col])
            except (json.JSONDecodeError, TypeError):
                pass
    return row


def load_stages(spark: SparkSession, run_id: str, catalog: str, schema: str) -> pd.DataFrame:
    """All stages for a run, ordered by ``started_at ASC``."""
    fqn = _fqn(catalog, schema, TABLE_STAGES)
    return run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' ORDER BY started_at ASC",
    )


def load_iterations(spark: SparkSession, run_id: str, catalog: str, schema: str) -> pd.DataFrame:
    """All iterations for a run, ordered by ``iteration ASC``."""
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    return run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' ORDER BY iteration ASC",
    )


def load_patches(spark: SparkSession, run_id: str, catalog: str, schema: str) -> pd.DataFrame:
    """All patches for a run, ordered by ``applied_at ASC``."""
    fqn = _fqn(catalog, schema, TABLE_PATCHES)
    return run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' ORDER BY applied_at ASC",
    )


def read_latest_stage(
    spark: SparkSession, run_id: str, catalog: str, schema: str
) -> dict | None:
    """Return the most recent stage row as a dict, or ``None``."""
    fqn = _fqn(catalog, schema, TABLE_STAGES)
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' "
        f"ORDER BY started_at DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    if row.get("detail_json") and isinstance(row["detail_json"], str):
        try:
            row["detail"] = json.loads(row["detail_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return row


def load_latest_full_iteration(
    spark: SparkSession, run_id: str, catalog: str, schema: str,
    *, include_rolled_back: bool = False,
    before_iteration: int | None = None,
) -> dict | None:
    """Latest iteration with ``eval_scope='full'``. Used for resume + convergence.

    Tier 1.2: by default excludes iterations marked ``rolled_back=true`` so
    downstream clustering / best-score computations don't re-read reverted
    state (the ghost-cluster feedback loop). Set ``include_rolled_back=True``
    only when a caller specifically needs to reason about the rolled-back
    data (e.g. post-mortem audits).

    When *before_iteration* is provided, rows at that iteration or later are
    ignored. This prevents the full-eval acceptance path from reading the
    candidate row it just wrote as its own control-plane baseline.
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    rollback_filter = (
        "" if include_rolled_back
        else " AND (rolled_back IS NULL OR rolled_back = false)"
    )
    before_filter = (
        f" AND iteration < {int(before_iteration)}"
        if before_iteration is not None
        else ""
    )
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' AND eval_scope = 'full'"
        f"{rollback_filter}"
        f"{before_filter} "
        f"ORDER BY iteration DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for col in ("scores_json", "failures_json", "remaining_failures", "arbiter_actions_json",
                "repeatability_json", "rows_json"):
        if row.get(col) and isinstance(row[col], str):
            try:
                row[col] = json.loads(row[col])
            except (json.JSONDecodeError, TypeError):
                pass
    return row


def load_latest_state_iteration(
    spark: SparkSession, run_id: str, catalog: str, schema: str,
    *, include_rolled_back: bool = False,
) -> dict | None:
    """Latest iteration row reflecting current Genie Space state.

    Includes ``eval_scope IN ('full', 'enrichment')`` so post-enrichment
    evals (which mutate the space without an intervening lever-loop
    iteration) are visible as the current state to clustering and
    proposal grounding. Without this, callers reading
    ``load_latest_full_iteration`` get the pre-enrichment baseline_eval
    row even though enrichment has already mutated the space — the
    cause of the AG1 zero-relevance regression.

    Ordered by ``iteration DESC, timestamp DESC`` so:

    * Cold start with both Task 2 ``full`` and Task 3 ``enrichment``
      rows at iteration 0 → enrichment wins (newer timestamp).
    * Mid-loop retry with iteration > 0 ``full`` rows → most recent
      lever iteration wins (higher iteration).
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    rollback_filter = (
        "" if include_rolled_back
        else " AND (rolled_back IS NULL OR rolled_back = false)"
    )
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' "
        f"AND eval_scope IN ('full', 'enrichment')"
        f"{rollback_filter} "
        f"ORDER BY iteration DESC, timestamp DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    for col in ("scores_json", "failures_json", "remaining_failures", "arbiter_actions_json",
                "repeatability_json", "rows_json"):
        if row.get(col) and isinstance(row[col], str):
            try:
                row[col] = json.loads(row[col])
            except (json.JSONDecodeError, TypeError):
                pass
    return row


def load_all_full_iterations(
    spark: SparkSession, run_id: str, catalog: str, schema: str
) -> list[dict]:
    """All iterations with ``eval_scope='full'``, ordered by ``iteration ASC``.

    Each row's JSON columns are parsed into native Python objects.  Used for
    cross-iteration verdict history (e.g. tracking ``genie_correct`` counts
    per question across multiple evaluations).
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' AND eval_scope = 'full' "
        f"ORDER BY iteration ASC",
    )
    if df.empty:
        return []
    rows = df.to_dict("records")
    for row in rows:
        for col in ("scores_json", "failures_json", "remaining_failures",
                     "arbiter_actions_json", "repeatability_json", "rows_json",
                     "reflection_json"):
            if row.get(col) and isinstance(row[col], str):
                try:
                    row[col] = json.loads(row[col])
                except (json.JSONDecodeError, TypeError):
                    pass
    return rows


def load_runs_for_space(
    spark: SparkSession, space_id: str, catalog: str, schema: str
) -> pd.DataFrame:
    """All runs for a Genie Space, ordered by ``started_at DESC``."""
    fqn = _fqn(catalog, schema, TABLE_RUNS)
    return run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE space_id = '{space_id}' ORDER BY started_at DESC",
    )


def load_recent_activity(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    space_id: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """Recent runs across the workspace (or for a single space).

    Used by the Dashboard view.
    """
    fqn = _fqn(catalog, schema, TABLE_RUNS)
    where = f"WHERE space_id = '{space_id}'" if space_id else ""
    return run_query(
        spark,
        f"SELECT * FROM {fqn} {where} ORDER BY started_at DESC LIMIT {limit}",
    )


def load_asi_results(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    iteration: int | None = None,
) -> pd.DataFrame:
    """All ASI judge results for a run, optionally filtered by iteration."""
    fqn = _fqn(catalog, schema, TABLE_ASI)
    where = f"WHERE run_id = '{run_id}'"
    if iteration is not None:
        where += f" AND iteration = {iteration}"
    return run_query(
        spark,
        f"SELECT * FROM {fqn} {where} ORDER BY question_id, judge",
    )


def load_provenance(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    iteration: int | None = None,
    lever: int | None = None,
) -> pd.DataFrame:
    """All provenance records for a run, optionally filtered by iteration/lever."""
    fqn = _fqn(catalog, schema, TABLE_PROVENANCE)
    where = f"WHERE run_id = '{run_id}'"
    if iteration is not None:
        where += f" AND iteration = {iteration}"
    if lever is not None:
        where += f" AND lever = {lever}"
    return run_query(
        spark,
        f"SELECT * FROM {fqn} {where} ORDER BY iteration, lever, question_id",
    )


# ═══════════════════════════════════════════════════════════════════════
# Queued Patches (high-risk, pending human review)
# ═══════════════════════════════════════════════════════════════════════

TABLE_QUEUED_PATCHES = "genie_opt_queued_patches"


def _ensure_queued_patches_table(spark: Any, catalog: str, schema: str) -> None:
    fqn = _fqn(catalog, schema, TABLE_QUEUED_PATCHES)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            run_id              STRING      NOT NULL,
            iteration           INT         NOT NULL,
            patch_type          STRING      NOT NULL,
            target_identifier   STRING      NOT NULL,
            confidence_tier     STRING,
            coverage_analysis   STRING      COMMENT 'JSON blob with schema overlap details',
            blame_iterations    INT,
            status              STRING      NOT NULL,
            created_at          TIMESTAMP   NOT NULL,
            resolved_at         TIMESTAMP
        ) USING DELTA
    """)


def write_queued_patch(
    spark: Any,
    run_id: str,
    iteration: int,
    patch_type: str,
    target_identifier: str,
    catalog: str,
    schema: str,
    *,
    confidence_tier: str = "",
    coverage_analysis: dict | None = None,
    blame_iterations: int = 0,
) -> None:
    """Persist a high-risk patch that needs human approval."""
    _ensure_queued_patches_table(spark, catalog, schema)
    fqn = _fqn(catalog, schema, TABLE_QUEUED_PATCHES)
    now = datetime.now(timezone.utc).isoformat()
    cov_json = json.dumps(coverage_analysis or {}).replace("'", "''")
    target_esc = target_identifier.replace("'", "''")
    execute_delta_write_with_retry(
        spark,
        f"""
        INSERT INTO {fqn} (run_id, iteration, patch_type, target_identifier,
                           confidence_tier, coverage_analysis, blame_iterations,
                           status, created_at)
        VALUES ('{run_id}', {iteration}, '{patch_type}', '{target_esc}',
                '{confidence_tier}', '{cov_json}', {blame_iterations},
                'pending', '{now}')
        """,
        operation_name="write_queued_patch",
        table_name=fqn,
    )


def get_queued_patches(
    spark: Any,
    catalog: str,
    schema: str,
    *,
    status: str = "pending",
) -> list[dict]:
    """Return all queued patches with the given status."""
    _ensure_queued_patches_table(spark, catalog, schema)
    fqn = _fqn(catalog, schema, TABLE_QUEUED_PATCHES)
    try:
        df = run_query(
            spark,
            f"SELECT * FROM {fqn} WHERE status = '{status}' ORDER BY created_at DESC",
        )
        return df.to_dict("records") if not df.empty else []
    except Exception:
        logger.debug("Could not read queued patches table", exc_info=True)
        return []


# ═══════════════════════════════════════════════════════════════════════
# Improvement Suggestions
# ═══════════════════════════════════════════════════════════════════════


def write_suggestion(
    spark: Any,
    catalog: str,
    schema: str,
    suggestion: dict,
) -> None:
    """Insert a single improvement suggestion row."""
    import uuid

    now = datetime.now(timezone.utc).isoformat()
    row = {
        "suggestion_id": suggestion.get("suggestion_id") or str(uuid.uuid4()),
        "run_id": suggestion["run_id"],
        "space_id": suggestion["space_id"],
        "iteration": suggestion.get("iteration"),
        "lever": suggestion.get("lever"),
        "type": suggestion["type"],
        "title": suggestion["title"],
        "rationale": suggestion.get("rationale"),
        "definition": suggestion.get("definition"),
        "affected_questions": json.dumps(suggestion.get("affected_questions", [])),
        "estimated_impact": suggestion.get("estimated_impact"),
        "status": suggestion.get("status", "PROPOSED"),
        "reviewed_by": None,
        "reviewed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    insert_row(spark, catalog, schema, TABLE_SUGGESTIONS, row)
    logger.info(
        "Wrote suggestion %s (%s) for run %s",
        row["suggestion_id"], row["type"], row["run_id"],
    )


def load_suggestions(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
) -> pd.DataFrame:
    """All suggestions for a run, ordered by created_at ASC."""
    fqn = _fqn(catalog, schema, TABLE_SUGGESTIONS)
    return run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' ORDER BY created_at ASC",
    )


def load_suggestion_by_id(
    spark: Any,
    suggestion_id: str,
    catalog: str,
    schema: str,
) -> dict | None:
    """Load a single suggestion by its ID. Returns dict or None."""
    fqn = _fqn(catalog, schema, TABLE_SUGGESTIONS)
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE suggestion_id = '{suggestion_id}' LIMIT 1",
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def update_suggestion_status(
    spark: Any,
    suggestion_id: str,
    catalog: str,
    schema: str,
    status: str,
    reviewed_by: str | None = None,
) -> None:
    """Update the status of a suggestion (ACCEPTED, REJECTED, IMPLEMENTED)."""
    now = datetime.now(timezone.utc).isoformat()
    updates: dict[str, Any] = {
        "status": status,
        "updated_at": now,
    }
    if reviewed_by:
        updates["reviewed_by"] = reviewed_by
        updates["reviewed_at"] = now
    update_row(spark, catalog, schema, TABLE_SUGGESTIONS, {"suggestion_id": suggestion_id}, updates)


def write_finalize_attestation_matrix(
    spark: SparkSession,
    run_id: str,
    *,
    iteration_idx: str,
    train_passes: dict[str, bool],
    heldout_passes: dict[str, bool],
    catalog: str,
    schema: str,
) -> None:
    """Bug #4 Phase 4 — persist per-qid pass/fail for a baseline / finalize
    sweep.

    ``iteration_idx`` is a canonical marker: ``"baseline"`` for the run-
    start sweep, ``"final"`` for the end-of-run sweep. Integer iteration
    values are accepted but not emitted by the standard harness.

    Writes one row per (run_id, qid, iteration_idx). Safe to call multiple
    times per run (different iteration_idx values) but NOT idempotent for
    the same marker — callers should delete existing rows before rewriting
    if re-running.
    """
    if not train_passes and not heldout_passes:
        return
    fqn = _fqn(catalog, schema, TABLE_FINALIZE_ATTESTATION)
    now = datetime.now(timezone.utc).isoformat()
    values: list[str] = []

    def _bool_sql(v: bool | None) -> str:
        if v is None:
            return "NULL"
        return "true" if v else "false"

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    for qid, passed in train_passes.items():
        values.append(
            f"('{_esc(run_id)}', '{_esc(qid)}', '{_esc(iteration_idx)}', "
            f"{_bool_sql(passed)}, false, TIMESTAMP '{now}')"
        )
    for qid, passed in heldout_passes.items():
        values.append(
            f"('{_esc(run_id)}', '{_esc(qid)}', '{_esc(iteration_idx)}', "
            f"{_bool_sql(passed)}, true, TIMESTAMP '{now}')"
        )

    if not values:
        return

    execute_delta_write_with_retry(
        spark,
        (
            f"INSERT INTO {fqn} "
            f"(run_id, qid, iteration_idx, passed, is_heldout, logged_at) "
            f"VALUES {', '.join(values)}"
        ),
        operation_name="write_finalize_attestation",
        table_name=fqn,
    )
    logger.info(
        "Wrote %d finalize_attestation rows for run %s (marker=%s)",
        len(values), run_id, iteration_idx,
    )
