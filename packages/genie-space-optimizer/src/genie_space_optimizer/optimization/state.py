"""
Delta-backed state machine for Genie Agent optimization runs.

Persists stage transitions, iteration scores, and patch records in the
canonical GSO Delta tables. The optimization workflow writes them and the
Workbench reads them.

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
    TABLE_ARTIFACTS,
    TABLE_BENCHMARK_MUTATIONS,
    TABLE_ITERATIONS,
    TABLE_PATCHES,
    TABLE_RUNS,
    TABLE_STAGES,
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
    ADDITIVE_COLUMN_MIGRATIONS,
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

    for table, col, col_def in ADDITIVE_COLUMN_MIGRATIONS:
        # Ignore migrations for tables that are no longer part of the active
        # schema. Existing historical tables are left untouched.
        if table not in _ALL_DDL:
            logger.debug(
                "  [SKIP] %s (retired / not in active DDL set) — no migration for %s",
                _fqn(catalog, schema, table), col,
            )
            continue

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
    "evaluated_count",
    "excluded_count",
    "reflection_json",
    # Version contract: write_iteration emits submitted, observed, and
    # champion-marker columns on every row.
    "config_json",
    "observed_config_json",
    "is_champion",
    # GSO v2 Phase 6: native official eval-run metadata (assessment contract).
    "num_needs_review",
    "eval_run_id",
    "eval_run_status",
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
    config_snapshot: dict | None = None,
    triggered_by: str | None = None,
    llm_model: str | None = None,
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
    if config_snapshot is not None:
        row["config_snapshot"] = json.dumps(config_snapshot)
    if triggered_by is not None:
        row["triggered_by"] = triggered_by
    if llm_model is not None:
        row["llm_model"] = llm_model

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
    convergence_reason: str | None = None,
    job_run_id: str | None = None,
    job_id: str | None = None,
    config_snapshot: dict | None = None,
    warehouse_id: str | None = None,
    max_benchmark_count: int | None = None,
    llm_model: str | None = None,
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
    if convergence_reason is not None:
        updates["convergence_reason"] = convergence_reason
    if job_run_id is not None:
        updates["job_run_id"] = job_run_id
    if job_id is not None:
        updates["job_id"] = job_id
    if config_snapshot is not None:
        updates["config_snapshot"] = json.dumps(config_snapshot)
    if warehouse_id is not None:
        updates["warehouse_id"] = warehouse_id
    if max_benchmark_count is not None:
        updates["max_benchmark_count"] = int(max_benchmark_count)
    if llm_model is not None:
        updates["llm_model"] = llm_model

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

    def _sql_val(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return str(val).lower()
        if isinstance(val, (int, float)):
            return str(val)
        escaped = str(val).replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"

    completed_at: str | None = None
    duration_seconds: float | None = None

    if status in ("COMPLETE", "FAILED", "SKIPPED", "ROLLED_BACK"):
        completed_at = now_iso
        started_df = run_query(
            spark,
            f"SELECT started_at FROM {fqn} "
            f"WHERE run_id = {_sql_val(run_id)} AND stage = {_sql_val(stage)} "
            "AND status = 'STARTED' "
            f"ORDER BY started_at DESC LIMIT 1",
        )
        if not started_df.empty:
            started_ts = pd.Timestamp(started_df.iloc[0]["started_at"])
            if started_ts.tzinfo is None:
                started_ts = started_ts.tz_localize("UTC")
            duration_seconds = (now - started_ts.to_pydatetime()).total_seconds()

    detail_json = json.dumps(detail) if detail else None

    col_names = (
        "run_id, task_key, stage, status, started_at, completed_at, "
        "duration_seconds, lever, iteration, detail_json, error_message"
    )

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
            _sql_val(detail_json),
            _sql_val(error_message),
        ]
    )

    execute_delta_write_with_retry(
        spark,
        f"INSERT INTO {fqn} ({col_names}) VALUES ({vals})",
        operation_name="write_stage",
        table_name=fqn,
    )
    logger.info("Stage %s/%s for run %s", stage, status, run_id)


def write_failure_stage_safely(
    spark: SparkSession,
    run_id: str,
    stage: str,
    *,
    task_key: str | None = None,
    detail: dict | None = None,
    error_message: str | None = None,
    catalog: str,
    schema: str,
) -> None:
    """Best-effort failure telemetry that never masks the original exception."""

    try:
        write_stage(
            spark,
            run_id,
            stage,
            "FAILED",
            task_key=task_key,
            detail=detail,
            error_message=error_message,
            catalog=catalog,
            schema=schema,
        )
    except Exception:
        logger.exception(
            "Could not persist FAILED stage %s for run %s; preserving original error",
            stage,
            run_id,
        )


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


# GSO v2 Phase 4 (D3): whitelist of documented Genie ``serialized_space`` keys
# persisted into ``genie_opt_iterations.config_json`` and
# ``observed_config_json``.
#
# This is INTENTIONALLY DIFFERENT from ``models._SAFE_SPACE_CONFIG_KEYS``
# (the MLflow artifact projection) — it is not a mirror:
#   * it EXTENDS that set with ``benchmarks`` and ``config`` because the
#     per-iteration record must track the FULL effective serialized space
#     (incl. the benchmark questions/SQL and the config block) that the
#     iteration was evaluated against, not just the MLflow snapshot subset;
#   * ``_project_config_for_iteration`` also prefers ``_parsed_space`` and
#     unwraps ``serialized_space`` so a raw fetched config and an
#     already-parsed ``metadata_snapshot`` yield the same documented shape;
#   * it deliberately OMITS ``permissions`` / ``owner`` (ACL / user-identity
#     fields) — there is no consumer of those in Delta and they are exactly
#     the PII we must not copy into the optimization tables (reviewer
#     suggestion #2). The omission is also the guard for a future caller
#     that passes a raw config WITHOUT ``_parsed_space``: ACL/owner data
#     still never reaches ``config_json``.
# Kept self-contained here (not imported from ``models``) so the Delta-only
# write path carries no MLflow dependency (D3: Delta is the sole store).
# Everything else — notably the optimizer-internal ``_*`` keys mutated by
# the lever loop (``_failure_clusters``, ``_data_profile``, ``_parsed_space``,
# ``_uc_columns``, ``_strategy`` …) — is dropped before serialization, which
# also removes the only source of circular references. The projection carries
# no credentials/tokens/secrets: those are not Genie-domain config keys and
# are never present on the parsed space.
_SAFE_ITERATION_CONFIG_KEYS: frozenset[str] = frozenset({
    "version",
    "data_sources",
    "instructions",
    "benchmarks",
    "config",
})


def _decycle_config(obj: Any, _seen: set[int] | None = None) -> Any:
    """Break reference cycles by replacing repeats with ``"<cycle>"``.

    Defense-in-depth for ``_project_config_for_iteration``: the whitelist
    projection already drops the ``_*`` keys where cycles originate, but a
    pathological nested structure must never crash ``json.dumps`` deep
    inside ``write_iteration``. ``_seen`` tracks identities on the current
    recursion path only, so legitimate shared sibling sub-trees survive.
    """
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if isinstance(obj, dict):
        if obj_id in _seen:
            return "<cycle>"
        _seen.add(obj_id)
        try:
            return {k: _decycle_config(v, _seen) for k, v in obj.items()}
        finally:
            _seen.discard(obj_id)
    if isinstance(obj, list):
        if obj_id in _seen:
            return "<cycle>"
        _seen.add(obj_id)
        try:
            return [_decycle_config(v, _seen) for v in obj]
        finally:
            _seen.discard(obj_id)
    if isinstance(obj, tuple):
        return [_decycle_config(v, _seen) for v in obj]
    return obj


def _project_config_for_iteration(config: Any) -> dict[str, Any]:
    """Return a clean, JSON-safe projection of the per-iteration config.

    Accepts either a raw fetched config (which nests the parsed space under
    ``_parsed_space`` / ``serialized_space``) or an already-parsed space dict
    (``metadata_snapshot`` in the lever loop); both yield the same parsed
    ``serialized_space`` shape. Drops every ``_``-prefixed
    (optimizer-internal) key and every key not in
    :data:`_SAFE_ITERATION_CONFIG_KEYS`, then de-cycles. Returns ``{}`` when
    *config* is not a dict or has no whitelisted keys.
    """
    if not isinstance(config, dict):
        return {}
    base = config
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        base = parsed
    else:
        serialized = config.get("serialized_space")
        if isinstance(serialized, dict):
            base = serialized
        elif isinstance(serialized, str) and serialized.strip():
            try:
                loaded = json.loads(serialized)
            except (json.JSONDecodeError, TypeError):
                loaded = None
            if isinstance(loaded, dict):
                base = loaded
    out: dict[str, Any] = {}
    for k, v in base.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if k not in _SAFE_ITERATION_CONFIG_KEYS:
            continue
        out[k] = v
    return _decycle_config(out)


# GSO v2 Phase 8 (arch §7.4): the ordered loop-state columns the two-mode
# controller commits on each per-attempt ``genie_opt_iterations`` row. Order is
# the contract shared by ``write_iteration`` (INSERT) and
# ``update_iteration_loop_state`` (UPDATE); ``(name, kind)`` where kind ∈
# {int, float, str, json}. ``best_iteration`` is intentionally absent — it is a
# ``genie_opt_runs`` column, not an iterations column.
_LOOP_STATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("attempt_no", "int"),
    ("attempt_mode", "str"),
    ("best_accuracy", "float"),
    ("best_config_version_id", "str"),
    ("current_hypothesis", "json"),
    ("do_not_repeat", "json"),
    ("terminal_reason", "str"),
    ("decision", "str"),
    ("decision_reason", "str"),
    ("surgical_attempts_used", "int"),
    ("next_hypothesis", "json"),
    ("target_accuracy", "float"),
    ("max_attempts", "int"),
)


def _render_loop_state_value(kind: str, value: Any, esc: Any) -> str:
    """Render one loop-state value as a Spark-SQL literal (or ``NULL``)."""
    if value is None:
        return "NULL"
    if kind == "int":
        return str(int(value))
    if kind == "float":
        return str(float(value))
    if kind == "json":
        if isinstance(value, str):
            return "NULL" if value == "" else f"'{esc(value)}'"
        return f"'{esc(json.dumps(value))}'"
    # str
    text = str(value)
    return "NULL" if text == "" else f"'{esc(text)}'"


def _render_loop_state_sql(loop_state: dict | None, esc: Any) -> tuple[str, list[str]]:
    """Return ``(column_list_sql, value_literals)`` for the loop-state columns.

    ``column_list_sql`` is a comma-joined column-name string (no trailing comma);
    ``value_literals`` is the matching list of SQL literals. Absent keys ⇒ ``NULL``.
    """
    ls = loop_state or {}
    cols = ", ".join(name for name, _ in _LOOP_STATE_COLUMNS)
    vals = [
        _render_loop_state_value(kind, ls.get(name), esc)
        for name, kind in _LOOP_STATE_COLUMNS
    ]
    return cols, vals


def update_iteration_loop_state(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    *,
    catalog: str,
    schema: str,
    loop_state: dict,
    eval_scope: str | None = None,
) -> None:
    """UPDATE the per-attempt ``genie_opt_iterations`` row with final loop-state.

    GSO v2 Phase 8 (arch §7.5): the surgical candidate's eval row is INSERTed by
    ``_run_gate_checks`` *before* the accept/reject decision is known, so the
    controller commits the post-decision loop-state (``decision``, ``decision_reason``,
    ``best_accuracy``, ``best_config_version_id``, ``terminal_reason``, ...) here at
    the END of each attempt. Only the keys present in ``loop_state`` are SET; this
    is additive over the existing row. Best-effort — a failed UPDATE is logged but
    never aborts the loop."""
    if not loop_state:
        return
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    set_clauses: list[str] = []
    for name, kind in _LOOP_STATE_COLUMNS:
        if name not in loop_state:
            continue
        set_clauses.append(
            f"{name} = {_render_loop_state_value(kind, loop_state.get(name), _esc)}"
        )
    if not set_clauses:
        return

    where = f"run_id = '{_esc(str(run_id))}' AND iteration = {int(iteration)}"
    if eval_scope:
        where += f" AND eval_scope = '{_esc(str(eval_scope))}'"
    try:
        execute_delta_write_with_retry(
            spark,
            f"UPDATE {fqn} SET {', '.join(set_clauses)} WHERE {where}",
            operation_name="update_iteration_loop_state",
            table_name=fqn,
        )
    except Exception:
        logger.warning(
            "Failed to update loop-state for run %s iteration %s (non-fatal)",
            run_id, iteration, exc_info=True,
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
    reflection_json: dict | None = None,
    config_snapshot: dict | None = None,
    observed_config_snapshot: dict | None = None,
    loop_state: dict | None = None,
    rolled_back: bool = False,
) -> None:
    """Insert into ``genie_opt_iterations`` with scores, failures, etc.

    ``rolled_back`` (GSO v2 Phase 8) marks the row excluded from current-state
    selection at INSERT time. ``load_latest_state_iteration`` /
    ``load_latest_full_iteration`` filter ``rolled_back = false``, so a coverage
    attempt that regressed and was rolled back to the frozen baseline must be
    written with ``rolled_back=True`` — otherwise resume/clustering would read the
    rejected coverage eval as current state (the baseline-pollution Phase 8 kills).

    ``loop_state`` (GSO v2 Phase 8, arch §7.4) carries the two-mode controller's
    per-attempt loop-state columns (``attempt_no``, ``attempt_mode``, ``decision``,
    ``best_accuracy``, ``surgical_attempts_used``, ``target_accuracy``, etc.). The
    map is the contract produced by the unified loop; absent keys are written
    ``NULL``. ``genie_opt_iterations`` is the single
    per-attempt truth, so the
    loop commits its state here rather than in a separate artifact."""
    now = datetime.now(timezone.utc).isoformat()
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)

    scores = eval_result.get("scores", {})
    failures = eval_result.get("failures", [])
    remaining = eval_result.get("remaining_failures", failures)
    thresholds_met = eval_result.get("thresholds_met", False)
    if isinstance(thresholds_met, (int, float)):
        thresholds_met = thresholds_met == 1.0

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

    # Bug #2 denominator contract fields.
    # Read from eval_result but fall back sensibly:
    #   evaluated_count defaults to total_questions (matches pre-Bug#2 behavior
    #   where no rows were excluded at runtime).
    #   excluded_count defaults to 0.
    # This keeps the write safe when a result omits explicit denominator fields.
    _total_questions = int(eval_result.get("total_questions", 0) or 0)
    _evaluated_count = eval_result.get("evaluated_count")
    if _evaluated_count is None:
        _evaluated_count = _total_questions
    _excluded_count = int(eval_result.get("excluded_count", 0) or 0)
    # Preserve both sides of the PATCH contract. ``config_json`` is the
    # submitted/local candidate for audit and legacy revert compatibility;
    # ``observed_config_json`` is the authoritative serialized_space returned
    # by a post-write GET. The API can normalize string fragments and SQL text,
    # so only the observed form is strong enough to prove version identity.
    # A missing read-back stays NULL and makes history incomplete rather than
    # causing a false external-drift claim.
    _config_payload = (
        _project_config_for_iteration(config_snapshot)
        if config_snapshot is not None
        else None
    )
    _observed_config_payload = (
        _project_config_for_iteration(observed_config_snapshot)
        if observed_config_snapshot is not None
        else None
    )

    # GSO v2 Phase 6: native official eval-run metadata surfaced to the
    # Workbench UI (assessment-centric contract). ``num_needs_review`` lets the
    # UI distinguish review-pending rows from failures; ``eval_run_id`` /
    # ``eval_run_status`` reference the underlying native run. All three are
    # produced by ``build_eval_output_from_official`` and default to NULL when
    # the native runner does not emit them.
    _num_needs_review = eval_result.get("num_needs_review")
    _num_needs_review = (
        int(_num_needs_review) if isinstance(_num_needs_review, (int, float)) else None
    )
    _eval_run_id = eval_result.get("eval_run_id") or None
    _eval_run_status = eval_result.get("eval_run_status") or None

    # Per-attempt loop-state columns emitted by the native optimizer.
    _loop_state_cols, _loop_state_vals = _render_loop_state_sql(loop_state, _esc)

    col_names = (
        "run_id, iteration, lever, eval_scope, timestamp, "
        "overall_accuracy, total_questions, correct_count, scores_json, failures_json, "
        "remaining_failures, "
        "thresholds_met, rows_json, reflection_json, "
        "evaluated_count, excluded_count, "
        "rolled_back, "
        "config_json, observed_config_json, is_champion, "
        "num_needs_review, eval_run_id, eval_run_status, "
        + _loop_state_cols
    )
    vals = ", ".join(
        [
            f"'{run_id}'",
            str(iteration),
            str(lever) if lever is not None else "NULL",
            f"'{eval_scope}'",
            f"TIMESTAMP '{now}'",
            str(eval_result.get("overall_accuracy", 0.0)),
            str(_total_questions),
            str(eval_result.get("correct_count", 0)),
            f"'{_esc(json.dumps(scores))}'",
            _opt_json(failures),
            _opt_json(remaining),
            str(thresholds_met).lower(),
            _opt_json(rows_data),
            _opt_json(reflection_json),
            str(int(_evaluated_count)),
            str(_excluded_count),
            "true" if rolled_back else "false",
            _opt_json(_config_payload) if _config_payload else "NULL",
            _opt_json(_observed_config_payload) if _observed_config_payload else "NULL",
            "false",
            str(_num_needs_review) if _num_needs_review is not None else "NULL",
            f"'{_esc(str(_eval_run_id))}'" if _eval_run_id is not None else "NULL",
            f"'{_esc(str(_eval_run_status))}'" if _eval_run_status is not None else "NULL",
        ]
        + _loop_state_vals
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


def mark_iteration_rolled_back(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    *,
    catalog: str,
    schema: str,
    eval_scope: str | None = None,
    reason: str = "",
) -> None:
    """Set ``rolled_back=true`` on a SPECIFIC ``genie_opt_iterations`` row.

    Unlike :func:`mark_patches_rolled_back` (which marks every row at
    ``iteration``), this is scoped by ``eval_scope`` for compatibility with older
    runs that wrote multiple scopes for one iteration. The active unified loop
    writes one full-scope row per baseline/patch attempt.

    This is REQUIRED, not best-effort: callers need a rejected row to be
    unselectable before continuing. ``run_id`` / ``eval_scope`` are escaped the
    same way as the rest of the per-row writers (SQL-literal quote-doubling).
    """
    now = datetime.now(timezone.utc).isoformat()
    iters_fqn = _fqn(catalog, schema, TABLE_ITERATIONS)

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "''")

    safe_reason = _esc(str(reason or ""))
    where = f"run_id = '{_esc(str(run_id))}' AND iteration = {int(iteration)}"
    if eval_scope:
        where += f" AND eval_scope = '{_esc(str(eval_scope))}'"
    execute_delta_write_with_retry(
        spark,
        f"UPDATE {iters_fqn} SET rolled_back = true, "
        f"rolled_back_at = TIMESTAMP '{now}', "
        f"rollback_reason = '{safe_reason}' WHERE {where}",
        operation_name="mark_iteration_rolled_back",
        table_name=iters_fqn,
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
        # deployed Genie Agent state is accurate. Only the iteration
        # filter downstream is affected; readers fall back to reading
        # ``rolled_back`` from patches via a join if needed.
        logger.warning(
            "Failed to stamp rolled_back on iterations row run=%s iter=%d",
            run_id, iteration, exc_info=True,
        )
    logger.info("Rolled back patches + iteration row for run %s iteration %d: %s", run_id, iteration, reason)


def mark_champion_iteration(
    spark: SparkSession,
    run_id: str,
    iteration: int,
    *,
    catalog: str,
    schema: str,
    eval_scope: str | None = None,
) -> None:
    """Stamp ``is_champion=true`` on the champion iteration row (GSO v2 Phase 4, D3).

    The champion is the best ``genie_opt_iterations`` row as chosen by the
    EXISTING Delta-driven selection (``models.promote_best_model`` —
    full/enrichment scope, rolled-back rows excluded, ``idxmax`` of
    ``overall_accuracy``). This writer does not re-derive that decision; it
    only persists it in Delta. There is intentionally **no UC model
    registration** here (Phase 5 decommissions the MLflow champion alias;
    this Delta marker is the durable signal that survives it).

    Implemented as a SINGLE run-scoped conditional UPDATE that recomputes
    ``is_champion`` for every row of the run as the boolean predicate
    ``(iteration = <best> [AND eval_scope = <scope>])``. This is atomic: a
    row can never be cleared without the new champion being set in the same
    statement (there is no clear-then-set window where a mid-write failure
    could leave the run with NO champion). ``eval_scope`` should be supplied
    (e.g. ``"full"`` or ``"enrichment"``) so the marker lands on the selected
    row rather than a same-iteration slice/p0 gate row. Best-effort: a write
    failure is logged, not raised — champion marking is a transparency
    signal, never a gating mechanism, and on failure the prior champion
    state is left intact.
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    safe_run = run_id.replace("'", "''")
    if eval_scope:
        safe_scope = eval_scope.replace("'", "''")
        champion_pred = (
            f"(iteration = {int(iteration)} AND eval_scope = '{safe_scope}')"
        )
    else:
        champion_pred = f"(iteration = {int(iteration)})"
    try:
        execute_delta_write_with_retry(
            spark,
            f"UPDATE {fqn} SET is_champion = {champion_pred} "
            f"WHERE run_id = '{safe_run}'",
            operation_name="mark_champion_iteration",
            table_name=fqn,
        )
        logger.info(
            "Marked champion iteration for run %s: iteration=%d scope=%s",
            run_id, int(iteration), eval_scope or "(any)",
        )
    except Exception:
        logger.warning(
            "Failed to mark champion iteration run=%s iter=%s scope=%s",
            run_id, iteration, eval_scope, exc_info=True,
        )


# ── Provenance Write Functions ───────────────────────────────────────────


def write_benchmark_mutations(
    spark: SparkSession,
    run_id: str,
    rows: list[dict],
    *,
    catalog: str,
    schema: str,
) -> int:
    """Append GSO benchmark-mutation provenance rows (v2 §3.5).

    Each ``row`` describes one mutation (or advisory) GSO recorded against
    the user's live Genie Agent benchmark set:
    ``{question_id, op, before, after, reason}`` where ``op`` ∈
    {``added``, ``removed``, ``changed``, ``prune_recommended``}.
    ``prune_recommended`` is a NON-mutating advisory row: the over-window
    (30–40) prune recommendation, recorded for transparency because the
    publisher never auto-prunes. ``before`` / ``after`` are
    ``{question, sql}`` dicts (JSON-serialized here) or ``None``. No-op when
    ``rows`` is empty. Best-effort: a write failure is logged but never
    aborts preflight (the push itself is the source of truth).

    Returns the number of rows written.
    """
    if not rows:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    written = 0

    def _opt_json(val: Any) -> str | None:
        if val is None:
            return None
        try:
            return json.dumps(val, default=str)[:5000]
        except (TypeError, ValueError):
            return None

    for r in rows:
        op = str(r.get("op", "")).strip()
        if op not in ("added", "removed", "changed", "prune_recommended"):
            logger.debug("Skipping benchmark mutation with bad op=%r", op)
            continue
        payload: dict[str, Any] = {
            "run_id": run_id,
            "question_id": str(r.get("question_id", "") or "")[:200],
            "op": op,
            "before": _opt_json(r.get("before")),
            "after": _opt_json(r.get("after")),
            "reason": (str(r.get("reason", "")) or "")[:500] or None,
            "logged_at": now,
        }
        try:
            insert_row(
                spark, catalog, schema, TABLE_BENCHMARK_MUTATIONS, payload,
            )
            written += 1
        except Exception:
            logger.warning(
                "Failed to write benchmark mutation (op=%s qid=%s) for run %s",
                op, payload["question_id"], run_id, exc_info=True,
            )
    if written:
        logger.info(
            "Wrote %d benchmark mutation row(s) for run %s", written, run_id,
        )
    return written


# GSO v2 orchestration (Phase 7, arch §7.1–7.3): the stage-level handoff
# blob kinds carried by genie_opt_artifacts. Per-attempt scored truth
# (scores, loop-state, patches, decisions) is NOT an artifact — it lives in
# genie_opt_iterations / genie_opt_patches / genie_eval_lever_loop_decisions.
ARTIFACT_KINDS: tuple[str, ...] = (
    "run_manifest",
    "wide_schema_inventory",
    "wide_schema_evidence",
    "wide_schema_selection_plan",
    "wide_schema_audit",
    "wide_schema_profile_telemetry",
    "wide_schema_prompt_telemetry",
    "space_metadata",
    "benchmark_qc",
    "space_quality_enrichment",
    "publish_record",
)


def write_artifact(
    spark: SparkSession,
    run_id: str,
    artifact_kind: str,
    payload: dict | list | str | None,
    *,
    catalog: str,
    schema: str,
    stage_name: str | None = None,
    iteration: int | None = None,
    source_notebook: str | None = None,
    parent_artifact_id: str | None = None,
) -> str | None:
    """Append one stage-level handoff blob to ``genie_opt_artifacts`` (arch §7.1).

    ``artifact_kind`` must be one of :data:`ARTIFACT_KINDS`. ``payload`` is
    JSON-serialized into ``artifact_json``; a ``content_hash`` is computed for
    dedupe / replay safety. Returns the generated ``artifact_id`` (or ``None``
    on a swallowed write failure — best-effort, never aborts the notebook).
    """
    import hashlib
    import uuid

    if artifact_kind not in ARTIFACT_KINDS:
        logger.warning(
            "write_artifact: unknown artifact_kind=%r (expected one of %s)",
            artifact_kind, ARTIFACT_KINDS,
        )

    if payload is None:
        artifact_json = None
    elif isinstance(payload, str):
        artifact_json = payload
    else:
        try:
            artifact_json = json.dumps(payload, default=str, sort_keys=True)
        except (TypeError, ValueError):
            logger.warning(
                "write_artifact: payload for kind=%s not JSON-serializable",
                artifact_kind,
            )
            artifact_json = None

    content_hash = (
        hashlib.sha256(artifact_json.encode("utf-8")).hexdigest()
        if artifact_json is not None
        else None
    )
    artifact_id = str(uuid.uuid4())
    payload_row: dict[str, Any] = {
        "artifact_id": artifact_id,
        "run_id": run_id,
        "stage_name": stage_name,
        "iteration": int(iteration) if iteration is not None else None,
        "artifact_kind": artifact_kind,
        "artifact_json": artifact_json,
        "content_hash": content_hash,
        "parent_artifact_id": parent_artifact_id,
        "source_notebook": source_notebook or stage_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        insert_row(
            spark,
            catalog,
            schema,
            TABLE_ARTIFACTS,
            payload_row,
            base64_string_columns={"artifact_json"},
        )
        logger.info(
            "Wrote %s artifact %s for run %s", artifact_kind, artifact_id, run_id,
        )
        return artifact_id
    except Exception:
        logger.warning(
            "Failed to write %s artifact for run %s", artifact_kind, run_id,
            exc_info=True,
        )
        return None


def load_artifacts(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    artifact_kind: str | None = None,
) -> pd.DataFrame:
    """Read ``genie_opt_artifacts`` rows for ``run_id`` (optionally one kind).

    Most-recent-first. Returns an empty DataFrame when the table is absent or
    has no matching rows (best-effort — never raises).
    """
    fqn = _fqn(catalog, schema, TABLE_ARTIFACTS)
    where = f"run_id = '{run_id}'"
    if artifact_kind:
        where += f" AND artifact_kind = '{artifact_kind}'"
    try:
        return run_query(
            spark, f"SELECT * FROM {fqn} WHERE {where} ORDER BY created_at DESC",
        )
    except Exception:
        logger.debug("load_artifacts: no rows for run %s", run_id, exc_info=True)
        return pd.DataFrame()


def load_latest_artifact_payload(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    artifact_kind: str,
) -> dict[str, Any] | None:
    """Return the newest JSON-object payload for one artifact kind."""
    rows = load_artifacts(
        spark,
        run_id,
        catalog,
        schema,
        artifact_kind=artifact_kind,
    )
    if rows.empty:
        return None
    raw = rows.iloc[0].get("artifact_json")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning(
            "Artifact %s for run %s contains invalid JSON",
            artifact_kind,
            run_id,
        )
        return None
    return payload if isinstance(payload, dict) else None


def load_latest_artifact_record(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    artifact_kind: str,
) -> dict[str, Any] | None:
    """Load and content-hash verify the newest artifact row.

    Unlike :func:`load_latest_artifact_payload`, this helper is strict: malformed
    JSON or a hash mismatch raises because wide-schema inventory and plan
    handoffs are required deterministic state.
    """
    import hashlib

    rows = load_artifacts(
        spark,
        run_id,
        catalog,
        schema,
        artifact_kind=artifact_kind,
    )
    if rows.empty:
        return None
    row = rows.iloc[0].to_dict()
    raw = row.get("artifact_json")
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError(f"Artifact {artifact_kind} for run {run_id} has no JSON payload")
    actual_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if str(row.get("content_hash") or "") != actual_hash:
        raise RuntimeError(f"Artifact {artifact_kind} for run {run_id} failed content-hash verification")
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Artifact {artifact_kind} for run {run_id} contains invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Artifact {artifact_kind} for run {run_id} is not a JSON object")
    row["payload"] = payload
    return row


def write_required_artifact(
    spark: SparkSession,
    run_id: str,
    artifact_kind: str,
    payload: dict[str, Any],
    *,
    catalog: str,
    schema: str,
    stage_name: str,
    source_notebook: str,
    iteration: int | None = None,
    parent_artifact_id: str | None = None,
) -> dict[str, Any]:
    """Write, read back, and hash-verify a required notebook handoff."""
    artifact_id = write_artifact(
        spark,
        run_id,
        artifact_kind,
        payload,
        catalog=catalog,
        schema=schema,
        stage_name=stage_name,
        iteration=iteration,
        source_notebook=source_notebook,
        parent_artifact_id=parent_artifact_id,
    )
    if not artifact_id:
        raise RuntimeError(f"Required artifact {artifact_kind} could not be persisted")
    record = load_latest_artifact_record(
        spark,
        run_id,
        catalog,
        schema,
        artifact_kind,
    )
    if record is None or record.get("artifact_id") != artifact_id:
        raise RuntimeError(f"Required artifact {artifact_kind} could not be read back")
    return record


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
    for col in ("scores_json", "failures_json", "remaining_failures", "rows_json"):
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
    """Latest iteration row reflecting current Genie Agent state.

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
    for col in ("scores_json", "failures_json", "remaining_failures", "rows_json"):
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
                    "rows_json", "reflection_json"):
            if row.get(col) and isinstance(row[col], str):
                try:
                    row[col] = json.loads(row[col])
                except (json.JSONDecodeError, TypeError):
                    pass
    return rows


def load_all_scored_iterations(
    spark: SparkSession, run_id: str, catalog: str, schema: str
) -> list[dict]:
    """All full-scope iterations plus historical enrichment rows.

    The unified loop writes only ``eval_scope='full'``. Historical runs may have
    ``eval_scope='enrichment'`` rows; publish/audit keeps reading them so old
    trajectories remain explainable, while champion selection restricts
    promotion to full-scope rows. Ordered by ``iteration ASC, timestamp ASC``;
    JSON columns parsed like ``load_all_full_iterations``.
    """
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' "
        f"AND eval_scope IN ('full', 'enrichment') "
        f"ORDER BY iteration ASC, timestamp ASC",
    )
    if df.empty:
        return []
    rows = df.to_dict("records")
    for row in rows:
        for col in ("scores_json", "failures_json", "remaining_failures",
                    "rows_json", "reflection_json"):
            if row.get(col) and isinstance(row[col], str):
                try:
                    row[col] = json.loads(row[col])
                except (json.JSONDecodeError, TypeError):
                    pass
    return rows


def load_runs_for_space(
    spark: SparkSession, space_id: str, catalog: str, schema: str
) -> pd.DataFrame:
    """All runs for a Genie Agent, ordered by ``started_at DESC``."""
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


# ═══════════════════════════════════════════════════════════════════════
# Queued Patches (high-risk, pending human review)
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# Improvement Suggestions
# ═══════════════════════════════════════════════════════════════════════
