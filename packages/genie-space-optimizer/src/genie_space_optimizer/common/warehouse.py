"""SQL Warehouse helpers using the Statement Execution API.

These utilities bypass Spark Connect entirely and are used by both the
standalone app's fallback path (``backend/routes/spaces.py``) and the
integration module (``integration/``).
"""

from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_DEFAULT_RE = re.compile(
    r"\bDEFAULT\s+(?:'[^']*'|[A-Za-z0-9_\-.+]+)",
    re.IGNORECASE,
)

_REQUIRED_RUN_COLUMNS = (
    "job_id",
    "llm_model",
    "benchmark_policy",
    "benchmark_mutation_count",
)


def sql_warehouse_query(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
) -> Any:
    """Execute SQL via the Statement Execution API and return a pandas DataFrame."""
    import pandas as pd
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    from genie_space_optimizer.common.query_tags import gso_query_tags

    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        query_tags=gso_query_tags(purpose="optimization"),
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        manifest_schema = resp.manifest.schema if resp.manifest else None
        schema_cols = manifest_schema.columns if manifest_schema else None
        columns = [str(c.name or "") for c in (schema_cols or [])]
        rows: list[dict] = []
        if resp.result and resp.result.data_array:
            for row_data in resp.result.data_array:
                rows.append(dict(zip(columns, row_data)))
        return pd.DataFrame(rows, columns=pd.Index(columns) if columns else None)
    error_msg = ""
    if resp.status and resp.status.error:
        error_msg = resp.status.error.message or str(resp.status.error)
    raise RuntimeError(f"SQL warehouse query failed: {error_msg}")


def sql_warehouse_execute(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
) -> None:
    """Execute a DML/DDL statement via the SQL warehouse (no result expected)."""
    from databricks.sdk.service.sql import StatementState
    from genie_space_optimizer.common.query_tags import gso_query_tags

    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        query_tags=gso_query_tags(purpose="optimization"),
    )
    if resp.status and resp.status.state != StatementState.SUCCEEDED:
        error_msg = ""
        if resp.status.error:
            error_msg = resp.status.error.message or str(resp.status.error)
        raise RuntimeError(f"SQL warehouse execute failed: {error_msg}")


def _column_names_from_describe(df: Any) -> set[str]:
    if getattr(df, "empty", True) or "col_name" not in getattr(df, "columns", []):
        return set()

    names: set[str] = set()
    for raw in df["col_name"].tolist():
        if raw is None:
            continue
        name = str(raw).strip()
        if not name or name.startswith("#"):
            continue
        names.add(name.lower())
    return names


def _strip_inline_default(col_def: str) -> tuple[str, str | None]:
    default_match = _DEFAULT_RE.search(col_def)
    if not default_match:
        return col_def, None
    return _DEFAULT_RE.sub("", col_def).strip(), default_match.group()


def _wh_apply_one_migration(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    fqn: str,
    col: str,
    col_def: str,
) -> None:
    add_def, default_clause = _strip_inline_default(col_def)
    try:
        sql_warehouse_execute(
            ws,
            warehouse_id,
            f"ALTER TABLE {fqn} ADD COLUMN {col} {add_def}",
        )
        logger.info("Added missing Delta column %s.%s via SQL warehouse", fqn, col)
    except Exception as exc:
        msg = str(exc).lower()
        if "already exists" in msg:
            return
        logger.error("Could not ADD COLUMN %s.%s via SQL warehouse: %s", fqn, col, exc)
        return

    if default_clause:
        try:
            sql_warehouse_execute(
                ws,
                warehouse_id,
                f"ALTER TABLE {fqn} ALTER COLUMN {col} SET {default_clause}",
            )
        except Exception as exc:
            logger.warning(
                "Column %s.%s was added, but SET DEFAULT was rejected "
                "(writers set explicit values): %s",
                fqn,
                col,
                exc,
            )


def _wh_describe_columns(
    ws: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
) -> set[str]:
    df = sql_warehouse_query(ws, warehouse_id, f"DESCRIBE TABLE {fqn}")
    return _column_names_from_describe(df)


def _wh_verify_required_run_columns(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> None:
    fqn = f"{catalog}.{schema}.genie_opt_runs"
    try:
        present = _wh_describe_columns(ws, warehouse_id, fqn)
    except Exception as exc:
        raise RuntimeError(
            f"Could not verify required columns on {fqn}: {exc}"
        ) from exc

    missing = [col for col in _REQUIRED_RUN_COLUMNS if col.lower() not in present]
    if missing:
        raise RuntimeError(
            f"{fqn} is missing columns required to launch optimization runs: "
            f"{', '.join(missing)}. Run the GSO table migration or grant the "
            "app service principal permission to ALTER the table."
        )


def wh_ensure_optimization_tables(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> None:
    """Create and migrate GSO Delta state tables via SQL Warehouse.

    The Databricks App trigger path is warehouse-first because Spark Connect can
    lose credentials in Apps. Keep this path in lockstep with the Spark
    bootstrapper so newly added columns exist before ``wh_create_run`` inserts.
    """
    from genie_space_optimizer.optimization.ddl import (
        ADDITIVE_COLUMN_MIGRATIONS,
        _ALL_DDL,
    )

    try:
        sql_warehouse_execute(
            ws,
            warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
        )
    except Exception as exc:
        msg = str(exc)
        if "PERMISSION_DENIED" in msg or "ACCESS_DENIED" in msg:
            logger.warning(
                "Cannot CREATE SCHEMA %s.%s via SQL warehouse; assuming it "
                "already exists and continuing with table initialization.",
                catalog,
                schema,
            )
        else:
            raise

    for name, ddl in _ALL_DDL.items():
        resolved = ddl.replace("{catalog}", catalog).replace("{schema}", schema)
        try:
            sql_warehouse_execute(ws, warehouse_id, resolved)
            logger.info("Ensured GSO table %s.%s.%s via SQL warehouse", catalog, schema, name)
        except Exception as exc:
            msg = str(exc)
            if "PERMISSION_DENIED" in msg or "ACCESS_DENIED" in msg:
                logger.warning(
                    "Cannot create table %s.%s.%s via SQL warehouse; it may "
                    "already exist or the principal may lack CREATE_TABLE.",
                    catalog,
                    schema,
                    name,
                )
            elif "SCHEMA_NOT_FOUND" in msg:
                raise RuntimeError(
                    f"Schema {catalog}.{schema} does not exist and could not be created."
                ) from exc
            else:
                raise

    for table, col, col_def in ADDITIVE_COLUMN_MIGRATIONS:
        fqn = f"{catalog}.{schema}.{table}"
        try:
            existing = _wh_describe_columns(ws, warehouse_id, fqn)
        except Exception:
            logger.warning(
                "Could not DESCRIBE %s via SQL warehouse while checking migrations",
                fqn,
                exc_info=True,
            )
            continue

        if col.lower() in existing:
            continue

        _wh_apply_one_migration(
            ws,
            warehouse_id,
            fqn=fqn,
            col=col,
            col_def=col_def,
        )

    _wh_verify_required_run_columns(ws, warehouse_id, catalog, schema)


def wh_create_run(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    run_id: str,
    space_id: str,
    domain: str,
    catalog: str,
    schema: str,
    apply_mode: str = "genie_config",
    levers: list[int] | None = None,
    triggered_by: str | None = None,
    config_snapshot: dict | None = None,
    llm_model: str | None = None,
    benchmark_policy: str = "repair_allowed",
    run_kind: str | None = None,
    status: str = "QUEUED",
    set_completed: bool = False,
    max_iterations: int | None = None,
) -> None:
    """Insert a run row via SQL warehouse (QUEUED by default).

    The single run-insert writer. MV-D23 sentinel advice runs reuse it through
    :func:`wh_create_advice_run` rather than a parallel INSERT — one writer, so
    the ``run_kind`` discriminator and the column set can never drift between
    the two paths. ``run_kind`` defaults to ``optimization``; ``status`` and
    ``set_completed`` let the advice caller write a born-terminal row.
    """
    from genie_space_optimizer.common.config import (
        DEFAULT_LEVER_ORDER,
        MAX_ITERATIONS,
        MV_RUN_KIND_OPTIMIZATION,
    )

    kind = run_kind or MV_RUN_KIND_OPTIMIZATION
    iterations = MAX_ITERATIONS if max_iterations is None else max_iterations
    snap_json = json.dumps(config_snapshot) if config_snapshot else ""
    snap_sql = "''"
    if snap_json:
        snap_b64 = base64.b64encode(snap_json.encode("utf-8")).decode("ascii")
        snap_sql = f"CAST(unbase64('{snap_b64}') AS STRING)"
    levers_json = json.dumps(levers if levers is not None else DEFAULT_LEVER_ORDER)
    user = (triggered_by or "").replace("'", "''")
    model_escaped = llm_model.replace("'", "''") if llm_model else ""
    model_sql = f"'{model_escaped}'" if model_escaped else "NULL"
    policy_escaped = benchmark_policy.replace("'", "''")
    status_escaped = status.replace("'", "''")
    kind_escaped = kind.replace("'", "''")
    completed_col = ", completed_at" if set_completed else ""
    completed_val = ", current_timestamp()" if set_completed else ""

    # GSO v2 Phase 5 (D3): the ``experiment_name`` column was scrubbed; the
    # surviving MLflow tracing self-resolves a deterministic experiment path in
    # ``preflight_persist_benchmark_corpus`` (no pointer column needed).
    sql = (
        f"INSERT INTO {catalog}.{schema}.genie_opt_runs "
        f"(run_id, space_id, domain, catalog, uc_schema, status, started_at, "
        f"max_iterations, levers, apply_mode, updated_at, "
        f"triggered_by, config_snapshot, llm_model, benchmark_policy, "
        f"benchmark_mutation_count, run_kind{completed_col}) VALUES ("
        f"'{run_id}', '{space_id}', '{domain}', '{catalog}', "
        f"'{catalog}.{schema}', '{status_escaped}', current_timestamp(), "
        f"{iterations}, '{levers_json}', '{apply_mode}', current_timestamp(), "
        f"'{user}', {snap_sql}, {model_sql}, '{policy_escaped}', 0, "
        f"'{kind_escaped}'{completed_val})"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info("Created run %s (kind=%s, status=%s) via SQL warehouse", run_id, kind, status)


def wh_create_advice_run(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    run_id: str,
    space_id: str,
    domain: str,
    catalog: str,
    schema: str,
    triggered_by: str | None = None,
    config_snapshot: dict | None = None,
    llm_model: str | None = None,
) -> None:
    """Create a born-terminal MV-D23 sentinel advice run.

    Guardrail (i): the row is written with :data:`MV_ADVICE_RUN_STATUS` and a
    ``completed_at`` immediately — never ``QUEUED``/``IN_PROGRESS`` — so
    :func:`wh_reconcile_active_runs` (active set ``{QUEUED, IN_PROGRESS}``) can
    never adopt it. ``run_kind = mv_advice`` excludes it from run-history and
    accuracy aggregates through the pinned :data:`MV_ADVICE_RUN_EXCLUSION`
    predicate. It runs no eval, so ``max_iterations = 0`` and ``levers = []``.

    Reuses :func:`wh_create_run` (one run-insert writer). The caller must have
    already run :func:`wh_ensure_optimization_tables` so ``run_kind`` exists.
    """
    from genie_space_optimizer.common.config import (
        MV_ADVICE_RUN_STATUS,
        MV_RUN_KIND_ADVICE,
    )

    wh_create_run(
        ws,
        warehouse_id,
        run_id=run_id,
        space_id=space_id,
        domain=domain,
        catalog=catalog,
        schema=schema,
        triggered_by=triggered_by,
        config_snapshot=config_snapshot,
        llm_model=llm_model,
        levers=[],
        run_kind=MV_RUN_KIND_ADVICE,
        status=MV_ADVICE_RUN_STATUS,
        set_completed=True,
        max_iterations=0,
    )
    logger.info("Created born-terminal MV-D23 advice run %s for space %s", run_id, space_id)


def wh_write_stage(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    run_id: str,
    stage: str,
    status: str,
    catalog: str,
    schema: str,
    detail: dict | None = None,
    error_message: str | None = None,
) -> None:
    """SQL-warehouse twin of :func:`optimization.state.write_stage` (MV-D21 pin).

    The interactive advice path has no SparkSession, so it cannot call the Spark
    ``write_stage``; without a twin the advice run left no ``genie_opt_stages``
    row, and MV-D31 hydration ("last scanned + N proposals", skip reason,
    duration) had nothing to read. This writes the same columns the Spark writer
    does, and — for a terminal status — computes ``duration_seconds`` the same
    way: by diffing the matching ``STARTED`` row's ``started_at``. ``detail`` is
    JSON-serialized into ``detail_json`` (base64-routed so nested JSON survives
    the warehouse's string-escape mode, as the candidate/consent JSON columns
    are). The advice caller writes one ``STARTED`` then one terminal row for the
    :data:`MV_ADVISOR_PHASE_NAME` stage; the four interactive sub-stages ride the
    SSE ``on_stage`` seam and are transient progress, never persisted here.

    Only the columns the advice path uses are exposed (no ``task_key`` / ``lever``
    / ``iteration``, which are job-orchestration concerns); the DDL leaves them
    nullable, so an advice stage row is well-formed without them.
    """
    from genie_space_optimizer.common.config import TABLE_STAGES

    if not run_id:
        raise ValueError("run_id is required")
    if not stage:
        raise ValueError("stage is required")

    fqn = f"{catalog}.{schema}.{TABLE_STAGES}"

    completed_at_sql = "NULL"
    duration_sql = "NULL"
    if status in ("COMPLETE", "FAILED", "SKIPPED", "ROLLED_BACK"):
        completed_at_sql = "current_timestamp()"
        # Duration is the diff against this stage's STARTED row, computed in-engine
        # so it uses the warehouse's own clock for both endpoints (no client-skew).
        duration_sql = (
            "(SELECT unix_timestamp(current_timestamp()) - "
            f"unix_timestamp(MAX(started_at)) FROM {fqn} "
            f"WHERE run_id = {_wh_literal(run_id)} AND stage = {_wh_literal(stage)} "
            "AND status = 'STARTED')"
        )

    detail_json = json.dumps(detail) if detail else None

    cols = (
        "run_id, stage, status, started_at, completed_at, "
        "duration_seconds, detail_json, error_message"
    )
    vals = ", ".join(
        [
            _wh_literal(run_id),
            _wh_literal(stage),
            _wh_literal(status),
            "current_timestamp()",
            completed_at_sql,
            duration_sql,
            _wh_literal(detail_json, encode=True) if detail_json else "NULL",
            _wh_literal(error_message),
        ]
    )
    # INSERT ... SELECT, not VALUES: the terminal-row duration is a scalar
    # subquery (the STARTED-row diff), and Databricks rejects scalar subqueries
    # in a VALUES clause (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.
    # SCALAR_SUBQUERY_IN_VALUES) — a SELECT projection permits it.
    sql_warehouse_execute(
        ws, warehouse_id, f"INSERT INTO {fqn} ({cols}) SELECT {vals}"
    )
    logger.info("Stage %s/%s for run %s (warehouse)", stage, status, run_id)


def wh_load_latest_advice_scan(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    space_id: str,
) -> dict | None:
    """Newest advice scan for a space, for MV-D31 hydrate-on-mount.

    Joins the newest ``mv_advice`` run (MV-D23 sentinel) for ``space_id`` to its
    terminal :data:`MV_ADVISOR_PHASE_NAME` stage row and returns the fields the
    hydrated panel shows: ``scanned_at`` (the run's ``completed_at``),
    ``duration_seconds`` (real wall time from the stage), ``status``, and the
    ``skip_reason`` / ``measures_found`` derived from ``detail_json`` — the
    note-2 source, a derivation from the existing stage detail, not a new column.
    Returns ``None`` when the space has never been scanned, which the panel
    renders as the never-scanned state rather than an empty result.
    """
    from genie_space_optimizer.common.config import (
        MV_RUN_KIND_ADVICE,
        TABLE_RUNS,
        TABLE_STAGES,
    )
    from genie_space_optimizer.optimization.mv_advisor import MV_ADVISOR_PHASE_NAME

    runs_fqn = f"{catalog}.{schema}.{TABLE_RUNS}"
    stages_fqn = f"{catalog}.{schema}.{TABLE_STAGES}"
    df = sql_warehouse_query(
        ws,
        warehouse_id,
        f"SELECT r.run_id AS run_id, r.completed_at AS scanned_at, "
        f"s.status AS status, s.duration_seconds AS duration_seconds, "
        f"s.detail_json AS detail_json "
        f"FROM {runs_fqn} r "
        f"LEFT JOIN {stages_fqn} s "
        f"ON s.run_id = r.run_id AND s.stage = {_wh_literal(MV_ADVISOR_PHASE_NAME)} "
        f"AND s.status <> 'STARTED' "
        f"WHERE r.space_id = {_wh_literal(space_id)} "
        f"AND r.run_kind = {_wh_literal(MV_RUN_KIND_ADVICE)} "
        f"ORDER BY r.started_at DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    detail_raw = row.get("detail_json")
    detail: dict = {}
    if detail_raw:
        try:
            detail = json.loads(detail_raw)
        except (ValueError, TypeError):
            detail = {}
    duration = row.get("duration_seconds")
    return {
        "run_id": row.get("run_id"),
        "scanned_at": row.get("scanned_at"),
        "status": row.get("status") or detail.get("status"),
        "duration_seconds": float(duration) if duration is not None else None,
        "skip_reason": detail.get("skip_reason"),
        "measures_found": detail.get("measures_found"),
    }


def wh_load_run(
    ws: WorkspaceClient,
    warehouse_id: str,
    run_id: str,
    catalog: str,
    schema_name: str,
) -> dict | None:
    """Read a single run from Delta via SQL Warehouse."""
    df = sql_warehouse_query(
        ws,
        warehouse_id,
        f"SELECT * FROM {catalog}.{schema_name}.genie_opt_runs "
        f"WHERE run_id = '{run_id}'",
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def wh_reconcile_active_runs(
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    warehouse_id: str,
    runs_df,
    catalog: str,
    schema_name: str,
    *,
    stale_queue_minutes: int = 10,
    terminal_job_states: frozenset[str] | None = None,
) -> bool:
    """Mark stale/terminated job-backed active rows as FAILED via SQL Warehouse.

    This is the warehouse-only equivalent of ``_reconcile_active_runs`` in
    ``backend/routes/spaces.py``.  It prevents old QUEUED/IN_PROGRESS rows
    from permanently blocking new optimization runs.
    """
    from datetime import datetime, timedelta, timezone

    _terminal = terminal_job_states or frozenset({
        "TERMINATED", "SKIPPED", "INTERNAL_ERROR",
    })
    _active = {"QUEUED", "IN_PROGRESS"}
    changed = False
    now = datetime.now(timezone.utc)

    for _, row in runs_df.iterrows():
        status = str(row.get("status") or "")
        if status not in _active:
            continue

        run_id = str(row.get("run_id") or "")
        job_run_id = row.get("job_run_id")

        if job_run_id:
            try:
                run = sp_ws.jobs.get_run(run_id=int(str(job_run_id)))
                life_cycle = (
                    str(run.state.life_cycle_state).split(".")[-1]
                    if run.state else ""
                )
                if life_cycle in _terminal:
                    result_state = (
                        str(run.state.result_state).split(".")[-1].lower()
                        if run.state else ""
                    )
                    suffix = (
                        f":{result_state}"
                        if result_state and result_state != "none"
                        else ""
                    )
                    sql_warehouse_execute(
                        ws, warehouse_id,
                        f"UPDATE {catalog}.{schema_name}.genie_opt_runs "
                        f"SET status = 'FAILED', "
                        f"convergence_reason = "
                        f"'job_{life_cycle.lower()}_without_state_update{suffix}', "
                        f"updated_at = current_timestamp() "
                        f"WHERE run_id = '{run_id}'",
                    )
                    changed = True
            except Exception:
                sql_warehouse_execute(
                    ws, warehouse_id,
                    f"UPDATE {catalog}.{schema_name}.genie_opt_runs "
                    f"SET status = 'FAILED', "
                    f"convergence_reason = 'job_run_lookup_failed', "
                    f"updated_at = current_timestamp() "
                    f"WHERE run_id = '{run_id}'",
                )
                changed = True
            continue

        started_at_raw = row.get("started_at")
        if started_at_raw:
            try:
                text = str(started_at_raw).strip()
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                started_at = datetime.fromisoformat(text)
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                started_at = None
        else:
            started_at = None

        if started_at and (now - started_at) > timedelta(minutes=stale_queue_minutes):
            sql_warehouse_execute(
                ws, warehouse_id,
                f"UPDATE {catalog}.{schema_name}.genie_opt_runs "
                f"SET status = 'FAILED', "
                f"convergence_reason = 'stale_queued_no_job_run', "
                f"updated_at = current_timestamp() "
                f"WHERE run_id = '{run_id}'",
            )
            changed = True

    return changed


# ── Metric view consents (MV-D7) ─────────────────────────────────────
#
# The probe and consent record are written by the FastAPI backend under OBO,
# and the backend has no SparkSession — so ``mv_state.upsert_mv_consent`` and
# ``mv_state.load_mv_consent`` cannot serve that path. These two are their
# Statement-Execution twins against the same ``genie_opt_mv_consents`` table,
# following ``wh_create_run`` rather than introducing a second write idiom.
# Keep the column set in lockstep with ``_GENIE_OPT_MV_CONSENTS_DDL``.


def _wh_literal(value: Any, *, encode: bool = False) -> str:
    """Render ``value`` as a SQL literal for the metric view consent writers.

    Mirrors ``delta_helpers._sql_literal``: ``None`` becomes ``NULL``, strings
    are quoted and escaped, and ``encode`` routes a string through
    ``unbase64`` so its UTF-8 bytes survive independently of the warehouse's
    string-literal escape mode — required for the nested-JSON column.
    """
    if isinstance(value, str):
        if encode:
            encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
            return f"CAST(unbase64('{encoded}') AS STRING)"
        escaped = value.replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _wh_timestamp_literal(value: str | None) -> str:
    """Render an ISO-8601 string as a TIMESTAMP literal, or ``current_timestamp()``."""
    if not value:
        return "current_timestamp()"
    escaped = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"CAST('{escaped}' AS TIMESTAMP)"


def wh_upsert_mv_consent(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    probe_id: str,
    granted_by: str,
    target_catalog: str,
    target_schema: str,
    verdict: str,
    run_id: str | None = None,
    materialize_consented: bool = False,
    probe_results: dict | None = None,
    granted_at: str | None = None,
    downgrade_reason: str | None = None,
) -> str:
    """Upsert one metric view consent row via SQL warehouse; return ``probe_id``.

    Single ``MERGE INTO`` keyed on ``probe_id``, so a retried probe refreshes
    its row instead of duplicating it. ``reverified_at_trigger`` is never
    written here — it is stamped immediately before an OBO write, so a stale
    authorization cannot be mistaken for a fresh one.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CONSENTS
    from genie_space_optimizer.optimization.mv_state import MV_CONSENT_VERDICTS

    if verdict not in MV_CONSENT_VERDICTS:
        raise ValueError(
            f"verdict must be one of {MV_CONSENT_VERDICTS}, got {verdict!r}"
        )
    if not probe_id:
        raise ValueError("probe_id is required")

    results_json = json.dumps(probe_results, default=str, sort_keys=True) if probe_results else None
    value_cols: dict[str, str] = {
        "run_id": _wh_literal(run_id),
        "granted_by": _wh_literal(granted_by),
        "granted_at": _wh_timestamp_literal(granted_at),
        "target_catalog": _wh_literal(target_catalog),
        "target_schema": _wh_literal(target_schema),
        "materialize_consented": _wh_literal(bool(materialize_consented)),
        "probe_results_json": _wh_literal(results_json, encode=results_json is not None),
        "verdict": _wh_literal(verdict),
        "downgrade_reason": _wh_literal(downgrade_reason),
        "updated_at": "current_timestamp()",
    }

    fqn = f"{catalog}.{schema}.{TABLE_MV_CONSENTS}"
    set_clause = ", ".join(f"t.{col} = {val}" for col, val in value_cols.items())
    insert_cols = ", ".join(["probe_id", *value_cols])
    insert_vals = ", ".join(["s.probe_id", *value_cols.values()])
    sql = (
        f"MERGE INTO {fqn} AS t "
        f"USING (SELECT {_wh_literal(probe_id)} AS probe_id) AS s "
        f"ON t.probe_id = s.probe_id "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Upserted metric view consent %s for %s on %s.%s (verdict=%s) via SQL warehouse",
        probe_id, granted_by, target_catalog, target_schema, verdict,
    )
    return probe_id


def wh_mark_mv_consent_reverified(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    probe_id: str,
    run_id: str | None = None,
    verdict: str | None = None,
    downgrade_reason: str | None = None,
) -> None:
    """SQL-warehouse twin of :func:`mv_state.mark_mv_consent_reverified`.

    Stamps ``reverified_at_trigger`` on an existing consent immediately before
    the backend's OBO create/attach write, and records the run the consent was
    bound to (``run_id``), the re-verified ``verdict``, and — when the run was
    auto-downgraded to ``suggest_only`` — the ``downgrade_reason``. Without this
    twin the backend (which has no SparkSession) had no way to close the
    consent→run loop, so ``/mv-created`` read ``run_id``/``downgrade_reason`` as
    ``NULL`` on a downgraded run (Tier-2 Scenario B). ``UPDATE``-only by
    construction: reverification stamps a consent the probe already wrote, never
    inserts one, so a stale authorization can never masquerade as a fresh row.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CONSENTS
    from genie_space_optimizer.optimization.mv_state import MV_CONSENT_VERDICTS

    if not probe_id:
        raise ValueError("probe_id is required")
    if verdict is not None and verdict not in MV_CONSENT_VERDICTS:
        raise ValueError(
            f"verdict must be one of {MV_CONSENT_VERDICTS}, got {verdict!r}"
        )

    updates: dict[str, str] = {
        "reverified_at_trigger": "current_timestamp()",
        "updated_at": "current_timestamp()",
    }
    if run_id is not None:
        updates["run_id"] = _wh_literal(run_id)
    if verdict is not None:
        updates["verdict"] = _wh_literal(verdict)
    if downgrade_reason is not None:
        updates["downgrade_reason"] = _wh_literal(downgrade_reason)

    fqn = f"{catalog}.{schema}.{TABLE_MV_CONSENTS}"
    set_clause = ", ".join(f"t.{col} = {val}" for col, val in updates.items())
    sql = (
        f"MERGE INTO {fqn} AS t "
        f"USING (SELECT {_wh_literal(probe_id)} AS probe_id) AS s "
        f"ON t.probe_id = s.probe_id "
        f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Re-verified metric view consent %s (run_id=%s, verdict=%s, downgrade=%s) "
        "via SQL warehouse", probe_id, run_id, verdict, downgrade_reason,
    )


def wh_load_mv_consent(
    ws: WorkspaceClient,
    warehouse_id: str,
    probe_id: str,
    catalog: str,
    schema: str,
) -> dict | None:
    """Read one metric view consent row via SQL warehouse, or ``None``.

    Decodes ``probe_results_json`` to ``probe_results`` so callers see the same
    field name ``mv_state.load_mv_consent`` exposes.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CONSENTS

    escaped = str(probe_id).replace("\\", "\\\\").replace("'", "''")
    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT * FROM {catalog}.{schema}.{TABLE_MV_CONSENTS} "
            f"WHERE probe_id = '{escaped}'",
        )
    except Exception:
        logger.debug("wh_load_mv_consent: could not read %s", probe_id, exc_info=True)
        return None
    if getattr(df, "empty", True):
        return None

    row = dict(df.iloc[0].to_dict())
    raw = row.pop("probe_results_json", None)
    if isinstance(raw, str) and raw.strip():
        try:
            row["probe_results"] = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid JSON in probe_results_json; surfacing raw text")
            row["probe_results"] = raw
    else:
        row["probe_results"] = raw
    return row


# ── Metric view candidates + created objects (MV-D21) ────────────────
#
# The backend has no SparkSession, so ``mv_state``'s Spark accessors cannot
# serve the Prompt 9 routes. These are the SQL-warehouse twins, pinned to the
# same tables, keys, and column contract by ``test_wh_mv_state.py`` — a drift
# between the two writers is the "two readers of one field disagree" failure the
# MV feature has guarded against throughout. Base64 is transport only: the
# ``unbase64(...) AS STRING`` literal stores plain JSON text, so the reads
# ``json.loads`` the ``*_json`` columns directly, exactly like ``_parse_json_columns``.

_WH_CANDIDATE_JSON_COLUMNS = (
    "score_components_json",
    "evidence_json",
    "provenance_json",
    "alternatives_json",
    "conflicts_json",
)


def _wh_decode_json_columns(row: dict, columns: tuple[str, ...]) -> dict:
    """Decode ``*_json`` storage columns back to their POV Part 4 field names."""
    out = dict(row)
    for column in columns:
        raw = out.pop(column, None)
        field = column[: -len("_json")]
        if isinstance(raw, str) and raw.strip():
            try:
                out[field] = json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Invalid JSON in %s; surfacing raw text", column)
                out[field] = raw
        else:
            out[field] = raw
    return out


def wh_load_mv_candidates(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    *,
    target_space_id: str | None = None,
    run_id: str | None = None,
    approved_for_rerun: bool | None = None,
    include_superseded: bool = False,
) -> list[dict]:
    """Read advisor proposals via SQL warehouse — the twin of ``mv_state.load_mv_candidates``.

    Same ordering (confidence DESC, then created_at DESC) and the same JSON-column
    decode, so the backend sees the shape the Spark reader exposes. At least one
    of ``target_space_id`` / ``run_id`` is required so a caller cannot scan every
    space.

    ``include_superseded`` defaults to ``False`` (MV-D30 as-implemented, Prompt
    15.6): proposal reads never surface a legacy per-measure row a view-grained
    bundle has retired. Filtered in Python on the decoded ``superseded_by`` field
    so a table missing the additive column reads exactly as before.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CANDIDATES

    if not target_space_id and not run_id:
        raise ValueError("wh_load_mv_candidates requires target_space_id or run_id")

    where: list[str] = []
    if target_space_id:
        where.append(f"target_space_id = {_wh_literal(target_space_id)}")
    if run_id:
        where.append(f"run_id = {_wh_literal(run_id)}")
    if approved_for_rerun is not None:
        where.append(f"approved_for_rerun = {'true' if approved_for_rerun else 'false'}")

    fqn = f"{catalog}.{schema}.{TABLE_MV_CANDIDATES}"
    query = (
        f"SELECT * FROM {fqn} WHERE {' AND '.join(where)} "
        "ORDER BY confidence_score DESC NULLS LAST, created_at DESC"
    )
    try:
        df = sql_warehouse_query(ws, warehouse_id, query)
    except Exception:
        logger.debug("wh_load_mv_candidates: no rows for %s", where, exc_info=True)
        return []
    if getattr(df, "empty", True):
        return []
    rows = [
        _wh_decode_json_columns(dict(record), _WH_CANDIDATE_JSON_COLUMNS)
        for record in df.to_dict(orient="records")
    ]
    if include_superseded:
        return rows
    return [row for row in rows if not row.get("superseded_by")]


def wh_supersede_legacy_mv_candidates(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    target_space_id: str,
    member_fingerprints: Iterable[str],
    superseded_by: str,
) -> list[str]:
    """Retire legacy per-measure rows a landing bundle covers — twin of
    ``mv_state.supersede_legacy_mv_candidates``.

    A single ``UPDATE`` stamps ``superseded_by`` on every candidate whose
    ``dedup_fingerprint`` is one of the bundle's member measure fingerprints and
    is not yet superseded. The backend suggest path (``_persist``) calls this
    right after upserting a bundle so hydration/suggest reads drop the covered
    legacy grain. The human decision is left untouched, so a superseded rejected
    row still feeds the suppression reader (MV-D30 as-implemented, Prompt 15.6).
    """
    from genie_space_optimizer.common.config import TABLE_MV_CANDIDATES

    if not target_space_id:
        raise ValueError("target_space_id is required to supersede candidates")
    if not superseded_by:
        raise ValueError("superseded_by (the covering bundle fingerprint) is required")
    members = sorted(
        {str(f) for f in member_fingerprints if f and str(f) != superseded_by}
    )
    if not members:
        return []
    in_list = ", ".join(_wh_literal(m) for m in members)
    fqn = f"{catalog}.{schema}.{TABLE_MV_CANDIDATES}"
    sql = (
        f"UPDATE {fqn} SET superseded_by = {_wh_literal(superseded_by)}, "
        "updated_at = current_timestamp() "
        f"WHERE target_space_id = {_wh_literal(target_space_id)} "
        f"AND dedup_fingerprint IN ({in_list}) "
        "AND superseded_by IS NULL"
    )
    try:
        sql_warehouse_execute(ws, warehouse_id, sql)
    except Exception:
        logger.debug(
            "wh_supersede_legacy_mv_candidates: update skipped for %s", target_space_id,
            exc_info=True,
        )
        return []
    logger.info(
        "Superseded up to %d legacy candidate(s) for space %s by bundle %s via SQL warehouse",
        len(members), target_space_id, superseded_by,
    )
    return members


def wh_record_mv_candidate_decision(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    target_space_id: str,
    dedup_fingerprint: str,
    decision: str,
    decided_by: str,
    suppressed_until: str | None = None,
    approved_for_rerun: bool | None = None,
) -> None:
    """Record a human approve/reject — the twin of ``mv_state.record_mv_candidate_decision``.

    UPDATE-only (never INSERT): a decision on a candidate that was never proposed
    would insert a row missing the NOT NULL columns. ``approved_for_rerun``
    tracks the decision (MV-D1) unless overridden. Keyed on
    ``(target_space_id, dedup_fingerprint)``.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CANDIDATES
    from genie_space_optimizer.optimization.mv_state import MV_CANDIDATE_DECISIONS

    if decision not in MV_CANDIDATE_DECISIONS:
        raise ValueError(
            f"decision must be one of {MV_CANDIDATE_DECISIONS}, got {decision!r}"
        )
    if not dedup_fingerprint:
        raise ValueError("dedup_fingerprint is required")
    if not target_space_id:
        raise ValueError("target_space_id is required")

    if approved_for_rerun is None:
        approved_for_rerun = decision == "approved"

    set_cols = {
        "decision": _wh_literal(decision),
        "decided_by": _wh_literal(decided_by),
        "decided_at": "current_timestamp()",
        "suppressed_until": _wh_timestamp_literal(suppressed_until)
        if suppressed_until
        else "NULL",
        "approved_for_rerun": "true" if approved_for_rerun else "false",
        "updated_at": "current_timestamp()",
    }
    fqn = f"{catalog}.{schema}.{TABLE_MV_CANDIDATES}"
    set_clause = ", ".join(f"{col} = {val}" for col, val in set_cols.items())
    sql = (
        f"UPDATE {fqn} SET {set_clause} "
        f"WHERE target_space_id = {_wh_literal(target_space_id)} "
        f"AND dedup_fingerprint = {_wh_literal(dedup_fingerprint)}"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Candidate %s for space %s %s by %s (approved_for_rerun=%s) via SQL warehouse",
        dedup_fingerprint, target_space_id, decision, decided_by, approved_for_rerun,
    )


def wh_suppress_mv_measures(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    target_space_id: str,
    measure_fingerprints: Iterable[str],
    originating_suggestion_id: str | None = None,
    suppressed_until: str | None = None,
    reason: str = "bundle_rejected",
) -> list[str]:
    """Fan a bundle rejection out to per-measure suppression — twin of
    ``mv_state.suppress_mv_measures``.

    One ``MERGE INTO genie_opt_mv_suppressions`` per member fingerprint, keyed on
    ``(target_space_id, measure_fingerprint)``. This is MV-D30's load-bearing
    half: because the view-grained bundle key changes when membership changes,
    only a per-measure record keeps a rejected measure out of a future bundle.
    The backend reject route calls this alongside
    ``wh_record_mv_candidate_decision`` on the bundle row.
    """
    from genie_space_optimizer.common.config import TABLE_MV_SUPPRESSIONS

    if not target_space_id:
        raise ValueError("target_space_id is required to suppress measures")

    fqn = f"{catalog}.{schema}.{TABLE_MV_SUPPRESSIONS}"
    written: list[str] = []
    for fingerprint in measure_fingerprints:
        if not fingerprint:
            continue
        value_cols = {
            "suppressed_until": _wh_timestamp_literal(suppressed_until)
            if suppressed_until
            else "NULL",
            "originating_suggestion_id": _wh_literal(originating_suggestion_id),
            "reason": _wh_literal(reason),
            "updated_at": "current_timestamp()",
        }
        set_clause = ", ".join(f"t.{col} = {val}" for col, val in value_cols.items())
        insert_cols = ", ".join(
            ["target_space_id", "measure_fingerprint", *value_cols, "created_at"]
        )
        insert_vals = ", ".join(
            ["s.target_space_id", "s.measure_fingerprint", *value_cols.values(),
             "current_timestamp()"]
        )
        sql = (
            f"MERGE INTO {fqn} AS t USING (SELECT "
            f"{_wh_literal(target_space_id)} AS target_space_id, "
            f"{_wh_literal(fingerprint)} AS measure_fingerprint) AS s "
            "ON t.target_space_id = s.target_space_id "
            "AND t.measure_fingerprint = s.measure_fingerprint "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        sql_warehouse_execute(ws, warehouse_id, sql)
        written.append(fingerprint)
    if written:
        logger.info(
            "Suppressed %d measure fingerprint(s) for space %s (from %s) via SQL warehouse",
            len(written), target_space_id, originating_suggestion_id,
        )
    return written


def wh_load_mv_suppressed_fingerprints(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    target_space_id: str,
) -> set[str]:
    """Return suppressed per-measure fingerprints — twin of
    ``mv_state.load_mv_suppressed_fingerprints``.

    Unions the non-expired fan-out ledger rows with legacy per-measure
    ``rejected`` candidate rows (bundle-key rejections are inert — a bundle key
    never equals a member measure fingerprint). Returns ``set()`` when neither
    table is readable.
    """
    from genie_space_optimizer.common.config import (
        TABLE_MV_CANDIDATES,
        TABLE_MV_SUPPRESSIONS,
    )

    suppressed: set[str] = set()
    if not target_space_id:
        return suppressed
    space_lit = _wh_literal(target_space_id)

    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT measure_fingerprint FROM {catalog}.{schema}.{TABLE_MV_SUPPRESSIONS} "
            f"WHERE target_space_id = {space_lit} "
            "AND (suppressed_until IS NULL OR suppressed_until > current_timestamp())",
        )
        if not getattr(df, "empty", True):
            suppressed.update(str(v) for v in df["measure_fingerprint"] if v)
    except Exception:
        logger.debug("wh_load_mv_suppressed_fingerprints: no suppressions table", exc_info=True)

    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT dedup_fingerprint FROM {catalog}.{schema}.{TABLE_MV_CANDIDATES} "
            f"WHERE target_space_id = {space_lit} AND decision = 'rejected'",
        )
        if not getattr(df, "empty", True):
            suppressed.update(str(v) for v in df["dedup_fingerprint"] if v)
    except Exception:
        logger.debug("wh_load_mv_suppressed_fingerprints: no candidates table", exc_info=True)

    return suppressed


def wh_upsert_mv_candidate(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    target_space_id: str,
    suggestion_id: str,
    dedup_fingerprint: str,
    candidate_type: str,
    confidence_score: float | None = None,
    tier: str | None = None,
    proposed_object: str | None = None,
    score_components: dict | None = None,
    evidence: dict | None = None,
    provenance: dict | None = None,
    alternatives: list | None = None,
    conflicts: list | None = None,
    requested_mode: str | None = None,
    effective_mode: str | None = None,
    yaml_text: str | None = None,
) -> str:
    """Upsert one advisor proposal — the twin of ``mv_state.upsert_mv_candidate``.

    The write side of MV-D21's contract for the standalone advice path (MV-D23):
    the backend has no SparkSession, so the sentinel-run advisor persists its
    candidates here rather than through ``mv_state``. Keyed on
    ``(target_space_id, dedup_fingerprint)`` and writing the *same* column set as
    the Spark writer, plus the additive ``yaml_text`` column that carries the
    rendered MV-D22 replay body on the candidate row — so a standalone candidate
    is replayable without a run-partitioned ``genie_opt_artifacts`` row. Human
    decision columns are never written here (a re-proposing run must not
    resurrect a rejected candidate); ``created_at`` and ``approved_for_rerun``
    are insert-only. Returns ``dedup_fingerprint``.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CANDIDATES
    from genie_space_optimizer.optimization.mv_state import MV_CANDIDATE_TYPES

    if candidate_type not in MV_CANDIDATE_TYPES:
        raise ValueError(
            f"candidate_type must be one of {MV_CANDIDATE_TYPES}, got {candidate_type!r}"
        )
    if not dedup_fingerprint:
        raise ValueError("dedup_fingerprint is required")
    if not target_space_id:
        raise ValueError("target_space_id is required")

    def _json_lit(value: Any) -> str:
        return _wh_literal(json.dumps(value), encode=True) if value is not None else "NULL"

    value_cols = {
        "suggestion_id": _wh_literal(suggestion_id),
        "run_id": _wh_literal(run_id),
        "candidate_type": _wh_literal(candidate_type),
        "confidence_score": _wh_literal(float(confidence_score))
        if confidence_score is not None
        else "NULL",
        "tier": _wh_literal(tier),
        "proposed_object": _wh_literal(proposed_object),
        "score_components_json": _json_lit(score_components),
        "evidence_json": _json_lit(evidence),
        "provenance_json": _json_lit(provenance),
        "alternatives_json": _json_lit(alternatives),
        "conflicts_json": _json_lit(conflicts),
        "requested_mode": _wh_literal(requested_mode),
        "effective_mode": _wh_literal(effective_mode),
        "yaml_text": _wh_literal(yaml_text, encode=True) if yaml_text is not None else "NULL",
        "updated_at": "current_timestamp()",
    }
    fqn = f"{catalog}.{schema}.{TABLE_MV_CANDIDATES}"
    set_clause = ", ".join(f"t.{col} = {val}" for col, val in value_cols.items())
    insert_cols = ", ".join(
        ["target_space_id", "dedup_fingerprint", *value_cols, "created_at", "approved_for_rerun"]
    )
    insert_vals = ", ".join(
        [
            "s.target_space_id",
            "s.dedup_fingerprint",
            *value_cols.values(),
            "current_timestamp()",
            "false",
        ]
    )
    sql = (
        f"MERGE INTO {fqn} AS t "
        f"USING (SELECT {_wh_literal(target_space_id)} AS target_space_id, "
        f"{_wh_literal(dedup_fingerprint)} AS dedup_fingerprint) AS s "
        f"ON t.target_space_id = s.target_space_id AND t.dedup_fingerprint = s.dedup_fingerprint "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Upserted metric view candidate %s (%s, tier=%s) for space %s run %s via SQL warehouse",
        suggestion_id, candidate_type, tier, target_space_id, run_id,
    )
    return dedup_fingerprint


def wh_upsert_mv_created_object(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    suggestion_id: str,
    full_name: str,
    created_by: str,
    status: str = "CREATED",
    attach_patch_id: str | None = None,
    baseline_eval_run_id: str | None = None,
    post_attach_eval_run_id: str | None = None,
    on_regression_action: str = "DETACH_ONLY_NEVER_DROP",
    provenance: str | None = None,
) -> str:
    """Record a metric view created under OBO — twin of ``mv_state.upsert_mv_created_object``.

    Keyed on ``(run_id, suggestion_id)``. ``created_by`` is the consenting user;
    the backend is the only writer of this table, because the job never issues
    metric view DDL (MV-D1). ``provenance`` (MV-D24) discriminates the OBO
    create-and-attach path (``OBO_CREATED``, the default) from a registered
    bring-your-own view (``USER_CREATED``). Returns ``full_name``.
    """
    from genie_space_optimizer.common.config import (
        MV_PROVENANCE_OBO_CREATED,
        TABLE_MV_CREATED_OBJECTS,
    )
    from genie_space_optimizer.optimization.mv_state import (
        MV_CREATED_OBJECT_STATUSES,
        MV_ON_REGRESSION_ACTIONS,
    )

    if status not in MV_CREATED_OBJECT_STATUSES:
        raise ValueError(
            f"status must be one of {MV_CREATED_OBJECT_STATUSES}, got {status!r}"
        )
    if on_regression_action not in MV_ON_REGRESSION_ACTIONS:
        raise ValueError(
            f"on_regression_action must be one of {MV_ON_REGRESSION_ACTIONS}, "
            f"got {on_regression_action!r}"
        )
    if not full_name:
        raise ValueError("full_name is required")

    value_cols = {
        "full_name": _wh_literal(full_name),
        "created_by": _wh_literal(created_by),
        "status": _wh_literal(status),
        "attach_patch_id": _wh_literal(attach_patch_id),
        "baseline_eval_run_id": _wh_literal(baseline_eval_run_id),
        "post_attach_eval_run_id": _wh_literal(post_attach_eval_run_id),
        "on_regression_action": _wh_literal(on_regression_action),
        "provenance": _wh_literal(provenance or MV_PROVENANCE_OBO_CREATED),
        "updated_at": "current_timestamp()",
    }
    fqn = f"{catalog}.{schema}.{TABLE_MV_CREATED_OBJECTS}"
    set_clause = ", ".join(f"t.{col} = {val}" for col, val in value_cols.items())
    insert_cols = ", ".join(["run_id", "suggestion_id", *value_cols, "created_at"])
    insert_vals = ", ".join(
        ["s.run_id", "s.suggestion_id", *value_cols.values(), "current_timestamp()"]
    )
    sql = (
        f"MERGE INTO {fqn} AS t "
        f"USING (SELECT {_wh_literal(run_id)} AS run_id, "
        f"{_wh_literal(suggestion_id)} AS suggestion_id) AS s "
        f"ON t.run_id = s.run_id AND t.suggestion_id = s.suggestion_id "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Upserted created metric view %s (status=%s) for run %s suggestion %s via SQL warehouse",
        full_name, status, run_id, suggestion_id,
    )
    return full_name


def wh_update_mv_created_object_status(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    suggestion_id: str,
    status: str,
    attach_patch_id: str | None = None,
    baseline_eval_run_id: str | None = None,
    post_attach_eval_run_id: str | None = None,
    lift_report_json: str | None = None,
) -> None:
    """Advance a created object's status — twin of ``mv_state.update_mv_created_object_status``.

    UPDATE keyed on ``(run_id, suggestion_id)``; only non-``None`` fields are
    written. ``lift_report_json`` is ``LiftReport.to_dict()`` serialized verbatim
    and travels base64-encoded like every JSON column.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CREATED_OBJECTS
    from genie_space_optimizer.optimization.mv_state import MV_CREATED_OBJECT_STATUSES

    if status not in MV_CREATED_OBJECT_STATUSES:
        raise ValueError(
            f"status must be one of {MV_CREATED_OBJECT_STATUSES}, got {status!r}"
        )

    set_cols = {"status": _wh_literal(status), "updated_at": "current_timestamp()"}
    if attach_patch_id is not None:
        set_cols["attach_patch_id"] = _wh_literal(attach_patch_id)
    if baseline_eval_run_id is not None:
        set_cols["baseline_eval_run_id"] = _wh_literal(baseline_eval_run_id)
    if post_attach_eval_run_id is not None:
        set_cols["post_attach_eval_run_id"] = _wh_literal(post_attach_eval_run_id)
    if lift_report_json is not None:
        set_cols["lift_report_json"] = _wh_literal(lift_report_json, encode=True)

    fqn = f"{catalog}.{schema}.{TABLE_MV_CREATED_OBJECTS}"
    set_clause = ", ".join(f"{col} = {val}" for col, val in set_cols.items())
    sql = (
        f"UPDATE {fqn} SET {set_clause} "
        f"WHERE run_id = {_wh_literal(run_id)} "
        f"AND suggestion_id = {_wh_literal(suggestion_id)}"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info(
        "Metric view for run %s suggestion %s is now %s via SQL warehouse",
        run_id, suggestion_id, status,
    )


def wh_load_mv_created_object(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
    suggestion_id: str,
) -> dict | None:
    """Read one created-object row via SQL warehouse, or ``None``.

    The drop route needs this to authorize against ``created_by`` and confirm
    ``status = DETACHED`` before it will drop anything (MV-D6). ``lift_report_json``
    is decoded to ``lift_report`` so callers see the ``mv_state`` field name.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CREATED_OBJECTS

    fqn = f"{catalog}.{schema}.{TABLE_MV_CREATED_OBJECTS}"
    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT * FROM {fqn} WHERE run_id = {_wh_literal(run_id)} "
            f"AND suggestion_id = {_wh_literal(suggestion_id)} "
            "ORDER BY updated_at DESC LIMIT 1",
        )
    except Exception:
        logger.debug(
            "wh_load_mv_created_object: could not read %s/%s",
            run_id, suggestion_id, exc_info=True,
        )
        return None
    if getattr(df, "empty", True):
        return None
    return _wh_decode_json_columns(dict(df.iloc[0].to_dict()), ("lift_report_json",))


def wh_load_mv_created_objects(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
) -> list[dict]:
    """Read every created-object row for a run via SQL warehouse — plural twin of
    ``wh_load_mv_created_object``.

    The run output/results screen needs the full ledger for a run (each proposal
    the backend created under OBO), not the single row the drop route authorizes
    against. Same ``lift_report_json`` → ``lift_report`` decode per row so callers
    see the ``mv_state`` field name. Newest first; ``[]`` on read failure.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CREATED_OBJECTS

    fqn = f"{catalog}.{schema}.{TABLE_MV_CREATED_OBJECTS}"
    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT * FROM {fqn} WHERE run_id = {_wh_literal(run_id)} "
            "ORDER BY updated_at DESC",
        )
    except Exception:
        logger.debug(
            "wh_load_mv_created_objects: could not read run %s", run_id, exc_info=True
        )
        return []
    if getattr(df, "empty", True):
        return []
    return [
        _wh_decode_json_columns(dict(record), ("lift_report_json",))
        for record in df.to_dict(orient="records")
    ]


def wh_load_mv_consent_by_run(
    ws: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    run_id: str,
) -> dict | None:
    """Read the consent row carried into a run via SQL warehouse, or ``None``.

    The consent table is keyed on ``probe_id`` (written before any run exists),
    but ``run_id`` is filled at trigger time (``mv_state.upsert_mv_consent``), so a
    run has at most one consent. The run output/results screen reads it for the
    run's ``downgrade_reason`` — why a ``create_and_attach`` run was downgraded to
    ``suggest_only``. Newest first, one row. Decodes ``probe_results_json`` to
    ``probe_results`` like ``wh_load_mv_consent``.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CONSENTS

    fqn = f"{catalog}.{schema}.{TABLE_MV_CONSENTS}"
    try:
        df = sql_warehouse_query(
            ws,
            warehouse_id,
            f"SELECT * FROM {fqn} WHERE run_id = {_wh_literal(run_id)} "
            "ORDER BY updated_at DESC LIMIT 1",
        )
    except Exception:
        logger.debug(
            "wh_load_mv_consent_by_run: could not read run %s", run_id, exc_info=True
        )
        return None
    if getattr(df, "empty", True):
        return None

    row = dict(df.iloc[0].to_dict())
    raw = row.pop("probe_results_json", None)
    if isinstance(raw, str) and raw.strip():
        try:
            row["probe_results"] = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid JSON in probe_results_json; surfacing raw text")
            row["probe_results"] = raw
    else:
        row["probe_results"] = raw
    return row


# ── Warehouse ID resolution ──────────────────────────────────────────
#
# The optimization pipeline historically read only
# ``GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID`` at each call site. In deployed
# Databricks Apps the resource is usually named ``SQL_WAREHOUSE_ID`` or
# ``GSO_WAREHOUSE_ID`` first, then job notebooks copy it into the legacy
# name. A single resolver prevents enrichment subtasks from silently
# falling back to Spark Connect when that copy is missing.

import os as _os


WAREHOUSE_ENV_KEYS: tuple[str, ...] = (
    "GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID",
    "GSO_WAREHOUSE_ID",
    "SQL_WAREHOUSE_ID",
)


def resolve_warehouse_id(explicit: str | None = None) -> str:
    """Return the first non-empty SQL warehouse ID.

    Precedence:
    1. Explicit function argument.
    2. Legacy optimizer env var ``GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID``.
    3. GSO app env var ``GSO_WAREHOUSE_ID``.
    4. Databricks Apps SQL warehouse resource env var ``SQL_WAREHOUSE_ID``.
    """
    explicit_s = str(explicit or "").strip()
    if explicit_s:
        return explicit_s
    for key in WAREHOUSE_ENV_KEYS:
        val = _os.getenv(key, "").strip()
        if val:
            return val
    return ""


def export_warehouse_id(warehouse_id: str | None) -> str:
    """Export a resolved warehouse ID under every runtime env key.

    Returns the resolved value so job notebooks can log it directly.
    Empty input leaves the environment unchanged and returns ``""``.
    """
    resolved = str(warehouse_id or "").strip()
    if not resolved:
        return ""
    for key in WAREHOUSE_ENV_KEYS:
        _os.environ[key] = resolved
    return resolved
