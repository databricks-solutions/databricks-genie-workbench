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
) -> None:
    """Insert a QUEUED run row via SQL warehouse."""
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER, MAX_ITERATIONS

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

    # GSO v2 Phase 5 (D3): the ``experiment_name`` column was scrubbed; the
    # surviving MLflow tracing self-resolves a deterministic experiment path in
    # ``preflight_persist_benchmark_corpus`` (no pointer column needed).
    sql = (
        f"INSERT INTO {catalog}.{schema}.genie_opt_runs "
        f"(run_id, space_id, domain, catalog, uc_schema, status, started_at, "
        f"max_iterations, levers, apply_mode, updated_at, "
        f"triggered_by, config_snapshot, llm_model, benchmark_policy, "
        f"benchmark_mutation_count) VALUES ("
        f"'{run_id}', '{space_id}', '{domain}', '{catalog}', "
        f"'{catalog}.{schema}', 'QUEUED', current_timestamp(), "
        f"{MAX_ITERATIONS}, '{levers_json}', '{apply_mode}', current_timestamp(), "
        f"'{user}', {snap_sql}, {model_sql}, '{policy_escaped}', 0)"
    )
    sql_warehouse_execute(ws, warehouse_id, sql)
    logger.info("Created run %s via SQL warehouse", run_id)


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
