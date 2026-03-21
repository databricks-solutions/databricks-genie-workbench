"""SQL Warehouse helpers using the Statement Execution API.

These utilities bypass Spark Connect entirely and are used by both the
standalone app's fallback path (``backend/routes/spaces.py``) and the
integration module (``integration/``).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def sql_warehouse_query(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
) -> Any:
    """Execute SQL via the Statement Execution API and return a pandas DataFrame."""
    import pandas as pd
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
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

    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state != StatementState.SUCCEEDED:
        error_msg = ""
        if resp.status.error:
            error_msg = resp.status.error.message or str(resp.status.error)
        raise RuntimeError(f"SQL warehouse execute failed: {error_msg}")


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
    experiment_name: str | None = None,
    config_snapshot: dict | None = None,
) -> None:
    """Insert a QUEUED run row via SQL warehouse."""
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER, MAX_ITERATIONS

    snap_json = json.dumps(config_snapshot).replace("'", "''") if config_snapshot else ""
    levers_json = json.dumps(levers if levers is not None else DEFAULT_LEVER_ORDER)
    exp = (experiment_name or "").replace("'", "''")
    user = (triggered_by or "").replace("'", "''")

    sql = (
        f"INSERT INTO {catalog}.{schema}.genie_opt_runs "
        f"(run_id, space_id, domain, catalog, uc_schema, status, started_at, "
        f"max_iterations, levers, apply_mode, updated_at, "
        f"experiment_name, triggered_by, config_snapshot) VALUES ("
        f"'{run_id}', '{space_id}', '{domain}', '{catalog}', "
        f"'{catalog}.{schema}', 'QUEUED', current_timestamp(), "
        f"{MAX_ITERATIONS}, '{levers_json}', '{apply_mode}', current_timestamp(), "
        f"'{exp}', '{user}', '{snap_json}')"
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
