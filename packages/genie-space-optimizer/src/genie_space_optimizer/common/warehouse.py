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
) -> list[dict]:
    """Read advisor proposals via SQL warehouse — the twin of ``mv_state.load_mv_candidates``.

    Same ordering (confidence DESC, then created_at DESC) and the same JSON-column
    decode, so the backend sees the shape the Spark reader exposes. At least one
    of ``target_space_id`` / ``run_id`` is required so a caller cannot scan every
    space.
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
    return [
        _wh_decode_json_columns(dict(record), _WH_CANDIDATE_JSON_COLUMNS)
        for record in df.to_dict(orient="records")
    ]


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
) -> str:
    """Record a metric view created under OBO — twin of ``mv_state.upsert_mv_created_object``.

    Keyed on ``(run_id, suggestion_id)``. ``created_by`` is the consenting user;
    the backend is the only writer of this table, because the job never issues
    metric view DDL (MV-D1). Returns ``full_name``.
    """
    from genie_space_optimizer.common.config import TABLE_MV_CREATED_OBJECTS
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
