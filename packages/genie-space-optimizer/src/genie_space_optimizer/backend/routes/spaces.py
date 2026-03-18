"""Space endpoints: list and detail."""

from __future__ import annotations

import json
import logging
import math

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import PermissionDenied
from databricks.sdk.service.sql import Disposition, Format
from fastapi import HTTPException

from ..constants import ACTIVE_RUN_STATUSES, TERMINAL_JOB_STATES
from ..core import Dependencies, create_router
from ..models import (
    AccessLevelEntry,
    CheckAccessRequest,
    FunctionInfo,
    JoinInfo,
    OptimizeResponse,
    RunSummary,
    SpaceDetail,
    SpaceListResponse,
    SpaceSummary,
    TableInfo,
)
from ..utils import ensure_utc_iso, get_sp_principal, safe_float
from .._spark import get_spark, reset_spark, _is_credential_error

router = create_router()
logger = logging.getLogger(__name__)
_ACTIVE_RUN_STATUSES = ACTIVE_RUN_STATUSES
_TERMINAL_JOB_STATES = TERMINAL_JOB_STATES
_STALE_QUEUE_TIMEOUT = timedelta(minutes=10)
SUPPORTED_APPLY_MODES = {"genie_config", "uc_artifact", "both"}
"""Valid ``apply_mode`` values for optimization triggers.

- ``"genie_config"``  -- Apply changes only to the Genie Space config (default).
- ``"uc_artifact"``   -- Apply UC-level changes only (DDL on tables/columns).
- ``"both"``          -- Apply both Genie Space config and UC-level changes.
"""
_PIPELINE_APPLY_MODE = "genie_config"


from genie_space_optimizer.common.warehouse import (
    sql_warehouse_execute as _sql_warehouse_execute,
    sql_warehouse_query as _sql_warehouse_query,
    wh_create_run as _wh_create_run,
)


def _genie_client(ws: WorkspaceClient, sp_ws: WorkspaceClient) -> tuple[WorkspaceClient, bool]:
    """Pick the best client for Genie API calls.

    Tries the user OBO client first; if the token lacks the ``genie`` scope
    falls back to the service-principal client transparently.

    Returns ``(client, is_obo)`` where *is_obo* is True when the OBO client was used.
    """
    try:
        ws.genie.list_spaces(page_size=1)
        return ws, True
    except PermissionDenied:
        logger.info("OBO token missing genie scope — falling back to SP client")
        return sp_ws, False
    except Exception:
        logger.warning("Unexpected error probing OBO genie access — falling back to SP client", exc_info=True)
        return sp_ws, False


def _parse_utc(raw: object) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _reconcile_active_runs(
    *,
    spark,
    runs_df,
    sp_ws: WorkspaceClient,
    catalog: str,
    schema_name: str,
) -> bool:
    """Mark stale/terminated job-backed active rows as FAILED.

    This prevents old QUEUED/IN_PROGRESS rows from permanently blocking
    new optimization runs in the UI/API.
    """
    from genie_space_optimizer.optimization.state import update_run_status

    changed = False
    now = datetime.now(timezone.utc)

    for _, row in runs_df.iterrows():
        status = str(row.get("status") or "")
        if status not in _ACTIVE_RUN_STATUSES:
            continue

        run_id = str(row.get("run_id") or "")
        job_run_id = row.get("job_run_id")

        if job_run_id:
            try:
                run = sp_ws.jobs.get_run(run_id=int(str(job_run_id)))
                life_cycle = str(run.state.life_cycle_state).split(".")[-1] if run.state else ""
                if life_cycle in _TERMINAL_JOB_STATES:
                    result_state = str(run.state.result_state).split(".")[-1].lower() if run.state else ""
                    suffix = f":{result_state}" if result_state and result_state != "none" else ""
                    update_run_status(
                        spark,
                        run_id,
                        catalog,
                        schema_name,
                        status="FAILED",
                        convergence_reason=f"job_{life_cycle.lower()}_without_state_update{suffix}",
                    )
                    changed = True
            except Exception:
                update_run_status(
                    spark,
                    run_id,
                    catalog,
                    schema_name,
                    status="FAILED",
                    convergence_reason="job_run_lookup_failed",
                )
                changed = True
            continue

        started_at = _parse_utc(row.get("started_at"))
        if started_at and (now - started_at) > _STALE_QUEUE_TIMEOUT:
            update_run_status(
                spark,
                run_id,
                catalog,
                schema_name,
                status="FAILED",
                convergence_reason="stale_queued_no_job_run",
            )
            changed = True

    return changed


def _sql_rows(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    statement: str,
) -> list[dict]:
    """Execute SQL through statement execution and return list-of-dicts rows."""
    res = ws.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    rows = res.result.data_array if res.result and res.result.data_array else []
    cols_info = (
        res.manifest.schema.columns
        if res.manifest and res.manifest.schema and res.manifest.schema.columns
        else []
    )
    cols = [c.name for c in cols_info]
    return [{k: v for k, v in zip(cols, row)} for row in rows]


def fetch_uc_metadata_obo(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    catalog: str,
    schema_name: str,
    genie_table_refs: list[tuple[str, str, str]] | None = None,
) -> dict[str, list[dict] | None]:
    """Fetch UC metadata via OBO user's SQL permissions.

    If ``genie_table_refs`` is provided (from ``extract_genie_space_table_refs``),
    queries are scoped to the exact Genie space data assets. Otherwise falls back
    to the optimizer's operational schema (legacy behavior).
    """
    if not warehouse_id:
        return {}

    if genie_table_refs:
        return _fetch_uc_metadata_obo_for_tables(ws, warehouse_id=warehouse_id, refs=genie_table_refs)

    safe_schema = schema_name.replace("'", "''")
    queries = {
        "uc_columns": (
            "SELECT table_name, column_name, data_type, comment "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{safe_schema}'"
        ),
        "uc_tags": (
            "SELECT * "
            f"FROM {catalog}.information_schema.table_tags "
            f"WHERE schema_name = '{safe_schema}'"
        ),
        "uc_routines": (
            "SELECT routine_name, routine_type, routine_definition, "
            "data_type AS return_type, routine_schema "
            f"FROM {catalog}.information_schema.routines "
            f"WHERE routine_schema = '{safe_schema}'"
        ),
    }

    result: dict[str, list[dict] | None] = {}
    for key, statement in queries.items():
        try:
            result[key] = _sql_rows(
                ws,
                warehouse_id=warehouse_id,
                statement=statement,
            )
        except Exception as exc:
            logger.warning("OBO metadata fetch failed for %s: %s", key, exc)
            result[key] = None
    logger.info(
        "OBO metadata prefetch (schema fallback): %s",
        {k: (len(v) if v is not None else "FAILED") for k, v in result.items()},
    )
    return result


def _fetch_uc_metadata_obo_for_tables(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    refs: list[tuple[str, str, str]],
) -> dict[str, list[dict] | None]:
    """Fetch UC metadata scoped to specific Genie space table references."""
    from genie_space_optimizer.common.uc_metadata import get_unique_schemas

    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    col_unions: list[str] = []
    tag_unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        col_unions.append(
            f"SELECT table_name, column_name, data_type, comment "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name IN ({safe_tables})"
        )
        tag_unions.append(
            f"SELECT * FROM {cat}.information_schema.table_tags "
            f"WHERE schema_name = '{sch}' AND table_name IN ({safe_tables})"
        )

    routine_unions: list[str] = []
    for cat, sch in get_unique_schemas(refs):
        routine_unions.append(
            f"SELECT routine_name, routine_type, routine_definition, "
            f"data_type AS return_type, routine_schema "
            f"FROM {cat}.INFORMATION_SCHEMA.ROUTINES "
            f"WHERE routine_schema = '{sch}'"
        )

    result: dict[str, list[dict] | None] = {}
    query_map = {
        "uc_columns": " UNION ALL ".join(col_unions) if col_unions else None,
        "uc_tags": " UNION ALL ".join(tag_unions) if tag_unions else None,
        "uc_routines": " UNION ALL ".join(routine_unions) if routine_unions else None,
    }
    for key, statement in query_map.items():
        if not statement:
            result[key] = []
            continue
        try:
            result[key] = _sql_rows(ws, warehouse_id=warehouse_id, statement=statement)
        except Exception as exc:
            logger.warning("OBO metadata fetch failed for %s (genie tables): %s", key, exc)
            result[key] = None
    logger.info(
        "OBO metadata prefetch (genie tables): %s",
        {k: (len(v) if v is not None else "FAILED") for k, v in result.items()},
    )
    return result


@router.get("/spaces", response_model=SpaceListResponse, operation_id="listSpaces")
def list_spaces(
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
    session: Dependencies.Session,
):
    """List all Genie Spaces the caller can see, enriched with quality scores.

    Returns every space visible to the caller without per-space permission or
    config fetches.  Permission checks happen lazily via the batch
    ``POST /spaces/check-access`` endpoint, keeping this endpoint fast.
    """
    from genie_space_optimizer.common.genie_client import list_spaces as _list_spaces
    from genie_space_optimizer.optimization.state import load_recent_activity

    client, is_obo = _genie_client(ws, sp_ws)
    try:
        all_spaces = _list_spaces(client)
    except Exception as exc:
        logger.error("Genie list_spaces failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Could not fetch Genie spaces: {exc}") from exc

    from ..gso_read_through import load_recent_activity_with_fallback

    score_by_space: dict[str, float] = {}
    try:
        spark = get_spark()
        activity_df = load_recent_activity_with_fallback(
            session, spark, config.catalog, config.schema_name, limit=200,
        )
        if not activity_df.empty:
            for _, row in activity_df.iterrows():
                sid = row.get("space_id", "")
                if sid and sid not in score_by_space:
                    raw = row.get("best_accuracy", 0.0) or 0.0
                    val = float(raw)
                    if not math.isfinite(val):
                        val = 0.0
                    score_by_space[sid] = val
    except Exception:
        logger.debug("Delta tables not yet available, skipping activity enrichment")

    summaries = [
        SpaceSummary(
            id=s["id"],
            name=str(s.get("title", "") or ""),
            qualityScore=score_by_space.get(s["id"]),
        )
        for s in all_spaces
    ]
    return SpaceListResponse(
        spaces=summaries,
        totalCount=len(summaries),
        scopedToUser=is_obo,
    )


_MAX_ACCESS_CHECK_IDS = 20


@router.post(
    "/spaces/check-access",
    response_model=list[AccessLevelEntry],
    operation_id="checkSpaceAccess",
)
def check_space_access(
    body: CheckAccessRequest,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    headers: Dependencies.Headers,
):
    """Return the caller's access level for a batch of space IDs."""
    from genie_space_optimizer.common.genie_client import get_user_access_level

    ids = body.spaceIds[:_MAX_ACCESS_CHECK_IDS]

    user_email: str | None = (headers.user_email or headers.user_name or "").lower() or None
    user_groups: set[str] | None = None
    if not user_email:
        try:
            me = ws.current_user.me()
            user_email = (me.user_name or "").lower()
            if me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        except Exception:
            user_email = ""
    if user_groups is None:
        try:
            me = ws.current_user.me()
            if me.groups:
                user_groups = {g.display.lower() for g in me.groups if g.display}
        except Exception:
            pass
    user_groups = user_groups or set()

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _check_one(sid: str) -> AccessLevelEntry:
        level = get_user_access_level(
            ws, sid,
            user_email=user_email,
            user_groups=user_groups,
            acl_client=sp_ws,
        )
        return AccessLevelEntry(spaceId=sid, accessLevel=level)

    results: list[AccessLevelEntry] = []
    with ThreadPoolExecutor(max_workers=min(len(ids), 5)) as pool:
        futures = {pool.submit(_check_one, sid): sid for sid in ids}
        for f in as_completed(futures):
            try:
                results.append(f.result())
            except Exception:
                results.append(AccessLevelEntry(spaceId=futures[f], accessLevel=None))
    return results


@router.get("/spaces/{space_id}", response_model=SpaceDetail, operation_id="getSpaceDetail")
def get_space_detail(
    space_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
    session: Dependencies.Session,
):
    """Full space config with UC metadata and optimization history."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import load_runs_for_space

    client, _ = _genie_client(ws, sp_ws)
    try:
        space_config = fetch_space_config(client, space_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Space not found: {exc}") from exc

    ss = space_config.get("_parsed_space", {})
    ds = ss.get("data_sources", {}) if isinstance(ss, dict) else {}

    tables_raw = ds.get("tables", []) if isinstance(ds, dict) else []
    tables: list[TableInfo] = []
    for t in tables_raw:
        ident = t.get("identifier", "")
        parts = ident.split(".") if ident else []
        cat = parts[0] if len(parts) >= 3 else config.catalog
        sch = parts[1] if len(parts) >= 3 else config.schema_name
        desc_val = t.get("description", [])
        desc_str = "\n".join(desc_val) if isinstance(desc_val, list) else str(desc_val or "")
        cols = t.get("column_configs", [])
        tables.append(
            TableInfo(
                name=ident,
                catalog=cat,
                schema_name=sch,
                description=desc_str,
                columnCount=len(cols),
            )
        )

    instr = ss.get("instructions", {}) if isinstance(ss, dict) else {}

    joins_raw = (instr.get("join_specs", []) if isinstance(instr, dict) else []) or (
        ds.get("join_specs", []) if isinstance(ds, dict) else []
    )
    joins: list[JoinInfo] = []
    for join in joins_raw:
        if not isinstance(join, dict):
            continue
        left_obj = join.get("left", {})
        right_obj = join.get("right", {})
        if isinstance(left_obj, dict) and isinstance(right_obj, dict):
            left = left_obj.get("identifier", "")
            right = right_obj.get("identifier", "")
        else:
            left = str(join.get("left_table_name") or "")
            right = str(join.get("right_table_name") or "")

        sql_arr = join.get("sql", [])
        rel: str | None = None
        join_columns: list[str] = []
        if isinstance(sql_arr, list) and sql_arr:
            for entry in sql_arr:
                entry_str = str(entry)
                if entry_str.startswith("--rt="):
                    rel = entry_str
                elif entry_str.strip():
                    join_columns.append(entry_str)
        else:
            rel = join.get("relationship_type")
            join_columns_raw = join.get("join_columns", [])
            if isinstance(join_columns_raw, list):
                for jc in join_columns_raw:
                    if not isinstance(jc, dict):
                        continue
                    left_col = str(jc.get("left_column") or "")
                    right_col = str(jc.get("right_column") or "")
                    if left_col or right_col:
                        join_columns.append(f"{left_col} = {right_col}")

        joins.append(
            JoinInfo(
                leftTable=left,
                rightTable=right,
                relationshipType=str(rel) if rel is not None else None,
                joinColumns=join_columns,
            )
        )
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instructions_str = ""
    if text_instr and isinstance(text_instr, list):
        content = text_instr[0].get("content", []) if text_instr else []
        if isinstance(content, list):
            instructions_str = "\n".join(str(c) for c in content)
        else:
            instructions_str = str(content)

    example_qs = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    sample_questions: list[str] = []
    for q in example_qs:
        if not isinstance(q, dict):
            continue
        val = q.get("question", "")
        if isinstance(val, list):
            val = val[0] if val else ""
        sample_questions.append(str(val))

    benchmark_questions: list[str] = []
    try:
        spark = get_spark()
        title = str(space_config.get("title", "") or "")
        domain = re.sub(r"[^a-z0-9_]+", "_", title.lower().replace(" ", "_").replace("-", "_")).strip("_")
        if not domain:
            domain = "default"
        bench_table = f"{config.catalog}.{config.schema_name}.genie_benchmarks_{domain}"
        bench_df = spark.sql(
            f"SELECT question FROM {bench_table} WHERE question IS NOT NULL ORDER BY question LIMIT 200"
        )
        seen: set[str] = set()
        for row in bench_df.collect():
            q = str(row["question"] or "")
            q = q.strip()
            if q and q not in seen:
                seen.add(q)
                benchmark_questions.append(q)
    except Exception:
        logger.debug("Benchmark questions not available for space %s", space_id)

    if not benchmark_questions:
        seen_bq: set[str] = set()
        bench_section = ss.get("benchmarks", {}) if isinstance(ss, dict) else {}
        bench_qs = bench_section.get("questions", []) if isinstance(bench_section, dict) else []
        for bq in (bench_qs if isinstance(bench_qs, list) else []):
            if not isinstance(bq, dict):
                continue
            q_raw = bq.get("question", [])
            if isinstance(q_raw, list):
                q_raw = q_raw[0] if q_raw else ""
            q_text = str(q_raw).strip()
            if q_text and q_text.lower() not in seen_bq:
                seen_bq.add(q_text.lower())
                benchmark_questions.append(q_text)

        for eq in (example_qs if isinstance(example_qs, list) else []):
            if not isinstance(eq, dict):
                continue
            q_raw = eq.get("question", "")
            if isinstance(q_raw, list):
                q_raw = q_raw[0] if q_raw else ""
            q_text = str(q_raw).strip()
            if q_text and q_text.lower() not in seen_bq:
                seen_bq.add(q_text.lower())
                benchmark_questions.append(q_text)

    sql_functions_raw = instr.get("sql_functions", []) if isinstance(instr, dict) else []
    functions: list[FunctionInfo] = []
    for fn in sql_functions_raw:
        if not isinstance(fn, dict):
            continue
        ident = fn.get("identifier", "")
        parts = ident.split(".") if ident else []
        cat = parts[0] if len(parts) >= 3 else config.catalog
        sch = parts[1] if len(parts) >= 3 else config.schema_name
        functions.append(FunctionInfo(name=ident, catalog=cat, schema_name=sch))

    from ..gso_read_through import load_runs_for_space_with_fallback

    history: list[RunSummary] = []
    try:
        spark = get_spark()
        runs_df = load_runs_for_space_with_fallback(
            session, spark, space_id, config.catalog, config.schema_name,
        )
        if not runs_df.empty and _reconcile_active_runs(
            spark=spark,
            runs_df=runs_df,
            sp_ws=sp_ws,
            catalog=config.catalog,
            schema_name=config.schema_name,
        ):
            runs_df = load_runs_for_space_with_fallback(
                session, spark, space_id, config.catalog, config.schema_name,
            )
        if not runs_df.empty:
            baseline_scores = _load_baseline_scores(
                spark, [r for r in runs_df["run_id"]], config.catalog, config.schema_name,
            )
            for _, row in runs_df.iterrows():
                run_id_val = row.get("run_id", "")
                history.append(
                    RunSummary(
                        runId=run_id_val,
                        status=row.get("status", ""),
                        baselineScore=_safe_float(baseline_scores.get(run_id_val)),
                        optimizedScore=_safe_float(row.get("best_accuracy")),
                        timestamp=ensure_utc_iso(row.get("started_at")) or "",
                    )
                )
    except Exception:
        logger.debug("Delta tables not yet available, skipping optimization history")

    has_active = any(r.status in _ACTIVE_RUN_STATUSES for r in history)

    return SpaceDetail(
        id=space_id,
        name=space_config.get("title", space_config.get("name", "")),
        description=space_config.get("description", ""),
        instructions=instructions_str,
        sampleQuestions=sample_questions,
        benchmarkQuestions=benchmark_questions,
        tables=tables,
        joins=joins,
        functions=functions,
        optimizationHistory=history,
        hasActiveRun=has_active,
    )


def _check_sp_data_access(
    spark, catalog: str, schema_name: str, genie_refs: list, sp_ws: WorkspaceClient,
) -> list[tuple[str, str]]:
    """Return list of (catalog, schema) pairs the SP does NOT have grants for."""
    from .settings import _probe_sp_required_access

    needed: set[tuple[str, str]] = set()
    for ref in genie_refs:
        cat = ref[0] if isinstance(ref, (list, tuple)) else ""
        sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
        if cat and sch:
            needed.add((cat.lower(), sch.lower()))

    if not needed:
        return []

    read_granted, _write_granted = _probe_sp_required_access(sp_ws, needed)
    return sorted(needed - read_granted)


def _check_sp_write_access(
    spark, catalog: str, schema_name: str, genie_refs: list, sp_ws: WorkspaceClient,
) -> list[tuple[str, str]]:
    """Return (catalog, schema) pairs where the SP lacks MODIFY (write) grant."""
    from .settings import _probe_sp_required_access

    needed: set[tuple[str, str]] = set()
    for ref in genie_refs:
        cat = ref[0] if isinstance(ref, (list, tuple)) else ""
        sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
        if cat and sch:
            needed.add((cat.lower(), sch.lower()))

    if not needed:
        return []

    _read_granted, write_granted = _probe_sp_required_access(sp_ws, needed)
    return sorted(needed - write_granted)


def do_start_optimization(
    space_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config,
    headers,
    apply_mode: str = "genie_config",
    levers: list[int] | None = None,
    deploy_target: str | None = None,
) -> OptimizeResponse:
    """Core optimization trigger logic shared by the UI route and the API trigger route.

    Creates a QUEUED run in Delta and dynamically submits a serverless
    optimization job.  Uses OBO for Genie domain inference, SP for job
    submission.  User identity comes from forwarded headers.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import (
        create_run,
        ensure_optimization_tables,
        load_runs_for_space,
        update_run_status,
    )
    from ..job_launcher import submit_optimization
    from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER

    levers_str = json.dumps(levers if levers else DEFAULT_LEVER_ORDER)

    requested_apply_mode = (apply_mode or "genie_config").strip().lower()
    if requested_apply_mode not in SUPPORTED_APPLY_MODES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported apply_mode '{apply_mode}'. "
                f"Use one of: {sorted(SUPPORTED_APPLY_MODES)}"
            ),
        )

    from genie_space_optimizer.common.genie_client import user_can_edit_space

    caller_email = (headers.user_email or headers.user_name or "").lower()
    if not user_can_edit_space(ws, space_id, user_email=caller_email, acl_client=sp_ws):
        raise HTTPException(
            status_code=403,
            detail=(
                "You need CAN_EDIT or CAN_MANAGE permission on this "
                "Genie Space to start optimization."
            ),
        )

    use_warehouse_fallback = False

    def _init_spark_state():
        _spark = get_spark()
        ensure_optimization_tables(_spark, config.catalog, config.schema_name)
        _df = load_runs_for_space(_spark, space_id, config.catalog, config.schema_name)
        return _spark, _df

    def _actionable_setup_error(exc: Exception) -> HTTPException | None:
        """Convert schema/permission errors into user-friendly 503 responses."""
        exc_str = str(exc)
        fqn = f"{config.catalog}.{config.schema_name}"
        if "SCHEMA_NOT_FOUND" in exc_str:
            return HTTPException(
                status_code=503,
                detail=(
                    f"Schema '{fqn}' does not exist. "
                    f"Create it with: CREATE SCHEMA IF NOT EXISTS {fqn} — "
                    f"then re-deploy the app."
                ),
            )
        if "PERMISSION_DENIED" in exc_str and "CREATE" in exc_str:
            return HTTPException(
                status_code=503,
                detail=(
                    f"Service principal lacks permission to create objects in '{fqn}'. "
                    f"Run 'make deploy WAREHOUSE_ID=...' again to apply UC grants, "
                    f"or grant permissions manually."
                ),
            )
        return None

    try:
        spark, runs_df = _init_spark_state()
    except Exception as _spark_err:
        setup_err = _actionable_setup_error(_spark_err)
        if setup_err:
            raise setup_err
        if _is_credential_error(_spark_err):
            logger.warning(
                "Spark credential error — resetting session: %s", str(_spark_err)[:200],
            )
            try:
                reset_spark()
                spark, runs_df = _init_spark_state()
            except Exception as _retry_err:
                setup_err = _actionable_setup_error(_retry_err)
                if setup_err:
                    raise setup_err
                if _is_credential_error(_retry_err):
                    logger.warning(
                        "Spark credentials still broken after reset — "
                        "falling back to SQL warehouse for state management"
                    )
                    use_warehouse_fallback = True
                    spark = get_spark()
                    runs_df = _sql_warehouse_query(
                        ws,
                        config.warehouse_id,
                        f"SELECT * FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
                        f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
                    )
                else:
                    raise
        else:
            raise
    if not runs_df.empty and _reconcile_active_runs(
        spark=spark,
        runs_df=runs_df,
        sp_ws=sp_ws,
        catalog=config.catalog,
        schema_name=config.schema_name,
    ):
        runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
    if not runs_df.empty:
        active = runs_df[runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not active.empty:
            active_id = active.iloc[0]["run_id"]
            raise HTTPException(
                status_code=409,
                detail=f"An optimization is already in progress for this space (run {active_id})",
            )

    prev_experiment: str | None = None
    if not runs_df.empty:
        terminal = runs_df[~runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not terminal.empty:
            exp_val = terminal.iloc[0].get("experiment_name")
            if exp_val and str(exp_val) not in ("", "None", "nan"):
                prev_experiment = str(exp_val)

    if prev_experiment and not prev_experiment.startswith("/Shared/"):
        logger.warning("Ignoring legacy experiment path %s, using /Shared/ template", prev_experiment)
        prev_experiment = None

    run_id = str(uuid.uuid4())

    space_snapshot: dict = {}
    _snap_errors: list[str] = []
    for label, cli in [("OBO/user", ws), ("SP", sp_ws)]:
        try:
            space_snapshot = fetch_space_config(cli, space_id)
            logger.info("Captured space snapshot via %s client for %s", label, space_id)
            break
        except PermissionDenied as exc:
            _snap_errors.append(f"{label}: {exc}")
            logger.info(
                "Snapshot via %s failed (PermissionDenied) for %s — trying next",
                label, space_id,
            )
        except Exception as exc:
            _snap_errors.append(f"{label}: {exc}")
            logger.info(
                "Snapshot via %s failed for %s — trying next: %s",
                label, space_id, exc,
            )

    if not space_snapshot:
        combined = "; ".join(_snap_errors)
        logger.error("All clients failed to export space %s: %s", space_id, combined)
        raise HTTPException(
            status_code=403,
            detail=(
                f"Cannot export Genie Space config for {space_id}. "
                "The optimization job requires the space config snapshot to be "
                "captured at trigger time. Ensure the requesting user AND/OR the "
                "app service principal has 'Can Edit' or 'Can Manage' permission "
                f"on the Genie Space. Errors: {combined}"
            ),
        )

    if space_snapshot:
        title = str(space_snapshot.get("title", "") or "")
        domain = re.sub(r"[^a-z0-9_]+", "_", title.lower().replace(" ", "_").replace("-", "_")).strip("_") if title else "default"
    else:
        domain = _infer_domain(_genie_client(ws, sp_ws)[0], space_id)

    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    genie_refs = extract_genie_space_table_refs(space_snapshot) if space_snapshot else []

    try:
        obo_uc_metadata = fetch_uc_metadata_obo(
            ws,
            warehouse_id=config.warehouse_id,
            catalog=config.catalog,
            schema_name=config.schema_name,
            genie_table_refs=genie_refs or None,
        )
        if space_snapshot and obo_uc_metadata:
            space_snapshot["_prefetched_uc_metadata"] = obo_uc_metadata
    except Exception:
        logger.warning("OBO UC metadata prefetch failed for run %s", run_id, exc_info=True)

    from genie_space_optimizer.common.genie_client import sp_can_manage_space
    from .settings import get_sp_principal_aliases

    sp_aliases = get_sp_principal_aliases(sp_ws)
    if not sp_can_manage_space(ws, space_id, sp_aliases, sp_client=sp_ws):
        raise HTTPException(
            status_code=403,
            detail=(
                f"The service principal does not have CAN_EDIT or CAN_MANAGE on Genie Space {space_id}. "
                "Please grant access from the Settings page before starting optimization."
            ),
        )

    try:
        if use_warehouse_fallback:
            missing = []
        else:
            missing = _check_sp_data_access(spark, config.catalog, config.schema_name, genie_refs, sp_ws)
        if missing:
            schemas_str = ", ".join(f"`{c}`.`{s}`" for c, s in missing)
            raise HTTPException(
                status_code=403,
                detail=(
                    f"The service principal is missing read access on: {schemas_str}. "
                    "Please grant data access from the Settings page before starting optimization."
                ),
            )
    except HTTPException:
        raise
    except Exception:
        logger.warning("SP data-access check failed for run %s", run_id, exc_info=True)

    if requested_apply_mode in ("both", "uc_artifact"):
        write_missing = _check_sp_write_access(
            spark, config.catalog, config.schema_name, genie_refs, sp_ws,
        )
        if write_missing:
            schemas_str = ", ".join(f"`{c}`.`{s}`" for c, s in write_missing)
            raise HTTPException(
                status_code=403,
                detail=(
                    f"UC write mode requires MODIFY on: {schemas_str}. "
                    "Please grant write access from the Settings page."
                ),
            )

    current_user = headers.user_email or headers.user_name or ""
    if not current_user:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot determine the requesting user's identity. "
                "Optimization jobs run under the user's permissions (OBO) and "
                "require a valid user email from the authentication headers."
            ),
        )

    from genie_space_optimizer.common.config import EXPERIMENT_PATH_TEMPLATE, format_mlflow_template
    experiment_name = prev_experiment or format_mlflow_template(
        EXPERIMENT_PATH_TEMPLATE, space_id=space_id, domain=domain,
    )

    resolved_levers = levers if levers else list(DEFAULT_LEVER_ORDER)

    if use_warehouse_fallback:
        _wh_create_run(
            ws, config.warehouse_id,
            run_id=run_id, space_id=space_id, domain=domain,
            catalog=config.catalog, schema=config.schema_name,
            apply_mode=requested_apply_mode, levers=resolved_levers,
            triggered_by=current_user,
            experiment_name=experiment_name,
            config_snapshot=space_snapshot if space_snapshot else None,
        )
    else:
        create_run(
            spark,
            run_id,
            space_id,
            domain,
            config.catalog,
            config.schema_name,
            apply_mode=requested_apply_mode,
            levers=resolved_levers,
            triggered_by=current_user,
            experiment_name=experiment_name,
            config_snapshot=space_snapshot if space_snapshot else None,
        )

    try:
        job_run_id, job_id = submit_optimization(
            sp_ws,
            job_id=config.job_id,
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            catalog=config.catalog,
            schema=config.schema_name,
            apply_mode=requested_apply_mode,
            levers=levers_str,
            triggered_by=current_user,
            experiment_name=experiment_name or "",
            deploy_target=deploy_target or "",
            warehouse_id=config.warehouse_id or "",
        )

        if use_warehouse_fallback:
            _sql_warehouse_execute(
                ws, config.warehouse_id,
                f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
                f"SET status = 'IN_PROGRESS', job_run_id = '{job_run_id}', "
                f"job_id = '{job_id}', "
                f"updated_at = current_timestamp() "
                f"WHERE run_id = '{run_id}'",
            )
        else:
            update_run_status(
                spark, run_id, config.catalog, config.schema_name,
                status="IN_PROGRESS",
                job_run_id=job_run_id,
                job_id=str(job_id),
            )

        host = (sp_ws.config.host or "").rstrip("/")
        workspace_id: int | None = None
        if host:
            try:
                workspace_id = sp_ws.get_workspace_id()
            except Exception:
                workspace_id = None
        if host and workspace_id is not None:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}?o={workspace_id}"
        elif host:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}"
        else:
            job_url = None
        return OptimizeResponse(runId=run_id, jobRunId=job_run_id, jobUrl=job_url)

    except Exception as exc:
        logger.exception("Job submission failed for run %s", run_id)
        try:
            if use_warehouse_fallback:
                _sql_warehouse_execute(
                    ws, config.warehouse_id,
                    f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
                    f"SET status = 'FAILED', "
                    f"convergence_reason = 'job_submission_error: {str(exc)[:500]}', "
                    f"updated_at = current_timestamp() "
                    f"WHERE run_id = '{run_id}'",
                )
            else:
                update_run_status(
                    spark, run_id, config.catalog, config.schema_name,
                    status="FAILED",
                    convergence_reason=f"job_submission_error: {exc}",
                )
        except Exception:
            logger.warning("Failed to update run status to FAILED for %s", run_id)
        raise HTTPException(status_code=500, detail=f"Job submission failed: {exc}") from exc


def _infer_domain(w, space_id: str) -> str:
    """Best-effort domain inference from the Genie Space title."""
    try:
        space = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
        title = space.get("title", "")
        raw = title.lower().replace(" ", "_").replace("-", "_")
        return re.sub(r"[^a-z0-9_]+", "_", raw).strip("_") or "default"
    except Exception:
        return "default"


def _load_baseline_scores(
    spark: Any, run_ids: list[str], catalog: str, schema: str,
) -> dict[str, float | None]:
    """Bulk-fetch baseline accuracy (iteration 0, full eval) for a list of runs."""
    from genie_space_optimizer.common.config import TABLE_ITERATIONS
    from genie_space_optimizer.common.delta_helpers import _fqn, run_query

    if not run_ids:
        return {}
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    ids_csv = ", ".join(f"'{rid}'" for rid in run_ids)
    try:
        df = run_query(
            spark,
            f"SELECT run_id, overall_accuracy FROM {fqn} "
            f"WHERE run_id IN ({ids_csv}) AND iteration = 0 AND eval_scope = 'full'",
        )
    except Exception:
        return {}
    result: dict[str, float | None] = {}
    if not df.empty:
        for _, row in df.iterrows():
            result[row.get("run_id", "")] = safe_float(row.get("overall_accuracy"))
    return result


_safe_float = safe_float
