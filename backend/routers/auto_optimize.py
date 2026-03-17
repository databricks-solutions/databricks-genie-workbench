"""Auto-Optimize router — thin proxy bridging Workbench auth to the GSO engine."""

import logging
import os
import re

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from backend.services.auth import get_workspace_client, get_service_principal_client
from backend.services import gso_lakebase
from genie_space_optimizer.integration import (
    trigger_optimization,
    apply_optimization,
    discard_optimization,
    get_lever_info,
    IntegrationConfig,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auto-optimize")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TriggerRequest(BaseModel):
    space_id: str
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
    can_start: bool
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    return bool(os.environ.get("GSO_CATALOG")) and bool(os.environ.get("GSO_JOB_ID"))


def _delta_query(sql: str) -> list[dict]:
    """Execute a query against the Delta table via SQL Warehouse.

    Returns a list of dicts (rows).  Returns [] on any failure.
    """
    config = _build_gso_config()
    if not config.warehouse_id:
        return []
    try:
        from genie_space_optimizer.common.warehouse import sql_warehouse_query
        ws = get_workspace_client()
        df = sql_warehouse_query(ws, config.warehouse_id, sql)
        if df.empty:
            return []
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.warning("Delta query failed: %s", exc, exc_info=True)
        return []


def _delta_table(name: str) -> str:
    """Return fully-qualified Delta table name for a GSO table."""
    config = _build_gso_config()
    return f"{config.catalog}.{config.schema_name}.{name}"


def _build_gso_config() -> IntegrationConfig:
    return IntegrationConfig(
        catalog=os.environ.get("GSO_CATALOG", ""),
        schema_name=os.environ.get("GSO_SCHEMA", "genie_space_optimizer"),
        warehouse_id=os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", ""),
        job_id=int(os.environ["GSO_JOB_ID"]) if os.environ.get("GSO_JOB_ID") else None,
    )


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
async def check_permissions(space_id: str):
    """Pre-check SP permissions for a Genie Space before optimization."""
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
        from genie_space_optimizer.backend.routes.settings import get_sp_principal_aliases
        from genie_space_optimizer.common.genie_client import sp_can_manage_space

        sp_aliases = get_sp_principal_aliases(sp_ws)
        sp_has_manage = sp_can_manage_space(sp_ws, space_id, sp_aliases)
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
        from genie_space_optimizer.backend.routes.settings import _probe_sp_required_access

        ws = get_workspace_client()
        try:
            config = fetch_space_config(ws, space_id)
        except Exception:
            config = fetch_space_config(sp_ws, space_id)
        refs = extract_genie_space_table_refs(config)
        unique_schemas = set(get_unique_schemas(refs))

        if unique_schemas:
            read_granted, _write_granted = _probe_sp_required_access(sp_ws, unique_schemas)
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

    all_read = all(s.read_granted for s in schemas) if schemas else True
    can_start = sp_has_manage and all_read

    return PermissionCheckResponse(
        sp_display_name=sp_display_name,
        sp_application_id=sp_application_id,
        sp_has_manage=sp_has_manage,
        schemas=schemas,
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
        # Active run already exists for this space
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to trigger optimization: {e}")
        raise HTTPException(status_code=500, detail="Failed to start optimization job.")


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    """Get full run detail including stages and iterations."""
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

    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_iterations')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration ASC"
        )

    # Build pipeline steps from stages
    steps = [
        {
            "stepNumber": i + 1,
            "name": s.get("stage", ""),
            "status": s.get("status", "pending"),
            "durationSeconds": s.get("duration_seconds"),
            "summary": s.get("summary"),
        }
        for i, s in enumerate(stages)
    ]

    # Find baseline and best scores from full-scope iterations only
    baseline_score = None
    baseline_iteration = None
    optimized_score = None
    best_iteration = None
    for it in iterations:
        if it.get("iteration") == 0 and it.get("eval_scope") == "full":
            baseline_score = it.get("overall_accuracy")
            baseline_iteration = 0
        if it.get("eval_scope") == "full":
            accuracy = it.get("overall_accuracy")
            if accuracy is not None and (optimized_score is None or accuracy > optimized_score):
                optimized_score = accuracy
                best_iteration = it.get("iteration")

    return {
        "runId": run.get("run_id"),
        "spaceId": run.get("space_id"),
        "status": run.get("status"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "baselineScore": baseline_score,
        "optimizedScore": optimized_score,
        "baselineIteration": baseline_iteration,
        "bestIteration": best_iteration,
        "steps": steps,
        "convergenceReason": run.get("convergence_reason"),
    }


@router.get("/runs/{run_id}/status")
async def get_run_status(run_id: str):
    """Lightweight status poll endpoint."""
    run = await gso_lakebase.load_gso_run(run_id)
    if not run and _is_configured():
        rows = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_runs')} WHERE run_id = '{run_id}'"
        )
        run = rows[0] if rows else None
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return {
        "runId": run.get("run_id"),
        "status": run.get("status"),
        "spaceId": run.get("space_id"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "baselineScore": run.get("best_accuracy"),
        "optimizedScore": run.get("best_accuracy"),
        "convergenceReason": run.get("convergence_reason"),
    }


@router.get("/levers")
async def list_levers():
    """List available optimization levers (1-5, excludes lever 0)."""
    all_levers = get_lever_info()
    return [lev for lev in all_levers if lev.get("id", 0) != 0]


@router.post("/runs/{run_id}/apply")
async def apply_run(run_id: str):
    """Apply an optimization run's results to the Genie Space."""
    ws = get_workspace_client()
    config = _build_gso_config()

    try:
        result = apply_optimization(run_id, ws, config)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except Exception as e:
        logger.exception(f"Failed to apply optimization {run_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to apply optimization.")


@router.post("/runs/{run_id}/discard")
async def discard_run(run_id: str):
    """Discard an optimization run and rollback to pre-optimization state."""
    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    config = _build_gso_config()

    try:
        result = discard_optimization(run_id, ws, sp_ws, config)
        return {"status": result.status, "runId": result.run_id, "message": result.message}
    except Exception as e:
        logger.exception(f"Failed to discard optimization {run_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to discard optimization.")


@router.get("/spaces/{space_id}/active-run")
async def get_active_run(space_id: str):
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


@router.get("/spaces/{space_id}/runs")
async def list_runs_for_space(space_id: str):
    """List past optimization runs for a space.

    Primary source is Lakebase (fast).  Falls back to the authoritative
    Delta table via SQL Warehouse when Lakebase returns no results.
    """
    runs = await gso_lakebase.load_gso_runs_for_space(space_id)
    if runs:
        return runs

    # Fallback: query Delta table directly
    if not _is_configured():
        return []

    config = _build_gso_config()
    if not config.warehouse_id:
        return []

    try:
        from genie_space_optimizer.common.warehouse import sql_warehouse_query

        ws = get_workspace_client()
        runs_df = sql_warehouse_query(
            ws,
            config.warehouse_id,
            f"SELECT run_id, space_id, status, started_at, completed_at, "
            f"best_accuracy, best_iteration, convergence_reason, triggered_by "
            f"FROM {config.catalog}.{config.schema_name}.genie_opt_runs "
            f"WHERE space_id = '{space_id}' ORDER BY started_at DESC",
        )
        if runs_df.empty:
            return []
        return runs_df.to_dict(orient="records")
    except Exception as exc:
        logger.warning("Delta fallback for runs history failed: %s", exc, exc_info=True)
        return []


@router.get("/runs/{run_id}/iterations")
async def list_iterations(run_id: str):
    """Get per-iteration evaluation details for a run."""
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    if not iterations and _is_configured():
        iterations = _delta_query(
            f"SELECT * FROM {_delta_table('genie_opt_iterations')} "
            f"WHERE run_id = '{run_id}' ORDER BY iteration ASC"
        )
    return iterations


@router.get("/runs/{run_id}/asi-results")
async def list_asi_results(run_id: str, iteration: int = Query(..., description="Iteration number")):
    """Get per-judge ASI failure analysis for a specific iteration."""
    results = await gso_lakebase.load_gso_asi_results(run_id, iteration)
    if not results and _is_configured():
        results = _delta_query(
            f"SELECT * FROM {_delta_table('genie_eval_asi_results')} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration}"
        )
    return results


@router.get("/runs/{run_id}/question-results")
async def list_question_results(run_id: str, iteration: int = Query(..., description="Iteration number")):
    """Get per-question results (question text + SQL) for a specific iteration."""
    import json

    rows_json_str = await gso_lakebase.load_gso_iteration_rows(run_id, iteration)
    if rows_json_str is None and _is_configured():
        delta_rows = _delta_query(
            f"SELECT rows_json FROM {_delta_table('genie_opt_iterations')} "
            f"WHERE run_id = '{run_id}' AND iteration = {iteration} LIMIT 1"
        )
        rows_json_str = delta_rows[0]["rows_json"] if delta_rows else None

    return _parse_question_rows(rows_json_str)


def _parse_question_rows(rows_json_str: str | None) -> list[dict]:
    """Parse rows_json from genie_opt_iterations into per-question results."""
    import json

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
        # Extract question text
        inputs = row.get("inputs") or {}
        question = str(
            row.get("inputs/question")
            or inputs.get("question")
            or row.get("question")
            or ""
        ).strip()

        # Extract question ID
        question_id = str(
            row.get("inputs/question_id")
            or inputs.get("question_id")
            or row.get("question_id")
            or ""
        ).strip()

        if not question_id and not question:
            continue

        # Extract SQL
        outputs = row.get("outputs") or {}
        generated_sql = outputs.get("generated_sql") or row.get("generated_sql")
        expected_sql = outputs.get("expected_sql") or row.get("expected_sql")

        # Determine pass/fail from comparison match_type first, then judge verdict
        comparison = outputs.get("comparison") or {}
        match_type = comparison.get("match_type") or row.get("match_type")

        if match_type is not None:
            passed = match_type not in ("no_match", "error", "failed", "")
        else:
            # Fall back to result_correctness judge verdict
            verdict = (
                row.get("result_correctness/value")
                or row.get("outputs/result_correctness/value")
                or outputs.get("result_correctness/value")
                or ""
            )
            passed = str(verdict).lower() in ("yes", "true", "correct", "pass", "1")

        results.append({
            "question_id": question_id,
            "question": question,
            "generated_sql": generated_sql,
            "expected_sql": expected_sql,
            "passed": passed,
            "match_type": match_type,
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
