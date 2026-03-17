"""Auto-Optimize router — thin proxy bridging Workbench auth to the GSO engine."""

import logging
import os

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    return bool(os.environ.get("GSO_CATALOG")) and bool(os.environ.get("GSO_JOB_ID"))


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
    """Check if GSO is configured for this deployment."""
    return {"configured": _is_configured()}


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
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stages = await gso_lakebase.load_gso_stages(run_id)
    iterations = await gso_lakebase.load_gso_iterations(run_id)

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

    # Find baseline and best scores from iterations
    baseline_score = None
    optimized_score = None
    for it in iterations:
        if it.get("eval_scope") == "baseline":
            baseline_score = it.get("overall_accuracy")
        accuracy = it.get("overall_accuracy")
        if accuracy is not None:
            if optimized_score is None or accuracy > optimized_score:
                optimized_score = accuracy

    return {
        "runId": run.get("run_id"),
        "spaceId": run.get("space_id"),
        "status": run.get("status"),
        "startedAt": _isoformat(run.get("started_at")),
        "completedAt": _isoformat(run.get("completed_at")),
        "baselineScore": baseline_score,
        "optimizedScore": optimized_score,
        "steps": steps,
        "convergenceReason": run.get("convergence_reason"),
    }


@router.get("/runs/{run_id}/status")
async def get_run_status(run_id: str):
    """Lightweight status poll endpoint."""
    run = await gso_lakebase.load_gso_run(run_id)
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


@router.get("/spaces/{space_id}/runs")
async def list_runs_for_space(space_id: str):
    """List past optimization runs for a space."""
    runs = await gso_lakebase.load_gso_runs_for_space(space_id)
    return runs


@router.get("/runs/{run_id}/iterations")
async def list_iterations(run_id: str):
    """Get per-iteration evaluation details for a run."""
    iterations = await gso_lakebase.load_gso_iterations(run_id)
    return iterations


@router.get("/runs/{run_id}/asi-results")
async def list_asi_results(run_id: str, iteration: int = Query(..., description="Iteration number")):
    """Get per-judge ASI failure analysis for a specific iteration."""
    results = await gso_lakebase.load_gso_asi_results(run_id, iteration)
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
