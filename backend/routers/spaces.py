"""Spaces router - org-wide Genie Space listing with IQ scoring."""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import json

from backend.services.auth import get_workspace_client, get_service_principal_client
from backend.services.genie_client import list_genie_spaces, _is_scope_error
from backend.services.lakebase import (
    get_latest_score,
    get_score_history,
    star_space,
    get_starred_spaces,
    get_all_scan_summaries,
)
from backend.services.scanner import scan_space
from backend.models import (
    SpaceListItem,
    SpaceScanRequest,
    StarToggleRequest,
    ScanResult,
    ScoreBreakdown,
    FixRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")



@router.get("/spaces")
async def list_spaces(
    search: Optional[str] = Query(None, description="Filter by display name"),
    starred_only: bool = Query(False, description="Only show starred spaces"),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    max_score: Optional[int] = Query(None, ge=0, le=100),
) -> list[SpaceListItem]:
    """List all Genie Spaces with their IQ scores.

    Fetches space list from Databricks API and enriches with stored IQ scores.
    """
    try:
        try:
            raw_spaces = list_genie_spaces()
        except Exception as e:
            logger.error(f"Failed to list Genie Spaces: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch Genie Spaces from Databricks")

        client = get_workspace_client()
        host = (client.config.host or "").rstrip("/")

        starred_ids = await get_starred_spaces()
        starred_set = set(starred_ids)

        items = []
        for space in raw_spaces:
            space_id = space.get("space_id", "")
            display_name = space.get("title", space_id)

            # Filter by starred
            if starred_only and space_id not in starred_set:
                continue

            # Filter by name search
            if search and search.lower() not in display_name.lower():
                continue

            # Get latest score from Lakebase
            score_data = await get_latest_score(space_id)
            score = score_data.get("score") if score_data else None
            maturity = score_data.get("maturity") if score_data else None
            last_scanned = score_data.get("scanned_at") if score_data else None

            # Filter by score range
            if min_score is not None and (score is None or score < min_score):
                continue
            if max_score is not None and (score is None or score > max_score):
                continue

            items.append(SpaceListItem(
                space_id=space_id,
                display_name=display_name,
                score=score,
                maturity=maturity,
                is_starred=(space_id in starred_set),
                last_scanned=last_scanned,
                space_url=f"{host}/genie/rooms/{space_id}" if host else None,
            ))

        # Sort: starred first, then by score descending (unscanned last)
        items.sort(key=lambda x: (
            not x.is_starred,
            -(x.score if x.score is not None else -1)
        ))

        return items
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to list spaces: {e}")
        raise HTTPException(status_code=500, detail="Failed to list spaces")


@router.get("/spaces/{space_id}")
async def get_space_detail(space_id: str) -> dict:
    """Get space details with latest scan result."""
    try:
        client = get_workspace_client()

        # Fetch space metadata, with SP fallback for scope errors
        try:
            space = client.api_client.do(
                method="GET",
                path=f"/api/2.0/genie/spaces/{space_id}",
            )
        except Exception as e:
            if _is_scope_error(e):
                logger.info("OBO token lacks genie scope, retrying with service principal")
                sp_client = get_service_principal_client()
                if sp_client is not client:
                    space = sp_client.api_client.do(
                        method="GET",
                        path=f"/api/2.0/genie/spaces/{space_id}",
                    )
                else:
                    raise
            else:
                raise

        # Get latest score
        score_data = await get_latest_score(space_id)
        starred_ids = await get_starred_spaces()

        return {
            "space": space,
            "scan_result": score_data,
            "is_starred": space_id in set(starred_ids),
        }
    except Exception as e:
        logger.exception(f"Failed to get space detail: {e}")
        raise HTTPException(status_code=500, detail="Failed to get space detail")


@router.post("/spaces/{space_id}/scan")
async def trigger_scan(space_id: str) -> ScanResult:
    """Trigger an IQ scan for a Genie Space and persist results."""
    try:
        scan_data = await scan_space(space_id)

        return ScanResult(
            space_id=space_id,
            score=scan_data["score"],
            maturity=scan_data["maturity"],
            breakdown=ScoreBreakdown(**scan_data["breakdown"]),
            findings=scan_data.get("findings", []),
            next_steps=scan_data.get("next_steps", []),
            scanned_at=scan_data["scanned_at"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Scan failed for {space_id}: {e}")
        raise HTTPException(status_code=500, detail="Scan failed")


@router.get("/spaces/{space_id}/history")
async def get_history(
    space_id: str,
    days: int = Query(30, ge=1, le=365),
) -> list[dict]:
    """Get score history for a Genie Space."""
    try:
        return await get_score_history(space_id, days=days)
    except Exception as e:
        logger.exception(f"Failed to get history for {space_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get history")


@router.put("/spaces/{space_id}/star")
async def toggle_star(space_id: str, request: StarToggleRequest) -> dict:
    """Toggle star status for a Genie Space."""
    try:
        await star_space(space_id, request.starred)
        return {"space_id": space_id, "starred": request.starred}
    except Exception as e:
        logger.exception(f"Failed to toggle star for {space_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to toggle star")


@router.post("/spaces/{space_id}/fix")
async def run_fix_agent(space_id: str, request: FixRequest):
    """Run the AI fix agent on a space. Returns SSE stream."""
    from backend.services.fix_agent import get_fix_agent

    async def generate():
        agent = get_fix_agent()
        async for event in agent.run(
            space_id=space_id,
            findings=request.findings,
            space_config=request.space_config,
        ):
            yield f"data: {json.dumps(event)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(generate(), media_type="text/event-stream", headers=headers)
