"""Read-only benchmark candidate gaps from manager-visible Genie traffic."""

from __future__ import annotations

import asyncio
import logging

from databricks.sdk.errors import DatabricksError, PermissionDenied
from fastapi import APIRouter, HTTPException

from backend.services.auth import require_obo_workspace_client
from backend.watch._validators import validate_space_id
from backend.watch.models import TrafficGapAnalysis
from backend.watch.services.traffic_gap_reader import (
    IncompleteTrafficRead,
    read_traffic_gap_analysis,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/watch/spaces")


@router.get("/{space_id}/traffic-gaps", response_model=TrafficGapAnalysis)
async def get_traffic_gaps(space_id: str) -> TrafficGapAnalysis:
    sid = validate_space_id(space_id)
    try:
        client = require_obo_workspace_client()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=401,
            detail="User authorization is required for traffic analysis.",
        ) from exc

    try:
        return await asyncio.to_thread(
            read_traffic_gap_analysis,
            client=client,
            space_id=sid,
        )
    except PermissionDenied as exc:
        raise HTTPException(
            status_code=403,
            detail="CAN_MANAGE permission is required to analyze all conversations.",
        ) from exc
    except IncompleteTrafficRead as exc:
        logger.info("Traffic analysis unavailable for %s: %s", sid, exc)
        raise HTTPException(
            status_code=503,
            detail="The complete traffic corpus is unavailable; no partial analysis was returned.",
        ) from exc
    except DatabricksError as exc:
        logger.warning("Traffic analysis API read failed for %s: %s", sid, type(exc).__name__)
        raise HTTPException(
            status_code=502,
            detail="The Genie traffic API request failed.",
        ) from exc
