"""Admin router - org-wide statistics, leaderboard, alerts, and maturity config."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.services.lakebase import get_all_scan_summaries
from backend.services.genie_client import list_genie_spaces
from backend.services.maturity_config import get_active_config, get_default_config, save_admin_overrides
from backend.models import AdminDashboardStats, LeaderboardEntry, AlertItem

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin")


def _list_genie_spaces_safe() -> list[dict]:
    """Fetch all Genie Spaces, returning empty list on failure (non-critical for admin views)."""
    try:
        return list_genie_spaces()
    except Exception as e:
        logger.error(f"Failed to list spaces: {e}")
        return []


def _get_display_name(space_id: str, spaces_map: dict) -> str:
    """Get display name for a space ID."""
    return spaces_map.get(space_id, {}).get("title", space_id)


@router.get("/dashboard")
async def get_dashboard() -> AdminDashboardStats:
    """Get org-wide statistics for the admin dashboard."""
    try:
        # Get all spaces from Databricks
        all_spaces = _list_genie_spaces_safe()
        total_spaces = len(all_spaces)

        # Get all scan summaries from Lakebase
        scan_summaries = await get_all_scan_summaries()
        scanned_spaces = len(scan_summaries)

        if not scan_summaries:
            return AdminDashboardStats(
                total_spaces=total_spaces,
                scanned_spaces=0,
                avg_score=0.0,
                critical_count=0,
                maturity_distribution={},
            )

        scores = [s["score"] for s in scan_summaries]
        avg_score = sum(scores) / len(scores) if scores else 0.0
        critical_count = sum(1 for s in scores if s < 40)

        maturity_dist: dict[str, int] = {}
        for s in scan_summaries:
            maturity = s.get("maturity", "Unknown")
            maturity_dist[maturity] = maturity_dist.get(maturity, 0) + 1

        return AdminDashboardStats(
            total_spaces=total_spaces,
            scanned_spaces=scanned_spaces,
            avg_score=round(avg_score, 1),
            critical_count=critical_count,
            maturity_distribution=maturity_dist,
        )
    except Exception as e:
        logger.exception(f"Failed to get dashboard stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get dashboard stats")


@router.get("/leaderboard")
async def get_leaderboard(top_n: int = 5) -> dict:
    """Get top and bottom N spaces by IQ score."""
    try:
        all_spaces = _list_genie_spaces_safe()
        spaces_map = {s.get("space_id", ""): s for s in all_spaces}

        scan_summaries = await get_all_scan_summaries()
        if not scan_summaries:
            return {"top": [], "bottom": []}

        sorted_summaries = sorted(scan_summaries, key=lambda x: x["score"], reverse=True)

        def make_entry(s: dict) -> LeaderboardEntry:
            return LeaderboardEntry(
                space_id=s["space_id"],
                display_name=_get_display_name(s["space_id"], spaces_map),
                score=s["score"],
                maturity=s.get("maturity", "Unknown"),
                last_scanned=s.get("scanned_at"),
            )

        top = [make_entry(s) for s in sorted_summaries[:top_n]]
        bottom = [make_entry(s) for s in sorted_summaries[-top_n:][::-1]]

        return {"top": [e.model_dump() for e in top], "bottom": [e.model_dump() for e in bottom]}
    except Exception as e:
        logger.exception(f"Failed to get leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Failed to get leaderboard")


@router.get("/alerts")
async def get_alerts(score_threshold: int = 40) -> list[AlertItem]:
    """Get spaces with critical scores (below threshold)."""
    try:
        all_spaces = _list_genie_spaces_safe()
        spaces_map = {s.get("space_id", ""): s for s in all_spaces}

        scan_summaries = await get_all_scan_summaries()
        critical = [s for s in scan_summaries if s["score"] < score_threshold]
        critical.sort(key=lambda x: x["score"])  # lowest first

        alerts = [
            AlertItem(
                space_id=s["space_id"],
                display_name=_get_display_name(s["space_id"], spaces_map),
                score=s["score"],
                top_finding=s["findings"][0] if s.get("findings") else None,
            )
            for s in critical[:20]  # max 20 alerts
        ]

        return alerts
    except Exception as e:
        logger.exception(f"Failed to get alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to get alerts")


# ===== Maturity Config =====


class MaturityConfigUpdate(BaseModel):
    """Request body for updating the maturity config."""
    config: dict


@router.get("/maturity-config")
async def get_maturity_config() -> dict:
    """Get the active maturity config (default + admin overrides)."""
    try:
        config = await get_active_config()
        default = get_default_config()
        return {
            "active": config,
            "default": default,
        }
    except Exception as e:
        logger.exception(f"Failed to get maturity config: {e}")
        raise HTTPException(status_code=500, detail="Failed to get maturity config")


@router.put("/maturity-config")
async def update_maturity_config(request: MaturityConfigUpdate) -> dict:
    """Update admin overrides for the maturity config.

    The request body contains partial overrides that are merged with
    the default config. Send only the fields you want to change.
    """
    try:
        await save_admin_overrides(request.config)
        active = await get_active_config()
        return {"status": "saved", "active": active}
    except Exception as e:
        logger.exception(f"Failed to update maturity config: {e}")
        raise HTTPException(status_code=500, detail="Failed to update maturity config")


@router.post("/maturity-config/reset")
async def reset_maturity_config() -> dict:
    """Reset maturity config to defaults (remove all admin overrides)."""
    try:
        await save_admin_overrides({})
        default = get_default_config()
        return {"status": "reset", "active": default}
    except Exception as e:
        logger.exception(f"Failed to reset maturity config: {e}")
        raise HTTPException(status_code=500, detail="Failed to reset maturity config")
