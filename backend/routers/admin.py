"""Admin router - org-wide statistics, leaderboard, and alerts."""

import logging

from fastapi import APIRouter, HTTPException

from backend.services.lakebase import get_all_scan_summaries
from backend.services.genie_client import list_genie_spaces
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
        critical_count = sum(1 for s in scores if s <= 25)

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
async def get_alerts(score_threshold: int = 26) -> list[AlertItem]:
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
