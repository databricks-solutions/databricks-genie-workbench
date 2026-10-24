"""Admin router - statistics, leaderboard, and alerts over the caller's agents."""

import asyncio
import logging

from fastapi import APIRouter, HTTPException

from backend.services.lakebase import get_all_scan_summaries
from backend.services.genie_client import list_genie_spaces
from backend.models import AdminDashboardStats, LeaderboardEntry, AlertItem
from genie_space_optimizer.iq_scan.scoring import viewer_safe_text

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin")

LISTING_UNAVAILABLE = "Could not list your Genie Agents. Reload to try again."


async def _list_visible_spaces() -> list[dict]:
    """The agents the caller can see. A failure is an error, never zero agents."""
    try:
        return await asyncio.to_thread(list_genie_spaces, sp_fallback=False)
    except Exception as exc:
        logger.error("Admin listing failed: %s", type(exc).__name__)
        raise HTTPException(status_code=503, detail=LISTING_UNAVAILABLE)


def _get_display_name(space_id: str, spaces_map: dict) -> str:
    """Get display name for a space ID."""
    return spaces_map.get(space_id, {}).get("title", space_id)


@router.get("/dashboard")
async def get_dashboard() -> AdminDashboardStats:
    """Get statistics over the caller's agents for the admin dashboard."""
    try:
        all_spaces = await _list_visible_spaces()
        total_spaces = len(all_spaces)

        # Get all scan summaries from Lakebase
        scan_summaries = await get_all_scan_summaries()
        valid_ids = {s.get("space_id", "") for s in all_spaces}
        scan_summaries = [s for s in scan_summaries if s["space_id"] in valid_ids]
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
        critical_count = sum(1 for s in scan_summaries if s.get("maturity") == "Not Ready")

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
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get dashboard stats: %s", type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to get dashboard stats")


@router.get("/leaderboard")
async def get_leaderboard(top_n: int = 5) -> dict:
    """Get top and bottom N spaces by IQ score."""
    try:
        all_spaces = await _list_visible_spaces()
        spaces_map = {s.get("space_id", ""): s for s in all_spaces}

        scan_summaries = await get_all_scan_summaries()
        scan_summaries = [s for s in scan_summaries if s["space_id"] in spaces_map]
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get leaderboard: %s", type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to get leaderboard")


@router.get("/alerts")
async def get_alerts() -> list[AlertItem]:
    """Get spaces with 'Not Ready' maturity."""
    try:
        all_spaces = await _list_visible_spaces()
        spaces_map = {s.get("space_id", ""): s for s in all_spaces}

        scan_summaries = await get_all_scan_summaries()
        scan_summaries = [s for s in scan_summaries if s["space_id"] in spaces_map]
        critical = [s for s in scan_summaries if s.get("maturity") == "Not Ready"]
        critical.sort(key=lambda x: x["score"])  # lowest first

        alerts = [
            AlertItem(
                space_id=s["space_id"],
                display_name=_get_display_name(s["space_id"], spaces_map),
                score=s["score"],
                top_finding=next(
                    (t for t in map(viewer_safe_text, s.get("findings") or []) if t), None),
            )
            for s in critical[:20]  # max 20 alerts
        ]

        return alerts
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get alerts: %s", type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to get alerts")
