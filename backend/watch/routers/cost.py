"""Watch cost router."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Optional

from fastapi import APIRouter, Query

from backend.services import lakebase
from backend.watch._validators import validate_days, validate_space_id
from backend.watch.models import (
    CostPerConversation,
    CostPoint,
    CostRollup,
    CostTopSpender,
    DailyVolumePoint,
    WorkspaceOverview,
)
from backend.watch.services import system_tables

router = APIRouter(prefix="/api/watch")


@router.get("/overview")
def workspace_overview(days: int = Query(7, ge=1, le=365)) -> dict:
    """Workspace-wide KPIs + daily query volume for the native cost-tab overview."""
    days = validate_days(days, default=7)
    summary = system_tables.workspace_summary(days=days)
    daily = system_tables.daily_volume_all_spaces(days=days)
    return WorkspaceOverview(
        days=days,
        active_spaces=int(summary.get("active_spaces") or 0),
        total_queries=int(summary.get("total_queries") or 0),
        distinct_users=int(summary.get("distinct_users") or 0),
        approx_usd=_f(summary.get("approx_usd")),
        feedback_pos=int(summary.get("pos_feedback") or 0),
        feedback_neg=int(summary.get("neg_feedback") or 0),
        daily=[
            DailyVolumePoint(day=r["day"], queries=int(r.get("queries") or 0))
            for r in daily
            if r.get("day")
        ],
    ).model_dump(mode="json")


@router.get("/spaces/{space_id}/cost")
def get_space_cost(space_id: str, days: int = Query(7, ge=1, le=365)) -> dict:
    sid = validate_space_id(space_id)
    days = validate_days(days, default=7)

    rows = system_tables.cost_per_space(sid, days=days)
    time_series: list[CostPoint] = []
    by_warehouse_acc: dict[str, dict] = defaultdict(lambda: {
        "query_count": 0, "approx_usd": 0.0, "approx_dbus": 0.0,
    })
    total_q = 0
    total_usd = 0.0
    total_dbus = 0.0
    for r in rows:
        cp = CostPoint(
            day=r["day"],
            warehouse_id=r.get("warehouse_id"),
            query_count=int(r.get("query_count") or 0),
            approx_usd=_f(r.get("approx_usd")),
            approx_dbus=_f(r.get("approx_dbus")),
        )
        time_series.append(cp)
        wid = cp.warehouse_id or "(unknown)"
        by_warehouse_acc[wid]["query_count"] += cp.query_count
        by_warehouse_acc[wid]["approx_usd"] += cp.approx_usd or 0.0
        by_warehouse_acc[wid]["approx_dbus"] += cp.approx_dbus or 0.0
        total_q += cp.query_count
        total_usd += cp.approx_usd or 0.0
        total_dbus += cp.approx_dbus or 0.0

    by_warehouse = [
        CostPoint(
            day=time_series[0].day if time_series else None,
            warehouse_id=wid,
            query_count=v["query_count"],
            approx_usd=v["approx_usd"],
            approx_dbus=v["approx_dbus"],
        )
        for wid, v in by_warehouse_acc.items()
    ] if time_series else []

    return CostRollup(
        space_id=sid,
        days=days,
        total_query_count=total_q,
        total_approx_usd=total_usd if rows else None,
        total_approx_dbus=total_dbus if rows else None,
        by_warehouse=by_warehouse,
        time_series=time_series,
    ).model_dump(mode="json")


@router.get("/cost/top")
async def top_spenders(days: int = Query(7, ge=1, le=365), limit: int = Query(10, ge=1, le=200)) -> list[dict]:
    days = validate_days(days, default=7)
    rows = await asyncio.to_thread(system_tables.top_spenders, days=days, limit=limit)

    # Genie Agent titles aren't in the system tables; resolve them from the
    # space cache (same source SpacesList uses). Best-effort: a missing cache
    # entry just leaves title=None and the UI falls back to the space id.
    titles: dict[str, str] = {}
    try:
        for s in await lakebase.watch_list_cached_spaces():
            sid, title = s.get("space_id"), s.get("title")
            if sid and title:
                titles[sid] = title
    except Exception:  # noqa: BLE001 - never fail cost data on a title lookup
        titles = {}

    return [
        CostTopSpender(
            space_id=r["space_id"],
            title=titles.get(r["space_id"]),
            workspace_id=r.get("workspace_id"),
            workspace_name=r.get("workspace_name"),
            query_count=int(r.get("query_count") or 0),
            approx_usd=_f(r.get("approx_usd")),
        ).model_dump(mode="json")
        for r in rows
        if r.get("space_id")
    ]


@router.get("/spaces/{space_id}/cost/top-queries")
def top_expensive_queries(
    space_id: str,
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict]:
    sid = validate_space_id(space_id)
    days = validate_days(days, default=7)
    return system_tables.top_expensive_queries(sid, days=days, limit=limit)


@router.get("/spaces/{space_id}/cost/conversations")
def cost_per_conversation(
    space_id: str,
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    sid = validate_space_id(space_id)
    days = validate_days(days, default=7)
    rows = system_tables.cost_per_conversation(sid, days=days, limit=limit)
    return [
        CostPerConversation(
            conversation_id=r.get("conversation_id") or "",
            user_email=r.get("user_email"),
            first_query_at=r.get("first_query_at"),
            last_query_at=r.get("last_query_at"),
            query_count=int(r.get("query_count") or 0),
            approx_usd=_f(r.get("approx_usd")),
        ).model_dump(mode="json")
        for r in rows
        if r.get("conversation_id")
    ]


def _f(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
