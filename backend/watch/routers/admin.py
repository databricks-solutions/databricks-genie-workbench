"""Watch admin router: manual rollup refresh (SP-only)."""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Query

from backend.services import lakebase
from backend.watch.services import system_tables

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/watch/admin")


@router.post("/refresh-rollup")
async def refresh_rollup(days: int = Query(7, ge=1, le=90)) -> dict:
    usage = system_tables.usage_summary_all_spaces(days=days)
    cost = {r["space_id"]: r for r in system_tables.top_spenders(days=days, limit=2000)}
    fb = {r["space_id"]: r for r in system_tables.feedback_summary_all_spaces(days=days)}

    written = 0
    today = date.today()
    for u in usage:
        sid = u.get("space_id")
        if not sid:
            continue
        c = cost.get(sid) or {}
        f = fb.get(sid) or {}
        await lakebase.watch_upsert_daily_rollup({
            "space_id": sid,
            "day": today,
            "queries": int(u.get("queries") or 0),
            "approx_dbus": None,
            "approx_usd": _f(c.get("approx_usd")),
            "feedback_pos": int(f.get("pos") or 0),
            "feedback_neg": int(f.get("neg") or 0),
        })
        written += 1
    return {"days": days, "spaces_written": written}


def _f(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
