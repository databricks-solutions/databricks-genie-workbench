"""Watch feedback router: workspace-wide feedback aggregation.

Returns a bundled response (summary + daily trend + per-space rollup + event
feed) read from `system.access.audit`. Workspace boundary is enforced by
intersecting audit results with the spaces visible to the app SP in this
workspace (same pattern as backend.watch.routers.spaces.list_spaces).
The daily trend is derived from the workspace-filtered event feed, so it
inherits the same boundary; for very high-volume windows it may undercount
older days. The summary is always accurate.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Query

from backend.services import lakebase
from backend.watch._validators import validate_days
from backend.watch.models import (
    FeedbackEvent,
    FeedbackSpaceRow,
    FeedbackTabResponse,
    FeedbackTabSummary,
    FeedbackTrendPoint,
)
from backend.watch.services import genie_client, system_tables

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/watch")


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.warning("%s failed: %s", fn.__name__, e)
        return []


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except Exception:
        return None


@router.get("/feedback")
async def get_feedback(
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(500, ge=1, le=2000),
) -> dict:
    days = validate_days(days, default=7)

    # Workspace boundary: live Genie listing with Lakebase fallback.
    spaces = await asyncio.to_thread(_safe, genie_client.list_genie_spaces)
    if not spaces:
        spaces = await lakebase.watch_list_cached_spaces() or []

    visible: dict[str, dict] = {}
    for s in spaces:
        sid = s.get("id") or s.get("space_id")
        if sid:
            visible[sid] = s

    if not visible:
        return FeedbackTabResponse(
            days=days,
            summary=FeedbackTabSummary(),
            trend=[],
            per_space=[],
            events=[],
        ).model_dump(mode="json")

    # Two parallel system-table queries — each result is TTL-cached for 5 min.
    # Trend is derived from event_rows below so it inherits the workspace filter.
    per_space_rows, event_rows = await asyncio.gather(
        asyncio.to_thread(_safe, system_tables.feedback_summary_all_spaces, days=days),
        asyncio.to_thread(_safe, system_tables.feedback_events_all_spaces, days=days, limit=limit),
    )

    # Intersect with visible-spaces set (workspace boundary).
    per_space_rows = [r for r in per_space_rows if r.get("space_id") in visible]
    event_rows = [r for r in event_rows if r.get("space_id") in visible]

    # Compute last_feedback_at per space from the (already newest-first) event feed.
    last_seen: dict[str, datetime] = {}
    for e in event_rows:
        sid = e.get("space_id")
        et = e.get("event_time")
        if sid and et and (sid not in last_seen or et > last_seen[sid]):
            last_seen[sid] = et

    # Build per_space rows (only spaces with feedback in the window).
    per_space: list[FeedbackSpaceRow] = []
    for r in per_space_rows:
        sid = r["space_id"]
        live = visible.get(sid) or {}
        pos = int(r.get("pos") or 0)
        neg = int(r.get("neg") or 0)
        total = int(r.get("total") or 0)
        if total == 0:
            continue
        neg_rate = (neg / total) * 100.0 if total else 0.0
        per_space.append(FeedbackSpaceRow(
            space_id=sid,
            title=live.get("display_name") or live.get("title"),
            owner_email=(live.get("creator") or {}).get("user_name") or live.get("owner_email"),
            positive=pos,
            negative=neg,
            total=total,
            neg_rate_pct=round(neg_rate, 1),
            last_feedback_at=last_seen.get(sid),
        ))
    per_space.sort(key=lambda r: (r.negative, r.total), reverse=True)

    # Trend: derived from the (workspace-filtered) event_rows so the trend
    # respects the workspace boundary. Fill missing days with zeros.
    # Note: capped at `limit` events; for very high-volume windows the trend
    # may undercount older days. The summary (from per_space_rows) is always
    # accurate.
    daily_map: dict[date, dict[str, int]] = defaultdict(lambda: {"pos": 0, "neg": 0})
    for e in event_rows:
        day_val = _coerce_date(e.get("event_time"))
        if day_val is None:
            continue
        rating = (e.get("rating") or "").upper()
        if rating == "POSITIVE":
            daily_map[day_val]["pos"] += 1
        elif rating == "NEGATIVE":
            daily_map[day_val]["neg"] += 1
    today = datetime.now(timezone.utc).date()
    trend: list[FeedbackTrendPoint] = []
    for i in range(days, 0, -1):
        d = today - timedelta(days=i - 1)
        v = daily_map.get(d, {"pos": 0, "neg": 0})
        trend.append(FeedbackTrendPoint(day=d, positive=v["pos"], negative=v["neg"]))

    # Summary computed from the workspace-filtered per_space rows
    # (NOT daily_rows, which are metastore-wide before filtering).
    total_pos = sum(r.positive for r in per_space)
    total_neg = sum(r.negative for r in per_space)
    total = total_pos + total_neg
    neg_rate = (total_neg / total) * 100.0 if total else 0.0
    summary = FeedbackTabSummary(
        positive=total_pos,
        negative=total_neg,
        total=total,
        neg_rate_pct=round(neg_rate, 1),
    )

    # Enrich event rows with space title.
    events: list[FeedbackEvent] = []
    for e in event_rows:
        et = e.get("event_time")
        if et is None:
            continue
        sid = e.get("space_id")
        live = visible.get(sid) or {}
        events.append(FeedbackEvent(
            event_time=et,
            user_email=e.get("user_email"),
            rating=e.get("rating"),
            comment=e.get("comment"),
            message_id=e.get("message_id"),
            conversation_id=e.get("conversation_id"),
            space_id=sid,
            space_title=live.get("display_name") or live.get("title"),
        ))

    return FeedbackTabResponse(
        days=days,
        summary=summary,
        trend=trend,
        per_space=per_space,
        events=events,
    ).model_dump(mode="json")
