"""Watch spaces router: list, detail, refresh."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.services import lakebase
from backend.watch._validators import validate_days, validate_space_id
from backend.watch.models import (
    WatchSpaceListItem,
    WatchSpacePermission,
    WatchSpaceSummary,
)
from backend.watch.services import genie_client, system_tables

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/watch/spaces")


def _manager_permissions(payload: Optional[dict]) -> list[WatchSpacePermission]:
    managers: list[WatchSpacePermission] = []
    if not payload:
        return managers
    for acl in payload.get("access_control_list", []) or []:
        principal = None
        principal_type = None
        for field, kind in (
            ("user_name", "user"),
            ("group_name", "group"),
            ("service_principal_name", "service_principal"),
        ):
            if acl.get(field):
                principal = acl[field]
                principal_type = kind
                break
        if not principal:
            continue
        manage_permissions = [
            p for p in (acl.get("all_permissions", []) or [])
            if p.get("permission_level") == "CAN_MANAGE"
        ]
        if not manage_permissions:
            continue
        managers.append(WatchSpacePermission(
            principal=principal,
            principal_type=principal_type,
            permission_level="CAN_MANAGE",
            inherited=any(
                bool(p.get("inherited") or p.get("inherited_from_object"))
                for p in manage_permissions
            ),
        ))
    return managers


def _to_summary(raw: dict, permissions: Optional[list[dict]] = None) -> dict:
    space_id = raw.get("id") or raw.get("space_id") or ""
    title = raw.get("display_name") or raw.get("title")
    description = raw.get("description")
    return {
        "space_id": space_id,
        "title": title,
        # Retained in the wire/database shape for compatibility. Genie Agents
        # can have multiple managers, so a synthesized single owner is omitted.
        "owner_email": None,
        "description": description,
        "permissions": permissions or [],
        "last_seen_at": datetime.now(timezone.utc),
    }


async def _populate_permissions(summaries: list[dict], *, force: bool = False) -> None:
    cached = {
        s["space_id"]: s.get("permissions") or []
        for s in await lakebase.watch_list_cached_spaces()
    }
    semaphore = asyncio.Semaphore(8)

    async def populate(summary: dict) -> None:
        sid = summary["space_id"]
        old_permissions = cached.get(sid, [])
        if old_permissions and not force:
            summary["permissions"] = old_permissions
            return
        try:
            async with semaphore:
                payload = await asyncio.to_thread(genie_client.list_space_permissions, sid)
            summary["permissions"] = [p.model_dump() for p in _manager_permissions(payload)]
        except Exception as e:
            logger.info("list_space_permissions(%s) failed: %s", sid, e)
            summary["permissions"] = old_permissions

    await asyncio.gather(*(populate(summary) for summary in summaries))


async def _refresh_cache_with_live_listing(*, force_permissions: bool = False) -> list[dict]:
    try:
        spaces = genie_client.list_genie_spaces()
    except Exception as e:
        logger.warning("genie list failed: %s", e)
        spaces = []
    summaries: list[dict] = []
    for s in spaces:
        summary = _to_summary(s)
        if not summary["space_id"]:
            continue
        summaries.append(summary)
    await _populate_permissions(summaries, force=force_permissions)
    for summary in summaries:
        await lakebase.watch_upsert_space(summary)
    return summaries


@router.get("")
async def list_spaces(days: int = Query(7, ge=1, le=365)) -> list[dict]:
    days = validate_days(days, default=7)
    try:
        live = genie_client.list_genie_spaces()
    except Exception as e:
        logger.warning("live list_genie_spaces failed (%s) — falling back to Lakebase cache", e)
        live = []

    if live:
        summaries = []
        for s in live:
            summary = _to_summary(s)
            if not summary["space_id"]:
                continue
            summaries.append(summary)
        await _populate_permissions(summaries)
        for summary in summaries:
            await lakebase.watch_upsert_space(summary)
    else:
        summaries = await lakebase.watch_list_cached_spaces()

    visible_ids = {s["space_id"] for s in summaries}

    def _safe(fn, **kwargs):
        try:
            return fn(**kwargs)
        except Exception as e:
            logger.warning("%s failed: %s", fn.__name__, e)
            return []

    usage_rows, spend_rows, fb_rows = await asyncio.gather(
        asyncio.to_thread(_safe, system_tables.usage_summary_all_spaces, days=days),
        asyncio.to_thread(_safe, system_tables.top_spenders, days=days, limit=500),
        asyncio.to_thread(_safe, system_tables.feedback_summary_all_spaces, days=days),
    )
    usage_by_id = {r["space_id"]: r for r in usage_rows if r.get("space_id") in visible_ids}
    spend_by_id = {r["space_id"]: r for r in spend_rows if r.get("space_id") in visible_ids}
    fb_by_id = {r["space_id"]: r for r in fb_rows if r.get("space_id") in visible_ids}

    out: list[dict] = []
    for s in summaries:
        sid = s["space_id"]
        u = usage_by_id.get(sid) or {}
        sp = spend_by_id.get(sid) or {}
        fb = fb_by_id.get(sid) or {}
        item = WatchSpaceListItem(
            **s,
            queries_7d=int(u.get("queries") or 0),
            cost_7d_usd=float(sp.get("approx_usd") or 0.0),
            feedback_pos_7d=int(fb.get("pos") or 0),
            feedback_neg_7d=int(fb.get("neg") or 0),
            last_query_at=u.get("last_query_at"),
        )
        out.append(item.model_dump(mode="json"))
    return out


@router.get("/{space_id}")
async def get_space(space_id: str) -> dict:
    sid = validate_space_id(space_id)
    try:
        raw = genie_client.get_genie_space(sid)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Genie Agent not found: {e}")

    cached = {
        s["space_id"]: s.get("permissions") or []
        for s in await lakebase.watch_list_cached_spaces()
    }
    permissions = cached.get(sid, [])
    try:
        payload = await asyncio.to_thread(genie_client.list_space_permissions, sid)
        permissions = [p.model_dump() for p in _manager_permissions(payload)]
    except Exception as e:
        logger.info("list_space_permissions(%s) failed: %s", sid, e)

    summary = _to_summary(raw, permissions=permissions)
    await lakebase.watch_upsert_space(summary)
    return WatchSpaceSummary(**summary).model_dump(mode="json")


@router.post("/refresh")
async def refresh_spaces() -> dict:
    summaries = await _refresh_cache_with_live_listing(force_permissions=True)
    return {"refreshed": len(summaries)}
