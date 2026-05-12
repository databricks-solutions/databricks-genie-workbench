"""Genie conversation enumeration with caching.

Always runs as the service principal so a single sync covers every space
regardless of which user is browsing the app. Persists into the shared
workbench Lakebase under the `genie.watch_*` tables.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from backend.services import lakebase
from backend.services.auth import get_service_principal_client

logger = logging.getLogger(__name__)


def _client():
    return get_service_principal_client()


def _list_conversations(space_id: str) -> list[dict[str, Any]]:
    client = _client()
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params = {"page_size": 50}
        if page_token:
            params["page_token"] = page_token
        resp = client.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{space_id}/conversations",
            query=params,
        )
        items = resp.get("conversations", []) or []
        out.extend(items)
        page_token = resp.get("next_page_token")
        if not page_token or not items:
            break
    return out


def _list_messages(space_id: str, conversation_id: str) -> list[dict[str, Any]]:
    client = _client()
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params = {"page_size": 50}
        if page_token:
            params["page_token"] = page_token
        resp = client.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
            query=params,
        )
        items = resp.get("messages", []) or []
        out.extend(items)
        page_token = resp.get("next_page_token")
        if not page_token or not items:
            break
    return out


async def sync_space(space_id: str, fetch_messages: bool = True) -> dict[str, int]:
    resource_key = f"conversations:{space_id}"
    try:
        conversations = _list_conversations(space_id)
    except Exception as e:
        logger.warning("conversation list failed for %s: %s", space_id, e)
        await lakebase.watch_set_watermark(resource_key, status="error", error=str(e))
        return {"conversations": 0, "messages": 0, "error": str(e)}

    n_msgs = 0
    for c in conversations:
        cid = c.get("id") or c.get("conversation_id") or ""
        if not cid:
            continue
        last_msg_at = None
        msg_count = 0
        if fetch_messages:
            try:
                msgs = _list_messages(space_id, cid)
            except Exception as e:
                logger.debug("message list failed for %s/%s: %s", space_id, cid, e)
                msgs = []
            msg_count = len(msgs)
            for m in msgs:
                created = m.get("created_timestamp") or m.get("created_at")
                if created and (last_msg_at is None or str(created) > str(last_msg_at)):
                    last_msg_at = created
                await lakebase.watch_upsert_message({
                    "space_id": space_id,
                    "conversation_id": cid,
                    "message_id": m.get("id") or m.get("message_id") or "",
                    "user_email": (m.get("user") or {}).get("user_name"),
                    "created_at": _ts(created),
                    "status": m.get("status"),
                    "has_sql": bool(_extract_sql(m)),
                    "feedback_rating": m.get("feedback") or None,
                })
                n_msgs += 1

        await lakebase.watch_upsert_conversation({
            "space_id": space_id,
            "conversation_id": cid,
            "user_email": (c.get("user") or {}).get("user_name"),
            "created_at": _ts(c.get("created_timestamp") or c.get("created_at")),
            "message_count": msg_count or c.get("message_count") or 0,
            "last_message_at": _ts(last_msg_at or c.get("last_active_timestamp")
                                   or c.get("updated_at")),
        })

    await lakebase.watch_set_watermark(resource_key, status="ok")
    return {"conversations": len(conversations), "messages": n_msgs}


def _ts(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value)
    try:
        if s.isdigit():
            return datetime.utcfromtimestamp(int(s) / 1000)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_sql(message: dict) -> str | None:
    for att in message.get("attachments", []) or []:
        q = att.get("query")
        if isinstance(q, str) and q.strip():
            return q
        if isinstance(q, dict):
            inner = q.get("query")
            if isinstance(inner, str) and inner.strip():
                return inner
    return None
