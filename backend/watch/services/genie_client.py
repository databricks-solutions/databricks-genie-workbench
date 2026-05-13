"""Read-only Genie API client used by the GenieWatch surface.

Distinct from `backend.services.genie_client` (workbench), which carries
create / fix / optimize methods. This module only exposes read methods that
the watch routers need (list, get, ACLs).

Auth is the shared workbench OBO contextvar + SP fallback.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from backend.services.auth import (
    get_service_principal_client,
    get_workspace_client,
    is_running_on_databricks_apps,
)

logger = logging.getLogger(__name__)


def _enum_value_upper(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").upper()


def _identifier_leaf(identifier: str) -> str:
    return identifier.replace("`", "").split(".")[-1].lower()


def _entry_declares_metric_view(entry: dict) -> bool:
    for key in ("table_type", "type", "object_type"):
        if "METRIC_VIEW" in _enum_value_upper(entry.get(key)):
            return True
    return False


def _uc_metric_view_status(client, identifier: str) -> bool | None:
    if not client or not identifier:
        return None
    try:
        info = client.tables.get(full_name=identifier)
    except Exception as exc:
        logger.debug("Unable to fetch UC table type for %s: %s", identifier, exc)
        return None
    table_type = _enum_value_upper(getattr(info, "table_type", None))
    if not table_type:
        return None
    return "METRIC_VIEW" in table_type


def normalize_metric_view_sources(space_data: dict, client=None) -> dict:
    data_sources = space_data.get("data_sources")
    if not isinstance(data_sources, dict):
        return space_data

    tables = data_sources.get("tables", [])
    metric_views = data_sources.get("metric_views", [])
    if not isinstance(tables, list):
        return space_data
    if not isinstance(metric_views, list):
        metric_views = []

    normalized_tables: list[Any] = []
    normalized_metric_views = [mv for mv in metric_views if isinstance(mv, dict)]
    metric_view_ids = {
        mv.get("identifier")
        for mv in normalized_metric_views
        if isinstance(mv.get("identifier"), str)
    }
    moved = 0

    for table in tables:
        if not isinstance(table, dict):
            normalized_tables.append(table)
            continue
        identifier = table.get("identifier")
        if not isinstance(identifier, str) or not identifier:
            normalized_tables.append(table)
            continue
        if identifier in metric_view_ids:
            moved += 1
            continue
        uc_status = _uc_metric_view_status(client, identifier)
        is_metric_view = (
            _entry_declares_metric_view(table)
            or uc_status is True
            or (uc_status is None and _identifier_leaf(identifier).startswith("mv_"))
        )
        if is_metric_view:
            normalized_metric_views.append(table)
            metric_view_ids.add(identifier)
            moved += 1
        else:
            normalized_tables.append(table)

    if moved:
        normalized_tables.sort(key=lambda x: x.get("identifier", "") if isinstance(x, dict) else "")
        normalized_metric_views.sort(key=lambda x: x.get("identifier", "") if isinstance(x, dict) else "")
        data_sources["tables"] = normalized_tables
        data_sources["metric_views"] = normalized_metric_views
    return space_data


def _is_scope_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "scope" in msg or "insufficient_scope" in msg


def get_genie_space(genie_space_id: str) -> dict:
    if not genie_space_id:
        raise ValueError("genie_space_id is required")
    client = get_workspace_client()
    logger.info(
        "Fetching Genie Space %s (apps=%s, host=%s)",
        genie_space_id, is_running_on_databricks_apps(), client.config.host,
    )
    try:
        return _get_space_with_client(client, genie_space_id)
    except Exception as e:
        if _is_scope_error(e):
            logger.info("OBO token lacks genie scope, retrying with SP")
            sp_client = get_service_principal_client()
            if sp_client is not client:
                return _get_space_with_client(sp_client, genie_space_id)
        raise


def _get_space_with_client(client, genie_space_id: str) -> dict:
    return client.api_client.do(
        method="GET",
        path=f"/api/2.0/genie/spaces/{genie_space_id}",
        query={"include_serialized_space": "true"},
    )


def list_genie_spaces() -> list[dict]:
    client = get_workspace_client()
    try:
        return _list_spaces_with_client(client)
    except Exception as e:
        if _is_scope_error(e):
            logger.info("OBO token lacks genie scope, retrying with SP")
            sp_client = get_service_principal_client()
            if sp_client is not client:
                return _list_spaces_with_client(sp_client)
        raise


def _list_spaces_with_client(client) -> list[dict]:
    spaces = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        response = client.api_client.do(
            method="GET",
            path="/api/2.0/genie/spaces",
            query=params,
        )
        items = response.get("spaces", [])
        spaces.extend(items)
        page_token = response.get("next_page_token")
        if not page_token or not items:
            break
    return spaces


def get_serialized_space(genie_space_id: str) -> dict:
    data = get_genie_space(genie_space_id=genie_space_id)
    raw = data.get("serialized_space")
    if not raw:
        return {}
    space_data = json.loads(raw) if isinstance(raw, str) else raw
    try:
        client = get_workspace_client()
    except Exception:
        client = None
    return normalize_metric_view_sources(space_data, client=client)


def list_space_permissions(genie_space_id: str) -> dict:
    if not genie_space_id:
        raise ValueError("genie_space_id is required")
    client = get_workspace_client()
    try:
        return client.api_client.do(
            method="GET",
            path=f"/api/2.0/permissions/genie/{genie_space_id}",
        )
    except Exception as e:
        if _is_scope_error(e):
            sp = get_service_principal_client()
            if sp is not client:
                return sp.api_client.do(
                    method="GET",
                    path=f"/api/2.0/permissions/genie/{genie_space_id}",
                )
        raise


def list_message_comments(
    space_id: str, conversation_id: str, message_id: str
) -> list[dict]:
    """Fetch user-typed comments attached to a Genie message.

    Used by the Feedback tab to surface free-text feedback that audit logs
    don't carry (audit only logs IDs, not content). Returns the raw
    `comments` array from the Genie API; the router dedupes / filters empty
    content / sorts.
    """
    if not (space_id and conversation_id and message_id):
        raise ValueError("space_id, conversation_id, and message_id are required")
    path = (
        f"/api/2.0/genie/spaces/{space_id}"
        f"/conversations/{conversation_id}"
        f"/messages/{message_id}/comments"
    )
    client = get_workspace_client()
    try:
        resp = client.api_client.do(method="GET", path=path)
        return resp.get("comments", []) or []
    except Exception as e:
        if _is_scope_error(e):
            sp = get_service_principal_client()
            if sp is not client:
                resp = sp.api_client.do(method="GET", path=path)
                return resp.get("comments", []) or []
        raise
