"""Read-only Genie API client used by the GenieWatch surface.

Thin wrapper over the canonical ``backend.services.genie_client`` (workbench),
which carries the create / fix / optimize methods. To avoid duplicating ~150
lines (and the patch-divergence risk that comes with it), the shared read
methods are re-exported here and this module only adds the three watch-only
endpoints:

  - ``get_serialized_space`` — like the workbench helper but tolerant of spaces
    that have no ``serialized_space`` (returns ``{}`` instead of raising).
  - ``list_space_permissions`` — Genie Agent ACLs.
  - ``list_message_comments`` — free-text feedback comments on a message.

Shared auth and metric-view normalization come from the canonical modules.
"""

from __future__ import annotations

import json
import logging

from backend.services.auth import get_service_principal_client, get_workspace_client
from backend.services.genie_client import (
    call_with_sp_fallback,
    get_genie_space,
    list_genie_spaces,
    normalize_metric_view_sources,
)

logger = logging.getLogger(__name__)

__all__ = [
    "get_genie_space",
    "list_genie_spaces",
    "normalize_metric_view_sources",
    "get_serialized_space",
    "list_space_permissions",
    "list_message_comments",
]


def get_serialized_space(genie_space_id: str) -> dict:
    """Fetch a space and return its parsed serialized config (or ``{}``).

    Unlike the workbench variant, this tolerates spaces with no
    ``serialized_space`` payload (the watch surface lists every space, including
    ones that have never been serialized).
    """
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
    path = f"/api/2.0/permissions/genie/{genie_space_id}"
    client = get_service_principal_client()
    return client.api_client.do(method="GET", path=path)


def list_message_comments(
    space_id: str, conversation_id: str, message_id: str
) -> list[dict]:
    """Fetch user-typed comments attached to a Genie message.

    Used by the Feedback tab to surface free-text feedback that audit logs
    don't carry (audit only logs IDs, not content). Returns the raw
    ``comments`` array from the Genie API; the router dedupes / filters empty
    content / sorts.
    """
    if not (space_id and conversation_id and message_id):
        raise ValueError("space_id, conversation_id, and message_id are required")
    path = (
        f"/api/2.0/genie/spaces/{space_id}"
        f"/conversations/{conversation_id}"
        f"/messages/{message_id}/comments"
    )
    resp = call_with_sp_fallback(
        lambda client: client.api_client.do(method="GET", path=path),
        what="list_message_comments",
    )
    return resp.get("comments", []) or []
