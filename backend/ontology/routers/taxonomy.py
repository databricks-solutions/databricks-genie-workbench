"""Ontology taxonomy (SP, tag-derived).

`GET /api/ontology/taxonomy` returns the Domain → Sub-Domain → member tree as it
already exists in governed tags (the ``/`` convention, MV-D37), plus an
``ungrouped`` bucket (metric views / Agents under no domain tag) as a coverage
signal. Read straight from tags + assignments — no clustering, no LLM. Drives
frame 17.0b. TTL-cached via the tag-graph reader. Empty allowlist → empty tree.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter

from backend.ontology.services import inventory, ont_settings, tag_graph, taxonomy
from backend.services import genie_client
from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 — degrade-not-hang
        logger.info("%s failed: %s", getattr(fn, "__name__", fn), e)
        return None


def _agent_labels(spaces: list[dict] | None) -> list[str]:
    """Stable agent identifiers for the ungrouped bucket: 'Title · <id>'."""
    labels: list[str] = []
    for s in spaces or []:
        sid = s.get("id") or s.get("space_id")
        if not sid:
            continue
        title = s.get("display_name") or s.get("title") or "Genie Agent"
        labels.append(f"{title} · {sid}")
    return labels


@router.get("/taxonomy")
async def get_taxonomy() -> dict:
    settings = await ont_settings.get_settings()
    allowlist = settings.catalog_allowlist
    client = get_workspace_client()

    graph, metric_views, spaces = await asyncio.gather(
        asyncio.to_thread(_safe, tag_graph.build_graph, allowlist),
        asyncio.to_thread(_safe, inventory.metric_view_fqns, client, allowlist),
        asyncio.to_thread(_safe, genie_client.list_genie_spaces),
    )

    result = taxonomy.build_taxonomy(
        graph or {"tags": []},
        metric_views or [],
        _agent_labels(spaces),
    )
    return result.model_dump(mode="json")
