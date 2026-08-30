"""Ontology taxonomy (SP, tag-derived).

`GET /api/ontology/taxonomy` returns the Domain → Sub-Domain → member tree as it
already exists in governed tags (the ``/`` convention, MV-D37), plus an
``ungrouped`` bucket (metric views / Agents under no domain tag) as a coverage
signal. Drives frame 17.0b.

Phase 2 reader swap (MV-D41/D43): serve the materialized mirror when it is fresh;
otherwise degrade to the Phase-1 live-SP path (read straight from tags +
assignments — no clustering, no LLM). The response model is unchanged; only the
source of the data (and the meaning of ``as_of``) widens. Empty allowlist → empty
tree.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter

from backend.ontology.models import OntologyTaxonomy
from backend.ontology.services import inventory, mirror, ont_settings, refresh, tag_graph, taxonomy
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

    # Mirror-first: serve the materialized tree when it is fresh (sub-second).
    ws = ont_settings._workspace_id()
    if await refresh.mirror_is_fresh(ws):
        tree = await mirror.read_taxonomy_tree(ws)
        if tree is not None:
            return OntologyTaxonomy(**tree).model_dump(mode="json")

    # Fallback: Phase-1 live-SP path (degrade-not-hang, never blocks on the job).
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
