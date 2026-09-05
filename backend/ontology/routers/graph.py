"""Ontology estate-graph route (Phase 3e / Ontology Map) — read-only.

Serves the pre-laid-out estate graph from the Lakebase mirror
(``genie_ont_graph_snapshot``) with a deterministic, pre-computed layout. It never
recomputes the fused signal graph on the request path. Degrade-not-hang (MV-D43):
a not-fresh / empty / failed read returns a typed empty graph (``state="cold"``),
never a 500. The ``router`` object is pre-registered in ``backend/main.py`` +
``routers/__init__.py``; this file only adds the handler.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from backend.ontology.models import (
    OntologyGraph,
    OntologyGraphEdge,
    OntologyGraphLevel,
    OntologyGraphNode,
)
from backend.ontology.services import mirror, ont_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ontology")


def _level(blob_level: dict) -> OntologyGraphLevel:
    """Project one layout-blob level ({nodes, edges, truncated}) into the typed model.

    Pydantic ignores extra blob keys and fills missing optionals, so asset nodes
    (no ``member_count``) and domain rollup nodes both map cleanly."""
    return OntologyGraphLevel(
        nodes=[OntologyGraphNode(**n) for n in blob_level.get("nodes", [])],
        edges=[OntologyGraphEdge(**e) for e in blob_level.get("edges", [])],
        truncated=bool(blob_level.get("truncated", False)),
    )


@router.get("/graph", response_model=OntologyGraph)
async def get_ontology_graph() -> OntologyGraph:
    """Return the estate graph for the current metastore, served from the mirror.

    Read-only and off the request-path compute (MV-D48). A missing/failed/empty
    snapshot degrades to a typed empty graph with ``state="cold"`` (MV-D43)."""
    try:
        metastore_id = ont_settings._metastore_id()
        snap = await mirror.read_graph_snapshot(metastore_id) if metastore_id else None
    except Exception:  # noqa: BLE001 — degrade to a cold graph, never 500
        logger.info("ontology graph read failed; serving cold graph", exc_info=True)
        snap = None

    if not snap or not snap.get("graph"):
        return OntologyGraph(state="cold")

    blob = snap["graph"]
    return OntologyGraph(
        domains=_level(blob.get("domains", {})),
        assets=_level(blob.get("assets", {})),
        layout=blob.get("layout", "none"),
        node_count=int(blob.get("node_count", 0)),
        edge_count=int(blob.get("edge_count", 0)),
        state="fresh",
        as_of=snap.get("as_of"),
    )
