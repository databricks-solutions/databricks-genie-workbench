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
    OntologyGraphExpand,
    OntologyGraphLevel,
    OntologyGraphNode,
)
from backend.ontology.services import mirror, ont_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ontology")

# Applied-vs-Proposed provenance (MV-D74); default "applied" (governed current state).
_ORIGINS = ("applied", "proposed")
# Bounded expand (MV-D43): never fan a single node into an unbounded child set.
_EXPAND_CHILD_CAP = 200


def _level(blob_level: dict) -> OntologyGraphLevel:
    """Project one layout-blob level ({nodes, edges, truncated}) into the typed model.

    Pydantic ignores extra blob keys and fills missing optionals, so asset nodes
    (no ``member_count``) and domain rollup nodes both map cleanly."""
    return OntologyGraphLevel(
        nodes=[OntologyGraphNode(**n) for n in blob_level.get("nodes", [])],
        edges=[OntologyGraphEdge(**e) for e in blob_level.get("edges", [])],
        truncated=bool(blob_level.get("truncated", False)),
    )


def _filter_by_origin(blob: dict, origin: str) -> dict:
    """Return a shallow copy of the blob keeping only rollups whose ``origin`` matches
    (MV-D74). ``ungrouped`` is always kept (neutral). Assets are pruned to the kept
    rollups (by ``domain_id``), and any edge with a filtered endpoint is dropped
    (emitted-only guard — no dangling edges).

    Back-compat / degrade (MV-D43): a legacy blob whose rollups carry no ``origin`` key
    at all is served UNFILTERED — never blank the map for a pre-MV-D73 snapshot."""
    domains = blob.get("domains", {}) or {}
    domain_nodes = domains.get("nodes", []) or []
    if not any("origin" in n for n in domain_nodes):
        return blob  # pre-MV-D73 blob: nothing to filter on

    kept_domain_ids: set = set()
    kept_domains: list = []
    for n in domain_nodes:
        if n.get("origin") == origin or n.get("kind") == "ungrouped":
            kept_domains.append(n)
            if n.get("id") is not None:
                kept_domain_ids.add(n["id"])
    domain_edges = [
        e for e in (domains.get("edges", []) or [])
        if e.get("src") in kept_domain_ids and e.get("dst") in kept_domain_ids
    ]

    assets = blob.get("assets", {}) or {}
    kept_asset_ids: set = set()
    kept_assets: list = []
    for n in assets.get("nodes", []) or []:
        if n.get("domain_id") in kept_domain_ids:
            kept_assets.append(n)
            if n.get("id") is not None:
                kept_asset_ids.add(n["id"])
    asset_edges = [
        e for e in (assets.get("edges", []) or [])
        if e.get("src") in kept_asset_ids and e.get("dst") in kept_asset_ids
    ]

    return {
        **blob,
        "domains": {**domains, "nodes": kept_domains, "edges": domain_edges},
        "assets": {**assets, "nodes": kept_assets, "edges": asset_edges},
    }


def _node_domain(blob: dict, node_id: str) -> str | None:
    """The ``domain_id`` of one blob node (assets first, then domain rollups), or None
    — used to hang expanded measures under their metric-view's domain."""
    for level in ("assets", "domains"):
        for n in (blob.get(level, {}) or {}).get("nodes", []) or []:
            if n.get("id") == node_id:
                return n.get("domain_id")
    return None


@router.get("/graph", response_model=OntologyGraph)
async def get_ontology_graph(origin: str = "applied") -> OntologyGraph:
    """Return the estate graph for the current metastore, served from the mirror.

    ``origin=applied`` (default) shows governed-tag domains; ``origin=proposed`` shows
    the engine's proposed clusters (MV-D74). Read-only and off the request-path compute
    (MV-D48). A missing/failed/empty snapshot degrades to a typed empty graph with
    ``state="cold"`` (MV-D43)."""
    view = origin if origin in _ORIGINS else "applied"
    try:
        metastore_id = ont_settings._metastore_id()
        snap = await mirror.read_graph_snapshot(metastore_id) if metastore_id else None
    except Exception:  # noqa: BLE001 — degrade to a cold graph, never 500
        logger.info("ontology graph read failed; serving cold graph", exc_info=True)
        snap = None

    if not snap or not snap.get("graph"):
        return OntologyGraph(state="cold")

    blob = _filter_by_origin(snap["graph"], view)
    # Node ``attach_level`` and edge ``verb``/``rel_class`` (MV-D82) flow through _level
    # automatically (Pydantic reads the extra blob keys); the ``org`` root is passed
    # explicitly here — absent on a pre-MV-D82 / empty blob, so root stays None (MV-D43).
    root_blob = blob.get("root")
    return OntologyGraph(
        domains=_level(blob.get("domains", {})),
        assets=_level(blob.get("assets", {})),
        layout=blob.get("layout", "none"),
        node_count=int(blob.get("node_count", 0)),
        edge_count=int(blob.get("edge_count", 0)),
        state="fresh",
        as_of=snap.get("as_of"),
        root=OntologyGraphNode(**root_blob) if isinstance(root_blob, dict) else None,
    )


@router.get("/graph/expand", response_model=OntologyGraphExpand)
async def get_ontology_graph_expand(node: str, origin: str = "applied") -> OntologyGraphExpand:
    """Return ONE node's children — expand-on-demand (MV-D73 §2.3).

    Slices the snapshot blob's ``snippets[node]`` index (baked by the deterministic
    batch, no request-path warehouse): measures ⇒ ``kind="measure"`` nodes + ``mv_measure``
    edges; Pages ⇒ ``kind="page"`` nodes + ``page_source`` edges. Read-only, bounded
    (``_EXPAND_CHILD_CAP``); any miss/failure ⇒ empty children, never a 500 (MV-D43).
    ``origin`` is accepted for symmetry with ``/graph``; the snippet layer is
    provenance-neutral, so it does not change the slice."""
    _ = origin  # accepted per the frozen contract; snippets are origin-neutral
    try:
        metastore_id = ont_settings._metastore_id()
        snap = await mirror.read_graph_snapshot(metastore_id) if metastore_id else None
    except Exception:  # noqa: BLE001 — degrade to empty children, never 500
        logger.info("ontology graph expand read failed; empty children", exc_info=True)
        snap = None

    if not snap or not snap.get("graph"):
        return OntologyGraphExpand(nodes=[], edges=[], parent_id=node, as_of=None)

    blob = snap["graph"]
    as_of = snap.get("as_of")
    snippet = (blob.get("snippets") or {}).get(node)
    if not isinstance(snippet, dict):
        return OntologyGraphExpand(nodes=[], edges=[], parent_id=node, as_of=as_of)

    parent_domain = _node_domain(blob, node)
    nodes: list[OntologyGraphNode] = []
    edges: list[OntologyGraphEdge] = []

    for m in (snippet.get("measures") or [])[:_EXPAND_CHILD_CAP]:
        ref = str(m.get("ref") or "").strip()
        if not ref:
            continue
        mid = f"measure:{ref}"
        nodes.append(
            OntologyGraphNode(id=mid, label=str(m.get("name") or ref), kind="measure", domain_id=parent_domain)
        )
        edges.append(OntologyGraphEdge(src=node, dst=mid, kind="mv_measure"))

    for p in (snippet.get("pages") or [])[:_EXPAND_CHILD_CAP]:
        pid = str(p.get("page_id") or "").strip()
        if not pid:
            continue
        nid = f"page:{pid}"
        page_domain = str(p["domain_id"]) if p.get("domain_id") else None
        nodes.append(
            OntologyGraphNode(id=nid, label=str(p.get("title") or pid), kind="page", domain_id=page_domain)
        )
        edges.append(OntologyGraphEdge(src=node, dst=nid, kind="page_source"))

    return OntologyGraphExpand(nodes=nodes, edges=edges, parent_id=node, as_of=as_of)
