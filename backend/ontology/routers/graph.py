"""Ontology estate-graph route (Phase 3e / Ontology Map) — read-only.

Pre-seeded seam (empty on purpose). The ``router`` object exists and is already
registered in ``backend/main.py`` + ``routers/__init__.py`` so the Phase-3e lane
adds its mirror-only ``GET /api/ontology/graph`` handler to *this* file only,
without touching the shared registration block. Until 3e lands this router
exposes no routes.

The graph is served from the Lakebase mirror (``genie_ont_graph_snapshot``) with
a deterministic, pre-computed layout — degrade-not-hang: not-fresh / empty /
failed returns a typed empty graph, never a 500. It never recomputes the fused
signal graph on the request path.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/ontology")
