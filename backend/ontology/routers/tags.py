"""Ontology tags lens (MV-D37).

`GET /api/ontology/tags` enumerates governed tags, reuse-vs-create collisions
(exact + fuzzy: case / plural / token — no embeddings), and cleanup flags
(orphan / near-empty / deprecated-but-assigned). Drives frame 17.0c.

Phase 2 reader swap (MV-D41/D43): use the materialized tag-graph mirror when it is
fresh; otherwise the Phase-1 live-SP path. Both feed the SAME downstream transform
pipeline, so the lens is identical whichever source served it. Response model
unchanged. Empty allowlist → empty lens.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from genie_space_optimizer.ontology import transforms

from backend.ontology.models import GovernedTag, TagLens
from backend.ontology.services import dedupe, mirror, ont_settings, refresh, tag_graph

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


@router.get("/tags")
async def get_tags() -> dict:
    settings = await ont_settings.get_settings()
    allowlist = settings.catalog_allowlist

    # Mirror-first: reconstruct the tag graph from the materialized rows when fresh.
    # The ontology grain is the metastore (MV-D49); the per-workspace app reads the
    # one metastore ontology.
    ms = ont_settings._metastore_id()
    graph = None
    if await refresh.mirror_is_fresh(ms):
        graph = await mirror.read_tag_graph(ms)

    # Fallback: Phase-1 live path, read as the ontology identity (OBO by default,
    # MV-D50; degrade-not-hang, never blocks on the job).
    if graph is None:
        ri = settings.read_identity
        sp_ok = await asyncio.to_thread(tag_graph.probe, "sp") if ri == "auto" else False
        graph = await asyncio.to_thread(tag_graph.build_graph, allowlist, ri, sp_probe_ok=sp_ok)
    graph = graph or {"tags": []}

    # One downstream pipeline (shared transforms) regardless of source → parity.
    tags = [GovernedTag(**r) for r in transforms.governed_tag_rows(graph)]

    # Phase 3a: prefer the mirror's embedding-backed dedupe verdicts when present
    # (richer collisions, same frozen TagLens shape); else the string transforms.
    enriched = dedupe.verdicts_from_graph(graph)
    if enriched is not None:
        collisions, cleanup = enriched
    else:
        collisions = dedupe.find_collisions(graph)
        cleanup = dedupe.find_cleanup(graph)

    lens = TagLens(
        tags=tags,
        collisions=collisions,
        cleanup=cleanup,
        as_of=graph.get("as_of") or datetime.now(timezone.utc).isoformat(),
    )
    return lens.model_dump(mode="json")
