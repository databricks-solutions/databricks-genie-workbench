"""Ontology tags lens (SP, MV-D37).

`GET /api/ontology/tags` enumerates governed tags, reuse-vs-create collisions
(exact + fuzzy: case / plural / token — no embeddings), and cleanup flags
(orphan / near-empty / deprecated-but-assigned). Drives frame 17.0c. TTL-cached
via the tag-graph reader. Empty allowlist → empty lens.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.ontology.models import GovernedTag, TagLens
from backend.ontology.services import dedupe, ont_settings, tag_graph, taxonomy

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ontology")


@router.get("/tags")
async def get_tags() -> dict:
    settings = await ont_settings.get_settings()
    allowlist = settings.catalog_allowlist

    graph = await asyncio.to_thread(tag_graph.build_graph, allowlist)
    graph = graph or {"tags": []}

    all_keys = [t["tag_key"] for t in graph.get("tags", [])]
    tags = [
        GovernedTag(
            tag_key=t["tag_key"],
            allowed_values=list(t.get("allowed_values") or []),
            assignment_count=int(t.get("assignment_count") or 0),
            acts_as_domain=taxonomy.acts_as_domain(t["tag_key"], all_keys),
            acts_as_subdomain=taxonomy.acts_as_subdomain(t["tag_key"]),
        )
        for t in graph.get("tags", [])
    ]

    lens = TagLens(
        tags=tags,
        collisions=dedupe.find_collisions(graph),
        cleanup=dedupe.find_cleanup(graph),
        as_of=graph.get("as_of") or datetime.now(timezone.utc).isoformat(),
    )
    return lens.model_dump(mode="json")
