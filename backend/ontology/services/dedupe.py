"""Deterministic tag dedupe + cleanup (Phase-1 route wrapper over shared transforms).

The pure collision/cleanup logic now lives in the GSO wheel
(``genie_space_optimizer.ontology.transforms``) so the Phase-2 batch job and the
Phase-1 route produce byte-identical verdicts (the parity guarantee). This module
keeps the Phase-1 public surface — ``find_collisions`` / ``find_cleanup`` returning
Pydantic models — so the route contract and the Phase-1 tests are unchanged.

Exact + fuzzy (case / plural / token) only; the MV-D40 Lakebase Search similarity
(embeddings + BM25) is a later phase and is NOT used here.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.ontology import transforms

from backend.ontology.models import TagCleanup, TagCollision


def find_collisions(graph: dict[str, Any]) -> list[TagCollision]:
    """Group near-duplicate tag keys; one collision per group of ≥2 keys."""
    return [TagCollision(**c) for c in transforms.find_collisions_dict(graph)]


def find_cleanup(graph: dict[str, Any]) -> list[TagCleanup]:
    """Flag orphan / near-empty / deprecated-but-assigned governed tags."""
    return [TagCleanup(**c) for c in transforms.find_cleanup_dict(graph)]


def verdicts_from_graph(graph: dict[str, Any]) -> tuple[list[TagCollision], list[TagCleanup]] | None:
    """Assemble collisions + cleanup from the mirror's embedding-backed per-tag
    ``dedupe_verdicts`` (Phase 3a), or ``None`` when the graph carries none.

    Same frozen ``TagCollision`` / ``TagCleanup`` shape as the string path — the
    content is richer (embedding-adjudicated) but the contract is unchanged. Used
    by the tags route on the mirror path; the live path keeps the string transforms.
    """
    tags = graph.get("tags", [])
    if not any(t.get("dedupe_verdicts") for t in tags):
        return None
    seen_collisions: set[tuple[str, ...]] = set()
    collisions: list[TagCollision] = []
    seen_cleanup: set[tuple[str, str]] = set()
    cleanup: list[TagCleanup] = []
    for t in tags:
        verdicts = t.get("dedupe_verdicts") or {}
        for c in verdicts.get("collisions", []) or []:
            key = tuple(sorted(c.get("members", []) or []))
            if len(key) < 2 or key in seen_collisions:
                continue
            seen_collisions.add(key)
            collisions.append(TagCollision(**c))
        for cl in verdicts.get("cleanup", []) or []:
            key2 = (cl.get("tag_key", ""), cl.get("flag", ""))
            if key2 in seen_cleanup:
                continue
            seen_cleanup.add(key2)
            cleanup.append(TagCleanup(**cl))
    collisions.sort(key=lambda c: tuple(c.members))
    cleanup.sort(key=lambda c: (c.tag_key, c.flag))
    return collisions, cleanup
