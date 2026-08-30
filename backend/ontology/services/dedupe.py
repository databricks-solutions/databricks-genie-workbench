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
