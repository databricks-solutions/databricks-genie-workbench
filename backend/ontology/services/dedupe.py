"""Deterministic tag dedupe + cleanup (no embeddings).

Collisions are found by normalized-key equality after case-fold, singular/plural
fold, and token-set compare. Cleanup flags come from assignment counts and a
deprecation marker. This is the Phase-1 reuse-vs-create lens (MV-D37); the
MV-D40 Lakebase Search similarity (embeddings + BM25) is explicitly a later phase
and is NOT used here.

All functions are pure over the tag-graph structure
(:func:`backend.ontology.services.tag_graph.build_graph`) so they are unit-tested
off fixtures with no live workspace.
"""

from __future__ import annotations

import re
from typing import Any

from backend.ontology.models import TagCleanup, TagCollision

# A tag with fewer than this many in-scope assignments (but > 0) is "near-empty".
_NEAR_EMPTY_FLOOR = 2
# Deprecation markers (no authoritative deprecation column exists in Phase 1, so
# this is a conservative naming/allowed-value heuristic — documented, not silent).
_DEPRECATED_VALUES = {"deprecated", "retired", "legacy"}
_DEPRECATED_KEY_RE = re.compile(r"(^|[_/-])(deprecated|legacy|retired)$", re.IGNORECASE)


def _tokens(tag_key: str) -> list[str]:
    """Split a tag key into lowercased, singularized tokens."""
    raw = [t for t in re.split(r"[^0-9a-zA-Z]+", tag_key) if t]
    return [_singularize(t.casefold()) for t in raw]


def _singularize(token: str) -> str:
    if len(token) > 3 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def _token_set_sig(tag_key: str) -> str:
    return "|".join(sorted(_tokens(tag_key)))


def _collision_kind(members: list[str]) -> str:
    """Classify a colliding group by the weakest fold needed to unify it."""
    lowered = {m.casefold() for m in members}
    if len(lowered) == 1:
        # Same up to case only.
        return "exact" if len(set(members)) == 1 else "fuzzy_case"
    # Compare on the joined singularized whole-key (no token reorder).
    singular_whole = {" ".join(_tokens(m)) for m in members}
    if len(singular_whole) == 1:
        return "fuzzy_plural"
    return "fuzzy_token"


def find_collisions(graph: dict[str, Any]) -> list[TagCollision]:
    """Group near-duplicate tag keys; one collision per group of ≥2 keys."""
    counts = {t["tag_key"]: int(t.get("assignment_count") or 0) for t in graph.get("tags", [])}
    groups: dict[str, list[str]] = {}
    for key in counts:
        groups.setdefault(_token_set_sig(key), []).append(key)

    collisions: list[TagCollision] = []
    for _, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        members = sorted(members)
        # Canonical = most-assigned, then shortest, then alphabetical.
        canonical = sorted(members, key=lambda k: (-counts[k], len(k), k))[0]
        others = [m for m in members if m != canonical]
        suggestion = (
            f"reuse `{canonical}` instead of creating "
            + ", ".join(f"`{o}`" for o in others)
        )
        collisions.append(TagCollision(
            kind=_collision_kind(members),  # type: ignore[arg-type]
            members=members,
            suggestion=suggestion,
        ))
    return collisions


def _is_deprecated(tag: dict[str, Any]) -> bool:
    if _DEPRECATED_KEY_RE.search(tag.get("tag_key", "")):
        return True
    return any(str(v).casefold() in _DEPRECATED_VALUES for v in tag.get("allowed_values", []))


def find_cleanup(graph: dict[str, Any]) -> list[TagCleanup]:
    """Flag orphan (0 assigns), near-empty (below the floor), and
    deprecated-but-assigned governed tags."""
    out: list[TagCleanup] = []
    for t in sorted(graph.get("tags", []), key=lambda x: x["tag_key"]):
        key = t["tag_key"]
        count = int(t.get("assignment_count") or 0)
        if _is_deprecated(t) and count > 0:
            out.append(TagCleanup(
                tag_key=key,
                flag="deprecated_but_assigned",
                detail=f"deprecated governed tag still assigned to {count} asset(s)",
            ))
            continue
        if count == 0:
            out.append(TagCleanup(
                tag_key=key,
                flag="orphan",
                detail="governed tag with no in-scope assignments",
            ))
        elif count < _NEAR_EMPTY_FLOOR:
            out.append(TagCleanup(
                tag_key=key,
                flag="near_empty",
                detail=f"only {count} in-scope assignment(s)",
            ))
    return out
