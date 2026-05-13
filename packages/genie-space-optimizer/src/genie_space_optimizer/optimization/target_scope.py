"""Phase 3 Action 3.2 — TargetScope enum + deterministic deriver.

The deriver maps an AG payload + cluster registry to one of four
typed scopes. The scope, together with ``repair_archetype`` from the
stamped repair kit, forms the ``AGShapeSignature`` that the AG-shape
gate compares across iterations on the same target.

Pure function: same input → same output. No I/O, no globals.
"""

from __future__ import annotations

from enum import Enum
from typing import Iterable


class TargetScope(str, Enum):
    """Phase 3 Action 3.2 — four scope tiers an AG can occupy.

    Used together with ``repair_archetype`` to form an
    ``AGShapeSignature``. Two AGs are "shape-equal" iff their
    ``(repair_archetype, target_scope)`` pair matches.
    """

    SINGLE_QID = "single_qid"
    CLUSTER_SCOPED = "cluster_scoped"
    MULTI_CLUSTER_SCOPED = "multi_cluster_scoped"
    GLOBAL_SCOPED = "global_scoped"


_GLOBAL_LEVER_PREFIXES = (
    "L5_global_",
    "global_instruction",
    "global_rewrite",
)


def _is_global_lever(directive: dict) -> bool:
    lever = str(directive.get("lever") or "")
    return any(lever.startswith(p) for p in _GLOBAL_LEVER_PREFIXES)


def _ag_target_qids(ag: dict) -> list[str]:
    raw = ag.get("target_qids") or ag.get("affected_questions") or []
    if not isinstance(raw, list):
        return []
    return [str(q) for q in raw if q]


def derive_target_scope(ag: dict, clusters: Iterable[dict]) -> TargetScope:
    """Phase 3 Action 3.2 — deterministic scope deriver.

    Decision order (first match wins):
      1. ``GLOBAL_SCOPED`` — AG carries a ``global_instruction_rewrite``
         OR any ``lever_directives[i].lever`` matches the global prefix
         set.
      2. ``MULTI_CLUSTER_SCOPED`` — target qids span more than one
         cluster in the registry.
      3. ``CLUSTER_SCOPED`` — target qids exactly match one cluster's
         qid set.
      4. ``SINGLE_QID`` — anything else (including empty targets, which
         we treat as the smallest scope to prevent trivial "no-target"
         shape equality).
    """
    if ag.get("global_instruction_rewrite"):
        return TargetScope.GLOBAL_SCOPED
    for directive in ag.get("lever_directives") or ():
        if isinstance(directive, dict) and _is_global_lever(directive):
            return TargetScope.GLOBAL_SCOPED

    target_qids = set(_ag_target_qids(ag))
    if len(target_qids) <= 1:
        return TargetScope.SINGLE_QID

    cluster_membership: dict[str, set[str]] = {}
    for cluster in clusters or ():
        cid = str(
            cluster.get("primary_cluster_id")
            or cluster.get("cluster_id")
            or ""
        )
        if not cid:
            continue
        qids = set(
            str(q) for q in (
                cluster.get("target_qids")
                or cluster.get("affected_questions")
                or ()
            )
        )
        cluster_membership[cid] = qids

    matching_clusters = [
        cid for cid, qids in cluster_membership.items()
        if qids and qids == target_qids
    ]
    if matching_clusters:
        return TargetScope.CLUSTER_SCOPED

    spanning_clusters = [
        cid for cid, qids in cluster_membership.items()
        if qids & target_qids
    ]
    if len(spanning_clusters) >= 2:
        return TargetScope.MULTI_CLUSTER_SCOPED

    return TargetScope.SINGLE_QID
