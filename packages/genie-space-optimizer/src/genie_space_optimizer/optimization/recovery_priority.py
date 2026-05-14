"""Phase 1.5 — strategist recovery-pivot priority.

After a rollback with out_of_target_regressed_qids non-empty, the
next strategist call must present an AG selecting the regressed
cluster as the primary target before retrying the original target.
"""
from __future__ import annotations

from typing import Mapping, Sequence


def build_recovery_priority_list(
    *,
    regressed_qids_to_cluster_id: Mapping[str, str],
    uncovered_cluster_ids: Sequence[str],
    original_target_cluster_id: str,
) -> tuple[str, ...]:
    """Return an ordered tuple of cluster ids the strategist should
    consider, in priority order.

    Priority:
      1. Clusters that received out-of-target regressions in the
         previous iteration (sorted by cluster_id ascending for
         determinism).
      2. Clusters with currently-uncovered hard failures.
      3. The original target cluster.

    Each cluster id appears at most once; the first-occurrence
    position wins.
    """
    seen: set[str] = set()
    out: list[str] = []

    regressed_clusters = sorted(set(regressed_qids_to_cluster_id.values()))
    for cid in regressed_clusters:
        if not cid:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        out.append(cid)

    for cid in (uncovered_cluster_ids or ()):
        if not cid:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        out.append(cid)

    if original_target_cluster_id:
        if original_target_cluster_id not in seen:
            out.append(original_target_cluster_id)
            seen.add(original_target_cluster_id)

    return tuple(out)
