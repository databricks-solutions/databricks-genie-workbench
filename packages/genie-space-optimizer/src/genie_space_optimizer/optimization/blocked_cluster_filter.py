"""Phase 2.1 — CLUSTER_BLOCKED_NO_RCA hard routing constraint.

Pure helper. Reads the iteration's decision_records list, extracts
cluster_ids that have an active CLUSTER_BLOCKED_NO_RCA record this
iteration, and removes them from the strategist's candidate cluster
pool.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence


def blocked_cluster_ids_from_records(
    records: Sequence[Mapping[str, Any]],
) -> frozenset[str]:
    """Return the set of cluster_ids that have an active
    CLUSTER_BLOCKED_NO_RCA record in ``records``.
    """
    return frozenset(
        str(r.get("cluster_id") or "")
        for r in (records or ())
        if str(r.get("record_type") or "") == "no_rca_ground"
        and r.get("cluster_id")
    )


def filter_clusters_blocked_no_rca(
    *,
    clusters: Sequence[Mapping[str, Any]],
    decision_records_this_iter: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Return ``clusters`` minus the entries whose ``cluster_id`` is
    in :func:`blocked_cluster_ids_from_records`.

    Order-preserving: the surviving clusters retain their input
    order.
    """
    blocked = blocked_cluster_ids_from_records(decision_records_this_iter)
    if not blocked:
        return list(clusters or ())
    return [
        c for c in (clusters or ())
        if str(c.get("cluster_id") or "") not in blocked
    ]
