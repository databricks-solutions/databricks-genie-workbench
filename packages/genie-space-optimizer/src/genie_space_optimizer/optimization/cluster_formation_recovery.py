"""Cycle 5 T5 — soft-cluster drift recovery.

When the soft-clusterer reads stale ASI / cached eval rows and emits
qids that the current eval no longer flags as judge-failing,
``assert_soft_cluster_currency`` (control_plane.py) raises and the
entire run aborts. Run ``2423b960-16e8-41d4-a0cb-74c563378e05`` had
two early task attempts crash on this assertion; the harness lost
the whole run.

T5 turns the assertion into a recovery path: drop the drifted qids
from each soft cluster (or drop the cluster entirely if every qid
drifted) and emit a typed ``SOFT_CLUSTER_DRIFT_RECOVERED`` decision
record so the operator transcript surfaces the recovery. The
existing assertion is preserved as the strict-mode default; recovery
is opt-in via behavior, not a flag (this is a survival fix, not a
feature).

Pure module — no harness coupling, no MLflow side effects, no
flag. Caller decides when to invoke.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class SoftClusterDriftRecovery:
    """Result of ``recover_from_soft_cluster_drift``.

    ``recovered_clusters`` is the cleaned soft-cluster list with
    drifted qids removed and any wholly-drifted clusters dropped
    entirely. ``drifted_qids_by_cluster`` maps cluster_id to the
    sorted tuple of qids that drifted out (for emit/audit). Empty
    when no drift was detected.
    """
    recovered_clusters: list[dict]
    drifted_qids_by_cluster: Mapping[str, tuple[str, ...]]
    dropped_cluster_ids: tuple[str, ...]


def _base_qid(q: str) -> str:
    """Strip a ``:vN`` benchmark-suffix variant for cross-eval
    matching. Mirrors ``control_plane._base_qid``."""
    s = str(q or "")
    return s.split(":", 1)[0] if ":" in s else s


def recover_from_soft_cluster_drift(
    *,
    soft_clusters: Iterable[Mapping],
    judge_failing_qids: Iterable[str],
) -> SoftClusterDriftRecovery:
    """Drop drifted qids from each soft cluster.

    A qid in a soft cluster has "drifted" when its base form is not
    present in ``judge_failing_qids`` (the qids the current eval shows
    as having at least one ``has_individual_judge_failure``-flagged
    row). When a cluster's qid set is fully drifted, the cluster is
    dropped from the slate entirely.

    Pure: does not mutate the input.
    """
    failing_bases = {_base_qid(q) for q in judge_failing_qids if str(q)}
    recovered: list[dict] = []
    drifted_by_cluster: dict[str, tuple[str, ...]] = {}
    dropped_ids: list[str] = []
    for cluster in soft_clusters or ():
        if not isinstance(cluster, Mapping):
            continue
        cid = str(cluster.get("cluster_id") or cluster.get("id") or "")
        qids = list(cluster.get("question_ids") or cluster.get("qids") or [])
        kept = [q for q in qids if _base_qid(str(q)) in failing_bases]
        drifted = sorted(
            str(q) for q in qids
            if _base_qid(str(q)) not in failing_bases
        )
        if drifted:
            drifted_by_cluster[cid] = tuple(drifted)
        if not kept:
            # Whole cluster drifted — drop it.
            if cid:
                dropped_ids.append(cid)
            continue
        if drifted:
            new_cluster = dict(cluster)
            if "question_ids" in cluster:
                new_cluster["question_ids"] = list(kept)
            if "qids" in cluster:
                new_cluster["qids"] = list(kept)
            recovered.append(new_cluster)
        else:
            recovered.append(dict(cluster))
    return SoftClusterDriftRecovery(
        recovered_clusters=recovered,
        drifted_qids_by_cluster=drifted_by_cluster,
        dropped_cluster_ids=tuple(dropped_ids),
    )
