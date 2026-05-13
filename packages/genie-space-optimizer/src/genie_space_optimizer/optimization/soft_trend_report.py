"""Phase 3 Action 3.3.3 — end-of-run operator trend report.

Aggregates soft clusters whose evidence did NOT match any hard
cluster across the run. Emitted as a ``SOFT_SIGNAL_TREND_REPORT``
decision record AND rendered into the operator transcript.

Soft signals NEVER enter the strategist's input as targets. This
report is for operator triage — if the operator wants to act on a
recurring unmatched soft signal, that's a benchmark or judge-tuning
conversation, not an optimizer iteration.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Iterable

from genie_space_optimizer.optimization.rca import RcaKind


@dataclass(frozen=True)
class UnmatchedSoftCluster:
    """Phase 3 Action 3.3.3 — one soft cluster with no hard-cluster match."""

    cluster_id: str
    dominant_root_cause: RcaKind
    qid_count: int
    top_blame_terms: tuple[str, ...]
    representative_counterfactual: str


@dataclass(frozen=True)
class SoftSignalTrendReport:
    """Phase 3 Action 3.3.3 — end-of-run aggregate report."""

    unmatched_clusters: tuple[UnmatchedSoftCluster, ...]
    total_soft_clusters: int
    matched_count: int
    unmatched_count: int
    count_by_root_cause: tuple[tuple[str, int], ...]


def _top_blame_terms(asi_by_qid: dict, k: int = 5) -> tuple[str, ...]:
    counter: Counter[str] = Counter()
    for meta in (asi_by_qid or {}).values():
        if not isinstance(meta, dict):
            continue
        for entry in meta.get("blame_set") or ():
            if isinstance(entry, str) and entry:
                counter[entry.strip()] += 1
    sorted_terms = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return tuple(term for term, _count in sorted_terms[:k])


def _representative_counterfactual(asi_by_qid: dict) -> str:
    for qid in sorted((asi_by_qid or {}).keys()):
        meta = (asi_by_qid or {}).get(qid)
        if not isinstance(meta, dict):
            continue
        cf = str(meta.get("counterfactual_fix") or "").strip()
        if cf:
            return cf
    return ""


def _root_cause_to_kind(value) -> RcaKind:
    if isinstance(value, RcaKind):
        return value
    if isinstance(value, str):
        for kind in RcaKind:
            if kind.value == value:
                return kind
    return RcaKind.UNKNOWN


def build_soft_signal_trend_report(
    *,
    soft_clusters_seen: Iterable[dict],
    matched_soft_qids_across_run: set[str],
) -> SoftSignalTrendReport:
    """Phase 3 Action 3.3.3 — pure builder.

    A soft cluster is **matched** iff ANY of its qids appears in
    ``matched_soft_qids_across_run``. A cluster is **unmatched** iff
    none of its qids appear there.
    """
    soft_list = list(soft_clusters_seen or ())
    unmatched: list[UnmatchedSoftCluster] = []
    matched_count = 0
    for cluster in soft_list:
        if not isinstance(cluster, dict):
            continue
        asi_by_qid = cluster.get("asi_by_qid") or {}
        cluster_qids = set(asi_by_qid.keys())
        if cluster_qids & matched_soft_qids_across_run:
            matched_count += 1
            continue
        unmatched.append(UnmatchedSoftCluster(
            cluster_id=str(cluster.get("cluster_id") or ""),
            dominant_root_cause=_root_cause_to_kind(
                cluster.get("dominant_root_cause")
            ),
            qid_count=len(cluster_qids),
            top_blame_terms=_top_blame_terms(asi_by_qid),
            representative_counterfactual=_representative_counterfactual(
                asi_by_qid
            ),
        ))
    unmatched.sort(key=lambda u: u.cluster_id)

    counter: Counter[str] = Counter(
        u.dominant_root_cause.value for u in unmatched
    )
    count_by_root_cause = tuple(
        sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    )
    return SoftSignalTrendReport(
        unmatched_clusters=tuple(unmatched),
        total_soft_clusters=len(soft_list),
        matched_count=matched_count,
        unmatched_count=len(unmatched),
        count_by_root_cause=count_by_root_cause,
    )
