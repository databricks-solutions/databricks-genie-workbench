"""Phase 3 Action 3.3.3 — SoftSignalTrendReport tests."""

from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.soft_trend_report import (
    SoftSignalTrendReport,
    UnmatchedSoftCluster,
    build_soft_signal_trend_report,
)


def _soft_cluster(
    *,
    cluster_id: str,
    root_cause: RcaKind,
    qids_with_blames: dict[str, list[str]],
    counterfactuals: dict[str, str] | None = None,
) -> dict:
    return {
        "cluster_id": cluster_id,
        "dominant_root_cause": root_cause,
        "asi_by_qid": {
            qid: {
                "blame_set": blames,
                "counterfactual_fix": (counterfactuals or {}).get(qid, ""),
            }
            for qid, blames in qids_with_blames.items()
        },
    }


def test_unmatched_soft_cluster_dataclass_shape() -> None:
    u = UnmatchedSoftCluster(
        cluster_id="S007",
        dominant_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        qid_count=4,
        top_blame_terms=("region",),
        representative_counterfactual="Add filter on region",
    )
    assert u.cluster_id == "S007"
    assert u.qid_count == 4


def test_unmatched_soft_cluster_is_frozen() -> None:
    u = UnmatchedSoftCluster(
        cluster_id="S007", dominant_root_cause=RcaKind.UNKNOWN, qid_count=0,
        top_blame_terms=(), representative_counterfactual="",
    )
    try:
        u.qid_count = 1  # type: ignore[misc]
        raised = False
    except dataclasses.FrozenInstanceError:
        raised = True
    assert raised


def test_soft_signal_trend_report_dataclass_shape() -> None:
    rep = SoftSignalTrendReport(
        unmatched_clusters=(),
        total_soft_clusters=5,
        matched_count=3,
        unmatched_count=2,
        count_by_root_cause=(),
    )
    assert rep.total_soft_clusters == 5
    assert rep.matched_count + rep.unmatched_count == rep.total_soft_clusters


def test_builder_marks_cluster_as_matched_when_appears_in_any_card() -> None:
    """ccf1d60d S001 (time_window) IS matched against H002 → it is
    excluded from unmatched_clusters; matched_count = 1."""
    soft_clusters = [
        _soft_cluster(
            cluster_id="S001",
            root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
            qids_with_blames={"gs_001": [], "gs_002": []},
            counterfactuals={"gs_001": "Add filter on time_window"},
        ),
    ]
    matched_soft_qids = {"gs_001", "gs_002"}
    report = build_soft_signal_trend_report(
        soft_clusters_seen=soft_clusters,
        matched_soft_qids_across_run=matched_soft_qids,
    )
    assert report.total_soft_clusters == 1
    assert report.matched_count == 1
    assert report.unmatched_count == 0
    assert report.unmatched_clusters == ()


def test_builder_includes_unmatched_clusters_with_top_blame_terms() -> None:
    soft = _soft_cluster(
        cluster_id="S007",
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        qids_with_blames={
            "gs_100": ["region", "north"],
            "gs_101": ["region", "south"],
            "gs_102": ["region", "category"],
        },
        counterfactuals={"gs_100": "Filter region = 'us'"},
    )
    report = build_soft_signal_trend_report(
        soft_clusters_seen=[soft], matched_soft_qids_across_run=set(),
    )
    assert report.unmatched_count == 1
    u = report.unmatched_clusters[0]
    assert u.cluster_id == "S007"
    assert u.dominant_root_cause == RcaKind.TIME_WINDOW_LOGIC_MISMATCH
    assert u.qid_count == 3
    assert u.top_blame_terms[0] == "region"
    assert u.representative_counterfactual == "Filter region = 'us'"


def test_builder_count_by_root_cause_sorted_descending() -> None:
    soft_a = _soft_cluster(
        cluster_id="S100", root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        qids_with_blames={"gs_a": []},
    )
    soft_b = _soft_cluster(
        cluster_id="S200", root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        qids_with_blames={"gs_b": []},
    )
    soft_c = _soft_cluster(
        cluster_id="S300", root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        qids_with_blames={"gs_c": []},
    )
    report = build_soft_signal_trend_report(
        soft_clusters_seen=[soft_a, soft_b, soft_c],
        matched_soft_qids_across_run=set(),
    )
    assert report.count_by_root_cause[0] == ("filter_logic_mismatch", 2)
    assert report.count_by_root_cause[1] == ("time_window_logic_mismatch", 1)


def test_builder_handles_no_soft_clusters() -> None:
    report = build_soft_signal_trend_report(
        soft_clusters_seen=[], matched_soft_qids_across_run=set(),
    )
    assert report.total_soft_clusters == 0
    assert report.unmatched_clusters == ()
