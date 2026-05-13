"""Phase 1 Addendum — tests for the deterministic soft-evidence matcher."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, SoftEvidenceMatch
from genie_space_optimizer.optimization.rca_card_builder import (
    match_soft_evidence,
)


def _hard() -> dict:
    return {
        "gs_021": {
            "failure_type": "missing_filter",
            "blame_set": ["time_window", "time_window = mtd"],
            "counterfactual_fix": "Add filter WHERE f.time_window = mtd",
            "wrong_clause": "WHERE",
        },
    }


def test_matcher_pairs_via_matching_counterfactual_on_time_window() -> None:
    """ccf1d60d H002 ↔ S001 worked example: hard cluster's
    counterfactual references time_window; soft cluster's
    counterfactual mentions the same column."""
    hard_asi = _hard()
    soft_clusters = [
        {
            "cluster_id": "S001",
            "dominant_root_cause": RcaKind.FILTER_LOGIC_MISMATCH,
            "asi_by_qid": {
                "gs_001": {
                    "failure_type": "missing_filter",
                    "blame_set": [],
                    "counterfactual_fix": "Add filter on time_window",
                    "wrong_clause": "WHERE",
                },
                "gs_002": {
                    "failure_type": "missing_filter",
                    "counterfactual_fix": "Add filter on time_window",
                    "wrong_clause": "WHERE",
                },
            },
        },
    ]
    matches = match_soft_evidence(
        hard_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        hard_asi_by_qid=hard_asi,
        soft_clusters=soft_clusters,
    )
    # Both soft qids match because counterfactual references shared
    # column ``time_window``.
    assert len(matches) == 2
    assert {m.soft_qid for m in matches} == {"gs_001", "gs_002"}
    assert all(m.match_kind == "matching_counterfactual" for m in matches)
    assert all(m.evidence_token == "time_window" for m in matches)


def test_matcher_pairs_via_shared_blame_term() -> None:
    hard_asi = _hard()
    soft_clusters = [
        {
            "cluster_id": "S002",
            "dominant_root_cause": RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
            "asi_by_qid": {
                "gs_007": {
                    "failure_type": "time_window_logic_mismatch",
                    "blame_set": ["time_window"],
                    "counterfactual_fix": "Adjust period",
                    "wrong_clause": "WHERE",
                },
            },
        },
    ]
    matches = match_soft_evidence(
        hard_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        hard_asi_by_qid=hard_asi,
        soft_clusters=soft_clusters,
    )
    assert len(matches) == 1
    assert matches[0].match_kind == "shared_blame"
    assert matches[0].evidence_token == "time_window"


def test_matcher_skips_when_root_family_mismatched() -> None:
    """Soft cluster in a different root family is skipped even if it
    shares a blame token. Family discipline prevents over-matching."""
    hard_asi = _hard()  # filter_*
    soft_clusters = [
        {
            "cluster_id": "S010",
            "dominant_root_cause": RcaKind.TOP_N_CARDINALITY_COLLAPSE,  # top_n_*
            "asi_by_qid": {
                "gs_500": {
                    "failure_type": "plural_top_n_collapse",
                    "blame_set": ["time_window"],  # would match if family OK
                },
            },
        },
    ]
    matches = match_soft_evidence(
        hard_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        hard_asi_by_qid=hard_asi,
        soft_clusters=soft_clusters,
    )
    assert matches == ()


def test_matcher_returns_deterministic_sort_order() -> None:
    """Same input → same output ordering. Required for replay
    byte-stability on the supporting_soft_evidence tuple."""
    hard_asi = _hard()
    soft_clusters = [
        {
            "cluster_id": "S002",
            "dominant_root_cause": RcaKind.FILTER_LOGIC_MISMATCH,
            "asi_by_qid": {
                "gs_b": {"failure_type": "missing_filter", "blame_set": ["time_window"]},
            },
        },
        {
            "cluster_id": "S001",
            "dominant_root_cause": RcaKind.FILTER_LOGIC_MISMATCH,
            "asi_by_qid": {
                "gs_a": {"failure_type": "missing_filter", "blame_set": ["time_window"]},
            },
        },
    ]
    matches = match_soft_evidence(
        hard_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        hard_asi_by_qid=hard_asi,
        soft_clusters=soft_clusters,
    )
    # Sort key: (soft_cluster_id, soft_qid). S001 comes before S002.
    assert tuple((m.soft_cluster_id, m.soft_qid) for m in matches) == (
        ("S001", "gs_a"),
        ("S002", "gs_b"),
    )


def test_matcher_returns_empty_when_no_soft_clusters() -> None:
    matches = match_soft_evidence(
        hard_root_cause=RcaKind.FILTER_LOGIC_MISMATCH,
        hard_asi_by_qid=_hard(),
        soft_clusters=[],
    )
    assert matches == ()
