"""Regression tests for the Phase D1 fix to ``_filter_tried_clusters``.

Previously the function only consulted the legacy 2-tuple keys
``(root_cause, blame)`` even though callers were writing lever-aware
3-tuples ``(root_cause, blame, frozenset(levers))``. D1 makes the
3-tuple path actually effective via a ``_feasible_lever_sets`` helper
that lists the levers the router could plausibly try for each root
cause. A cluster is suppressed only when every feasible lever set has
been recorded as tried.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _feasible_lever_sets,
    _filter_tried_clusters,
)


def _cluster(cluster_id: str, root_cause: str, blame=()) -> dict:
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "asi_failure_type": root_cause,
        "asi_blame_set": list(blame),
        "question_ids": ["q1"],
        "affected_judge": "result_correctness",
    }


def test_feasible_lever_sets_knows_sql_shape_causes() -> None:
    fs = _feasible_lever_sets("missing_filter")
    assert frozenset({6}) in fs
    assert frozenset({5}) in fs


def test_feasible_lever_sets_unknown_cause_returns_empty() -> None:
    assert _feasible_lever_sets("completely_novel_cause") == ()


def test_legacy_2tuple_suppresses_cluster_across_all_levers() -> None:
    c = _cluster("C1", "missing_filter")
    tried = {("missing_filter", "")}
    assert _filter_tried_clusters([c], tried) == []


def test_lever_aware_partial_suppression_keeps_cluster() -> None:
    """Only Lever 6 has been tried — Lever 5 remains feasible, so the
    cluster is still available for retry."""
    c = _cluster("C1", "missing_filter")
    tried = {("missing_filter", "", frozenset({6}))}
    out = _filter_tried_clusters([c], tried)
    assert out == [c]


def test_lever_aware_full_suppression_drops_cluster() -> None:
    """Both feasible lever sets (6 and 5) have been tried — the cluster
    is truly dead and should be filtered out."""
    c = _cluster("C1", "missing_filter")
    tried = {
        ("missing_filter", "", frozenset({6})),
        ("missing_filter", "", frozenset({5})),
    }
    assert _filter_tried_clusters([c], tried) == []


def test_unknown_root_cause_only_suppressed_by_legacy_key() -> None:
    """Unknown root causes have no feasible lever map, so the 3-tuple
    suppression path never fires — only the legacy 2-tuple can suppress."""
    c = _cluster("C1", "novel_cause")
    lever_only = {("novel_cause", "", frozenset({6}))}
    assert _filter_tried_clusters([c], lever_only) == [c]
    legacy_only = {("novel_cause", "")}
    assert _filter_tried_clusters([c], legacy_only) == []


def test_blame_normalisation_matches_reflection_storage() -> None:
    """Reflection entries store blame as a sorted tuple. The filter must
    build the same representation when comparing against lever_keys."""
    c = _cluster("C1", "missing_filter", blame=["zone_name", "market_description"])
    tried = {
        (
            "missing_filter",
            ("market_description", "zone_name"),  # same normalisation as C2
            frozenset({6}),
        ),
        (
            "missing_filter",
            ("market_description", "zone_name"),
            frozenset({5}),
        ),
    }
    assert _filter_tried_clusters([c], tried) == []
