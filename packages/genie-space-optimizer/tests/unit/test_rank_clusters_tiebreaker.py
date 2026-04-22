"""Regression tests for the IQ Scan tiebreaker in ``rank_clusters``.

Guards:

- Clear impact-score winners (> 1.0 delta) are never reordered, regardless of
  scan recommendations.
- Clusters within the tiebreak threshold are reordered when the lower-impact
  cluster matches a scan-recommended lever.
- When the scan recommends the higher-impact cluster's lever, ordering is
  unchanged (no-op tiebreak).
- Without ``recommended_levers`` (current production behavior), sorting is
  strictly by ``impact_score`` and no ``_scan_lever_overlap`` field is added.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    _RANK_TIEBREAK_THRESHOLD,
    rank_clusters,
)


def _cluster(cluster_id: str, root_cause: str, question_ids: list[str] | None = None) -> dict:
    """Minimal cluster stub. ``cluster_impact`` uses q_count × weights → we
    control the impact by choosing question_ids length and root_cause.
    """
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "asi_failure_type": root_cause,
        "affected_judge": "schema_accuracy",
        "question_ids": question_ids or ["q1"],
        "asi_blame_set": [],
        "asi_counterfactual_fixes": [],
    }


# ---------------------------------------------------------------------------
# Baseline: no scan input
# ---------------------------------------------------------------------------

class TestNoScanInput:
    def test_sorts_strictly_by_impact_desc(self):
        ranked = rank_clusters([
            _cluster("a", "wrong_column", ["q1"]),
            _cluster("b", "wrong_column", ["q1", "q2", "q3"]),
            _cluster("c", "wrong_column", ["q1", "q2"]),
        ])
        assert [c["cluster_id"] for c in ranked] == ["b", "c", "a"]

    def test_does_not_add_overlap_field_without_levers(self):
        ranked = rank_clusters([_cluster("a", "wrong_column")])
        assert "_scan_lever_overlap" not in ranked[0]


# ---------------------------------------------------------------------------
# Clear winners are never overridden
# ---------------------------------------------------------------------------

class TestClearWinnerNoOverride:
    def test_large_impact_gap_ignores_scan(self):
        # Cluster A: 5 questions × wrong_column → lever 1
        # Cluster B: 1 question × wrong_column → lever 1
        # Impact delta > threshold; ordering must stay A,B regardless of scan.
        clusters = [
            _cluster("A", "wrong_column", ["q1", "q2", "q3", "q4", "q5"]),
            _cluster("B", "wrong_column", ["q1"]),
        ]
        ranked = rank_clusters(clusters, recommended_levers={4})
        assert [c["cluster_id"] for c in ranked] == ["A", "B"]
        assert ranked[0]["impact_score"] - ranked[1]["impact_score"] > _RANK_TIEBREAK_THRESHOLD


# ---------------------------------------------------------------------------
# Tie-breaker activates within threshold
# ---------------------------------------------------------------------------

class TestTiebreakerWithinThreshold:
    def test_lower_impact_with_matching_lever_wins(self):
        # Both clusters have same question count → identical impact.
        # A maps to lever 1 (wrong_column), B maps to lever 4 (missing_join_spec).
        # Scan recommends lever 4 → B must come first after tiebreak.
        a = _cluster("A", "wrong_column", ["q1", "q2"])
        b = _cluster("B", "missing_join_spec", ["q1", "q2"])
        ranked = rank_clusters([a, b], recommended_levers={4})
        assert ranked[0]["cluster_id"] == "B"
        assert ranked[1]["cluster_id"] == "A"
        assert ranked[0]["_scan_lever_overlap"] == 1.0
        assert ranked[1]["_scan_lever_overlap"] == 0.0

    def test_higher_impact_already_matches_no_change(self):
        # A impact > B impact and A's lever is recommended → no swap.
        a = _cluster("A", "wrong_column", ["q1", "q2", "q3"])
        b = _cluster("B", "missing_join_spec", ["q1", "q2"])
        ranked = rank_clusters([a, b], recommended_levers={1})
        assert ranked[0]["cluster_id"] == "A"
        assert ranked[1]["cluster_id"] == "B"

    def test_neither_matches_keeps_impact_order(self):
        # Neither lever recommended → pure impact order.
        a = _cluster("A", "wrong_column", ["q1"])
        b = _cluster("B", "missing_join_spec", ["q1"])
        ranked = rank_clusters([a, b], recommended_levers={6})
        assert ranked[0]["cluster_id"] == "A"
        assert ranked[1]["cluster_id"] == "B"
