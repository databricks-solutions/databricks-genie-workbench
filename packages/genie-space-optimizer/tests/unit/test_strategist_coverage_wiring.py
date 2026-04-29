"""Harness must add coverage AGs for patchable hard clusters the strategist drops."""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    diagnostic_action_group_for_cluster,
    uncovered_patchable_clusters,
)


def test_uncovered_patchable_clusters_returns_dropped_clusters() -> None:
    h001 = {"cluster_id": "H001", "question_ids": ["q026"]}
    h002 = {"cluster_id": "H002", "question_ids": ["q009"]}
    ags = [{"id": "AG1", "source_cluster_ids": ["H002"], "affected_questions": ["q009"]}]
    uncovered = uncovered_patchable_clusters([h001, h002], ags)
    assert [c["cluster_id"] for c in uncovered] == ["H001"]


def test_diagnostic_action_group_has_correct_shape() -> None:
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["q026"],
        "root_cause": "plural_top_n_collapse",
        "asi_counterfactual_fixes": ["Remove RANK() filter."],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert ag["source_cluster_ids"] == ["H001"]
    assert "q026" in ag["affected_questions"]
    assert ag["coverage_reason"] == "strategist_omitted_patchable_hard_cluster"
