"""Tests for the per-cluster recommended_levers stamping helper and
the strategist ranking_text renderer that surfaces it.

Cycle 2 Task 4 closeout — the helper ``recommended_levers_for_cluster``
landed in commit c782dbc but no caller stamps the result on each
cluster, so the strategist prompt never sees the per-cluster lever
hint. These tests pin the contract for the stamping helper and the
ranking-text renderer that consumes it.
"""
from __future__ import annotations


def test_stamp_recommended_levers_on_clusters():
    from genie_space_optimizer.optimization.stages.action_groups import (
        stamp_recommended_levers_on_clusters,
    )

    clusters = [
        {
            "cluster_id": "H003",
            "question_ids": ["gs_009"],
            "q_count": 1,
            "root_cause": "plural_top_n_collapse",
            "impact_score": 1.7,
            "rank": 1,
        },
        {
            "cluster_id": "H_MULTI",
            "question_ids": ["gs_001", "gs_002", "gs_003"],
            "q_count": 3,
            "root_cause": "plural_top_n_collapse",
            "impact_score": 1.5,
            "rank": 2,
        },
    ]
    stamped = stamp_recommended_levers_on_clusters(clusters)
    # Single-question shape RCA → per-question levers (3, 5),
    # NOT lever 6.
    assert 6 not in stamped[0]["recommended_levers"]
    assert 3 in stamped[0]["recommended_levers"]
    # Multi-question still gets the default set including 6.
    assert 6 in stamped[1]["recommended_levers"]


def test_stamp_recommended_levers_preserves_other_fields():
    from genie_space_optimizer.optimization.stages.action_groups import (
        stamp_recommended_levers_on_clusters,
    )

    clusters = [{
        "cluster_id": "H003",
        "question_ids": ["gs_009"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
        "impact_score": 1.7,
        "asi_blame_set": ["gs_009"],
    }]
    stamped = stamp_recommended_levers_on_clusters(clusters)
    assert stamped[0]["asi_blame_set"] == ["gs_009"]
    assert stamped[0]["impact_score"] == 1.7


def test_stamp_recommended_levers_idempotent():
    from genie_space_optimizer.optimization.stages.action_groups import (
        stamp_recommended_levers_on_clusters,
    )

    clusters = [{
        "cluster_id": "H003",
        "question_ids": ["gs_009"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
    }]
    once = stamp_recommended_levers_on_clusters(clusters)
    twice = stamp_recommended_levers_on_clusters(once)
    assert once[0]["recommended_levers"] == twice[0]["recommended_levers"]


def test_stamp_recommended_levers_does_not_mutate_input():
    from genie_space_optimizer.optimization.stages.action_groups import (
        stamp_recommended_levers_on_clusters,
    )

    inp = [{
        "cluster_id": "H003",
        "question_ids": ["gs_009"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
    }]
    stamp_recommended_levers_on_clusters(inp)
    # Original dict must not gain the recommended_levers key.
    assert "recommended_levers" not in inp[0]
