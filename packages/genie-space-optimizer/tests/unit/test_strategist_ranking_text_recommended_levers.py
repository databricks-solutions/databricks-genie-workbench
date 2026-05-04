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


def test_ranking_text_surfaces_recommended_levers_per_cluster():
    """The strategist's ranking_text must include each cluster's
    recommended_levers so the LLM sees the per-cluster lever hint.
    """
    from genie_space_optimizer.optimization.optimizer import (
        format_strategist_ranking_text,
    )

    clusters = [
        {
            "cluster_id": "H003",
            "rank": 1,
            "root_cause": "plural_top_n_collapse",
            "affected_judge": "schema_accuracy",
            "question_ids": ["gs_009"],
            "impact_score": 1.7,
            "recommended_levers": [3, 5],
        },
        {
            "cluster_id": "H_MULTI",
            "rank": 2,
            "root_cause": "plural_top_n_collapse",
            "affected_judge": "schema_accuracy",
            "question_ids": ["gs_001", "gs_002"],
            "impact_score": 1.5,
            "recommended_levers": [3, 5, 6],
        },
    ]
    text = format_strategist_ranking_text(clusters)
    # Each cluster's ranking line surfaces its recommended_levers.
    assert "recommended_levers=[3, 5]" in text
    assert "recommended_levers=[3, 5, 6]" in text
    # Cluster identity stays present.
    assert "H003" in text
    assert "H_MULTI" in text


def test_ranking_text_omits_levers_when_field_absent():
    """Backwards-compatible: clusters without ``recommended_levers``
    render as before (no levers field appended)."""
    from genie_space_optimizer.optimization.optimizer import (
        format_strategist_ranking_text,
    )

    clusters = [{
        "cluster_id": "H001",
        "rank": 1,
        "root_cause": "missing_filter",
        "affected_judge": "logical_accuracy",
        "question_ids": ["gs_005"],
        "impact_score": 1.0,
    }]
    text = format_strategist_ranking_text(clusters)
    assert "H001" in text
    assert "recommended_levers" not in text


def test_ranking_text_empty_clusters_yields_placeholder():
    """No clusters → '(no clusters)' placeholder, matching the
    pre-extraction inline behaviour."""
    from genie_space_optimizer.optimization.optimizer import (
        format_strategist_ranking_text,
    )

    assert format_strategist_ranking_text([]) == "(no clusters)"


def test_ranking_text_pre_existing_format_preserved():
    """The pre-extraction format ('Rank N: [cluster_id] root_cause
    (judge=..., questions=N, impact=X.X)') is the byte-stable contract
    that downstream consumers (operator transcript, replay) depend on.
    """
    from genie_space_optimizer.optimization.optimizer import (
        format_strategist_ranking_text,
    )

    clusters = [{
        "cluster_id": "H001",
        "rank": 1,
        "root_cause": "missing_filter",
        "affected_judge": "logical_accuracy",
        "question_ids": ["gs_005", "gs_006"],
        "impact_score": 1.7,
    }]
    text = format_strategist_ranking_text(clusters)
    assert (
        "Rank 1: [H001] missing_filter "
        "(judge=logical_accuracy, questions=2, impact=1.7)"
    ) in text
