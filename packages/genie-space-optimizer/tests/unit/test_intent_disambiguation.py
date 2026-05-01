"""Pin the cross-cluster column collision detector."""

from __future__ import annotations

from genie_space_optimizer.optimization.intent_disambiguation import (
    detect_intent_collisions,
)


def test_detects_collision_when_same_term_maps_to_different_columns() -> None:
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["gs_002"],
            "asi_blame_set": ["region_name"],
            "representative_question": "Sales by region",
            "asi_intent_terms": ["region"],
        },
        {
            "cluster_id": "H002",
            "question_ids": ["gs_008"],
            "asi_blame_set": ["region_combination"],
            "representative_question": "US sales flow by region",
            "asi_intent_terms": ["region"],
        },
    ]
    collisions = detect_intent_collisions(clusters)
    assert len(collisions) == 1
    assert collisions[0]["term"] == "region"
    assert "region_name" in collisions[0]["column_choices"]
    assert "region_combination" in collisions[0]["column_choices"]
    assert set(collisions[0]["clusters_by_column"]["region_name"]) == {"H001"}
    assert set(collisions[0]["clusters_by_column"]["region_combination"]) == {"H002"}


def test_no_collision_when_term_maps_consistently() -> None:
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["gs_002"],
            "asi_blame_set": ["region_name"],
            "asi_intent_terms": ["region"],
        },
        {
            "cluster_id": "H002",
            "question_ids": ["gs_006"],
            "asi_blame_set": ["region_name"],
            "asi_intent_terms": ["region"],
        },
    ]
    assert detect_intent_collisions(clusters) == []


def test_no_collision_when_clusters_have_no_overlapping_terms() -> None:
    clusters = [
        {"cluster_id": "H001", "asi_blame_set": ["region_name"], "asi_intent_terms": ["region"]},
        {"cluster_id": "H002", "asi_blame_set": ["sales_amount"], "asi_intent_terms": ["sales"]},
    ]
    assert detect_intent_collisions(clusters) == []
