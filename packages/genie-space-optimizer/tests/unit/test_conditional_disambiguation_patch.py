"""Pin the new add_conditional_disambiguation_instruction patch type."""

from __future__ import annotations

from genie_space_optimizer.optimization.intent_disambiguation import (
    build_conditional_disambiguation_patch,
)


def test_builds_patch_with_per_intent_branches() -> None:
    collision = {
        "term": "region",
        "column_choices": {"region_name", "region_combination"},
        "clusters_by_column": {
            "region_name": ["H001"],
            "region_combination": ["H002"],
        },
        "questions_by_column": {
            "region_name": ["gs_002", "gs_006"],
            "region_combination": ["gs_008"],
        },
    }
    representatives = {
        "gs_002": "Total sales by region last quarter",
        "gs_006": "Average ticket by region",
        "gs_008": "US sales flow across regions",
    }
    patch = build_conditional_disambiguation_patch(
        collision=collision,
        representatives=representatives,
        proposal_id="P_INTENT_001",
    )
    assert patch["type"] == "add_conditional_disambiguation_instruction"
    assert patch["term"] == "region"
    assert "region_name" in patch["mappings"]
    assert "region_combination" in patch["mappings"]
    assert patch["lever"] == 5
    body = patch["proposed_value"]
    assert "region_name" in body
    assert "region_combination" in body
    assert "sales flow" in body.lower() or "hierarchy" in body.lower()


def test_target_qids_cover_all_collision_questions() -> None:
    collision = {
        "term": "region",
        "column_choices": {"a", "b"},
        "clusters_by_column": {"a": ["H1"], "b": ["H2"]},
        "questions_by_column": {"a": ["q1"], "b": ["q2"]},
    }
    patch = build_conditional_disambiguation_patch(
        collision=collision,
        representatives={"q1": "Q1?", "q2": "Q2?"},
        proposal_id="P_INTENT_002",
    )
    assert sorted(patch["target_qids"]) == ["q1", "q2"]
