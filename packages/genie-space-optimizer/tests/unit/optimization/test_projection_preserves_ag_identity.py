"""``_project_pipeline_to_action_groups`` must populate the AG identity
fields (``source_cluster_ids``, ``primary_cluster_id``,
``affected_questions``) from the pipeline result. Trial-5 left these
empty, which prevented narrow-replacement and reflection paths from
routing proposals back to clusters."""

from __future__ import annotations


def _pipeline_result() -> dict:
    return {
        "ag_id": "AG_PIPELINE",
        "discovery_rationale": "missing aggregation on revenue",
        "stage_1_picks": [{
            "skill_id": "lever-2-mv-column-refinement",
            "target_objects": ["mv.fact.revenue"],
            "expected_impact_qids": ["Q42", "Q43"],
        }],
        "stage_2_results": [{
            "skill_id": "lever-2-mv-column-refinement",
            "ag_id": "AG_PIPELINE",
            "proposals": [{"patch_type": "add_column_description"}],
        }],
        "cluster_briefs": [
            {"cluster_id": "C1", "question_ids": ["Q42"]},
            {"cluster_id": "C2", "question_ids": ["Q43"]},
        ],
    }


def test_projection_populates_source_cluster_ids_from_cluster_briefs():
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _project_pipeline_to_action_groups,
    )
    ags = _project_pipeline_to_action_groups(_pipeline_result())
    assert len(ags) == 1
    assert ags[0]["source_cluster_ids"] == ["C1", "C2"]


def test_projection_populates_primary_cluster_id_to_first_brief():
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _project_pipeline_to_action_groups,
    )
    ags = _project_pipeline_to_action_groups(_pipeline_result())
    assert ags[0]["primary_cluster_id"] == "C1"


def test_projection_populates_affected_questions_from_expected_impact_qids():
    """Stage-1 picks carry expected_impact_qids; the projection must
    union them into the AG's affected_questions so the reflection
    buffer can correlate proposals to questions."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _project_pipeline_to_action_groups,
    )
    ags = _project_pipeline_to_action_groups(_pipeline_result())
    assert sorted(ags[0]["affected_questions"]) == ["Q42", "Q43"]


def test_projection_with_no_cluster_briefs_returns_empty_lists():
    """If the pipeline result is missing cluster_briefs (legacy or
    degraded path), the AG identity fields must still be present as
    empty lists — not absent."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _project_pipeline_to_action_groups,
    )
    result = _pipeline_result()
    result.pop("cluster_briefs")
    ags = _project_pipeline_to_action_groups(result)
    assert ags[0]["source_cluster_ids"] == []
    assert ags[0]["primary_cluster_id"] == ""
