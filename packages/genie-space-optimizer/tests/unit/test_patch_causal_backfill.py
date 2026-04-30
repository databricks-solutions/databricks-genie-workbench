from __future__ import annotations


def test_backfill_patch_causal_metadata_uses_affected_questions() -> None:
    from genie_space_optimizer.optimization.harness import _backfill_patch_causal_metadata

    patches = [
        {"proposal_id": "P1", "type": "update_column_description", "lever": 1},
    ]
    ag = {
        "id": "AG2",
        "primary_cluster_id": "H003",
        "source_cluster_ids": ["H003"],
        "affected_questions": ["q007", "q009"],
    }

    enriched = _backfill_patch_causal_metadata(
        patches=patches,
        action_group=ag,
        source_clusters=[],
    )

    assert enriched[0]["action_group_id"] == "AG2"
    assert enriched[0]["primary_cluster_id"] == "H003"
    assert enriched[0]["source_cluster_ids"] == ["H003"]
    assert enriched[0]["target_qids"] == ["q007", "q009"]
    assert enriched[0]["_grounding_target_qids"] == ["q007", "q009"]


def test_backfill_patch_causal_metadata_preserves_explicit_rca_targets() -> None:
    from genie_space_optimizer.optimization.harness import _backfill_patch_causal_metadata

    patches = [
        {
            "proposal_id": "P_rca",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "rca_id": "rca_q007_filter",
            "target_qids": ["q007"],
        },
    ]
    ag = {
        "id": "AG2",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q007", "q009"],
    }

    enriched = _backfill_patch_causal_metadata(
        patches=patches,
        action_group=ag,
        source_clusters=[],
    )

    assert enriched[0]["rca_id"] == "rca_q007_filter"
    assert enriched[0]["target_qids"] == ["q007"]
    assert enriched[0]["_grounding_target_qids"] == ["q007"]
