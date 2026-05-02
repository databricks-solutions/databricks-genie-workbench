from __future__ import annotations


def test_proposal_id_survives_update_column_description_conversion() -> None:
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    patches = proposals_to_patches([{
        "proposal_id": "AG1_COL1",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_7now_fact_sales",
        "column": "cy_tot_orders",
        "description": "Current year total order count.",
        "target_qids": ["q022"],
        "_grounding_target_qids": ["q022"],
    }])

    assert len(patches) == 1
    assert patches[0]["proposal_id"].startswith("AG1_COL1")
    assert patches[0]["source_proposal_id"] == "AG1_COL1"
    assert patches[0]["parent_proposal_id"] == "AG1_COL1"
    assert patches[0]["target_qids"] == ["q022"]
    assert patches[0]["_grounding_target_qids"] == ["q022"]


def test_proposal_id_survives_sql_snippet_conversion() -> None:
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    patches = proposals_to_patches([{
        "proposal_id": "AG1_SQL1",
        "patch_type": "add_sql_snippet_measure",
        "table": "cat.sch.mv_7now_fact_sales",
        "sql": "SUM(mv_7now_fact_sales.cy_tot_orders)",
        "snippet_type": "measure",
        "validation_passed": True,
        "target_qids": ["q022"],
        "_grounding_target_qids": ["q022"],
    }])

    assert len(patches) == 1
    assert patches[0]["proposal_id"].startswith("AG1_SQL1")
    assert patches[0]["source_proposal_id"] == "AG1_SQL1"
    assert patches[0]["parent_proposal_id"] == "AG1_SQL1"
    assert patches[0]["_grounding_target_qids"] == ["q022"]


def test_update_column_description_rejects_empty_list_column_target() -> None:
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    patches = proposals_to_patches([{
        "proposal_id": "BAD_EMPTY",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_7now_fact_sales",
        "column": [],
        "description": "Invalid empty column target.",
    }])

    assert patches == []


def test_update_column_description_rejects_multi_column_list_target() -> None:
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    patches = proposals_to_patches([{
        "proposal_id": "BAD_MULTI",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_7now_store_sales",
        "column": ["zone_combination", "7now_avg_txn_diff_day"],
        "description": "Invalid multi-column target.",
    }])

    assert patches == []


def test_expanded_column_description_children_keep_parent_proposal_id() -> None:
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    patches = proposals_to_patches([{
        "proposal_id": "AG1_COL1",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_7now_fact_sales",
        "column": "cy_tot_orders",
        "structured_sections": {
            "definition": "Current year orders.",
            "aggregation": "SUM. Do not pre-filter NULL measures.",
        },
        "target_qids": ["q022"],
    }])

    assert len(patches) >= 1
    assert {p["parent_proposal_id"] for p in patches} == {"AG1_COL1"}
    assert all(p["proposal_id"].startswith("AG1_COL1#") for p in patches)
    assert all(p["expanded_patch_id"].startswith("AG1_COL1#") for p in patches)


def test_proposals_to_patches_preserves_risk_and_cluster_metadata() -> None:
    """A scanned high-risk proposal must become a patch carrying every risk
    and cluster field the cap, gates, and survival ledger consume."""
    from genie_space_optimizer.optimization.applier import (
        PROPOSAL_METADATA_ALLOWLIST,
        proposals_to_patches,
    )

    proposal = {
        "proposal_id": "P_RISKY",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_fact",
        "column": "tkt_coupon",
        "description": "Coupon code applied to the ticket.",
        "target_qids": ["q005"],
        "_grounding_target_qids": ["q005"],
        "passing_dependents": ["q010", "q011", "q012", "q013", "q014", "q015"],
        "passing_dependents_outside_target": ["q010", "q011", "q012", "q013", "q014", "q015"],
        "high_collateral_risk": True,
        "target_dependents": ["q005"],
        "cluster_id": "H001",
        "source_cluster_id": "H001",
        "source_cluster_ids": ["H001"],
        "primary_cluster_id": "H001",
        "root_cause": "wrong_filter_condition",
        "rca_kind": "filter_drift",
        "relevance_score": 0.91,
        "causal_attribution_tier": 3,
        "rca_id": "rca_q005_filter",
    }

    patches = proposals_to_patches([proposal])

    assert len(patches) >= 1
    patch = patches[0]
    expected_fields = (
        "passing_dependents",
        "passing_dependents_outside_target",
        "high_collateral_risk",
        "target_dependents",
        "cluster_id",
        "source_cluster_id",
        "source_cluster_ids",
        "primary_cluster_id",
        "root_cause",
        "rca_kind",
        "relevance_score",
        "causal_attribution_tier",
        "rca_id",
        "target_qids",
        "_grounding_target_qids",
    )
    for field in expected_fields:
        assert field in patch, (
            f"proposal-to-patch contract violated: '{field}' missing from patch; "
            f"allowlist={PROPOSAL_METADATA_ALLOWLIST!r}"
        )
    assert patch["passing_dependents"] == proposal["passing_dependents"]
    assert patch["high_collateral_risk"] is True
    assert patch["primary_cluster_id"] == "H001"
    assert patch["relevance_score"] == 0.91
