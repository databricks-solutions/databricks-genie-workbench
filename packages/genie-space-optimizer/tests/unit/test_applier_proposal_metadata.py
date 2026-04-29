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
    assert patches[0]["proposal_id"] == "AG1_COL1"
    assert patches[0]["source_proposal_id"] == "AG1_COL1"
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
    assert patches[0]["proposal_id"] == "AG1_SQL1"
    assert patches[0]["source_proposal_id"] == "AG1_SQL1"
    assert patches[0]["_grounding_target_qids"] == ["q022"]
