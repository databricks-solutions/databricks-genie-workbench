from genie_space_optimizer.optimization.optimizer import (
    _diagnose_lever3_directive_emission,
    _lever6_reject_payload,
)


def test_lever6_reject_payload_is_structured() -> None:
    payload = _lever6_reject_payload(
        reason="invalid_identifiers",
        cluster_id="AG3",
        target_table="cat.sch.mv_7now_store_sales",
        detail=["Unknown table: bogus"],
    )

    assert payload == {
        "rejected": True,
        "reject_reason": "invalid_identifiers",
        "cluster_id": "AG3",
        "target_table": "cat.sch.mv_7now_store_sales",
        "detail": ["Unknown table: bogus"],
    }


def test_diagnose_lever3_directive_emission_reports_missing_action_group() -> None:
    clusters = [
        {
            "cluster_id": "AG3",
            "root_cause": "missing_data_asset",
            "question_ids": ["gs_023"],
            "asi_blame_set": ["fn_mtd_or_mtday"],
        }
    ]
    strategy = {
        "action_groups": [
            {
                "action_group_id": "AG1",
                "target_lever": 5,
                "affected_questions": ["gs_001"],
            }
        ]
    }

    diagnostics = _diagnose_lever3_directive_emission(clusters, strategy)

    assert diagnostics == [
        {
            "cluster_id": "AG3",
            "expected_lever": 3,
            "status": "missing_lever3_action_group",
            "question_ids": ["gs_023"],
            "blame_set": ["fn_mtd_or_mtday"],
        }
    ]
