from genie_space_optimizer.optimization.optimizer import (
    _diagnose_lever3_directive_emission,
    _lever1_theme_key,
    _lever6_reject_payload,
    _strategist_memo_key,
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


def test_strategist_memo_key_is_stable_for_same_cluster_signature() -> None:
    cluster = {
        "cluster_signature": "sig-abc",
        "root_cause": "missing_data_asset",
        "question_ids": ["gs_023", "gs_024"],
        "asi_blame_set": ["fn_mtd_or_mtday"],
    }

    key1 = _strategist_memo_key([cluster], {"space_revision": "r1"})
    key2 = _strategist_memo_key([dict(cluster)], {"space_revision": "r1"})

    assert key1 == key2
    assert "sig-abc" in key1
    assert "r1" in key1


def test_lever1_theme_key_groups_by_root_cause_and_blame() -> None:
    cluster = {
        "root_cause": "wrong_measure",
        "patch_family": "contrastive_measure_disambiguation",
        "asi_blame_set": ["mv_7now_store_sales._7now_cy_sales_mtd"],
    }

    assert _lever1_theme_key(cluster) == (
        "wrong_measure",
        "contrastive_measure_disambiguation",
        ("mv_7now_store_sales._7now_cy_sales_mtd",),
    )
