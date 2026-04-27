def test_asi_metadata_accepts_typed_rca_fields():
    from genie_space_optimizer.optimization.evaluation import build_asi_metadata

    meta = build_asi_metadata(
        failure_type="wrong_table",
        severity="major",
        confidence=0.9,
        blame_set=["mv_7now_store_sales"],
        counterfactual_fix=(
            "Use mv_esr_store_sales for unqualified average transaction value."
        ),
        expected_objects=["mv_esr_store_sales", "avg_txn_day"],
        actual_objects=["mv_7now_store_sales", "7now_avg_txn_cy_day"],
        rca_kind="metric_view_routing_confusion",
        patch_family="contrastive_metric_routing",
        recommended_levers=[1, 5],
    )

    assert meta["rca_kind"] == "metric_view_routing_confusion"
    assert meta["expected_objects"] == ["mv_esr_store_sales", "avg_txn_day"]
    assert meta["recommended_levers"] == [1, 5]
