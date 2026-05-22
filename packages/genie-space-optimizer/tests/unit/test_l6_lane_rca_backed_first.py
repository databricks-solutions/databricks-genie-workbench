"""L6 lane ordering: when an RCA card has scoped metadata
(target_object, causal_target, failing_sql_anchor, rca_card_id),
the RCA-backed scoped synthesis path runs first. Broad emit is only
the fallback when no usable RCA card is present.
"""
def test_l6_lane_prefers_rca_backed_when_rca_card_complete():
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        choose_l6_synthesis_order,
    )

    rca_card = {
        "rca_card_id": "rca_gs_021_missing_mtd_v1",
        "intended_patch_shape": "structural",
        "target_objects": ["mv_7now_fact_sales.time_window"],
        "causal_target": "mv_7now_fact_sales.time_window",
        "failing_sql_anchor": "FROM mv_7now_fact_sales",
    }
    order = choose_l6_synthesis_order(rca_card=rca_card)
    assert order == ("rca_backed_scoped", "broad_emit")


def test_l6_lane_falls_back_to_broad_emit_when_no_rca_card():
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        choose_l6_synthesis_order,
    )

    order = choose_l6_synthesis_order(rca_card=None)
    assert order == ("broad_emit",)


def test_l6_lane_falls_back_to_broad_emit_when_rca_card_lacks_target_objects():
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        choose_l6_synthesis_order,
    )

    rca_card = {"rca_card_id": "rca_partial", "target_objects": []}
    order = choose_l6_synthesis_order(rca_card=rca_card)
    assert order == ("broad_emit",)
