"""Tests for the high → medium risk downgrade when a scoped variant
is available (Phase 2 Action 2.3 ↔ Action 2.2 integration)."""

from __future__ import annotations

from genie_space_optimizer.optimization.kit_safety import (
    KitSafetyPolicy,
    select_kit_aware_patch_cap,
)


def test_high_risk_kit_with_scoped_variant_is_accepted_and_marked_downgraded() -> None:
    patches = [
        {
            "proposal_id": "P_hub",
            "_repair_archetype": "plural_top_n_collapse",
            "target_qids": ("gs_026",),
            "_expected_causal_effect": "ORDER BY",
            "patch_type": "add_sql_snippet_filter",
            "target_table": "mv_7now_fact_sales",
            "high_collateral_risk": True,
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
        {
            "proposal_id": "P_hub_scoped",
            "_scoped_from_pid": "P_hub",
            "_scoped_variant_pid": "P_hub_scoped",
            "_repair_archetype": "plural_top_n_collapse",
            "target_qids": ("gs_026",),
            "_expected_causal_effect": "ORDER BY",
            "patch_type": "add_sql_snippet_filter",
            "target_table": "mv_7now_fact_sales",
            "scoped_to_qids": ("gs_026",),
            "passing_dependents": ["gs_026"],
        },
    ]
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    accepted = [o for o in kit_outcomes if o.get("accepted")]
    assert len(accepted) == 1
    assert accepted[0].get("risk_downgraded_from_high_to_medium") is True


def test_high_risk_kit_without_scoped_variant_is_rejected() -> None:
    patches = [
        {
            "proposal_id": "P_hub",
            "_repair_archetype": "plural_top_n_collapse",
            "target_qids": ("gs_026",),
            "_expected_causal_effect": "ORDER BY",
            "patch_type": "add_sql_snippet_filter",
            "target_table": "mv_7now_fact_sales",
            "high_collateral_risk": True,
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
    ]
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    rejected = [o for o in kit_outcomes if not o.get("accepted")]
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "high_risk_no_scoped_alternative"
