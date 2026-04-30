from __future__ import annotations

from genie_space_optimizer.optimization.applier_audit import (
    ApplierDecision,
    build_applier_decision,
    diff_selected_vs_applied,
)


def test_build_applier_decision_extracts_identity_fields_from_patch() -> None:
    patch = {
        "id": "P002#3",
        "proposal_id": "P002",
        "parent_proposal_id": "P002",
        "expanded_patch_id": "P002#3",
        "lever": 4,
        "type": "update_column_description",
        "target_object": "main.demo.mv_7now_store_sales.time_window",
        "rca_id": "RCA_AG1_001",
        "_grounding_target_qids": ["gs_021"],
        "causal_attribution_tier": "primary",
    }
    decision = build_applier_decision(
        patch=patch,
        decision="applied",
        reason="render_and_apply_succeeded",
    )
    assert decision == ApplierDecision(
        proposal_id="P002",
        parent_proposal_id="P002",
        expanded_patch_id="P002#3",
        lever=4,
        patch_type="update_column_description",
        target_asset="main.demo.mv_7now_store_sales.time_window",
        rca_id="RCA_AG1_001",
        target_qids=("gs_021",),
        causal_attribution_tier="primary",
        decision="applied",
        reason="render_and_apply_succeeded",
        error_excerpt="",
    )


def test_build_applier_decision_truncates_error_to_500_chars() -> None:
    patch = {"id": "P010#1", "type": "add_sql_snippet_filter"}
    long_err = "x" * 1000
    decision = build_applier_decision(
        patch=patch,
        decision="dropped_exception",
        reason="apply_threw",
        error=long_err,
    )
    assert decision.decision == "dropped_exception"
    assert decision.reason == "apply_threw"
    assert len(decision.error_excerpt) == 500


def test_diff_selected_vs_applied_reports_both_directions() -> None:
    selected = ["P002#3", "P004#1", "P005#1"]
    applied = ["P010#1"]
    diff = diff_selected_vs_applied(selected_ids=selected, applied_ids=applied)
    assert diff.selected_but_not_applied == ("P002#3", "P004#1", "P005#1")
    assert diff.applied_but_not_selected == ("P010#1",)
    assert diff.in_agreement is False


def test_diff_selected_vs_applied_in_agreement_when_sets_match() -> None:
    diff = diff_selected_vs_applied(
        selected_ids=["P002#3", "P004#1"],
        applied_ids=["P004#1", "P002#3"],
    )
    assert diff.in_agreement is True
    assert diff.selected_but_not_applied == ()
    assert diff.applied_but_not_selected == ()


def test_apply_patch_set_emits_decision_per_patch_in_apply_log() -> None:
    import inspect

    from genie_space_optimizer.optimization import applier

    source = inspect.getsource(applier.apply_patch_set)
    assert "applier_decisions" in source
    assert "build_applier_decision(" in source
    assert "from genie_space_optimizer.optimization.applier_audit import" in inspect.getsource(applier)
