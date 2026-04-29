"""Reflection rollback reason must surface the real gate verdict."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _format_rollback_reflection,
)


def test_post_arbiter_not_improved_label() -> None:
    text = _format_rollback_reflection(
        rollback_reason="full_eval: acceptance_gate (rejected_insufficient_gain)",
        control_plane_reason="post_arbiter_not_improved",
        any_target_improved=False,
        regressions=[],
        patch_types=["update_column_description", "add_sql_snippet_filter"],
        root_cause_summary="Genie omits the required time_window filter",
        accuracy_delta_pp=0.0,
    )
    assert "Rollback (no_overall_improvement)" in text
    assert "post_arbiter_not_improved" in text


def test_out_of_target_regression_label() -> None:
    text = _format_rollback_reflection(
        rollback_reason="full_eval: acceptance_gate (rejected_insufficient_gain)",
        control_plane_reason="out_of_target_hard_regression",
        any_target_improved=True,
        regressions=[{"qid": "q001"}],
        patch_types=["add_sql_snippet_filter"],
        root_cause_summary="time_window filter",
        accuracy_delta_pp=-4.5,
    )
    assert "Rollback (collateral_regression)" in text
    assert "q001" in text or "1 collateral" in text


def test_unknown_reason_falls_back_to_legacy() -> None:
    text = _format_rollback_reflection(
        rollback_reason="patch_deploy_failed: timeout",
        control_plane_reason="",
        any_target_improved=False,
        regressions=[],
        patch_types=["add_instruction"],
        root_cause_summary="root",
        accuracy_delta_pp=0.0,
    )
    assert "Rollback" in text
    assert "patch_deploy_failed" in text
