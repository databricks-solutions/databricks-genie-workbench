from genie_space_optimizer.optimization.rca_next_action import (
    RcaNextAction,
    next_action_for_rejection,
)


def test_no_grounded_patches_with_no_scoped_rows_repairs_target_identity() -> None:
    action = next_action_for_rejection(
        rollback_reason="no_grounded_patches",
        grounding_failure_category="no_scoped_rows",
        repeated_count=2,
    )

    assert action.action == RcaNextAction.REPAIR_TARGET_IDENTITY
    assert action.forced_levers == ()


def test_below_min_relevance_rotates_patch_family() -> None:
    action = next_action_for_rejection(
        rollback_reason="no_grounded_patches",
        grounding_failure_category="below_min_relevance",
        repeated_count=2,
    )

    assert action.action == RcaNextAction.ROTATE_PATCH_FAMILY
    assert action.forced_levers == (5, 6)


def test_target_not_improved_keeps_rca_but_changes_patch_family() -> None:
    action = next_action_for_rejection(
        rollback_reason="target_qids_not_improved",
        grounding_failure_category="grounded",
        repeated_count=1,
    )

    assert action.action == RcaNextAction.CHANGE_PATCH_FAMILY
    assert action.forced_levers == (1, 5, 6)


def test_out_of_target_regression_marks_conflict() -> None:
    action = next_action_for_rejection(
        rollback_reason="out_of_target_hard_regression",
        grounding_failure_category="grounded",
        repeated_count=1,
    )

    assert action.action == RcaNextAction.MARK_CONFLICTING_THEME
    assert action.terminal_status == ""


def test_repeated_synthesis_failure_switches_away_from_example_sql() -> None:
    action = next_action_for_rejection(
        rollback_reason="synthesis_failed",
        grounding_failure_category="",
        repeated_count=2,
    )

    assert action.action == RcaNextAction.SWITCH_TO_NON_EXAMPLE_LEVERS
    assert action.forced_levers == (1, 5, 6)


def test_repeated_judge_failure_terminal() -> None:
    action = next_action_for_rejection(
        rollback_reason="judge_unreliable",
        grounding_failure_category="",
        repeated_count=3,
    )

    assert action.action == RcaNextAction.TERMINATE
    assert action.terminal_status == "judge_unreliable"
