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


# ── Defect Plan 2 (2026-05-12) — closed-loop action for no_applied_patches ─


def test_no_applied_patches_rotates_patch_family() -> None:
    """Defect Plan 2: when every grounded patch is dropped pre-applier
    (typically blast-radius), the closed-loop next-action must be
    ROTATE_PATCH_FAMILY with forced levers (5, 6) — i.e. switch from
    rewrite/instruction (the most common blast-radius victim) to SQL
    snippets (lever 5) or synthesis (lever 6) which trigger different
    code paths in the applier.

    Pre-Defect-2 this reason fell to ``RcaNextAction.NONE`` because
    the closed-loop mapping had no branch for it; the strategist's
    extra payload carried no actionable hint and the loop spent its
    full iteration budget retrying the same patch family.
    """
    action = next_action_for_rejection(
        rollback_reason="no_applied_patches",
        grounding_failure_category="",
        repeated_count=1,
    )

    assert action.action == RcaNextAction.ROTATE_PATCH_FAMILY
    assert action.forced_levers == (5, 6)
    assert action.terminal_status == ""
    assert "no_applied_patches" in action.reason


def test_no_applied_patches_repeated_still_rotates() -> None:
    """Defect Plan 2: repeated no_applied_patches is the same defect
    pattern as iter-1; the mapping is not repeat-sensitive (unlike
    synthesis_failed which escalates at repeats >= 2). Forced levers
    stay (5, 6) — the only escalation is to TERMINATE which is
    reserved for unpatchable / judge-unreliable cases."""
    action = next_action_for_rejection(
        rollback_reason="no_applied_patches",
        grounding_failure_category="",
        repeated_count=3,
    )

    assert action.action == RcaNextAction.ROTATE_PATCH_FAMILY
    assert action.forced_levers == (5, 6)
