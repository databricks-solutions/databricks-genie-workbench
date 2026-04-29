"""Closed-loop next-action mapping for RCA optimizer rejections."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RcaNextAction(str, Enum):
    NONE = "none"
    REPAIR_TARGET_IDENTITY = "repair_target_identity"
    ROTATE_PATCH_FAMILY = "rotate_patch_family"
    CHANGE_PATCH_FAMILY = "change_patch_family"
    MARK_CONFLICTING_THEME = "mark_conflicting_theme"
    REVISE_SQL_SNIPPET = "revise_sql_snippet"
    SWITCH_TO_NON_EXAMPLE_LEVERS = "switch_to_non_example_levers"
    REGENERATE_TEACHING_EXAMPLE = "regenerate_teaching_example"
    TERMINATE = "terminate"


@dataclass(frozen=True)
class RcaNextActionDecision:
    action: RcaNextAction
    forced_levers: tuple[int, ...] = ()
    terminal_status: str = ""
    reason: str = ""


def next_action_for_rejection(
    *,
    rollback_reason: str,
    grounding_failure_category: str = "",
    repeated_count: int = 1,
) -> RcaNextActionDecision:
    """Map a rollback reason + grounding category to a deterministic next action.

    Each rejection class either redirects the loop to a different RCA action
    (different patch family, different lever, repaired target identity) or
    marks the run as terminal. The mapping is closed: every supported reason
    has an explicit next step, so retries cannot blindly repeat the same
    failure mode.
    """
    reason = str(rollback_reason or "").strip()
    category = str(grounding_failure_category or "").strip()
    repeats = int(repeated_count or 0)

    if reason == "no_grounded_patches":
        if category in {"no_scoped_rows", "empty_surface"}:
            return RcaNextActionDecision(
                RcaNextAction.REPAIR_TARGET_IDENTITY,
                reason=f"{reason}:{category}",
            )
        if category in {"no_overlap", "below_min_relevance", "generic_rca_overlap"}:
            return RcaNextActionDecision(
                RcaNextAction.ROTATE_PATCH_FAMILY,
                forced_levers=(5, 6),
                reason=f"{reason}:{category}",
            )
        return RcaNextActionDecision(
            RcaNextAction.ROTATE_PATCH_FAMILY,
            forced_levers=(5,),
            reason=reason,
        )

    if reason == "target_qids_not_improved":
        return RcaNextActionDecision(
            RcaNextAction.CHANGE_PATCH_FAMILY,
            forced_levers=(1, 5, 6),
            reason=reason,
        )

    if reason == "post_arbiter_not_improved":
        return RcaNextActionDecision(
            RcaNextAction.CHANGE_PATCH_FAMILY,
            forced_levers=(5, 6),
            reason=reason,
        )

    if reason == "out_of_target_hard_regression":
        return RcaNextActionDecision(
            RcaNextAction.MARK_CONFLICTING_THEME,
            reason=reason,
        )

    if reason in {"sql_validation_failed", "sql_snippet_validation_failed"}:
        return RcaNextActionDecision(
            RcaNextAction.REVISE_SQL_SNIPPET,
            forced_levers=(6,),
            reason=reason,
        )

    if reason == "synthesis_failed":
        if repeats >= 2:
            return RcaNextActionDecision(
                RcaNextAction.SWITCH_TO_NON_EXAMPLE_LEVERS,
                forced_levers=(1, 5, 6),
                reason=reason,
            )
        return RcaNextActionDecision(
            RcaNextAction.REGENERATE_TEACHING_EXAMPLE,
            forced_levers=(5,),
            reason=reason,
        )

    if reason == "benchmark_leakage_rejected":
        return RcaNextActionDecision(
            RcaNextAction.REGENERATE_TEACHING_EXAMPLE,
            forced_levers=(5,),
            reason=reason,
        )

    if reason in {"judge_unreliable", "benchmark_broken", "unpatchable_with_six_levers"}:
        return RcaNextActionDecision(
            RcaNextAction.TERMINATE,
            terminal_status=reason,
            reason=reason,
        )

    return RcaNextActionDecision(RcaNextAction.NONE, reason=reason or "unknown")
