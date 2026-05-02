"""Track E — reflection-as-validator must distinguish split-children of
two different parent rewrites that touch the same instruction section.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.reflection_retry import (
    patch_retry_signature,
    retry_allowed_after_rollback,
)


def _split_child_patch(
    parent_proposal_id: str,
    section: str,
    instruction_text: str,
) -> dict:
    return {
        "proposal_id": f"{parent_proposal_id}#1",
        "parent_proposal_id": parent_proposal_id,
        "_split_from": "rewrite_instruction",
        "type": "update_instruction_section",
        "section_name": section,
        "value": instruction_text,
    }


def test_two_parents_touching_same_section_have_distinct_signatures() -> None:
    """Different parent_proposal_id => different retry signature even
    when patch_type, target, and section are identical.
    """
    parent_a_child = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Day-over-Day for trend KPIs."
    )
    parent_b_child = _split_child_patch(
        "P_REWRITE_B", "QUERY RULES", "Use Day-over-Day for trend KPIs."
    )

    sig_a = patch_retry_signature(parent_a_child)
    sig_b = patch_retry_signature(parent_b_child)
    assert sig_a != sig_b, (
        f"split-children of two different parents share a signature; "
        f"sig_a={sig_a!r}, sig_b={sig_b!r}"
    )


def test_same_parent_same_section_same_content_collide() -> None:
    """A re-proposal of the SAME parent rewrite, SAME section, SAME
    content must collide with itself so reflection blocks the retry.
    """
    first = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Day-over-Day for trend KPIs."
    )
    rerun = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Day-over-Day for trend KPIs."
    )
    assert patch_retry_signature(first) == patch_retry_signature(rerun), (
        "same parent + same section + same content must collide for "
        "reflection-as-validator to block the retry"
    )


def test_same_parent_same_section_different_content_distinct() -> None:
    """A re-proposal of the SAME parent + section but DIFFERENT content
    must be distinguishable so reflection allows fresh content even
    when section matches a prior rolled-back attempt.
    """
    first = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Day-over-Day for trend KPIs."
    )
    revised = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Month-over-Month for trend KPIs."
    )
    assert patch_retry_signature(first) != patch_retry_signature(revised), (
        "different content for the same parent+section must produce "
        "distinct retry signatures so fresh content is not over-blocked"
    )


def test_retry_decision_allows_fresh_content_after_prior_rollback() -> None:
    """End-to-end: a child of parent A was rolled back. A new proposal
    for the SAME section but DIFFERENT content must be allowed.
    """
    rolled_back = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Day-over-Day."
    )
    fresh_attempt = _split_child_patch(
        "P_REWRITE_A", "QUERY RULES", "Use Month-over-Month."
    )
    decision = retry_allowed_after_rollback(
        current_patch=fresh_attempt,
        rolled_back_patches=[rolled_back],
        rollback_cause="content_regression",
    )
    assert decision.allowed is True, (
        f"reflection over-blocked fresh content; reason={decision.reason}"
    )
