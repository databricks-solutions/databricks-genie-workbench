"""Plan 2 Task 1 — AbstainReason enum contract."""
from __future__ import annotations

from enum import StrEnum

from genie_space_optimizer.optimization.llm_abstain import AbstainReason


def test_abstain_reason_is_strenum() -> None:
    assert issubclass(AbstainReason, StrEnum)


def test_abstain_reason_members_cover_reviewer_required_set() -> None:
    expected = {
        "missing_schema_context",
        "ambiguous_failure",
        "unsafe_patch",
        "no_applicable_patch_type",
        "insufficient_blame_set",
        "context_token_budget_exceeded",
        "other",
    }
    actual = {r.value for r in AbstainReason}
    assert actual == expected, (
        f"AbstainReason vocabulary drift — missing={expected - actual}, "
        f"unexpected={actual - expected}"
    )


def test_abstain_reason_values_are_stable_strings() -> None:
    assert AbstainReason.MISSING_SCHEMA_CONTEXT == "missing_schema_context"
    assert AbstainReason.AMBIGUOUS_FAILURE == "ambiguous_failure"
    assert AbstainReason.UNSAFE_PATCH == "unsafe_patch"
    assert AbstainReason.NO_APPLICABLE_PATCH_TYPE == "no_applicable_patch_type"
    assert AbstainReason.INSUFFICIENT_BLAME_SET == "insufficient_blame_set"
    assert (
        AbstainReason.CONTEXT_TOKEN_BUDGET_EXCEEDED
        == "context_token_budget_exceeded"
    )
    assert AbstainReason.OTHER == "other"
