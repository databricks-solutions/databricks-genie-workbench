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
        "optimizer_capacity_starved",
        "prompt_too_large",
        # P4 C1 — typed Stage-1 abstain when the LLM cannot produce a
        # repair diagnosis with concrete enough evidence to fire the
        # structural-repair lane. Pins the vocabulary so no future PR
        # silently removes the typed abstain in favor of the legacy
        # ``generic_judge_guidance`` fall-through.
        "repair_intent_indeterminate",
        # P4 C3 — typed Stage-3 abstain when the synthesizer's
        # producer-side ``validate_sql_snippet`` call rejects the
        # LLM-emitted SQL snippet.
        "snippet_invalid",
        # Trial 24 Follow-on B — typed Stage-3 decline when the snippet
        # SQL is a tautology / no-op suppression (``1=1`` / ``TRUE``);
        # distinct from ``snippet_invalid`` so the synthesizer can
        # degrade a filter-removal kit to an instruction-only solo.
        "snippet_noop_suppression",
        # P4 C4 — typed Stage-3 abstain when the synthesizer's
        # producer-side target preflight cannot resolve a column-
        # touching proposal's ``catalog.schema.table.column`` path.
        "target_unresolvable",
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
    assert (
        AbstainReason.OPTIMIZER_CAPACITY_STARVED
        == "optimizer_capacity_starved"
    )
    assert AbstainReason.PROMPT_TOO_LARGE == "prompt_too_large"
    assert AbstainReason.OTHER == "other"
