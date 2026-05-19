"""Plan 2 Task 7 — budget overflow raises a typed AbstainVerdict."""
from __future__ import annotations

from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
)


def test_budget_overflow_abstain_has_typed_reason() -> None:
    b = IterationTokenBudget(itpm_limit=100, otpm_limit=100)
    b.reserve(input_tokens=80, max_output_tokens=80)
    verdict = b.make_overflow_abstain(input_tokens=30, max_output_tokens=30)
    assert verdict.reason is AbstainReason.CONTEXT_TOKEN_BUDGET_EXCEEDED


def test_budget_overflow_abstain_explanation_names_limit_type() -> None:
    """Mirroring Databricks 429's limit_type so postmortem can route."""
    b = IterationTokenBudget(itpm_limit=100, otpm_limit=200)
    b.reserve(input_tokens=80, max_output_tokens=10)
    verdict = b.make_overflow_abstain(input_tokens=30, max_output_tokens=10)
    assert "input_tokens_per_minute" in verdict.explanation


def test_budget_overflow_abstain_suggests_retry_after_iteration() -> None:
    b = IterationTokenBudget(itpm_limit=100, otpm_limit=100)
    b.reserve(input_tokens=80, max_output_tokens=80)
    verdict = b.make_overflow_abstain(input_tokens=30, max_output_tokens=30)
    assert verdict.suggested_next_step == "defer_to_next_iteration"


def test_budget_overflow_abstain_needed_evidence_is_empty() -> None:
    """Token-budget overflow is framework-internal; upstream cannot
    provide extra evidence to help."""
    b = IterationTokenBudget(itpm_limit=100, otpm_limit=100)
    b.reserve(input_tokens=80, max_output_tokens=80)
    verdict = b.make_overflow_abstain(input_tokens=30, max_output_tokens=30)
    assert verdict.needed_evidence == ()
