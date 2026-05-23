"""Plan 6 Task 1 — LlmCritiqueVerdictOutput Pydantic shape (per-proposal).

Per roadmap.md:387-399 — six LLM-emitted fields:
  addresses_target_failure: bool
  is_overgeneralized: bool
  likely_neighbor_regressions: list[str]
  matches_intended_shape: bool
  overall_recommendation: Literal["proceed", "rework", "discard"]
  rationale: str

intent_id from the roadmap excerpt is renamed to proposal_id here for
clarity (one verdict per PROPOSAL, multiple proposals per intent are
possible during Plan 8 rework retries). proposal_id is framework-
stamped (not LLM-minted) — mirrors Plan 4's cluster_id and Plan 5's
intent_id discipline.

declined lives on the envelope (AbstainableEnvelope[T]) per the Plan-2
abstain contract, not on the verdict itself.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)
from genie_space_optimizer.skills.candidate_critique.output_schema import (
    LlmCritiqueVerdictOutput,
)


def test_output_subclasses_llm_output_contract() -> None:
    assert issubclass(LlmCritiqueVerdictOutput, LLMOutputContract)


def test_required_fields_present_and_typed() -> None:
    """Six LLM-emitted fields after dropping proposal_id (framework-
    stamped) and declined (envelope-level)."""
    fields = LlmCritiqueVerdictOutput.model_fields
    expected = {
        "addresses_target_failure",
        "is_overgeneralized",
        "likely_neighbor_regressions",
        "matches_intended_shape",
        "overall_recommendation",
        "rationale",
    }
    assert set(fields.keys()) == expected, (
        f"field drift: missing={expected - set(fields.keys())}, "
        f"unexpected={set(fields.keys()) - expected}"
    )


def test_proposal_id_is_intentionally_absent() -> None:
    """Framework stamps proposal_id deterministically — the LLM never
    mints IDs (mirrors Plan 5's intent_id discipline)."""
    assert "proposal_id" not in LlmCritiqueVerdictOutput.model_fields


def test_intent_id_is_intentionally_absent() -> None:
    """The verdict references the proposal, not the intent — intent_id
    is recoverable from the proposal via Plan 1's
    extract_repair_intent_from_proposal helper."""
    assert "intent_id" not in LlmCritiqueVerdictOutput.model_fields


def test_overall_recommendation_is_literal_proceed_rework_discard() -> None:
    """Closed enum, mirrors roadmap.md:396 vocabulary."""
    inst = LlmCritiqueVerdictOutput(
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=[],
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="example_sql cleanly demonstrates top-N pattern",
    )
    assert inst.overall_recommendation == "proceed"

    with pytest.raises(ValidationError):
        LlmCritiqueVerdictOutput(
            addresses_target_failure=True,
            is_overgeneralized=False,
            likely_neighbor_regressions=[],
            matches_intended_shape=True,
            overall_recommendation="ok",
            rationale="x",
        )


def test_likely_neighbor_regressions_accepts_empty_list() -> None:
    inst = LlmCritiqueVerdictOutput(
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=[],
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="x",
    )
    assert inst.likely_neighbor_regressions == []


def test_likely_neighbor_regressions_accepts_qid_strings() -> None:
    inst = LlmCritiqueVerdictOutput(
        addresses_target_failure=True,
        is_overgeneralized=True,
        likely_neighbor_regressions=["gs_044", "gs_055"],
        matches_intended_shape=True,
        overall_recommendation="rework",
        rationale="example_sql touches revenue but unrelated to region",
    )
    assert inst.likely_neighbor_regressions == ["gs_044", "gs_055"]


def test_rationale_is_string() -> None:
    inst = LlmCritiqueVerdictOutput(
        addresses_target_failure=False,
        is_overgeneralized=True,
        likely_neighbor_regressions=["gs_044"],
        matches_intended_shape=False,
        overall_recommendation="discard",
        rationale=(
            "patch generalizes from a single failing qid to all "
            "questions touching revenue — high regression risk"
        ),
    )
    assert "regression risk" in inst.rationale


def test_extra_fields_are_forbidden() -> None:
    """LLMOutputContract sets extra=forbid."""
    with pytest.raises(ValidationError):
        LlmCritiqueVerdictOutput(
            addresses_target_failure=True,
            is_overgeneralized=False,
            likely_neighbor_regressions=[],
            matches_intended_shape=True,
            overall_recommendation="proceed",
            rationale="x",
            proposal_id="prop_001",
        )


def test_envelope_response_format_is_databricks_strict_safe() -> None:
    """AbstainableEnvelope[LlmCritiqueVerdictOutput] must build a clean
    response_format with no anyOf / oneOf / $ref / pattern (Plan 2
    Foundation Model API strict-mode requirement).

    Uses ``assert_no_forbidden_schema_keys`` (PR-C) instead of the
    pre-PR-C ``forbidden in repr(fmt)`` substring check, which false-
    positives on prose containing the literal word ``pattern``.
    """
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[LlmCritiqueVerdictOutput]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)
