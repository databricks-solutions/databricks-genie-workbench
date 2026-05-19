"""Plan 2 Task 1 — AbstainVerdict dataclass contract."""
from __future__ import annotations

import dataclasses

import pytest

from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_abstain_verdict_is_frozen_dataclass_with_slots() -> None:
    assert dataclasses.is_dataclass(AbstainVerdict)
    assert AbstainVerdict.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in AbstainVerdict.__dict__


def test_abstain_verdict_mixes_in_json_round_trip() -> None:
    assert issubclass(AbstainVerdict, JsonRoundTrip)


def test_abstain_verdict_round_trips_to_and_from_json() -> None:
    verdict = AbstainVerdict(
        reason=AbstainReason.MISSING_SCHEMA_CONTEXT,
        explanation="No catalog metadata for blame set members.",
        needed_evidence=("table_metadata", "column_descriptions"),
        suggested_next_step="re_dispatch_after_uc_enrichment",
    )
    payload = verdict.to_json()
    assert payload == {
        "reason": "missing_schema_context",
        "explanation": "No catalog metadata for blame set members.",
        "needed_evidence": ["table_metadata", "column_descriptions"],
        "suggested_next_step": "re_dispatch_after_uc_enrichment",
    }
    rebuilt = AbstainVerdict.from_json(payload)
    assert rebuilt == verdict


def test_abstain_verdict_explanation_max_len_enforced_at_construction() -> None:
    too_long = "x" * 201
    with pytest.raises(ValueError, match="explanation must be"):
        AbstainVerdict(
            reason=AbstainReason.OTHER,
            explanation=too_long,
            needed_evidence=(),
            suggested_next_step="",
        )


def test_abstain_verdict_needed_evidence_is_tuple() -> None:
    verdict = AbstainVerdict(
        reason=AbstainReason.AMBIGUOUS_FAILURE,
        explanation="Two blame sets equally fit the cluster.",
        needed_evidence=("disambiguating_judge_verdict",),
        suggested_next_step="",
    )
    assert isinstance(verdict.needed_evidence, tuple)


def test_abstain_verdict_pretty_renders_one_line_per_field() -> None:
    verdict = AbstainVerdict(
        reason=AbstainReason.UNSAFE_PATCH,
        explanation="Patch would shadow a benchmark example.",
        needed_evidence=("leakage_review",),
        suggested_next_step="hold_and_request_human_review",
    )
    rendered = verdict.to_pretty()
    assert "reason" in rendered
    assert "unsafe_patch" in rendered or "UNSAFE_PATCH" in rendered
    assert "Patch would shadow a benchmark example." in rendered
