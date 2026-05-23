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


def test_abstain_verdict_accepts_explanations_under_soft_cap_without_truncation() -> None:
    """The cap was raised from 200 to 1000 chars after Trial 10 (PR-3).

    Production LLM outputs were 207 / 335 chars (well-formed,
    semantically-correct abstain explanations) that crashed the SM
    under the prior strict 200-char raise. Anything under the new soft
    cap must round-trip unchanged.
    """
    for length in (201, 207, 335, 999, 1000):
        explanation = "x" * length
        verdict = AbstainVerdict(
            reason=AbstainReason.OTHER,
            explanation=explanation,
            needed_evidence=(),
            suggested_next_step="",
        )
        assert verdict.explanation == explanation
        assert len(verdict.explanation) == length


def test_abstain_verdict_truncates_explanations_over_soft_cap_without_raising() -> None:
    """Past the soft cap the dataclass truncates gracefully — it must
    never raise on LLM-emitted explanations.

    Rationale: ``AbstainVerdict`` is constructed inside
    ``parse_envelope`` from raw LLM output. The 200-char cap was
    enforced *client-side only* (Databricks endpoint rejects the JSON
    Schema ``maxLength`` keyword via ``_UNSUPPORTED_KEYWORDS`` in
    ``prompt_io.py``) so the LLM could not be told about it via
    response_format. A strict raise turned every overlong abstain
    verdict into a SM-stage exception that cascaded into a legacy
    fallback and tripped ``InputProjectionContractViolation``. The
    truncate-with-headroom contract is the safer alternative.
    """
    over = "x" * 5_000
    verdict = AbstainVerdict(
        reason=AbstainReason.OTHER,
        explanation=over,
        needed_evidence=(),
        suggested_next_step="",
    )
    assert len(verdict.explanation) == 1_000
    assert verdict.explanation.endswith("...")
    assert verdict.explanation.startswith("xxx")


def test_abstain_verdict_truncation_is_round_trippable() -> None:
    """Truncated verdicts must still serialize and deserialize cleanly.

    A second ``from_json`` of an already-truncated payload must be a
    fixpoint (no further truncation, no raise) so postmortem replay
    doesn't double-truncate.
    """
    over = "x" * 5_000
    verdict = AbstainVerdict(
        reason=AbstainReason.OTHER,
        explanation=over,
        needed_evidence=(),
        suggested_next_step="",
    )
    payload = verdict.to_json()
    rebuilt = AbstainVerdict.from_json(payload)
    assert rebuilt == verdict
    assert len(rebuilt.explanation) == 1_000


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
