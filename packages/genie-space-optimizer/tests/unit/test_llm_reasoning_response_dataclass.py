"""Plan 2 Task 4 — LlmReasoningResponse dataclass contract."""
from __future__ import annotations

import dataclasses

import pytest

from genie_space_optimizer.optimization.llm_abstain import (
    AbstainReason,
    AbstainVerdict,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_response_is_frozen_dataclass_with_slots() -> None:
    assert dataclasses.is_dataclass(LlmReasoningResponse)
    assert LlmReasoningResponse.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in LlmReasoningResponse.__dict__


def test_response_mixes_in_json_round_trip() -> None:
    assert issubclass(LlmReasoningResponse, JsonRoundTrip)


def test_response_construction_success_path_round_trips() -> None:
    parsed = {"answer": "yes"}
    resp = LlmReasoningResponse(
        call_id="c1",
        skill_id="rca-evidence-extraction",
        succeeded=True,
        parsed_output=parsed,
        declined=None,
        raw_text='{"result": {"answer": "yes"}, "declined": null}',
        tokens_input=120,
        tokens_output=8,
        duration_ms=1450,
        error=None,
    )
    rebuilt = LlmReasoningResponse.from_json(resp.to_json())
    assert rebuilt == resp


def test_response_construction_abstain_path_round_trips() -> None:
    verdict = AbstainVerdict(
        reason=AbstainReason.MISSING_SCHEMA_CONTEXT,
        explanation="no metadata",
        needed_evidence=("table_metadata",),
        suggested_next_step="re_dispatch",
    )
    resp = LlmReasoningResponse(
        call_id="c2",
        skill_id="rca-evidence-extraction",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text='{"result": null, "declined": {...}}',
        tokens_input=120,
        tokens_output=24,
        duration_ms=1100,
        error=None,
    )
    payload = resp.to_json()
    assert payload["declined"]["reason"] == "missing_schema_context"
    rebuilt = LlmReasoningResponse.from_json(payload)
    assert rebuilt.declined == verdict


def test_response_construction_error_path() -> None:
    resp = LlmReasoningResponse(
        call_id="c3",
        skill_id="rca-evidence-extraction",
        succeeded=False,
        parsed_output=None,
        declined=None,
        raw_text="",
        tokens_input=0,
        tokens_output=0,
        duration_ms=400,
        error="HTTP 429 after 3 retries",
    )
    assert resp.succeeded is False
    assert resp.error == "HTTP 429 after 3 retries"


def test_response_succeeded_and_parsed_output_must_agree() -> None:
    with pytest.raises(ValueError, match="succeeded=True"):
        LlmReasoningResponse(
            call_id="c",
            skill_id="s",
            succeeded=True,
            parsed_output=None,
            declined=None,
            raw_text="",
            tokens_input=0,
            tokens_output=0,
            duration_ms=0,
            error=None,
        )


def test_response_declined_and_succeeded_are_mutually_exclusive() -> None:
    verdict = AbstainVerdict(
        reason=AbstainReason.OTHER,
        explanation="",
        needed_evidence=(),
        suggested_next_step="",
    )
    with pytest.raises(ValueError, match="declined.*succeeded"):
        LlmReasoningResponse(
            call_id="c",
            skill_id="s",
            succeeded=True,
            parsed_output={"x": 1},
            declined=verdict,
            raw_text="",
            tokens_input=0,
            tokens_output=0,
            duration_ms=0,
            error=None,
        )
