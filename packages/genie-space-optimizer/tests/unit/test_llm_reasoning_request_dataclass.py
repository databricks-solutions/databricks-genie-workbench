"""Plan 2 Task 3 — LlmReasoningRequest dataclass contract."""
from __future__ import annotations

import dataclasses

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _DummyResult(LLMOutputContract):
    answer: str


def test_request_is_frozen_dataclass_with_slots() -> None:
    assert dataclasses.is_dataclass(LlmReasoningRequest)
    assert LlmReasoningRequest.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in LlmReasoningRequest.__dict__


def test_request_construction_populates_all_required_fields() -> None:
    req = LlmReasoningRequest(
        call_id="call_001",
        skill_id="rca-evidence-extraction",
        system_msg="You are an RCA evidence extractor.",
        user_prompt="qid: gs_009 ...",
        result_cls=_DummyResult,
        max_tokens=1000,
    )
    assert req.call_id == "call_001"
    assert req.skill_id == "rca-evidence-extraction"
    assert req.system_msg == "You are an RCA evidence extractor."
    assert req.user_prompt == "qid: gs_009 ..."
    assert req.result_cls is _DummyResult
    assert req.max_tokens == 1000


def test_request_rejects_max_tokens_zero_or_negative() -> None:
    for bad in (0, -1, -100):
        with pytest.raises(ValueError, match="max_tokens must be > 0"):
            LlmReasoningRequest(
                call_id="x",
                skill_id="rca-evidence-extraction",
                system_msg="s",
                user_prompt="u",
                result_cls=_DummyResult,
                max_tokens=bad,
                    )


def test_request_rejects_empty_call_id() -> None:
    with pytest.raises(ValueError, match="call_id"):
        LlmReasoningRequest(
            call_id="",
            skill_id="rca-evidence-extraction",
            system_msg="s",
            user_prompt="u",
            result_cls=_DummyResult,
            max_tokens=100,
            )


def test_request_rejects_empty_skill_id() -> None:
    with pytest.raises(ValueError, match="skill_id"):
        LlmReasoningRequest(
            call_id="c",
            skill_id="",
            system_msg="s",
            user_prompt="u",
            result_cls=_DummyResult,
            max_tokens=100,
            )


# Plan 8 Task 11 — model_override field removed from LlmReasoningRequest;
# the test that asserted its None default is retired with the field.
