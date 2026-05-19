"""Plan 2 Task 13 — end-to-end test of the reasoning-call framework.

Threads together every Plan-2 component using the reference
smoke-test skill: skill loader → request construction → invoke →
envelope parse → typed response. No mocks beyond ``_get_openai_client``
(which always needs stubbing — the alternative is a real LLM call).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.skills._loader import _SKILL_LOADER


_SKILL_ID = "_reference_smoke_test"


def _stub_with(envelope_json: str, *, prompt_tokens=12, completion_tokens=8):
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
    )
    client.chat.completions.create.return_value = completion
    return client


def _make_request_from_skill(*, call_id: str, user_prompt: str):
    """Realistic request construction: read skill metadata → output
    schema → build request. This is what Plans 3-7 callers do."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    assert rsm is not None
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    meta = _SKILL_LOADER.load_metadata(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=meta["prompt_constant_name"],
    )
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def test_end_to_end_success_path() -> None:
    req = _make_request_from_skill(call_id="e2e_001", user_prompt="hello")
    envelope = '{"result": {"echoed": "hello"}, "declined": null}'
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(envelope),
    ):
        resp = LlmReasoningCall().invoke(w=None, request=req)

    assert isinstance(resp, LlmReasoningResponse)
    assert resp.succeeded is True
    assert resp.parsed_output == {"echoed": "hello"}
    assert resp.declined is None
    assert resp.error is None
    assert resp.tokens_input == 12
    assert resp.tokens_output == 8


def test_end_to_end_abstain_path() -> None:
    req = _make_request_from_skill(call_id="e2e_002", user_prompt="")
    declined_envelope = """{
        "result": null,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "empty input",
            "needed_evidence": ["concrete_input"],
            "suggested_next_step": "skip"
        }
    }"""
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(declined_envelope),
    ):
        resp = LlmReasoningCall().invoke(w=None, request=req)

    assert resp.succeeded is False
    assert resp.declined is not None
    assert resp.declined.reason == AbstainReason.AMBIGUOUS_FAILURE


def test_end_to_end_budget_meter_reflects_actuals_across_two_calls() -> None:
    """Two sequential calls within the same iteration share the
    budget meter and the second call sees the credit-back from the
    first."""
    budget = IterationTokenBudget(itpm_limit=10_000, otpm_limit=10_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        envelope1 = '{"result": {"echoed": "first"}, "declined": null}'
        envelope2 = '{"result": {"echoed": "second"}, "declined": null}'

        req1 = _make_request_from_skill(call_id="b1", user_prompt="first")
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_with(
                envelope1, prompt_tokens=40, completion_tokens=12,
            ),
        ):
            LlmReasoningCall().invoke(w=None, request=req1)

        assert budget.actual_input_tokens == 40
        assert budget.actual_output_tokens == 12

        req2 = _make_request_from_skill(call_id="b2", user_prompt="second")
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_with(
                envelope2, prompt_tokens=35, completion_tokens=10,
            ),
        ):
            LlmReasoningCall().invoke(w=None, request=req2)

        assert budget.actual_input_tokens == 75
        assert budget.actual_output_tokens == 22
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_budget_abstain_when_second_call_overflows() -> None:
    """First call uses most of the budget; second call's max_tokens
    would push over → typed budget abstain, OpenAI never called."""
    budget = IterationTokenBudget(itpm_limit=10_000, otpm_limit=300)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        envelope = '{"result": {"echoed": "x"}, "declined": null}'
        req1 = _make_request_from_skill(call_id="o1", user_prompt="first")
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_with(
                envelope, prompt_tokens=5, completion_tokens=12,
            ),
        ):
            LlmReasoningCall().invoke(w=None, request=req1)
        budget.reserve(input_tokens=0, max_output_tokens=200)

        req2 = _make_request_from_skill(call_id="o2", user_prompt="second")
        stub_never_called = _stub_with(envelope)
        with patch.object(
            optimizer, "_get_openai_client", return_value=stub_never_called,
        ):
            resp = LlmReasoningCall().invoke(w=None, request=req2)

        assert resp.succeeded is False
        assert resp.declined is not None
        assert resp.declined.reason == AbstainReason.CONTEXT_TOKEN_BUDGET_EXCEEDED
        assert stub_never_called.chat.completions.create.call_count == 0
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)
