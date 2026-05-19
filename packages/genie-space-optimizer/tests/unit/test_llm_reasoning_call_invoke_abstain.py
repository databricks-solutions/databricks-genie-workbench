"""Plan 2 Task 9 — LlmReasoningCall.invoke abstain + error paths."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _EchoOutput(LLMOutputContract):
    echoed: str


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=10, completion_tokens=5, total_tokens=15,
    )
    client.chat.completions.create.return_value = completion
    return client


def _make_req(**overrides) -> LlmReasoningRequest:
    base = dict(
        call_id="c",
        skill_id="_reference-smoke-test",
        system_msg="s",
        user_prompt="u",
        result_cls=_EchoOutput,
        max_tokens=100,
    )
    base.update(overrides)
    return LlmReasoningRequest(**base)


def test_invoke_returns_declined_when_llm_chose_abstain() -> None:
    declined_envelope = """{
        "result": null,
        "declined": {
            "reason": "missing_schema_context",
            "explanation": "no metadata",
            "needed_evidence": ["table_metadata"],
            "suggested_next_step": "re_dispatch"
        }
    }"""
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(declined_envelope),
    ):
        resp = LlmReasoningCall().invoke(w=None, request=_make_req())
    assert resp.succeeded is False
    assert resp.parsed_output is None
    assert resp.declined is not None
    assert resp.declined.reason == AbstainReason.MISSING_SCHEMA_CONTEXT
    assert resp.declined.needed_evidence == ("table_metadata",)
    assert resp.error is None


def test_invoke_mints_budget_overflow_abstain_before_dispatch() -> None:
    """When the per-iteration meter would be crossed, the runner must
    mint a typed abstain WITHOUT touching the OpenAI client."""
    budget = IterationTokenBudget(itpm_limit=100, otpm_limit=100)
    budget.reserve(input_tokens=95, max_output_tokens=95)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        req = _make_req(max_tokens=100)
        stub = _stub_with('{"result": {"echoed": "x"}, "declined": null}')
        with patch.object(
            optimizer, "_get_openai_client", return_value=stub,
        ):
            resp = LlmReasoningCall().invoke(w=None, request=req)
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)

    assert resp.succeeded is False
    assert resp.declined is not None
    assert resp.declined.reason == AbstainReason.CONTEXT_TOKEN_BUDGET_EXCEEDED
    assert stub.chat.completions.create.call_count == 0


def test_invoke_propagates_http_failure_as_typed_error() -> None:
    """When _traced_llm_call raises, the runner returns succeeded=False
    with the error message captured."""
    req = _make_req()

    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")

    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        resp = LlmReasoningCall().invoke(w=None, request=req)

    assert resp.succeeded is False
    assert resp.parsed_output is None
    assert resp.declined is None
    assert resp.error is not None
    assert "429" in resp.error
    assert "RuntimeError" in resp.error


def test_invoke_propagates_envelope_parse_failure_as_typed_error() -> None:
    """When the LLM returns content that fails envelope validation,
    the runner classifies it as an error (NOT an abstain).

    Validation can fire from two layers: ``_traced_llm_call``'s
    response_model validator (after retries exhaust) OR the runner's
    own ``parse_envelope`` post-extraction. Either way the response
    error names the envelope contract.
    """
    malformed_envelope = '{"this": "is not envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(malformed_envelope),
    ):
        resp = LlmReasoningCall().invoke(w=None, request=_make_req())

    assert resp.succeeded is False
    assert resp.parsed_output is None
    assert resp.declined is None
    assert resp.error is not None
    assert (
        "EnvelopeContractError" in resp.error
        or "AbstainableEnvelope" in resp.error
    )
