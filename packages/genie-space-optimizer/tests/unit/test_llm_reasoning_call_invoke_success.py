"""Plan 2 Task 8 — LlmReasoningCall.invoke success path."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _EchoOutput(LLMOutputContract):
    echoed: str


def _stub_openai_client_with_envelope(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=42, completion_tokens=18, total_tokens=60,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_invoke_success_returns_typed_response_with_parsed_output() -> None:
    req = LlmReasoningRequest(
        call_id="call_001",
        skill_id="_reference-smoke-test",
        system_msg="echo system",
        user_prompt="echo user",
        result_cls=_EchoOutput,
        max_tokens=100,
    )
    envelope = '{"result": {"echoed": "hello"}, "declined": null}'
    with patch.object(
        optimizer,
        "_get_openai_client",
        return_value=_stub_openai_client_with_envelope(envelope),
    ):
        resp = LlmReasoningCall().invoke(w=None, request=req)

    assert isinstance(resp, LlmReasoningResponse)
    assert resp.succeeded is True
    assert resp.parsed_output == {"echoed": "hello"}
    assert resp.declined is None
    assert resp.error is None
    assert resp.tokens_input == 42
    assert resp.tokens_output == 18
    assert resp.raw_text.strip() == envelope


def test_invoke_dispatches_through_traced_llm_call() -> None:
    """The runner MUST route through optimizer._traced_llm_call."""
    req = LlmReasoningRequest(
        call_id="call_002",
        skill_id="_reference-smoke-test",
        system_msg="s",
        user_prompt="u",
        result_cls=_EchoOutput,
        max_tokens=100,
    )
    envelope = '{"result": {"echoed": "ok"}, "declined": null}'
    seen: list[dict] = []
    real_traced = optimizer._traced_llm_call

    def _spy(*args, **kwargs):
        seen.append({"args": args, "kwargs": kwargs})
        return real_traced(*args, **kwargs)

    with patch.object(
        optimizer,
        "_get_openai_client",
        return_value=_stub_openai_client_with_envelope(envelope),
    ):
        with patch.object(optimizer, "_traced_llm_call", side_effect=_spy):
            LlmReasoningCall().invoke(w=None, request=req)

    assert len(seen) == 1
    assert seen[0]["kwargs"]["span_name"] == "reasoning_call._reference-smoke-test"
    assert seen[0]["kwargs"]["max_tokens"] == 100


# Plan 8 Task 11 — model_override removed from LlmReasoningRequest;
# the per-skill override test is retired with the field.
