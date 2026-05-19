"""Plan 2 Task 10 — invoke continues to honour _LLM_CALLER_OVERRIDE
(tape replay) and _LLM_CALL_RECORDER (capture).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_call_recorder import (
    InMemoryLLMCallRecorder,
    RecorderBinding,
    _RECORDER_BINDING,
)
from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _EchoOutput(LLMOutputContract):
    echoed: str


def test_invoke_honours_llm_caller_override_for_tape_replay() -> None:
    """When the override is installed, invoke must NOT call the real
    OpenAI client. The override returns canned (text, response)
    tuples."""
    replay_text = '{"result": {"echoed": "from-tape"}, "declined": null}'

    class _TapeOverride:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def call(self, **kwargs):
            self.calls.append(kwargs)
            response = MagicMock(name="TapeResponse")
            response.usage = None
            return replay_text, response

    override = _TapeOverride()
    override_token = optimizer._LLM_CALLER_OVERRIDE.set(override)
    real_client_stub = MagicMock(name="RealClientShouldNotBeCalled")
    try:
        with patch.object(
            optimizer, "_get_openai_client", return_value=real_client_stub,
        ):
            req = LlmReasoningRequest(
                call_id="c",
                skill_id="_reference-smoke-test",
                system_msg="s",
                user_prompt="u",
                result_cls=_EchoOutput,
                max_tokens=100,
            )
            resp = LlmReasoningCall().invoke(w=None, request=req)
    finally:
        optimizer._LLM_CALLER_OVERRIDE.reset(override_token)

    assert resp.succeeded is True
    assert resp.parsed_output == {"echoed": "from-tape"}
    assert real_client_stub.chat.completions.create.call_count == 0
    assert len(override.calls) == 1
    assert override.calls[0]["span_name"] == "reasoning_call._reference-smoke-test"


def test_invoke_real_path_records_into_llm_call_recorder() -> None:
    """When the override is NOT installed, the production recorder
    must capture the call."""
    rec = InMemoryLLMCallRecorder()
    rec_token = optimizer._LLM_CALL_RECORDER.set(rec)
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=2, ag_id="AG_1", cluster_id="H001"),
    )
    envelope = '{"result": {"echoed": "ok"}, "declined": null}'
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=20, completion_tokens=10, total_tokens=30,
    )
    client.chat.completions.create.return_value = completion
    try:
        with patch.object(
            optimizer, "_get_openai_client", return_value=client,
        ):
            req = LlmReasoningRequest(
                call_id="c",
                skill_id="_reference-smoke-test",
                system_msg="sys-msg",
                user_prompt="user-prompt-text",
                result_cls=_EchoOutput,
                max_tokens=100,
            )
            LlmReasoningCall().invoke(w=None, request=req)
    finally:
        optimizer._LLM_CALL_RECORDER.reset(rec_token)
        _RECORDER_BINDING.reset(binding_token)

    assert len(rec.calls) == 1
    captured = rec.calls[0]
    assert captured["span_name"] == "reasoning_call._reference-smoke-test"
    assert captured["iteration"] == 2
    assert captured["ag_id"] == "AG_1"
    assert captured["cluster_id"] == "H001"
    assert captured["prompt"] == "user-prompt-text"
    assert captured["system_msg"] == "sys-msg"
