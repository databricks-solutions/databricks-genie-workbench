"""Phase 3.5 Task 2 — _traced_llm_call calls the recorder after success.

This test does not call the real LLM; it stubs the OpenAI client at
the module level so we observe the recorder hook deterministically.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_call_recorder import (
    InMemoryLLMCallRecorder,
    RecorderBinding,
    _RECORDER_BINDING,
)


def _stub_openai_client_with_completion(text: str):
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = text
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=10, completion_tokens=20, total_tokens=30,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_recorder_records_real_call_with_binding():
    rec = InMemoryLLMCallRecorder()
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=4, ag_id="AG_77", cluster_id="H001"),
    )
    rec_token = optimizer._LLM_CALL_RECORDER.set(rec)
    try:
        with patch.object(
            optimizer,
            "_get_openai_client",
            return_value=_stub_openai_client_with_completion('{"x": 1}'),
        ):
            text, _resp = optimizer._traced_llm_call(
                w=None,
                system_msg="sys",
                prompt="prompt-text",
                span_name="stage_1_discovery",
                max_retries=1,
            )
        assert text.strip() == '{"x": 1}'
    finally:
        optimizer._LLM_CALL_RECORDER.reset(rec_token)
        _RECORDER_BINDING.reset(binding_token)

    assert len(rec.calls) == 1
    call = rec.calls[0]
    assert call["span_name"] == "stage_1_discovery"
    assert call["iteration"] == 4
    assert call["ag_id"] == "AG_77"
    assert call["cluster_id"] == "H001"
    assert call["prompt"] == "prompt-text"
    assert call["response_text"].strip() == '{"x": 1}'


def test_recorder_does_not_fire_on_override_path():
    """Phase 3 replay must not record — that would create a feedback loop."""
    rec = InMemoryLLMCallRecorder()
    rec_token = optimizer._LLM_CALL_RECORDER.set(rec)

    class FakeOverride:
        def call(self, **kwargs):
            return ("replayed", MagicMock(name="ReplayResponse"))

    override_token = optimizer._LLM_CALLER_OVERRIDE.set(FakeOverride())
    try:
        text, _resp = optimizer._traced_llm_call(
            w=None,
            system_msg="",
            prompt="anything",
            span_name="adaptive_strategy",
            max_retries=1,
        )
        assert text == "replayed"
    finally:
        optimizer._LLM_CALLER_OVERRIDE.reset(override_token)
        optimizer._LLM_CALL_RECORDER.reset(rec_token)

    assert len(rec.calls) == 0


def test_recorder_exception_does_not_crash_real_call():
    class BadRecorder:
        def record(self, **kwargs):
            raise RuntimeError("boom")

    rec_token = optimizer._LLM_CALL_RECORDER.set(BadRecorder())
    try:
        with patch.object(
            optimizer,
            "_get_openai_client",
            return_value=_stub_openai_client_with_completion("ok"),
        ):
            text, _ = optimizer._traced_llm_call(
                w=None,
                system_msg="",
                prompt="",
                span_name="adaptive_strategy",
                max_retries=1,
            )
        assert text.strip() == "ok"
    finally:
        optimizer._LLM_CALL_RECORDER.reset(rec_token)


def test_no_recorder_is_a_complete_no_op():
    """When _LLM_CALL_RECORDER is unset, the real path returns normally."""
    with patch.object(
        optimizer,
        "_get_openai_client",
        return_value=_stub_openai_client_with_completion("hi"),
    ):
        text, _ = optimizer._traced_llm_call(
            w=None,
            system_msg="",
            prompt="",
            span_name="adaptive_strategy",
            max_retries=1,
        )
    assert text.strip() == "hi"
