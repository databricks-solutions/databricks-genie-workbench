"""Unit tests for the _LLM_CALLER_OVERRIDE injection at _traced_llm_call."""
from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.optimizer import (
    _LLM_CALLER_OVERRIDE,
    _traced_llm_call,
)


class _StubOverride:
    def __init__(self, response_text: str):
        self._response_text = response_text
        self.calls: list[dict] = []

    def call(
        self,
        *,
        w: Any,
        system_msg: str,
        prompt: str,
        span_name: str,
        max_retries: int,
        temperature: float,
        max_tokens: int | None,
        response_validator,
        response_format,
        response_model,
    ) -> tuple[str, Any]:
        self.calls.append({
            "span_name": span_name,
            "prompt": prompt,
            "system_msg": system_msg,
        })
        return (self._response_text, {"stub": True})


def test_traced_llm_call_short_circuits_when_override_is_set():
    override = _StubOverride(response_text="stubbed")
    token = _LLM_CALLER_OVERRIDE.set(override)
    try:
        text, resp = _traced_llm_call(
            w=None,
            system_msg="sys",
            prompt="hello",
            span_name="adaptive_strategy",
        )
    finally:
        _LLM_CALLER_OVERRIDE.reset(token)

    assert text == "stubbed"
    assert resp == {"stub": True}
    assert override.calls == [
        {"span_name": "adaptive_strategy", "prompt": "hello", "system_msg": "sys"},
    ]


def test_traced_llm_call_default_path_unchanged_when_override_unset():
    # Sanity: when no override is set, the ContextVar is None and the
    # legacy code path executes (we don't invoke the real LLM in this
    # test — only assert the default ContextVar state).
    assert _LLM_CALLER_OVERRIDE.get() is None
