"""PR-2C — runtime pre-flight in ``_traced_llm_call`` refuses to
dispatch a malformed envelope.

The dc89d1a9 / 98ec8950 trial-postmortem failure surface was: every
Plan 11 Stage 1 call dispatched an envelope whose
``response_format.json_schema.name`` violated the Databricks endpoint
regex, the endpoint returned 400, and the retry loop burned every
attempt against the same deterministically-broken request.

After PR-2C: ``_traced_llm_call`` consults
``DatabricksEndpointRequestContract.validate`` BEFORE
``client.chat.completions.create``. On any violation it raises
``RequestEnvelopeInvalidError`` and BREAKS out of the retry loop —
never re-attempts, never hits the wire.

These tests stub the OpenAI client and assert both invariants:
  * The exception is raised with the expected class name.
  * ``client.chat.completions.create`` is not called.
"""
from __future__ import annotations

from typing import Any

import pytest

from genie_space_optimizer.optimization.databricks_request_contract import (
    RequestEnvelopeInvalidError,
)
from genie_space_optimizer.optimization import optimizer


class _RecordingChatCompletions:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []

    def create(self, **kwargs):  # noqa: ANN003 — mirroring SDK signature
        self.create_calls.append(kwargs)

        class _Resp:
            class _Choice:
                class _Message:
                    content = '{"declined": {"reason": "stub"}}'

                message = _Message()

            choices = [_Choice()]
            usage = type("U", (), {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            })()

        return _Resp()


class _RecordingChat:
    def __init__(self) -> None:
        self.completions = _RecordingChatCompletions()


class _StubOpenAIClient:
    def __init__(self) -> None:
        self.chat = _RecordingChat()


def _install_stub_client(monkeypatch) -> _StubOpenAIClient:
    stub = _StubOpenAIClient()
    monkeypatch.setattr(optimizer, "_get_openai_client", lambda _w: stub)
    return stub


def _bad_response_format() -> dict[str, Any]:
    """Return a response_format whose json_schema.name violates the
    Databricks endpoint regex (the dc89d1a9 / 98ec8950 root cause)."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "AbstainableEnvelope[Plan11DiagnoseOutput]",
            "schema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "strict": True,
        },
    }


def _good_response_format() -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "AbstainableEnvelope_Plan11DiagnoseOutput",
            "schema": {
                "type": "object",
                "properties": {
                    "declined": {
                        "type": "object",
                        "properties": {"reason": {"type": "string"}},
                        "additionalProperties": False,
                    },
                },
                "additionalProperties": False,
            },
            "strict": True,
        },
    }


def test_preflight_raises_request_envelope_invalid_on_bad_name(monkeypatch) -> None:
    stub = _install_stub_client(monkeypatch)
    with pytest.raises(RequestEnvelopeInvalidError) as exc_info:
        optimizer._traced_llm_call(
            w=None,
            system_msg="sys",
            prompt="prompt",
            span_name="plan11_diagnose",
            response_format=_bad_response_format(),
            max_tokens=4096,
            max_retries=3,
        )
    err = exc_info.value
    assert any(
        v.field == "response_format.json_schema.name" for v in err.violations
    )
    # CRITICAL invariant: the wire was never touched. Burning retries
    # against a deterministically-broken envelope is exactly the
    # failure mode PR-2C exists to prevent.
    assert stub.chat.completions.create_calls == []


def test_preflight_does_not_block_well_formed_envelope(monkeypatch) -> None:
    """A correctly-sanitized name passes pre-flight and the call
    proceeds to ``client.chat.completions.create``."""
    stub = _install_stub_client(monkeypatch)
    text, resp = optimizer._traced_llm_call(
        w=None,
        system_msg="sys",
        prompt="prompt",
        span_name="plan11_diagnose",
        response_format=_good_response_format(),
        max_tokens=4096,
        max_retries=1,
    )
    assert "declined" in text
    assert len(stub.chat.completions.create_calls) == 1
    sent = stub.chat.completions.create_calls[0]
    assert sent["response_format"] == _good_response_format()


def test_preflight_violation_is_routed_to_request_envelope_invalid_error_kind() -> None:
    """End-to-end through the classifier: a
    ``RequestEnvelopeInvalidError`` class name surfaces as
    ``error_kind="request_envelope_invalid"`` (PR-1C arm). This
    mirrors the path ``LlmReasoningCall.invoke`` →
    ``_format_provider_error`` → ``diagnose._classify_llm_error``
    takes in production."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        _classify_llm_error,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    req = LlmReasoningRequest(
        call_id="t",
        skill_id="plan11_diagnose",
        system_msg="s",
        user_prompt="u",
        result_cls=type("X", (), {}),
        max_tokens=100,
    )
    assert (
        _classify_llm_error(
            "RequestEnvelopeInvalidError",
            "constraint_violations=[response_format.json_schema.name|"
            "must match ^[a-zA-Z0-9_-]{1,128}$]",
            0,
            req,
        )
        == "request_envelope_invalid"
    )
