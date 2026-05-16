"""Tests for the response_model extension of _traced_llm_call.

Plan reference: docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.optimizer import _traced_llm_call
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _Example(LLMOutputContract):
    description: str
    synonyms: list[str] = []


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_response_model_sets_response_format_on_openai_call(mock_client_factory):
    """When response_model is passed, response_format must be injected
    into the OpenAI call kwargs."""
    fake_client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"description": "x", "synonyms": []}'))]
    fake_client.chat.completions.create.return_value = fake_resp
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(
        None, "sys", "prompt", span_name="t",
        response_model=_Example,
    )
    create_kwargs = fake_client.chat.completions.create.call_args.kwargs
    assert "response_format" in create_kwargs
    rf = create_kwargs["response_format"]
    assert rf["type"] == "json_schema"
    assert rf["json_schema"]["name"] == "_Example"
    assert rf["json_schema"]["strict"] is True


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_response_model_retries_on_invalid_json_response(mock_client_factory):
    """If the LLM somehow returns malformed JSON despite response_format,
    the response_validator path must retry."""
    fake_client = MagicMock()
    bad_resp = MagicMock()
    bad_resp.choices = [MagicMock(message=MagicMock(content="this is not json"))]
    good_resp = MagicMock()
    good_resp.choices = [MagicMock(message=MagicMock(content='{"description": "x"}'))]
    fake_client.chat.completions.create.side_effect = [bad_resp, good_resp]
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(
        None, "sys", "prompt", span_name="t",
        response_model=_Example, max_retries=3,
    )
    assert fake_client.chat.completions.create.call_count == 2
    assert "description" in text


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_no_response_model_preserves_legacy_behaviour(mock_client_factory):
    """When response_model is None (the default), no response_format is
    sent — preserving every existing callsite."""
    fake_client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content="plain text"))]
    fake_client.chat.completions.create.return_value = fake_resp
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(None, "sys", "prompt", span_name="t")
    create_kwargs = fake_client.chat.completions.create.call_args.kwargs
    assert "response_format" not in create_kwargs
    assert text == "plain text"


def test_call_llm_for_proposal_accepts_response_model_kwarg():
    """Smoke test — _call_llm_for_proposal must accept response_model
    as a keyword argument so Stage-2 callsites can opt into structured
    outputs. The legacy path (response_model=None) is unchanged."""
    import inspect
    from genie_space_optimizer.optimization.optimizer import _call_llm_for_proposal
    sig = inspect.signature(_call_llm_for_proposal)
    assert "response_model" in sig.parameters
    assert sig.parameters["response_model"].default is None
