from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import llm_client


@pytest.fixture(autouse=True)
def _reset_client_cache():
    llm_client._openai_client_cache.clear()
    yield
    llm_client._openai_client_cache.clear()


def _fake_workspace_client():
    config = SimpleNamespace(
        host="https://example.databricks.com",
        token="dapi-test-token",
        authenticate=lambda: {"Authorization": "Bearer dapi-test-token"},
    )
    return SimpleNamespace(config=config)


def test_call_llm_passes_timeout_to_openai_client(monkeypatch):
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))]
    )
    fake_completions = MagicMock()
    fake_completions.create.return_value = fake_response
    fake_client = SimpleNamespace(
        chat=SimpleNamespace(completions=fake_completions),
        api_key="dapi-test-token",
    )
    monkeypatch.setattr(llm_client, "get_openai_client", lambda w: fake_client)

    content, _ = llm_client.call_llm(
        _fake_workspace_client(),
        messages=[{"role": "user", "content": "hi"}],
    )

    assert content == "ok"
    kwargs = fake_completions.create.call_args.kwargs
    assert "timeout" in kwargs, "OpenAI request must carry an explicit timeout"
    assert kwargs["timeout"] == llm_client.eval_llm_timeout_seconds()


@pytest.mark.parametrize(
    ("structured_content", "expected"),
    [
        (
            [
                {"type": "text", "text": '{"findings":'},
                {"type": "text", "text": " []}"},
            ],
            '{"findings": []}',
        ),
        (
            [
                SimpleNamespace(type="text", text="first "),
                SimpleNamespace(type="text", text="block"),
            ],
            "first block",
        ),
    ],
)
def test_call_llm_normalizes_structured_content_blocks(
    monkeypatch,
    structured_content,
    expected,
):
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=structured_content))]
    )
    fake_completions = MagicMock()
    fake_completions.create.return_value = fake_response
    fake_client = SimpleNamespace(
        chat=SimpleNamespace(completions=fake_completions),
        api_key="dapi-test-token",
    )
    monkeypatch.setattr(llm_client, "get_openai_client", lambda w: fake_client)

    content, _ = llm_client.call_llm(
        _fake_workspace_client(),
        messages=[{"role": "user", "content": "hi"}],
    )

    assert content == expected


def test_eval_llm_timeout_seconds_respects_env(monkeypatch):
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_EVAL_LLM_TIMEOUT_SECONDS", "120")
    assert llm_client.eval_llm_timeout_seconds() == 120


def test_eval_llm_timeout_seconds_defaults_to_600(monkeypatch):
    monkeypatch.delenv("GENIE_SPACE_OPTIMIZER_EVAL_LLM_TIMEOUT_SECONDS", raising=False)
    assert llm_client.eval_llm_timeout_seconds() == 600


def test_eval_llm_timeout_seconds_floors_invalid_values(monkeypatch):
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_EVAL_LLM_TIMEOUT_SECONDS", "5")
    assert llm_client.eval_llm_timeout_seconds() == 30  # min floor
