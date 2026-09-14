"""Site-3 (GSO call_llm) AI-Gateway routing tests."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import llm_client


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    llm_client._openai_client_cache.clear()
    monkeypatch.setattr(llm_client, "get_llm_endpoint", lambda: "databricks-claude-sonnet-4-6")
    yield
    llm_client._openai_client_cache.clear()


def _fake_wc():
    return SimpleNamespace(config=SimpleNamespace(
        host="https://example.databricks.com/", token="tok",
        authenticate=lambda: {"Authorization": "Bearer tok"}))


def _fake_openai_client(monkeypatch):
    completions = MagicMock()
    completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions), api_key="tok")
    monkeypatch.setattr(llm_client, "get_openai_client", lambda w: client)
    return completions


def test_get_openai_client_base_url_by_route(monkeypatch):
    import openai
    seen = {}

    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None):
            seen["base_url"] = base_url
            self.api_key = api_key

    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    monkeypatch.setattr(llm_client, "_resolve_bearer_token", lambda wc: "tok")

    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    llm_client.get_openai_client(_fake_wc())
    assert seen["base_url"] == "https://example.databricks.com/serving-endpoints"

    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    llm_client.get_openai_client(_fake_wc())
    assert seen["base_url"] == "https://example.databricks.com/ai-gateway/mlflow/v1"


def test_call_llm_classic_no_headers_unmapped_model(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    completions = _fake_openai_client(monkeypatch)
    llm_client.call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}])
    kwargs = completions.create.call_args.kwargs
    assert kwargs["model"] == "databricks-claude-sonnet-4-6"
    assert "extra_headers" not in kwargs


def test_call_llm_gateway_maps_model_and_tags_with_run_id(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setenv("GSO_RUN_ID", "run-42")
    completions = _fake_openai_client(monkeypatch)
    llm_client.call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}])
    kwargs = completions.create.call_args.kwargs
    assert kwargs["model"] == "system.ai.claude-sonnet-4-6"
    tags = json.loads(kwargs["extra_headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "gso-optimize", "run_id": "run-42"}


def test_call_llm_gateway_tags_without_run_id(monkeypatch):
    """Absent GSO_RUN_ID: tags carry no run_id key (empty scoped extras dropped)."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.delenv("GSO_RUN_ID", raising=False)
    completions = _fake_openai_client(monkeypatch)
    llm_client.call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}])
    kwargs = completions.create.call_args.kwargs
    tags = json.loads(kwargs["extra_headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "gso-optimize"}
