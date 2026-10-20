"""GSO LLM and embedding clients route through Unity Gateway (AI Gateway v3)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from genie_space_optimizer.optimization import leakage, llm_client


def test_openai_client_uses_unity_gateway_base_url(monkeypatch):
    captured: dict = {}

    class FakeOpenAI:
        def __init__(self, *, api_key, base_url):
            captured["api_key"] = api_key
            captured["base_url"] = base_url

    monkeypatch.setattr("openai.OpenAI", FakeOpenAI)
    monkeypatch.setattr(llm_client, "_openai_client_cache", {})
    ws = SimpleNamespace(
        config=SimpleNamespace(host="https://example.cloud.databricks.com/", token="tok")
    )

    llm_client.get_openai_client(ws)

    assert captured == {
        "api_key": "tok",
        "base_url": "https://example.cloud.databricks.com/ai-gateway/mlflow/v1",
    }


def test_get_embedding_posts_to_unity_gateway_with_normalized_model():
    ws = MagicMock()
    ws.api_client.do.return_value = {"data": [{"embedding": [0.5, 1]}]}

    emb = leakage.get_embedding("hello", ws, endpoint="databricks-bge-large-en")

    assert emb == [0.5, 1.0]
    ws.api_client.do.assert_called_once_with(
        "POST",
        "/ai-gateway/mlflow/v1/embeddings",
        body={"model": "system.ai.bge-large-en", "input": ["hello"]},
    )


def test_get_embedding_default_model_is_system_ai():
    assert leakage.EMBEDDING_ENDPOINT == "system.ai.bge-large-en"


def test_get_embedding_returns_none_on_gateway_error():
    ws = MagicMock()
    ws.api_client.do.side_effect = RuntimeError("404 NOT_FOUND")

    assert leakage.get_embedding("hello", ws) is None


def test_get_embedding_skips_blank_text():
    ws = MagicMock()

    assert leakage.get_embedding("   ", ws) is None
    ws.api_client.do.assert_not_called()
