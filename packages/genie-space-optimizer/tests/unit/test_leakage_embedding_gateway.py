"""Site-4 (leakage.get_embedding) AI-Gateway routing tests."""
from __future__ import annotations

import json
from types import SimpleNamespace

from genie_space_optimizer.optimization import leakage


def _wc():
    return SimpleNamespace(config=SimpleNamespace(
        host="https://example.databricks.com/",
        authenticate=lambda: {"Authorization": "Bearer tok"},
    ))


def test_classic_uses_sdk_query_verbatim(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    called = {}

    def fake_post(*a, **k):
        called["posted"] = True
        return SimpleNamespace(status_code=200, json=lambda: {"data": [{"embedding": [9.9]}]})

    monkeypatch.setattr(leakage.httpx, "post", fake_post)
    w = SimpleNamespace(serving_endpoints=SimpleNamespace(
        query=lambda name, input: SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2, 0.3])])))
    out = leakage.get_embedding("hello", w, endpoint="databricks-bge-large-en")
    assert out == [0.1, 0.2, 0.3]          # from the SDK, not httpx's 9.9
    assert "posted" not in called          # classic never touches httpx


def test_gateway_posts_maps_and_tags(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json, headers=headers)
        return SimpleNamespace(status_code=200, json=lambda: {"data": [{"embedding": [0.5, 0.6]}]})

    monkeypatch.setattr(leakage.httpx, "post", fake_post)
    out = leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en")
    assert out == [0.5, 0.6]
    assert captured["url"] == "https://example.databricks.com/ai-gateway/mlflow/v1/embeddings"
    assert captured["json"] == {"model": "system.ai.bge-large-en", "input": ["q"]}
    assert captured["headers"]["Authorization"] == "Bearer tok"
    tags = json.loads(captured["headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "leakage-embed"}


def test_gateway_non_200_returns_none(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setattr(leakage.httpx, "post",
                        lambda *a, **k: SimpleNamespace(status_code=404, json=lambda: {}))
    assert leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en") is None


def test_gateway_malformed_body_returns_none(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setattr(leakage.httpx, "post",
                        lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {}))
    assert leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en") is None


def test_gateway_component_override_tags_mv_suggest(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json, headers=headers)
        return SimpleNamespace(status_code=200, json=lambda: {"data": [{"embedding": [0.5, 0.6]}]})

    monkeypatch.setattr(leakage.httpx, "post", fake_post)
    out = leakage.get_embedding("q", _wc(), endpoint="databricks-gte-large-en",
                                component="mv-suggest")
    assert out == [0.5, 0.6]
    assert captured["json"]["model"] == "system.ai.gte-large-en"
    tags = json.loads(captured["headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags["component"] == "mv-suggest"


def test_gateway_exception_returns_none(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")

    def boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(leakage.httpx, "post", boom)
    assert leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en") is None


def test_preflight_under_gateway(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setattr(leakage.httpx, "post",
                        lambda *a, **k: SimpleNamespace(status_code=200,
                                                        json=lambda: {"data": [{"embedding": [1.0]}]}))
    assert leakage.preflight_embedding_endpoint(_wc(), endpoint="databricks-bge-large-en") is True
