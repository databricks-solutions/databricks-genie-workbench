"""Unit tests for the single wheel-native LLM client (MV-D65).

Covers ``get_openai_client`` (host-cache + per-call token refresh + injected
identity) and ``call_llm_core`` (retry/backoff, response_format-reject fallback,
content-block normalization, and model resolution). No network, no real
``WorkspaceClient``, no ``openai`` transport — everything is faked.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from genie_space_optimizer.common import llm as common_llm
from genie_space_optimizer.common.config import LLM_MAX_RETRIES


def _fake_wc(token: str = "tok-1", host: str = "https://ws.example.com"):
    cfg = SimpleNamespace(
        host=host,
        token=token,
        authenticate=lambda: {"Authorization": f"Bearer {token}"},
    )
    return SimpleNamespace(config=cfg)


class _FakeOpenAI:
    instances: list["_FakeOpenAI"] = []

    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key
        self.base_url = base_url
        _FakeOpenAI.instances.append(self)


def _resp(content):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


class _FakeCompletions:
    def __init__(self, script: list):
        self.script = list(script)
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _fake_client(script: list):
    comp = _FakeCompletions(script)
    return SimpleNamespace(chat=SimpleNamespace(completions=comp)), comp


# ── get_openai_client ─────────────────────────────────────────────────────────


def test_get_openai_client_refreshes_token_and_caches_by_host(monkeypatch):
    common_llm._openai_client_cache.clear()
    _FakeOpenAI.instances.clear()
    monkeypatch.setattr("openai.OpenAI", _FakeOpenAI)

    c1 = common_llm.get_openai_client(_fake_wc(token="tok-1"))
    c2 = common_llm.get_openai_client(_fake_wc(token="tok-2"))

    assert c1 is c2                              # one client per host (cached)
    assert len(_FakeOpenAI.instances) == 1
    assert c2.api_key == "tok-2"                 # bearer token refreshed every call
    common_llm._openai_client_cache.clear()


def test_get_openai_client_uses_injected_w_no_implicit_workspaceclient(monkeypatch):
    common_llm._openai_client_cache.clear()
    _FakeOpenAI.instances.clear()
    monkeypatch.setattr("openai.OpenAI", _FakeOpenAI)

    def _boom(*a, **k):
        raise AssertionError("WorkspaceClient() must not be constructed when w is injected")

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _boom)

    client = common_llm.get_openai_client(_fake_wc(token="tok-injected"))
    assert client.api_key == "tok-injected"
    assert client.base_url == "https://ws.example.com/serving-endpoints"
    common_llm._openai_client_cache.clear()


# ── call_llm_core ─────────────────────────────────────────────────────────────


def test_call_llm_core_retries_transient_then_succeeds(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client, comp = _fake_client([RuntimeError("503 transient"), _resp("ok")])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    text, _ = common_llm.call_llm_core(None, messages=[{"role": "user", "content": "hi"}], model="m")

    assert text == "ok"
    assert len(comp.calls) == 2  # one transient failure, one success


def test_call_llm_core_raises_after_exhausting_retries(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client, comp = _fake_client([RuntimeError(f"e{i}") for i in range(LLM_MAX_RETRIES)])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    with pytest.raises(RuntimeError):
        common_llm.call_llm_core(None, messages=[{"role": "user", "content": "hi"}], model="m")

    assert len(comp.calls) == LLM_MAX_RETRIES


def test_call_llm_core_response_format_reject_retries_without(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client, comp = _fake_client([ValueError("response_format not supported"), _resp("ok")])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    text, _ = common_llm.call_llm_core(
        None, messages=[{"role": "user", "content": "hi"}], model="m",
        response_format={"type": "json_object"},
    )

    assert text == "ok"
    assert len(comp.calls) == 2
    assert "response_format" in comp.calls[0]       # first attempt carried it
    assert "response_format" not in comp.calls[1]   # retried without it after reject


def test_call_llm_core_normalizes_structured_content_blocks(monkeypatch):
    client, comp = _fake_client([
        _resp([{"type": "text", "text": '{"a":'}, {"type": "text", "text": " 1}"}])
    ])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    text, _ = common_llm.call_llm_core(None, messages=[{"role": "user", "content": "hi"}], model="m")
    assert text == '{"a": 1}'


def test_call_llm_core_model_none_resolves_endpoint(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "databricks-my-endpoint")
    monkeypatch.delenv("GSO_LLM_ENDPOINT", raising=False)
    client, comp = _fake_client([_resp("ok")])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    common_llm.call_llm_core(None, messages=[{"role": "user", "content": "hi"}])
    assert comp.calls[0]["model"] == "databricks-my-endpoint"


def test_call_llm_core_honors_explicit_model(monkeypatch):
    client, comp = _fake_client([_resp("ok")])
    monkeypatch.setattr(common_llm, "get_openai_client", lambda w: client)

    common_llm.call_llm_core(None, messages=[{"role": "user", "content": "hi"}], model="pinned-model")
    assert comp.calls[0]["model"] == "pinned-model"


def test_call_llm_core_passes_injected_w_through(monkeypatch):
    seen = {}
    client, _comp = _fake_client([_resp("ok")])

    def _get(w):
        seen["w"] = w
        return client

    monkeypatch.setattr(common_llm, "get_openai_client", _get)
    sentinel = object()
    common_llm.call_llm_core(sentinel, messages=[{"role": "user", "content": "hi"}], model="m")
    assert seen["w"] is sentinel
