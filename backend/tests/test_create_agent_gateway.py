"""Site-2 (create_agent streaming) AI-Gateway routing tests."""
import json
from types import SimpleNamespace

import pytest

from backend.services import create_agent


class _Resp:
    def __init__(self, status_code=200, lines=None, text=""):
        self.status_code = status_code
        self.ok = status_code == 200
        self.headers = {}
        self.text = text
        self.encoding = None
        self._lines = lines or []
        self.closed = False

    def iter_lines(self, decode_unicode=True):
        yield from self._lines

    def close(self):
        self.closed = True


class _NoTextResp(_Resp):
    """A 200 streaming response whose ``.text`` explodes if ever touched.

    Guards the SSE-streaming contract: reading ``requests.Response.text`` forces
    ``.content`` (a full ``iter_content`` buffer), which defeats incremental
    streaming. ``_stream_llm`` must never read ``.text`` on a 200 response.
    """

    def __init__(self, lines=None):
        # Do NOT call ``_Resp.__init__`` — it assigns ``self.text``, which the
        # read-only property below would reject. Set the rest by hand.
        self.status_code = 200
        self.ok = True
        self.headers = {}
        self.encoding = None
        self._lines = lines or []
        self.closed = False

    @property
    def text(self):  # type: ignore[override]
        raise AssertionError("resp.text must not be read on a 200 streaming response")


class _Session:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json=None, stream=None, timeout=None, headers=None):
        self.calls.append(SimpleNamespace(url=url, json=json, headers=headers))
        return self._responses.pop(0)


def _agent_with_session(monkeypatch, responses):
    session = _Session(responses)
    client = SimpleNamespace(
        config=SimpleNamespace(host="https://example.databricks.com/"),
        api_client=SimpleNamespace(_api_client=SimpleNamespace(_session=session)),
    )
    monkeypatch.setattr(create_agent, "get_workspace_client", lambda: client)
    return create_agent.CreateGenieAgent(), session


_TOOLS = [{"type": "function", "function": {"name": "noop"}}]
_LINE = 'data: {"choices":[{"delta":{"content":"hi"}}]}'


def test_stream_classic_is_unchanged(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "classic")
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6", space_id="sp1"))
    call = session.calls[0]
    assert call.url == "https://example.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
    assert "model" not in call.json
    assert not call.headers or "Databricks-Ai-Gateway-Request-Tags" not in call.headers


def test_stream_gateway_tags_maps_and_scopes_space(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6", space_id="sp1"))
    call = session.calls[0]
    assert call.url == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
    assert call.json["model"] == "system.ai.claude-sonnet-4-6"
    tags = json.loads(call.headers["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "create-agent", "space_id": "sp1"}


def test_stream_terminates_on_done(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "classic")
    agent, _ = _agent_with_session(monkeypatch, [_Resp(lines=[
        _LINE, "data: [DONE]", 'data: {"choices":[{"delta":{"content":"AFTER"}}]}'])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1                                   # stops at [DONE]


def test_stream_terminates_on_iterlines_end_without_done(monkeypatch):
    """GPT-style ending: no [DONE], finish_reason line then the stream simply ends."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "classic")
    fr = 'data: {"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}]}'
    agent, _ = _agent_with_session(monkeypatch, [_Resp(lines=[fr])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1 and chunks[0]["choices"][0]["finish_reason"] == "stop"


def test_reasoning_model_retries_once_with_none(monkeypatch):
    """First plain attempt 400s on reasoning_effort; retry adds reasoning_effort='none' and succeeds."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    bad = _Resp(status_code=400, text='{"error":"reasoning_effort is required"}')
    good = _Resp(lines=[_LINE, "data: [DONE]"])
    agent, session = _agent_with_session(monkeypatch, [bad, good])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="system.ai.gpt-5-6-sol"))
    assert len(session.calls) == 2
    assert "reasoning_effort" not in session.calls[0].json          # first attempt is plain
    assert session.calls[1].json["reasoning_effort"] == "none"      # retry sends the flag
    assert len(chunks) == 1


def test_claude_style_200_does_not_add_flag(monkeypatch):
    """A model that accepts tools plainly (200) never gets reasoning_effort added."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6"))
    assert len(session.calls) == 1
    assert "reasoning_effort" not in session.calls[0].json


def test_stream_gateway_404_downgrades(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, _ = _agent_with_session(monkeypatch, [
        _Resp(status_code=404, text='{"error_code":"NOT_FOUND","message":"raw"}')])
    with pytest.raises(RuntimeError) as ei:
        list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                               model="databricks-claude-sonnet-4-6", space_id="sp1"))
    assert str(ei.value) == create_agent.MODEL_UNAVAILABLE_MESSAGE


def test_stream_classic_404_keeps_raw_body(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "classic")
    agent, _ = _agent_with_session(monkeypatch, [
        _Resp(status_code=404, text="raw provider body")])
    with pytest.raises(RuntimeError) as ei:
        list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                               model="databricks-claude-sonnet-4-6"))
    assert "LLM endpoint returned 404" in str(ei.value)


def test_stream_gateway_200_never_reads_text(monkeypatch):
    """Guard the Phase-1 SSE lesson: a 200 stream must not touch resp.text."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, session = _agent_with_session(monkeypatch, [_NoTextResp(lines=[_LINE, "data: [DONE]"])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1
    assert chunks[0]["choices"][0]["delta"]["content"] == "hi"
    assert len(session.calls) == 1


@pytest.mark.parametrize("route", ["classic", "gateway"])
def test_stream_never_reads_text_on_200(monkeypatch, route):
    """SSE-streaming contract: a 200 streaming response must be consumed without
    ever reading ``.text`` (which forces ``.content`` and defeats streaming).

    Runs under BOTH the classic and gateway env. ``_NoTextResp.text``
    raises AssertionError if the guard evaluates it on the 200 response.
    """
    monkeypatch.setenv("GENIE_LLM_ROUTE", route)
    agent, session = _agent_with_session(monkeypatch, [_NoTextResp(lines=[_LINE, "data: [DONE]"])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1
    assert chunks[0]["choices"][0]["delta"]["content"] == "hi"
    assert len(session.calls) == 1  # no retry POST fired


_SCOPE_403 = "Invalid scope, required scopes: ai-gateway"


def _scope_rig(monkeypatch, user_first, sp_responses=(), sp_is_user=False):
    agent, user_session = _agent_with_session(monkeypatch, [user_first])
    user_client = create_agent.get_workspace_client()
    sp_session = _Session(list(sp_responses))
    sp_client = user_client if sp_is_user else SimpleNamespace(
        config=SimpleNamespace(host="https://example.databricks.com/"),
        api_client=SimpleNamespace(_api_client=SimpleNamespace(_session=sp_session)),
    )
    monkeypatch.setattr(create_agent, "get_service_principal_client", lambda: sp_client)
    return agent, user_session, sp_session


def test_stream_gateway_scope_403_retries_once_as_the_service_principal(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    first = _Resp(status_code=403, text=_SCOPE_403)
    agent, user_session, sp_session = _scope_rig(
        monkeypatch, first, [_NoTextResp(lines=[_LINE, "data: [DONE]"])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6", space_id="sp1"))
    assert [c["choices"][0]["delta"]["content"] for c in chunks] == ["hi"]
    assert first.closed
    assert len(user_session.calls) == 1 and len(sp_session.calls) == 1
    retry = sp_session.calls[0]
    assert retry.url.endswith("/ai-gateway/mlflow/v1/chat/completions")
    assert json.loads(retry.headers["Databricks-Ai-Gateway-Request-Tags"])["space_id"] == "sp1"


@pytest.mark.parametrize(
    ("route", "status", "text", "sp_is_user"),
    [
        ("gateway", 403, "PERMISSION_DENIED: not allowed", False),
        ("classic", 403, _SCOPE_403, False),
        ("gateway", 401, _SCOPE_403, False),
        ("gateway", 403, _SCOPE_403, True),
    ],
)
def test_stream_no_service_principal_retry_outside_a_gateway_scope_403(
    monkeypatch, route, status, text, sp_is_user
):
    monkeypatch.setenv("GENIE_LLM_ROUTE", route)
    agent, user_session, sp_session = _scope_rig(
        monkeypatch, _Resp(status_code=status, text=text), sp_is_user=sp_is_user)
    with pytest.raises(RuntimeError) as ei:
        list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                               model="databricks-claude-sonnet-4-6"))
    assert f"LLM endpoint returned {status}" in str(ei.value)
    assert len(user_session.calls) == 1 and sp_session.calls == []
