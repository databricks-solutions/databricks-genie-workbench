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
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
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
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    agent, _ = _agent_with_session(monkeypatch, [_Resp(lines=[
        _LINE, "data: [DONE]", 'data: {"choices":[{"delta":{"content":"AFTER"}}]}'])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1                                   # stops at [DONE]


def test_stream_terminates_on_iterlines_end_without_done(monkeypatch):
    """GPT-style ending: no [DONE], finish_reason line then the stream simply ends."""
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
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


@pytest.mark.parametrize("route", [None, "gateway"])
def test_stream_never_reads_text_on_200(monkeypatch, route):
    """SSE-streaming contract: a 200 streaming response must be consumed without
    ever reading ``.text`` (which forces ``.content`` and defeats streaming).

    Runs under BOTH classic (route=None) and gateway env. ``_NoTextResp.text``
    raises AssertionError if the guard evaluates it on the 200 response.
    """
    if route is None:
        monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    else:
        monkeypatch.setenv("GENIE_LLM_ROUTE", route)
    agent, session = _agent_with_session(monkeypatch, [_NoTextResp(lines=[_LINE, "data: [DONE]"])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1
    assert chunks[0]["choices"][0]["delta"]["content"] == "hi"
    assert len(session.calls) == 1  # no retry POST fired
