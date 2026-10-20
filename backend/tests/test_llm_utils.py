"""Tests for LLM JSON repair and parsing (backend/services/llm_utils.py).

Tests _repair_json() and parse_json_from_llm_response() — pure string→dict
functions, no mocking required.
"""

import json
import re
from types import SimpleNamespace

import pytest

from backend.services import llm_utils
from backend.services.llm_utils import (
    _repair_json,
    normalize_message_content,
    parse_json_from_llm_response,
)


# ---------------------------------------------------------------------------
# normalize_message_content
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("plain text", "plain text"),
        (
            [
                {"type": "text", "text": "first "},
                {"type": "text", "text": "block"},
            ],
            "first block",
        ),
        (
            [
                {"type": "metadata", "signature": "ignored"},
                {"type": "text", "content": [{"value": "nested text"}]},
            ],
            "nested text",
        ),
        (None, ""),
    ],
)
def test_normalize_message_content(content, expected):
    assert normalize_message_content(content) == expected


def test_call_serving_endpoint_normalizes_structured_content(monkeypatch):
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://example.databricks.com",
            authenticate=lambda: {"Authorization": "Bearer test"},
        )
    )
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {
            "choices": [{
                "message": {
                    "content": [
                        {"type": "text", "text": '{"findings":'},
                        {"type": "text", "text": " []}"},
                    ]
                }
            }]
        },
    )
    monkeypatch.setattr(llm_utils, "get_workspace_client", lambda: client)
    monkeypatch.setattr(llm_utils.httpx, "post", lambda *args, **kwargs: response)

    result = llm_utils.call_serving_endpoint(
        [{"role": "user", "content": "Return JSON"}]
    )

    assert result == '{"findings": []}'


def _gateway_client():
    return SimpleNamespace(
        config=SimpleNamespace(
            host="https://example.databricks.com/",
            authenticate=lambda: {"Authorization": "Bearer test"},
        )
    )


def test_call_serving_endpoint_posts_to_unity_gateway(monkeypatch):
    captured = {}

    def fake_post(url, *, json, headers, timeout):
        captured.update(url=url, json=json, headers=headers)
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"choices": [{"message": {"content": "ok"}}]},
        )

    monkeypatch.setenv("LLM_MODEL", "databricks-claude-sonnet-4-6")
    monkeypatch.setattr(llm_utils, "get_workspace_client", _gateway_client)
    monkeypatch.setattr(llm_utils.httpx, "post", fake_post)

    assert llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}], max_tokens=5) == "ok"
    assert captured["url"] == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
    assert captured["json"] == {
        "model": "system.ai.claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5,
    }


def test_call_serving_endpoint_normalizes_explicit_legacy_model(monkeypatch):
    captured = {}

    def fake_post(url, *, json, headers, timeout):
        captured["model"] = json["model"]
        return SimpleNamespace(status_code=200, json=lambda: {"choices": [{"message": {"content": "ok"}}]})

    monkeypatch.setattr(llm_utils, "get_workspace_client", _gateway_client)
    monkeypatch.setattr(llm_utils.httpx, "post", fake_post)

    llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}], model="databricks-gpt-5-4")

    assert captured["model"] == "system.ai.gpt-5-4"


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (404, '{"error_code":"NOT_FOUND","message":"\'system.ai.x\' does not exist."}',
         "Unity Gateway returned 404 NOT_FOUND for model=system.ai.x: 'system.ai.x' does not exist."),
        (403, '{"error_code":"PERMISSION_DENIED","message":"required scopes: ai-gateway"}',
         "missing the 'ai-gateway' permission"),
    ],
)
def test_call_serving_endpoint_gateway_errors(monkeypatch, status, body, expected):
    monkeypatch.setattr(llm_utils, "get_workspace_client", _gateway_client)
    monkeypatch.setattr(
        llm_utils.httpx, "post",
        lambda *a, **k: SimpleNamespace(status_code=status, text=body, headers={}),
    )

    with pytest.raises(RuntimeError, match=re.escape(expected)):
        llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}], model="system.ai.x")


def test_get_llm_model_defaults_and_normalizes(monkeypatch):
    monkeypatch.delenv("LLM_MODEL", raising=False)
    assert llm_utils.get_llm_model() == "system.ai.claude-sonnet-4-6"
    monkeypatch.setenv("LLM_MODEL", "databricks-claude-opus-5")
    assert llm_utils.get_llm_model() == "system.ai.claude-opus-5"


# ---------------------------------------------------------------------------
# _repair_json
# ---------------------------------------------------------------------------

class TestRepairJson:
    def test_valid_json_unchanged(self):
        valid = '{"key": "value", "num": 42}'
        result = json.loads(_repair_json(valid))
        assert result == {"key": "value", "num": 42}

    def test_trailing_comma_in_object(self):
        broken = '{"a": 1, "b": 2,}'
        result = json.loads(_repair_json(broken))
        assert result == {"a": 1, "b": 2}

    def test_trailing_comma_in_array(self):
        broken = '{"items": [1, 2, 3,]}'
        result = json.loads(_repair_json(broken))
        assert result["items"] == [1, 2, 3]

    def test_missing_comma_between_objects(self):
        broken = '[{"a": 1}\n{"b": 2}]'
        result = json.loads(_repair_json(broken))
        assert len(result) == 2

    def test_missing_comma_between_strings(self):
        broken = '{"a": "x"\n"b": "y"}'
        repaired = _repair_json(broken)
        result = json.loads(repaired)
        assert result["a"] == "x"
        assert result["b"] == "y"

    def test_missing_comma_after_brace_before_string(self):
        broken = '{"inner": {"x": 1}\n"outer": 2}'
        repaired = _repair_json(broken)
        result = json.loads(repaired)
        assert result["outer"] == 2

    def test_missing_comma_after_bool(self):
        broken = '{"a": true\n"b": "val"}'
        repaired = _repair_json(broken)
        result = json.loads(repaired)
        assert result["a"] is True
        assert result["b"] == "val"

    def test_nested_trailing_commas(self):
        broken = '{"a": {"b": [1, 2,],},}'
        result = json.loads(_repair_json(broken))
        assert result["a"]["b"] == [1, 2]


# ---------------------------------------------------------------------------
# parse_json_from_llm_response
# ---------------------------------------------------------------------------

class TestParseJsonFromLlmResponse:
    def test_plain_json(self):
        result = parse_json_from_llm_response('{"key": "value"}')
        assert result == {"key": "value"}

    def test_json_code_block(self):
        content = '```json\n{"key": "value"}\n```'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_code_block_no_language(self):
        content = '```\n{"key": "value"}\n```'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_text_before_json(self):
        content = 'Here is the configuration:\n{"key": "value"}'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_nested_braces_extracted_correctly(self):
        content = 'Result: {"outer": {"inner": [1, 2]}} done.'
        result = parse_json_from_llm_response(content)
        assert result == {"outer": {"inner": [1, 2]}}

    def test_empty_response_raises_valueerror(self):
        with pytest.raises(ValueError, match="empty"):
            parse_json_from_llm_response("   ")

    def test_repairable_json_succeeds(self):
        content = '```json\n{"a": 1, "b": 2,}\n```'
        result = parse_json_from_llm_response(content)
        assert result == {"a": 1, "b": 2}

    def test_unrepairable_json_raises_jsondecodeerror(self):
        content = '{"key": value_without_quotes}'
        with pytest.raises(json.JSONDecodeError):
            parse_json_from_llm_response(content)

    def test_text_after_json_is_ignored_when_starts_with_brace(self):
        content = '{"key": "value"}\nThat is the result.'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_multiple_json_objects_returns_first(self):
        content = (
            '{"needs_repair": false}\n'
            'Wait, let me reconsider and return valid JSON only.\n'
            '{"needs_repair": true}'
        )
        result = parse_json_from_llm_response(content)
        assert result == {"needs_repair": False}

    def test_text_before_and_after_json_extracted(self):
        """When text precedes JSON, brace-matching extracts just the JSON."""
        content = 'Here: {"key": "value"} and more text.'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_whitespace_handling(self):
        content = '\n\n  {"key": "value"}  \n\n'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}
