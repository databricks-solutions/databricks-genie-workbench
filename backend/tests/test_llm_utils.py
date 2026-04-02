"""Tests for LLM JSON repair and parsing (backend/services/llm_utils.py).

Tests _repair_json() and parse_json_from_llm_response() — pure string→dict
functions, no mocking required.
"""

import json
import pytest

from backend.services.llm_utils import _repair_json, parse_json_from_llm_response


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

    def test_text_after_json_not_handled_when_starts_with_brace(self):
        """When content starts with '{', brace-matching is skipped.
        Text after JSON causes a parse error (known limitation)."""
        content = '{"key": "value"}\nThat is the result.'
        with pytest.raises(json.JSONDecodeError):
            parse_json_from_llm_response(content)

    def test_text_before_and_after_json_extracted(self):
        """When text precedes JSON, brace-matching extracts just the JSON."""
        content = 'Here: {"key": "value"} and more text.'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_whitespace_handling(self):
        content = '\n\n  {"key": "value"}  \n\n'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}
