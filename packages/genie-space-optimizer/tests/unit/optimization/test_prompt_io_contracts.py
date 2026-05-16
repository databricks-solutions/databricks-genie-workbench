"""Tests for the typed prompt I/O contract infrastructure.

Plan reference: docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md
"""
from __future__ import annotations

import pytest
from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
    validate_and_parse,
)


class _Example(LLMOutputContract):
    description: str
    synonyms: list[str] = Field(default_factory=list)


def test_build_response_format_returns_json_schema_payload():
    rf = build_response_format(_Example)
    assert rf["type"] == "json_schema"
    assert rf["json_schema"]["name"] == "_Example"
    schema = rf["json_schema"]["schema"]
    assert schema["type"] == "object"
    assert "description" in schema["properties"]
    assert "synonyms" in schema["properties"]


def test_build_response_format_strips_unsupported_keywords():
    """Databricks Foundation Model APIs reject pattern, anyOf, oneOf,
    allOf, prefixItems, $ref, maxProperties, minProperties, maxLength.
    """
    rf = build_response_format(_Example)
    serialized = str(rf)
    for forbidden in ["pattern", "anyOf", "oneOf", "allOf", "prefixItems", "$ref", "maxLength"]:
        assert forbidden not in serialized, (
            f"build_response_format leaked unsupported JSON-schema "
            f"keyword {forbidden!r}; Databricks will reject the call. "
            f"Flattened schema: {rf}"
        )


def test_build_response_format_marks_strict_true():
    rf = build_response_format(_Example)
    assert rf["json_schema"]["strict"] is True


def test_validate_and_parse_returns_model_on_valid_json():
    parsed = validate_and_parse(
        '{"description": "a fact table", "synonyms": ["sales"]}', _Example,
    )
    assert isinstance(parsed, _Example)
    assert parsed.description == "a fact table"
    assert parsed.synonyms == ["sales"]


def test_validate_and_parse_extracts_json_from_code_fence():
    parsed = validate_and_parse(
        '```json\n{"description": "x"}\n```', _Example,
    )
    assert parsed.description == "x"


def test_validate_and_parse_raises_on_missing_required_field():
    with pytest.raises(ValueError):
        validate_and_parse('{"synonyms": ["x"]}', _Example)


def test_validate_and_parse_raises_on_non_json():
    with pytest.raises(ValueError):
        validate_and_parse("not json at all", _Example)
