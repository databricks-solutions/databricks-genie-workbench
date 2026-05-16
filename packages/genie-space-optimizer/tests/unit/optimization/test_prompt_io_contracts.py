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


# ── Stage-1 discovery contract (Task 14) ──────────────────────────────


def test_stage_1_discovery_output_parses_canonical_response():
    from genie_space_optimizer.optimization.prompt_io import Stage1DiscoveryOutput
    raw = """
    {
      "applicable_skills": [
        {
          "skill_id": "lever-6-sql-expression",
          "target_objects": ["catalog.schema.fact_sales"],
          "expected_impact_qids": ["Q1", "Q2"],
          "evidence_refs": ["H001"],
          "why": "wrong_aggregation",
          "priority": 1
        }
      ],
      "discovery_rationale": "all clusters point at fact_sales"
    }
    """
    parsed = validate_and_parse(raw, Stage1DiscoveryOutput)
    assert len(parsed.applicable_skills) == 1
    pick = parsed.applicable_skills[0]
    assert pick.skill_id == "lever-6-sql-expression"
    assert pick.target_objects == ["catalog.schema.fact_sales"]
    assert pick.priority == 1
    assert parsed.discovery_rationale == "all clusters point at fact_sales"


def test_stage_1_discovery_output_rejects_unknown_priority():
    """priority must be in {1,2,3}; anything else fails validation."""
    from genie_space_optimizer.optimization.prompt_io import Stage1DiscoveryOutput
    raw = '{"applicable_skills": [{"skill_id": "x", "target_objects": [], "expected_impact_qids": [], "evidence_refs": [], "why": "x", "priority": 99}], "discovery_rationale": ""}'
    with pytest.raises(ValueError):
        validate_and_parse(raw, Stage1DiscoveryOutput)


def test_stage_1_discovery_output_allows_empty_skills_list():
    """Stage-1 may emit an empty list when no skill applies (the no-fit branch)."""
    from genie_space_optimizer.optimization.prompt_io import Stage1DiscoveryOutput
    raw = '{"applicable_skills": [], "discovery_rationale": "no actionable target"}'
    parsed = validate_and_parse(raw, Stage1DiscoveryOutput)
    assert parsed.applicable_skills == []


# ── Lever-6 SQL-expression contract (Task 15) ─────────────────────────


def test_lever_6_output_parses_canonical_proposal():
    from genie_space_optimizer.optimization.prompt_io import (
        Lever6SqlExpressionOutput,
    )
    raw = """
    {
      "snippet_type": "expression",
      "display_name": "Top N by Rank",
      "alias": "top_n_by_rank",
      "sql": "ROW_NUMBER() OVER (ORDER BY x DESC)",
      "synonyms": ["top n", "ranking"],
      "instruction": "Use for top-N selection",
      "rationale": "wrong_aggregation",
      "target_table": "catalog.schema.fact_sales",
      "affected_questions": ["Q1"]
    }
    """
    parsed = validate_and_parse(raw, Lever6SqlExpressionOutput)
    assert parsed.snippet_type == "expression"
    assert parsed.affected_questions == ["Q1"]


def test_lever_6_output_rejects_invalid_snippet_type():
    from genie_space_optimizer.optimization.prompt_io import (
        Lever6SqlExpressionOutput,
    )
    raw = '{"snippet_type": "join_spec", "display_name": "X", "sql": "y", "target_table": "t"}'
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever6SqlExpressionOutput)


# ── Lever-1 RCA-bridge contract (Task 16) ─────────────────────────────


def test_lever_1_rca_bridge_table_level_parses_description_only():
    from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput
    raw = '{"description": "fact table for sales transactions"}'
    parsed = validate_and_parse(raw, Lever1RcaBridgeOutput)
    assert parsed.description == "fact table for sales transactions"
    assert parsed.synonyms == []


def test_lever_1_rca_bridge_column_level_parses_synonyms():
    from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput
    raw = '{"description": "store name", "synonyms": ["store", "outlet"]}'
    parsed = validate_and_parse(raw, Lever1RcaBridgeOutput)
    assert parsed.description == "store name"
    assert parsed.synonyms == ["store", "outlet"]


def test_lever_1_rca_bridge_rejects_extra_fields():
    """extra='forbid' on LLMOutputContract prevents hash/debug field
    leaks from the model."""
    from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput
    raw = '{"description": "x", "synonyms": [], "extra_debug": "leak"}'
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever1RcaBridgeOutput)
