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


# ── Lever-1/2 column-description contract (Task 17) ───────────────────


def test_lever_1_2_column_output_parses_canonical_response():
    from genie_space_optimizer.optimization.prompt_io import Lever12ColumnOutput
    raw = """
    {
      "changes": [{
        "table": "catalog.schema.dim_store",
        "column": "store_name",
        "entity_type": "column_dim",
        "sections": {"definition": "store display name", "synonyms": "store, outlet"}
      }],
      "table_changes": [{
        "table": "catalog.schema.fact_sales",
        "sections": {"purpose": "transactional sales", "grain": "per line"}
      }],
      "rationale": "metadata gap"
    }
    """
    parsed = validate_and_parse(raw, Lever12ColumnOutput)
    assert len(parsed.changes) == 1
    assert parsed.changes[0].entity_type == "column_dim"
    assert parsed.table_changes[0].sections["grain"] == "per line"
    assert parsed.rationale == "metadata gap"


def test_lever_1_2_column_output_rejects_invalid_entity_type():
    from genie_space_optimizer.optimization.prompt_io import Lever12ColumnOutput
    raw = """{"changes": [{"table": "t", "column": "c", "entity_type": "foo", "sections": {}}], "table_changes": [], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever12ColumnOutput)


# ── Strategist family (Task 18) ───────────────────────────────────────


def test_adaptive_strategist_output_parses_canonical_response():
    from genie_space_optimizer.optimization.prompt_io import AdaptiveStrategistOutput
    raw = """
    {
      "action_groups": [{
        "id": "AG1",
        "root_cause_summary": "wrong aggregation on revenue measure",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["Q1", "Q2"],
        "priority": 1,
        "lever_directives": {
          "6": {"sql_expressions": [{"snippet_type": "measure", "display_name": "X", "alias": "x", "sql": "SUM(t.c)", "synonyms": [], "instruction": "i"}]}
        },
        "coordination_notes": "lever 6 alone",
        "proposals": []
      }],
      "global_instruction_rewrite": {
        "PURPOSE": "Sales analytics."
      },
      "rationale": "single-defect AG"
    }
    """
    parsed = validate_and_parse(raw, AdaptiveStrategistOutput)
    assert len(parsed.action_groups) == 1
    assert parsed.action_groups[0].id == "AG1"
    assert "6" in parsed.action_groups[0].lever_directives
    assert parsed.global_instruction_rewrite["PURPOSE"] == "Sales analytics."


def test_strategist_triage_output_parses_minimal_response():
    from genie_space_optimizer.optimization.prompt_io import StrategistTriageOutput
    raw = '{"action_groups": [{"id": "AG1", "priority": 2}], "rationale": "triage"}'
    parsed = validate_and_parse(raw, StrategistTriageOutput)
    assert parsed.action_groups[0].priority == 2


def test_strategist_detail_output_allows_extra_nested_fields():
    """The permissive base allows the strategist family to grow new
    nested fields without breaking validation."""
    from genie_space_optimizer.optimization.prompt_io import StrategistDetailOutput
    raw = """
    {
      "action_groups": [{
        "id": "AG1",
        "new_experimental_field": "ok",
        "priority": 1
      }],
      "rationale": "detail"
    }
    """
    parsed = validate_and_parse(raw, StrategistDetailOutput)
    assert parsed.action_groups[0].id == "AG1"


# ── Lever-4 + Lever-5 family (Task 19) ────────────────────────────────


def test_lever_4_join_discovery_output_parses_canonical_response():
    from genie_space_optimizer.optimization.prompt_io import (
        Lever4JoinDiscoveryOutput,
    )
    raw = """
    {
      "join_specs": [{
        "left": {"identifier": "catalog.schema.fact_sales", "alias": "fs"},
        "right": {"identifier": "catalog.schema.dim_store", "alias": "ds"},
        "sql": ["fs.store_id = ds.store_id", "--rt=inner--"],
        "instruction": "join on store_id"
      }],
      "rationale": "missing join"
    }
    """
    parsed = validate_and_parse(raw, Lever4JoinDiscoveryOutput)
    assert len(parsed.join_specs) == 1
    assert parsed.join_specs[0].left.identifier == "catalog.schema.fact_sales"
    assert parsed.join_specs[0].sql[0] == "fs.store_id = ds.store_id"


def test_lever_4_join_discovery_output_allows_empty_join_specs():
    """When no valid joins are found, lever-4 returns empty join_specs."""
    from genie_space_optimizer.optimization.prompt_io import (
        Lever4JoinDiscoveryOutput,
    )
    raw = '{"join_specs": [], "rationale": "no valid pairs"}'
    parsed = validate_and_parse(raw, Lever4JoinDiscoveryOutput)
    assert parsed.join_specs == []


def test_lever_5a_instruction_output_parses_prose_only():
    from genie_space_optimizer.optimization.prompt_io import Lever5aInstructionOutput
    raw = '{"instruction_text": "PURPOSE:\\nSales analytics.", "rationale": "added purpose section"}'
    parsed = validate_and_parse(raw, Lever5aInstructionOutput)
    assert parsed.instruction_text.startswith("PURPOSE:")


def test_lever_5_instruction_output_discriminates_three_types():
    """Lever-5 holistic emits one of three shapes; the contract pins
    instruction_type to the closed enum."""
    from genie_space_optimizer.optimization.prompt_io import Lever5InstructionOutput
    for kind in ("example_sql", "text_instruction", "sql_expression"):
        raw = f'{{"instruction_type": "{kind}", "rationale": "x"}}'
        parsed = validate_and_parse(raw, Lever5InstructionOutput)
        assert parsed.instruction_type == kind
    # Unknown kind rejected:
    bad = '{"instruction_type": "join_spec", "rationale": "x"}'
    with pytest.raises(ValueError):
        validate_and_parse(bad, Lever5InstructionOutput)
