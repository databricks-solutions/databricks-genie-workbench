"""Plan 5 Task 2 — RepairProposal frozen+slots+JsonRoundTrip dataclass."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    PatchBodyValidationError,
    RepairProposal,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_dataclass_is_frozen_with_slots() -> None:
    assert dataclasses.is_dataclass(RepairProposal)
    assert RepairProposal.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in RepairProposal.__dict__


def test_dataclass_mixes_in_json_round_trip() -> None:
    assert issubclass(RepairProposal, JsonRoundTrip)


def test_field_set_includes_intent_id_plus_eight_llm_fields() -> None:
    # Plan 9 Task 1 — target_objects (LLM-emitted typed slice).
    # Plan 9 Task 3 — required_constructs (LLM-emitted SQL-clause
    # contract that replaces archetype.output_shape["requires_constructs"]).
    # Plan 11 — repair_hypothesis (free-text replacement for repair_shape)
    #           and target_qids (which QIDs the proposal is meant to fix).
    field_names = {f.name for f in dataclasses.fields(RepairProposal)}
    assert field_names == {
        "intent_id",
        "intent_name", "intent_description", "repair_shape",
        "patch_type", "rationale", "confidence",
        "patch_body", "blame_set",
        "target_objects",
        "required_constructs",
        "repair_hypothesis",
        "target_qids",
        # Trial 17 — LLM-led Lever Selection Contract fields.
        "selected_lever",
        # Phase 2 P2.1 — primary lever-kit list (selected_lever is
        # the back-compat fallback).
        "selected_levers",
        "expected_behavioral_change",
        "fallback_lever",
        "bundle_id",
        # Trial 20 D1 — free-text single-lever justification
        # (DEPRECATED at P2.1; field retained for serialization
        # compatibility with pre-P2.1 persisted proposals).
        "single_lever_justification",
    }


def test_patch_body_validation_error_is_a_valueerror() -> None:
    assert issubclass(PatchBodyValidationError, ValueError)


def test_round_trip_through_to_json_from_json() -> None:
    inst = RepairProposal(
        intent_id="intent_H001_AG3_001",
        intent_name="top_n_revenue_by_region",
        intent_description="add example_sql for top-N revenue by region",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="cluster blames missing LIMIT/ORDER BY",
        confidence="high",
        patch_body={
            "example_question": "What are the top 3 regions?",
            "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
            "usage_guidance": "use when the user asks for top-N",
            "parameters": [],
        },
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
    )
    payload = inst.to_json()
    assert payload["intent_id"] == "intent_H001_AG3_001"
    assert payload["repair_shape"] == "top_n_by_metric"
    assert payload["patch_type"] == "add_example_sql"
    assert payload["blame_set"] == [
        "sales.fact_sales.revenue", "sales.fact_sales.region",
    ]
    rebuilt = RepairProposal.from_json(payload)
    assert rebuilt == inst


def test_sequence_fields_are_tuples_not_lists() -> None:
    inst = RepairProposal(
        intent_id="intent_H001_AG3_002",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_INSTRUCTION,
        rationale="x", confidence="low",
        patch_body={"instruction_text": "Always group by region."},
        blame_set=("a.b.c",),
    )
    assert isinstance(inst.blame_set, tuple)


def test_to_proposal_dict_for_add_example_sql_returns_l5b_contract() -> None:
    inst = RepairProposal(
        intent_id="intent_H001_AG3_003",
        intent_name="top_n_revenue_by_region",
        intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high",
        patch_body={
            "example_question": "What are the top 3 regions?",
            "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
            "usage_guidance": "use when the user asks for top-N",
            "parameters": [{"name": "n", "type": "int", "default": 3}],
        },
        blame_set=("sales.fact_sales.revenue",),
    )
    out = inst.to_proposal_dict()
    assert out == {
        "example_question": "What are the top 3 regions?",
        "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
        "usage_guidance": "use when the user asks for top-N",
        "parameters": [{"name": "n", "type": "int", "default": 3}],
    }


def test_to_proposal_dict_for_add_sql_snippet_expression_returns_l6_contract() -> None:
    inst = RepairProposal(
        intent_id="intent_H001_AG3_004",
        intent_name="revenue_by_region_expr",
        intent_description="x",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        rationale="x", confidence="high",
        patch_body={
            "name": "revenue_by_region",
            "sql_expression": "SUM(revenue) OVER (PARTITION BY region)",
            "usage_guidance": "use when the user asks for windowed revenue",
        },
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
    )
    out = inst.to_proposal_dict()
    assert out == {
        "name": "revenue_by_region",
        "sql_expression": "SUM(revenue) OVER (PARTITION BY region)",
        "usage_guidance": "use when the user asks for windowed revenue",
    }


def test_to_proposal_dict_raises_when_required_fields_missing() -> None:
    import pytest

    inst = RepairProposal(
        intent_id="intent_H001_AG3_005",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high",
        patch_body={"example_question": "only the question"},
        blame_set=(),
    )
    with pytest.raises(PatchBodyValidationError) as exc:
        inst.to_proposal_dict()
    assert "example_sql" in str(exc.value)


def test_from_llm_output_stamps_intent_id_and_converts_to_tuples() -> None:
    from genie_space_optimizer.skills.repair_intent_synthesis.output_schema import (
        LlmRepairProposalOutput,
    )

    pyd = LlmRepairProposalOutput(
        intent_name="top_n", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high",
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
        blame_set=["a.b.c"],
    )
    rp = RepairProposal.from_llm_output(
        pyd, intent_id="intent_H001_AG3_001",
    )
    assert rp.intent_id == "intent_H001_AG3_001"
    assert isinstance(rp.blame_set, tuple)
    assert rp.blame_set == ("a.b.c",)
    assert rp.repair_shape is RepairShape.TOP_N_BY_METRIC
