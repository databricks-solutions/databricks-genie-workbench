"""Plan 5 Task 1 — LlmRepairProposalOutput Pydantic shape."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.skills.repair_intent_synthesis.output_schema import (
    LlmRepairProposalOutput,
)


def test_output_subclasses_llm_output_contract() -> None:
    assert issubclass(LlmRepairProposalOutput, LLMOutputContract)


def test_required_fields_present_and_typed() -> None:
    # Plan 9 Task 1 — target_objects (LLM emits typed slice).
    # Plan 9 Task 3 — required_constructs (LLM emits SQL-clause contract).
    fields = LlmRepairProposalOutput.model_fields
    expected = {
        "intent_name", "intent_description", "repair_shape",
        "patch_type", "rationale", "confidence",
        "patch_body", "blame_set",
        "target_objects",
        "required_constructs",
        # Trial 17 — LLM-led Lever Selection Contract fields.
        "selected_lever",
        "expected_behavioral_change",
        "fallback_lever",
        "bundle_id",
        # Trial 20 D1 — single-lever justification (required after kept_insufficient,
        # optional otherwise; presence on schema gates parser preservation).
        "single_lever_justification",
    }
    assert set(fields.keys()) == expected, (
        f"field drift: missing={expected - set(fields.keys())}, "
        f"unexpected={set(fields.keys()) - expected}"
    )


def test_intent_id_is_intentionally_absent() -> None:
    """Framework stamps intent_id; LLM never mints IDs."""
    assert "intent_id" not in LlmRepairProposalOutput.model_fields


def test_repair_shape_is_bound_to_repair_shape_enum() -> None:
    inst = LlmRepairProposalOutput(
        intent_name="top_n_revenue_by_region",
        intent_description="add example_sql for top-N revenue grouping",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="cluster blames missing LIMIT/ORDER BY",
        confidence="high",
        patch_body={
            "example_question": "What are the top 3 regions by revenue?",
            "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
            "usage_guidance": "use when the user asks for top-N",
            "parameters": [],
        },
        blame_set=["sales.fact_sales.revenue", "sales.fact_sales.region"],
    )
    assert inst.repair_shape is RepairShape.TOP_N_BY_METRIC
    assert inst.patch_type is PatchType.ADD_EXAMPLE_SQL


def test_patch_type_other_is_rejected_at_parse_time() -> None:
    """PatchType has no OTHER. RepairShape has OTHER, not PatchType."""
    with pytest.raises(ValidationError):
        LlmRepairProposalOutput(
            intent_name="x", intent_description="x",
            repair_shape=RepairShape.OTHER,
            patch_type="hallucinated_arm",
            rationale="x", confidence="low",
            patch_body={}, blame_set=[],
        )


def test_repair_shape_other_with_known_patch_type_is_accepted() -> None:
    inst = LlmRepairProposalOutput(
        intent_name="tvf_invocation_pattern",
        intent_description="add a TVF teaching example",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="no closed shape matches; pattern is novel",
        confidence="low",
        patch_body={
            "example_question": "How do I invoke the revenue TVF?",
            "example_sql": "SELECT * FROM TABLE(analytics.fn_revenue('Q1'))",
            "usage_guidance": "use for TVF call demonstrations",
            "parameters": [],
        },
        blame_set=["analytics.fn_revenue"],
    )
    assert inst.repair_shape is RepairShape.OTHER
    assert inst.patch_type is PatchType.ADD_EXAMPLE_SQL


def test_confidence_is_literal_high_medium_low() -> None:
    with pytest.raises(ValidationError):
        LlmRepairProposalOutput(
            intent_name="x", intent_description="x",
            repair_shape=RepairShape.TOP_N_BY_METRIC,
            patch_type=PatchType.ADD_EXAMPLE_SQL,
            rationale="x", confidence="absolutely",
            patch_body={}, blame_set=[],
        )


def test_patch_body_accepts_arbitrary_dict_pydantic_does_not_constrain() -> None:
    """Loose schema by design — per-patch-type field constraints
    enforced by deterministic validator."""
    inst = LlmRepairProposalOutput(
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.JOIN_DISCOVERY,
        patch_type=PatchType.ADD_JOIN_SPEC,
        rationale="x", confidence="high",
        patch_body={
            "left": "crm.customer", "right": "crm.orders",
            "on": "customer_id",
        },
        blame_set=["crm.customer.customer_id", "crm.orders.customer_id"],
    )
    assert inst.patch_body == {
        "left": "crm.customer", "right": "crm.orders",
        "on": "customer_id",
    }


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        LlmRepairProposalOutput(
            intent_name="x", intent_description="x",
            repair_shape=RepairShape.TOP_N_BY_METRIC,
            patch_type=PatchType.ADD_EXAMPLE_SQL,
            rationale="x", confidence="high",
            patch_body={}, blame_set=[],
            intent_id="intent_H001_xxx_001",
        )


def test_envelope_response_format_is_databricks_strict_safe() -> None:
    """No Databricks-unsupported keyword leaks. PR-C-aware version
    that walks dict keys instead of substring-matching ``repr(fmt)``,
    which would false-positive on prose like "failure pattern" now
    that descriptions are preserved end-to-end."""
    from tests._schema_utils import assert_no_forbidden_schema_keys

    EnvCls = AbstainableEnvelope[LlmRepairProposalOutput]
    fmt = build_response_format(EnvCls)
    assert_no_forbidden_schema_keys(fmt)
