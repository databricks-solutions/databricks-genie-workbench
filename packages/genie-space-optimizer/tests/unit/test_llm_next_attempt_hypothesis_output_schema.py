"""Plan 7 Task 1 — LlmNextAttemptHypothesisOutput Pydantic shape (per-cluster)."""
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
from genie_space_optimizer.skills.rollback_learning.output_schema import (
    LlmNextAttemptHypothesisOutput,
)


def test_output_subclasses_llm_output_contract() -> None:
    assert issubclass(LlmNextAttemptHypothesisOutput, LLMOutputContract)


def test_required_fields_present_and_typed() -> None:
    fields = LlmNextAttemptHypothesisOutput.model_fields
    expected = {
        "why_failed",
        "failure_mode",
        "revised_repair_shape",
        "revised_patch_type",
        "revised_blame_set",
        "additional_evidence_needed",
        "forbidden_signatures",
        "confidence",
    }
    assert set(fields.keys()) == expected, (
        f"field drift: missing={expected - set(fields.keys())}, "
        f"unexpected={set(fields.keys()) - expected}"
    )


def test_rolled_back_intent_id_is_intentionally_absent() -> None:
    """Framework stamps rolled_back_intent_id; LLM never mints IDs."""
    assert "rolled_back_intent_id" not in LlmNextAttemptHypothesisOutput.model_fields


def test_cluster_id_is_intentionally_absent() -> None:
    """cluster_id is framework-stamped."""
    assert "cluster_id" not in LlmNextAttemptHypothesisOutput.model_fields


def test_revised_repair_shape_uses_closed_enum_and_accepts_none() -> None:
    inst = LlmNextAttemptHypothesisOutput(
        why_failed="patch was too broad",
        failure_mode="overgeneralized_filter",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=["sales.fact_sales.revenue"],
        additional_evidence_needed=[],
        forbidden_signatures=[],
        confidence="high",
    )
    assert inst.revised_repair_shape == RepairShape.TOP_N_BY_METRIC

    inst_none = LlmNextAttemptHypothesisOutput(
        why_failed="x", failure_mode="x",
        revised_repair_shape=None,
        revised_patch_type=None,
        revised_blame_set=None,
        additional_evidence_needed=[],
        forbidden_signatures=[],
        confidence="medium",
    )
    assert inst_none.revised_repair_shape is None
    assert inst_none.revised_patch_type is None
    assert inst_none.revised_blame_set is None


def test_revised_patch_type_uses_closed_enum() -> None:
    inst = LlmNextAttemptHypothesisOutput(
        why_failed="x", failure_mode="x",
        revised_repair_shape=None,
        revised_patch_type=PatchType.ADD_SQL_SNIPPET_FILTER,
        revised_blame_set=None,
        additional_evidence_needed=[],
        forbidden_signatures=[],
        confidence="high",
    )
    assert inst.revised_patch_type == PatchType.ADD_SQL_SNIPPET_FILTER

    with pytest.raises(ValidationError):
        LlmNextAttemptHypothesisOutput(
            why_failed="x", failure_mode="x",
            revised_repair_shape=None,
            revised_patch_type="add_emoji",
            revised_blame_set=None,
            additional_evidence_needed=[],
            forbidden_signatures=[],
            confidence="low",
        )


def test_confidence_is_literal_high_medium_low() -> None:
    for conf in ("high", "medium", "low"):
        inst = LlmNextAttemptHypothesisOutput(
            why_failed="x", failure_mode="x",
            revised_repair_shape=None,
            revised_patch_type=None,
            revised_blame_set=None,
            additional_evidence_needed=[],
            forbidden_signatures=[],
            confidence=conf,  # type: ignore[arg-type]
        )
        assert inst.confidence == conf

    with pytest.raises(ValidationError):
        LlmNextAttemptHypothesisOutput(
            why_failed="x", failure_mode="x",
            revised_repair_shape=None,
            revised_patch_type=None,
            revised_blame_set=None,
            additional_evidence_needed=[],
            forbidden_signatures=[],
            confidence="very_high",
        )


def test_failure_mode_is_free_form_string() -> None:
    inst = LlmNextAttemptHypothesisOutput(
        why_failed="x",
        failure_mode="LLM_invented_label_with_underscores",
        revised_repair_shape=None,
        revised_patch_type=None,
        revised_blame_set=None,
        additional_evidence_needed=[],
        forbidden_signatures=[],
        confidence="medium",
    )
    assert inst.failure_mode == "LLM_invented_label_with_underscores"


def test_list_fields_accept_empty_list() -> None:
    inst = LlmNextAttemptHypothesisOutput(
        why_failed="x", failure_mode="x",
        revised_repair_shape=None,
        revised_patch_type=None,
        revised_blame_set=None,
        additional_evidence_needed=[],
        forbidden_signatures=[],
        confidence="low",
    )
    assert inst.additional_evidence_needed == []
    assert inst.forbidden_signatures == []


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        LlmNextAttemptHypothesisOutput(
            why_failed="x", failure_mode="x",
            revised_repair_shape=None,
            revised_patch_type=None,
            revised_blame_set=None,
            additional_evidence_needed=[],
            forbidden_signatures=[],
            confidence="medium",
            rolled_back_intent_id="intent_001",
        )


def test_envelope_response_format_is_databricks_strict_safe() -> None:
    EnvCls = AbstainableEnvelope[LlmNextAttemptHypothesisOutput]
    fmt = build_response_format(EnvCls)
    schema_blob = repr(fmt)
    for forbidden in ("anyOf", "oneOf", "$ref", "pattern"):
        assert forbidden not in schema_blob, (
            f"envelope schema contains forbidden keyword {forbidden!r}: "
            f"{schema_blob[:500]}"
        )
