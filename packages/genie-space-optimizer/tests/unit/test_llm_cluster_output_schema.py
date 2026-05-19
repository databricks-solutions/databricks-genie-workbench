"""Plan 4 Task 1 — LlmClusterOutput Pydantic shape (per-cluster)."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.skills.failure_clustering.output_schema import (
    LlmClusterOutput,
)


def test_per_cluster_subclasses_llm_output_contract() -> None:
    assert issubclass(LlmClusterOutput, LLMOutputContract)


def test_per_cluster_required_fields_present_and_typed() -> None:
    fields = LlmClusterOutput.model_fields
    expected = {
        "semantic_theme", "member_qids", "unifying_evidence",
        "suggested_repair_shape", "primary_blame_set", "confidence",
    }
    assert set(fields.keys()) == expected, (
        f"field drift: missing={expected - set(fields.keys())}, "
        f"unexpected={set(fields.keys()) - expected}"
    )


def test_cluster_id_is_intentionally_absent() -> None:
    """Framework stamps cluster_id deterministically; the LLM emits
    NO cluster_id to eliminate duplicate-ID hallucination."""
    assert "cluster_id" not in LlmClusterOutput.model_fields


def test_suggested_repair_shape_is_bound_to_repair_shape_enum() -> None:
    inst = LlmClusterOutput(
        semantic_theme="top-N ranking missing",
        member_qids=["gs_009", "gs_017"],
        unifying_evidence="all three SQL outputs collapse to a single row",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=["sales.fact_sales.revenue"],
        confidence="high",
    )
    assert inst.suggested_repair_shape is RepairShape.TOP_N_BY_METRIC


def test_repair_shape_other_is_accepted_as_escape_hatch() -> None:
    inst = LlmClusterOutput(
        semantic_theme="novel structural pattern",
        member_qids=["gs_001"],
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.OTHER,
        primary_blame_set=[],
        confidence="low",
    )
    assert inst.suggested_repair_shape is RepairShape.OTHER


def test_confidence_field_is_literal_high_medium_low() -> None:
    with pytest.raises(ValidationError):
        LlmClusterOutput(
            semantic_theme="x",
            member_qids=["gs_001"],
            unifying_evidence="x",
            suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
            primary_blame_set=[],
            confidence="totally_sure",
        )


def test_member_qids_must_be_non_empty() -> None:
    with pytest.raises(ValidationError):
        LlmClusterOutput(
            semantic_theme="x",
            member_qids=[],
            unifying_evidence="x",
            suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
            primary_blame_set=[],
            confidence="high",
        )


def test_member_qids_and_primary_blame_set_are_lists_of_str() -> None:
    inst = LlmClusterOutput(
        semantic_theme="x",
        member_qids=["gs_001", "gs_002"],
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.JOIN_DISCOVERY,
        primary_blame_set=["catalog.schema.table.col"],
        confidence="medium",
    )
    assert inst.member_qids == ["gs_001", "gs_002"]
    assert inst.primary_blame_set == ["catalog.schema.table.col"]


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        LlmClusterOutput(
            semantic_theme="x",
            member_qids=["gs_001"],
            unifying_evidence="x",
            suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
            primary_blame_set=[],
            confidence="high",
            cluster_id="H001",
        )
