"""Plan 7 Task 2 — NextAttemptHypothesis frozen+slots+JsonRoundTrip dataclass."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_dataclass_is_frozen_with_slots() -> None:
    assert dataclasses.is_dataclass(NextAttemptHypothesis)
    assert NextAttemptHypothesis.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in NextAttemptHypothesis.__dict__


def test_dataclass_mixes_in_json_round_trip() -> None:
    assert issubclass(NextAttemptHypothesis, JsonRoundTrip)


def test_field_set_includes_provenance_plus_eight_llm_fields() -> None:
    field_names = {f.name for f in dataclasses.fields(NextAttemptHypothesis)}
    assert field_names == {
        "rolled_back_intent_id",
        "cluster_id",
        "ag_id",
        "iteration",
        "why_failed",
        "failure_mode",
        "revised_repair_shape",
        "revised_patch_type",
        "revised_blame_set",
        "additional_evidence_needed",
        "forbidden_signatures",
        "confidence",
    }


def test_sequence_fields_are_tuples_not_lists() -> None:
    inst = NextAttemptHypothesis(
        rolled_back_intent_id="intent_H001_AG3_001",
        cluster_id="H001",
        ag_id="AG3",
        iteration=3,
        why_failed="x",
        failure_mode="x",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=("sales.fact_sales.revenue",),
        additional_evidence_needed=("data_profile",),
        forbidden_signatures=("fp_abc123",),
        confidence="high",
    )
    assert isinstance(inst.revised_blame_set, tuple)
    assert isinstance(inst.additional_evidence_needed, tuple)
    assert isinstance(inst.forbidden_signatures, tuple)


def test_revised_blame_set_supports_none() -> None:
    inst = NextAttemptHypothesis(
        rolled_back_intent_id="x", cluster_id="x", ag_id="x", iteration=1,
        why_failed="x", failure_mode="x",
        revised_repair_shape=None,
        revised_patch_type=None,
        revised_blame_set=None,
        additional_evidence_needed=(),
        forbidden_signatures=(),
        confidence="low",
    )
    assert inst.revised_blame_set is None


def test_round_trip_through_to_json_from_json() -> None:
    inst = NextAttemptHypothesis(
        rolled_back_intent_id="intent_H001_AG3_001",
        cluster_id="H001",
        ag_id="AG3",
        iteration=3,
        why_failed="patch was too broad",
        failure_mode="overgeneralized_filter",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=("sales.fact_sales.revenue",),
        additional_evidence_needed=("data_profile",),
        forbidden_signatures=("fp_abc123",),
        confidence="high",
    )
    payload = inst.to_json()
    assert payload["cluster_id"] == "H001"
    assert payload["confidence"] == "high"
    assert payload["revised_repair_shape"] == "top_n_by_metric"
    assert payload["revised_patch_type"] == "add_example_sql"
    assert payload["revised_blame_set"] == ["sales.fact_sales.revenue"]
    rebuilt = NextAttemptHypothesis.from_json(payload)
    assert rebuilt == inst


def test_round_trip_with_none_optionals() -> None:
    inst = NextAttemptHypothesis(
        rolled_back_intent_id="x", cluster_id="x", ag_id="x", iteration=1,
        why_failed="x", failure_mode="x",
        revised_repair_shape=None,
        revised_patch_type=None,
        revised_blame_set=None,
        additional_evidence_needed=(),
        forbidden_signatures=(),
        confidence="medium",
    )
    payload = inst.to_json()
    assert payload["revised_repair_shape"] is None
    assert payload["revised_patch_type"] is None
    assert payload["revised_blame_set"] is None
    rebuilt = NextAttemptHypothesis.from_json(payload)
    assert rebuilt == inst


def test_from_llm_output_stamps_provenance_and_converts_to_tuples() -> None:
    from genie_space_optimizer.skills.rollback_learning.output_schema import (
        LlmNextAttemptHypothesisOutput,
    )

    pyd = LlmNextAttemptHypothesisOutput(
        why_failed="x", failure_mode="x",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=["sales.fact_sales.revenue"],
        additional_evidence_needed=["data_profile"],
        forbidden_signatures=["fp_abc"],
        confidence="high",
    )
    nh = NextAttemptHypothesis.from_llm_output(
        pyd,
        rolled_back_intent_id="intent_H001_AG3_001",
        cluster_id="H001",
        ag_id="AG3",
        iteration=3,
    )
    assert nh.rolled_back_intent_id == "intent_H001_AG3_001"
    assert nh.cluster_id == "H001"
    assert nh.ag_id == "AG3"
    assert nh.iteration == 3
    assert isinstance(nh.revised_blame_set, tuple)
    assert nh.revised_blame_set == ("sales.fact_sales.revenue",)
    assert nh.confidence == "high"


def test_reason_code_helper_maps_confidence_to_postmortem_code() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    for conf, code in (
        ("high", ReasonCode.HYPOTHESIS_HIGH_CONFIDENCE),
        ("medium", ReasonCode.HYPOTHESIS_MEDIUM_CONFIDENCE),
        ("low", ReasonCode.HYPOTHESIS_LOW_CONFIDENCE),
    ):
        nh = NextAttemptHypothesis(
            rolled_back_intent_id="x", cluster_id="x", ag_id="x",
            iteration=1,
            why_failed="x", failure_mode="x",
            revised_repair_shape=None,
            revised_patch_type=None,
            revised_blame_set=None,
            additional_evidence_needed=(),
            forbidden_signatures=(),
            confidence=conf,  # type: ignore[arg-type]
        )
        assert nh.reason_code() == code
