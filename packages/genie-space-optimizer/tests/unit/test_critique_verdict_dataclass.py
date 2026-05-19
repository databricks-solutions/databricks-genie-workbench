"""Plan 6 Task 2 — CritiqueVerdict frozen+slots+JsonRoundTrip dataclass.

Wire-stable carrier through the critique stage + decision-record
emitters. Distinct from the Pydantic LlmCritiqueVerdictOutput
(response_format binding only); same field set PLUS the framework-
stamped proposal_id.
"""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_dataclass_is_frozen_with_slots() -> None:
    assert dataclasses.is_dataclass(CritiqueVerdict)
    assert CritiqueVerdict.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in CritiqueVerdict.__dict__


def test_dataclass_mixes_in_json_round_trip() -> None:
    assert issubclass(CritiqueVerdict, JsonRoundTrip)


def test_field_set_includes_proposal_id_plus_six_llm_fields() -> None:
    field_names = {f.name for f in dataclasses.fields(CritiqueVerdict)}
    assert field_names == {
        "proposal_id",
        "addresses_target_failure",
        "is_overgeneralized",
        "likely_neighbor_regressions",
        "matches_intended_shape",
        "overall_recommendation",
        "rationale",
    }


def test_round_trip_through_to_json_from_json() -> None:
    inst = CritiqueVerdict(
        proposal_id="prop_H001_AG3_001",
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=("gs_044", "gs_055"),
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="example_sql cleanly demonstrates top-N pattern",
    )
    payload = inst.to_json()
    assert payload["proposal_id"] == "prop_H001_AG3_001"
    assert payload["overall_recommendation"] == "proceed"
    assert payload["likely_neighbor_regressions"] == ["gs_044", "gs_055"]
    rebuilt = CritiqueVerdict.from_json(payload)
    assert rebuilt == inst


def test_likely_neighbor_regressions_is_tuple_not_list() -> None:
    """Sequence fields must be tuples (frozen dataclass requirement)."""
    inst = CritiqueVerdict(
        proposal_id="prop_001",
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=("gs_001",),
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="x",
    )
    assert isinstance(inst.likely_neighbor_regressions, tuple)


def test_is_blocking_true_for_discard_recommendation() -> None:
    inst = CritiqueVerdict(
        proposal_id="prop_001",
        addresses_target_failure=False,
        is_overgeneralized=True,
        likely_neighbor_regressions=("gs_044",),
        matches_intended_shape=False,
        overall_recommendation="discard",
        rationale="overgeneralized; regression risk on gs_044",
    )
    assert inst.is_blocking() is True


def test_is_blocking_false_for_proceed_recommendation() -> None:
    inst = CritiqueVerdict(
        proposal_id="prop_001",
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=(),
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="x",
    )
    assert inst.is_blocking() is False


def test_is_blocking_false_for_rework_recommendation() -> None:
    """rework lets through as advisory — Plan 8 will wire rework into
    Plan 5's synthesizer for a retry. For now rework == proceed
    semantically with a distinct reason_code for postmortem."""
    inst = CritiqueVerdict(
        proposal_id="prop_001",
        addresses_target_failure=True,
        is_overgeneralized=True,
        likely_neighbor_regressions=("gs_044",),
        matches_intended_shape=True,
        overall_recommendation="rework",
        rationale="partial shape match; consider narrower blame_set",
    )
    assert inst.is_blocking() is False


def test_from_llm_output_stamps_proposal_id_and_converts_to_tuples() -> None:
    """Bridge from Pydantic LlmCritiqueVerdictOutput to the dataclass.
    Stamps proposal_id (framework-deterministic); converts
    likely_neighbor_regressions list → tuple for the frozen dataclass."""
    from genie_space_optimizer.skills.candidate_critique.output_schema import (
        LlmCritiqueVerdictOutput,
    )

    pyd = LlmCritiqueVerdictOutput(
        addresses_target_failure=True,
        is_overgeneralized=True,
        likely_neighbor_regressions=["gs_044", "gs_055"],
        matches_intended_shape=True,
        overall_recommendation="rework",
        rationale="x",
    )
    cv = CritiqueVerdict.from_llm_output(pyd, proposal_id="prop_H001_AG3_001")
    assert cv.proposal_id == "prop_H001_AG3_001"
    assert isinstance(cv.likely_neighbor_regressions, tuple)
    assert cv.likely_neighbor_regressions == ("gs_044", "gs_055")
    assert cv.overall_recommendation == "rework"


def test_reason_code_helper_maps_recommendation_to_postmortem_code() -> None:
    """Postmortem cardinality control: each recommendation maps to
    exactly one ReasonCode for grouping."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    for rec, code in (
        ("proceed", ReasonCode.CRITIQUE_PROCEED),
        ("rework", ReasonCode.CRITIQUE_REWORK),
        ("discard", ReasonCode.CRITIQUE_DISCARD),
    ):
        cv = CritiqueVerdict(
            proposal_id="prop_001",
            addresses_target_failure=True,
            is_overgeneralized=False,
            likely_neighbor_regressions=(),
            matches_intended_shape=True,
            overall_recommendation=rec,  # type: ignore[arg-type]
            rationale="x",
        )
        assert cv.reason_code() == code
