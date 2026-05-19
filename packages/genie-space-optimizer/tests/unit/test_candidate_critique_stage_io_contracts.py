"""Plan 6 Task 8 — CritiqueInput / CritiqueOutcome JsonRoundTrip + I/O shape.

Pin the typed stage boundaries that match the existing F5/F6 pattern:
  ProposalSlate-shaped input + ProposalSlate-shaped output (preserves
  the slate dict structure so stages.gates reads it unchanged) PLUS
  a verdict_by_proposal_id sidecar.
"""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.stages.candidate_critique import (
    INPUT_CLASS,
    OUTPUT_CLASS,
    STAGE_KEY,
    CritiqueInput,
    CritiqueOutcome,
)


def test_stage_key_constant_matches_registry_key() -> None:
    assert STAGE_KEY == "candidate_critique"


def test_input_class_and_output_class_constants_exported() -> None:
    """Stage registry imports these by name."""
    assert INPUT_CLASS is CritiqueInput
    assert OUTPUT_CLASS is CritiqueOutcome


def test_critique_input_is_dataclass_mixing_json_round_trip() -> None:
    assert dataclasses.is_dataclass(CritiqueInput)
    assert issubclass(CritiqueInput, JsonRoundTrip)


def test_critique_input_field_set() -> None:
    fields = {f.name for f in dataclasses.fields(CritiqueInput)}
    assert fields == {
        "proposals_by_ag",
        "repair_intents_by_id",
        "rca_evidence_typed_by_cluster",
        "passing_qids_at_risk_by_proposal_id",
        "cluster_semantic_theme_by_cluster",
        "cluster_id_by_proposal_id",
        "ag_id_by_proposal_id",
    }


def test_critique_outcome_is_dataclass_mixing_json_round_trip() -> None:
    assert dataclasses.is_dataclass(CritiqueOutcome)
    assert issubclass(CritiqueOutcome, JsonRoundTrip)


def test_critique_outcome_field_set() -> None:
    fields = {f.name for f in dataclasses.fields(CritiqueOutcome)}
    assert fields == {
        "proposals_by_ag",
        "verdict_by_proposal_id",
        "dropped_by_critique",
        "advised_count",
    }


def test_critique_outcome_proposals_by_ag_matches_gates_input_shape() -> None:
    """stages.gates.GatesInput.proposals_by_ag is
    dict[str, tuple[dict[str, Any], ...]]. CritiqueOutcome's must match
    so the next stage reads it unchanged."""
    from genie_space_optimizer.optimization.stages.gates import GatesInput
    co = CritiqueOutcome(
        proposals_by_ag={"AG3": ({"proposal_id": "p1"},)},
        verdict_by_proposal_id={},
        dropped_by_critique=(),
        advised_count=0,
    )
    gi = GatesInput(
        proposals_by_ag=co.proposals_by_ag, ags=(),
    )
    assert gi.proposals_by_ag == co.proposals_by_ag


def test_critique_outcome_round_trips_through_to_json_from_json() -> None:
    verdict = CritiqueVerdict(
        proposal_id="prop_001",
        addresses_target_failure=True, is_overgeneralized=False,
        likely_neighbor_regressions=(), matches_intended_shape=True,
        overall_recommendation="proceed", rationale="x",
    )
    co = CritiqueOutcome(
        proposals_by_ag={"AG3": ({"proposal_id": "prop_001", "x": 1},)},
        verdict_by_proposal_id={"prop_001": verdict},
        dropped_by_critique=("prop_002",),
        advised_count=2,
    )
    payload = co.to_json()
    rebuilt = CritiqueOutcome.from_json(payload)
    assert rebuilt.proposals_by_ag == co.proposals_by_ag
    assert rebuilt.verdict_by_proposal_id == co.verdict_by_proposal_id
    assert rebuilt.dropped_by_critique == co.dropped_by_critique
    assert rebuilt.advised_count == 2


def test_critique_input_round_trips_through_to_json_from_json() -> None:
    intent = RepairIntent(
        intent_id="intent_001", intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=(), blame_set=(),
        rca_card_id="", ag_id="AG3",
    )
    ev = PerQidRcaEvidence(
        qid="gs_001", observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=(),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )
    ci = CritiqueInput(
        proposals_by_ag={"AG3": ({"proposal_id": "prop_001"},)},
        repair_intents_by_id={"intent_001": intent},
        rca_evidence_typed_by_cluster={"H001": {"gs_001": ev}},
        passing_qids_at_risk_by_proposal_id={"prop_001": ("gs_044",)},
        cluster_semantic_theme_by_cluster={"H001": "x"},
        cluster_id_by_proposal_id={"prop_001": "H001"},
        ag_id_by_proposal_id={"prop_001": "AG3"},
    )
    payload = ci.to_json()
    rebuilt = CritiqueInput.from_json(payload)
    assert rebuilt.passing_qids_at_risk_by_proposal_id == {
        "prop_001": ("gs_044",),
    }
    assert rebuilt.cluster_id_by_proposal_id == {"prop_001": "H001"}
    assert rebuilt.repair_intents_by_id["intent_001"] == intent
