"""Phase 3 Task 3 — directive_outcome module unit tests.

Mirrors test_proposal_failure_policy patterns: closed-vocabulary
exhaustiveness, classifier branch coverage, dataclass frozen-ness, and
marker payload shape.
"""

from __future__ import annotations

import pytest


def test_directive_outcome_code_vocabulary_is_exactly_seven() -> None:
    """The closed vocabulary has exactly seven entries (Phase 6.5 added
    ``SYNTHESIS_ATTEMPTED_EMPTY``). Any future addition requires
    updating this assertion + the invariant + the inventory doc."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
    )

    assert sorted(c.value for c in DirectiveOutcomeCode) == sorted(
        [
            "proposal_emitted",
            "no_structural_candidate",
            "synthesis_attempted_empty",
            "force_llm_declined",
            "applyability_rejected",
            "collateral_rejected",
            "lever_not_proposal_generating",
        ]
    )


def test_classifier_lever_3_routes_to_not_proposal_generating() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=3,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.LEVER_NOT_PROPOSAL_GENERATING
    )


def test_classifier_lever_6_force_llm_declined() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=6,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=True,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.FORCE_LLM_DECLINED
    )


def test_classifier_lever_5_no_proposals_no_force_llm_routes_to_no_structural() -> None:
    """The 2314bb2c AG2 L5 shape — directive present, generator returned
    nothing, no structural-gate drops were recorded."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )


def test_classifier_all_proposals_applyability_dropped() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=6,
        proposals_emitted_count=3,
        structural_gate_drop_count=0,
        applyability_drop_count=3,
        collateral_drop_count=0,
        force_llm_declined=False,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.APPLYABILITY_REJECTED
    )


def test_classifier_all_proposals_collateral_dropped() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=6,
        proposals_emitted_count=2,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=2,
        force_llm_declined=False,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.COLLATERAL_REJECTED
    )


def test_classifier_some_proposals_survive_routes_to_proposal_emitted() -> None:
    """3 emitted, 1 applyability drop, 1 collateral drop, 1 survivor."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=3,
        structural_gate_drop_count=0,
        applyability_drop_count=1,
        collateral_drop_count=1,
        force_llm_declined=False,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.PROPOSAL_EMITTED
    )


def test_ledger_to_marker_payload_round_trips_via_json() -> None:
    """Marker payload must be JSON-serialisable so the stdout marker
    survives a json.dumps round-trip."""
    import json

    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )

    ledger = AgDirectiveLedger(
        ag_id="AG2",
        iteration=2,
        directives_present=(5, 6),
        outcomes_by_lever={
            5: DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
            6: DirectiveOutcomeCode.FORCE_LLM_DECLINED,
        },
    )
    payload = ledger.to_marker_payload()
    encoded = json.dumps(payload)
    decoded = json.loads(encoded)
    assert decoded == {
        "ag_id": "AG2",
        "iteration": 2,
        "directives_present": [5, 6],
        "outcomes_by_lever": {
            "5": "no_structural_candidate",
            "6": "force_llm_declined",
        },
    }


def test_ledger_is_frozen_on_top_level_fields_but_mutable_on_outcomes_dict() -> None:
    """The ledger dataclass is frozen so ag_id / iteration / directives_present
    cannot be reassigned, but ``outcomes_by_lever`` is intentionally a mutable
    dict so the harness can populate it inside the per-AG loop."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )

    ledger = AgDirectiveLedger(
        ag_id="AG2",
        iteration=2,
        directives_present=(5, 6),
    )

    with pytest.raises((AttributeError, TypeError)):
        ledger.ag_id = "AG3"  # type: ignore[misc]

    # The outcomes dict IS mutable — the harness populates it incrementally
    # as each lever loop iteration finishes.
    ledger.outcomes_by_lever[5] = DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    assert ledger.outcomes_by_lever[5] == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )
