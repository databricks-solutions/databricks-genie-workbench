"""Plan 8 Task 9 — plan7_inputs.build_critique_verdicts_by_intent_id
wraps the Task 5 legacy-shape join helper so the harness wire-in
needs only one import."""
from __future__ import annotations

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.plan7_inputs import (
    build_critique_verdicts_by_intent_id,
)
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueOutcome,
)


def _v(proposal_id: str = "P1") -> CritiqueVerdict:
    return CritiqueVerdict(
        proposal_id=proposal_id,
        addresses_target_failure=True, is_overgeneralized=False,
        likely_neighbor_regressions=(), matches_intended_shape=True,
        overall_recommendation="proceed", rationale="ok",
    )


def test_build_critique_verdicts_by_intent_id_uses_index():
    proposals_by_ag = {
        "AG_X": [{"proposal_id": "P1", "intent_id": "I1"}],
    }
    outcome = CritiqueOutcome(
        proposals_by_ag={"AG_X": ()},
        verdict_by_proposal_id={"P1": _v("P1")},
    )
    out = build_critique_verdicts_by_intent_id(outcome, proposals_by_ag)
    assert "I1" in out


def test_build_critique_verdicts_by_intent_id_handles_none_outcome():
    """When the critique stage didn't run, the builder returns {}."""
    out = build_critique_verdicts_by_intent_id(None, {"AG_X": []})
    assert out == {}
