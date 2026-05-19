"""Plan 8 Task 5 — verdict_by_intent_id joins CritiqueOutcome.verdict_
by_proposal_id against each proposal's intent_id stamp."""
from __future__ import annotations

from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.critique_verdict_index import (
    verdict_by_intent_id,
)
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueOutcome,
)
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalSlate,
)


def _verdict(proposal_id: str = "P") -> CritiqueVerdict:
    return CritiqueVerdict(
        proposal_id=proposal_id,
        addresses_target_failure=True,
        is_overgeneralized=False,
        likely_neighbor_regressions=(),
        matches_intended_shape=True,
        overall_recommendation="proceed",
        rationale="ok",
    )


def test_verdict_by_intent_id_joins_keys():
    slate = ProposalSlate(
        proposals_by_ag={"AG_X": (
            {"proposal_id": "P001", "intent_id": "I001"},
            {"proposal_id": "P002", "intent_id": "I002"},
        )},
        repair_intents_by_id={},
    )
    outcome = CritiqueOutcome(
        proposals_by_ag={"AG_X": ()},
        verdict_by_proposal_id={
            "P001": _verdict("P001"),
            "P002": _verdict("P002"),
        },
    )
    by_intent = verdict_by_intent_id(outcome, slate)
    assert set(by_intent.keys()) == {"I001", "I002"}


def test_verdict_by_intent_id_skips_unstamped_proposals():
    slate = ProposalSlate(
        proposals_by_ag={"AG_X": (
            {"proposal_id": "P001", "intent_id": "I001"},
            {"proposal_id": "P002"},  # legacy / unstamped
        )},
        repair_intents_by_id={},
    )
    outcome = CritiqueOutcome(
        proposals_by_ag={"AG_X": ()},
        verdict_by_proposal_id={
            "P001": _verdict("P001"),
            "P002": _verdict("P002"),
        },
    )
    by_intent = verdict_by_intent_id(outcome, slate)
    assert set(by_intent.keys()) == {"I001"}


def test_verdict_by_intent_id_from_proposals_by_ag_legacy_shape():
    """Plan 8 v2 — harness consumes the legacy dict shape, not a
    typed ProposalSlate. Exercises the underscore-suffixed helper
    that Plan 8 Task 9 uses for the harness Plan 7 wire-in."""
    from genie_space_optimizer.optimization.critique_verdict_index import (
        verdict_by_intent_id_from_proposals_by_ag,
    )
    proposals_by_ag = {
        "AG_X": [
            {"proposal_id": "P001", "intent_id": "I001"},
            {"proposal_id": "P002", "intent_id": "I002"},
            {"proposal_id": "P003"},  # legacy / unstamped
        ],
    }
    outcome = CritiqueOutcome(
        proposals_by_ag={"AG_X": ()},
        verdict_by_proposal_id={
            "P001": _verdict("P001"),
            "P002": _verdict("P002"),
            "P003": _verdict("P003"),
        },
    )
    by_intent = verdict_by_intent_id_from_proposals_by_ag(
        outcome, proposals_by_ag,
    )
    assert set(by_intent.keys()) == {"I001", "I002"}
