"""Plan N1 Task 3 — pin the lane-key journey-validation defect.

Inspiration: run 2afb0be2-88b6-4832-99aa-c7e78fbc90f7 retry attempt
993610879088298. The producer keys ``proposed`` events on the parent
``proposal_id`` (e.g. ``P001``) and ``applied_targeted`` /
``applied_broad_ag_scope`` events on the **expanded** patch id (e.g.
``P001#1``, ``P001#2``). The validator's ``_split_trunk_and_lanes``
keys lanes on raw ``proposal_id``, so the parent lane has only
``proposed`` and the child lanes have only ``applied_targeted``,
which makes ``ag_assigned -> applied_targeted`` look illegal even
though the producer emitted the correct chain.

These tests pin the producer-side fix (``parent_proposal_id`` field
on ``QuestionJourneyEvent`` plus lane-key unification) and the
matching contract edges (Task 5).
"""
from __future__ import annotations


def test_proposed_and_applied_share_a_lane_under_parent_key() -> None:
    """``_split_trunk_and_lanes`` must collapse ``proposed`` (parent
    proposal id) and ``applied_targeted`` (expanded patch id) into
    the same lane when both events carry the same
    ``parent_proposal_id``. The lane key is the parent.
    """
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        _split_trunk_and_lanes,
    )

    events = [
        QuestionJourneyEvent(question_id="qid_x", stage="evaluated"),
        QuestionJourneyEvent(question_id="qid_x", stage="ag_assigned"),
        QuestionJourneyEvent(
            question_id="qid_x",
            stage="proposed",
            proposal_id="P001",
            parent_proposal_id="P001",
        ),
        QuestionJourneyEvent(
            question_id="qid_x",
            stage="applied_targeted",
            proposal_id="P001#1",
            parent_proposal_id="P001",
        ),
        QuestionJourneyEvent(question_id="qid_x", stage="post_eval"),
    ]
    _, lanes_by_key = _split_trunk_and_lanes(events)
    assert list(lanes_by_key.keys()) == ["P001"], (
        "lane-splitter must collapse parent + child events into a "
        "single lane keyed on parent_proposal_id; got "
        f"{list(lanes_by_key.keys())}"
    )
    chain = lanes_by_key["P001"]
    assert chain[0] == "proposed", chain
    assert "applied_targeted" in chain, chain


def test_validate_question_journeys_accepts_proposed_to_applied_targeted() -> None:
    """``_LEGAL_NEXT[PROPOSED]`` must permit ``APPLIED_TARGETED`` and
    ``APPLIED_BROAD_AG_SCOPE`` (Track 3/E split of ``APPLIED``).
    Today only the obsolete bare ``APPLIED`` is permitted; the split
    versions are emitted but never validated as legal successors.
    """
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        _LEGAL_NEXT,
    )

    legal_next_proposed = _LEGAL_NEXT[JourneyStage.PROPOSED]
    assert JourneyStage.APPLIED_TARGETED in legal_next_proposed, (
        "PROPOSED must transition to APPLIED_TARGETED; "
        f"got {sorted(s.value for s in legal_next_proposed)}"
    )
    assert JourneyStage.APPLIED_BROAD_AG_SCOPE in legal_next_proposed, (
        "PROPOSED must transition to APPLIED_BROAD_AG_SCOPE; "
        f"got {sorted(s.value for s in legal_next_proposed)}"
    )

    # And the new applied stages must terminate cleanly into
    # ACCEPTED / ROLLED_BACK / ACCEPTED_WITH_REGRESSION_DEBT, mirroring
    # the legacy APPLIED edges.
    expected_terminals = {
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
    }
    for stage in (
        JourneyStage.APPLIED_TARGETED,
        JourneyStage.APPLIED_BROAD_AG_SCOPE,
    ):
        assert _LEGAL_NEXT.get(stage, frozenset()) >= expected_terminals, (
            f"{stage.value} must transition to all of "
            f"{[s.value for s in expected_terminals]}; got "
            f"{sorted(s.value for s in _LEGAL_NEXT.get(stage, frozenset()))}"
        )


def test_lane_dedup_collapses_repeated_proposed_for_same_qid() -> None:
    """When the producer emits ``proposed`` twice for the same qid
    under the same ``parent_proposal_id`` (once for the H-cluster
    and once for the rca_* cluster), the validator must see exactly
    one ``proposed`` in the lane chain — multi-cluster routing is
    metadata, not a state change.
    """
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        _split_trunk_and_lanes,
    )

    events = [
        QuestionJourneyEvent(question_id="qid_x", stage="ag_assigned"),
        QuestionJourneyEvent(
            question_id="qid_x",
            stage="proposed",
            proposal_id="P001",
            parent_proposal_id="P001",
            cluster_id="H001",
        ),
        QuestionJourneyEvent(
            question_id="qid_x",
            stage="proposed",
            proposal_id="P001",
            parent_proposal_id="P001",
            cluster_id="rca_top_n_collapse",
        ),
    ]
    _, lanes_by_key = _split_trunk_and_lanes(events)
    chain = lanes_by_key.get("P001", [])
    proposed_count = sum(1 for s in chain if s == "proposed")
    assert proposed_count == 1, (
        f"lane chain must have exactly one ``proposed`` event for "
        f"qid_x even when emitted twice with different cluster_ids; "
        f"got {proposed_count}: chain={chain}"
    )
