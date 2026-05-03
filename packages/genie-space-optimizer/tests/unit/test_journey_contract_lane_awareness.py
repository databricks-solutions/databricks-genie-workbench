"""Lane-aware journey validation regression tests.

The flat validator emits illegal_transition for every same-stage adjacent
pair when a qid has >1 proposal in one iteration. The lane-aware
validator splits per-qid events into trunk + per-proposal_id chains and
validates each chain independently.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)
from genie_space_optimizer.optimization.question_journey_contract import (
    validate_question_journeys,
)


def _ev(qid: str, stage: str, **fields) -> QuestionJourneyEvent:
    return QuestionJourneyEvent(question_id=qid, stage=stage, **fields)


def test_single_proposal_lane_remains_valid():
    events = [
        _ev("q1", "evaluated"),
        _ev("q1", "clustered", cluster_id="C1"),
        _ev("q1", "ag_assigned", ag_id="AG_1"),
        _ev("q1", "proposed", proposal_id="P_1", patch_type="instruction"),
        _ev("q1", "applied", proposal_id="P_1", patch_type="instruction"),
        _ev("q1", "rolled_back", ag_id="AG_1"),
        _ev("q1", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    report = validate_question_journeys(events=events, eval_qids=["q1"])
    assert report.violations == [], f"expected 0 violations, got {report.violations}"
    assert report.is_valid


def test_multiple_proposal_lanes_validate_independently_no_cross_lane_illegal_transition():
    """Three proposal lanes for the same qid in one iteration.

    Lane P_A: proposed -> applied -> rolled_back   (legal)
    Lane P_B: proposed -> dropped_at_grounding     (legal)
    Lane P_C: proposed -> applied -> rolled_back   (legal)

    The flat validator reports `proposed -> proposed`, `applied ->
    applied`, `applied -> dropped_at_grounding`, etc. The lane-aware
    validator returns ZERO violations.
    """
    events = [
        _ev("q1", "evaluated"),
        _ev("q1", "clustered", cluster_id="C1"),
        _ev("q1", "ag_assigned", ag_id="AG_1"),
        _ev("q1", "proposed", proposal_id="P_A", patch_type="instruction"),
        _ev("q1", "proposed", proposal_id="P_B", patch_type="join_template"),
        _ev("q1", "proposed", proposal_id="P_C", patch_type="example_sql"),
        _ev("q1", "applied", proposal_id="P_A", patch_type="instruction"),
        _ev("q1", "applied", proposal_id="P_C", patch_type="example_sql"),
        _ev("q1", "dropped_at_grounding", proposal_id="P_B", patch_type="join_template"),
        _ev("q1", "rolled_back", ag_id="AG_1"),
        _ev("q1", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    report = validate_question_journeys(events=events, eval_qids=["q1"])
    assert report.violations == [], (
        f"expected 0 violations, got: "
        f"{[(v.kind, v.detail) for v in report.violations]}"
    )
    assert report.is_valid


def test_genuinely_illegal_transition_is_still_caught_within_a_lane():
    """If the trunk regresses from `applied -> evaluated`, that is illegal."""
    events = [
        _ev("q1", "evaluated"),
        _ev("q1", "clustered", cluster_id="C1"),
        _ev("q1", "ag_assigned", ag_id="AG_1"),
        _ev("q1", "proposed", proposal_id="P_X"),
        _ev("q1", "applied", proposal_id="P_X"),
        _ev("q1", "evaluated"),  # illegal regression within the trunk
        _ev("q1", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    report = validate_question_journeys(events=events, eval_qids=["q1"])
    assert any(
        v.kind == "illegal_transition" for v in report.violations
    ), f"expected at least one illegal_transition, got {report.violations}"


def test_lane_terminal_does_not_require_post_eval_per_lane():
    """A lane that ends at `dropped_at_grounding` is terminal for that lane;
    only the trunk needs a post_eval closer.
    """
    events = [
        _ev("q1", "evaluated"),
        _ev("q1", "clustered", cluster_id="C1"),
        _ev("q1", "ag_assigned", ag_id="AG_1"),
        _ev("q1", "proposed", proposal_id="P_X"),
        _ev("q1", "dropped_at_grounding", proposal_id="P_X"),
        # No applied/rolled_back for P_X — that lane is terminally dropped.
        _ev("q1", "post_eval", was_passing=False, is_passing=False, transition="hold_fail"),
    ]
    report = validate_question_journeys(events=events, eval_qids=["q1"])
    assert report.violations == [], f"got {report.violations}"


def test_trunk_only_qid_validates_unchanged():
    """A qid with no proposals (already-passing path) still validates."""
    events = [
        _ev("q1", "evaluated"),
        _ev("q1", "already_passing"),
        _ev("q1", "post_eval", was_passing=True, is_passing=True, transition="hold_pass"),
    ]
    report = validate_question_journeys(events=events, eval_qids=["q1"])
    assert report.violations == []
    assert report.is_valid
