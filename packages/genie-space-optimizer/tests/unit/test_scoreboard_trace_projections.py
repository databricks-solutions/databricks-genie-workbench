"""Phase D Task 2: trace projection helpers.

Covers:
- _records_for_iteration filters by iteration only.
- _records_by_type_for_iteration filters by both iteration and DecisionType.
- _events_by_qid groups journey events by question_id (iteration-implicit
  per the harness's per-iteration accumulation contract).
"""
from __future__ import annotations


def _make_record(**overrides):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord,
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    base = dict(
        run_id="run_demo",
        iteration=1,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.PROPOSAL_EMITTED,
        question_id="q1",
        proposal_id="P1",
    )
    base.update(overrides)
    return DecisionRecord(**base)


def _make_event(**overrides):
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )

    base = dict(question_id="q1", stage="evaluated")
    base.update(overrides)
    return QuestionJourneyEvent(**base)


def _make_trace(records, events=()):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )
    return OptimizationTrace(
        decision_records=tuple(records),
        journey_events=tuple(events),
    )


def test_records_for_iteration_filters_by_iteration():
    from genie_space_optimizer.optimization.scoreboard import (
        _records_for_iteration,
    )

    trace = _make_trace([
        _make_record(iteration=1, proposal_id="P1"),
        _make_record(iteration=2, proposal_id="P2"),
        _make_record(iteration=1, proposal_id="P3"),
    ])
    iter1 = list(_records_for_iteration(trace, iteration=1))
    assert {r.proposal_id for r in iter1} == {"P1", "P3"}


def test_records_by_type_for_iteration_filters_by_both():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )
    from genie_space_optimizer.optimization.scoreboard import (
        _records_by_type_for_iteration,
    )

    trace = _make_trace([
        _make_record(iteration=1, decision_type=DecisionType.PROPOSAL_GENERATED, proposal_id="P1"),
        _make_record(iteration=1, decision_type=DecisionType.PATCH_APPLIED, proposal_id="P2"),
        _make_record(iteration=2, decision_type=DecisionType.PROPOSAL_GENERATED, proposal_id="P3"),
    ])
    proposed = list(_records_by_type_for_iteration(
        trace, iteration=1, decision_type=DecisionType.PROPOSAL_GENERATED,
    ))
    assert [r.proposal_id for r in proposed] == ["P1"]


def test_events_by_qid_groups_events():
    from genie_space_optimizer.optimization.scoreboard import _events_by_qid

    trace = _make_trace([], events=[
        _make_event(question_id="q1", stage="evaluated"),
        _make_event(question_id="q1", stage="applied_targeted"),
        _make_event(question_id="q2", stage="evaluated"),
    ])
    grouped = _events_by_qid(trace)
    assert sorted(grouped.keys()) == ["q1", "q2"]
    assert [e.stage for e in grouped["q1"]] == ["evaluated", "applied_targeted"]
