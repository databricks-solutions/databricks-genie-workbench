"""Pin that AG outcomes and post_eval comparisons are journey-emitted."""

from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_emit_ag_outcome_journey_records_accepted_qids() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_ag_outcome_journey,
    )

    journey: list[QuestionJourneyEvent] = []

    def emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        for q in qids:
            journey.append(QuestionJourneyEvent(
                question_id=str(q), stage=stage, **fields,
            ))

    _emit_ag_outcome_journey(
        emit=emit,
        ag_id="AG1",
        outcome="accepted",
        affected_qids=["gs_001", "gs_002"],
    )
    stages = [ev.stage for ev in journey if ev.question_id == "gs_001"]
    assert "accepted" in stages


def test_emit_post_eval_journey_records_pass_fail_transition() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_post_eval_journey,
    )

    journey: list[QuestionJourneyEvent] = []

    def emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        qid = fields.pop("question_id", None)
        target = list(qids) if qids else ([qid] if qid else [])
        for q in target:
            journey.append(QuestionJourneyEvent(
                question_id=str(q), stage=stage, **fields,
            ))

    _emit_post_eval_journey(
        emit=emit,
        eval_qids=["gs_001", "gs_002"],
        was_passing_qids={"gs_001"},
        is_passing_qids={"gs_001", "gs_002"},
    )
    transitions = {
        ev.question_id: (ev.was_passing, ev.is_passing, ev.transition)
        for ev in journey if ev.stage == "post_eval"
    }
    assert transitions["gs_001"] == (True, True, "hold_pass")
    assert transitions["gs_002"] == (False, True, "fail_to_pass")
