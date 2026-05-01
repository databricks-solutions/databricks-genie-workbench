"""Pin that AG assignments are journey-emitted with ag_id + affected qids."""

from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_emit_ag_assignment_journey_records_ag_assigned_for_each_qid() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_ag_assignment_journey,
    )

    journey: list[QuestionJourneyEvent] = []

    def emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        for q in qids:
            journey.append(QuestionJourneyEvent(
                question_id=str(q), stage=stage, **fields,
            ))

    _emit_ag_assignment_journey(
        emit=emit,
        ag_id="AG_42",
        affected_qids=["q_001", "q_002", "q_003"],
    )

    stages_by_qid = {ev.question_id: ev.stage for ev in journey}
    assert stages_by_qid == {
        "q_001": "ag_assigned",
        "q_002": "ag_assigned",
        "q_003": "ag_assigned",
    }
    ag_ids = {ev.ag_id for ev in journey}
    assert ag_ids == {"AG_42"}


def test_emit_ag_assignment_journey_skips_empty_inputs() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_ag_assignment_journey,
    )

    journey: list[QuestionJourneyEvent] = []

    def emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        for q in qids:
            journey.append(QuestionJourneyEvent(
                question_id=str(q), stage=stage, **fields,
            ))

    _emit_ag_assignment_journey(emit=emit, ag_id="AG_1", affected_qids=[])
    _emit_ag_assignment_journey(emit=emit, ag_id="", affected_qids=["q1"])
    _emit_ag_assignment_journey(emit=emit, ag_id="AG_1", affected_qids=[None, ""])

    assert journey == []
