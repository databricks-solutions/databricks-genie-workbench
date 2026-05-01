"""Pin that every evaluated qid emits an evaluated event plus a classification event."""

from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_eval_entry_emits_evaluated_for_every_row() -> None:
    """When _emit_eval_entry_journey is called, every qid in the eval set
    must appear in the journey with an evaluated event."""
    from genie_space_optimizer.optimization.harness import (
        _emit_eval_entry_journey,
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

    _emit_eval_entry_journey(
        emit=emit,
        eval_qids=["gs_001", "gs_002", "gs_003", "gs_004"],
        already_passing_qids=["gs_001"],
        hard_qids=["gs_002"],
        soft_qids=["gs_003"],
        gt_correction_qids=["gs_004"],
    )

    qids_with_evaluated = {
        ev.question_id for ev in journey if ev.stage == "evaluated"
    }
    assert qids_with_evaluated == {"gs_001", "gs_002", "gs_003", "gs_004"}

    classifications = {
        ev.question_id: ev.stage for ev in journey if ev.stage != "evaluated"
    }
    assert classifications["gs_001"] == "already_passing"
    assert classifications["gs_003"] == "soft_signal"
    assert classifications["gs_004"] == "gt_correction_candidate"
    # gs_002 (hard) is intentionally not classified at entry; it gets
    # a 'clustered' event later.
    assert "gs_002" not in classifications
