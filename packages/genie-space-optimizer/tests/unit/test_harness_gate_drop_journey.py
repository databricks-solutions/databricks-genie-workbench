"""Pin that every gate that drops a proposal emits a dropped_at_<gate> event."""

from __future__ import annotations

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_emit_gate_drop_journey_records_each_dropped_proposal() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_gate_drop_journey,
    )

    journey: list[QuestionJourneyEvent] = []

    def emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        for q in qids:
            journey.append(QuestionJourneyEvent(
                question_id=str(q), stage=stage, **fields,
            ))

    dropped_proposals = [
        {
            "proposal_id": "P1", "cluster_id": "H001",
            "patch_type": "add_sql_snippet",
            "_grounding_target_qids": ["gs_002"],
            "_drop_reason": "missing_table_for_column",
        },
        {
            "proposal_id": "P2", "cluster_id": "H002",
            "patch_type": "update_column_description",
            "target_qids": ["gs_017"],
            "_drop_reason": "unknown_table",
        },
    ]
    _emit_gate_drop_journey(
        emit=emit,
        gate="grounding",
        dropped=dropped_proposals,
    )
    stages = [(ev.question_id, ev.stage, ev.reason) for ev in journey]
    assert ("gs_002", "dropped_at_grounding", "missing_table_for_column") in stages
    assert ("gs_017", "dropped_at_grounding", "unknown_table") in stages
