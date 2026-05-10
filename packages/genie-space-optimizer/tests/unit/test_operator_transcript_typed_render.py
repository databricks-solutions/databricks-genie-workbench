"""Unit tests for render_typed_stage_block (C15 Phase 5 Task 5.1)."""
from genie_space_optimizer.optimization.operator_process_transcript import (
    render_typed_stage_block,
)
from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    AgOutcome,
    AgOutcomeRecord,
)


def test_render_typed_stage_block_includes_input_and_output() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={"AG1": ()},
        ags=({"id": "AG1"},),
        baseline_accuracy=0.833,
        candidate_accuracy=0.958,
    )
    out = AgOutcome(
        outcomes_by_ag={
            "AG1": AgOutcomeRecord(
                ag_id="AG1",
                outcome="accepted",
                reason_code="accepted_with_attribution_drift",
                target_qids=("gs_024",),
                affected_qids=("gs_024",),
            ),
        },
    )
    text = render_typed_stage_block(
        stage_index=9,
        stage_key="acceptance_decision",
        inp=inp,
        out=out,
        markers_emitted=("GSO_FULL_EVAL_V1", "GSO_ATTRIBUTION_DRIFT_V1"),
    )
    assert "STAGE 9: acceptance_decision" in text
    assert "─ Input" in text
    assert "─ Output" in text
    assert "baseline_accuracy" in text
    assert "candidate_accuracy" in text
    assert "GSO_FULL_EVAL_V1" in text
    assert "GSO_ATTRIBUTION_DRIFT_V1" in text


def test_render_typed_stage_block_handles_no_markers() -> None:
    inp = AcceptanceInput()
    out = AgOutcome()
    text = render_typed_stage_block(
        stage_index=9,
        stage_key="acceptance_decision",
        inp=inp,
        out=out,
        markers_emitted=(),
    )
    assert "STAGE 9: acceptance_decision" in text
    assert "─ Markers emitted" not in text
