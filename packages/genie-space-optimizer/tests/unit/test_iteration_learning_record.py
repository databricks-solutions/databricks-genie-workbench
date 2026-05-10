"""Phase H Fidelity Task 4 — pin Stage 11 (Learning / Next Action)
emission for no-op and rollback iteration paths.

Run ``3b050ec5-4032-457f-a785-2d1a3942a097`` exhibited Stage 10 reading
``(no decisions emitted for this stage in this iteration)`` for every
iteration despite the postmortem evidence being explicit:
``proposals_empty`` four iterations in a row plus a rollback in
iteration 1. Stage 10 was empty because ``_STAGE_DECISION_TYPE_MAP``
mapped the stage to ``(AG_RETIRED, QID_RESOLUTION)`` and the no-op
paths emit neither.

This test file pins the new contract: every iteration finalises with a
typed learning record summarising the exit path, AG-level counters, and
operator-facing next action. The renderer also surfaces these records
in Stage 11 so the operator transcript becomes the source of truth for
"what should the next iteration do".

C15 Phase 2: learning_next_action moved from position 10 to position 11
after strategist_context was inserted at position 4.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.decision_emitters import (
    iteration_learning_record,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


# ── Helper API contract ───────────────────────────────────────────


def test_iteration_learning_record_for_proposals_empty_path() -> None:
    rec = iteration_learning_record(
        run_id="r1",
        iteration=2,
        exit_path="proposals_empty",
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
    )
    assert rec is not None
    assert rec.decision_type == DecisionType.ITERATION_BUDGET_DECISION
    assert rec.outcome == DecisionOutcome.SKIPPED
    assert rec.reason_code == ReasonCode.PROPOSAL_GENERATION_EMPTY
    assert rec.iteration == 2
    assert rec.next_action  # operator-facing guidance must be present
    assert "proposals_empty" in (rec.metrics.get("exit_path") or "")


def test_iteration_learning_record_for_rolled_back_path() -> None:
    rec = iteration_learning_record(
        run_id="r1",
        iteration=1,
        exit_path="rolled_back",
        accepted_count=0,
        rolled_back_count=1,
        skipped_count=0,
        gate_drop_count=3,
    )
    assert rec.outcome == DecisionOutcome.ROLLED_BACK
    assert rec.reason_code == ReasonCode.PATCH_SKIPPED
    assert rec.metrics.get("rolled_back_count") == 1
    assert rec.metrics.get("gate_drop_count") == 3


def test_iteration_learning_record_for_accepted_path() -> None:
    rec = iteration_learning_record(
        run_id="r1",
        iteration=3,
        exit_path="completed",
        accepted_count=1,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
    )
    assert rec.outcome == DecisionOutcome.ACCEPTED
    assert rec.reason_code == ReasonCode.PATCH_APPLIED
    assert rec.metrics.get("accepted_count") == 1


def test_iteration_learning_record_for_unknown_exit_path_degrades_gracefully() -> None:
    """An unrecognised exit_path string must not crash the helper. The
    record degrades to ``DecisionOutcome.INFO`` so the iteration still
    has at least one Stage 11 line."""
    rec = iteration_learning_record(
        run_id="r1",
        iteration=4,
        exit_path="some_future_exit_path",
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
    )
    assert rec.decision_type == DecisionType.ITERATION_BUDGET_DECISION
    assert rec.outcome == DecisionOutcome.INFO


# ── Renderer integration ──────────────────────────────────────────


def test_learning_next_action_stage_renders_iteration_budget_decision() -> None:
    """Phase H Fidelity Task 4: the operator-transcript stage map must
    surface ITERATION_BUDGET_DECISION records in Stage 11 so the
    learning record actually appears in the operator transcript."""
    from genie_space_optimizer.optimization.operator_process_transcript import (
        _STAGE_DECISION_TYPE_MAP,
    )

    assert (
        DecisionType.ITERATION_BUDGET_DECISION
        in _STAGE_DECISION_TYPE_MAP["learning_next_action"]
    )


def test_full_transcript_renders_learning_record_in_stage_11() -> None:
    """End-to-end: when an iteration trace contains a learning record,
    Stage 11 of the rendered transcript must contain that record's text
    (decision_type + reason_code) instead of the empty placeholder.
    C15 Phase 2: learning_next_action moved from position 10 to 11."""
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )

    rec = iteration_learning_record(
        run_id="r1",
        iteration=1,
        exit_path="proposals_empty",
        accepted_count=0,
        rolled_back_count=0,
        skipped_count=0,
        gate_drop_count=0,
    )
    trace = OptimizationTrace(
        journey_events=tuple(),
        decision_records=(rec,),
    )
    rendered = render_iteration_transcript(
        iteration=1,
        trace=trace,
        iteration_summary={"iteration": 1, "exit_path": "proposals_empty"},
    )
    assert "11. Learning / Next Action" in rendered
    assert "iteration_budget_decision" in rendered
    assert "proposal_generation_empty" in rendered
