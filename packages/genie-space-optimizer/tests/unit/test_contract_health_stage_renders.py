"""Phase H Fidelity Task 5 — pin Stage 14 (Contract Health) renderer.

Run ``3b050ec5-4032-457f-a785-2d1a3942a097`` exhibited Stage 13 reading
``(no decisions emitted for this stage in this iteration)`` despite
producer exceptions and invariant violations being present in the
postmortem evidence. The transcript renderer's
``_STAGE_DECISION_TYPE_MAP["contract_health"]`` was mapped to ``()``,
which is why the stage was permanently empty.

This test pins the new contract: PRODUCER_EXCEPTION and
INVARIANT_VIOLATION records render in the Contract Health stage so the
operator transcript surfaces contract-health regressions instead of hiding
them.

C15 Phase 1: contract_health moved from position 11 to position 13
after bundle_assembly (11) and run_manifest (12) were added to
PROCESS_STAGE_ORDER.
C15 Phase 2: contract_health moved from position 13 to position 14
after strategist_context was inserted at position 4.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.operator_process_transcript import (
    _STAGE_DECISION_TYPE_MAP,
    render_iteration_transcript,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
    OptimizationTrace,
    ReasonCode,
)


def test_contract_health_stage_includes_producer_exception_and_invariant() -> None:
    """The renderer's stage map must declare both contract-health
    decision types so they surface in Stage 11."""
    types = _STAGE_DECISION_TYPE_MAP["contract_health"]
    assert DecisionType.PRODUCER_EXCEPTION in types
    assert DecisionType.INVARIANT_VIOLATION in types


def test_render_stage_14_with_producer_exception_record() -> None:
    rec = DecisionRecord(
        run_id="r1",
        iteration=1,
        decision_type=DecisionType.PRODUCER_EXCEPTION,
        outcome=DecisionOutcome.FAILED,
        reason_code=ReasonCode.PRODUCER_EXCEPTION,
        reason_detail="ValueError: example failure",
    )
    trace = OptimizationTrace(
        journey_events=tuple(),
        decision_records=(rec,),
    )
    rendered = render_iteration_transcript(
        iteration=1,
        trace=trace,
        iteration_summary={"iteration": 1, "exit_path": "in_progress"},
    )
    assert "14. Contract Health" in rendered
    assert "producer_exception" in rendered
    # Reason detail must surface so operators see *which* producer
    # raised and what kind of error.
    assert "ValueError" in rendered


def test_render_stage_14_with_invariant_violation_record() -> None:
    rec = DecisionRecord(
        run_id="r1",
        iteration=2,
        decision_type=DecisionType.INVARIANT_VIOLATION,
        outcome=DecisionOutcome.FAILED,
        reason_code=ReasonCode.INVARIANT_VIOLATION,
        reason_detail="cap_conservation_repaired",
    )
    trace = OptimizationTrace(
        journey_events=tuple(),
        decision_records=(rec,),
    )
    rendered = render_iteration_transcript(
        iteration=2,
        trace=trace,
        iteration_summary={"iteration": 2, "exit_path": "completed"},
    )
    assert "14. Contract Health" in rendered
    assert "invariant_violation" in rendered
    assert "cap_conservation_repaired" in rendered


def test_render_stage_14_empty_when_no_contract_health_records() -> None:
    """When neither producer exceptions nor invariant violations exist,
    the stage continues to render the empty placeholder (back-compat)."""
    trace = OptimizationTrace(
        journey_events=tuple(),
        decision_records=tuple(),
    )
    rendered = render_iteration_transcript(
        iteration=1,
        trace=trace,
        iteration_summary={"iteration": 1, "exit_path": "completed"},
    )
    assert "14. Contract Health" in rendered
    # The placeholder text is the fallback for empty stages.
    assert "no decisions emitted for this stage" in rendered
