"""Phase H Fidelity Task 5 — pin Stage 11 (Contract Health) renderer.

Run ``11110001-0000-4000-8000-000000000001`` exhibited Stage 11 reading
``(no decisions emitted for this stage in this iteration)`` despite
producer exceptions and invariant violations being present in the
postmortem evidence. The transcript renderer's
``_STAGE_DECISION_TYPE_MAP["contract_health"]`` was mapped to ``()``,
which is why the stage was permanently empty.

This test pins the new contract: PRODUCER_EXCEPTION and
INVARIANT_VIOLATION records render in Stage 11 so the operator
transcript surfaces contract-health regressions instead of hiding them.
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


def test_render_stage_11_with_producer_exception_record() -> None:
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
    assert "11. Contract Health" in rendered
    assert "producer_exception" in rendered
    # Reason detail must surface so operators see *which* producer
    # raised and what kind of error.
    assert "ValueError" in rendered


def test_render_stage_11_with_invariant_violation_record() -> None:
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
    assert "11. Contract Health" in rendered
    assert "invariant_violation" in rendered
    assert "cap_conservation_repaired" in rendered


def test_render_stage_11_empty_when_no_contract_health_records() -> None:
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
    assert "11. Contract Health" in rendered
    # The placeholder text is the fallback for empty stages.
    assert "no decisions emitted for this stage" in rendered
