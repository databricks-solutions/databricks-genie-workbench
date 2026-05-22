"""StateMachine.run_iteration() drives a batch of states to settled."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.orchestrator import StateMachine
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    ValidationGate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict,
    TransformerContext,
    ValidationContext,
)


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1)


def _ctx() -> TransformerContext:
    return TransformerContext(
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )


def test_run_iteration_drives_each_state_to_settled():
    advance_once = ValidationGate(
        name="advance",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=lambda s, c: GateVerdict.success(),
    )
    sm = StateMachine(transformers={FunnelStage.HARD_QID_SEEN: (advance_once,)})
    states = tuple(
        build_initial_state(qid=f"gs_{i:03}", iteration=1, seen=_seen())
        for i in range(3)
    )
    final = sm.run_iteration(states, _ctx())
    assert len(final) == 3
    for s in final:
        assert s.current_stage == FunnelStage.DIAGNOSED
