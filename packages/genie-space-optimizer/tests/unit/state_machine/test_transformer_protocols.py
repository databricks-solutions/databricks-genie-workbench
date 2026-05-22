"""StateTransformer / LlmStateTransformer / ValidationGate / BatchTransformer protocol shape."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
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


def _ctx() -> TransformerContext:
    return TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1)


def test_validation_gate_returns_new_state_on_success():
    def predicate(state, vc):
        return GateVerdict.success()

    gate = ValidationGate(
        name="dummy_gate",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=predicate,
    )
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s2 = gate.transform(s, _ctx())
    assert s2.current_stage == FunnelStage.DIAGNOSED
    assert len(s2.transitions) == 1
    assert s2.transitions[0].transformer_name == "dummy_gate"
    assert s2.transitions[0].transition_kind == "validation_gate"


def test_validation_gate_terminates_on_reject_terminal():
    from genie_space_optimizer.optimization.state_machine.records import TerminalRecord

    def predicate(state, vc):
        return GateVerdict.reject_terminal(
            TerminalRecord("OPTIMIZER_NO_CANDIDATES", "no candidates", FunnelStage.HARD_QID_SEEN, "sig")
        )

    gate = ValidationGate(
        name="reject_gate",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=predicate,
    )
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s2 = gate.transform(s, _ctx())
    assert s2.current_stage == FunnelStage.TERMINATED
    assert s2.terminal is not None
    assert s2.terminal.kind == "OPTIMIZER_NO_CANDIDATES"


def test_validation_gate_protocol_attributes():
    g = ValidationGate(
        name="g",
        from_stage=FunnelStage.PROPOSED,
        to_stage_on_success=FunnelStage.NORMALIZED,
        to_stage_on_reject=FunnelStage.PROPOSED,
        predicate=lambda s, c: GateVerdict.success(),
    )
    assert g.name == "g"
    assert g.from_stage == FunnelStage.PROPOSED
    assert g.to_stage_on_success == FunnelStage.NORMALIZED
    assert g.to_stage_on_reject == FunnelStage.PROPOSED
