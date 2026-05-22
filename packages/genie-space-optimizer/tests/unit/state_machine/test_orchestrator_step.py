"""StateMachine.step() routes through registered transformers; emits one marker per transition."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.orchestrator import (
    StateMachine,
)
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
        iteration=1, run_id="r", validation_context=ValidationContext(1, "r", {}),
    )


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1)


def test_step_applies_registered_transformer_and_advances_stage(capsys):
    gate = ValidationGate(
        name="ok_gate",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=lambda s, c: GateVerdict.success(),
    )
    sm = StateMachine(transformers={FunnelStage.HARD_QID_SEEN: (gate,)})
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s2 = sm.step(s, _ctx())
    assert s2.current_stage == FunnelStage.DIAGNOSED

    captured = capsys.readouterr().out
    assert "GSO_QSTATE_TRANSITION_V1" in captured


def test_step_no_transformers_returns_state_unchanged():
    sm = StateMachine(transformers={})
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    assert sm.step(s, _ctx()) is s


def test_step_stops_at_terminated(capsys):
    from genie_space_optimizer.optimization.state_machine.records import TerminalRecord

    reject_gate = ValidationGate(
        name="reject",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=lambda s, c: GateVerdict.reject_terminal(
            TerminalRecord("OPTIMIZER_NO_CANDIDATES", "no", FunnelStage.HARD_QID_SEEN, "sig")
        ),
    )
    never_ran = ValidationGate(
        name="never",
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage_on_success=FunnelStage.DIAGNOSED,
        to_stage_on_reject=FunnelStage.TERMINATED,
        predicate=lambda s, c: GateVerdict.success(),
    )
    sm = StateMachine(transformers={FunnelStage.HARD_QID_SEEN: (reject_gate, never_ran)})
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s2 = sm.step(s, _ctx())
    assert s2.current_stage == FunnelStage.TERMINATED
    out = capsys.readouterr().out
    # Exactly one transition marker emitted (the reject), not two.
    assert out.count("GSO_QSTATE_TRANSITION_V1") == 1
