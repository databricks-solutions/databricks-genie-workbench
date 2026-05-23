"""Step §M wire-in tests for escalation_ladder rungs 1/2/3/4.

  * Rung 1 (structural_repair_rejected) → synthesize_escalation_for_state(SCOPED_L6)
  * Rung 2 (blast_radius_rejected)      → narrow_replacement_with_llm
  * Rung 3 (applyability_rejected)      → synthesize_escalation_for_state(ADD_EXAMPLE_SQL)
  * Rung 4 (3+ attempts)                → synthesize_escalation_for_state(NARROWED_EXAMPLE_SQL)
"""

import pytest

# SM Cutover Phase 3 (2026-05-23): routing_gate and escalation_ladder
# transformers were quarantined to ``optimization/_legacy/`` because the
# production state machine no longer mimics the legacy lever-cascade
# escalation inside the SM. These tests are kept for archival reference
# but are excluded from default test runs.
pytestmark = pytest.mark.skip(reason="legacy: routing_gate/escalation_ladder quarantined in SM Cutover Phase 3")

from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    escalation_ladder as ladder_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _make_proposal(intent_id="intent_failed", patch_type_str="add_join_spec"):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type_str),
        rationale="r", confidence="high",
        patch_body={"a": "b"},
        blame_set=("x:a",),
        target_qids=("q1",),
    )


def _state_with_attempt(outcome: str, attempts: int = 1):
    s = build_initial_state(
        qid="q1", iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "S", "x", 1,
        ),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            "plan11_stage1", "k", "s", "f", "e", "high", "rca",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "llm",
        ),
        clustered=ClusterMembershipRecord(
            "H001", "AG_H001", ("q1",), 0, "ek",
        ),
    )
    # Build N attempts; final attempt carries the requested outcome.
    proposals = ()
    for i in range(attempts - 1):
        proposals = proposals + (ProposalAttempt(
            attempt_index=i, intent_id=f"intent_{i}",
            patch_type="add_join_spec",
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="escalated",
            outcome_reason=f"escalated_to_attempt_{i + 1}",
            escalated_to_attempt_index=i + 1,
        ),)
    proposals = proposals + (ProposalAttempt(
        attempt_index=attempts - 1, intent_id="intent_failed",
        patch_type="add_join_spec",
        deepest_stage_in_attempt=FunnelStage.PROPOSED,
        outcome=outcome,
        outcome_reason="prior rejection reason",
    ),)
    return s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=proposals,
    )


def _ctx_with_proposal(rp) -> TransformerContext:
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        w=None,
    )
    ctx.proposal_store.remember(rp)
    return ctx


def _stub_synth_returning(monkeypatch, intent_id, patch_type):
    """Stub the unified dispatcher to return a proposal dict so the
    rung helpers' adapter machinery has something to wrap."""
    from dataclasses import dataclass

    @dataclass
    class _R:
        proposal: object = None
        attempted_archetypes: tuple = ()
        skipped_reason: str | None = None

    proposal_dict = {
        "intent_id": intent_id,
        "intent_name": "new", "intent_description": "d",
        "repair_shape": "other", "patch_type": patch_type,
        "rationale": "r", "confidence": "high",
        "patch_body": ({"a": "b"} if patch_type != "add_example_sql"
                       else {"example_question": "?", "example_sql": "S"}),
        "blame_set": ["x:a"], "target_objects": [],
        "required_constructs": [],
        "repair_hypothesis": "h", "target_qids": ["q1"],
    }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".synthesize_escalation_for_state",
        lambda **kw: _R(proposal=proposal_dict),
    )
    return proposal_dict


def test_rung_1_structural_repair_rejected_invokes_scoped_l6(monkeypatch):
    _stub_synth_returning(monkeypatch, "intent_rung1", "add_join_spec")

    rp = _make_proposal()
    s = _state_with_attempt("structural_repair_rejected")
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)

    # Ladder cycles back to PROPOSED with a new attempt.
    assert out.current_stage == FunnelStage.PROPOSED
    assert len(out.proposals) == len(s.proposals) + 1
    # Prior attempt marked as escalated; new attempt is in-flight.
    assert out.proposals[-2].outcome == "escalated"
    assert out.proposals[-1].intent_id == "intent_rung1"


def test_rung_2_blast_radius_rejected_uses_narrow_replacement(monkeypatch):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    narrowed = RepairProposal(
        intent_id="intent_rung2",
        intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_SYNONYM,
        rationale="r", confidence="high",
        patch_body={"object_id": "t:c"}, blame_set=("t:c",),
        target_qids=("q1",),
    )

    captured = {}

    def fake_narrow(patch, *, collateral_qids, protected_sql, cluster,
                    w, **kwargs):
        captured["patch"] = patch
        captured["collateral"] = collateral_qids
        return narrowed

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.narrow_replacement"
        ".narrow_replacement_with_llm",
        fake_narrow,
    )

    rp = _make_proposal()
    s = _state_with_attempt("blast_radius_rejected")
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].intent_id == "intent_rung2"
    # The rejected proposal was passed in to the narrow loop.
    assert captured["patch"].intent_id == "intent_failed"


def test_rung_3_applyability_rejected_invokes_add_example_sql(monkeypatch):
    _stub_synth_returning(monkeypatch, "intent_rung3", "add_example_sql")
    rp = _make_proposal()
    s = _state_with_attempt("applyability_rejected")
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].intent_id == "intent_rung3"


def test_rung_4_third_attempt_narrowed_example_sql(monkeypatch):
    _stub_synth_returning(monkeypatch, "intent_rung4", "add_example_sql")
    rp = _make_proposal()
    s = _state_with_attempt("structural_repair_rejected", attempts=3)
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].intent_id == "intent_rung4"


def test_ladder_exhausts_on_unmatched_outcome_terminates_safe_noop(monkeypatch):
    """An outcome the choose_rung function doesn't recognize → safe noop."""
    rp = _make_proposal()
    s = _state_with_attempt("contract_failed")  # no rung handles this
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert out.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP"


def test_synthesize_returns_none_terminates_safe_noop(monkeypatch):
    """When the rung's underlying synth declines, ladder safe-noops."""
    from dataclasses import dataclass

    @dataclass
    class _R:
        proposal: object = None
        attempted_archetypes: tuple = ()
        skipped_reason: str | None = "exception:declined"

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".synthesize_escalation_for_state",
        lambda **kw: _R(proposal=None),
    )

    rp = _make_proposal()
    s = _state_with_attempt("structural_repair_rejected")
    ctx = _ctx_with_proposal(rp)

    out = ladder_module.escalation_ladder.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert out.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP"
