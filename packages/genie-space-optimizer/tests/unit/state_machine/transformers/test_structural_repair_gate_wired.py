"""Step §E of the production-seam wire-in plan.

``_proposal_passes_structural_check`` now adapts the legacy
``enforce_structural_repair_shape`` (in
``optimization.structural_repair_gate``) and reads the typed proposal
from ``ctx.proposal_store``.
"""
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
    structural_repair_gate as gate_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _make_proposal(patch_type_str: str, intent_id: str = "intent_1"):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n",
        intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type_str),
        rationale="r",
        confidence="high",
        patch_body={
            "example_question": "?",
            "example_sql": "SELECT 1",
        } if patch_type_str == "add_example_sql" else {"object_id": "t:c"},
        blame_set=("catalog.schema.t:c",),
        target_qids=("q1",),
    )


def _state_at_proposed(intent_id: str = "intent_1"):
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
            source="plan11_stage1", rca_kind_label="k",
            evidence_summary="s", observed_failure="f",
            expected_sql_shape="e", confidence="high", rca_card_id="rca_1",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "llm",
        ),
        clustered=ClusterMembershipRecord(
            cluster_id="H001", ag_id="AG_H001", co_member_qids=("q1",),
            effective_target_lever=0, routing_evidence_kind="ek",
        ),
    )
    return s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(ProposalAttempt(
            attempt_index=0, intent_id=intent_id, patch_type="any",
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied", outcome_reason="pending_gates",
        ),),
    )


def _ctx_with_proposal(rp) -> TransformerContext:
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    ctx.proposal_store.remember(rp)
    return ctx


def test_structural_intent_with_structural_emitted_passes():
    """add_join_spec is a structural intent + structural emitted shape →
    gate admits → state advances to NORMALIZED."""
    rp = _make_proposal("add_join_spec")
    s = _state_at_proposed(intent_id=rp.intent_id)
    ctx = _ctx_with_proposal(rp)

    out = gate_module.structural_repair_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.NORMALIZED


def test_instruction_intent_admitted_with_instruction_emitted():
    """add_instruction has non-structural intent + INSTRUCTION emitted →
    legacy gate admits (fail-open for non-structural intent)."""
    rp = _make_proposal("add_instruction")
    s = _state_at_proposed(intent_id=rp.intent_id)
    ctx = _ctx_with_proposal(rp)

    out = gate_module.structural_repair_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.NORMALIZED


def test_proposal_store_miss_cycles_back_to_proposed():
    """No proposal in the store under the latest attempt's intent_id →
    treat as gate-fail with proposal_store_miss reason. The state
    stays at PROPOSED (cycle back so escalation_ladder can pick up)."""
    s = _state_at_proposed(intent_id="intent_missing")
    ctx = TransformerContext(  # no proposal store population
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )

    out = gate_module.structural_repair_gate.transform(s, ctx)
    # ValidationGate's to_stage_on_reject is PROPOSED (cycle back).
    assert out.current_stage == FunnelStage.PROPOSED
    # Latest proposal attempt should record structural_repair_rejected.
    assert out.proposals[-1].outcome == "structural_repair_rejected"
    assert "proposal_store_miss" in out.proposals[-1].outcome_reason
