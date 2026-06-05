"""Step §F of the production-seam wire-in plan.

``_assess_blast_radius`` now adapts
``optimization.proposal_grounding.patch_blast_radius_is_safe``. Reads
the typed proposal from ``ctx.proposal_store`` to derive
``ag_target_qids``.
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
    blast_radius_batch as blast_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _make_proposal(intent_id="intent_1", *, passing_dependents=None,
                   high_collateral_risk=False):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    body = {"object_id": "t:c"}
    if passing_dependents is not None:
        body["passing_dependents"] = list(passing_dependents)
    if high_collateral_risk:
        body["high_collateral_risk"] = True
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_SYNONYM,
        rationale="r", confidence="high",
        patch_body=body,
        blame_set=("t:c",),
        target_qids=("q1", "q2"),
    )


def _state_at_normalized(intent_id="intent_1"):
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
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(
            FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm",
        ),
        proposals=(ProposalAttempt(
            attempt_index=0, intent_id=intent_id, patch_type="add_column_synonym",
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied", outcome_reason="pending_gates",
        ),),
    )
    return s.advance(
        FunnelStage.NORMALIZED,
        StageTransition(
            FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "t", "gate",
        ),
    )


def _ctx(rp) -> TransformerContext:
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        live_hard_qids=("q1", "q2"),
    )
    ctx.proposal_store.remember(rp)
    return ctx


def test_safe_when_passing_dependents_stamped_empty_advances_to_applyable():
    """Trial 20 E2 contract — ``passing_dependents`` MUST be stamped on
    every proposal (empty list = scanner ran and found none). An
    explicitly-stamped empty list is the safe-by-default signal; a
    MISSING field means E1 stamping never ran and the proposal is
    treated as unsafe (``passing_dependents_missing``) so plumbing
    regressions surface instead of being absorbed as false-safe.

    Pre-Trial-20 this test asserted that a MISSING ``passing_dependents``
    field was safe-by-default. That contract was the root cause behind
    postmortem 519131527536322 (the airline rollback) — Trial 20 E2
    explicitly flipped it to unsafe-by-default.
    """
    rp = _make_proposal(passing_dependents=[])  # scanner ran → no dependents
    s = _state_at_normalized(intent_id=rp.intent_id)
    out = blast_module.blast_radius_batch.transform(s, _ctx(rp))
    assert out.current_stage == FunnelStage.APPLYABLE


def test_missing_passing_dependents_is_rejected_under_trial20_e2():
    """Trial 20 E2 — a proposal that reaches blast_radius without
    ``passing_dependents`` stamped means E1 plumbing dropped the
    counterfactual scan. The gate emits
    ``GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1`` and rejects with reason
    ``passing_dependents_missing`` so the regression is loud, not
    silent.
    """
    rp = _make_proposal()  # no passing_dependents → unsafe under E2
    s = _state_at_normalized(intent_id=rp.intent_id)
    out = blast_module.blast_radius_batch.transform(s, _ctx(rp))
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].outcome == "blast_radius_rejected"
    assert "passing_dependents_missing" in out.proposals[-1].outcome_reason


def test_safe_when_outside_target_within_threshold():
    """passing_dependents present but all within target_qids → safe."""
    rp = _make_proposal(passing_dependents=["q1"])  # q1 IS in target_qids
    s = _state_at_normalized(intent_id=rp.intent_id)
    out = blast_module.blast_radius_batch.transform(s, _ctx(rp))
    assert out.current_stage == FunnelStage.APPLYABLE


def test_reject_when_outside_target_exceeds_threshold():
    """passing_dependents with a QID outside target_qids → reject (cycle
    back to PROPOSED) with collateral on the rejection record."""
    rp = _make_proposal(passing_dependents=["q_other"])  # outside target
    s = _state_at_normalized(intent_id=rp.intent_id)
    out = blast_module.blast_radius_batch.transform(s, _ctx(rp))
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].outcome == "blast_radius_rejected"
    assert "q_other" in out.proposals[-1].outcome_reason


def test_proposal_store_miss_treated_as_reject():
    """Latest ProposalAttempt has an intent_id not in the store → reject
    so escalation_ladder can pick up (rather than crash)."""
    s = _state_at_normalized(intent_id="intent_missing")
    ctx = TransformerContext(  # no proposal_store population
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        live_hard_qids=(),
    )
    out = blast_module.blast_radius_batch.transform(s, ctx)
    assert out.current_stage == FunnelStage.PROPOSED
    assert out.proposals[-1].outcome == "blast_radius_rejected"
    assert "proposal_store_miss" in out.proposals[-1].outcome_reason
