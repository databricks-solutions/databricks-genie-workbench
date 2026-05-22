"""Step §I of the production-seam wire-in plan.

``_assess_collateral`` now computes per-QID collateral regression by
comparing ``ctx.baseline_eval_rows`` (pre-apply) with
``ctx.post_apply_eval_rows`` (set by §H), excluding the rejected
proposal's ``target_qids``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    acceptance_gate as accept_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_evaluated(post_score=1.0, pre_score=0.0):
    s = build_initial_state(
        qid="q1", iteration=1,
        seen=HardQidSeenRecord(
            "er", "row_is_hard_failure", pre_score,
            "SELECT 1", "agg", 1,
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
            attempt_index=0, intent_id="intent_1",
            patch_type="add_column_synonym",
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied", outcome_reason="pending_gates",
        ),),
    )
    s = s.advance(
        FunnelStage.NORMALIZED,
        StageTransition(
            FunnelStage.PROPOSED, FunnelStage.NORMALIZED, 4, "t", "gate",
        ),
    )
    s = s.advance(
        FunnelStage.APPLYABLE,
        StageTransition(
            FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, 5, "t", "gate",
        ),
    )
    s = s.advance(
        FunnelStage.APPLIED,
        StageTransition(
            FunnelStage.APPLYABLE, FunnelStage.APPLIED, 6, "t", "gate",
        ),
        applied=AppliedRecord(
            applied_at_ms=10, apply_call_id="ac_1",
            proposal_attempt_index=0, applied_intent_ids=("intent_1",),
        ),
    )
    return s.advance(
        FunnelStage.EVALUATED,
        StageTransition(
            FunnelStage.APPLIED, FunnelStage.EVALUATED, 7, "t", "gate",
        ),
        evaluated=EvaluatedRecord(
            pre_apply_score=pre_score,
            post_apply_score=post_score,
            pre_apply_sql="SELECT 1",
            post_apply_sql="SELECT COUNT(*) FROM t",
            eval_row_id_post="er_post",
        ),
    )


def _make_proposal():
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id="intent_1",
        intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_COLUMN_SYNONYM,
        rationale="r", confidence="high",
        patch_body={"object_id": "t:c"},
        blame_set=("t:c",),
        target_qids=("q1",),
    )


def _ctx(*, baseline=(), post=()) -> TransformerContext:
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        baseline_eval_rows=baseline,
        post_apply_eval_rows=post,
    )
    ctx.proposal_store.remember(_make_proposal())
    return ctx


def test_target_fixed_no_collateral_advances_to_accepted():
    """post_score > pre_score, no collateral regression → ACCEPTED."""
    s = _state_at_evaluated(post_score=1.0, pre_score=0.0)
    ctx = _ctx(baseline=(), post=())
    out = accept_module.acceptance_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.ACCEPTED
    assert out.accepted is not None
    assert out.accepted.decision == "accepted"


def test_target_not_fixed_terminates_no_gain():
    """post_score <= pre_score → OPTIMIZER_TRIED_NO_GAIN terminal."""
    s = _state_at_evaluated(post_score=0.0, pre_score=0.0)
    ctx = _ctx()
    out = accept_module.acceptance_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert out.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
    assert "target_unchanged" in out.terminal.reason


def test_target_fixed_but_collateral_regression_terminates():
    """post_score > pre_score, but another QID went from passing →
    failing → OPTIMIZER_TRIED_NO_GAIN with collateral_regressions
    surfaced in the reason."""
    baseline = (
        {"question_id": "q1", "feedback/result_correctness/value": 0.0},
        {"question_id": "q_collateral", "feedback/result_correctness/value": 1.0},
    )
    post = (
        {"question_id": "q1", "feedback/result_correctness/value": 1.0},
        {"question_id": "q_collateral", "feedback/result_correctness/value": 0.0},
    )
    s = _state_at_evaluated(post_score=1.0, pre_score=0.0)
    ctx = _ctx(baseline=baseline, post=post)
    out = accept_module.acceptance_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.TERMINATED
    assert "q_collateral" in out.terminal.reason


def test_target_qid_regression_is_not_counted_as_collateral():
    """If only the target QID changed (which is intentional), there is
    no collateral regression — gate should accept."""
    baseline = (
        {"question_id": "q1", "feedback/result_correctness/value": 0.0},
        {"question_id": "q_other", "feedback/result_correctness/value": 1.0},
    )
    post = (
        {"question_id": "q1", "feedback/result_correctness/value": 1.0},
        {"question_id": "q_other", "feedback/result_correctness/value": 1.0},
    )
    s = _state_at_evaluated(post_score=1.0, pre_score=0.0)
    ctx = _ctx(baseline=baseline, post=post)
    out = accept_module.acceptance_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.ACCEPTED


def test_empty_eval_rows_no_collateral_detected():
    """When ctx carries no eval rows (v3 iteration 1 without harness
    plumbing), collateral defaults to () — safe accept on target_fixed."""
    s = _state_at_evaluated(post_score=1.0, pre_score=0.0)
    ctx = _ctx(baseline=(), post=())
    out = accept_module.acceptance_gate.transform(s, ctx)
    assert out.current_stage == FunnelStage.ACCEPTED
