"""Step §H of the production-seam wire-in plan.

``_run_post_apply_eval`` now adapts
``stages.evaluation.evaluate_post_patch``. Pulls eval kwargs and stage
context from ``TransformerContext``; finds the row matching the
state's QID in the result.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
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
    evaluated_gate as eval_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_applied():
    s = build_initial_state(
        qid="q1", iteration=1,
        seen=HardQidSeenRecord(
            "er_orig", "row_is_hard_failure", 0.0,
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
    return s.advance(
        FunnelStage.APPLIED,
        StageTransition(
            FunnelStage.APPLYABLE, FunnelStage.APPLIED, 6, "t", "gate",
        ),
        applied=AppliedRecord(
            applied_at_ms=10, apply_call_id="ac_1",
            proposal_attempt_index=0, applied_intent_ids=("intent_1",),
        ),
    )


def _ctx(**kw) -> TransformerContext:
    base = dict(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        space_id="space_abc",
        eval_qids=("q1",),
        stage_ctx=object(),  # opaque legacy ctx
        eval_kwargs=object(),
        metadata_snapshot={"schema_columns": []},
    )
    base.update(kw)
    return TransformerContext(**base)


def _fake_result(score: float, sql: str, eval_row_id: str = "er_post"):
    from dataclasses import dataclass

    @dataclass
    class _R:
        scoreboard: dict
        eval_rows: tuple
        hard_failure_qids: tuple = ()
        soft_signal_qids: tuple = ()
        already_passing_qids: tuple = ()
        gt_correction_candidate_qids: tuple = ()
        per_qid_judge: dict = None
        asi_metadata: dict = None
        eval_provenance: dict = None
        raw: dict = None

    return _R(
        scoreboard={},
        eval_rows=(
            {
                "question_id": "q1",
                "generated_sql": sql,
                "eval_row_id": eval_row_id,
                "feedback/result_correctness/value": score,
            },
        ),
    )


def test_passing_eval_advances_to_evaluated(monkeypatch):
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.evaluation.evaluate_post_patch",
        lambda ctx, inp, *, eval_kwargs: _fake_result(
            1.0, "SELECT COUNT(*) FROM t",
        ),
    )

    s = _state_at_applied()
    out = eval_module.evaluated_gate.transform(s, _ctx())

    assert out.current_stage == FunnelStage.EVALUATED
    assert out.evaluated is not None
    assert out.evaluated.post_apply_score == 1.0
    assert "COUNT" in out.evaluated.post_apply_sql
    assert out.evaluated.eval_row_id_post == "er_post"
    # Pre-apply baseline preserved from state.seen.
    assert out.evaluated.pre_apply_sql == "SELECT 1"


def test_failing_eval_still_advances_to_evaluated(monkeypatch):
    """The evaluated gate records the post-apply score; acceptance is
    the *next* gate's call. A 0.0 score still advances to EVALUATED."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.evaluation.evaluate_post_patch",
        lambda ctx, inp, *, eval_kwargs: _fake_result(0.0, "SELECT 1"),
    )

    s = _state_at_applied()
    out = eval_module.evaluated_gate.transform(s, _ctx())
    assert out.current_stage == FunnelStage.EVALUATED
    assert out.evaluated.post_apply_score == 0.0


def test_missing_row_for_qid_terminates(monkeypatch):
    """evaluate_post_patch returned rows but none for this state's QID
    → terminate cleanly with OPTIMIZER_INVARIANT_VIOLATION."""
    from dataclasses import dataclass

    @dataclass
    class _R:
        scoreboard: dict
        eval_rows: tuple
        hard_failure_qids: tuple = ()
        soft_signal_qids: tuple = ()
        already_passing_qids: tuple = ()
        gt_correction_candidate_qids: tuple = ()
        per_qid_judge: dict = None
        asi_metadata: dict = None
        eval_provenance: dict = None
        raw: dict = None

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.evaluation.evaluate_post_patch",
        lambda ctx, inp, *, eval_kwargs: _R(
            scoreboard={},
            eval_rows=({"question_id": "someone_else", "generated_sql": "x"},),
        ),
    )

    s = _state_at_applied()
    out = eval_module.evaluated_gate.transform(s, _ctx())
    assert out.current_stage == FunnelStage.TERMINATED


def test_evaluate_post_patch_raises_terminates(monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("eval infra down")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.evaluation.evaluate_post_patch",
        boom,
    )

    s = _state_at_applied()
    out = eval_module.evaluated_gate.transform(s, _ctx())
    assert out.current_stage == FunnelStage.TERMINATED
