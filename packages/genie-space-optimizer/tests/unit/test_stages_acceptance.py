from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    AgOutcome,
    AgOutcomeRecord,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=3,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )


def _row(qid: str, *, hard: bool = False, passing: bool = False) -> dict:
    if passing:
        return {"question_id": qid, "result_correctness": "yes",
                "arbiter": "both_correct"}
    if hard:
        return {"question_id": qid, "result_correctness": "no",
                "arbiter": "ground_truth_correct"}
    return {"question_id": qid, "result_correctness": "yes",
            "arbiter": "genie_correct"}


def test_acceptance_input_required_fields() -> None:
    inp = AcceptanceInput(
        applied_entries_by_ag={"AG_001": (
            {"patch": {"proposal_id": "P_001", "ag_id": "AG_001",
                       "target_qids": ["q1"], "content_fingerprint": "abc"}},
        )},
        ags=({"id": "AG_001", "affected_questions": ["q1"]},),
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,
        baseline_pre_arbiter_accuracy=83.3,
        candidate_pre_arbiter_accuracy=87.5,
        pre_rows=(_row("q1", hard=True),),
        post_rows=(_row("q1", passing=True),),
    )
    assert inp.candidate_pre_arbiter_accuracy == 87.5


def test_ag_outcome_record_required_fields() -> None:
    rec = AgOutcomeRecord(
        ag_id="AG_001",
        outcome="accepted",
        reason_code="accepted_pre_arbiter_improvement",
        target_qids=("q1",),
        affected_qids=("q1",),
        content_fingerprints=("abc",),
    )
    assert rec.outcome == "accepted"


def test_ag_outcome_required_fields() -> None:
    out = AgOutcome(
        outcomes_by_ag={"AG_001": AgOutcomeRecord(
            ag_id="AG_001",
            outcome="accepted",
            reason_code="accepted_pre_arbiter_improvement",
            target_qids=("q1",),
            affected_qids=("q1",),
        )},
        qid_resolutions={"q1": "fail_to_pass"},
        rolled_back_content_fingerprints=set(),
    )
    assert out.outcomes_by_ag["AG_001"].reason_code == "accepted_pre_arbiter_improvement"


def test_decide_accepts_pre_arbiter_improvement_when_post_arbiter_saturated() -> None:
    """PR-E regression: post-arbiter saturated, pre-arbiter improved →
    accepted with reason_code=accepted_pre_arbiter_improvement."""
    from genie_space_optimizer.optimization.stages import acceptance as ac
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    # 22 passing + 2 hard before; same 22 + 2 hard after (post saturated).
    pre_rows = tuple(_row(f"q{i}", passing=True) for i in range(22)) + (
        _row("q23", hard=True), _row("q24", hard=True),
    )
    post_rows = pre_rows  # same hard set, pre-arbiter raw improved

    inp = ac.AcceptanceInput(
        applied_entries_by_ag={"AG_001": (
            {"patch": {"proposal_id": "P_001", "ag_id": "AG_001",
                       "target_qids": ["q23"], "content_fingerprint": "abc"}},
        )},
        ags=({"id": "AG_001", "affected_questions": ["q23"]},),
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,  # saturated
        baseline_pre_arbiter_accuracy=83.3,
        candidate_pre_arbiter_accuracy=87.5,  # +4.2pp
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    out = ac.decide(ctx, inp)
    rec = out.outcomes_by_ag["AG_001"]
    assert rec.outcome == "accepted"
    assert rec.reason_code == "accepted_pre_arbiter_improvement"
    # ACCEPTANCE_DECIDED record emitted.
    assert any(r.decision_type.value == "acceptance_decided" for r in captured)


def test_decide_rolls_back_when_post_arbiter_drops_with_collateral() -> None:
    """Post-arbiter regressed with out-of-target hard regression →
    rolled_back."""
    from genie_space_optimizer.optimization.stages import acceptance as ac
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    pre_rows = (
        _row("q1", passing=True),
        _row("q23", hard=True),
    )
    post_rows = (
        _row("q1", hard=True),  # collateral regression
        _row("q23", hard=True),
    )

    inp = ac.AcceptanceInput(
        applied_entries_by_ag={"AG_002": (
            {"patch": {"proposal_id": "P_002", "ag_id": "AG_002",
                       "target_qids": ["q23"], "content_fingerprint": "xyz"}},
        )},
        ags=({"id": "AG_002", "affected_questions": ["q23"]},),
        baseline_accuracy=50.0,
        candidate_accuracy=0.0,
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    out = ac.decide(ctx, inp)
    rec = out.outcomes_by_ag["AG_002"]
    assert rec.outcome == "rolled_back"
    # Content fingerprint of rolled-back patch is recorded for F6's
    # PR-E content-fingerprint dedup.
    assert "xyz" in out.rolled_back_content_fingerprints


def test_decide_emits_qid_resolution_records() -> None:
    """post_eval_resolution_records fires one record per eval qid."""
    from genie_space_optimizer.optimization.stages import acceptance as ac
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    pre_rows = (
        _row("q1", passing=True),
        _row("q2", hard=True),
    )
    post_rows = (
        _row("q1", passing=True),  # hold_pass
        _row("q2", passing=True),  # fail_to_pass
    )

    inp = ac.AcceptanceInput(
        applied_entries_by_ag={"AG_001": (
            {"patch": {"proposal_id": "P_001", "ag_id": "AG_001",
                       "target_qids": ["q2"], "content_fingerprint": "abc"}},
        )},
        ags=({"id": "AG_001", "affected_questions": ["q2"]},),
        baseline_accuracy=50.0,
        candidate_accuracy=100.0,
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    out = ac.decide(ctx, inp)
    # 2 QID_RESOLUTION records (one per eval qid).
    qid_resolution_records = [
        r for r in captured if r.decision_type.value == "qid_resolution"
    ]
    assert len(qid_resolution_records) == 2
    # qid_resolutions dict reflects the transitions.
    assert out.qid_resolutions == {"q1": "hold_pass", "q2": "fail_to_pass"}


def test_decide_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.stages import acceptance as ac
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ac.AcceptanceInput(
        applied_entries_by_ag={},
        ags=(),
        pre_rows=(),
        post_rows=(),
    )
    out = ac.decide(ctx, inp)
    assert out.outcomes_by_ag == {}
    assert out.qid_resolutions == {}
    assert out.rolled_back_content_fingerprints == set()
    assert captured == []
