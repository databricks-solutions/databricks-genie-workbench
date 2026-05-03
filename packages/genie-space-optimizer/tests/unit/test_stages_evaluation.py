from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.evaluation import (
    EvaluationInput,
    EvaluationResult,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=1,
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


def test_evaluation_input_required_fields() -> None:
    inp = EvaluationInput(
        space_state={"id": "s1"},
        eval_qids=("q1", "q2"),
        run_role="iteration_eval",
        iteration_label="iter_01",
        scope="full",
    )
    assert inp.eval_qids == ("q1", "q2")
    assert inp.run_role == "iteration_eval"
    assert inp.scope == "full"


def test_evaluation_result_required_fields() -> None:
    res = EvaluationResult(
        scoreboard={"overall_accuracy": 0.875},
        hard_failure_qids=("q3",),
        soft_signal_qids=("q4",),
        already_passing_qids=("q1", "q2"),
        gt_correction_candidate_qids=(),
        eval_rows=({"qid": "q1", "passed": True},),
        per_qid_judge={},
        asi_metadata={},
        eval_provenance={"source_run_id": "r1"},
    )
    assert res.scoreboard["overall_accuracy"] == 0.875
    assert res.hard_failure_qids == ("q3",)


def test_evaluate_post_patch_partitions_rows_and_emits_classification(
    monkeypatch,
) -> None:
    """F1 contract: evaluate_post_patch(ctx, inp, eval_kwargs=...)
    returns an EvaluationResult whose hard / soft / already_passing /
    gt_correction partitions match the production control_plane.* row
    predicates, and one EVAL_CLASSIFIED record is emitted per row.
    """
    from genie_space_optimizer.optimization.stages import evaluation as eval_stage

    captured_records: list = []

    ctx = _stub_ctx()
    ctx.decision_emit = lambda record: captured_records.append(record)

    fake_rows = [
        {"question_id": "q1", "result_correctness": "yes",
         "arbiter": "both_correct"},                              # already_passing
        {"question_id": "q2", "result_correctness": "no",
         "arbiter": "ground_truth_correct"},                      # hard failure
        {"question_id": "q3", "result_correctness": "no",
         "arbiter": "neither_correct",
         "actionable_soft_signal": True},                         # soft (production-only path)
        {"question_id": "q4", "result_correctness": "yes",
         "arbiter": "genie_correct"},                             # gt_correction
    ]

    def _stub_run_evaluation(*a, **k):
        return {
            "rows": fake_rows,
            "overall_accuracy": 0.5,
            "pre_arbiter_accuracy": 0.5,
            "scores": {},
        }

    monkeypatch.setattr(eval_stage._eval_primitives, "run_evaluation", _stub_run_evaluation)

    inp = eval_stage.EvaluationInput(
        space_state={"id": "s1"},
        eval_qids=("q1", "q2", "q3", "q4"),
        run_role="iteration_eval",
        iteration_label="iter_01",
        scope="full",
    )

    out = eval_stage.evaluate_post_patch(
        ctx, inp,
        eval_kwargs={
            "space_id": "s1",
            "experiment_name": "exp",
            "iteration": 1,
            "benchmarks": [],
            "domain": "airline",
            "model_id": None,
            "eval_scope": "full",
            "predict_fn": MagicMock(),
            "scorers": [],
        },
    )

    assert "q2" in out.hard_failure_qids
    assert "q1" in out.already_passing_qids
    assert "q4" in out.gt_correction_candidate_qids
    # One EVAL_CLASSIFIED per row.
    assert len(captured_records) == len(fake_rows)


def test_classify_eval_rows_agrees_with_lever_loop_replay_partition() -> None:
    """The production partition (using control_plane predicates) must
    agree with the replay-side helper on rows where they're both
    well-defined. This catches predicate drift early."""
    from genie_space_optimizer.optimization.stages import evaluation as eval_stage
    from genie_space_optimizer.optimization import lever_loop_replay as llr

    rows = [
        {"question_id": "q1", "result_correctness": "yes", "arbiter": "both_correct"},
        {"question_id": "q2", "result_correctness": "no",  "arbiter": "ground_truth_correct"},
        {"question_id": "q4", "result_correctness": "yes", "arbiter": "genie_correct"},
    ]
    a1, h1, s1, g1 = eval_stage._classify_eval_rows(rows)
    a2, h2, s2, g2 = llr._classify_eval_rows(rows)

    assert a1 == a2, "already_passing partitions must agree"
    assert h1 == h2, "hard partitions must agree"
    assert g1 == g2, "gt_correction partitions must agree"
    # Soft may differ — production has actionable-soft-signal logic that
    # the replay helper falls back to via "conservative default".
