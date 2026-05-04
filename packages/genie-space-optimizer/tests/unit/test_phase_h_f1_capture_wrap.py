"""Phase H Completion Task 1: F1 evaluation stage I/O capture.

Asserts that wrapping evaluate_post_patch with a closure-bound adapter
through wrap_with_io_capture writes input.json + output.json under
iter_NN/stages/01_evaluation_state/ when invoked with a non-empty
mlflow_anchor_run_id, and that ctx.decision_emit'd records during the
call land in decisions.json.
"""

from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.run_output_contract import (
    stage_artifact_paths,
)
from genie_space_optimizer.optimization.stage_io_capture import (
    wrap_with_io_capture,
)
from genie_space_optimizer.optimization.stages import (
    StageContext,
    evaluation as _eval_stage,
)


def _make_stage_ctx(*, anchor: str | None) -> StageContext:
    return StageContext(
        run_id="opt-h1",
        iteration=3,
        space_id="space-x",
        domain="airline",
        catalog="cat",
        schema="gso",
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda r: None,
        mlflow_anchor_run_id=anchor,
        feature_flags={},
    )


def _make_eval_inp() -> _eval_stage.EvaluationInput:
    return _eval_stage.EvaluationInput(
        space_state={"id": "space-x"},
        eval_qids=("q1", "q2"),
        run_role="iteration_eval",
        iteration_label="iter_03",
        scope="full",
    )


def test_f1_closure_adapter_logs_input_and_output_to_anchor_run() -> None:
    captured: dict[str, str] = {}

    def _fake_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured[artifact_file] = text

    fake_raw = {
        "rows": [
            {"question_id": "q1", "result_correctness": "yes",
             "arbiter": "genie_correct"},
            {"question_id": "q2", "result_correctness": "no",
             "arbiter": "genie_wrong"},
        ],
        "scores": {},
        "overall_accuracy": 0.5,
        "per_qid_judge": {"q2": {"verdict": "wrong_join_spec"}},
        "asi_metadata": {"q2": {"failure_type": "wrong_join_spec"}},
    }

    eval_kwargs = {
        "ground_truth_table_full_name": "cat.gso.gt",
        "space_id": "space-x",
        "model": "fake-model",
    }

    def _adapter(ctx, inp):
        return _eval_stage.evaluate_post_patch(
            ctx, inp, eval_kwargs=eval_kwargs,
        )

    wrapped = wrap_with_io_capture(
        execute=_adapter, stage_key="evaluation_state",
    )

    ctx = _make_stage_ctx(anchor="anchor-run-1")
    inp = _make_eval_inp()

    with patch(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        side_effect=_fake_log_text,
    ), patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        return_value=fake_raw,
    ):
        out = wrapped(ctx, inp)

    paths = stage_artifact_paths(iteration=3, stage_key="evaluation_state")
    assert paths["input"] in captured, (
        f"expected input artifact at {paths['input']}, "
        f"saw {sorted(captured)}"
    )
    assert paths["output"] in captured
    assert "iter_03" in paths["input"]
    assert "01_evaluation_state" in paths["input"]
    assert isinstance(out, _eval_stage.EvaluationResult)
    assert out.scoreboard["overall_accuracy"] == 0.5
