"""Phase H Completion Task 2: F2 rca_evidence wire-up.

Asserts that calling stages.rca_evidence.collect with a fully-populated
RcaEvidenceInput (sourced from F1's EvaluationResult) produces a non-
empty per_qid_evidence dict, and that wrapping with the capture
decorator writes input.json + output.json to
iter_NN/stages/02_rca_evidence/.
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
    rca_evidence as _rca_stage,
)


def _make_ctx(*, iteration: int, anchor: str | None) -> StageContext:
    return StageContext(
        run_id="opt-h2",
        iteration=iteration,
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


def test_f2_collect_with_f1_metadata_populates_evidence() -> None:
    inp = _rca_stage.RcaEvidenceInput(
        eval_rows=(
            {"question_id": "q1",
             "outputs.genie_response/sql": "SELECT * FROM t"},
            {"question_id": "q2",
             "outputs.genie_response/sql": "SELECT a FROM t LIMIT 5"},
        ),
        hard_failure_qids=("q1",),
        soft_signal_qids=("q2",),
        per_qid_judge={
            "q1": {"verdict": "wrong_join_spec",
                   "judge_name": "judge_asi"},
            "q2": {"verdict": "tvf_parameter_error",
                   "judge_name": "judge_asi"},
        },
        asi_metadata={
            "q1": {"failure_type": "wrong_join_spec"},
            "q2": {"failure_type": "tvf_parameter_error"},
        },
    )
    ctx = _make_ctx(iteration=2, anchor=None)
    out = _rca_stage.collect(ctx, inp)
    assert out.per_qid_evidence, (
        "F2 returned empty per_qid_evidence; check F1->F2 plumbing"
    )
    assert "q1" in out.per_qid_evidence
    assert out.rca_kinds_by_qid["q1"]


def test_f2_capture_wrap_writes_to_anchor_run() -> None:
    captured: dict[str, str] = {}

    def _fake_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured[artifact_file] = text

    inp = _rca_stage.RcaEvidenceInput(
        eval_rows=(
            {"question_id": "q1",
             "outputs.genie_response/sql": "SELECT a FROM t"},
        ),
        hard_failure_qids=("q1",),
        soft_signal_qids=(),
        per_qid_judge={
            "q1": {"verdict": "wrong_join_spec",
                   "judge_name": "judge_asi"},
        },
        asi_metadata={
            "q1": {"failure_type": "wrong_join_spec"},
        },
    )
    ctx = _make_ctx(iteration=4, anchor="anchor-run-2")

    wrapped = wrap_with_io_capture(
        execute=_rca_stage.collect, stage_key="rca_evidence",
    )
    with patch(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        side_effect=_fake_log_text,
    ):
        wrapped(ctx, inp)

    paths = stage_artifact_paths(iteration=4, stage_key="rca_evidence")
    assert paths["input"] in captured
    assert paths["output"] in captured
    assert "02_rca_evidence" in paths["input"]
    assert "iter_04" in paths["input"]
