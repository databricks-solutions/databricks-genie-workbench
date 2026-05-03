"""Phase E.0 Task 8 — regression rail: fresh persistence anchors correctly.

This test exercises the harness helpers _persist_phase_a_artifact_to_anchor
and _persist_phase_b_artifacts_to_anchor with a file:// MLflow store and
synthetic sibling runs. It asserts that the artifacts land on the
genie.run_type=lever_loop sibling regardless of which run is currently
active when the helpers are invoked.
"""

from pathlib import Path


def test_one_iteration_smoke_lands_phase_a_and_phase_b_on_anchor(
    tmp_path: Path,
) -> None:
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(f"file://{tmp_path}/mlruns_smoke")
    mlflow.set_experiment("phase_e0_smoke")

    opt_run_id = "smoke_opt_run_1"

    # Build the sibling-run topology: one lever_loop parent + one
    # full_eval child currently active.
    parent = mlflow.start_run(run_name="lever_loop_parent")
    parent_run_id = parent.info.run_id
    mlflow.set_tags({
        "genie.optimization_run_id": opt_run_id,
        "genie.run_type": "lever_loop",
    })
    mlflow.end_run()

    mlflow.start_run(run_name="full_eval_child")
    mlflow.set_tags({
        "genie.optimization_run_id": opt_run_id,
        "genie.run_type": "full_eval",
    })

    from genie_space_optimizer.optimization.harness import (
        _persist_phase_a_artifact_to_anchor,
        _persist_phase_b_artifacts_to_anchor,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType, DecisionOutcome, ReasonCode,
        DecisionRecord, OptimizationTrace,
        canonical_decision_json, render_operator_transcript,
    )

    phase_a_result = _persist_phase_a_artifact_to_anchor(
        opt_run_id=opt_run_id,
        iteration=0,
        report_dict={"violations": [], "is_valid": True, "iteration": 0},
    )
    assert phase_a_result.success is True
    assert phase_a_result.anchor_run_id == parent_run_id

    record = DecisionRecord(
        run_id=opt_run_id, iteration=0,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )
    transcript = render_operator_transcript(
        trace=OptimizationTrace(decision_records=(record,)),
        iteration=0,
    )
    phase_b_result = _persist_phase_b_artifacts_to_anchor(
        opt_run_id=opt_run_id,
        iteration=0,
        decision_json=canonical_decision_json([record]),
        transcript=transcript,
        record_count=1,
        violation_count=0,
    )
    assert phase_b_result.success is True
    assert phase_b_result.anchor_run_id == parent_run_id

    mlflow.end_run()

    client = MlflowClient()

    def _walk(run_id: str, path: str = "") -> list[str]:
        out: list[str] = []
        for fi in client.list_artifacts(run_id, path):
            if fi.is_dir:
                out.extend(_walk(run_id, fi.path))
            else:
                out.append(fi.path)
        return out

    parent_files = _walk(parent_run_id)
    expected = {
        "phase_a/journey_validation/iter_0.json",
        "phase_b/decision_trace/iter_0.json",
        "phase_b/operator_transcript/iter_0.txt",
    }
    assert expected.issubset(set(parent_files)), (
        f"Anchor run is missing expected artifacts. "
        f"Found: {parent_files}"
    )
