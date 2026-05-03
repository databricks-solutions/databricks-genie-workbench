"""Phase E.0 Task 3 — stdout markers for artifact persistence outcomes."""

import json


def test_phase_a_artifact_marker_success_payload() -> None:
    from genie_space_optimizer.common.mlflow_markers import phase_a_artifact_marker

    line = phase_a_artifact_marker(
        optimization_run_id="run_1",
        iteration=4,
        anchor_run_id="abc123",
        artifact_path="phase_a/journey_validation/iter_4.json",
        success=True,
        exception_class="",
    )
    assert line.startswith("GSO_PHASE_A_ARTIFACT_V1 ")
    payload = json.loads(line[len("GSO_PHASE_A_ARTIFACT_V1 "):])
    assert payload == {
        "optimization_run_id": "run_1",
        "iteration": 4,
        "anchor_run_id": "abc123",
        "artifact_path": "phase_a/journey_validation/iter_4.json",
        "success": True,
        "exception_class": "",
    }


def test_phase_a_artifact_marker_failure_payload() -> None:
    from genie_space_optimizer.common.mlflow_markers import phase_a_artifact_marker

    line = phase_a_artifact_marker(
        optimization_run_id="run_1",
        iteration=4,
        anchor_run_id="",
        artifact_path="phase_a/journey_validation/iter_4.json",
        success=False,
        exception_class="MlflowException",
    )
    payload = json.loads(line[len("GSO_PHASE_A_ARTIFACT_V1 "):])
    assert payload["success"] is False
    assert payload["exception_class"] == "MlflowException"
    assert payload["anchor_run_id"] == ""


def test_phase_b_artifact_marker_emits_decision_and_transcript_paths() -> None:
    from genie_space_optimizer.common.mlflow_markers import phase_b_artifact_marker

    line = phase_b_artifact_marker(
        optimization_run_id="run_1",
        iteration=4,
        anchor_run_id="abc123",
        decision_trace_path="phase_b/decision_trace/iter_4.json",
        operator_transcript_path="phase_b/operator_transcript/iter_4.txt",
        success=True,
        exception_class="",
    )
    assert line.startswith("GSO_PHASE_B_ARTIFACT_V1 ")
    payload = json.loads(line[len("GSO_PHASE_B_ARTIFACT_V1 "):])
    assert payload["decision_trace_path"] == "phase_b/decision_trace/iter_4.json"
    assert payload["operator_transcript_path"] == "phase_b/operator_transcript/iter_4.txt"
    assert payload["success"] is True
