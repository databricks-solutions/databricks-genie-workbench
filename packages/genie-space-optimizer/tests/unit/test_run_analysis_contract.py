from __future__ import annotations

import json


def _json_payload(line: str) -> dict:
    _prefix, payload = line.split(" ", 1)
    return json.loads(payload)


def test_marker_line_is_compact_sorted_json() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import marker_line

    line = marker_line("GSO_TEST_V1", {"b": 2, "a": 1})

    assert line == 'GSO_TEST_V1 {"a":1,"b":2}'


def test_run_manifest_marker_has_required_fields() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        run_manifest_marker,
    )

    line = run_manifest_marker(
        optimization_run_id="opt_run_1",
        databricks_job_id="123",
        databricks_parent_run_id="456",
        lever_loop_task_run_id="789",
        mlflow_experiment_id="42",
        space_id="space_1",
        event="start",
    )

    assert line.startswith("GSO_RUN_MANIFEST_V1 ")
    payload = _json_payload(line)
    assert payload == {
        "databricks_job_id": "123",
        "databricks_parent_run_id": "456",
        "event": "start",
        "lever_loop_task_run_id": "789",
        "mlflow_experiment_id": "42",
        "optimization_run_id": "opt_run_1",
        "space_id": "space_1",
    }


def test_phase_b_marker_reports_trace_artifacts() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_marker,
    )

    line = phase_b_marker(
        optimization_run_id="opt_run_1",
        iteration=3,
        decision_record_count=12,
        decision_validation_count=0,
        transcript_chars=2000,
        decision_trace_artifact="phase_b/decision_trace/iter_3.json",
        operator_transcript_artifact="phase_b/operator_transcript/iter_3.txt",
        persist_ok=True,
    )

    assert line.startswith("GSO_PHASE_B_V1 ")
    payload = _json_payload(line)
    assert payload["decision_record_count"] == 12
    assert payload["decision_validation_count"] == 0
    assert payload["persist_ok"] is True
