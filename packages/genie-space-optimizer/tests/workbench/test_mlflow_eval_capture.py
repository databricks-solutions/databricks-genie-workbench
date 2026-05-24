"""Unit tests for ``mlflow_eval_capture.trace_to_eval_row``.

The integration concerns (Databricks SDK, MLflow client) live in
``test_local_workbench_full_funnel_production_replay.py`` and the
manual CLI smoke. These tests lock in the trace → eval-row contract
that PR-2 (from-rows entrypoint) depends on.
"""
from __future__ import annotations

import json

import pytest

from local_lever_workbench.mlflow_eval_capture import (
    CaptureSpec,
    default_output_path,
    trace_to_eval_row,
    write_capture,
)


def _sample_trace(*, qid: str = "domain_a_gs_009") -> dict:
    """Build a minimal trace dict shaped like ``MlflowClient.get_trace``.

    Mirrors the real shape captured from production:
    - root span ``genie_predict_fn`` with span_inputs/outputs/question_id
    - one LLM_JUDGE assessment with metadata.blame_set
    - one HUMAN assessment carrying the expected_response value.
    """
    return {
        "info": {
            "trace_id": "tr-deadbeef",
            "client_request_id": "crid-001",
            "state": "OK",
            "request_time": "2026-05-20T13:46:00.000Z",
            "execution_duration_ms": 4321,
            "assessments": [
                {
                    "assessment_name": "completeness",
                    "trace_id": "tr-deadbeef",
                    "span_id": "root",
                    "source": {
                        "source_type": "LLM_JUDGE",
                        "source_id": "databricks:/databricks-claude-opus-4-6",
                    },
                    "feedback": {"value": "no"},
                    "rationale": "Missing columns in the projection.",
                    "metadata": {
                        "blame_set": "['ORIG_AIRPORT_CD']",
                        "failure_type": "missing_column",
                        "counterfactual_fix": "Add ORIG_AIRPORT_CD.",
                        "severity": "major",
                        "confidence": "0.9",
                        "mlflow.assessment.sourceRunId": "ignored",
                    },
                    "valid": True,
                },
                # ``row_is_hard_failure`` requires result_correctness=no AND
                # arbiter not in the correct set. Without these the row is
                # admitted but not classified as a hard failure, and the
                # workbench legitimately rejects the bundle.
                {
                    "assessment_name": "result_correctness",
                    "trace_id": "tr-deadbeef",
                    "span_id": "root",
                    "source": {"source_type": "CODE", "source_id": "internal"},
                    "feedback": {"value": "no"},
                    "metadata": {},
                },
                {
                    "assessment_name": "arbiter",
                    "trace_id": "tr-deadbeef",
                    "span_id": "root",
                    "source": {
                        "source_type": "LLM_JUDGE",
                        "source_id": "databricks:/databricks-claude-opus-4-6",
                    },
                    "feedback": {"value": "both_wrong"},
                    "metadata": {},
                },
                {
                    "assessment_name": "expected_response",
                    "trace_id": "tr-deadbeef",
                    "span_id": "root",
                    "source": {"source_type": "HUMAN", "source_id": "spark"},
                    "expectation": {
                        "value": "SELECT * FROM t WHERE FARE_BREAK_AMT > 1000",
                    },
                    "metadata": {},
                },
            ],
        },
        "data": {
            "spans": [
                {
                    "name": "genie_predict_fn",
                    "attributes": {
                        "question_id": json.dumps(qid),
                        "mlflow.spanInputs": json.dumps(
                            {
                                "question": "Which coupons exceed 1000 USD?",
                                "expected_sql": (
                                    "SELECT * FROM t WHERE FARE_BREAK_AMT > 1000"
                                ),
                            }
                        ),
                        "mlflow.spanOutputs": json.dumps(
                            {
                                "response": (
                                    "SELECT * FROM t WHERE FARE_BREAK_AMT > "
                                    "1000 AND CURRENCY = 'USD'"
                                ),
                            }
                        ),
                    },
                }
            ]
        },
    }


@pytest.mark.workbench
def test_trace_to_eval_row_emits_canonical_row_shape() -> None:
    row = trace_to_eval_row(_sample_trace())
    assert row is not None

    # QID extraction ladder hits.
    from genie_space_optimizer.optimization._qid_extraction import extract_question_id

    qid, source = extract_question_id(row)
    assert qid == "domain_a_gs_009", f"qid={qid!r} source={source!r}"

    # Question and gold SQL paths the Stage 1 input-card reader walks.
    from genie_space_optimizer.optimization.eval_row_access import (
        row_expected_sql,
        row_generated_sql,
        row_question,
    )

    assert row_question(row) == "Which coupons exceed 1000 USD?"
    assert row_expected_sql(row).startswith("SELECT * FROM t WHERE")
    assert "CURRENCY = 'USD'" in row_generated_sql(row)

    # Judge metadata available via both surface shapes.
    assert row["completeness/value"] == "no"
    assert "Missing columns" in row["completeness/rationale"]
    assert row["completeness/metadata"]["failure_type"] == "missing_column"
    assert row["metadata/completeness/failure_type"] == "missing_column"
    assert row["metadata/completeness/blame_set"] == "['ORIG_AIRPORT_CD']"

    # HUMAN expected_response surfaces under the standard production
    # path used by ``row_expected_sql``.
    assert row["expected_response/value"].startswith("SELECT * FROM t")

    # mlflow.* private metadata is not leaked into the flat key surface.
    assert "metadata/completeness/mlflow.assessment.sourceRunId" not in row


@pytest.mark.workbench
def test_trace_to_eval_row_returns_none_without_root_span_or_qid() -> None:
    # No spans
    assert trace_to_eval_row({"info": {"assessments": []}, "data": {"spans": []}}) is None

    # Spans without question_id
    trace = {
        "info": {"assessments": []},
        "data": {
            "spans": [
                {
                    "name": "genie_predict_fn",
                    "attributes": {
                        "mlflow.spanInputs": "{}",
                        "mlflow.spanOutputs": "{}",
                    },
                }
            ]
        },
    }
    assert trace_to_eval_row(trace) is None


@pytest.mark.workbench
def test_write_capture_uses_workbench_fixture_shape(tmp_path) -> None:
    row = trace_to_eval_row(_sample_trace())
    assert row is not None
    spec = CaptureSpec(
        experiment_id="123",
        experiment_name="/test/exp",
        optimization_run_id="run-uuid",
        job_id="job-1",
        task_run_id="task-1",
        task_key="enrichment",
    )
    out = default_output_path(
        docs_root=tmp_path, optimization_run_id="run-uuid", task_run_id="task-1"
    )
    written = write_capture(spec=spec, rows=[row], output_path=out)
    payload = json.loads(written.read_text())
    assert payload["_schema_version"] == "workbench_eval_capture_v1"
    assert payload["_provenance"]["task_key"] == "enrichment"
    assert payload["_provenance"]["trace_count"] == 1
    assert isinstance(payload["eval_rows"], list)
    assert len(payload["eval_rows"]) == 1
    # ``from_run_analysis_dir`` walks the file glob — make sure the
    # default path matches the pattern that file uses.
    assert written.name.startswith("replay_fixture_from_latest_export_")
    assert written.parent.name == "evidence"
    assert written.parent.parent.name == "run-uuid"


@pytest.mark.workbench
def test_workbench_admits_captured_rows_as_hard_failures(tmp_path) -> None:
    """End-to-end PR-1 → workbench wiring smoke.

    Builds a captured fixture from synthetic traces, then drives
    ``from_run_analysis_dir`` against it and confirms the row admits
    as a hard failure (so PR-2 can drive the SM from it).
    """
    from local_lever_workbench.input_bundle import from_run_analysis_dir

    row = trace_to_eval_row(_sample_trace())
    assert row is not None
    spec = CaptureSpec(
        experiment_id="123",
        experiment_name="/test/exp",
        optimization_run_id="run-uuid",
        job_id="job-1",
        task_run_id="task-1",
        task_key="enrichment",
    )
    out = default_output_path(
        docs_root=tmp_path, optimization_run_id="run-uuid", task_run_id="task-1"
    )
    write_capture(spec=spec, rows=[row], output_path=out)

    bundle_dir = tmp_path / "runid_analysis" / "run-uuid"
    bundle = from_run_analysis_dir(bundle_dir)
    assert len(bundle.hard_cases) == 1
    assert bundle.hard_cases[0].qid == "domain_a_gs_009"
