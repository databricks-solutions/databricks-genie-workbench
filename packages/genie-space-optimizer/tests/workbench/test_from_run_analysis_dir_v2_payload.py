"""Trial 13j — ``from_run_analysis_dir`` honors v2 capture payloads.

The loader lifts ``schema_columns`` and ``serialized_space`` from v2
payloads into ``metadata_snapshot`` so that the Trial 13i
``_derive_schema_columns`` priority chain enters at step 1
(``metadata_snapshot`` source). v1 payloads are unchanged — the
derivation chain falls back to step 3 / step 4 as before.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from local_lever_workbench.input_bundle import from_run_analysis_dir
from local_lever_workbench.mlflow_eval_capture import (
    CaptureSpec,
    default_output_path,
    trace_to_eval_row,
    write_capture,
)


def _sample_trace(qid: str = "domain_a_gs_009") -> dict:
    """Proven trace shape that admits as a hard failure.

    Mirrored verbatim from ``test_mlflow_eval_capture._sample_trace``
    (the existing capture-path smoke). Keeping a local copy here
    avoids a cross-test import while preserving the contract.
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
                        "blame_set": "['main.airlines.coupons.ORIG_AIRPORT_CD']",
                        "failure_type": "missing_column",
                        "counterfactual_fix": "Add ORIG_AIRPORT_CD.",
                        "severity": "major",
                        "confidence": "0.9",
                    },
                    "valid": True,
                },
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


def _spec(genie_space_id: str) -> CaptureSpec:
    return CaptureSpec(
        experiment_id="123",
        experiment_name="/test/exp",
        optimization_run_id="run-uuid",
        job_id="job-1",
        task_run_id="task-1",
        task_key="enrichment",
        genie_space_id=genie_space_id,
    )


@pytest.mark.workbench
def test_v2_payload_populates_metadata_snapshot_schema_columns(
    tmp_path: Path,
) -> None:
    """V2 capture → ``metadata_snapshot["schema_columns"]`` non-empty.

    Trial 13i ``_derive_schema_columns`` is then guaranteed to return
    source ``"metadata_snapshot"`` for the resulting bundle, which is
    the contract Phase 2.G end-to-end verification depends on.
    """
    row = trace_to_eval_row(_sample_trace())
    assert row is not None
    spec = _spec(genie_space_id="01f143dfbeec15a3a0e87ced8662f4ed")
    out = default_output_path(
        docs_root=tmp_path,
        optimization_run_id="run-uuid",
        task_run_id="task-1",
    )
    ss = {
        "version": 1,
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.airlines.coupons",
                    "column_configs": [
                        {"column_name": "ORIG_AIRPORT_CD"},
                        {"column_name": "DEST_AIRPORT_CD"},
                    ],
                }
            ]
        },
    }
    write_capture(
        spec=spec,
        rows=[row],
        output_path=out,
        serialized_space=ss,
        schema_columns=(
            "main.airlines.coupons.ORIG_AIRPORT_CD",
            "main.airlines.coupons.DEST_AIRPORT_CD",
        ),
        schema_columns_source="genie_api",
    )

    bundle = from_run_analysis_dir(tmp_path / "runid_analysis" / "run-uuid")
    snap = dict(bundle.metadata_snapshot)
    assert snap.get("schema_columns") == [
        "main.airlines.coupons.ORIG_AIRPORT_CD",
        "main.airlines.coupons.DEST_AIRPORT_CD",
    ]
    assert snap.get("_schema_columns_source") == "genie_api"
    # Serialized space tables also propagated for the strict pydantic
    # validator the applier runs (extra="allow"); the data_sources
    # block is now present in the snapshot.
    assert isinstance(snap.get("data_sources"), dict)

    # And the Trial 13i ``_derive_schema_columns`` priority chain
    # enters at step 1 (``metadata_snapshot``) given this snapshot.
    from genie_space_optimizer.optimization.schema_columns import (
        _derive_schema_columns,
    )

    cols, source = _derive_schema_columns(snap, None, None)
    assert source == "metadata_snapshot"
    assert "main.airlines.coupons.ORIG_AIRPORT_CD" in cols


@pytest.mark.workbench
def test_v1_payload_loads_without_schema_columns(tmp_path: Path) -> None:
    """V1 capture → ``metadata_snapshot["schema_columns"]`` not set.

    The Trial 13i derivation chain on this snapshot returns
    ``"empty"`` (step 4) — the exact branch Trial 13j is designed to
    eliminate by upgrading to v2 capture format.
    """
    row = trace_to_eval_row(_sample_trace())
    assert row is not None
    spec = _spec(genie_space_id="")
    out = default_output_path(
        docs_root=tmp_path,
        optimization_run_id="run-uuid",
        task_run_id="task-1",
    )
    write_capture(spec=spec, rows=[row], output_path=out)

    bundle = from_run_analysis_dir(tmp_path / "runid_analysis" / "run-uuid")
    snap = dict(bundle.metadata_snapshot)
    assert "schema_columns" not in snap
    assert "_schema_columns_source" not in snap

    from genie_space_optimizer.optimization.schema_columns import (
        _derive_schema_columns,
    )

    cols, source = _derive_schema_columns(snap, None, None)
    assert source == "empty"
    assert cols == ()
