"""Unit tests for the GSO_BUNDLE_ASSEMBLY_FAILED_V1 marker."""

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    bundle_assembly_failed_marker,
)


def test_bundle_assembly_failed_marker_round_trips():
    line = bundle_assembly_failed_marker(
        optimization_run_id="run-abc",
        parent_bundle_run_id="anchor-xyz",
        error_type="RuntimeError",
        error_message="MLflow client unavailable",
    )
    assert line.startswith("GSO_BUNDLE_ASSEMBLY_FAILED_V1 ")
    payload = json.loads(line.split(" ", 1)[1])
    assert payload == {
        "optimization_run_id": "run-abc",
        "parent_bundle_run_id": "anchor-xyz",
        "error_type": "RuntimeError",
        "error_message": "MLflow client unavailable",
    }


def test_bundle_assembly_failed_marker_truncates_long_messages():
    long_msg = "x" * 5000
    line = bundle_assembly_failed_marker(
        optimization_run_id="r",
        parent_bundle_run_id=None,
        error_type="ValueError",
        error_message=long_msg,
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert len(payload["error_message"]) == 2000
    assert payload["parent_bundle_run_id"] is None
