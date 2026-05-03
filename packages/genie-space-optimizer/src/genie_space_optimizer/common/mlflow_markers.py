"""Stable stdout markers for MLflow artifact persistence outcomes.

Phase E.0 Task 3. Each marker is a single line of the form
`GSO_<NAME>_V1 <json_payload>` matching the existing GSO_*_V1
convention so the postmortem analyzer can grep them deterministically.
"""

from __future__ import annotations

import json


def phase_a_artifact_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    anchor_run_id: str,
    artifact_path: str,
    success: bool,
    exception_class: str,
) -> str:
    """One-line marker emitted after each Phase A artifact persistence attempt."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "anchor_run_id": str(anchor_run_id),
        "artifact_path": str(artifact_path),
        "success": bool(success),
        "exception_class": str(exception_class),
    }
    return "GSO_PHASE_A_ARTIFACT_V1 " + json.dumps(payload, sort_keys=True)


def phase_b_artifact_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    anchor_run_id: str,
    decision_trace_path: str,
    operator_transcript_path: str,
    success: bool,
    exception_class: str,
) -> str:
    """One-line marker emitted after Phase B decision-trace + transcript persistence."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "anchor_run_id": str(anchor_run_id),
        "decision_trace_path": str(decision_trace_path),
        "operator_transcript_path": str(operator_transcript_path),
        "success": bool(success),
        "exception_class": str(exception_class),
    }
    return "GSO_PHASE_B_ARTIFACT_V1 " + json.dumps(payload, sort_keys=True)
