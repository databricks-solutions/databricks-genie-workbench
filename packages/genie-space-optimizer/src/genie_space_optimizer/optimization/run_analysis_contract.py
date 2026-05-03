"""CLI-readable output contract for GSO lever-loop run analysis.

This module is intentionally Spark/Databricks/MLflow free. It only builds
stable single-line JSON markers that the run-analysis skill can parse from
Databricks task stdout.
"""

from __future__ import annotations

import json
from typing import Any, Mapping


def _clean(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _clean(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_clean(v) for v in value]
    return str(value)


def marker_line(marker: str, payload: Mapping[str, Any]) -> str:
    """Return one stable stdout marker line."""
    clean_marker = str(marker).strip()
    if not clean_marker.startswith("GSO_") or not clean_marker.endswith("_V1"):
        raise ValueError(f"invalid GSO marker name: {marker!r}")
    clean_payload = {str(k): _clean(v) for k, v in payload.items()}
    encoded = json.dumps(clean_payload, sort_keys=True, separators=(",", ":"))
    return f"{clean_marker} {encoded}"


def run_manifest_marker(
    *,
    optimization_run_id: str,
    databricks_job_id: str = "",
    databricks_parent_run_id: str = "",
    lever_loop_task_run_id: str = "",
    mlflow_experiment_id: str = "",
    space_id: str = "",
    event: str,
) -> str:
    return marker_line(
        "GSO_RUN_MANIFEST_V1",
        {
            "optimization_run_id": optimization_run_id,
            "databricks_job_id": databricks_job_id,
            "databricks_parent_run_id": databricks_parent_run_id,
            "lever_loop_task_run_id": lever_loop_task_run_id,
            "mlflow_experiment_id": mlflow_experiment_id,
            "space_id": space_id,
            "event": event,
        },
    )


def iteration_summary_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    accepted_count: int,
    rolled_back_count: int,
    skipped_count: int,
    gate_drop_count: int,
    decision_record_count: int,
    journey_violation_count: int,
) -> str:
    return marker_line(
        "GSO_ITERATION_SUMMARY_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "accepted_count": int(accepted_count),
            "rolled_back_count": int(rolled_back_count),
            "skipped_count": int(skipped_count),
            "gate_drop_count": int(gate_drop_count),
            "decision_record_count": int(decision_record_count),
            "journey_violation_count": int(journey_violation_count),
        },
    )


def phase_b_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    decision_record_count: int,
    decision_validation_count: int,
    transcript_chars: int,
    decision_trace_artifact: str,
    operator_transcript_artifact: str,
    persist_ok: bool,
) -> str:
    return marker_line(
        "GSO_PHASE_B_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "decision_record_count": int(decision_record_count),
            "decision_validation_count": int(decision_validation_count),
            "transcript_chars": int(transcript_chars),
            "decision_trace_artifact": decision_trace_artifact,
            "operator_transcript_artifact": operator_transcript_artifact,
            "persist_ok": bool(persist_ok),
        },
    )


def convergence_marker(
    *,
    optimization_run_id: str,
    reason: str,
    iteration_counter: int,
    best_accuracy: float | None,
    thresholds_met: bool,
) -> str:
    return marker_line(
        "GSO_CONVERGENCE_V1",
        {
            "optimization_run_id": optimization_run_id,
            "reason": reason,
            "iteration_counter": int(iteration_counter),
            "best_accuracy": "" if best_accuracy is None else float(best_accuracy),
            "thresholds_met": bool(thresholds_met),
        },
    )
