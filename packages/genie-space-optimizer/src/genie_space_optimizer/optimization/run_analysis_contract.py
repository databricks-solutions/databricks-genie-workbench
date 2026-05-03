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


def phase_b_no_records_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    reason: str,
    producer_exceptions: Mapping[str, int] | None = None,
    contract_version: str = "v1",
) -> str:
    """Marker emitted when an iteration produces zero ``DecisionRecord``s.

    Distinguishes "Phase B ran but had nothing to record" from "Phase B
    never ran" (deploy is stale; ``contract_version`` tag absent) and
    from a silent producer error (``producer_exceptions`` carries the
    counters). The reason string is drawn from the closed
    ``NoRecordsReason`` vocabulary in
    ``optimization/decision_emitters.py``.
    """
    return marker_line(
        "GSO_PHASE_B_NO_RECORDS_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "reason": str(reason or ""),
            "producer_exceptions": dict(producer_exceptions or {}),
            "contract_version": str(contract_version or ""),
        },
    )


def phase_b_end_marker(
    *,
    optimization_run_id: str,
    total_records: int,
    iter_record_counts: list[int],
    iter_violation_counts: list[int],
    no_records_iterations: list[int],
    contract_version: str,
) -> str:
    """Marker emitted once at lever-loop terminate.

    Carries the per-iter record/violation counts plus a list of
    iterations that produced zero records (so the analyzer can correlate
    the end-of-loop view with per-iter ``GSO_PHASE_B_NO_RECORDS_V1``
    markers). Fires on every termination path (plateau, max-iterations,
    convergence, raise) — see harness exit-path audit test.
    """
    return marker_line(
        "GSO_PHASE_B_END_V1",
        {
            "optimization_run_id": optimization_run_id,
            "total_records": int(total_records),
            "iter_record_counts": [int(n) for n in (iter_record_counts or [])],
            "iter_violation_counts": [int(n) for n in (iter_violation_counts or [])],
            "no_records_iterations": [int(n) for n in (no_records_iterations or [])],
            "contract_version": str(contract_version or ""),
        },
    )


def lever_loop_exit_manifest(
    *,
    optimization_run_id: str,
    mlflow_experiment_id: str,
    accuracy: float,
    iteration_counter: int,
    levers_attempted: list,
    levers_accepted: list,
    levers_rolled_back: list,
    per_iteration_decision_counts: list[int],
    per_iteration_journey_violations: list[int],
    no_decision_record_reasons: list[str],
    phase_b_decision_artifacts: list[str],
    phase_b_transcript_artifacts: list[str],
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    the lever-loop task.

    Surfaces decision counts, journey violations, and Phase B artifact
    paths so ``databricks jobs get-run-output`` reveals the same numbers
    MLflow has. Returned as a JSON string (not dict) so the call site
    stays a single ``dbutils.notebook.exit(lever_loop_exit_manifest(...))``.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "mlflow_experiment_id": str(mlflow_experiment_id),
        "accuracy": float(accuracy),
        "iteration_counter": int(iteration_counter),
        "levers_attempted": list(levers_attempted),
        "levers_accepted": list(levers_accepted),
        "levers_rolled_back": list(levers_rolled_back),
        "per_iteration_decision_counts": [
            int(n) for n in (per_iteration_decision_counts or [])
        ],
        "per_iteration_journey_violations": [
            int(n) for n in (per_iteration_journey_violations or [])
        ],
        "no_decision_record_reasons": [
            str(r) for r in (no_decision_record_reasons or [])
        ],
        "phase_b_decision_artifacts": [
            str(p) for p in (phase_b_decision_artifacts or [])
        ],
        "phase_b_transcript_artifacts": [
            str(p) for p in (phase_b_transcript_artifacts or [])
        ],
    }
    return json.dumps(payload, default=str)


def finalize_exit_manifest(
    *,
    optimization_run_id: str,
    status: str,
    convergence_reason: str,
    repeatability_pct: float,
    elapsed_seconds: float,
    report_path: str,
    promoted_to_champion: bool,
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    the finalize task.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "status": str(status),
        "convergence_reason": str(convergence_reason),
        "repeatability_pct": float(repeatability_pct),
        "elapsed_seconds": float(elapsed_seconds),
        "report_path": str(report_path),
        "promoted_to_champion": bool(promoted_to_champion),
    }
    return json.dumps(payload, default=str)
