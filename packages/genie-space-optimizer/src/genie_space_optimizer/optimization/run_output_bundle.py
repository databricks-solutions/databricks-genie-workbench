"""Pure bundle assembly helpers (Phase H).

Produces JSON payloads for manifest.json, run_summary.json,
artifact_index.json from completed run state. No MLflow / Spark / I/O —
the harness wire-up calls these helpers and pushes the result to MLflow.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.run_output_contract import (
    bundle_artifact_paths,
    iteration_bundle_prefix,
    stage_artifact_paths,
)
from genie_space_optimizer.optimization.stages import STAGES


SCHEMA_VERSION = "v1"


def _normalize_accuracy_pct(value: Any) -> Any:
    """Cycle 6 F-6 — collapse 0-1 fraction inputs and 0-100 percent
    inputs to a single canonical 0-100 representation, rounded to one
    decimal. The harness has historically passed both shapes for
    ``overall_accuracy`` and ``accuracy_delta_pp`` depending on call
    site; the bundle write must speak one unit so the operator
    transcript no longer prints ``Baseline accuracy: 8947.0%``.

    Non-numeric values pass through unchanged so the helper is safe
    to call on partial/legacy payloads.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return value
    if 0.0 <= f <= 1.0:
        f = f * 100.0
    return round(f, 1)


def build_manifest(
    *,
    optimization_run_id: str,
    databricks_job_id: str,
    databricks_parent_run_id: str,
    lever_loop_task_run_id: str,
    iterations: list[int],
    missing_pieces: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build manifest.json for the parent bundle."""
    return {
        "schema_version": SCHEMA_VERSION,
        "optimization_run_id": optimization_run_id,
        "databricks_job_id": databricks_job_id,
        "databricks_parent_run_id": databricks_parent_run_id,
        "lever_loop_task_run_id": lever_loop_task_run_id,
        "iteration_count": len(iterations),
        "iterations": list(iterations),
        "missing_pieces": missing_pieces,
        "stage_keys_in_process_order": [e.stage_key for e in STAGES],
    }


def _stage_dir_name_for(paths: dict[str, str]) -> str:
    """Extract the ``NN_<stage_key>`` directory name from a stage path."""
    parts = paths["input"].split("/")
    # Path shape: gso_postmortem_bundle/iterations/iter_NN/stages/<NN_key>/input.json
    # The component immediately after "stages" is the dir name.
    return parts[parts.index("stages") + 1]


def build_artifact_index(*, iterations: list[int]) -> dict[str, Any]:
    """Build artifact_index.json — a flat path map for postmortem skills.

    Includes per-stage paths so the gso-postmortem skill can
    deterministically reach every iteration's stage I/O without
    walking directories.
    """
    base = bundle_artifact_paths(iterations=iterations)
    flat: dict[str, Any] = {
        "manifest":               base["manifest"],
        "run_summary":            base["run_summary"],
        "operator_transcript":    base["operator_transcript"],
        "decision_trace_all":     base["decision_trace_all"],
        "journey_validation_all": base["journey_validation_all"],
        "replay_fixture":         base["replay_fixture"],
        "scoreboard":             base["scoreboard"],
        "failure_buckets":        base["failure_buckets"],
        "iterations": {},
    }
    for iteration in iterations:
        prefix = iteration_bundle_prefix(iteration)
        per_iter: dict[str, Any] = {
            "summary":             f"{prefix}/summary.json",
            "operator_transcript": f"{prefix}/operator_transcript.md",
            "decision_trace":      f"{prefix}/decision_trace.json",
            "journey_validation":  f"{prefix}/journey_validation.json",
            "stages": {},
        }
        for entry in STAGES:
            stage_paths = stage_artifact_paths(iteration, entry.stage_key)
            per_iter["stages"][_stage_dir_name_for(stage_paths)] = stage_paths
        flat["iterations"][str(iteration)] = per_iter
    return flat


def build_run_summary(
    *,
    baseline: dict[str, Any],
    terminal_state: dict[str, Any],
    iteration_count: int,
    accuracy_delta_pp: float,
) -> dict[str, Any]:
    """Build run_summary.json — the high-level run outcome.

    Cycle 6 F-6: accuracy fields are normalized to 0-100 percent
    units at the boundary so downstream renderers never multiply.
    """
    normalized_baseline = dict(baseline or {})
    if "overall_accuracy" in normalized_baseline:
        normalized_baseline["overall_accuracy"] = _normalize_accuracy_pct(
            normalized_baseline["overall_accuracy"]
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "baseline": normalized_baseline,
        "terminal_state": terminal_state,
        "iteration_count": iteration_count,
        "accuracy_delta_pp": _normalize_accuracy_pct(accuracy_delta_pp),
    }
