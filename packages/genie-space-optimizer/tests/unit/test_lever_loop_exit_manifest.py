"""TDD coverage for the lever-loop and finalize exit manifests (Cycle9 T12).

Builders return JSON strings ready for ``dbutils.notebook.exit(...)``
so the call site stays a single-line typed handoff. Surfaces decision
counts, journey violations, and Phase B artifact paths in the
notebook-exit JSON so ``databricks jobs get-run-output`` reveals the
same numbers MLflow has.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T12.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    finalize_exit_manifest,
    lever_loop_exit_manifest,
)


def test_lever_loop_manifest_carries_decision_counts():
    payload_str = lever_loop_exit_manifest(
        optimization_run_id="run_1",
        mlflow_experiment_id="123",
        accuracy=0.875,
        iteration_counter=5,
        levers_attempted=[5, 1, 2, 6],
        levers_accepted=[],
        levers_rolled_back=[],
        per_iteration_decision_counts=[0, 0, 0, 0, 0],
        per_iteration_journey_violations=[0, 0, 0, 0, 0],
        no_decision_record_reasons=["producers_silent"] * 5,
        phase_b_decision_artifacts=[],
        phase_b_transcript_artifacts=[],
    )
    payload = json.loads(payload_str)
    assert payload["accuracy"] == 0.875
    assert payload["iteration_counter"] == 5
    assert payload["per_iteration_decision_counts"] == [0, 0, 0, 0, 0]
    assert payload["no_decision_record_reasons"] == ["producers_silent"] * 5


def test_finalize_manifest_carries_status_and_artifacts():
    payload_str = finalize_exit_manifest(
        optimization_run_id="run_1",
        status="MAX_ITERATIONS",
        convergence_reason="max_iterations",
        repeatability_pct=100.0,
        elapsed_seconds=1044.7,
        report_path="/tmp/report.md",
        promoted_to_champion=False,
    )
    payload = json.loads(payload_str)
    assert payload["status"] == "MAX_ITERATIONS"
    assert payload["repeatability_pct"] == 100.0
    assert payload["promoted_to_champion"] is False
    assert payload["report_path"] == "/tmp/report.md"


def test_lever_loop_manifest_returns_valid_json_for_databricks_exit():
    """Output must round-trip through json.loads — Databricks Jobs CLI
    ``get-run-output`` parses this JSON to surface the manifest."""
    payload_str = lever_loop_exit_manifest(
        optimization_run_id="run_x",
        mlflow_experiment_id="999",
        accuracy=0.5,
        iteration_counter=1,
        levers_attempted=[],
        levers_accepted=[],
        levers_rolled_back=[],
        per_iteration_decision_counts=[],
        per_iteration_journey_violations=[],
        no_decision_record_reasons=[],
        phase_b_decision_artifacts=[],
        phase_b_transcript_artifacts=[],
    )
    assert isinstance(payload_str, str)
    payload = json.loads(payload_str)
    assert payload["optimization_run_id"] == "run_x"
