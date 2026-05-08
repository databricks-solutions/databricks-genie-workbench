"""Phase H Task 7: bundle assembly helpers."""

from __future__ import annotations


def test_build_manifest_carries_run_id_and_iteration_count() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_manifest,
    )
    manifest = build_manifest(
        optimization_run_id="abc-123",
        databricks_job_id="j1",
        databricks_parent_run_id="r1",
        lever_loop_task_run_id="t1",
        iterations=[1, 2, 3],
        missing_pieces=[],
    )
    assert manifest["optimization_run_id"] == "abc-123"
    assert manifest["iteration_count"] == 3
    assert manifest["missing_pieces"] == []
    assert "schema_version" in manifest


def test_build_artifact_index_lists_all_iterations_and_stages() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_artifact_index,
    )
    index = build_artifact_index(iterations=[1, 2])
    assert "manifest" in index
    assert "operator_transcript" in index
    assert "iterations" in index
    assert len(index["iterations"]) == 2
    iter_1 = index["iterations"]["1"]
    assert "stages" in iter_1
    assert "01_evaluation_state" in iter_1["stages"]
    assert "input" in iter_1["stages"]["01_evaluation_state"]


def test_build_run_summary_carries_baseline_and_terminal_state() -> None:
    """Cycle 6 F-6 — accuracy fields are normalized to canonical 0-100
    units at the build_run_summary boundary so downstream renderers
    never multiply (the original symptom was ``Baseline accuracy:
    8947.0%``). 0-1 fractions get scaled by 100; 0-100 percents pass
    through unchanged. Both rounded to one decimal."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.875},
        terminal_state={"status": "convergence", "should_continue": False},
        iteration_count=5,
        accuracy_delta_pp=4.2,
    )
    # 0.875 (fraction) → 87.5 (percent), per _normalize_accuracy_pct.
    assert summary["baseline"]["overall_accuracy"] == 87.5
    assert summary["terminal_state"]["status"] == "convergence"
    # 4.2 is already in 0-100 range so it passes through unchanged.
    assert summary["accuracy_delta_pp"] == 4.2


def test_build_manifest_includes_stage_keys_in_process_order() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_manifest,
    )
    manifest = build_manifest(
        optimization_run_id="r",
        databricks_job_id="j",
        databricks_parent_run_id="p",
        lever_loop_task_run_id="t",
        iterations=[1],
        missing_pieces=[],
    )
    # Phase H Fidelity Task 6: manifest stage order mirrors the
    # 11-entry transcript contract (PROCESS_STAGE_ORDER), not the
    # 9-entry executable STAGES registry. The transcript-only stages
    # ``post_patch_evaluation`` (between ``applied_patches`` and
    # ``acceptance_decision``) and ``contract_health`` (final) are
    # included so postmortem skills walk every stage the operator
    # transcript renders.
    keys = manifest["stage_keys_in_process_order"]
    assert keys[0] == "evaluation_state"
    assert keys[-1] == "contract_health"
    assert len(keys) == 11
    # Executable subset (9 stages from STAGES) is still exposed for
    # consumers that need to reach stage I/O artifacts.
    exec_keys = manifest["executable_stage_keys"]
    assert exec_keys[0] == "evaluation_state"
    assert exec_keys[-1] == "learning_next_action"
    assert len(exec_keys) == 9


# Cycle 12-T3 — parent-bundle aggregators / minimal producers
def test_build_decision_trace_all_aggregates_per_iter_traces() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_decision_trace_all,
    )

    iter_traces = [
        {"iteration": 1, "records": [{"id": "r1"}, {"id": "r2"}]},
        {"iteration": 2, "records": [{"id": "r3"}]},
    ]

    out = build_decision_trace_all(iter_traces=iter_traces)

    assert out["schema_version"] == "v1"
    assert out["iteration_count"] == 2
    assert out["total_record_count"] == 3
    assert out["iterations"] == iter_traces


def test_build_decision_trace_all_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_decision_trace_all,
    )

    out = build_decision_trace_all(iter_traces=[])

    assert out["iteration_count"] == 0
    assert out["total_record_count"] == 0
    assert out["iterations"] == []


def test_build_decision_trace_all_handles_missing_records_key() -> None:
    """Defensive: per-iteration entries without ``records`` count as 0."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_decision_trace_all,
    )

    iter_traces = [{"iteration": 1}, {"iteration": 2, "records": [{"id": "r1"}]}]

    out = build_decision_trace_all(iter_traces=iter_traces)

    assert out["total_record_count"] == 1


def test_build_journey_validation_all_aggregates_per_iter_reports() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_journey_validation_all,
    )

    reports = [
        {"iteration": 1, "is_valid": True, "violations": []},
        {"iteration": 2, "is_valid": False, "violations": [{"qid": "q1", "kind": "illegal_transition"}]},
    ]

    out = build_journey_validation_all(iter_reports=reports)

    assert out["schema_version"] == "v1"
    assert out["iteration_count"] == 2
    assert out["total_violation_count"] == 1
    assert out["any_invalid"] is True
    assert out["iterations"] == reports


def test_build_journey_validation_all_handles_all_valid() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_journey_validation_all,
    )

    reports = [{"iteration": 1, "is_valid": True, "violations": []}]

    out = build_journey_validation_all(iter_reports=reports)

    assert out["any_invalid"] is False
    assert out["total_violation_count"] == 0


def test_build_journey_validation_all_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_journey_validation_all,
    )

    out = build_journey_validation_all(iter_reports=[])

    assert out["iteration_count"] == 0
    assert out["any_invalid"] is False
    assert out["total_violation_count"] == 0


def test_build_scoreboard_minimal_shape() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_scoreboard,
    )

    out = build_scoreboard(
        iter_record_counts=[24, 24, 12, 0, 0],
        iter_violation_counts=[0, 0, 0, 0, 0],
        no_records_iterations=[4, 5],
        levers_attempted={6: 3, 5: 2},
        levers_accepted={5: 1},
        levers_rolled_back={6: 3},
        best_accuracy=85.7,
        baseline_accuracy=82.1,
        iteration_count=5,
    )

    assert out["schema_version"] == "v1"
    assert out["iteration_count"] == 5
    assert out["best_accuracy"] == 85.7
    assert out["baseline_accuracy"] == 82.1
    assert out["accuracy_delta_pp"] == 3.6
    assert out["iter_record_counts"] == [24, 24, 12, 0, 0]
    assert out["iter_violation_counts"] == [0, 0, 0, 0, 0]
    assert out["no_records_iterations"] == [4, 5]
    assert out["levers_attempted"] == {"5": 2, "6": 3}
    assert out["levers_accepted"] == {"5": 1}
    assert out["levers_rolled_back"] == {"6": 3}


def test_build_scoreboard_handles_none_accuracy() -> None:
    """Pre-baseline runs that crash before evaluation may have None accuracy."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_scoreboard,
    )

    out = build_scoreboard(
        iter_record_counts=[],
        iter_violation_counts=[],
        no_records_iterations=[],
        levers_attempted={},
        levers_accepted={},
        levers_rolled_back={},
        best_accuracy=None,
        baseline_accuracy=None,
        iteration_count=0,
    )

    assert out["best_accuracy"] is None
    assert out["baseline_accuracy"] is None
    assert out["accuracy_delta_pp"] is None
