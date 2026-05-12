"""Plan P-A acceptance test (depends on Bundle Status Wiring Fix).

Drives the per-iter writer + the assembler completeness check in
sequence and asserts:
  1. ``assembler_completeness_check`` reports ``complete=True`` because
     every declared per-iter contract path was materialized.
  2. ``GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1`` would NOT fire (the caller's
     `if not report["complete"]:` branch is False).
  3. ``build_contract_health_summary`` reports
     ``bundle_status == "complete"`` end-to-end.

This is the integration test the user pinned in the plan-charter
dependency: it cannot pass without both P-A (this plan) AND the
Bundle Status Wiring Fix landing.
"""
from __future__ import annotations


def test_completeness_after_per_iter_materialization_is_complete() -> None:
    from genie_space_optimizer.optimization import harness as h
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
        bundle_artifact_paths,
    )

    iterations = [1, 2, 3]
    materialized: list[str] = []

    class _RecordingClient:
        def log_text(
            self, *, run_id: str, text: str, artifact_file: str,
        ) -> None:
            materialized.append(artifact_file)

    client = _RecordingClient()
    paths = bundle_artifact_paths(iterations=iterations)

    # Synthesize the parent-bundle uploads (mirrors the harness terminate
    # path's parent-bundle upload block).
    for parent_key in (
        "manifest", "run_summary", "artifact_index",
        "operator_transcript", "decision_trace_all",
        "journey_validation_all", "replay_fixture",
        "scoreboard", "failure_buckets",
    ):
        client.log_text(
            run_id="anchor", text="{}", artifact_file=paths[parent_key],
        )

    # Drive the per-iter writer for all 3 iterations.
    h._materialize_per_iter_contract_paths(
        client=client,
        anchor_run_id="anchor",
        iterations=iterations,
        iter_summaries={
            1: {"iteration": 1, "exit_path": "completed"},
            2: {"iteration": 2, "exit_path": "rolled_back"},
            3: {"iteration": 3, "exit_path": "skipped_no_applied_patches"},
        },
        iter_decision_records={1: [], 2: [], 3: []},
        iter_journey_reports={},
        iter_rca_ledgers={},
        iter_proposal_inventories={},
        iter_transcripts={1: "", 2: "", 3: ""},
        stage_capture_index={},
        iter_invariant_violations={},
    )

    declared_paths: list[str] = []
    for k, v in paths.items():
        if k == "iterations":
            for iter_paths in v.values():
                for p in iter_paths.values():
                    if isinstance(p, str):
                        declared_paths.append(p)
        elif isinstance(v, str):
            declared_paths.append(v)

    report = assembler_completeness_check(
        declared_paths=declared_paths,
        materialized_paths=materialized,
    )

    assert report["complete"] is True, (
        f"Bundle still incomplete after Plan P-A materializer ran. "
        f"Missing parent: {report['parent_level_missing']}, "
        f"missing per-iter: {report['unmigrated_per_iteration_missing']}"
    )
    assert report["unmigrated_per_iteration_missing"] == []
    assert report["parent_level_missing"] == []
    # Sanity: 3 iterations × 8 per-iter paths + 9 parent paths = 33 declared.
    assert report["total_declared"] == 33
    assert report["missing_count"] == 0


def test_contract_health_reports_bundle_status_complete_after_p_a() -> None:
    """Plan P-A acceptance gate: with the Bundle Status Wiring Fix in
    place AND the per-iter materializer running, the end-of-run
    ``GSO_CONTRACT_HEALTH_V1`` marker reports
    ``bundle_status == "complete"``.

    This test mirrors the wiring-fix shim test
    (``tests/unit/test_bundle_status_payload_accumulation.py``) but
    drives the full pipeline: parent uploads + per-iter materializer
    + completeness check + contract-health builder.
    """
    from genie_space_optimizer.optimization import harness as h
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
        bundle_artifact_paths,
    )

    iterations = [1, 2]
    materialized: list[str] = []

    class _RecordingClient:
        def log_text(
            self, *, run_id: str, text: str, artifact_file: str,
        ) -> None:
            materialized.append(artifact_file)

    client = _RecordingClient()
    paths = bundle_artifact_paths(iterations=iterations)
    for parent_key in (
        "manifest", "run_summary", "artifact_index",
        "operator_transcript", "decision_trace_all",
        "journey_validation_all", "replay_fixture",
        "scoreboard", "failure_buckets",
    ):
        client.log_text(
            run_id="anchor", text="{}", artifact_file=paths[parent_key],
        )

    h._materialize_per_iter_contract_paths(
        client=client,
        anchor_run_id="anchor",
        iterations=iterations,
        iter_summaries={
            1: {"iteration": 1, "exit_path": "completed"},
            2: {"iteration": 2, "exit_path": "rolled_back"},
        },
        iter_decision_records={1: [], 2: []},
        iter_journey_reports={},
        iter_rca_ledgers={},
        iter_proposal_inventories={},
        iter_transcripts={1: "", 2: ""},
        stage_capture_index={},
        iter_invariant_violations={},
    )

    declared_paths: list[str] = []
    for k, v in paths.items():
        if k == "iterations":
            for iter_paths in v.values():
                for p in iter_paths.values():
                    if isinstance(p, str):
                        declared_paths.append(p)
        elif isinstance(v, str):
            declared_paths.append(v)
    report = assembler_completeness_check(
        declared_paths=declared_paths, materialized_paths=materialized,
    )
    bundle_assembly_incomplete = (
        None if report["complete"] else dict(report)
    )

    summary = build_contract_health_summary(
        optimization_run_id="opt_test",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=bundle_assembly_incomplete,
        replay_validation=None,
    )
    assert summary.bundle_status == "complete"
