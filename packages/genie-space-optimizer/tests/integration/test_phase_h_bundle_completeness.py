"""Cycle 12-T3 — post-upload completeness check + typed marker."""
from __future__ import annotations

import json


def test_completeness_check_clean_run_emits_no_marker() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
    )

    declared = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/replay_fixture.json",
    ]
    materialized = list(declared)

    report = assembler_completeness_check(
        declared_paths=declared,
        materialized_paths=materialized,
    )

    assert report["complete"] is True
    # Caller logic: `if not report["complete"]: emit_marker()`. We assert
    # the marker would not be emitted.


def test_completeness_check_partial_assembly_marker_payload() -> None:
    """Simulate a run that uploaded 3 of 5 declared parent paths.
    Verify the typed marker payload enumerates the gap correctly."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        bundle_assembly_incomplete_marker,
    )
    from genie_space_optimizer.optimization.run_output_contract import (
        assembler_completeness_check,
    )

    declared = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/run_summary.json",
        "gso_postmortem_bundle/replay_fixture.json",
        "gso_postmortem_bundle/scoreboard.json",
        "gso_postmortem_bundle/failure_buckets.json",
        "gso_postmortem_bundle/iterations/iter_01/decision_trace.json",
        "gso_postmortem_bundle/iterations/iter_01/operator_transcript.md",
    ]
    materialized = [
        "gso_postmortem_bundle/manifest.json",
        "gso_postmortem_bundle/run_summary.json",
        "gso_postmortem_bundle/replay_fixture.json",
    ]

    report = assembler_completeness_check(
        declared_paths=declared,
        materialized_paths=materialized,
    )

    assert report["complete"] is False
    line = bundle_assembly_incomplete_marker(
        optimization_run_id="opt_run_1",
        parent_bundle_run_id="run_anchor",
        total_declared=report["total_declared"],
        total_materialized=report["total_materialized"],
        missing_count=report["missing_count"],
        parent_level_missing=report["parent_level_missing"],
        unmigrated_per_iteration_missing=report["unmigrated_per_iteration_missing"],
    )

    payload = json.loads(line.split(" ", 1)[1])
    assert payload["missing_count"] == 4
    assert sorted(payload["parent_level_missing"]) == [
        "gso_postmortem_bundle/failure_buckets.json",
        "gso_postmortem_bundle/scoreboard.json",
    ]
    assert sorted(payload["unmigrated_per_iteration_missing"]) == [
        "gso_postmortem_bundle/iterations/iter_01/decision_trace.json",
        "gso_postmortem_bundle/iterations/iter_01/operator_transcript.md",
    ]


def test_marker_round_trips_through_marker_parser() -> None:
    """The parser surfaces the marker on `MarkerLog.bundle_assembly_incomplete`."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        bundle_assembly_incomplete_marker,
    )
    from genie_space_optimizer.tools.marker_parser import parse_markers

    line = bundle_assembly_incomplete_marker(
        optimization_run_id="opt_run_1",
        parent_bundle_run_id="run_anchor",
        total_declared=10,
        total_materialized=8,
        missing_count=2,
        parent_level_missing=["gso_postmortem_bundle/scoreboard.json"],
        unmigrated_per_iteration_missing=["gso_postmortem_bundle/iterations/iter_01/decision_trace.json"],
    )

    log = parse_markers(line + "\n")

    assert log.bundle_assembly_incomplete is not None
    assert log.bundle_assembly_incomplete[0]["missing_count"] == 2
