"""Phase H Task 16: end-to-end bundle discoverability smoke."""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    artifact_index_marker,
)
from genie_space_optimizer.optimization.run_output_bundle import (
    build_artifact_index,
    build_manifest,
    build_run_summary,
)
from genie_space_optimizer.optimization.run_output_contract import (
    PROCESS_STAGE_ORDER,
    bundle_artifact_paths,
    stage_artifact_paths,
)
from genie_space_optimizer.tools.marker_parser import parse_markers


def test_bundle_paths_for_3_iterations_cover_all_9_stages_per_iteration() -> None:
    paths = bundle_artifact_paths(iterations=[1, 2, 3])
    for iteration in [1, 2, 3]:
        assert iteration in paths["iterations"]
    for iteration in [1, 2, 3]:
        for stage in PROCESS_STAGE_ORDER:
            if stage.key in {"post_patch_evaluation", "contract_health"}:
                continue  # transcript-only
            stage_paths = stage_artifact_paths(iteration, stage.key)
            assert "input.json" in stage_paths["input"]
            assert "output.json" in stage_paths["output"]
            assert "decisions.json" in stage_paths["decisions"]
            # Path is process-ordered:  ../stages/<NN>_<key>/...
            assert f"iter_{iteration:02d}/stages/" in stage_paths["input"]
            assert f"_{stage.key}/" in stage_paths["input"]


def test_marker_round_trips_through_parser() -> None:
    marker = artifact_index_marker(
        optimization_run_id="opt-1",
        parent_bundle_run_id="br-1",
        artifact_index_path="gso_postmortem_bundle/artifact_index.json",
        iterations=[1, 2],
    )
    log = parse_markers(marker + "\n")
    assert log.artifact_index is not None
    assert log.artifact_index["optimization_run_id"] == "opt-1"
    assert log.artifact_index["parent_bundle_run_id"] == "br-1"


def test_manifest_artifact_index_run_summary_round_trip_via_json() -> None:
    """Build all three bundle files and assert they round-trip through json."""
    manifest = build_manifest(
        optimization_run_id="opt-1",
        databricks_job_id="j1",
        databricks_parent_run_id="r1",
        lever_loop_task_run_id="t1",
        iterations=[1, 2],
        missing_pieces=[],
    )
    index = build_artifact_index(iterations=[1, 2])
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.875},
        terminal_state={"status": "convergence", "should_continue": False},
        iteration_count=2,
        accuracy_delta_pp=4.2,
    )
    for payload in [manifest, index, summary]:
        text = json.dumps(payload, sort_keys=True, indent=2)
        assert json.loads(text) == payload


def test_artifact_index_lists_every_stage_per_iteration() -> None:
    """artifact_index includes per-stage paths under each iteration so a
    postmortem skill can index by (iteration, stage_key) without
    walking directories."""
    index = build_artifact_index(iterations=[1, 2])
    for iteration in [1, 2]:
        per_iter = index["iterations"][str(iteration)]
        # 9 executable stages (post_patch_evaluation + contract_health
        # are transcript-only and not in the per-stage paths).
        assert len(per_iter["stages"]) == 9
        assert "01_evaluation_state" in per_iter["stages"]
        # PROCESS_STAGE_ORDER positions: 8=post_patch_evaluation (transcript-only),
        # so the 9 executable stages get positions 1-7, 9, 10.
        assert "09_acceptance_decision" in per_iter["stages"]
        assert "10_learning_next_action" in per_iter["stages"]
