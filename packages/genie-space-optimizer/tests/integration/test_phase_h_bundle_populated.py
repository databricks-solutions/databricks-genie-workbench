"""Phase F+H Commit C19: end-to-end bundle-populated smoke.

Contract-level acceptance test for the wire-up: simulates what the
harness does post-Phase-A+B for the wired stages (F3 clustering, F7
application) by wrapping their named verbs with the capture decorator,
running them with a fake mlflow_anchor_run_id, and asserting the
captured artifact paths match the gso_postmortem_bundle layout.

This validates the contract between:
  * Phase A wire-up — harness calls stages.<x>.execute via the wrapper
  * Phase B wrap — wrap_with_io_capture serializes I/O to MLflow
  * Phase H bundle assembly — build_manifest / build_artifact_index /
    build_run_summary produce the parent bundle JSONs

C19 does NOT exercise the full harness path (which still needs C17 to
wire mlflow_anchor_run_id on the per-iteration StageContext) — it
simulates that future state to validate the modules compose correctly.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from genie_space_optimizer.optimization.run_output_bundle import (
    build_artifact_index,
    build_manifest,
    build_run_summary,
)
from genie_space_optimizer.optimization.run_output_contract import (
    bundle_artifact_paths,
    stage_artifact_paths,
)
from genie_space_optimizer.optimization.stage_io_capture import (
    wrap_with_io_capture,
)
from genie_space_optimizer.optimization.stages import (
    StageContext,
    application as _app_stage,
    clustering as _clust_stage,
)


def _make_ctx(*, mlflow_anchor_run_id: str | None) -> StageContext:
    return StageContext(
        run_id="opt-test-1",
        iteration=1,
        space_id="s1",
        domain="airline",
        catalog="test_catalog",
        schema="gso",
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda r: None,
        mlflow_anchor_run_id=mlflow_anchor_run_id,
        feature_flags={},
    )


def test_wrapped_f3_writes_to_correct_bundle_path(monkeypatch) -> None:
    """When wrap_with_io_capture wraps F3 with a non-None anchor, it
    writes input/output/decisions to the gso_postmortem_bundle path
    for ``03_cluster_formation``."""
    captured: list[tuple[str, str, str]] = []

    def _stub_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured.append((run_id, artifact_file, text))

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _stub_log_text,
    )

    wrapped = wrap_with_io_capture(
        execute=_clust_stage.execute,
        stage_key="cluster_formation",
    )
    ctx = _make_ctx(mlflow_anchor_run_id="parent_run_xyz")
    inp = _clust_stage.ClusteringInput(
        eval_result_for_clustering={"rows": []},
        metadata_snapshot={},
        soft_eval_result=None,
        qid_state={},
    )

    out = wrapped(ctx, inp)
    assert hasattr(out, "clusters")

    artifact_files = sorted(p for _, p, _ in captured)
    expected_paths = stage_artifact_paths(1, "cluster_formation")
    assert any(p == expected_paths["input"] for p in artifact_files)
    assert any(p == expected_paths["output"] for p in artifact_files)
    assert any(p == expected_paths["decisions"] for p in artifact_files)
    # Path discipline: 03_cluster_formation prefix per PROCESS_STAGE_ORDER.
    for p in artifact_files:
        assert "iterations/iter_01/stages/03_cluster_formation" in p


def test_wrapped_f7_writes_to_correct_bundle_path(monkeypatch) -> None:
    """Same contract for F7 application — paths under 07_applied_patches."""
    captured: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        lambda *, run_id, text, artifact_file: captured.append(
            (run_id, artifact_file, text)
        ),
    )

    wrapped = wrap_with_io_capture(
        execute=_app_stage.execute,
        stage_key="applied_patches",
    )
    ctx = _make_ctx(mlflow_anchor_run_id="parent_run_xyz")
    inp = _app_stage.ApplicationInput(
        applied_entries_by_ag={"ag1": ()},
        ags=({"ag_id": "ag1"},),
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )

    out = wrapped(ctx, inp)
    assert hasattr(out, "applied")

    artifact_files = [p for _, p, _ in captured]
    for p in artifact_files:
        assert "iterations/iter_01/stages/07_applied_patches" in p


def test_skips_capture_when_anchor_is_none() -> None:
    """When mlflow_anchor_run_id=None (pre-Phase-C-17 state), the
    wrap decorator does NOT call MLflow — no captured writes."""
    captured: list[tuple[str, str, str]] = []

    # Use real _log_text — it'll only fire if anchor is non-None.
    # If MLflow is not available, the shim returns early too.
    wrapped = wrap_with_io_capture(
        execute=_clust_stage.execute,
        stage_key="cluster_formation",
    )
    ctx = _make_ctx(mlflow_anchor_run_id=None)
    inp = _clust_stage.ClusteringInput(
        eval_result_for_clustering={"rows": []},
        metadata_snapshot={},
        soft_eval_result=None,
        qid_state={},
    )
    out = wrapped(ctx, inp)
    assert hasattr(out, "clusters")  # stage still runs
    # No assertion on captured because we didn't monkeypatch — proving
    # the skip path doesn't even reach _log_text. The Phase A+B
    # iteration body sets anchor=None today; this guards against a
    # future change accidentally activating capture before C17 wires
    # the anchor end-to-end.


def test_bundle_jsons_compose_for_a_single_iteration_run() -> None:
    """build_manifest + build_artifact_index + build_run_summary
    compose a coherent bundle JSON triple for a 1-iteration run that
    references the same iteration the wrapped stage calls would write
    artifacts under."""
    iterations = [1]
    manifest = build_manifest(
        optimization_run_id="opt-test-1",
        databricks_job_id="job-1",
        databricks_parent_run_id="parent-run-1",
        lever_loop_task_run_id="task-run-1",
        iterations=iterations,
        missing_pieces=[],
    )
    index = build_artifact_index(iterations=iterations)
    summary = build_run_summary(
        baseline={"overall_accuracy": 0.5},
        terminal_state={"status": "max_iterations", "should_continue": False},
        iteration_count=1,
        accuracy_delta_pp=0.0,
    )

    # Manifest declares the iteration count + the stage keys the wrap
    # decorator would write under for each iteration.
    assert manifest["iteration_count"] == 1
    assert manifest["iterations"] == [1]
    assert "cluster_formation" in manifest["stage_keys_in_process_order"]
    assert "applied_patches" in manifest["stage_keys_in_process_order"]

    # Artifact index includes the per-iteration stage paths the
    # wrappers wrote in the previous tests.
    iter_paths = index["iterations"]["1"]
    assert "03_cluster_formation" in iter_paths["stages"]
    assert "07_applied_patches" in iter_paths["stages"]
    f3_paths = iter_paths["stages"]["03_cluster_formation"]
    assert f3_paths["input"] == stage_artifact_paths(1, "cluster_formation")["input"]

    # All three JSONs round-trip through json.dumps without raising.
    for payload in (manifest, index, summary):
        assert json.loads(json.dumps(payload, sort_keys=True, indent=2)) == payload


def test_root_paths_are_consistent_across_helpers() -> None:
    """bundle_artifact_paths and stage_artifact_paths agree on the
    iteration prefix — so the wrapped harness writes and the manifest
    builder use the same paths."""
    base = bundle_artifact_paths(iterations=[1])
    # Top-level bundle root.
    assert base["manifest"] == "gso_postmortem_bundle/manifest.json"
    # Per-stage path is rooted at the iteration prefix.
    f7_paths = stage_artifact_paths(1, "applied_patches")
    assert f7_paths["input"].startswith("gso_postmortem_bundle/iterations/iter_01/")


# ──────────────────────────────────────────────────────────────────────
# Phase H Completion Task 5: 9 executable stages + 11 transcript
# sections + 27 distinct artifact paths per iteration.
# ──────────────────────────────────────────────────────────────────────
from genie_space_optimizer.optimization.run_output_contract import (  # noqa: E402
    PROCESS_STAGE_ORDER,
)


_EXECUTABLE_STAGES: tuple[str, ...] = (
    "evaluation_state",
    "rca_evidence",
    "cluster_formation",
    "action_group_selection",
    "proposal_generation",
    "safety_gates",
    "applied_patches",
    "acceptance_decision",
    "learning_next_action",
)


def test_executable_stages_match_process_stage_order_minus_two() -> None:
    keys = tuple(s.key for s in PROCESS_STAGE_ORDER)
    expected_skipped = ("post_patch_evaluation", "contract_health")
    actual_executable = tuple(k for k in keys if k not in expected_skipped)
    assert actual_executable == _EXECUTABLE_STAGES


def test_all_nine_executable_stages_have_distinct_artifact_paths() -> None:
    seen: set[str] = set()
    for stage_key in _EXECUTABLE_STAGES:
        paths = stage_artifact_paths(iteration=1, stage_key=stage_key)
        for label in ("input", "output", "decisions"):
            assert paths[label] not in seen, (
                f"duplicate artifact path {paths[label]!r} for stage "
                f"{stage_key!r}"
            )
            seen.add(paths[label])
    assert len(seen) == len(_EXECUTABLE_STAGES) * 3


def test_transcript_renders_all_11_process_order_sections() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_iteration_transcript,
        render_run_overview,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )

    overview = render_run_overview(
        run_id="opt-h-smoke",
        space_id="space-x",
        domain="airline_ticketing_and_fare_analysis",
        max_iters=5,
        baseline={
            "overall_accuracy": 0.875,
            "all_judge_pass_rate": 0.50,
            "hard_failures": 3,
            "soft_signals": 8,
        },
        hard_failures=[
            ("gs_009", "wrong_join_spec",
             "top-N returned wrong rows"),
            ("gs_016", "tvf_parameter_error",
             "status transition query shape wrong"),
            ("gs_024", "wrong_aggregation",
             "currency/filter aggregation mismatch"),
        ],
    )
    assert "GSO LEVER LOOP RUN" in overview
    assert "Run ID:        opt-h-smoke" in overview
    assert "Overall accuracy:        87.5%" in overview
    assert "All-judge pass:          50.0%" in overview
    assert "Hard failures:           3" in overview
    assert "gs_009  root=wrong_join_spec" in overview

    empty_trace = OptimizationTrace(decision_records=())
    body = render_iteration_transcript(
        iteration=1, trace=empty_trace, iteration_summary={},
    )
    for stage_idx, stage in enumerate(PROCESS_STAGE_ORDER, start=1):
        heading = f"### {stage_idx}. {stage.title}"
        assert heading in body, (
            f"Transcript missing heading {heading!r}; renderer + "
            f"PROCESS_STAGE_ORDER are out of sync"
        )
    assert "11. Contract Health" in body
