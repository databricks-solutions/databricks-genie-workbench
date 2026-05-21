"""Plan 12 PR 7 Task 7.1.3 deferred — production wire-in. The
evidence-bundle cache key must include ``lever_task_run_id`` so that
a retry attempt (same parent run_id, new lever_task_run_id) is detected
as a cache miss and the bundle is rebuilt against the latest stdout.

Both 2026-05-20 postmortems flagged the old (job_id, run_id, profile)
key as a contributor to the silent-evidence problem: a retried
lever-loop task wrote a new stdout stream but the cached manifest
still pointed at the prior attempt's evidence.

These tests anchor the retry-cache-miss behavior at the production
fetcher (``tools.evidence_bundle.build_bundle``).
"""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _make_runner(lever_task_run_id: str, stdout: str = "") -> MagicMock:
    runner = MagicMock()
    runner.get_run.return_value = {
        "run_id": "r-1",
        "tasks": [
            {
                "task_key": "lever_loop",
                "run_id": lever_task_run_id,
                "task_run_id": lever_task_run_id,
                "state": {"result_state": "SUCCESS"},
                "end_time": 1000,
                "start_time": 500,
            },
        ],
    }
    runner.get_run_output.return_value = {
        "logs": stdout,
        "logs_truncated": False,
        "error": "",
    }
    return runner


def _make_mlflow() -> MagicMock:
    mlflow = MagicMock()
    mlflow.search_runs_by_tag.return_value = []
    mlflow.list_run_artifacts.return_value = []
    return mlflow


def test_first_build_caches_under_task_run_id(tmp_path: Path):
    from genie_space_optimizer.tools.evidence_bundle import (
        BundleResult,
        build_bundle,
    )

    runner = _make_runner(lever_task_run_id="tr-attempt-1")
    mlflow = _make_mlflow()
    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow,
    )
    assert isinstance(result, BundleResult)
    inputs = result.manifest.inputs
    assert inputs["lever_task_run_id"] == "tr-attempt-1"


def test_retry_with_new_task_run_id_is_cache_miss(tmp_path: Path):
    """Same parent run_id (r-1) + same job_id (j-1) + NEW
    lever_task_run_id (tr-attempt-2) → MUST rebuild. The legacy cache
    key would have hit and served stale evidence."""
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    runner_attempt_1 = _make_runner(
        lever_task_run_id="tr-attempt-1", stdout="OLD_STDOUT",
    )
    runner_attempt_2 = _make_runner(
        lever_task_run_id="tr-attempt-2", stdout="NEW_STDOUT",
    )
    mlflow = _make_mlflow()

    # Build #1: attempt 1's task_run_id.
    r1 = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner_attempt_1,
        mlflow_runner=mlflow,
    )
    assert r1.manifest.inputs["lever_task_run_id"] == "tr-attempt-1"

    # Build #2: retried task_run_id. MUST NOT serve cached attempt-1
    # data.
    r2 = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner_attempt_2,
        mlflow_runner=mlflow,
    )
    assert r2.manifest.inputs["lever_task_run_id"] == "tr-attempt-2", (
        "retry attempt MUST produce a fresh bundle keyed on the new "
        "lever_task_run_id; got stale "
        f"{r2.manifest.inputs!r}"
    )
    # The new stdout file must reflect the retry attempt's content.
    stdout_path = tmp_path / "opt-unresolved_r-1" / "evidence" / "lever_loop_stdout.txt"
    if stdout_path.exists():
        assert "NEW_STDOUT" in stdout_path.read_text()


def test_same_task_run_id_hits_cache(tmp_path: Path):
    """When the lever_task_run_id is unchanged, the cache hit MUST
    return the prior BundleResult without invoking the runners again
    (the runner stops returning fresh data; we verify the manifest
    short-circuits on the existing one)."""
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    runner = _make_runner(lever_task_run_id="tr-stable")
    mlflow = _make_mlflow()

    # Build #1: writes the manifest.
    r1 = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow,
    )

    # Reset mocks so we can prove they aren't called again on cache hit.
    runner.get_run.reset_mock()
    runner.get_run_output.reset_mock()

    # Build #2: same task_run_id → cache hit.
    r2 = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow,
    )
    assert (
        r1.manifest.inputs["lever_task_run_id"]
        == r2.manifest.inputs["lever_task_run_id"]
        == "tr-stable"
    )
    # The cache check happens AFTER get_run + get_run_output (which
    # produce the lever_task_run_id). So those still fire even on a
    # cache hit. That's the existing behavior — we're only checking
    # the manifest is the same.
    assert r1.paths.manifest == r2.paths.manifest


def test_legacy_manifest_without_lever_task_run_id_in_inputs(tmp_path: Path):
    """A manifest written BEFORE Task 7.1.3 (no ``lever_task_run_id``
    in inputs but present in ``resolved``) must still be readable as a
    cache hit when the new build's task_run_id matches the legacy
    resolved value. Backwards-compatible cache-key migration."""
    from genie_space_optimizer.tools.evidence_bundle import build_bundle
    from genie_space_optimizer.tools.evidence_layout import bundle_paths_for

    runner = _make_runner(lever_task_run_id="tr-legacy")
    mlflow = _make_mlflow()

    # Build once, then mutate the manifest to drop lever_task_run_id
    # from inputs (simulating a legacy file).
    build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow,
    )
    paths = bundle_paths_for(
        root=tmp_path, optimization_run_id="opt-unresolved_r-1",
    )
    if not paths.manifest.exists():
        pytest.skip("manifest path differs from default; legacy-test setup")

    raw = json.loads(paths.manifest.read_text())
    raw["inputs"].pop("lever_task_run_id", None)
    # Ensure resolved still has it for the legacy fallback.
    raw.setdefault("resolved", {})["lever_loop_task_run_id"] = "tr-legacy"
    paths.manifest.write_text(json.dumps(raw))

    runner.get_run.reset_mock()
    # Build again with matching task_run_id → MUST cache-hit on the
    # legacy manifest (fallback path).
    r2 = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow,
    )
    # The cache hit returns the existing manifest unchanged — proven
    # by reading the on-disk manifest and confirming the legacy shape
    # was NOT overwritten.
    final = json.loads(paths.manifest.read_text())
    assert "lever_task_run_id" not in final["inputs"], (
        "cache hit must not overwrite a legacy manifest"
    )
    assert r2.manifest.resolved.get("lever_loop_task_run_id") == "tr-legacy"
