"""Unit test for Plan 5 scripts/regen_fixtures_from_bundle.py.

Verifies the orchestrator:
  * Reads bundle manifest correctly.
  * Skips plans whose capture file is missing (no crash).
  * Invokes each exporter with the right CLI args.
  * Returns a non-zero exit if any exporter fails.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _load_regen_module():
    repo_root = Path(__file__).resolve().parents[3]  # to packages/genie-space-optimizer
    script_path = repo_root / "scripts" / "regen_fixtures_from_bundle.py"
    spec = importlib.util.spec_from_file_location("regen_fixtures", script_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Register before exec_module — dataclass() inside the module needs
    # to find itself in sys.modules at class-creation time.
    sys.modules["regen_fixtures"] = mod
    spec.loader.exec_module(mod)
    return mod


def _write_minimal_bundle(tmp_path: Path) -> Path:
    """Create a synthetic bundle directory layout matching what the
    evidence_bundle puller produces."""
    opt_run_id = "test-opt-run-123"
    bundle_dir = tmp_path / opt_run_id
    evidence = bundle_dir / "evidence"
    evidence.mkdir(parents=True)
    captures = evidence / "gso_trial_captures"
    captures.mkdir()
    manifest = {
        "schema_version": 1,
        "bundle_version": 1,
        "resolved": {
            "optimization_run_id": opt_run_id,
            "mlflow_experiment_id": "exp-456",
            "anchor_mlflow_run_id": "anchor-789",
        },
        "missing_pieces": [],
    }
    (evidence / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return bundle_dir


def test_orchestrator_reports_no_captures_when_directory_empty(tmp_path):
    regen_fixtures = _load_regen_module().regen_fixtures
    bundle_dir = _write_minimal_bundle(tmp_path)
    summary = regen_fixtures(bundle_dir=bundle_dir, output_root=tmp_path / "out")
    assert summary["plans_attempted"] == []
    assert summary["plans_skipped_no_capture"] == [
        "narrowing_v1", "lever5_split_v1", "three_stage_v1", "raw_evidence_v1",
    ]
    assert summary["exit_code"] == 0


def test_orchestrator_invokes_each_exporter_for_existing_capture(tmp_path):
    regen_fixtures = _load_regen_module().regen_fixtures
    bundle_dir = _write_minimal_bundle(tmp_path)
    captures = bundle_dir / "evidence" / "gso_trial_captures"
    (captures / "narrowing_v1.ndjson").write_text(
        '{"skill_id": "lever-4-join-discovery", "rendered_at_ts": 1.0}\n',
        encoding="utf-8",
    )
    (captures / "raw_evidence_v1.ndjson").write_text(
        '{"skill_id": "lever-1-table-column-description", "structural_diff": "identical"}\n',
        encoding="utf-8",
    )
    fake_run = MagicMock(return_value=MagicMock(returncode=0, stdout="{}\n", stderr=""))
    with patch("subprocess.run", fake_run):
        summary = regen_fixtures(
            bundle_dir=bundle_dir, output_root=tmp_path / "out",
        )
    assert sorted(summary["plans_attempted"]) == ["narrowing_v1", "raw_evidence_v1"]
    assert sorted(summary["plans_skipped_no_capture"]) == [
        "lever5_split_v1", "three_stage_v1",
    ]
    # Two subprocess.run calls — one per attempted plan.
    assert fake_run.call_count == 2
    # Verify each invocation passed --mlflow-experiment-id and the
    # correct capture path.
    for call in fake_run.call_args_list:
        argv = call.args[0]
        assert "--mlflow-experiment-id" in argv
        assert argv[argv.index("--mlflow-experiment-id") + 1] == "exp-456"
        # exporter scripts use either --narrowing-capture-path,
        # --lever5-split-capture-path, --three-stage-capture-path,
        # --raw-evidence-capture-path
        assert any(a.endswith("-capture-path") for a in argv)
        assert any(a.endswith(".ndjson") for a in argv)
    assert summary["exit_code"] == 0


def test_orchestrator_propagates_nonzero_exporter_exit(tmp_path):
    regen_fixtures = _load_regen_module().regen_fixtures
    bundle_dir = _write_minimal_bundle(tmp_path)
    captures = bundle_dir / "evidence" / "gso_trial_captures"
    (captures / "narrowing_v1.ndjson").write_text("{}\n", encoding="utf-8")
    fake_run = MagicMock(return_value=MagicMock(returncode=2, stdout="", stderr="boom"))
    with patch("subprocess.run", fake_run):
        summary = regen_fixtures(
            bundle_dir=bundle_dir, output_root=tmp_path / "out",
        )
    assert summary["exit_code"] != 0
    assert summary["plans_failed"] == ["narrowing_v1"]


def test_orchestrator_handles_missing_manifest(tmp_path):
    regen_fixtures = _load_regen_module().regen_fixtures
    # Bundle directory exists but no manifest file inside it.
    (tmp_path / "evidence").mkdir()
    summary = regen_fixtures(bundle_dir=tmp_path, output_root=tmp_path / "out")
    assert summary["exit_code"] != 0
    assert summary["error"] == "manifest_missing"


def test_orchestrator_handles_missing_experiment_id(tmp_path):
    regen_fixtures = _load_regen_module().regen_fixtures
    bundle_dir = _write_minimal_bundle(tmp_path)
    # Strip experiment id from manifest.
    manifest_path = bundle_dir / "evidence" / "manifest.json"
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    m["resolved"].pop("mlflow_experiment_id")
    manifest_path.write_text(json.dumps(m), encoding="utf-8")
    summary = regen_fixtures(bundle_dir=bundle_dir, output_root=tmp_path / "out")
    assert summary["exit_code"] != 0
    assert summary["error"] == "mlflow_experiment_id_missing"


def test_orchestrator_dispatch_table_points_at_existing_scripts():
    """Plan 5 — the four exporter script paths in _DISPATCH must exist
    on disk. If a future PR renames or moves a script, this test
    catches the break before the orchestrator is run for real."""
    regen_mod = _load_regen_module()
    dispatch = regen_mod._DISPATCH  # noqa: SLF001
    repo_root = Path(__file__).resolve().parents[3]
    for plan in dispatch:
        script_path = repo_root / plan.exporter_script
        assert script_path.is_file(), (
            f"plan {plan.plan_id} dispatches to {plan.exporter_script} "
            f"but {script_path} does not exist. If you renamed the "
            f"exporter script, update _DISPATCH in "
            f"scripts/regen_fixtures_from_bundle.py."
        )
