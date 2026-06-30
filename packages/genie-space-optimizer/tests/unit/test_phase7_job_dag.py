"""Structural tests for the GSO v2 Phase 7 DABs job reshape (databricks.yml).

Verifies the 5-task linear DAG, the deleted ``deploy``/condition tasks, the new
job parameters, and that every task carries the job parameters as
``base_parameters`` (no inter-task task-value plumbing — D9).
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

import genie_space_optimizer

_PKG_ROOT = Path(genie_space_optimizer.__file__).resolve().parents[2]
_BUNDLE = _PKG_ROOT / "databricks.yml"

_EXPECTED_TASKS = [
    "00_intake_and_snapshot",
    "01_benchmark_qc_and_repair",
    "02_baseline_eval_and_triage",
    "03_optimize",
    "publish_and_audit",
]

_TASK_NOTEBOOK = {
    "00_intake_and_snapshot": "run_00_intake_and_snapshot.py",
    "01_benchmark_qc_and_repair": "run_01_benchmark_qc_and_repair.py",
    "02_baseline_eval_and_triage": "run_02_baseline_eval_and_triage.py",
    "03_optimize": "run_03_optimize.py",
    "publish_and_audit": "run_publish_and_audit.py",
}


def _load_job() -> dict:
    doc = yaml.safe_load(_BUNDLE.read_text())
    jobs = doc["resources"]["jobs"]
    # single job in the bundle
    (job,) = jobs.values()
    return job


def test_bundle_exists():
    assert _BUNDLE.exists()


def test_five_linear_tasks_in_order():
    job = _load_job()
    keys = [t["task_key"] for t in job["tasks"]]
    assert keys == _EXPECTED_TASKS, f"expected the 5-task DAG in order; got {keys}"


def test_deploy_and_legacy_tasks_removed():
    job = _load_job()
    keys = {t["task_key"] for t in job["tasks"]}
    for retired in (
        "deploy",
        "preflight",
        "baseline_eval",
        "enrichment",
        "lever_loop",
        "finalize",
    ):
        assert retired not in keys, f"retired task {retired} must be gone"


def test_no_condition_tasks():
    job = _load_job()
    for t in job["tasks"]:
        assert "condition_task" not in t, (
            f"task {t['task_key']} must not be a condition task (D9 linear DAG)"
        )
        assert "notebook_task" in t, f"task {t['task_key']} must be a notebook_task"


def test_dependencies_form_a_linear_chain():
    job = _load_job()
    by_key = {t["task_key"]: t for t in job["tasks"]}
    # First task has no dependency.
    assert "depends_on" not in by_key["00_intake_and_snapshot"]
    # Each subsequent task depends solely on its predecessor.
    for prev, cur in zip(_EXPECTED_TASKS, _EXPECTED_TASKS[1:]):
        deps = by_key[cur].get("depends_on", [])
        assert [d["task_key"] for d in deps] == [prev], (
            f"{cur} must depend only on {prev}; got {deps}"
        )


def test_task_notebook_paths():
    job = _load_job()
    for t in job["tasks"]:
        path = t["notebook_task"]["notebook_path"]
        assert path.endswith(_TASK_NOTEBOOK[t["task_key"]]), (
            f"{t['task_key']} -> {path}"
        )


def test_new_job_parameters_present_with_defaults():
    job = _load_job()
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params.get("max_attempts") == "3"
    assert params.get("target_accuracy") == "0.90"
    assert params.get("benchmark_repair_max_tries") == "3"


def test_legacy_deploy_target_param_removed():
    job = _load_job()
    names = {p["name"] for p in job["parameters"]}
    # deploy is out of scope (D7) — its param is gone.
    assert "deploy_target" not in names


def test_every_task_passes_run_id_via_base_parameters_not_task_values():
    """D9: no inter-task task-value plumbing. Every task bootstraps from job
    parameters (base_parameters), so each must carry run_id/catalog/schema."""
    job = _load_job()
    for t in job["tasks"]:
        bp = t["notebook_task"].get("base_parameters", {})
        for required in ("run_id", "catalog", "schema"):
            assert required in bp, (
                f"{t['task_key']} must pass {required} via base_parameters; got {list(bp)}"
            )
        # base_parameters reference job.parameters (not task values).
        assert "{{job.parameters.run_id}}" in bp["run_id"]


def test_repair_task_receives_benchmark_repair_max_tries():
    job = _load_job()
    by_key = {t["task_key"]: t for t in job["tasks"]}
    bp = by_key["01_benchmark_qc_and_repair"]["notebook_task"]["base_parameters"]
    assert "benchmark_repair_max_tries" in bp


def test_optimize_and_publish_receive_loop_params():
    job = _load_job()
    by_key = {t["task_key"]: t for t in job["tasks"]}
    for key in ("03_optimize", "publish_and_audit"):
        bp = by_key[key]["notebook_task"]["base_parameters"]
        assert "max_attempts" in bp
        assert "target_accuracy" in bp


def test_no_dbutils_notebook_run_or_task_values_in_new_notebooks():
    """D9: the new notebook entrypoints must not use dbutils.notebook.run or
    inter-task task values."""
    jobs_dir = _PKG_ROOT / "src" / "genie_space_optimizer" / "jobs"
    for nb in _TASK_NOTEBOOK.values():
        src = (jobs_dir / nb).read_text()
        assert "dbutils.notebook.run" not in src, f"{nb} uses dbutils.notebook.run"
        assert "taskValues.set" not in src, f"{nb} sets task values"
        assert "taskValues.get" not in src, f"{nb} reads task values directly"


def _publish_notebook_src() -> str:
    jobs_dir = _PKG_ROOT / "src" / "genie_space_optimizer" / "jobs"
    return (jobs_dir / "run_publish_and_audit.py").read_text()


def test_publish_notebook_delegates_to_publish_and_audit():
    """Phase 9: the notebook is a thin shell over ``optimization/publish.py``."""
    src = _publish_notebook_src()
    assert "from genie_space_optimizer.optimization.publish import publish_and_audit" in src
    assert "publish_and_audit(" in src


def test_publish_notebook_does_not_rederive_terminal_reason_from_accuracy():
    """Phase 9 correctness fix: terminal_reason is READ off the stamped champion
    row, never re-derived from accuracy-vs-target (the Phase-7 shell's collapse
    bug). The shell's tell-tale derivation must be gone from the notebook."""
    src = _publish_notebook_src()
    # The Phase-7 shell derived the reason via this accuracy comparison.
    assert "_acc_fraction" not in src
    assert 'terminal_reason = "TARGET_REACHED"' not in src
    assert 'terminal_reason = "MAX_ATTEMPTS"' not in src
