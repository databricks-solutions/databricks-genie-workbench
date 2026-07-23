"""Structural tests for the GSO v2 DABs job shape (databricks.yml).

Verifies the 4-task linear DAG, the deleted ``deploy``/condition tasks, the new
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
    "intake_and_snapshot",
    "benchmark_qc_and_repair",
    "optimize",
    "publish_and_audit",
]

_TASK_NOTEBOOK = {
    "intake_and_snapshot": "run_intake_and_snapshot.py",
    "benchmark_qc_and_repair": "run_benchmark_qc_and_repair.py",
    "optimize": "run_optimize.py",
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


def test_four_linear_tasks_in_order():
    job = _load_job()
    keys = [t["task_key"] for t in job["tasks"]]
    assert keys == _EXPECTED_TASKS, f"expected the 4-task DAG in order; got {keys}"


def test_deploy_and_legacy_tasks_removed():
    job = _load_job()
    keys = {t["task_key"] for t in job["tasks"]}
    for retired in (
        "deploy",
        "preflight",
        "baseline_eval",
        "02_baseline_eval_and_triage",
        "enrichment",
        "lever_loop",
        "finalize",
        "03_optimize",
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
    assert "depends_on" not in by_key["intake_and_snapshot"]
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
    assert params.get("workload_warehouse_ids") == "[]"


def test_legacy_deploy_target_param_removed():
    job = _load_job()
    names = {p["name"] for p in job["parameters"]}
    # deploy is out of scope (D7) — its param is gone.
    assert "deploy_target" not in names
    assert "max_iterations" not in names


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
    bp = by_key["benchmark_qc_and_repair"]["notebook_task"]["base_parameters"]
    assert "benchmark_repair_max_tries" in bp


def test_intake_receives_optional_workload_warehouses():
    job = _load_job()
    by_key = {t["task_key"]: t for t in job["tasks"]}
    bp = by_key["intake_and_snapshot"]["notebook_task"]["base_parameters"]
    assert bp["workload_warehouse_ids"] == "{{job.parameters.workload_warehouse_ids}}"


def test_repair_task_uses_canonical_quality_review_and_persists_findings():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_benchmark_qc_and_repair.py"
    ).read_text()
    assert "review_benchmark_quality(" in src
    assert '"quality_findings"' in src
    assert '"semantic_review_coverage"' in src


def test_optimize_and_publish_receive_loop_params():
    job = _load_job()
    by_key = {t["task_key"]: t for t in job["tasks"]}
    for key in ("optimize", "publish_and_audit"):
        bp = by_key[key]["notebook_task"]["base_parameters"]
        assert "max_attempts" in bp
        assert "target_accuracy" in bp
    assert "max_iterations" not in by_key["optimize"]["notebook_task"]["base_parameters"]


def test_submit_optimization_threads_loop_knobs_into_job_parameters():
    """GSO v2 Phase 10 (item 4): an app-chosen target_accuracy / max_attempts
    rides into the Jobs run_now job_parameters so a user override beats the
    databricks.yml default."""
    from unittest.mock import MagicMock

    from genie_space_optimizer.backend.job_launcher import submit_optimization

    ws = MagicMock()
    ws.jobs.run_now.return_value = MagicMock(run_id=4242)
    job_run_id, resolved_job_id = submit_optimization(
        ws,
        job_id=99,
        run_id="r1",
        space_id="s1",
        domain="d",
        catalog="c",
        schema="sch",
        target_accuracy="0.85",
        max_attempts="5",
        workload_warehouse_ids='["wh-a","wh-b"]',
    )
    assert resolved_job_id == 99
    params = ws.jobs.run_now.call_args.kwargs["job_parameters"]
    assert params["target_accuracy"] == "0.85"
    assert params["max_attempts"] == "5"
    assert params["workload_warehouse_ids"] == '["wh-a","wh-b"]'


def test_submit_optimization_loop_knobs_default_to_job_defaults():
    """When the caller omits the knobs, submit_optimization passes the same
    defaults the databricks.yml declares (0.90 / 3)."""
    from unittest.mock import MagicMock

    from genie_space_optimizer.backend.job_launcher import submit_optimization

    ws = MagicMock()
    ws.jobs.run_now.return_value = MagicMock(run_id=1)
    submit_optimization(
        ws, job_id=1, run_id="r", space_id="s", domain="d", catalog="c", schema="x",
    )
    params = ws.jobs.run_now.call_args.kwargs["job_parameters"]
    assert params["target_accuracy"] == "0.90"
    assert params["max_attempts"] == "3"


def test_no_dbutils_notebook_run_or_task_values_in_new_notebooks():
    """D9: the new notebook entrypoints must not use dbutils.notebook.run or
    inter-task task values."""
    jobs_dir = _PKG_ROOT / "src" / "genie_space_optimizer" / "jobs"
    for nb in _TASK_NOTEBOOK.values():
        src = (jobs_dir / nb).read_text()
        assert "dbutils.notebook.run" not in src, f"{nb} uses dbutils.notebook.run"
        assert "taskValues.set" not in src, f"{nb} sets task values"
        assert "taskValues.get" not in src, f"{nb} reads task values directly"
