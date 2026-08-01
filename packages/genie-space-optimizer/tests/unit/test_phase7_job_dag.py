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
    assert params.get("benchmark_policy") == "repair_allowed"
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


def test_every_task_receives_benchmark_policy():
    job = _load_job()
    for task in job["tasks"]:
        bp = task["notebook_task"]["base_parameters"]
        assert bp["benchmark_policy"] == "{{job.parameters.benchmark_policy}}"


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
    assert "build_actionable_warning_repair(" in src
    assert "changed_benchmarks=_warning_repairs_applied" in src
    assert '"quality_findings"' in src
    assert '"semantic_review_coverage"' in src
    assert "review_benchmark_format_coverage(" in src
    assert '"format_coverage"' in src


def test_repair_task_allows_cascading_warning_repairs():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_benchmark_qc_and_repair.py"
    ).read_text()

    assert "_warning_repair_attempted_ids" not in src
    assert "_warning_repair_rounds_by_id" in src
    assert '"warning_repair_rounds": dict(_warning_repair_rounds_by_id)' in src
    assert '_quality_result_for_benchmark(benchmark).get("disposition")' in src


def test_benchmark_qc_is_a_required_verified_handoff():
    jobs_dir = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
    )
    repair_src = (jobs_dir / "run_benchmark_qc_and_repair.py").read_text()
    assert (
        'write_required_artifact(\n'
        '    spark, run_id, "benchmark_qc", _qc_payload,'
    ) in repair_src

    for notebook_name in ("run_optimize.py", "run_publish_and_audit.py"):
        src = (jobs_dir / notebook_name).read_text()
        load = src.index("_benchmark_qc_record = load_latest_artifact_record(")
        missing_gate = src.index("if _benchmark_qc_record is None:", load)
        verified_payload = src.index(
            '_benchmark_qc = _benchmark_qc_record["payload"]',
            missing_gate,
        )
        eligibility_gate = src.index(
            'if _benchmark_qc.get("optimization_eligible") is False:',
            verified_payload,
        )
        assert load < missing_gate < verified_payload < eligibility_gate


def test_repair_task_uses_persisted_push_mutation_count():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_benchmark_qc_and_repair.py"
    ).read_text()
    assert '_push.get("benchmark_mutation_count")' in src
    assert 'getattr(_push_report, "added_count"' not in src


def test_review_only_policy_never_enters_generation_or_live_push():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_benchmark_qc_and_repair.py"
    ).read_text()

    assert "extract_review_only_benchmarks(" in src
    assert 'if benchmark_policy == "review_only":' in src
    assert (
        'if benchmark_policy == "repair_allowed" and not _repair_failed'
        in src
    )
    review_start = src.index('if benchmark_policy == "review_only":')
    repair_start = src.index("    else:\n        ctx_bench = preflight_generate_benchmarks(")
    assert review_start < repair_start


def test_insufficient_qc_short_circuits_optimize_before_input_load_or_loop():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_optimize.py"
    ).read_text()

    skip_gate = src.index('if _benchmark_qc.get("optimization_eligible") is False:')
    input_load = src.index("def _load_optimize_inputs(")
    loop_call = src.index("run_unified_optimization_loop(", input_load)
    assert skip_gate < input_load < loop_call
    assert '"status": "SKIPPED"' in src[skip_gate:input_load]


def test_repair_task_enforces_corpus_floor_before_publish_and_optimize():
    src = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_benchmark_qc_and_repair.py"
    ).read_text()
    floor_call = "        require_minimum_valid_benchmarks(\n            _benchmarks,"
    push_call = "        _push = preflight_push_benchmarks_to_space("
    assert floor_call in src
    assert push_call in src
    assert src.index(floor_call) < src.index(push_call)


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
        benchmark_policy="review_only",
        workload_warehouse_ids='["wh-a","wh-b"]',
    )
    assert resolved_job_id == 99
    params = ws.jobs.run_now.call_args.kwargs["job_parameters"]
    assert params["target_accuracy"] == "0.85"
    assert params["max_attempts"] == "5"
    assert params["benchmark_policy"] == "review_only"
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
    assert params["benchmark_policy"] == "repair_allowed"


def test_no_dbutils_notebook_run_or_task_values_in_new_notebooks():
    """D9: the new notebook entrypoints must not use dbutils.notebook.run or
    inter-task task values."""
    jobs_dir = _PKG_ROOT / "src" / "genie_space_optimizer" / "jobs"
    for nb in _TASK_NOTEBOOK.values():
        src = (jobs_dir / nb).read_text()
        assert "dbutils.notebook.run" not in src, f"{nb} uses dbutils.notebook.run"
        assert "taskValues.set" not in src, f"{nb} sets task values"
        assert "taskValues.get" not in src, f"{nb} reads task values directly"
