"""Contract: the ontology materialize job threads the LLM endpoint like its siblings.

Regression guard for the ``llm_auto=0`` gap. ``ontology-materialize-runner`` (in the
**root** ``databricks.yml``) was the only GSO job NOT passing ``llm_model``, so the batch
page-body drafter fell back to the wheel-default endpoint — which the job's run_as identity
could not ``CAN QUERY`` — and every draft degraded to a stub (``body_source != llm_auto``),
exactly as MV-D43 degrade-not-hang intends, but with zero ``llm_auto`` pages.

The fix mirrors the optimization jobs (``run_optimize`` / ``run_intake_and_snapshot``):
a ``llm_model`` job parameter (default ``${var.llm_model}``) + a task ``base_parameter`` +
the notebook reading the widget into ``os.environ['LLM_MODEL']`` BEFORE the enrichers resolve
``get_llm_endpoint()``. This pins that wiring (and fails loudly if the root bundle — which the
package-level ``test_phase7_job_dag`` never reads — stops parsing, the near-miss that seeded
this test)."""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

import genie_space_optimizer

# parents[4] == repo root (matches test_exposure_matrix); the ontology job lives in the
# ROOT databricks.yml, not the package one that test_phase7_job_dag reads.
_REPO_ROOT = Path(genie_space_optimizer.__file__).resolve().parents[4]
_ROOT_BUNDLE = _REPO_ROOT / "databricks.yml"
_NOTEBOOK = (
    _REPO_ROOT / "packages" / "genie-space-optimizer" / "src"
    / "genie_space_optimizer" / "jobs" / "run_ontology_materialize.py"
)


def _ontology_job() -> dict:
    assert _ROOT_BUNDLE.is_file(), f"{_ROOT_BUNDLE} missing"
    doc = yaml.safe_load(_ROOT_BUNDLE.read_text())  # also fails loudly on a broken bundle
    return doc["resources"]["jobs"]["ontology-materialize-runner"]


def test_ontology_job_declares_llm_model_param_with_var_default():
    params = {p["name"]: p["default"] for p in _ontology_job()["parameters"]}
    assert params.get("llm_model") == "${var.llm_model}", (
        "ontology-materialize-runner must declare llm_model (default ${var.llm_model}), "
        "like the optimization jobs — else the batch drafter uses the wheel default endpoint"
    )


def test_ontology_task_threads_llm_model_base_parameter():
    bp = _ontology_job()["tasks"][0]["notebook_task"]["base_parameters"]
    assert bp.get("llm_model") == "{{job.parameters.llm_model}}", (
        "the job param must reach the notebook via base_parameters (the pass-through trap): "
        f"got {bp.get('llm_model')!r}"
    )


def test_notebook_reads_llm_model_widget_into_env_before_the_enrichers():
    src = _NOTEBOOK.read_text()
    assert 'dbutils.widgets.text("llm_model"' in src, "notebook must declare the llm_model widget"
    assert 'os.environ["LLM_MODEL"] = llm_model' in src, (
        "notebook must set LLM_MODEL from the widget (mirrors run_optimize/run_intake)"
    )
    # The env must be set before the enrichers resolve get_llm_endpoint() (page_drafter et al).
    assert src.index('os.environ["LLM_MODEL"] = llm_model') < src.index("default_page_drafter("), (
        "LLM_MODEL must be set before default_page_drafter() resolves the endpoint"
    )
