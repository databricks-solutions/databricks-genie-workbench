"""Batch materialize `run_as` wiring (MV-D50, spec §11).

The ontology materialize job reads system tables as a configurable `run_as`
identity so no app-SP system-table grant is required. Offline contract:

- ``databricks.yml`` declares the additive ``ontology_job_run_as`` variable
  (empty default) and the ``ontology-materialize-runner`` job references it.
- Substituting the variable renders ``run_as`` as ``{user_name}`` or
  ``{service_principal_name}`` when set; an empty value renders no run_as block
  (backward-compatible — runs as the deploy identity).
- The reworded description/header no longer say the job reads "as the SP".
"""

from __future__ import annotations

import pathlib

import yaml

_REPO = pathlib.Path(__file__).resolve().parents[2]
_DATABRICKS_YML = _REPO / "databricks.yml"
_RUN_AS_REF = "${var.ontology_job_run_as}"


def _bundle() -> dict:
    return yaml.safe_load(_DATABRICKS_YML.read_text())


def _job() -> dict:
    return _bundle()["resources"]["jobs"]["ontology-materialize-runner"]


def _substitute(obj, value):
    """Emulate DABs substitution of ${var.ontology_job_run_as} for this one field."""
    if isinstance(obj, dict):
        return {k: _substitute(v, value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute(v, value) for v in obj]
    if obj == _RUN_AS_REF:
        return value
    return obj


def test_variable_declared_additive_and_empty_default():
    var = _bundle()["variables"]["ontology_job_run_as"]
    assert var.get("type") == "complex"
    # Empty default ⇒ unset is backward-compatible (no run_as).
    assert not var.get("default")


def test_job_references_run_as_variable():
    assert _job().get("run_as") == _RUN_AS_REF


def test_run_as_renders_user_when_set():
    rendered = _substitute(_job(), {"user_name": "admin@company.com"})
    assert rendered["run_as"] == {"user_name": "admin@company.com"}


def test_run_as_renders_service_principal_when_set():
    rendered = _substitute(_job(), {"service_principal_name": "sp-app-42"})
    assert rendered["run_as"] == {"service_principal_name": "sp-app-42"}


def test_run_as_absent_when_unset():
    # The empty default (unset) renders no run_as fields → deploy identity.
    rendered = _substitute(_job(), {})
    assert not rendered["run_as"]


def test_description_no_longer_says_as_the_sp():
    job = _job()
    desc = job["description"].lower()
    assert "run_as" in desc  # frames the read identity as run_as, not "the SP"
    assert "as the sp" not in desc


def test_notebook_header_reworded_off_sp():
    header = (
        _REPO
        / "packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_ontology_materialize.py"
    ).read_text().lower()
    assert "run_as" in header
    # The old "(SP, allowlist-scoped)" / "SP reads" framing is gone.
    assert "(sp, allowlist-scoped)" not in header
