"""GSO v2 Phase 5 decommission guards (D3/D6/D7).

Locks in the removal of the MLflow tracking/versioning path, the UC Model
Registry path, the MLflow Review App labeling session, the MLflow Prompt
Registry judge-prompt registration gate, and the now-dead MLflow pointer
columns / job param. These are source/contract-level guards so the dead paths
cannot quietly return.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest


# ── Item 1 + 2: models.py is Delta-only — no MLflow LoggedModel / UC registry ──

def test_models_module_has_no_mlflow_loggedmodel_or_uc_symbols():
    from genie_space_optimizer.optimization import models as models_mod

    for sym in (
        "create_genie_model_version",
        "link_eval_scores_to_model",
        "rollback_to_model",
        "register_uc_model",
        "_register_uc_version",
        "_GenieConfigSnapshot",
        "_extract_space_dimensions",
    ):
        assert not hasattr(models_mod, sym), f"{sym} should be removed from models.py"

    # promote_best_model survives as the Delta-only champion selector.
    assert hasattr(models_mod, "promote_best_model")


def test_models_module_does_not_import_mlflow():
    from genie_space_optimizer.optimization import models as models_mod

    src = Path(inspect.getfile(models_mod)).read_text(encoding="utf-8")
    assert "import mlflow" not in src
    assert "mlflow." not in src


def test_promote_best_model_is_delta_only():
    from genie_space_optimizer.optimization import models as models_mod

    src = inspect.getsource(models_mod.promote_best_model)
    assert "mark_champion_iteration" in src
    assert "register_uc_model" not in src
    assert "set_logged_model_alias" not in src
    assert "best_model_id" not in src


# ── Item 2: UC Model Registry config switches removed ──

def test_uc_model_registration_config_removed():
    from genie_space_optimizer.common import config as cfg

    for sym in (
        "ENABLE_UC_MODEL_REGISTRATION",
        "UC_REGISTERED_MODEL_TEMPLATE",
        "DEPLOYMENT_JOB_NAME_TEMPLATE",
        "MODEL_NAME_TEMPLATE",
    ):
        assert not hasattr(cfg, sym), f"{sym} should be removed from config.py"


# ── Item 2: cross-env deploy implementation removed ──

def test_cross_env_deploy_removed():
    from genie_space_optimizer.backend import job_launcher

    assert not hasattr(job_launcher, "ensure_deployment_job")
    assert "run_cross_env_deploy" not in job_launcher._NOTEBOOK_SOURCES
    assert "run_deploy_approval" not in job_launcher._NOTEBOOK_SOURCES

    jobs_dir = Path(job_launcher.__file__).resolve().parent.parent / "jobs"
    assert not (jobs_dir / "run_cross_env_deploy.py").exists()
    assert not (jobs_dir / "run_deploy_approval.py").exists()


# ── Item 3: MLflow Review App labeling session removed; Delta flagging kept ──

def test_labeling_module_review_app_removed_flagging_kept():
    from genie_space_optimizer.optimization import labeling

    for sym in (
        "create_review_session",
        "ensure_labeling_schemas",
        "ingest_human_feedback",
        "sync_corrections_to_dataset",
    ):
        assert not hasattr(labeling, sym), f"{sym} (MLflow Review App) should be removed"

    # Delta-backed flagging (the NEEDS_REVIEW surfacing) survives.
    for sym in ("flag_for_human_review", "get_flagged_questions", "resolve_stale_flags"):
        assert hasattr(labeling, sym)


def test_labeling_module_does_not_import_mlflow():
    from genie_space_optimizer.optimization import labeling

    src = Path(inspect.getfile(labeling)).read_text(encoding="utf-8")
    assert "import mlflow" not in src
    assert "mlflow." not in src


def test_labeling_run_name_removed():
    from genie_space_optimizer.common import mlflow_names

    assert not hasattr(mlflow_names, "labeling_run_name")


# ── Item 4: MLflow Prompt Registry judge registration + gate removed ──

def test_register_judge_prompts_and_strict_gate_removed():
    from genie_space_optimizer.optimization import evaluation

    assert not hasattr(evaluation, "register_judge_prompts")
    assert not hasattr(evaluation, "STRICT_PROMPT_REGISTRATION")


def test_preflight_prompt_registry_gate_removed():
    from genie_space_optimizer.optimization import preflight

    assert not hasattr(preflight, "preflight_probe_prompt_registry")
    src = Path(inspect.getfile(preflight)).read_text(encoding="utf-8")
    assert "check_prompt_registry" not in src


def test_judge_prompts_remain_config_constants():
    # D6: judge prompts stay as versioned config.py constants.
    from genie_space_optimizer.common import config as cfg

    assert isinstance(cfg.JUDGE_PROMPTS, dict) and cfg.JUDGE_PROMPTS


# ── Item 5: scrubbed Delta columns ──

def test_runs_ddl_scrubbed_mlflow_pointer_columns():
    from genie_space_optimizer.optimization import ddl

    runs_ddl = ddl._GENIE_OPT_RUNS_DDL
    for col in (
        "best_model_id",
        "experiment_name",
        "experiment_id",
        "labeling_session_name",
        "labeling_session_run_id",
        "labeling_session_url",
    ):
        assert col not in runs_ddl, f"{col} should be scrubbed from genie_opt_runs DDL"


def test_iterations_ddl_scrubbed_mlflow_pointer_columns():
    from genie_space_optimizer.optimization import ddl

    iters_ddl = ddl._GENIE_OPT_ITERATIONS_DDL
    assert "mlflow_run_id" not in iters_ddl
    assert "model_id" not in iters_ddl


def test_labeling_columns_removed_from_additive_migrations():
    from genie_space_optimizer.optimization import ddl

    migrated_cols = {col for _table, col, _def in ddl.ADDITIVE_COLUMN_MIGRATIONS}
    assert "labeling_session_name" not in migrated_cols
    assert "labeling_session_run_id" not in migrated_cols
    assert "labeling_session_url" not in migrated_cols


# ── Item 5: removed kwargs no longer in writer/launcher signatures ──

def test_write_iteration_signature_drops_model_id():
    from genie_space_optimizer.optimization.state import write_iteration

    params = inspect.signature(write_iteration).parameters
    assert "model_id" not in params


def test_update_run_status_signature_drops_mlflow_pointers():
    from genie_space_optimizer.optimization.state import update_run_status

    params = inspect.signature(update_run_status).parameters
    for p in (
        "best_model_id",
        "experiment_name",
        "experiment_id",
        "labeling_session_name",
        "labeling_session_run_id",
        "labeling_session_url",
    ):
        assert p not in params, f"update_run_status should not accept {p}"


def test_create_run_signature_drops_experiment_columns():
    from genie_space_optimizer.optimization.state import create_run

    params = inspect.signature(create_run).parameters
    assert "experiment_name" not in params
    assert "experiment_id" not in params


def test_submit_optimization_drops_experiment_name_job_param():
    from genie_space_optimizer.backend.job_launcher import submit_optimization

    params = inspect.signature(submit_optimization).parameters
    assert "experiment_name" not in params


def test_databricks_yml_has_no_experiment_name_param():
    from genie_space_optimizer.backend import job_launcher

    pkg_root = Path(job_launcher.__file__).resolve().parents[3]
    yml = (pkg_root / "databricks.yml").read_text(encoding="utf-8")
    assert "experiment_name" not in yml


# ── Cross-review regression guards (codex review of PR #238) ──
# Three parallel dangling-reference spots missed in the first pass: a notebook
# entrypoint importing the removed preflight probe (ImportError at runtime —
# unit suite never imports job notebooks), the iteration ORM mirror, and the
# app-backend Lakebase reader still SELECTing the scrubbed columns.

def test_run_preflight_notebook_has_no_prompt_probe_reference():
    from genie_space_optimizer.optimization import preflight

    jobs_dir = Path(preflight.__file__).resolve().parent.parent / "jobs"
    src = (jobs_dir / "run_preflight.py").read_text(encoding="utf-8")
    assert "preflight_probe_prompt_registry" not in src


def test_iteration_orm_mirror_has_no_mlflow_pointer_fields():
    from genie_space_optimizer.backend.models_db import GSOIterationRecord

    fields = set(getattr(GSOIterationRecord, "model_fields", {}) or {})
    fields |= set(getattr(GSOIterationRecord, "__annotations__", {}) or {})
    assert "mlflow_run_id" not in fields
    assert "model_id" not in fields


def test_app_lakebase_iteration_select_drops_scrubbed_columns():
    # backend/services/gso_lakebase.py is the app backend (separate package),
    # so inspect it by path rather than importing it.
    repo_root = Path(__file__).resolve().parents[4]
    src = (repo_root / "backend" / "services" / "gso_lakebase.py").read_text(encoding="utf-8")
    import re

    # Find the explicit column-list string in load_gso_iterations (the line
    # that starts the SELECT projection); assert it lists neither scrubbed col.
    m = re.search(r'"run_id, iteration, lever, eval_scope, timestamp,[^"]*"', src)
    assert m is not None, "expected the load_gso_iterations SELECT column list"
    select_cols = m.group(0)
    assert "mlflow_run_id" not in select_cols
    assert "model_id" not in select_cols


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
