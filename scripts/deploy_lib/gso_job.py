"""SDK/REST-native GSO job deployment for notebook installs."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any
from urllib.parse import quote

from .config import GSO_JOB_BASENAME, GsoJobInfo, InstallConfig
from .workspace_source import (
    default_gso_path,
    mkdirs,
    upload_source_notebook,
    workspace_api_path,
    write_workspace_file,
)


# GSO v2 linear 4-task DAG. This MIRRORS the package bundle job at
# packages/genie-space-optimizer/databricks.yml (the validated source of truth,
# guarded by tests/unit/test_phase7_job_dag.py): same task keys, order,
# entrypoints, linear dependencies, and per-task base_parameters. There is NO
# condition task and NO `deploy` task (deploy is out of scope — D7). Each entry
# is (task_key, notebook_stem, depends_on, base_param_keys). `base_param_keys`
# lists the job parameters passed to that task as base_parameters (every task
# reads job params + durable Delta state by run_id — no task-value plumbing, D9).
#
# `llm_model` is a Workbench-specific extra beyond the package bundle's params:
# the local-terminal deploy passes it via `--var llm_model` (deploy.sh) and the
# notebook installer defaults it to cfg.llm_model, so the operator-selected
# serving endpoint reaches each task (every v2 entrypoint honors the llm_model
# widget). See build_job_settings for the parameter declarations.
TASKS = [
    (
        "intake_and_snapshot",
        "run_intake_and_snapshot",
        None,
        [
            "run_id", "space_id", "domain", "catalog", "schema", "apply_mode",
            "levers", "max_attempts", "target_accuracy",
            "benchmark_repair_max_tries", "triggered_by",
            "warehouse_id", "llm_model",
        ],
    ),
    (
        "benchmark_qc_and_repair",
        "run_benchmark_qc_and_repair",
        "intake_and_snapshot",
        [
            "run_id", "space_id", "domain", "catalog", "schema", "apply_mode",
            "benchmark_repair_max_tries", "warehouse_id", "llm_model",
        ],
    ),
    (
        "optimize",
        "run_optimize",
        "benchmark_qc_and_repair",
        [
            "run_id", "space_id", "domain", "catalog", "schema", "apply_mode",
            "levers", "max_attempts", "target_accuracy",
            "triggered_by", "warehouse_id", "llm_model",
        ],
    ),
    (
        "publish_and_audit",
        "run_publish_and_audit",
        "optimize",
        [
            "run_id", "space_id", "domain", "catalog", "schema", "apply_mode",
            "target_accuracy", "max_attempts", "warehouse_id", "llm_model",
        ],
    ),
]

# Declared job parameters + defaults. Mirrors the package bundle's 4-task param
# set: bounded patch/eval budget (max_attempts), stop-early target
# (target_accuracy), and benchmark repair bound (benchmark_repair_max_tries).
# `experiment_name` (MLflow decommissioned — Phase 5) and `deploy_target`
# (deploy out of scope — D7) are dropped. `llm_model` is the Workbench-specific extra (see TASKS above);
# its default is overridden with cfg.llm_model in build_job_settings.
JOB_PARAMETERS = {
    "run_id": "",
    "space_id": "",
    "domain": "default",
    "catalog": "",
    "schema": "",
    "apply_mode": "genie_config",
    "levers": "[1,2,3,4,5,6]",
    "max_attempts": "3",
    "target_accuracy": "0.90",
    "benchmark_repair_max_tries": "3",
    "triggered_by": "",
    "warehouse_id": "",
    "llm_model": "",
}


def _api_do(w, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    return w.api_client.do(method=method, path=path, body=body)


def _jobs_dir(repo_root: Path) -> Path:
    return repo_root / "packages" / "genie-space-optimizer" / "src" / "genie_space_optimizer" / "jobs"


def upload_job_notebooks(w, cfg: InstallConfig, deployer_user: str) -> str:
    repo_root = Path(cfg.repo_root or "").resolve()
    jobs_dir = _jobs_dir(repo_root)
    if not jobs_dir.exists():
        raise FileNotFoundError(f"GSO jobs directory not found: {jobs_dir}")

    notebooks_path = f"{default_gso_path(deployer_user, cfg.app_name)}/jobs"
    mkdirs(w, notebooks_path)
    for _task_key, notebook_stem, _depends_on, _base_param_keys in TASKS:
        upload_source_notebook(w, jobs_dir / f"{notebook_stem}.py", f"{notebooks_path}/{notebook_stem}")
    return notebooks_path


def build_gso_wheel(repo_root: Path) -> Path:
    package_dir = repo_root / "packages" / "genie-space-optimizer"
    if not package_dir.exists():
        raise FileNotFoundError(f"GSO package directory not found: {package_dir}")

    tmp_dir = Path(tempfile.mkdtemp(prefix="genie-gso-wheel-"))
    try:
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(tmp_dir),
            ".",
        ]
        result = subprocess.run(cmd, cwd=package_dir, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                "Could not build GSO wheel. Ensure hatchling and uv-dynamic-versioning "
                f"are installed in the notebook environment.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        wheels = sorted(tmp_dir.glob("genie_space_optimizer-*.whl"))
        if not wheels:
            raise RuntimeError("GSO wheel build completed but no wheel was produced")
        stable = tmp_dir / "genie_space_optimizer-0.0.0-py3-none-any.whl"
        shutil.copyfile(wheels[0], stable)
        return stable
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def upload_gso_wheel(w, cfg: InstallConfig) -> str:
    repo_root = Path(cfg.repo_root or "").resolve()
    wheel_path = (
        cfg.gso_wheel_path
        or f"/Volumes/{cfg.catalog}/{cfg.gso_schema}/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl"
    )
    built = build_gso_wheel(repo_root)
    try:
        if Path(wheel_path).is_absolute() and Path(wheel_path).parent.exists():
            Path(wheel_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(built, wheel_path)
        elif hasattr(w, "files"):
            with built.open("rb") as f:
                w.files.upload(wheel_path, f, overwrite=True)
        else:
            write_workspace_file(w, wheel_path, built.read_bytes())
    finally:
        shutil.rmtree(built.parent, ignore_errors=True)
    return wheel_path


def _task_payload(
    task_key: str,
    notebook_stem: str,
    depends_on: str | None,
    base_param_keys: list[str],
    notebooks_path: str,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": task_key,
        "notebook_task": {
            "notebook_path": f"{notebooks_path}/{notebook_stem}",
            "source": "WORKSPACE",
            "base_parameters": {
                key: f"{{{{job.parameters.{key}}}}}" for key in base_param_keys
            },
        },
        "environment_key": "default",
        "timeout_seconds": 14400,
        "max_retries": 0,
    }
    if depends_on:
        task["depends_on"] = [{"task_key": depends_on}]
    return task


def build_job_settings(cfg: InstallConfig, notebooks_path: str, wheel_path: str) -> dict[str, Any]:
    cfg = cfg.normalized()
    tasks = [_task_payload(*task, notebooks_path) for task in TASKS]
    return {
        "name": cfg.gso_job_name,
        "description": (
            "GSO v2 bounded hill-climbing runner managed by Genie Workbench "
            "(intake_and_snapshot -> benchmark_qc_and_repair -> "
            "optimize -> publish_and_audit). "
            "Linear 4-task serverless DAG; baseline eval and the hill-climb run "
            "in-process inside optimize through the native Benchmark API."
        ),
        "max_concurrent_runs": 20,
        "queue": {"enabled": True},
        "tags": {
            "app": cfg.app_name,
            "managed-by": "notebook-installer",
            "pattern": "persistent-dag",
        },
        "parameters": [
            {
                "name": name,
                "default": cfg.llm_model if name == "llm_model" else default,
            }
            for name, default in JOB_PARAMETERS.items()
        ],
        "tasks": tasks,
        "environments": [
            {
                "environment_key": "default",
                "spec": {
                    "environment_version": "4",
                    "dependencies": [wheel_path],
                },
            }
        ],
    }


def _job_matches_settings(
    job: dict[str, Any],
    settings: dict[str, Any],
    *,
    allow_legacy_name: bool = False,
) -> bool:
    job_settings = job.get("settings") or {}
    job_tags = job_settings.get("tags") or {}
    tags = settings.get("tags") or {}
    job_name = job_settings.get("name")
    expected_name = settings.get("name")
    name_matches = job_name == expected_name or (allow_legacy_name and job_name == GSO_JOB_BASENAME)
    return (
        name_matches
        and job_tags.get("app") == tags.get("app")
        and job_tags.get("managed-by") == tags.get("managed-by")
        and job_tags.get("pattern") == tags.get("pattern")
    )


def find_existing_job(w, settings: dict[str, Any]) -> int | None:
    page_token: str | None = None
    legacy_id: int | None = None
    while True:
        path = "/api/2.1/jobs/list?limit=100&expand_tasks=false"
        if page_token:
            path += f"&page_token={quote(page_token, safe='')}"
        data = _api_do(w, "GET", path)
        for job in data.get("jobs") or []:
            if _job_matches_settings(job, settings):
                return int(job["job_id"])
            if legacy_id is None and _job_matches_settings(job, settings, allow_legacy_name=True):
                legacy_id = int(job["job_id"])
        page_token = data.get("next_page_token")
        if not page_token:
            break
    return legacy_id


def upsert_job(w, settings: dict[str, Any]) -> int:
    existing_id = find_existing_job(w, settings)
    if existing_id:
        _api_do(w, "POST", "/api/2.1/jobs/reset", {"job_id": existing_id, "new_settings": settings})
        return existing_id
    created = _api_do(w, "POST", "/api/2.1/jobs/create", settings)
    return int(created["job_id"])


def set_job_permissions(w, job_id: int, deployer_user: str, app_sp_client_id: str) -> None:
    _api_do(
        w,
        "PUT",
        f"/api/2.0/permissions/jobs/{job_id}",
        {
            "access_control_list": [
                {"user_name": deployer_user, "permission_level": "IS_OWNER"},
                {"group_name": "users", "permission_level": "CAN_VIEW"},
                {"service_principal_name": app_sp_client_id, "permission_level": "CAN_MANAGE"},
            ]
        },
    )


def grant_directory_permissions(w, workspace_dir: str, app_sp_client_id: str) -> None:
    try:
        status = _api_do(
            w,
            "GET",
            f"/api/2.0/workspace/get-status?path={quote(workspace_api_path(workspace_dir), safe='')}",
        )
        object_id = status.get("object_id")
        if object_id:
            _api_do(
                w,
                "PATCH",
                f"/api/2.0/permissions/directories/{object_id}",
                {
                    "access_control_list": [
                        {
                            "service_principal_name": app_sp_client_id,
                            "permission_level": "CAN_MANAGE",
                        }
                    ]
                },
            )
    except Exception:
        pass


def ensure_gso_job(w, cfg: InstallConfig, app_sp_client_id: str, deployer_user: str) -> GsoJobInfo:
    cfg = cfg.normalized()
    notebooks_path = upload_job_notebooks(w, cfg, deployer_user)
    wheel_path = upload_gso_wheel(w, cfg)
    settings = build_job_settings(cfg, notebooks_path, wheel_path)
    job_id = upsert_job(w, settings)
    set_job_permissions(w, job_id, deployer_user, app_sp_client_id)
    grant_directory_permissions(w, notebooks_path, app_sp_client_id)
    return GsoJobInfo(
        job_id=job_id,
        job_name=cfg.gso_job_name,
        notebooks_path=notebooks_path,
        wheel_path=wheel_path,
    )
