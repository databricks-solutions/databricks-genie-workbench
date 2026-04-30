"""SDK/REST-native GSO job deployment for notebook installs."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any
from urllib.parse import quote

from .config import GsoJobInfo, InstallConfig
from .workspace_source import default_gso_path, mkdirs, upload_source_notebook, write_workspace_file


TASKS = [
    ("preflight", "run_preflight", None),
    ("baseline_eval", "run_baseline", "preflight"),
    ("enrichment", "run_enrichment", "baseline_eval"),
    ("lever_loop", "run_lever_loop", "enrichment"),
    ("finalize", "run_finalize", "lever_loop"),
]

JOB_PARAMETERS = {
    "run_id": "",
    "space_id": "",
    "domain": "default",
    "catalog": "",
    "schema": "",
    "apply_mode": "genie_config",
    "levers": "[1,2,3,4,5,6]",
    "max_iterations": "5",
    "triggered_by": "",
    "experiment_name": "",
    "deploy_target": "",
    "warehouse_id": "",
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
    for _, notebook_stem, _ in TASKS:
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
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(tmp_dir),
        ]
        result = subprocess.run(cmd, cwd=package_dir, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                "Could not build GSO wheel. Install the 'build' package in the notebook "
                f"environment and retry.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
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


def _task_payload(task_key: str, notebook_stem: str, depends_on: str | None, notebooks_path: str) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": task_key,
        "notebook_task": {
            "notebook_path": f"{notebooks_path}/{notebook_stem}",
            "source": "WORKSPACE",
        },
        "environment_key": "default",
        "timeout_seconds": 7200,
        "max_retries": 0,
    }
    if depends_on:
        task["depends_on"] = [{"task_key": depends_on}]
    if task_key == "preflight":
        task["notebook_task"]["base_parameters"] = {
            "run_id": "{{job.parameters.run_id}}",
            "space_id": "{{job.parameters.space_id}}",
            "domain": "{{job.parameters.domain}}",
            "catalog": "{{job.parameters.catalog}}",
            "schema": "{{job.parameters.schema}}",
            "apply_mode": "{{job.parameters.apply_mode}}",
            "levers": "{{job.parameters.levers}}",
            "max_iterations": "{{job.parameters.max_iterations}}",
            "experiment_name": "{{job.parameters.experiment_name}}",
            "deploy_target": "{{job.parameters.deploy_target}}",
            "warehouse_id": "{{job.parameters.warehouse_id}}",
        }
    return task


def build_job_settings(cfg: InstallConfig, notebooks_path: str, wheel_path: str) -> dict[str, Any]:
    tasks = [_task_payload(*task, notebooks_path) for task in TASKS]
    tasks.append(
        {
            "task_key": "deploy",
            "depends_on": [{"task_key": "finalize"}],
            "condition_task": {"op": "EQUAL_TO", "left": "deploy", "right": "disabled"},
        }
    )
    return {
        "name": cfg.gso_job_name,
        "description": (
            "Persistent DAG optimization runner managed by Genie Workbench "
            "(preflight -> baseline_eval -> enrichment -> lever_loop -> finalize -> deploy)."
        ),
        "max_concurrent_runs": 20,
        "queue": {"enabled": True},
        "tags": {
            "app": "genie-workbench",
            "managed-by": "notebook-installer",
            "pattern": "persistent-dag",
        },
        "parameters": [{"name": name, "default": default} for name, default in JOB_PARAMETERS.items()],
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


def find_existing_job(w, job_name: str) -> int | None:
    data = _api_do(w, "GET", "/api/2.1/jobs/list?limit=100&expand_tasks=false")
    for job in data.get("jobs") or []:
        settings = job.get("settings") or {}
        if settings.get("name") == job_name:
            return int(job["job_id"])
    return None


def upsert_job(w, settings: dict[str, Any]) -> int:
    existing_id = find_existing_job(w, settings["name"])
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
            f"/api/2.0/workspace/get-status?path={quote(workspace_dir, safe='')}",
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
