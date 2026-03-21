"""Ensure the GSO optimization job exists with correct artifacts.

Finds an existing job by name/tag, or creates one from scratch:
  1. Builds the genie-space-optimizer wheel
  2. Uploads wheel to UC Volume
  3. Uploads 6 job notebooks to workspace
  4. Creates the Databricks job

Prints the job ID to stdout. Progress messages go to stderr.

Usage:
    python scripts/ensure_gso_job.py \
        --profile DEFAULT \
        --catalog main \
        --schema genie_space_optimizer \
        --app-name genie-workbench \
        --project-dir /path/to/repo
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

_NOTEBOOKS = [
    "run_preflight",
    "run_baseline",
    "run_enrichment",
    "run_lever_loop",
    "run_finalize",
    "run_deploy",
]
_WS_NOTEBOOK_BASE = "/Workspace/Shared/genie-space-optimizer"
_JOB_NAME = "genie-space-optimizer-job"
_VOLUME_NAME = "app_artifacts"


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _run(cmd: list[str], **kwargs) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{stderr}")
    return (result.stdout or "").strip()


def _run_json(cmd: list[str], **kwargs) -> dict:
    out = _run(cmd, **kwargs)
    if not out:
        return {}
    return json.loads(out)


# ---------------------------------------------------------------------------
# Step 1: Find existing job
# ---------------------------------------------------------------------------

def _find_existing_job(profile: str) -> str | None:
    """Find the GSO optimization job by name and tag."""
    try:
        data = _run_json(["databricks", "jobs", "list", "--profile", profile, "-o", "json"])
    except Exception:
        return None
    jobs = data if isinstance(data, list) else data.get("jobs", [])
    for j in jobs:
        name = (j.get("settings") or {}).get("name", "")
        tags = (j.get("settings") or {}).get("tags", {})
        if _JOB_NAME in name and tags.get("pattern") == "persistent-dag":
            return str(j.get("job_id", ""))
    return None


# ---------------------------------------------------------------------------
# Step 2: Build wheel
# ---------------------------------------------------------------------------

def _build_wheel(project_dir: str) -> str:
    """Build the genie-space-optimizer wheel and return its local path."""
    gso_dir = os.path.join(project_dir, "packages", "genie-space-optimizer")
    if not os.path.isdir(gso_dir):
        raise RuntimeError(f"GSO package not found at {gso_dir}")

    dist_dir = os.path.join(project_dir, ".build")
    os.makedirs(dist_dir, exist_ok=True)

    # Clean old wheels
    for f in os.listdir(dist_dir):
        if f.startswith("genie_space_optimizer") and f.endswith(".whl"):
            os.remove(os.path.join(dist_dir, f))

    # Try uv first (fast), then fall back to python -m build
    built = False
    for cmd in [
        ["uv", "build", "--wheel", "-o", dist_dir],
        ["python3", "-m", "build", "--wheel", "-o", dist_dir],
    ]:
        try:
            _run(cmd, cwd=gso_dir)
            built = True
            break
        except Exception:
            continue

    if not built:
        raise RuntimeError(
            "Could not build the genie-space-optimizer wheel. "
            "Ensure `uv` or `python3 -m build` is available."
        )

    wheels = sorted(
        f for f in os.listdir(dist_dir)
        if f.startswith("genie_space_optimizer") and f.endswith(".whl")
    )
    if not wheels:
        raise RuntimeError(f"No wheel found in {dist_dir} after build")
    return os.path.join(dist_dir, wheels[-1])


# ---------------------------------------------------------------------------
# Step 3: Upload wheel to UC Volume
# ---------------------------------------------------------------------------

def _upload_wheel(profile: str, wheel_path: str, catalog: str, schema: str) -> str:
    """Upload wheel to UC Volume and return the Volume path."""
    wheel_name = os.path.basename(wheel_path)
    dist_dir = f"/Volumes/{catalog}/{schema}/{_VOLUME_NAME}/dist"
    vol_path = f"{dist_dir}/{wheel_name}"

    # Create the dist/ directory first — fs cp can't create parent dirs.
    # Both mkdir and cp require the dbfs: prefix for Volume paths.
    _run(["databricks", "fs", "mkdir", f"dbfs:{dist_dir}", "--profile", profile])
    _run([
        "databricks", "fs", "cp", wheel_path, f"dbfs:{vol_path}",
        "--profile", profile, "--overwrite",
    ])
    return vol_path


# ---------------------------------------------------------------------------
# Step 4: Upload notebooks to workspace
# ---------------------------------------------------------------------------

def _upload_notebooks(profile: str, project_dir: str) -> None:
    """Upload the 6 job notebooks to the shared workspace location."""
    gso_jobs_dir = os.path.join(
        project_dir, "packages", "genie-space-optimizer",
        "src", "genie_space_optimizer", "jobs",
    )

    _run(["databricks", "workspace", "mkdirs", _WS_NOTEBOOK_BASE, "--profile", profile])

    for nb_name in _NOTEBOOKS:
        src = os.path.join(gso_jobs_dir, f"{nb_name}.py")
        if not os.path.isfile(src):
            _log(f"  ⚠ Notebook not found: {src}")
            continue
        dst = f"{_WS_NOTEBOOK_BASE}/{nb_name}"
        _run([
            "databricks", "workspace", "import", dst,
            "--profile", profile,
            "--file", src,
            "--format", "SOURCE",
            "--language", "PYTHON",
            "--overwrite",
        ])


# ---------------------------------------------------------------------------
# Step 5: Create the job
# ---------------------------------------------------------------------------

def _create_job(
    profile: str,
    catalog: str,
    schema: str,
    app_name: str,
    vol_wheel_path: str,
) -> str:
    """Create the optimization job and return its ID."""
    job_spec = {
        "name": _JOB_NAME,
        "description": (
            "Persistent DAG optimization runner managed by Genie Workbench "
            "(preflight -> baseline_eval -> enrichment -> lever_loop -> finalize -> deploy). "
            "SP executes with granted privileges on user schemas."
        ),
        "max_concurrent_runs": 20,
        "queue": {"enabled": True},
        "tags": {
            "app": app_name,
            "managed-by": "databricks-bundle",
            "pattern": "persistent-dag",
        },
        "parameters": [
            {"name": "run_id", "default": ""},
            {"name": "space_id", "default": ""},
            {"name": "domain", "default": "default"},
            {"name": "catalog", "default": ""},
            {"name": "schema", "default": ""},
            {"name": "apply_mode", "default": "genie_config"},
            {"name": "levers", "default": "[1,2,3,4,5]"},
            {"name": "max_iterations", "default": "5"},
            {"name": "triggered_by", "default": ""},
            {"name": "experiment_name", "default": ""},
            {"name": "deploy_target", "default": ""},
            {"name": "warehouse_id", "default": ""},
        ],
        "tasks": [
            {
                "task_key": "preflight",
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_preflight",
                    "source": "WORKSPACE",
                    "base_parameters": {
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
                    },
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
            {
                "task_key": "baseline_eval",
                "depends_on": [{"task_key": "preflight"}],
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_baseline",
                    "source": "WORKSPACE",
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
            {
                "task_key": "enrichment",
                "depends_on": [{"task_key": "baseline_eval"}],
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_enrichment",
                    "source": "WORKSPACE",
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
            {
                "task_key": "lever_loop",
                "depends_on": [{"task_key": "enrichment"}],
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_lever_loop",
                    "source": "WORKSPACE",
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
            {
                "task_key": "finalize",
                "depends_on": [{"task_key": "lever_loop"}],
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_finalize",
                    "source": "WORKSPACE",
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
            {
                "task_key": "deploy",
                "depends_on": [{"task_key": "finalize"}],
                "notebook_task": {
                    "notebook_path": f"{_WS_NOTEBOOK_BASE}/run_deploy",
                    "source": "WORKSPACE",
                },
                "environment_key": "default",
                "timeout_seconds": 7200,
                "max_retries": 0,
            },
        ],
        "environments": [
            {
                "environment_key": "default",
                "spec": {
                    "client": "4",
                    "dependencies": [
                        vol_wheel_path,
                        "mlflow[databricks]>=3.10.1",
                        "databricks-sdk>=0.40.0",
                    ],
                },
            },
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(job_spec, f)
        tmp = f.name

    try:
        result = _run_json([
            "databricks", "jobs", "create",
            "--profile", profile,
            "--json", f"@{tmp}",
        ])
    finally:
        os.unlink(tmp)

    job_id = result.get("job_id")
    if not job_id:
        raise RuntimeError(f"Job creation returned no job_id: {result}")
    return str(job_id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ensure the GSO optimization job exists.",
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--project-dir", required=True)
    args = parser.parse_args()

    # 1. Check for existing job
    _log("  Checking for existing optimization job...")
    existing_id = _find_existing_job(args.profile)
    if existing_id:
        _log(f"  ✓ Found existing optimization job: {existing_id}")
        print(existing_id)
        return 0

    # 2. Build wheel
    _log("  Building GSO wheel...")
    wheel_path = _build_wheel(args.project_dir)
    _log(f"  ✓ Wheel built: {os.path.basename(wheel_path)}")

    # 3. Upload wheel to Volume
    _log("  Uploading wheel to UC Volume...")
    vol_wheel_path = _upload_wheel(args.profile, wheel_path, args.catalog, args.schema)
    _log(f"  ✓ Wheel uploaded: {vol_wheel_path}")

    # 4. Upload notebooks
    _log("  Uploading job notebooks...")
    _upload_notebooks(args.profile, args.project_dir)
    _log(f"  ✓ Notebooks uploaded to {_WS_NOTEBOOK_BASE}")

    # 5. Create job
    _log("  Creating optimization job...")
    job_id = _create_job(
        args.profile, args.catalog, args.schema,
        args.app_name, vol_wheel_path,
    )
    _log(f"  ✓ Created optimization job: {job_id}")

    # Print job ID to stdout for caller to capture
    print(job_id)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        _log(f"  ✗ GSO job setup failed: {exc}")
        sys.exit(1)
