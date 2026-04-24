# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Workbench — Non-CLI Setup
# MAGIC
# MAGIC This notebook provisions Genie Workbench from inside Databricks — no
# MAGIC local CLI, Node.js, or `uv` required. After this notebook completes,
# MAGIC finish the install via the Apps UI using the workspace folder where
# MAGIC this notebook lives.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Clone the `databricks-genie-workbench` repo into a Databricks Git folder
# MAGIC    (Workspace → click "+" → Git folder → paste the repo URL).
# MAGIC 2. Attach this notebook to **serverless compute**.
# MAGIC 3. You need `CREATE SCHEMA` permission on the Unity Catalog you pick,
# MAGIC    and permission to create Databricks Apps.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Creates/updates the Databricks App (`apps create` via SDK)
# MAGIC - Runs `setup_workbench.provision_workbench()` — UC schema/tables/grants,
# MAGIC   Lakebase project/role/grants, Apps PATCH (scopes + resources),
# MAGIC   `app.yaml` placeholder substitution in the workspace folder, and
# MAGIC   Genie Space SP grants.
# MAGIC - Builds the GSO wheel and creates the 6-stage optimization job via SDK.
# MAGIC - Grants the app SP read access on the repo workspace folder.
# MAGIC
# MAGIC **Finish the install:**
# MAGIC After this notebook completes, open the Databricks Apps UI, click the
# MAGIC app name, go to **Deploy** → **Deploy from a workspace folder**, and
# MAGIC select the repo's workspace folder.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Install build dependencies
# MAGIC
# MAGIC Installs `build` (for the GSO wheel), `psycopg` (for Lakebase grant repair), and ensures `databricks-sdk` supports the Lakebase and Apps APIs used by setup.

# COMMAND ----------
# MAGIC %pip install --quiet --upgrade "build>=1.0.0" "databricks-sdk==0.102.0" "protobuf>=5.26.1,<6" "psycopg[binary]==3.3.3"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Widgets
# MAGIC
# MAGIC Fill these in, then run the rest of the notebook.

# COMMAND ----------
dbutils.widgets.text("app_name", "genie-workbench", "App name")
dbutils.widgets.text("catalog", "", "Unity Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("llm_model", "databricks-claude-sonnet-4-6", "LLM endpoint")
dbutils.widgets.text("lakebase_project", "", "Lakebase Autoscaling project (optional)")
dbutils.widgets.text("mlflow_experiment_id", "", "MLflow experiment ID (optional)")
dbutils.widgets.text("workspace_folder", "", "Workspace folder (leave blank to auto-detect)")
dbutils.widgets.dropdown(
    "grant_genie_spaces", "Y", ["Y", "N"],
    "Grant SP CAN_EDIT on editable Genie Spaces?",
)

# COMMAND ----------
# Read widgets
app_name = dbutils.widgets.get("app_name").strip()
catalog = dbutils.widgets.get("catalog").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
llm_model = dbutils.widgets.get("llm_model").strip() or "databricks-claude-sonnet-4-6"
lakebase_project = dbutils.widgets.get("lakebase_project").strip()
mlflow_experiment_id = dbutils.widgets.get("mlflow_experiment_id").strip()
workspace_folder = dbutils.widgets.get("workspace_folder").strip()
grant_genie_spaces = dbutils.widgets.get("grant_genie_spaces") == "Y"

missing = [k for k, v in {"app_name": app_name, "catalog": catalog, "warehouse_id": warehouse_id}.items() if not v]
if missing:
    raise ValueError(f"Required widgets missing: {missing}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Auto-detect workspace folder
# MAGIC
# MAGIC The workspace folder is the repo root (where `app.yaml` lives). This
# MAGIC notebook is at `<workspace_folder>/scripts/notebooks/setup_workbench`,
# MAGIC so the repo root is two directories up.

# COMMAND ----------
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

if not workspace_folder:
    parts = notebook_path.strip("/").split("/")
    # Look for .../scripts/notebooks/setup_workbench — repo root is everything
    # before "scripts"
    if "scripts" in parts:
        idx = parts.index("scripts")
        workspace_folder = "/Workspace/" + "/".join(parts[:idx])
    else:
        raise RuntimeError(
            f"Could not auto-detect workspace folder from notebook path {notebook_path!r}. "
            f"Set the 'workspace_folder' widget to the repo's workspace path "
            f"(e.g. /Workspace/Users/you@company.com/databricks-genie-workbench)."
        )

print(f"Notebook path:    {notebook_path}")
print(f"Workspace folder: {workspace_folder}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Import `setup_workbench` from the workspace folder

# COMMAND ----------
import sys

for path in (
    f"{workspace_folder}/scripts",
    f"{workspace_folder}/packages/genie-space-optimizer/src",
):
    if path not in sys.path:
        sys.path.insert(0, path)

from setup_workbench import (  # noqa: E402
    provision_workbench, _gso_job_dag, GSO_JOB_NAME,
)
from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.errors import NotFound  # noqa: E402

w = WorkspaceClient()
me = w.current_user.me()
print(f"Authenticated as: {me.user_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ensure the app exists
# MAGIC
# MAGIC Creates the app (and its service principal) if it doesn't already exist.
# MAGIC Waits up to 2 minutes for the SP to be provisioned.

# COMMAND ----------
import time

def _wait_for_sp(app_name: str, timeout_s: int = 120) -> str:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            a = w.apps.get(name=app_name)
            sp = (
                getattr(a, "service_principal_client_id", None)
                or getattr(a, "service_principal_name", None)
            )
            if sp:
                return sp
        except NotFound:
            pass
        time.sleep(5)
    raise TimeoutError(
        f"App '{app_name}' did not get a service principal within {timeout_s}s."
    )

try:
    app = w.apps.get(name=app_name)
    print(f"App '{app_name}' exists.")
except NotFound:
    print(f"Creating app '{app_name}'...")
    w.api_client.do(
        "POST", "/api/2.0/apps",
        body={
            "name": app_name,
            "description": "Genie Workbench - Create, score, and optimize Genie Spaces",
        },
    )
    print("Waiting for app service principal to be provisioned...")

sp_client_id = _wait_for_sp(app_name)
print(f"App SP: {sp_client_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Grant SP read access on the workspace folder
# MAGIC
# MAGIC The app needs `CAN_READ` on the repo workspace folder so the Apps
# MAGIC platform can deploy from it, and the GSO job's SP can execute its
# MAGIC notebooks from there. Without this grant, `apps deploy` and job runs
# MAGIC fail with permission errors.

# COMMAND ----------
try:
    status = w.workspace.get_status(path=workspace_folder)
    obj_id = getattr(status, "object_id", None)
    if not obj_id:
        raise RuntimeError(f"Could not resolve object_id for {workspace_folder}")
    w.api_client.do(
        "PATCH", f"/api/2.0/permissions/directories/{obj_id}",
        body={
            "access_control_list": [
                {"service_principal_name": sp_client_id, "permission_level": "CAN_MANAGE"}
            ]
        },
    )
    print(f"Granted CAN_MANAGE to SP on {workspace_folder}")
except Exception as e:
    print(f"WARN: Could not grant SP on workspace folder: {e}")
    print("  App deploy and job execution may fail. Grant manually via the Workspace UI.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Build the GSO optimization wheel
# MAGIC
# MAGIC The GSO package is source-controlled but deployed as a wheel for faster
# MAGIC job startup. This cell builds it in-place under the workspace folder.

# COMMAND ----------
import os
import subprocess

pkg_dir = f"{workspace_folder}/packages/genie-space-optimizer"
build_out = f"{pkg_dir}/.build"
os.makedirs(build_out, exist_ok=True)

print(f"Building wheel in {pkg_dir}...")
r = subprocess.run(
    [sys.executable, "-m", "build", "--wheel", "--outdir", build_out],
    cwd=pkg_dir,
    capture_output=True,
    text=True,
)
if r.returncode != 0:
    print("STDOUT:", r.stdout)
    print("STDERR:", r.stderr)
    raise RuntimeError(f"Wheel build failed (exit {r.returncode})")

wheels = sorted(f for f in os.listdir(build_out) if f.endswith(".whl"))
if not wheels:
    raise RuntimeError(f"No wheel produced in {build_out}")
wheel_path = f"{build_out}/{wheels[-1]}"
print(f"Built: {wheel_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Create or update the GSO optimization job
# MAGIC
# MAGIC Uses the 6-task DAG defined in `scripts/setup_workbench.py` (single
# MAGIC source of truth for the non-CLI path). If a job with the target name
# MAGIC already exists, it is reset to match this spec.

# COMMAND ----------
job_spec = _gso_job_dag(workspace_folder=workspace_folder, wheel_path=wheel_path)

# Find existing job by name
existing_job_id = ""
try:
    for j in w.jobs.list(name=GSO_JOB_NAME):
        if j.settings and j.settings.name == GSO_JOB_NAME:
            existing_job_id = str(j.job_id)
            break
except Exception:
    pass

if existing_job_id:
    print(f"Resetting existing job {GSO_JOB_NAME} (id={existing_job_id})...")
    w.api_client.do(
        "POST", "/api/2.1/jobs/reset",
        body={"job_id": int(existing_job_id), "new_settings": job_spec},
    )
    gso_job_id = existing_job_id
else:
    print(f"Creating new job {GSO_JOB_NAME}...")
    created = w.api_client.do("POST", "/api/2.1/jobs/create", body=job_spec)
    gso_job_id = str(created["job_id"])

print(f"GSO job id: {gso_job_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Provision everything else
# MAGIC
# MAGIC UC schema/tables/grants, Lakebase project/role/grants, Apps PATCH,
# MAGIC `app.yaml` placeholder substitution, GSO job permissions, Genie
# MAGIC Space SP grants — all in one call to `provision_workbench()`.

# COMMAND ----------
result = provision_workbench(
    app_name=app_name,
    catalog=catalog,
    warehouse_id=warehouse_id,
    llm_model=llm_model,
    lakebase_project=lakebase_project,
    mlflow_experiment_id=mlflow_experiment_id,
    workspace_folder=workspace_folder,
    gso_job_id=gso_job_id,
    grant_genie_spaces=grant_genie_spaces,
    deployer_email=me.user_name,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Next steps — deploy from the Apps UI

# COMMAND ----------
import json as _json

print("=" * 70)
print("  Provisioning complete — finish the install in the Apps UI:")
print("=" * 70)
print()
print(f"  1. Open:      Databricks → Compute → Apps → '{app_name}'")
print(f"  2. Click:     Deploy → Deploy from a workspace folder")
print(f"  3. Folder:    {workspace_folder}")
print(f"  4. Click:     Deploy")
print()
print("  The Apps platform will run `npm ci && npm run build` (frontend)")
print("  and `uv sync` (backend) on first deploy — this takes ~3-5 minutes.")
print()
print("  After the app is RUNNING, open the URL printed in the Apps UI to")
print("  verify IQ Scan, Create Agent, and Auto-Optimize all work.")
print()
print("-" * 70)
print("  Resolved IDs (for troubleshooting):")
print("-" * 70)
print(_json.dumps(result.to_dict(), indent=2))
