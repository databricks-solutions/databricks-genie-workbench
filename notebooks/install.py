# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Genie Workbench Notebook Installer
# MAGIC
# MAGIC **Required compute:** attach this notebook to **Databricks Serverless Environment v5**. The installer expects Python 3.11+ and workspace-native Databricks SDK auth.
# MAGIC
# MAGIC **Required workspace previews/settings:** Databricks Apps, **Databricks Apps On-Behalf-of-User authorization** (Public Preview), and **Managed MLflow Prompt Registry** (Beta) must be enabled. Workspace admins manage preview toggles from the Databricks **Previews** page.
# MAGIC
# MAGIC **Optional:** Lakebase Autoscaling is recommended for persistent app state. Set `lakebase_mode` to `skip` only when ephemeral in-memory state is acceptable.

# COMMAND ----------
# MAGIC %pip install databricks-sdk==0.117.0 pyyaml==6.0.3 "psycopg[binary]==3.3.3" hatchling==1.29.0 uv-dynamic-versioning==0.13.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from pathlib import Path
import sys

# Workspace Git folders (wsfs) reject __pycache__ writes; skip bytecode caching
# so importing scripts.deploy_lib does not log wsfs errors.
sys.dont_write_bytecode = True


def path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        # Databricks workspace-backed paths can raise PermissionError when
        # probing a missing sibling inside a Git folder.
        return False


def find_repo_root(start: Path) -> Path:
    try:
        current = start.resolve()
    except OSError:
        current = start.absolute()
    for candidate in [current, *current.parents]:
        if path_exists(candidate / "app.yaml") and path_exists(candidate / "pyproject.toml"):
            return candidate
    raise RuntimeError("Could not locate repo root containing app.yaml and pyproject.toml")


repo_root = find_repo_root(Path.cwd())
for path in [
    repo_root,
    repo_root / "packages" / "genie-space-optimizer" / "src",
]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

print(f"Repo root: {repo_root}")

# COMMAND ----------
dbutils.widgets.text("app_name", "genie-workbench")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.dropdown("lakebase_mode", "create", ["create", "existing", "skip"])
dbutils.widgets.text("lakebase_project_name", "")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Before You Run The Installer
# MAGIC
# MAGIC Confirm these workspace prerequisites manually:
# MAGIC
# MAGIC - [ ] Notebook is attached to **Databricks Serverless Environment v5**.
# MAGIC - [ ] **Databricks Apps** is enabled in this workspace.
# MAGIC - [ ] **Databricks Apps On-Behalf-of-User authorization** Public Preview is enabled.
# MAGIC - [ ] **Managed MLflow Prompt Registry** Beta is enabled.
# MAGIC - [ ] The selected SQL warehouse exists and you have `CAN_USE`.
# MAGIC - [ ] The selected Unity Catalog exists and you can create/use the target schema.
# MAGIC - [ ] If `lakebase_mode` is not `skip`, Lakebase Autoscaling is available.

# COMMAND ----------
from datetime import datetime
import importlib

from databricks.sdk import WorkspaceClient

import scripts.deploy_lib.app_yaml
import scripts.deploy_lib.apps
import scripts.deploy_lib.config
import scripts.deploy_lib.genie_spaces
import scripts.deploy_lib.gso_job
import scripts.deploy_lib.install
import scripts.deploy_lib.lakebase
import scripts.deploy_lib.uc
import scripts.deploy_lib.verify
import scripts.deploy_lib.workspace_source

for module in [
    scripts.deploy_lib.workspace_source,
    scripts.deploy_lib.app_yaml,
    scripts.deploy_lib.apps,
    scripts.deploy_lib.config,
    scripts.deploy_lib.genie_spaces,
    scripts.deploy_lib.lakebase,
    scripts.deploy_lib.uc,
    scripts.deploy_lib.verify,
    scripts.deploy_lib.gso_job,
    scripts.deploy_lib.install,
]:
    importlib.reload(module)

from scripts.deploy_lib.config import InstallConfig
from scripts.deploy_lib.install import run_install


def notebook_status(message: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(f"[genie-workbench install] {line}", flush=True)


app_name = dbutils.widgets.get("app_name").strip()
lakebase_mode = dbutils.widgets.get("lakebase_mode").strip()
explicit_lakebase = dbutils.widgets.get("lakebase_project_name").strip()

if lakebase_mode == "skip":
    lakebase_instance = None
elif explicit_lakebase:
    lakebase_instance = explicit_lakebase
else:
    lakebase_instance = f"{app_name}-lakebase"

cfg = InstallConfig(
    app_name=app_name,
    catalog=dbutils.widgets.get("catalog").strip(),
    warehouse_id=dbutils.widgets.get("warehouse_id").strip(),
    mlflow_experiment_id=None,
    lakebase_mode=lakebase_mode,
    lakebase_instance=lakebase_instance,
    repo_root=str(repo_root),
    grant_genie_spaces=True,
)

w = WorkspaceClient()
notebook_status("Starting Genie Workbench notebook install")
result = run_install(w, cfg, status_fn=notebook_status)
notebook_status("Notebook install finished")
result
