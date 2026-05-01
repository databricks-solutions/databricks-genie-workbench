# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.102.0 pyyaml==6.0.3 "psycopg[binary]==3.3.3" build==1.4.0 hatchling==1.29.0 uv-dynamic-versioning==0.13.0

# COMMAND ----------
from pathlib import Path
import sys


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "app.yaml").exists() and (candidate / "pyproject.toml").exists():
            return candidate
    raise RuntimeError("Could not locate repo root containing app.yaml and pyproject.toml")


repo_root = find_repo_root(Path.cwd())
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

print(f"Repo root: {repo_root}")

# COMMAND ----------
dbutils.widgets.text("app_name", "genie-workbench")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("mlflow_experiment_id", "")
dbutils.widgets.dropdown("lakebase_mode", "create", ["create", "existing", "skip"])
dbutils.widgets.text("lakebase_instance", "")
dbutils.widgets.dropdown("grant_genie_spaces", "false", ["false", "true"])

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from scripts.deploy_lib.config import InstallConfig
from scripts.deploy_lib.install import run_install


app_name = dbutils.widgets.get("app_name").strip()
lakebase_mode = dbutils.widgets.get("lakebase_mode").strip()
explicit_lakebase = dbutils.widgets.get("lakebase_instance").strip()

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
    llm_model=dbutils.widgets.get("llm_model").strip(),
    mlflow_experiment_id=dbutils.widgets.get("mlflow_experiment_id").strip() or None,
    lakebase_mode=lakebase_mode,
    lakebase_instance=lakebase_instance,
    repo_root=str(repo_root),
    grant_genie_spaces=dbutils.widgets.get("grant_genie_spaces").strip().lower() == "true",
)

w = WorkspaceClient()
result = run_install(w, cfg)
result
