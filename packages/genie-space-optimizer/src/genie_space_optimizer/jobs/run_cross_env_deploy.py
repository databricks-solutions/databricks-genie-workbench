# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Environment Deployment
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Deploy an optimized Genie Space config to a target workspace |
# MAGIC | **Triggered by** | Deployment Job linked to a UC Registered Model |
# MAGIC | **Parameters** | `model_name`, `model_version`, `target_workspace_url`, `target_space_id` |
# MAGIC | **Reads from** | UC Registered Model artifacts (space_config.json) |
# MAGIC | **Publishes to** | Target workspace Genie Space via PATCH API |
# MAGIC
# MAGIC ## Flow
# MAGIC
# MAGIC 1. Read job parameters from widgets
# MAGIC 2. Load the model version's `space_config.json` artifact from MLflow
# MAGIC 3. Connect to the target workspace
# MAGIC 4. Apply the config via the Genie PATCH API
# MAGIC 5. Tag the model version with deployment status

# COMMAND ----------

import json
import traceback
from functools import partial
from typing import Any, cast

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow import MlflowClient

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "CROSS-ENV DEPLOY"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Read Job Parameters
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `model_name` | Fully-qualified UC model name (e.g. `main.genie_optimization.genie_space_abc123`) |
# MAGIC | `model_version` | Version number of the registered model to deploy |
# MAGIC | `target_workspace_url` | Databricks workspace URL to deploy to |
# MAGIC | `target_space_id` | Genie Space ID in the target workspace |

# COMMAND ----------

dbutils.widgets.text("model_name", "", "UC Model Name")
dbutils.widgets.text("model_version", "", "Model Version")
dbutils.widgets.text("target_workspace_url", "", "Target Workspace URL")
dbutils.widgets.text("target_space_id", "", "Target Space ID")

model_name = dbutils.widgets.get("model_name").strip()
model_version = dbutils.widgets.get("model_version").strip()
target_workspace_url = dbutils.widgets.get("target_workspace_url").strip()
target_space_id = dbutils.widgets.get("target_space_id").strip()

_banner("Resolved Parameters")
_log(
    "Inputs",
    model_name=model_name,
    model_version=model_version,
    target_workspace_url=target_workspace_url,
    target_space_id=target_space_id,
)

if not model_name or not model_version:
    raise ValueError("model_name and model_version are required parameters")

if not target_workspace_url:
    _log("target_workspace_url empty, checking model version tags for fallback")
    from mlflow import MlflowClient as _TagClient
    _tag_client = _TagClient(registry_uri="databricks-uc")
    _tag_mv = _tag_client.get_model_version(model_name, model_version)
    _mv_tags = _tag_mv.tags or {}
    target_workspace_url = _mv_tags.get("genie.deploy_target_url", "")
    if target_workspace_url:
        _log("Resolved target_workspace_url from model version tag", url=target_workspace_url)

if not target_workspace_url:
    raise ValueError("target_workspace_url is required (not in widget or model version tags)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Model Artifacts
# MAGIC
# MAGIC Download `space_config.json` from the registered model version's
# MAGIC MLflow artifacts. The config was logged during the optimization run.

# COMMAND ----------

_banner("Loading Model Artifacts")

from mlflow import set_registry_uri
set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

mv = client.get_model_version(model_name, model_version)
source_run_id = mv.run_id

_log("Model version info", run_id=source_run_id, status=mv.status)

space_config: dict | None = None
if source_run_id:
    from mlflow.artifacts import download_artifacts
    artifacts_dir = download_artifacts(
        run_id=source_run_id,
        artifact_path="model_snapshots",
    )
    import os
    for root, _dirs, files in os.walk(artifacts_dir):
        for f in files:
            if f == "space_config.json":
                with open(os.path.join(root, f)) as fh:
                    space_config = json.load(fh)
                break
        if space_config:
            break

if not space_config:
    params = mv.tags or {}
    config_json = params.get("model_space_config")
    if config_json:
        space_config = json.loads(config_json)

if not space_config:
    raise RuntimeError(
        f"Could not load space_config from model {model_name} v{model_version}. "
        "No space_config.json artifact and no model_space_config tag found."
    )

_log("Config loaded", keys=list(space_config.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to Target Workspace
# MAGIC
# MAGIC Create a `WorkspaceClient` pointed at the target workspace URL.
# MAGIC Authentication uses the current service principal's credentials
# MAGIC (must have permissions in both source and target workspaces).

# COMMAND ----------

_banner("Connecting to Target Workspace")

target_url = target_workspace_url.rstrip("/")
target_ws = WorkspaceClient(host=target_url)

current_user = target_ws.current_user.me()
_log(
    "Connected",
    target_url=target_url,
    user=current_user.user_name if current_user else "unknown",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Config to Target Genie Space
# MAGIC
# MAGIC Use the Genie PATCH API to apply the optimized configuration.
# MAGIC If `target_space_id` is empty, try to extract it from the model tags.

# COMMAND ----------

_banner("Applying Config to Target Space")

deploy_space_id = target_space_id
if not deploy_space_id:
    deploy_space_id = (mv.tags or {}).get("space_id", "")

if not deploy_space_id:
    raise ValueError(
        "target_space_id is empty and could not be inferred from model tags"
    )

_log("Deploying", space_id=deploy_space_id, target_url=target_url)

try:
    from genie_space_optimizer.common.genie_client import patch_space_config

    result = patch_space_config(target_ws, deploy_space_id, space_config)
    _log("PATCH applied", result_keys=list(result.keys()) if isinstance(result, dict) else str(result))
except Exception as exc:
    _banner("Deploy FAILED — PATCH error")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    client.set_model_version_tag(
        model_name, model_version, "deploy_status", "FAILED"
    )
    client.set_model_version_tag(
        model_name, model_version, "deploy_error", str(exc)[:500]
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Tag Model Version with Deployment Status
# MAGIC
# MAGIC Record the deployment outcome on the model version for traceability.

# COMMAND ----------

_banner("Tagging Model Version")

client.set_model_version_tag(model_name, model_version, "deploy_status", "DEPLOYED")
client.set_model_version_tag(model_name, model_version, "deploy_target_url", target_url)
client.set_model_version_tag(model_name, model_version, "deploy_target_space_id", deploy_space_id)

deploy_result = {
    "status": "DEPLOYED",
    "model_name": model_name,
    "model_version": model_version,
    "target_workspace_url": target_url,
    "target_space_id": deploy_space_id,
}

_banner("Cross-Environment Deploy Completed")
_log("Result", **deploy_result)

dbutils.notebook.exit(json.dumps(deploy_result, default=str))
