# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Approval Check
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Gate cross-environment deployment behind human approval |
# MAGIC | **Triggered by** | MLflow Deployment Job (auto-triggered on new UC model version) |
# MAGIC | **Parameters** | `model_name`, `model_version`, `approval_tag_name` |
# MAGIC | **Reads from** | UC model version tags |
# MAGIC | **Expected behavior** | Fails on first run (no approval tag); passes after reviewer clicks Approve in UC UI |
# MAGIC
# MAGIC ## How Approval Works
# MAGIC
# MAGIC 1. This task **always fails** on its first run because the approval tag does not yet exist.
# MAGIC 2. A reviewer opens the UC model version page and reviews evaluation metrics.
# MAGIC 3. If satisfied, the reviewer clicks **Approve** in the deployment job sidebar.
# MAGIC 4. Databricks auto-repairs the job run; this task re-runs and passes because the tag now exists.
# MAGIC 5. The downstream `cross_env_deploy` task then executes.
# MAGIC
# MAGIC ## Special Case: No Deploy Target
# MAGIC
# MAGIC If the model version was not tagged with `genie.deploy_target_url` (meaning the
# MAGIC user did not specify a deployment target in the frontend), this task exits cleanly
# MAGIC with status `SKIPPED` so the deployment job does not block.

# COMMAND ----------

import json
from functools import partial
from typing import Any, cast

from mlflow import MlflowClient

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "DEPLOY APPROVAL"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Parameters
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `model_name` | Fully-qualified UC model name |
# MAGIC | `model_version` | Version number to check approval for |
# MAGIC | `approval_tag_name` | UC tag key used for approval (defaults to task name via `{{task.name}}`) |

# COMMAND ----------

dbutils.widgets.text("model_name", "", "UC Model Name")
dbutils.widgets.text("model_version", "", "Model Version")
dbutils.widgets.text("approval_tag_name", "", "Approval Tag Name")

model_name = dbutils.widgets.get("model_name").strip()
model_version = dbutils.widgets.get("model_version").strip()
tag_name = dbutils.widgets.get("approval_tag_name").strip()

_banner("Resolved Parameters")
_log(
    "Inputs",
    model_name=model_name,
    model_version=model_version,
    approval_tag_name=tag_name,
)

if not model_name or not model_version:
    raise ValueError("model_name and model_version are required parameters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Deploy Target
# MAGIC
# MAGIC Verify the model version has a deploy target tagged. If not, skip approval
# MAGIC entirely since there is nowhere to deploy to.

# COMMAND ----------

_banner("Fetching Model Version Tags")

client = MlflowClient(registry_uri="databricks-uc")
mv = client.get_model_version(model_name, model_version)
tags = mv.tags or {}

_log("Model version tags", tags=tags, checking_for=tag_name)

deploy_target = tags.get("genie.deploy_target_url", "")
if not deploy_target:
    _banner("No Deploy Target — Skipping Approval")
    _log("Skipping", reason="No deploy target URL tagged on model version")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "no_deploy_target"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Approval Tag
# MAGIC
# MAGIC Look for a UC tag whose key matches `approval_tag_name` (typically the task
# MAGIC name `Approval_Check`). The tag value must be `Approved` (case-insensitive).
# MAGIC
# MAGIC - **Tag missing:** Raise an exception (expected on first run; triggers the
# MAGIC   Approve button in the UC model version UI).
# MAGIC - **Tag present but not `Approved`:** Raise an exception.
# MAGIC - **Tag present and `Approved`:** Continue to deployment.

# COMMAND ----------

_banner("Checking Approval Status")

if tag_name not in tags:
    raise Exception(
        f"Model version {model_name} v{model_version} not approved for deployment. "
        f"Tag '{tag_name}' not found. Please approve in the UC Model Version UI."
    )

if tags[tag_name].lower() != "approved":
    raise Exception(
        f"Model version {model_name} v{model_version} not approved. "
        f"Tag '{tag_name}' = '{tags[tag_name]}' (expected 'Approved')."
    )

_banner("Approved")
_log(
    "Model version approved for deployment",
    model_name=model_name,
    version=model_version,
    deploy_target=deploy_target,
)

dbutils.notebook.exit(json.dumps({
    "status": "APPROVED",
    "model_name": model_name,
    "model_version": model_version,
    "deploy_target": deploy_target,
}))
