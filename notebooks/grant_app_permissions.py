# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Workbench: Grant App Permissions
# MAGIC
# MAGIC This notebook grants the Genie Workbench Databricks App's **service principal** access to
# MAGIC resources that require programmatic permission grants. Run through each cell in
# MAGIC order after deploying the app.
# MAGIC
# MAGIC ### Resources configured by this notebook
# MAGIC
# MAGIC | Resource | Permission | Required For |
# MAGIC |----------|-----------|--------------|
# MAGIC | Workspace Directory | **Can Manage** | Creating new Genie Spaces (Optimize mode) |
# MAGIC | Unity Catalog / Schema | **USE CATALOG**, **USE SCHEMA**, **SELECT** | GenieIQ benchmark execution |
# MAGIC
# MAGIC ### Resources to grant via UI
# MAGIC
# MAGIC | Resource | Permission | Where |
# MAGIC |----------|-----------|-------|
# MAGIC | LLM Serving Endpoint | **Can Query** | Serving > Endpoint > Permissions |
# MAGIC | SQL Warehouse | **Can Use** | SQL > Warehouse > Permissions |
# MAGIC | Genie Space(s) | **Can Edit** | Genie Space > Settings |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. The Genie Workbench app has been deployed via **Compute > Apps**.
# MAGIC 2. You have **admin or manage** permissions on the resources listed above.
# MAGIC 3. A Lakebase instance has been created (run `scripts/setup_lakebase.sh`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Configuration
# MAGIC
# MAGIC Fill in the variables below, then run each subsequent cell.

# COMMAND ----------

APP_NAME = ""
WORKSPACE_DIRECTORY = ""
CATALOG_NAME = ""
SCHEMA_NAME = ""

# COMMAND ----------

assert APP_NAME, "App Name is required."
print(f"App Name:             {APP_NAME}")
print(f"Workspace Directory:  {WORKSPACE_DIRECTORY or '(skipped)'}")
print(f"Catalog:              {CATALOG_NAME or '(skipped)'}")
print(f"Schema:               {SCHEMA_NAME or '(skipped)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Look Up the App Service Principal

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Look up the app's service principal
app_info = w.api_client.do(method="GET", path=f"/api/2.0/apps/{APP_NAME}")
sp_id = app_info["service_principal_id"]
assert (
    sp_id
), f"Could not find service principal for app '{APP_NAME}'. Ensure the app is deployed."

# Resolve the service principal name (used by permission and GRANT APIs)
sp = w.service_principals.get(sp_id)
SP_NAME = sp.display_name
SP_APP_ID = sp.application_id

print(f"Service Principal ID:   {sp_id}")
print(f"Service Principal Name: {SP_NAME}")
print(f"Application ID:         {SP_APP_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Grant Workspace Directory Permission
# MAGIC
# MAGIC Required for **Optimize mode** so the app can create new Genie Spaces in the
# MAGIC target directory. This grants **Can Manage** on the workspace folder.

# COMMAND ----------

if WORKSPACE_DIRECTORY:
    # Get the object ID of the workspace directory
    obj_info = w.api_client.do(
        method="GET",
        path="/api/2.0/workspace/get-status",
        body={"path": WORKSPACE_DIRECTORY},
    )
    object_id = obj_info["object_id"]

    w.api_client.do(
        method="PATCH",
        path=f"/api/2.0/permissions/directories/{object_id}",
        body={
            "access_control_list": [
                {
                    "service_principal_name": SP_APP_ID,
                    "permission_level": "CAN_MANAGE",
                }
            ]
        },
    )
    print(
        f"Granted CAN_MANAGE on directory '{WORKSPACE_DIRECTORY}' (ID: {object_id}) to {SP_NAME}"
    )
else:
    print("Skipped - no workspace directory specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Grant Unity Catalog Permissions
# MAGIC
# MAGIC Required for **GenieIQ benchmark execution** so the app can run SQL against the
# MAGIC tables your Genie Spaces reference. This grants **USE CATALOG**, **USE SCHEMA**,
# MAGIC and **SELECT** on the schema (inherited by all tables).

# COMMAND ----------

if CATALOG_NAME and SCHEMA_NAME:
    grants_executed = []

    # USE CATALOG
    stmt = f"GRANT USE CATALOG ON CATALOG `{CATALOG_NAME}` TO `{SP_APP_ID}`"
    spark.sql(stmt)
    grants_executed.append(stmt)

    # USE SCHEMA
    stmt = (
        f"GRANT USE SCHEMA ON SCHEMA `{CATALOG_NAME}`.`{SCHEMA_NAME}` TO `{SP_APP_ID}`"
    )
    spark.sql(stmt)
    grants_executed.append(stmt)

    # SELECT on all tables in schema (inherited by all current and future tables)
    stmt = f"GRANT SELECT ON SCHEMA `{CATALOG_NAME}`.`{SCHEMA_NAME}` TO `{SP_APP_ID}`"
    spark.sql(stmt)
    grants_executed.append(stmt)

    print("Executed grants:")
    for g in grants_executed:
        print(f"  {g}")
elif CATALOG_NAME or SCHEMA_NAME:
    print("Both catalog and schema are required for Unity Catalog grants. Skipped.")
else:
    print("Skipped - no catalog/schema specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Run the cell below to verify all permissions were applied.

# COMMAND ----------

print("=" * 60)
print("  Genie Workbench Permission Setup Complete")
print("=" * 60)
print()
print(f"  App:                {APP_NAME}")
print(f"  Service Principal:  {SP_NAME} ({sp_id})")
print()

checks = {
    "Workspace Directory": bool(WORKSPACE_DIRECTORY),
    "Unity Catalog Grants": bool(CATALOG_NAME and SCHEMA_NAME),
}

for resource, granted in checks.items():
    status = "GRANTED" if granted else "SKIPPED"
    print(f"  [{status:^8}]  {resource}")

skipped = [r for r, g in checks.items() if not g]
if skipped:
    print()
    print("  Skipped resources can be configured later by re-running")
    print("  this notebook with the appropriate values.")

print()
print("  Don't forget to grant these via the UI:")
print("    - LLM Serving Endpoint: Can Query")
print("    - SQL Warehouse: Can Use")
print("    - Genie Space(s): Can Edit")
print()
