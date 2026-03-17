# 08 -- Permissions and Security

[Back to Index](00-index.md) | Previous: [07 API Reference](07-api-reference.md) | Next: [09 Deployment Guide](09-deployment-guide.md)

---

## Overview

The Genie Space Optimizer uses a dual-client authentication model and operates as an **advisor** -- it reads permissions and shows you exactly what's missing with copyable grant commands, but never executes `GRANT` or `REVOKE` on your behalf.

Three permission domains must be satisfied:

1. **OBO API scopes** -- what the app can do on behalf of the logged-in user
2. **Service Principal (SP) UC grants** -- what the app's SP can access in Unity Catalog
3. **Genie Space permissions** -- what the SP can do with each Genie Space

---

## Authentication Model

### On-Behalf-Of (OBO) User Client

When the app runs as a Databricks App, the Databricks Apps proxy injects an `X-Forwarded-Access-Token` header with a scoped OBO token. This token represents the logged-in user's identity and permissions.

The backend creates an OBO `WorkspaceClient` via `Dependencies.UserClient`. If the token is missing or lacks a required scope, the backend falls back to the SP client transparently.

### Service Principal (SP) Client

The app also has its own service principal identity via `Dependencies.Client`. This is always available and is used for:

- Job submission (`jobs.run_now()`)
- Delta state table reads/writes
- Fallback Genie API access when OBO token is unavailable
- UC metadata collection during optimization jobs

### Fallback Behavior

The `_genie_client(ws, sp_ws)` function implements the fallback:

1. Try OBO client for Genie API call
2. If `PermissionDenied` is raised, fall back to SP client
3. Log a warning that OBO fallback is active

When OBO is unavailable, all Genie Spaces accessible to the SP are shown (unfiltered by user permissions). Activity feeds are also unfiltered.

---

## Required OBO API Scopes

These scopes are configured in `databricks.yml` under `user_api_scopes`:

| Scope | Purpose |
|-------|---------|
| `dashboards.genie` | Read/write Genie Space configurations, run queries |
| `files.files` | File operations (workspace artifacts) |
| `catalog.catalogs:read` | List Unity Catalog catalogs |
| `catalog.schemas:read` | List UC schemas |
| `catalog.tables:read` | Read UC table/column metadata via REST |
| `sql` | Execute SQL statements via SQL Warehouse |
| `iam.access-control:read` | Check space permissions |
| `iam.current-user:read` | Identify the authenticated user |

---

## Service Principal UC Grants

The SP needs two categories of UC grants:

### Operational Schema Grants

The operational schema (`{catalog}.{schema}`, default `main.genie_optimization`) stores Delta state tables, MLflow experiments, and benchmark data. The SP needs full access:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `USE_CATALOG` | On the catalog | Access catalog |
| `USE_SCHEMA` | On the schema | Access schema |
| `SELECT` | On the schema | Read state tables |
| `MODIFY` | On the schema | Write state tables |
| `CREATE_TABLE` | On the schema | Create state tables on first run |
| `CREATE_FUNCTION` | On the schema | Create UDFs if needed |
| `CREATE_MODEL` | On the schema | Create MLflow models |
| `EXECUTE` | On the schema | Execute stored procedures |

These grants are applied automatically by `resources/grant_app_uc_permissions.py` during `databricks bundle deploy`.

**Grant script usage:**

```bash
python resources/grant_app_uc_permissions.py \
  --profile <profile> \
  --app-name genie-space-optimizer \
  --catalog <catalog> \
  --schema <schema>
```

The script:
1. Looks up the app's service principal via `databricks apps get`
2. Applies catalog-level `USE_CATALOG` grant
3. Applies schema-level grants (all 7 privileges)
4. Verifies all grants were applied
5. On first deploy (app not yet created): logs a warning and exits cleanly

### Data Schema Grants

The Genie Space's data tables live in separate schemas (e.g., `analytics.sales`, `finance.revenue`). The SP needs read access to these schemas:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `USE_CATALOG` | On the data catalog | Access catalog |
| `USE_SCHEMA` | On the data schema | Access schema |
| `SELECT` | On the data schema | Read table data during evaluation |

If `apply_mode` is `both` or `uc_artifact`, write access is also needed:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `MODIFY` | On the data schema | Write UC artifacts (table/column descriptions) |

The optimizer's **preflight stage validates write access** when `apply_mode` requires it, failing fast with a clear error message if `MODIFY` is missing.

---

## Genie Space Permissions

The SP needs `CAN_MANAGE` permission on each Genie Space it optimizes. This permission allows the SP to:

- Read the full Genie Space configuration (including serialized space data)
- Modify instructions, sample questions, table descriptions, join specs, and column configs
- Run queries through the Genie API

**How to grant:** Share the Genie Space with the app's service principal from the Genie Space UI:

1. Open the Genie Space in the Databricks workspace
2. Click the share/permissions button
3. Add the service principal (search by name)
4. Set permission to `CAN_MANAGE`

---

## The Advisor Model

The app operates as an **advisor** -- it never executes `GRANT` or `REVOKE` statements. Instead, the Settings page (`/settings`) provides:

### Schema Permissions View

For each schema referenced by Genie Spaces:

| Field | Description |
|-------|-------------|
| Read status | Whether the SP has SELECT on this schema |
| Write status | Whether the SP has MODIFY on this schema |
| Read grant command | Copyable SQL: `GRANT USE CATALOG ON CATALOG ...`, `GRANT USE SCHEMA ON SCHEMA ...`, `GRANT SELECT ON SCHEMA ...` |
| Write grant command | Copyable SQL: `GRANT MODIFY ON SCHEMA ...` |

### Space Permissions View

For each Genie Space:

| Field | Description |
|-------|-------------|
| SP has CAN_MANAGE | Whether the SP can manage this space |
| Grant instructions | Step-by-step sharing instructions if CAN_MANAGE is missing |

### Workflow

1. Navigate to Settings (gear icon)
2. Review the permission status for each schema and space
3. Copy the provided SQL commands into a query editor and execute them
4. Follow the sharing instructions for Genie Spaces
5. Refresh the Settings page to confirm grants were applied

---

## Permission Checks During Optimization

The optimizer performs several permission checks at different stages:

| Stage | Check | Failure Behavior |
|-------|-------|-----------------|
| **Trigger** | SP can access at least one table in the space | 500 error with clear message |
| **Preflight** | Core access: `w.tables.get()` succeeds for space tables | Fail with `permission denied` |
| **Preflight** | Write access (if `apply_mode` includes UC): MODIFY on schemas | Fail fast with clear error |
| **Baseline** | Genie query execution: SP can run queries | Fail with Genie API error |
| **Lever loop** | Genie config patch: SP has CAN_MANAGE | Fail with `PermissionDenied` |
| **Apply (UC)** | OBO user has MODIFY on data schemas | Fail with SQL execution error |

---

## Security Considerations

### No Credential Storage

The app does not store any credentials. OBO tokens are injected per-request by the Databricks Apps proxy. The SP credentials are managed by the Databricks Apps runtime.

### Minimal Privilege Principle

- The app only requests the OBO scopes it needs
- Data schema access is read-only unless `apply_mode` explicitly includes UC writes
- The SP's operational schema grants are scoped to the optimization schema only

### Audit Trail

All optimization actions are recorded in Delta tables:

- `genie_opt_runs` records who triggered each optimization (`triggered_by`)
- `genie_opt_patches` records every configuration change
- `genie_opt_stages` records the full stage timeline
- MLflow traces are tagged with the optimization run ID for complete traceability

---

## Quick Reference: What Needs What

| Action | OBO Required? | SP Required? |
|--------|--------------|-------------|
| View dashboard | No (SP fallback) | Yes (list spaces) |
| View space detail | No (SP fallback) | Yes (fetch config) |
| Start optimization | Preferred (user identity) | Yes (job submission) |
| Monitor run | No | Yes (read Delta state) |
| Apply optimization | No (SP applies) | Yes (update config) |
| Apply UC writes | Yes (OBO SQL execution) | No |
| View Settings | No (SP fallback) | Yes (probe grants) |

---

Next: [09 -- Deployment Guide](09-deployment-guide.md)
