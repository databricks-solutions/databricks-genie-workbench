# 09 -- Deployment Guide

[Back to Index](00-index.md) | Previous: [08 Permissions and Security](08-permissions-and-security.md) | Next: [10 Operations Guide](10-operations-guide.md)

---

## Overview

The Genie Space Optimizer is deployed as a Databricks App using Databricks Asset Bundles (DABs). The deployment pipeline builds a Python wheel, syncs files to the workspace, grants UC permissions, and restarts the app.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Backend runtime |
| [uv](https://docs.astral.sh/uv/) | latest | Python package manager |
| [bun](https://bun.sh/) | latest | JavaScript runtime and package manager |
| [apx](https://github.com/databricks-solutions/apx) | latest | Full-stack app framework |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | latest | Deployment and workspace management |

Authenticate the Databricks CLI:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks auth env  # verify
```

---

## Configuration

### `databricks.yml`

The bundle definition configures all resources. Key variables to set:

```yaml
variables:
  catalog:
    default: "your_catalog_name"        # UC catalog for state tables
  gold_schema:
    default: "genie_optimization"        # Schema for state tables (created if needed)
  warehouse_id:
    default: "your_warehouse_id"         # SQL Warehouse ID
```

**Finding your Warehouse ID:** Navigate to SQL Warehouses in your workspace. The ID is in the URL: `https://<workspace>/sql/warehouses/<warehouse_id>`.

### `app.yml`

The app entry point configuration:

```yaml
command: ["uvicorn", "genie_space_optimizer.backend.app:app", "--workers", "2"]
env:
  - name: GENIE_SPACE_OPTIMIZER_CATALOG
    value: "main"
  - name: GENIE_SPACE_OPTIMIZER_SCHEMA
    value: "genie_optimization"
  - name: GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID
    value: "your_warehouse_id"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GENIE_SPACE_OPTIMIZER_CATALOG` | UC catalog for Delta state tables | `main` |
| `GENIE_SPACE_OPTIMIZER_SCHEMA` | Schema for Delta state tables | `genie_optimization` |
| `GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID` | SQL Warehouse ID | (workspace-specific) |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT` | Seconds to wait after config patch | `30` |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING` | Extra wait for entity matching | `90` |
| `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` | Verbose evaluation logging | `true` |
| `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` | Max retry attempts per evaluation | `4` |
| `GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS` | Heartbeat interval during finalize | `30` |
| `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` | Soft timeout for finalize | `6600` |

---

## Deployment Pipeline

### Recommended: `make deploy`

The `Makefile` orchestrates the full deployment:

```bash
make deploy PROFILE=<your-profile>
```

This runs four steps:

1. **`clean-wheels`** -- Removes stale `.whl` files from the workspace to prevent cache issues
2. **`_bundle_deploy`** -- Runs `apx build` (creates production wheel), removes `.build/.gitignore`, syncs files to workspace via `databricks bundle deploy`
3. **`_app_deploy`** -- Creates a new deployment snapshot and restarts the app via `databricks apps deploy`
4. **`verify`** -- Confirms the new wheel is present on the workspace

Both `databricks bundle deploy` and `databricks apps deploy` are required:
- Bundle deploy syncs files but does not restart the app
- Apps deploy creates an immutable snapshot from synced files and restarts

### What `databricks bundle deploy` Does

1. Runs the `artifacts.default.build` command:
   - `apx build` -- builds the frontend and Python wheel
   - `rm -f .build/.gitignore` -- removes gitignore that would block sync
   - `python resources/grant_app_uc_permissions.py` -- grants UC permissions to app SP
2. Syncs the `.build/` directory to the workspace
3. Creates/updates the Databricks App resource definition
4. Creates/updates the SQL Warehouse, PostgreSQL database, and Job definitions

### Resources Provisioned

| Resource | Description |
|----------|-------------|
| **Databricks App** (`genie-space-optimizer`) | Full-stack web application with OBO auth |
| **PostgreSQL Database** (Lakebase) | CU_1 capacity instance |
| **SQL Warehouse** | Statement execution and UC metadata queries |
| **Optimization Job** (`genie-space-optimizer-runner`) | Persistent job triggered on-demand |

---

## Deployment Verification

### Verify the app is running

```bash
databricks apps get genie-space-optimizer -p <profile> -o json | python3 -c "import sys,json; print(json.load(sys.stdin).get('status', {}))"
```

### Verify the wheel is deployed

```bash
make verify PROFILE=<profile>
```

### Verify the wheel loaded correctly

Check app startup logs:

```bash
databricks apps logs genie-space-optimizer -p <profile> | grep "App wheel:"
```

Expected output:
```
App wheel: genie_space_optimizer-x.y.z.whl (size=NNNNN bytes)
```

### Verify the API is responding

```bash
APP_URL=$(databricks apps get genie-space-optimizer -p <profile> -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])")
curl -s "${APP_URL}/api/genie/version" | python3 -m json.tool
```

### Verify UC grants

Navigate to the Settings page in the app UI, or check app logs:

```bash
databricks apps logs genie-space-optimizer -p <profile> | grep "grant"
```

---

## Post-Deployment: Grant Data Access

After deployment, the app's service principal needs access to the UC schemas containing your Genie Space data. The app operates as an **advisor** -- navigate to the Settings page to see what's missing:

1. Open the app at `https://<workspace>/apps/genie-space-optimizer`
2. Click the gear icon to open Settings
3. Review the permission status for each schema
4. Copy the provided `GRANT` SQL commands into a query editor
5. Execute the grants
6. Refresh Settings to confirm

Alternatively, use the grant script directly:

```bash
python resources/grant_app_uc_permissions.py \
  --profile <profile> \
  --app-name genie-space-optimizer \
  --catalog <catalog> \
  --schema <schema>
```

See [08 -- Permissions and Security](08-permissions-and-security.md) for the complete permissions model.

---

## Updating the App

To deploy code changes:

```bash
make deploy PROFILE=<your-profile>
```

This handles the full cycle: clean stale wheels, build, sync, restart, and verify.

### Common Deployment Issues

| Issue | Symptom | Resolution |
|-------|---------|------------|
| Stale wheel | Old code runs despite changes | Always use `make deploy` (handles `.gitignore` removal) |
| App not restarting | Bundle deployed but app shows old version | Run `databricks apps deploy genie-space-optimizer -p <profile>` |
| Grant script fails | "App not found" | Normal on first deploy; grants apply on next deploy |
| Wheel not found | Job fails immediately | Run `make deploy` to build and upload wheel |

---

## Local Development

For making changes locally before deploying:

```bash
# Install dependencies
uv sync                  # Python
apx bun install          # Frontend

# Start dev servers
apx dev start            # Backend (8000) + Frontend (5173) + OpenAPI watcher

# Check code
apx dev check            # TypeScript + Python type checks

# View logs
apx dev logs -f          # Stream logs

# Stop servers
apx dev stop
```

The OpenAPI client (`ui/lib/api.ts`) auto-regenerates on backend changes when dev servers are running.

---

Next: [10 -- Operations Guide](10-operations-guide.md)
