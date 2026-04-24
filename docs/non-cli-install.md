# Non-CLI Install Path

Install Genie Workbench entirely from inside Databricks — no local
Databricks CLI, Node.js, `uv`, or shell required. The user only interacts
with the Databricks web UI and one setup notebook.

This doc is the Customer Use Journey (CUJ) reference for Epic #109. For
the scripted, laptop-driven install path, see
[08-deployment-guide.md](08-deployment-guide.md).

## When to pick which path

| Constraint | CLI path (`install.sh`) | Non-CLI path (this doc) |
|---|---|---|
| Can install `databricks` CLI + Node.js + `uv` locally | ✓ faster iteration | ✓ works too |
| Locked-down laptop (no admin, no CLI) | ✗ | ✓ |
| No outbound network to `registry.npmjs.org` from laptop | ✗ | ✓ (platform builds) |
| VPN-only workspace access | ✓ (via VPN) | ✓ |
| Want to redeploy in 30s after a local code edit | ✓ | ✗ (requires re-sync) |
| Platform lacks "workspace folder" deploy in Apps UI | ✓ | ✗ (prereq) |

Both paths produce the same app, the same Databricks job, the same UC
schema/tables, and the same resources. Once installed, either path can
update the app going forward.

## Prerequisites

- A Databricks workspace with **Databricks Apps** enabled.
- A serverless SQL Warehouse (any size; Genie Workbench uses it for DDL
  and catalog discovery).
- A Unity Catalog you have `CREATE SCHEMA` on.
- *(Optional, recommended)* A Lakebase Autoscaling project. Without
  Lakebase, scan history and stars don't persist across app restarts.
- *(Optional)* An MLflow experiment ID to enable agent tracing.
- Permission to create Databricks Apps in the workspace.

## Step 1 — Clone the repo into a Databricks Git folder

1. In the Databricks workspace, click **Workspace** in the left nav.
2. Click **Add** → **Git folder**.
3. Paste the repo URL
   `https://github.com/databricks-solutions/databricks-genie-workbench`
   and choose a branch (usually `main`).
4. Pick a destination under `/Users/<you>/` (keep the default name).

> **Private forks**: if you've forked the repo privately, configure a
> workspace-level Git credential first
> (**User Settings** → **Git integration**) so the clone can authenticate.

## Step 2 — Run the setup notebook

1. In the Git folder, open `scripts/notebooks/setup_workbench` and
   **attach it to serverless compute** (top-right attach menu).
2. Fill in the widgets at the top:

   | Widget | What to enter |
   |---|---|
   | `app_name` | Lowercase-with-hyphens name for the app (default `genie-workbench`). |
   | `catalog` | Unity Catalog you have `CREATE SCHEMA` on. |
   | `warehouse_id` | SQL Warehouse ID (open the warehouse → copy ID from URL). |
   | `llm_model` | LLM serving endpoint name. Default is `databricks-claude-sonnet-4-6`. |
   | `lakebase_project` | *(optional)* Lakebase Autoscaling project ID. Leave blank for in-memory. |
   | `mlflow_experiment_id` | *(optional)* MLflow experiment ID for agent tracing. |
   | `workspace_folder` | *(optional)* Absolute workspace path to the repo. Leave blank to auto-detect from the notebook path. |
   | `grant_genie_spaces` | `Y` grants the app SP `CAN_EDIT` on every Genie Space you can edit. `N` to skip. |

3. Click **Run all**. The notebook runs 10 cells in sequence and typically
   takes 2–3 minutes. What it does:
   - Creates the Databricks App and its service principal.
   - Grants the SP `CAN_MANAGE` on the repo workspace folder.
   - Builds the GSO optimization wheel in the workspace.
   - Creates the 6-task optimization job (or updates it if it exists).
   - Creates the UC schema + GSO Delta tables + volume; grants the SP.
   - Provisions the Lakebase project, SP role, and database grants.
   - `PATCH`es the app with OAuth scopes and resources (`sql-warehouse`,
     `postgres`).
   - Writes a placeholder-substituted `app.yaml` into the workspace folder.
   - Moves `requirements.txt` aside in the workspace folder so Apps uses
     `pyproject.toml` + `uv.lock` instead of pip.
   - Grants the SP `CAN_EDIT` on your Genie Spaces (if `grant_genie_spaces=Y`).
4. Confirm the last cell prints **"Provisioning complete — finish the
   install in the Apps UI"**. Copy the listed **workspace folder** path;
   you'll paste it in the next step.

## Step 3 — Deploy from the Apps UI

1. In the Databricks workspace, click **Compute** → **Apps**.
2. Click the app name you chose (e.g. `genie-workbench`).
3. Click **Deploy** (top-right) → **Deploy from a workspace folder**.
4. Paste the workspace folder path from Step 2 (e.g.
   `/Workspace/Users/you@company.com/databricks-genie-workbench`).
5. Click **Deploy**.

The Apps platform runs root `npm install`, then root `npm run build`.
When `frontend/dist` is absent, the root build script runs
`cd frontend && npm ci && npm run build`. The setup notebook moves the
pip-compatible `requirements.txt` reference file aside before this step so
the workspace-folder path uses the same `uv` install path as CLI deploy.
First deploy takes 3–5 minutes.
Subsequent deploys are faster because `node_modules/` and the virtualenv
are cached.

When the app reaches **RUNNING**, click the app URL and verify:
- **IQ Scan** works on a Genie Space you have `CAN_EDIT` on.
- **Create Agent** loads the catalog picker (UC access).
- **Auto-Optimize** → settings shows the GSO job is detected.

## Updating the app later

Any of these paths works:

- **Edit in the workspace**: open a file in the Git folder, edit, pull
  via Git UI, then in the Apps UI click **Deploy** again.
- **Re-run the setup notebook**: if you change `catalog`/`warehouse_id`/
  Lakebase wiring, re-running the notebook updates those and re-patches
  `app.yaml`. Safe to re-run (idempotent).
- **Switch to the CLI path**: `./scripts/install.sh` detects the existing
  app and reuses it — no reinstall needed.

## Troubleshooting

### "Could not resolve object_id for /Workspace/..."

The SP grant on the workspace folder failed. Open the workspace folder
in the UI → click the **︙** menu → **Permissions** → add the app's
service principal with **CAN_MANAGE**. Re-run the notebook's cell 6.

### "App '<name>' has no service principal yet"

Apps provisioning is slow on first creation. Wait 30 seconds and re-run
cell 5. The notebook retries for up to 2 minutes internally.

### "Lakebase grants incomplete — app may fall back to in-memory storage"

The SP role was created but database grants couldn't be applied.
Confirm `lakebase_project` points to a **Lakebase Autoscaling** project
(not provisioned Lakebase). Then open the Lakebase SQL Editor for your
project and run:
```sql
GRANT CONNECT ON DATABASE databricks_postgres TO "<sp-client-id>";
GRANT CREATE ON DATABASE databricks_postgres TO "<sp-client-id>";
```

### App reaches RUNNING but "Failed to list spaces" in the UI

The `postgres` app resource wasn't wired. Open the app → **Resources**
→ confirm `postgres` points at your Lakebase database with
`CAN_CONNECT_AND_CREATE`. If missing, re-run the notebook — Step 9's
Apps PATCH will wire it.

### Optimization job runs fail with "No module named _metadata"

The `_metadata.py` stub didn't sync. Open the workspace folder →
`packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py`
and confirm it exists. If missing, pull the latest commit in the Git
folder UI and re-run the notebook.

### "Could not grant USE_CATALOG on '<catalog>'"

You don't have `MANAGE` on the catalog. Ask a catalog owner to run:
```sql
GRANT USE_CATALOG ON CATALOG `<catalog>` TO `<sp-client-id>`;
```
The notebook surfaces the exact GRANT statement to send.

## What this path does NOT cover (yet)

- **Git-source Apps deploy** (paste a `github.com` URL directly in Apps
  UI and skip the workspace folder step). Requires refactoring
  `app.yaml` placeholders to use `valueFrom` resource projections.
  Deferred to a follow-up — see
  [TODO/109-non-cli-install.md](../TODO/109-non-cli-install.md).
- **Auto-Optimize UC → Lakebase synced tables**. Currently a manual step
  via Catalog Explorer. The app falls back to direct Delta reads, so
  Auto-Optimize still works.

## References

- Platform docs: [Deploy an app from a workspace folder](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy#deploy-from-a-workspace-folder)
- CLI install path: [08-deployment-guide.md](08-deployment-guide.md)
- Shared provisioning module: `scripts/setup_workbench.py`
- Setup notebook source: `scripts/notebooks/setup_workbench.py`
