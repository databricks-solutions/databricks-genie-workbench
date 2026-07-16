---
sidebar_position: 3
description: "First-time setup, deploy commands, Lakebase, and the configuration reference."
---

# Deployment Guide

Genie Workbench is deployed as a Databricks App. This guide covers both supported install paths:

- **Method 1 (Recommended): Databricks Workspace Notebook** — run `notebooks/install.py` directly inside the Databricks workspace UI, using notebook-native `WorkspaceClient()` authentication. No local terminal, CLI profile, Node, npm, or uv setup required.
- **Method 2 (Backup): Local Terminal** — `scripts/install.sh` plus `scripts/deploy.sh`, using the Databricks CLI and Asset Bundles.

Both paths deploy the same app and provision the same core resources. If you are already working inside Databricks, use Method 1.

## Prerequisites

### Workspace resources

A Databricks workspace with:

- Apps enabled
- A SQL Warehouse (Serverless recommended)
- A Unity Catalog with CREATE SCHEMA permission
- Lakebase Autoscaling available (optional but recommended for persistent scan history, starred spaces, and agent sessions)
- MLflow Prompt Registry enabled (required for Auto-Optimize judge prompts)
- Databricks Foundation Model APIs enabled for the curated Create Agent and Auto-Optimize model list

### Enable required preview features

Before installing, enable these on the Databricks **Previews** page (workspace admins manage preview toggles):

- **Managed MLflow Prompt Registry** (Beta) — required for Auto-Optimize judge prompts
- **Databricks Apps – On-Behalf-Of User Authorization** (Public Preview)

:::note
On-Behalf-Of User Authorization is what lets the app act as the signed-in user (OBO auth). Without it, Genie API calls that require user identity will fail. See [Authentication & Permissions](/docs/platform/authentication) for details.
:::

### Installer permissions

The installer creates apps, UC objects, Lakebase projects, MLflow experiments, and jobs, and grants the app service principal across all of them. **The person running the installer typically needs workspace-admin equivalents.** If you are uncertain, have a workspace admin run the installer. See [Authentication & Permissions](/docs/platform/authentication) for the full entitlement list.

### Method 1 (notebook) prerequisites

- The repo cloned into a Databricks Git folder (covered in the steps below)
- A Serverless compute session (**environment v5**) that can run the notebook

### Method 2 (local terminal) prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) **v0.297.2+** (validated by preflight)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- Node.js ^20.19.0 or >=22.12.0 and npm
- Python 3.11+
- **Network access to your npm registry** — required to install frontend npm dependencies during the build step. Databricks internal users can run `npm config set registry https://npm-proxy.dev.databricks.com/`; external users use `registry.npmjs.org`. If you are behind a corporate firewall or VPN that blocks this, allowlist the registry or connect via a network that permits outbound HTTPS to it. The deploy script validates this connectivity during pre-flight checks.

## First-Time Setup

Choose one install path. Do not mix Method 1 and Method 2 for the same app instance unless you intentionally understand which source path is being deployed.

## Method 1: Databricks Workspace Notebook (Recommended)

Use this path when you are already working inside Databricks. It runs entirely in the workspace UI — no local terminal, CLI profile, Node, npm, or uv required.

### 1. Clone the repo into your workspace

In the Databricks workspace, go to **Workspace → Create → Git folder** and paste the repo URL:

```
https://github.com/databricks-solutions/databricks-genie-workbench.git
```

This creates a Git folder containing the full project, including `notebooks/install.py`.

### 2. Run `notebooks/install.py`

Open `notebooks/install.py`, attach a **Serverless** compute session (**environment v5**), set the notebook widgets, and **Run All**.

| Widget | Required | Description |
|--------|----------|-------------|
| `app_name` | Yes | Databricks App name to create or update |
| `catalog` | Yes | Unity Catalog where the optimizer writes its tables and artifacts |
| `warehouse_id` | Yes | Serverless SQL Warehouse ID used by the app and GSO |
| `lakebase_mode` | Yes | `create`, `existing`, or `skip` |
| `lakebase_project_name` | Conditional | Lakebase project name for `create` or `existing`; defaults to `<app-name>-lakebase` for `create`. Leave blank when `lakebase_mode` is `skip` |

:::note
The notebook user must hold the [installer permissions](/docs/platform/authentication). The notebook automatically grants visible Genie Spaces to the app service principal, so the user needs `CAN_MANAGE` on each visible space they want the app to manage.
:::

<!-- TODO(screenshot): the installer notebook open in the workspace with widgets visible -->
<!-- TODO(screenshot): example of the input variables filled in -->

### 3. What the notebook does

The installer uses the shared `scripts.deploy_lib` Python library and notebook-native `WorkspaceClient()` authentication. It:

1. Creates or updates the Databricks App and resolves its service principal
2. Generates a clean source folder under `/Workspace/Users/<you>/.genie-workbench-deploy/<app-name>/app` (excluding deploy-only files, docs, tests, notebooks, `scripts/`, `.git`, `.env*`, `node_modules`, and `requirements.txt`)
3. Provisions the UC schema, volume, GSO tables, CDF, and permissions
4. Provisions or attaches Lakebase when requested
5. Creates or updates the GSO optimization job (`<app-name>-gso-optimization-job`) via the SDK/Jobs API
6. Renders a patched `app.yaml` into the generated source folder and patches app OAuth scopes and resources
7. Deploys the app from the generated source folder
8. Grants the app SP access to visible Genie Spaces

The Git folder is never modified — the generated workspace folder is deployment output; do not edit it by hand.

### 4. Updating a notebook-installed app

Pull the latest repo changes in the Databricks Git folder, then rerun `notebooks/install.py` from the top with the same `app_name` and Lakebase widget values.

## Method 2: Local Terminal Installer (Backup)

Use this path when you prefer to deploy from a local machine with the Databricks CLI.

### 1. Clone the repo

```bash
git clone https://github.com/databricks-solutions/databricks-genie-workbench.git
cd databricks-genie-workbench
```

### 2. Authenticate with Databricks CLI

```bash
databricks auth login --profile <workspace-profile>
```

:::danger
**Do NOT run `databricks bundle init`** — it overwrites the project configuration.
:::

### 3. Run the guided installer

```bash
./scripts/install.sh
```

The installer will:

1. Check prerequisites (CLI version, Node, Python, npm, uv)
2. Ask for your Databricks CLI profile
3. Ask for catalog (auto-discovered from your workspace)
4. Ask for SQL warehouse (auto-discovered)
5. Optionally configure MLflow tracing (creates or links an experiment)
6. Ask for app name
7. Ask for the Lakebase Autoscaling project (create / existing / skip)
8. Write `.env.deploy` with your configuration (including the default LLM endpoint)
9. Run `scripts/deploy.sh` to build and deploy the app
10. Resolve the app's service principal
11. Optionally grant the SP access to your existing Genie Spaces

## Lakebase (automated)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

**Lakebase setup is fully automated by both install paths:**
- Creates a Lakebase Autoscaling project via the SDK
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE ON DATABASE)
- Attaches the `postgres` resource to the app via the Apps API

The app creates the `genie` schema and tables on first startup. Since the SP executes the DDL, it owns all objects — no manual grants needed.

The notebook path reads the Lakebase project from the `lakebase_mode` / `lakebase_project_name` widgets. The local terminal path asks for a project name (defaults to the app name, stored as `GENIE_LAKEBASE_INSTANCE` in `.env.deploy`). No manual steps required for either.

:::note
The GRANT step requires `psycopg[binary]` in the project venv (installed by `uv sync`). If unavailable, the script prints the commands to run manually in the Lakebase SQL Editor.
:::

## What `deploy.sh` Does

This applies to **Method 2 (local terminal)**. The notebook installer runs the equivalent steps through the `scripts.deploy_lib` library instead — see [What the notebook does](#3-what-the-notebook-does).

### Full Deploy (8 steps)

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm ci` + `npm run build` (strict lockfile)
3. **Create app** — `databricks apps create` (skipped if app already exists)
4. **Sync files** — `databricks sync --full` + explicit `frontend/dist/` upload
5. **Grant UC permissions** — resolves app SP, creates GSO schema/tables, grants SP access, enables CDF
6. **Set up optimization job** — builds GSO wheel, uploads notebooks, creates/finds the Databricks job, grants SP CAN_MANAGE
7. **Redeploy app** — patches `app.yaml` with config values, configures scopes, deploys
8. **Verify** — checks critical files, waits for deployment to succeed

### Deploy Commands

```bash
./scripts/deploy.sh                           # Full deploy
./scripts/deploy.sh --update                  # Code-only update (skips app creation)
./scripts/deploy.sh --destroy                 # Tear down app and clean up jobs
./scripts/deploy.sh --destroy --auto-approve  # Tear down without confirmation
```

### What `--update` skips

`--update` skips step 3 (app creation). Use it for iterating on code changes after the initial deploy.

### What `--destroy` cleans up (and what it doesn't)

`--destroy` deletes:
- The Databricks App
- Runtime-created jobs
- The bundle-managed optimization job

It does **not** remove:
- Lakebase data (the `genie` schema in `databricks_postgres`)
- Unity Catalog schema/tables (`<catalog>.genie_space_optimizer` and its tables)
- Genie Space SP permissions granted during install
- MLflow experiments created during install
- Synced tables (if manually created)

Clean these up manually if you want a full teardown.

## Configuration Reference

Set these in `.env.deploy` or as environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENIE_WAREHOUSE_ID` | Yes | — | SQL Warehouse ID |
| `GENIE_CATALOG` | Yes | — | Unity Catalog name (needs CREATE SCHEMA) |
| `GENIE_APP_NAME` | No | `genie-workbench` | Databricks App name (unique in workspace) |
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile name |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint |
| `GENIE_LAKEBASE_INSTANCE` | No | empty | Lakebase Autoscaling project name (auto-provisioned by deploy); installer suggests `<app-name>-lakebase` |

## Manual Setup (Method 2, without installer)

If you prefer non-interactive local terminal setup:

### 1. Create `.env.deploy`

```bash
cat > .env.deploy <<'EOF'
GENIE_WAREHOUSE_ID=<your-sql-warehouse-id>
GENIE_CATALOG=<your-catalog-name>
GENIE_APP_NAME=genie-workbench
GENIE_DEPLOY_PROFILE=genie-workbench
GENIE_LLM_MODEL=databricks-claude-sonnet-4-6
GENIE_LAKEBASE_INSTANCE=genie-workbench
EOF
```

### 2. Deploy

```bash
./scripts/deploy.sh
```

## Platform Build Strategy

The Databricks Apps platform detects `package.json` at the root and runs `npm install` then `npm run build`. To avoid cross-platform failures and redundant rebuilds:

- **Root `postinstall`**: No-op. Frontend deps are installed by `deploy.sh` locally.
- **Root `build`**: Checks for pre-built `frontend/dist/index.html`. If present (uploaded by `deploy.sh`), skips the rebuild. Falls back to a full build only if dist is missing.
- **Python deps**: Use `uv sync` on the platform (because `requirements.txt` is excluded via `.databricksignore`). This gives a clean venv with SHA256-verified hashes.

## Dependency Security

All dependencies are pinned to exact versions with integrity hashes. Lock files are the source of truth.

| File | Covers | Verification |
|------|--------|-------------|
| `uv.lock` | Workspace-wide Python transitive deps (root + GSO) | SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps | SHA-512 integrity |

### Updating Python dependencies

```bash
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt \
  | grep -v "^-e " > requirements.txt
echo "-e ./packages/genie-space-optimizer" >> requirements.txt
git add uv.lock requirements.txt
```

:::warning
Do not edit `requirements.txt` manually. It is generated from `uv.lock`.
:::

### Updating npm dependencies

```bash
cd frontend
npm install <package>@<new-version>
# Update package.json to exact version (remove ^ prefix)
git add package.json package-lock.json
```

## Typical Workflow

**Method 1 (notebook):** pull the latest repo changes in the Databricks Git folder and rerun `notebooks/install.py` from the top — for both first-time installs and updates.

**Method 2 (local terminal):**

```bash
# First time
./scripts/install.sh

# After code changes
./scripts/deploy.sh --update

# Tear down
./scripts/deploy.sh --destroy
```

## Related Documentation

- [Operations Guide](/docs/platform/operations) — post-deploy monitoring and management
- [Authentication & Permissions](/docs/platform/authentication) — SP permissions granted during deploy
- [Troubleshooting](/docs/reference/troubleshooting) — common deployment issues
- [Environment Variables](/docs/reference/environment-variables) — full variable reference
