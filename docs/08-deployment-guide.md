# Deployment Guide

Genie Workbench is deployed as a Databricks App. This guide covers both supported install paths:

- Local terminal installer: `scripts/install.sh` plus `scripts/deploy.sh`
- Databricks notebook installer: `notebooks/install.py` from a Databricks Git folder

Both paths deploy the same app and provision the same core resources. The local path uses the Databricks CLI and Asset Bundles. The notebook path uses notebook-native `WorkspaceClient()` authentication and deploys from a generated workspace source folder.

## Prerequisites

- A Databricks workspace with:
  - Apps enabled
  - A SQL Warehouse (Serverless recommended)
  - A Unity Catalog with CREATE SCHEMA permission
  - Permission to create or use a Lakebase Autoscaling project for persistent scan history and sessions
  - MLflow Prompt Registry enabled (required for Auto-Optimize judge prompts)

For the local terminal installer:

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) **v0.297.2+** (validated by preflight)
- [uv](https://docs.astral.sh/uv/) - Python package manager
- Node.js ^20.19.0 or >=22.12.0 and npm
- Python 3.11+
- Network access to your configured npm registry. Databricks internal users can use `npm config set registry https://npm-proxy.dev.databricks.com/`; external users should use `npm config set registry https://registry.npmjs.org/`.

For the Databricks notebook installer:

- Repo cloned into a Databricks Git folder
- A Databricks compute session that can run `%pip install`
- No local Databricks CLI profile, Node, npm, or uv setup required

## First-Time Setup

Choose one install path. Do not mix the local terminal installer and notebook installer for the same app instance unless you intentionally understand which source path is being deployed.

## Option A: Local Terminal Installer

### 1. Clone the repo

```bash
git clone <repo-url>
cd databricks-genie-workbench
```

### 2. Authenticate with Databricks CLI

```bash
databricks auth login --profile <workspace-profile>
```

> **Do NOT run `databricks bundle init`** — it overwrites the project configuration.

### 3. Run the guided installer

```bash
./scripts/install.sh
```

The installer will:

1. Check prerequisites (CLI version, Node, Python, npm, uv)
2. Ask for your Databricks CLI profile
3. Ask for catalog (auto-discovered from your workspace)
4. Ask for SQL warehouse (auto-discovered)
5. Ask for LLM model endpoint
6. Optionally configure MLflow tracing (creates or links an experiment)
7. Ask for app name
8. Create a fresh Lakebase Autoscaling project, choose a different new name, skip persistence, or use advanced existing-project attachment
9. Write `.env.deploy` with your configuration
10. Run `scripts/deploy.sh` to build and deploy the app
11. Resolve the app's service principal
12. Optionally grant the SP access to your existing Genie Spaces

## Option B: Databricks Notebook Installer

Use this path when you are already working inside Databricks and do not want a local terminal, Databricks CLI profile, local Node/npm, or local uv setup.

1. Clone the repo into a Databricks Git folder.
2. Open `notebooks/install.py`.
3. Set the notebook widgets:

| Widget | Required | Description |
|--------|----------|-------------|
| `app_name` | Yes | Databricks App name to create or update |
| `catalog` | Yes | Unity Catalog for GSO tables and artifacts |
| `warehouse_id` | Yes | SQL Warehouse ID used by the app and GSO |
| `llm_model` | No | Model serving endpoint name |
| `mlflow_experiment_id` | No | MLflow experiment ID for tracing |
| `lakebase_mode` | Yes | `create`, `existing`, or `skip` |
| `lakebase_instance` | Conditional | Lakebase project name for `create` or `existing` |
| `grant_genie_spaces` | No | Whether to grant visible Genie Spaces to the app SP |

4. Run the notebook from the top.

The notebook:

1. Uses notebook-native Databricks auth via `WorkspaceClient()`
2. Creates or updates the Databricks App
3. Resolves the app service principal
4. Generates a clean source folder under `/Workspace/Users/<you>/.genie-workbench-deploy/<app-name>/app`
5. Excludes deploy-only files, docs, tests, notebooks, `scripts/`, `.git`, `.databricks`, `.env*`, `node_modules`, and `requirements.txt`
6. Provisions the UC schema, volume, GSO tables, CDF, and permissions
7. Provisions or attaches Lakebase when requested
8. Creates or updates the `gso-optimization-job` job with the SDK/Jobs API
9. Renders a patched `app.yaml` into the generated source folder
10. Patches app OAuth scopes and resources
11. Deploys the app from the generated source folder
12. Optionally grants the app SP access to visible Genie Spaces

The Git folder remains unchanged. The generated workspace folder is deployment output; do not edit it by hand. To update a notebook-installed app, pull the latest repo changes in Databricks Git and re-run `notebooks/install.py` from the top.

## Lakebase (optional project)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

The guided installer recommends creating a fresh Lakebase Autoscaling project
for each new app instance. It defaults to `<app-name>-lakebase` and, if that
name already exists, suggests a numbered fresh name instead. If you choose to
skip Lakebase, the app still deploys but history and starred spaces are stored
only in memory.

**For a new or deliberately attached Lakebase project, setup is fully automated by the installer:**
- Creates the Lakebase Autoscaling project via the SDK (`scripts/setup_lakebase.py`) if it does not exist
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE ON DATABASE)
- Attaches the `postgres` resource to the app via the Apps API

The local terminal path runs this through `deploy.sh`. The notebook path runs the same resource flow through `scripts.deploy_lib.lakebase`. The app creates the `genie` schema and tables on first startup. Since the SP executes the DDL, it owns all objects - no manual grants needed.

The local terminal installer writes the project name as `GENIE_LAKEBASE_INSTANCE` in
`.env.deploy`. The notebook installer reads the Lakebase project from widgets.
If you skip Lakebase during install, set `GENIE_LAKEBASE_INSTANCE` later and
run `./scripts/deploy.sh --update` for the local path, or set the notebook
`lakebase_mode`/`lakebase_instance` widgets and rerun `notebooks/install.py`.
Attaching an existing Lakebase project is an advanced path that requires
explicit confirmation because cross-app reuse can fail on object ownership.

> **Note:** The GRANT step requires `psycopg[binary]` in the project venv (installed by `uv sync`). If unavailable, the script prints the commands to run manually in the Lakebase SQL Editor.

### Lakebase reuse and app identity

Lakebase app state is tied to the Databricks App service principal that first
created the `genie` schema. For normal updates, keep `GENIE_APP_NAME`
unchanged and update through the same install path:

```bash
./scripts/deploy.sh --update
```

For notebook-installed apps, rerun `notebooks/install.py` with the same
`app_name` and Lakebase widget values.

Do not point a new app instance at a Lakebase project that already contains a
`genie` schema from an older app instance. A new Databricks App gets a new
service principal, so existing tables and sequences can remain owned by the
old app principal. In that state, IQ scans can fail with:

```text
permission denied for sequence scan_results_id_seq
```

If you need a new app instance, use a fresh Lakebase project name. Cross-app
Lakebase reuse is not a supported install path unless a Lakebase project owner
or workspace admin deliberately migrates ownership of the existing `genie`
schema, tables, and sequences.

## What `deploy.sh` Does

### Full Deploy (8 steps)

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm ci` + `npm run build` (strict lockfile)
3. **Create app** — `databricks apps create` (skipped if app already exists)
4. **Sync files** — `databricks sync --full` + explicit `frontend/dist/` upload
5. **Grant UC permissions** — resolves app SP, creates GSO schema/tables, grants SP access, enables CDF
6. **Set up optimization job** — builds GSO wheel, uploads notebooks, creates/finds the Databricks job, grants SP CAN_MANAGE
7. **Redeploy app** — patches `app.yaml` with config values, configures scopes, deploys
8. **Verify** — checks critical files, waits for deployment to succeed

## What `notebooks/install.py` Does

The notebook installer uses the shared `scripts.deploy_lib` Python library. It keeps `app.yaml` in the Git folder as a template and writes only the patched copy into the generated workspace source folder.

Key differences from `deploy.sh`:

- Auth uses notebook-native `WorkspaceClient()`, not a local CLI profile.
- App source is generated under `/Workspace/Users/<you>/.genie-workbench-deploy/<app-name>/app`.
- `requirements.txt` is intentionally excluded so Databricks Apps uses `uv sync` from `pyproject.toml` and `uv.lock`.
- The GSO job is created or reset through the SDK/Jobs API instead of `databricks bundle deploy`.
- The checked-in `app.yaml`, `databricks.yml`, `scripts/install.sh`, and `scripts/deploy.sh` are not mutated.

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

For the local terminal installer, set these in `.env.deploy` or as environment variables. For the notebook installer, the equivalent values come from notebook widgets.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENIE_WAREHOUSE_ID` | Yes | — | SQL Warehouse ID |
| `GENIE_CATALOG` | Yes | — | Unity Catalog name (needs CREATE SCHEMA) |
| `GENIE_APP_NAME` | No | `genie-workbench` | Databricks App name (unique in workspace) |
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile name |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint |
| `GENIE_LAKEBASE_INSTANCE` | No | empty | Lakebase Autoscaling project to use or create; installer defaults new installs to `<app-name>-lakebase`; keep stable for the same app, use a fresh project for a new app instance |

## Manual Setup (without local terminal installer)

If you prefer non-interactive local terminal setup:

### 1. Create `.env.deploy`

```bash
cat > .env.deploy <<'EOF'
GENIE_WAREHOUSE_ID=<your-sql-warehouse-id>
GENIE_CATALOG=<your-catalog-name>
GENIE_APP_NAME=genie-workbench
GENIE_DEPLOY_PROFILE=genie-workbench
GENIE_LLM_MODEL=databricks-claude-sonnet-4-6
GENIE_LAKEBASE_INSTANCE=genie-workbench-lakebase
EOF
```

### 2. Deploy

```bash
./scripts/deploy.sh
```

## Platform Build Strategy

The Databricks Apps platform detects `package.json` at the root and runs `npm install` then `npm run build`. To avoid cross-platform failures and redundant rebuilds:

- **Root `postinstall`**: No-op. It does not invoke nested npm commands during `npm install`.
- **Root `build`**: Checks for pre-built `frontend/dist/index.html`. If present (uploaded by `deploy.sh`), skips the rebuild. If dist is missing, runs `cd frontend && npm ci && npm run build`.
- **Python deps**: Use `uv sync` on the platform (because `requirements.txt` is excluded via `.databricksignore`). This gives a clean venv with SHA256-verified hashes.

## Dependency Security

All dependencies are pinned to exact versions with integrity hashes. Lock files are the source of truth.

| File | Covers | Verification |
|------|--------|-------------|
| `uv.lock` | Root Python transitive deps | SHA256 hashes |
| `packages/genie-space-optimizer/uv.lock` | GSO Python deps | SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps | SHA-512 integrity |
| `packages/genie-space-optimizer/package-lock.json` | GSO UI npm deps | SHA-512 integrity |

### Updating Python dependencies

```bash
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt > requirements.txt
git add uv.lock requirements.txt
```

> Do not edit `requirements.txt` manually. It is generated from `uv.lock`.

### Updating npm dependencies

```bash
cd frontend
npm install <package>@<new-version>
# Update package.json to exact version (remove ^ prefix)
git add package.json package-lock.json
```

Committed npm lockfiles must stay registry-neutral. Keep `omit-lockfile-registry-resolved=true` in project `.npmrc` files so future updates do not commit private registry hosts. Public `registry.npmjs.org` lockfile URLs are safe because npm can rewrite them to the configured registry; configure private/public npm registry hosts in user or global npm config only.

## Typical Workflow

```bash
# Local terminal path, first time
./scripts/install.sh

# Local terminal path, after code changes
./scripts/deploy.sh --update

# Local terminal path, tear down
./scripts/deploy.sh --destroy
```

For the Databricks notebook path, pull the latest repo changes in the Databricks Git folder and rerun `notebooks/install.py` from the top.

## Related Documentation

- [Operations Guide](09-operations-guide.md) — post-deploy monitoring and management
- [Authentication & Permissions](03-authentication-and-permissions.md) — SP permissions granted during deploy
- [Troubleshooting](appendices/B-troubleshooting.md) — common deployment issues
- [Environment Variables](appendices/C-environment-variables.md) — full variable reference
