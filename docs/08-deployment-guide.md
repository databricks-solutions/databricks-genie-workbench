# Deployment Guide

Genie Workbench is deployed as a Databricks App using the provided deploy scripts. This guide covers first-time setup, subsequent deploys, teardown, and configuration.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) **v0.239.0+** (validated by preflight)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- Node.js 18+ and npm
- Python 3.11+
- A Databricks workspace with:
  - Apps enabled
  - A SQL Warehouse (Serverless recommended)
  - A Unity Catalog with CREATE SCHEMA permission
  - MLflow Prompt Registry enabled (required for Auto-Optimize judge prompts)

## First-Time Setup

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
7. Ask for Lakebase instance name
8. Ask for app name
9. Write `.env.deploy` with your configuration
10. Run `scripts/deploy.sh` to build and deploy the app
11. Resolve the app's service principal
12. Optionally grant the SP access to your existing Genie Spaces

### 4. Attach Lakebase (optional but recommended)

Without Lakebase, scan results and starred spaces are lost on app restart.

> If you used `install.sh`, it already collected your Lakebase instance name (stored as `GENIE_LAKEBASE_INSTANCE` in `.env.deploy`). You still need to attach the resource manually.

**Create a Lakebase instance** (if you don't have one):
1. In the workspace UI, go to **Catalog → Lakebase**
2. Click **Create** → name it (e.g. `genie-workbench`), capacity **CU_1**

**Grant the app's SP access:**
1. Go to **Databases → your instance → Roles**
2. Find the app's SP and grant **CREATEDB** attribute
3. Go to **Databases → your instance → Permissions** and grant **Can manage**

**Attach to your app:**
1. Open **Databricks Apps UI** → your app → **Resources**
2. Click **+ Add resource** → **PostgreSQL (Lakebase)** → select your instance
3. Set resource key to `postgres` with **CAN_CONNECT_AND_CREATE** permission
4. Save and **redeploy** — the app auto-creates the `genie` schema and all tables

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
| `GENIE_LAKEBASE_INSTANCE` | No | `<app-name>` | Lakebase instance name |

## Manual Setup (without installer)

If you prefer non-interactive setup:

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
| `uv.lock` | Root Python transitive deps | SHA256 hashes |
| `packages/genie-space-optimizer/uv.lock` | GSO Python deps | SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps | SHA-512 integrity |
| `packages/genie-space-optimizer/bun.lock` | GSO UI deps | Integrity hashes |

### Updating Python dependencies

```bash
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt \
  | grep -v "^-e " > requirements.txt
echo "-e ./packages/genie-space-optimizer" >> requirements.txt
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

## Typical Workflow

```bash
# First time
./scripts/install.sh

# After code changes
./scripts/deploy.sh --update

# Tear down
./scripts/deploy.sh --destroy
```

## Related Documentation

- [Operations Guide](09-operations-guide.md) — post-deploy monitoring and management
- [Authentication & Permissions](03-authentication-and-permissions.md) — SP permissions granted during deploy
- [Troubleshooting](appendices/B-troubleshooting.md) — common deployment issues
- [Environment Variables](appendices/C-environment-variables.md) — full variable reference
