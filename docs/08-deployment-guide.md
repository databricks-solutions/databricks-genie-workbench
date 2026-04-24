# Deployment Guide

Genie Workbench is deployed as a Databricks App using the provided deploy scripts. This guide covers first-time setup, subsequent deploys, teardown, and configuration.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) **v0.297.2+** (validated by preflight)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- Node.js ^20.19.0 or >=22.12.0 and npm
- Python 3.11+
- **Network access to your configured npm registry** — required to install frontend npm dependencies during the build step. Databricks internal users can use `npm config set registry https://npm-proxy.dev.databricks.com/`; external users should use `npm config set registry https://registry.npmjs.org/`. The deploy script validates this connectivity during pre-flight checks.
- A Databricks workspace with:
  - Apps enabled
  - A SQL Warehouse (Serverless recommended)
  - A Unity Catalog with CREATE SCHEMA permission
  - Permission to create or use a Lakebase Autoscaling project for persistent scan history and sessions
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
7. Ask for app name
8. Create a fresh Lakebase Autoscaling project, choose a different new name, skip persistence, or use advanced existing-project attachment
9. Write `.env.deploy` with your configuration
10. Run `scripts/deploy.sh` to build and deploy the app
11. Resolve the app's service principal
12. Optionally grant the SP access to your existing Genie Spaces

### 4. Lakebase (optional project)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

The guided installer recommends creating a fresh Lakebase Autoscaling project
for each new app instance. It defaults to `<app-name>-lakebase` and, if that
name already exists, suggests a numbered fresh name instead. If you choose to
skip Lakebase, the app still deploys but history and starred spaces are stored
only in memory.

**For a new or deliberately attached Lakebase project, setup is fully automated by `deploy.sh`:**
- Creates the Lakebase Autoscaling project via the SDK (`scripts/setup_lakebase.py`) if it does not exist
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE ON DATABASE)
- Attaches the `postgres` resource to the app via the Apps API

The app creates the `genie` schema and tables on first startup. Since the SP executes the DDL, it owns all objects — no manual grants needed.

The installer writes the project name as `GENIE_LAKEBASE_INSTANCE` in
`.env.deploy`. If you skip Lakebase during install, set
`GENIE_LAKEBASE_INSTANCE` later and run `./scripts/deploy.sh --update`.
Attaching an existing Lakebase project is an advanced path that requires
explicit confirmation because cross-app reuse can fail on object ownership.

> **Note:** The GRANT step requires `psycopg[binary]` in the project venv (installed by `uv sync`). If unavailable, the script prints the commands to run manually in the Lakebase SQL Editor.

### Lakebase reuse and app identity

Lakebase app state is tied to the Databricks App service principal that first
created the `genie` schema. For normal updates, keep `GENIE_APP_NAME`
unchanged and run:

```bash
./scripts/deploy.sh --update
```

Do not point a new app instance at a Lakebase project that already contains a
`genie` schema from an older app instance. A new Databricks App gets a new
service principal, so existing tables and sequences can remain owned by the
old app principal. In that state, IQ scans can fail with:

```text
permission denied for sequence scan_results_id_seq
```

If you need a new app instance, set `GENIE_LAKEBASE_INSTANCE` to a fresh
Lakebase project name. Cross-app Lakebase reuse is not a supported install
path unless a Lakebase project owner or workspace admin deliberately migrates
ownership of the existing `genie` schema, tables, and sequences.

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
| `GENIE_LAKEBASE_INSTANCE` | No | empty | Lakebase Autoscaling project to use or create; installer defaults new installs to `<app-name>-lakebase`; keep stable for the same app, use a fresh project for a new app instance |

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
| `packages/genie-space-optimizer/bun.lock` | GSO UI deps | Integrity hashes |

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
