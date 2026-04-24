# Deployment Guide

Genie Workbench is deployed as a Databricks App. There are two install
paths — both produce the same app, same UC schema/tables, same
optimization job, and same resources:

- **CLI path** (this guide): laptop-driven. Requires the Databricks CLI,
  Node.js, `uv`, and shell access. Faster iteration once set up.
- **Non-CLI path**: entirely inside the Databricks UI — clone into a Git
  folder, run a setup notebook on serverless compute, then deploy from
  the Apps UI. See [non-cli-install.md](non-cli-install.md).

Both paths call the same shared provisioning module
(`scripts/setup_workbench.py`) for UC grants, Lakebase, Apps PATCH, and
Genie Space permissions.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) **v0.297.2+** (validated by preflight)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- Node.js 18+ and npm
- Python 3.11+
- **Network access to `registry.npmjs.org`** — required to install frontend npm dependencies during the build step. If you are behind a corporate firewall or VPN that blocks this, you must either allowlist `registry.npmjs.org` or connect via a network that permits outbound HTTPS to it. The deploy script validates this connectivity during pre-flight checks.
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
7. Ask for Lakebase Autoscaling project name
8. Ask for app name
9. Ask whether to grant the SP access to your existing Genie Spaces
10. Write `.env.deploy` with your configuration
11. Run `scripts/deploy.sh` to build, bundle-deploy, and provision resources

Under the hood, `deploy.sh` delegates UC grants, Lakebase provisioning,
Apps PATCH, `app.yaml` patching, job permissions, bundle-directory
grants, and Genie Space grants to `scripts/setup_workbench.py` — the
same module the non-CLI install notebook uses.

### 4. Lakebase (automated)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

**Lakebase setup is fully automated by `setup_workbench.py`** (driven by
`deploy.sh` on the CLI path, or the setup notebook on the non-CLI path):

- Creates a Lakebase Autoscaling project via the SDK
  (`scripts/setup_lakebase.py`, reused as a library)
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE ON DATABASE)
- Attaches the `postgres` resource to the app via the Apps API

The app creates the `genie` schema and tables on first startup. Since the SP executes the DDL, it owns all objects — no manual grants needed.

The installer asks for a Lakebase project name (defaults to the app name, stored as `GENIE_LAKEBASE_INSTANCE` in `.env.deploy`). No manual steps required.

> **Note:** The GRANT step requires `psycopg[binary]` in the project venv (installed by `uv sync`). If unavailable, the script prints the commands to run manually in the Lakebase SQL Editor.

## What `deploy.sh` Does

### Full Deploy (9 steps)

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm ci` + `npm run build` (strict lockfile)
3. **Create app** — `databricks apps create` (skipped if app already exists)
4. **Sync files** — `databricks sync --full` + explicit `frontend/dist/` upload
5. **Bundle deploy** — builds GSO wheel, uploads notebooks, creates the optimization job; syncs `_metadata.py`; cleans up legacy jobs
6. **Wait for app compute** — starts the app compute and waits for it to reach `ACTIVE` (required by `apps deploy`)
7. **Provision resources** — calls `scripts/setup_workbench.py` which does UC schema/tables/grants, Lakebase project/role/grants, Apps PATCH (scopes + resources), `app.yaml` placeholder substitution, job permissions, bundle-directory SP grant, and (optional) Genie Space SP grants — all in one pure-SDK pass
8. **Redeploy app** — `databricks apps deploy --source-code-path`
9. **Verify** — checks critical files, waits for deployment to succeed

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
| `GENIE_LAKEBASE_INSTANCE` | No | `<app-name>` | Lakebase Autoscaling project name (auto-provisioned by deploy) |

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
