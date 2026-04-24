# Deployment Guide

Genie Workbench is deployed as a Databricks App. There are two install
paths — both produce the same app, same UC schema/tables, same
optimization job, and same resources:

- **CLI path** (this guide): laptop-driven. Requires the Databricks CLI,
  Node.js, `uv`, and shell access. Faster iteration once set up.
- **Web Terminal path**: runs the same scripts from Databricks Web
  Terminal when local VM policy blocks Databricks CLI usage. See
  [web-terminal-install.md](web-terminal-install.md).

Both paths call the same shared provisioning module
(`scripts/setup_workbench.py`) for UC grants, Lakebase, Apps PATCH, and
Genie Space permissions.

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
7. Select an existing Lakebase Autoscaling project, create a new one, or skip persistence
8. Ask for app name
9. Ask whether to grant the SP access to your existing Genie Spaces
10. Write `.env.deploy` with your configuration
11. Run `scripts/deploy.sh` to build, bundle-deploy, and provision resources

Under the hood, `deploy.sh` delegates UC grants, Lakebase provisioning,
Apps PATCH, `app.yaml` patching, job permissions, bundle-directory
grants, and Genie Space grants to `scripts/setup_workbench.py`. The same
deploy flow runs locally and from Databricks Web Terminal.

### 4. Lakebase (optional project)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

The guided installer discovers existing Lakebase Autoscaling projects in your
workspace and also offers to create a new project during deploy. If you choose
to skip Lakebase, the app still deploys but history and starred spaces are
stored only in memory.

**For a selected or newly named Lakebase project, setup is automated by
`setup_workbench.py`** (driven by `deploy.sh` on both the local CLI path and
the Web Terminal path):

- Creates the Lakebase Autoscaling project if it does not exist
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE ON DATABASE)
- Attaches the `postgres` resource to the app via the Apps API

The app creates the `genie` schema and tables on first startup. Since the SP executes the DDL, it owns all objects — no manual grants are needed for a fresh Lakebase project.

The installer writes the project name as `GENIE_LAKEBASE_INSTANCE` in
`.env.deploy`. If you skip Lakebase during install, set
`GENIE_LAKEBASE_INSTANCE` later and run `./scripts/deploy.sh --update`.

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
old app principal. In that state, app startup may log owner-only maintenance
warnings and IQ scans can fail with:

```text
permission denied for sequence scan_results_id_seq
```

If you need a new app instance, set `GENIE_LAKEBASE_INSTANCE` to a fresh
Lakebase project name. Cross-app Lakebase reuse is not a supported install
path unless a Lakebase project owner or workspace admin deliberately migrates
ownership of the existing `genie` schema, tables, and sequences.

## What `deploy.sh` Does

### Full Deploy (9 steps)

1. **Pre-flight checks** — validates tools, CLI auth, warehouse, catalog, app state
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
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile name; set to empty string for Web Terminal current-user auth |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint |
| `GENIE_LAKEBASE_INSTANCE` | No | empty | Lakebase Autoscaling project to use or create; keep stable for the same app, use a fresh project for a new app instance |

## Manual Setup (without installer)

If you prefer non-interactive setup:

### 1. Create `.env.deploy`

```bash
cat > .env.deploy <<'EOF'
GENIE_WAREHOUSE_ID=<your-sql-warehouse-id>
GENIE_CATALOG=<your-catalog-name>
GENIE_APP_NAME=genie-workbench
GENIE_DEPLOY_PROFILE=genie-workbench  # use "" in Databricks Web Terminal
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
