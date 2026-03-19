# Genie Workbench

Genie Workbench is a unified developer tool for creating, scoring, and optimizing Databricks Genie Spaces. The tool helps Genie developers by:

* Creating Genie spaces from scratch using an agent that gathers business logic, profiles data sources, and generates the initial configuration
* Scoring space quality on a 0-100 rubric across categorized best-practice dimensions with a four-stage maturity model
* Optimizing configurations through a benchmark-driven loop that compares Genie's generated SQL against expected answers and automatically recommends improvements
* Tracking history of every configuration change and score over time, stored in Lakebase
* Versioning and rollback of Genie space configurations, which Genie does not natively support
* Managing multiple spaces across projects and stakeholders from a single dashboard
* Providing scientific proof of lift via MLflow experiment tracking on every benchmark run

## Architecture

The app is a FastAPI backend serving a React/Vite frontend, deployed as a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/). User identity flows via OBO (On-Behalf-Of) auth so each user operates under their own Databricks permissions. Score history and session state are persisted in Lakebase (PostgreSQL), and the GSO optimization pipeline runs as a Databricks job managed by the bundle.

## Prerequisites

* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and authenticated
* Node.js 18+ and npm
* Python 3.11+
* A Databricks workspace with Apps enabled
* A SQL Warehouse (Serverless or Pro)
* A Unity Catalog with CREATE SCHEMA permission

## Quick Start

The guided installer walks you through profile selection, catalog/warehouse discovery, and deployment:

```bash
git clone <repo-url>
cd databricks-genie-workbench
./scripts/install.sh
```

This writes a `.env.deploy` config file and runs `deploy.sh` automatically.

## Manual Deploy

If you prefer non-interactive setup:

```bash
# 1. Configure deployment
cp .env.deploy.template .env.deploy
# Edit .env.deploy — set GENIE_WAREHOUSE_ID and GENIE_CATALOG (both required)

# 2. Deploy
./deploy.sh
```

### Configuration Reference

Set these in `.env.deploy` or as environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `GENIE_WAREHOUSE_ID` | Yes | | SQL Warehouse ID for query execution |
| `GENIE_CATALOG` | Yes | | Unity Catalog name (must have CREATE SCHEMA permission) |
| `GENIE_APP_NAME` | No | `genie-workbench` | Databricks App name |
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint name |
| `GENIE_DEPLOY_TARGET` | No | auto-detected | Bundle target (`dev` or `dev-lakebase`) |

The deploy target is auto-detected: `dev-lakebase` when `databricks.yml` defines a `postgres_projects` resource, `dev` otherwise.

## Deploy Modes

### Full Deploy (default)

Creates or updates everything from scratch: app, job, UC schema, permissions, Lakebase.

```bash
./deploy.sh
```

**What it does (9 steps):**

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm install` + `npm run build`, verifies `frontend/dist/index.html` exists
3. **Clean stale wheels** — removes old GSO package wheels from workspace
4. **Bundle deploy** — `databricks bundle deploy` (Terraform creates/updates app + job resources)
5. **Full-sync files** — `databricks sync --full` + explicit `frontend/dist/` upload (gitignored files)
6. **Grant UC permissions** — creates GSO schema/tables/volume, grants SP access
7. **Configure job permissions** — resolves job ID, grants SP CAN_MANAGE
8. **Redeploy app** — patches `app.yaml` with real GSO values, starts compute, triggers deployment
9. **Verify** — checks critical files on workspace, waits for app to reach RUNNING state

### Code Update (`--update`)

For code-only changes when the app and job already exist. Skips bundle deploy and wheel cleanup.

```bash
./deploy.sh --update
```

### Destroy (`--destroy`)

Tears down the app, job, Lakebase project, and all workspace files.

```bash
./deploy.sh --destroy                # interactive confirmation
./deploy.sh --destroy --auto-approve # skip confirmation
```

## Iterating on Changes

After the initial deploy, use `--update` for fast code iteration:

```bash
# Edit code locally, then:
./deploy.sh --update
```

This builds the frontend, syncs all files (including `frontend/dist/`), re-applies permissions, and redeploys the app. No Terraform changes.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Could not import module "backend.main"` | Bundle deploy only synced changed files; backend source missing on workspace | Re-run `./deploy.sh` (full-sync in step 5 fixes this) or `./deploy.sh --update` |
| `No dependencies file found` | `requirements.txt` not on workspace | Same as above — full-sync uploads it |
| App shows blank page | `frontend/dist/` missing (gitignored, not synced by bundle) | Same as above — deploy.sh explicitly uploads `frontend/dist/` |
| `Catalog 'X' is not accessible` | Wrong catalog for the target workspace | Check available catalogs: `databricks catalogs list --profile <profile>` |
| `Invalid SQL warehouse resource` | Warehouse doesn't exist or deployer lacks CAN_USE | Verify with `databricks warehouses list --profile <profile>` |
| `Maximum number of apps` | Workspace hit the 300-app limit | Delete unused apps in the workspace |
| `stat .build: no such file or directory` during destroy | Bundle validate needs `.build` dir from `sync.include` | Use `./deploy.sh --destroy` (auto-creates stub) |
| App crashes on startup (Lakebase error) | Lakebase instance not yet provisioned or not wired to app | Check app resources in UI; Lakebase provisions automatically on `dev-lakebase` target |
| Unresolved `__GSO_*__` placeholders | deploy.sh couldn't patch `app.yaml` | Ensure `GENIE_CATALOG` is set; check deploy output for warnings |

**Debug commands:**

```bash
# View app logs
databricks apps logs <app-name> --profile <profile>

# Check app status
databricks apps get <app-name> --profile <profile>

# List workspace files to verify sync
databricks workspace list /Workspace/Users/<email>/.bundle/genie-workbench/<target>/files/backend --profile <profile>
```

## How to Get Help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library | description | license | source |
|---|---|---|---|
