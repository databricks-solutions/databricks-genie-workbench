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

The app is a FastAPI backend serving a React/Vite frontend, deployed as a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/). User identity flows via OBO (On-Behalf-Of) auth so each user operates under their own Databricks permissions. Score history and session state are persisted in Lakebase (PostgreSQL).

## Prerequisites

* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.230+ recommended)
* Node.js (18+ recommended) and npm
* Python 3.11+
* A Databricks workspace with:
  * Apps enabled
  * A SQL Warehouse (Serverless recommended)
  * A Unity Catalog with CREATE SCHEMA permission
  * MLflow Prompt Registry enabled (required for Auto-Optimize judge prompt traceability)

## Quick Start

### 1. Clone the repo

```bash
git clone <repo-url>
cd databricks-genie-workbench
```

### 2. Authenticate with Databricks CLI

```bash
databricks auth login --profile <workspace-profile>
```

> **Do NOT run `databricks bundle init`** — it overwrites the project configuration. The deploy scripts handle everything.

### 3. Run the guided installer

```bash
./scripts/install.sh
```

The installer will:
1. Check prerequisites (CLI, Node, Python)
2. Ask for your Databricks CLI profile
3. Ask for catalog and SQL warehouse (auto-discovered from your workspace)
4. Ask for LLM model endpoint
5. Ask for Lakebase instance name and app name
6. Write `.env.deploy` with your configuration
7. Run `scripts/deploy.sh` to build and deploy the app
8. Resolve the app's service principal and optionally grant access to your existing Genie Spaces

### 4. Attach Lakebase (optional but recommended)

Without Lakebase, scan results and starred spaces are lost on app restart.

> **Note:** If you used `install.sh`, it already collected your Lakebase instance name (stored as `GENIE_LAKEBASE_INSTANCE` in `.env.deploy`). You still need to attach the resource manually in the Apps UI as described below.

**Create a Lakebase instance** (if you don't have one):
1. In the workspace UI, go to **Catalog → Lakebase** (or **SQL → Lakebase**)
2. Click **Create** → name it (e.g. `genie-workbench`), capacity **CU_1**

**Grant the app's SP access:**
1. Go to **Databases → your instance → Roles**
2. Find the app's SP (e.g. `app-xxxx genie-workbench-v0`) and grant **CREATEDB** attribute
3. Go to **Databases → your instance → Permissions** and grant the SP **Can manage**

**Attach to your app:**
1. Open **Databricks Apps UI** → your app → **Resources**
2. Click **+ Add resource** → **PostgreSQL (Lakebase)** → select your instance
3. Set resource key to `postgres` with **CAN_CONNECT_AND_CREATE** permission
4. Save and **redeploy** — the app creates a `genie` schema and all tables automatically

The app stores data in the `genie` schema within the `databricks_postgres` database. Tables: `scan_results`, `starred_spaces`, `seen_spaces`, `optimization_runs`, `agent_sessions`.

## Manual Setup (without installer)

If you prefer non-interactive setup:

### 1. Create `.env.deploy` in the project root

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

### Configuration Reference

Set these in `.env.deploy` or as environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `GENIE_WAREHOUSE_ID` | Yes | — | SQL Warehouse ID (hex string from warehouse URL or detail page) |
| `GENIE_CATALOG` | Yes | — | Unity Catalog name (you need CREATE SCHEMA permission) |
| `GENIE_APP_NAME` | No | `genie-workbench` | Databricks App name (must be unique in your workspace) |
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile name |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint for analysis |
| `GENIE_LAKEBASE_INSTANCE` | No | `<app-name>` | Lakebase instance name (patched into `app.yaml` at deploy) |

## Deploy Commands

```bash
./scripts/deploy.sh                           # Full deploy: create app, sync code, configure, deploy
./scripts/deploy.sh --update                  # Code-only update: sync + redeploy (faster)
./scripts/deploy.sh --destroy                 # Delete the app and clean up jobs
./scripts/deploy.sh --destroy --auto-approve  # Delete without confirmation prompt
```

### What `deploy.sh` does

**Full deploy (8 steps):**

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm install` + `npm run build`
3. **Create app** — `databricks apps create` (skipped if app already exists)
4. **Sync files** — `databricks sync --full` + explicit `frontend/dist/` upload
5. **Grant UC permissions** — resolves app SP, creates GSO schema/tables, grants SP access, enables CDF
6. **Set up optimization job** — builds GSO wheel, uploads notebooks, creates/finds the Databricks job, grants SP CAN_MANAGE
7. **Redeploy app** — patches `app.yaml` with config values, configures scopes, deploys
8. **Verify** — checks critical files, waits for deployment to succeed

**Code update** (`--update`) skips step 3 (app creation) — use for iterating on code changes.

### Typical workflow

```bash
# First time
./scripts/deploy.sh

# After code changes
./scripts/deploy.sh --update

# Tear down
./scripts/deploy.sh --destroy
```

## Auto-Optimize (GSO Package)

The Auto-Optimize optimization job is created automatically during deploy. The deploy script builds the GSO wheel, uploads job notebooks, and creates the Databricks job — no separate deployment step needed.

If the job already exists (from a previous deploy), it is reused. To force recreation, delete the job in the Databricks UI and re-run `./scripts/deploy.sh --update`.

## Post-Deploy: Genie Space Access

The app uses On-Behalf-Of (OBO) auth — users see only Genie Spaces they have permission to manage. The app's service principal also needs access for fallback operations:

1. The installer grants SP access to your existing Genie Spaces
2. For spaces created after install, share them with the app's service principal (CAN_MANAGE)
3. The SP needs SELECT on schemas referenced by your Genie Spaces:
   ```sql
   GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`
   ```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| App shows blank page | `frontend/dist/` missing (gitignored) | Re-run `./scripts/deploy.sh --update` |
| `Could not import module "backend.main"` | Source files missing on workspace | Re-run `./scripts/deploy.sh --update` (full-sync uploads everything) |
| `No dependencies file found` | `requirements.txt` not on workspace | Same — `./scripts/deploy.sh --update` |
| "Failed to list spaces" | Lakebase not attached | Attach a `postgres` resource in Apps UI (see step 4 above) |
| `Catalog 'X' is not accessible` | Wrong catalog or missing permissions | `databricks catalogs list --profile <profile>` |
| `Invalid SQL warehouse resource` | Warehouse doesn't exist or no CAN_USE | `databricks warehouses list --profile <profile>` |
| `Maximum number of apps` | Workspace hit the 300-app limit | Delete unused apps |
| Auto-Optimize fails at "Baseline Evaluation" with `FEATURE_DISABLED` | Prompt Registry not enabled on workspace | Contact workspace admin to enable MLflow Prompt Registry |
| Unresolved `__GSO_*__` placeholders | deploy.sh couldn't patch `app.yaml` | Ensure `GENIE_CATALOG` is set; check deploy output for warnings |
| GSO job creation fails during deploy | Missing build dependencies or UC Volume permission | Check `ensure_gso_job.py` output; run `pip install build` if needed |
| `ModuleNotFoundError: build` | `build` package not installed for wheel creation | `pip install build` |
| Notebook upload fails (`RESOURCE_DOES_NOT_EXIST`) | `/Workspace/Shared/` not writable by deployer | Check workspace-level permissions on the upload path |

> **Note on MLflow tracing:** The `MLFLOW_EXPERIMENT_ID` in `app.yaml` is workspace-specific. The app validates it at startup and silently disables tracing if the experiment doesn't exist in your workspace. To enable tracing, create an MLflow experiment and update the value in `app.yaml` before deploying.

**Debug commands:**

```bash
# View app logs
databricks apps logs <app-name> --profile <profile>

# Check app status
databricks apps get <app-name> --profile <profile>

# List workspace files to verify sync
databricks workspace list /Workspace/Users/<email>/<app-name>/backend --profile <profile>
```

## How to Get Help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library | description | license | source |
|---|---|---|---|
| asyncpg | Fast PostgreSQL client for asyncio | Apache-2.0 | https://pypi.org/project/asyncpg/ |
| class-variance-authority | CSS class name composition utility | Apache-2.0 | https://github.com/joe-bell/cva |
| clsx | Utility for constructing className strings | MIT | https://github.com/lukeed/clsx |
| databricks-sdk | Databricks SDK for Python | Apache-2.0 | https://pypi.org/project/databricks-sdk/ |
| fastapi | Modern async web framework for APIs | MIT | https://pypi.org/project/fastapi/ |
| httpx | Async/sync HTTP client | BSD-3-Clause | https://pypi.org/project/httpx/ |
| lucide-react | Icon library for React | ISC | https://github.com/lucide-icons/lucide |
| mlflow | ML experiment tracking and model registry | Apache-2.0 | https://pypi.org/project/mlflow/ |
| pandas | Data manipulation and analysis | BSD-3-Clause | https://pypi.org/project/pandas/ |
| prism-react-renderer | Syntax highlighting with Prism for React | MIT | https://github.com/FormidableLabs/prism-react-renderer |
| psycopg | PostgreSQL database adapter (v3) | LGPL-3.0 | https://pypi.org/project/psycopg/ |
| pydantic | Data validation using Python type hints | MIT | https://pypi.org/project/pydantic/ |
| pydantic-settings | Settings management with Pydantic | MIT | https://pypi.org/project/pydantic-settings/ |
| python-dotenv | Load environment variables from .env files | BSD-3-Clause | https://pypi.org/project/python-dotenv/ |
| pyyaml | YAML parser and emitter | MIT | https://pypi.org/project/PyYAML/ |
| react | Library for building user interfaces | MIT | https://github.com/facebook/react |
| react-diff-viewer-continued | Text diff viewer component for React | MIT | https://github.com/aeolun/react-diff-viewer-continued |
| react-dom | React DOM rendering | MIT | https://github.com/facebook/react |
| react-markdown | Render Markdown as React components | MIT | https://github.com/remarkjs/react-markdown |
| recharts | Charting library for React | MIT | https://github.com/recharts/recharts |
| remark-gfm | GitHub Flavored Markdown support for remark | MIT | https://github.com/remarkjs/remark-gfm |
| requests | HTTP library for Python | Apache-2.0 | https://pypi.org/project/requests/ |
| sql-formatter | SQL query formatter | MIT | https://github.com/sql-formatter-org/sql-formatter |
| sqlglot | SQL parser, transpiler, and optimizer | MIT | https://pypi.org/project/sqlglot/ |
| sqlmodel | SQL databases with Python and Pydantic | MIT | https://pypi.org/project/sqlmodel/ |
| tailwind-merge | Merge Tailwind CSS classes without conflicts | MIT | https://github.com/dcastil/tailwind-merge |
| uvicorn | ASGI web server | BSD-3-Clause | https://pypi.org/project/uvicorn/ |
