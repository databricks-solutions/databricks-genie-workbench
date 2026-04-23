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

## Permissions & Authentication Model

Genie Workbench uses a **dual-identity model**: the signed-in user's token for interactive operations and the app's Service Principal for background jobs. This section summarizes how each identity is used and why.

> For a full deep dive with code references, sequence diagrams, and GRANT statements, see [docs/03-authentication-and-permissions.md](docs/03-authentication-and-permissions.md).

### OBO (On-Behalf-Of) Auth

All interactive API calls use the signed-in user's identity. The Databricks Apps platform forwards the user's access token via the `x-forwarded-access-token` header. Middleware in `backend/main.py` stores a per-request `WorkspaceClient` in a Python `ContextVar`, so every downstream service call — browsing Unity Catalog, listing Genie Spaces, executing SQL, creating spaces — runs under the user's permissions. Users only see what they have access to.

### Service Principal Fallback for Genie API

Some user OAuth tokens lack the `dashboards.genie` scope. When the app detects a scope error (via `_is_scope_error()` in `backend/services/genie_client.py`), it transparently retries the call with the app's Service Principal. For this fallback to work, the SP must have **CAN_MANAGE** on each Genie Space.

### Service Principal for Optimization Jobs

The Auto-Optimize (GSO) pipeline runs as a **Lakeflow Job** — a long-running, multi-task DAG that can take minutes to complete. Lakeflow Jobs execute in a separate environment with a fixed `run_as` identity; there is no mechanism to forward the user's short-lived OAuth token into a background job. Therefore the optimization job runs as the app's Service Principal.

Security is preserved because:

1. **Authorization at trigger time** — the app verifies the user has `CAN_EDIT` or `CAN_MANAGE` on the Genie Space (via OBO) before submitting the job.
2. **SP entitlement validated** — the app confirms the SP has `CAN_MANAGE` on the space before job submission.
3. **Minimum-privilege SP** — the SP only needs read access to referenced data schemas and manage access to the optimization state schema.

### SP Permissions Required

| Scope | Permission | Purpose |
|-------|-----------|---------|
| Each Genie Space | `CAN_MANAGE` | API fallback + optimization patches |
| Referenced data schemas | `SELECT`, `USE_SCHEMA`, `USE_CATALOG` | Data access during optimization benchmarks |
| GSO optimizer schema | `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_MODEL`, `CREATE_VOLUME`, `EXECUTE`, `MANAGE` | Optimizer state tables, MLflow models, prompt registry |

### Permission Boundary Summary

| Operation | Identity | Rationale |
|-----------|----------|-----------|
| Browse Genie Spaces, UC catalogs/schemas/tables | OBO (user) | User sees only what they have access to |
| Genie API (fetch/list spaces) | OBO, SP fallback on scope error | User token may lack `dashboards.genie` scope |
| Create Agent (tools, SQL, space creation) | OBO (user) | Space created under user's identity |
| Quick Fix (generate + apply patches) | OBO (user) | Patches applied as the user |
| Trigger optimization (permission check) | OBO (user) | Verifies user has CAN_EDIT/CAN_MANAGE |
| Optimization job execution (6-task DAG) | SP | Lakeflow Jobs have no OBO; SP runs the pipeline |
| GSO Delta table reads/writes | SP | Optimizer state tables owned by SP |
| Lakebase persistence | SP | App-level storage, not user-scoped |

## Prerequisites

* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.297.2+ required)
* [uv](https://docs.astral.sh/uv/) — Python package manager (used for dependency management and hash-verified installs)
* Node.js (18+ recommended) and npm
* Python 3.11+

> **Databricks internal users:** before running `./scripts/install.sh`, make sure your `uv`/`pip` and `npm` clients are configured for public registry access.

* A Databricks workspace with:
  * Apps enabled
  * A SQL Warehouse (Serverless recommended)
  * A Unity Catalog with CREATE SCHEMA permission
  * MLflow Prompt Registry enabled (required for Auto-Optimize judge prompt traceability)

## Two install paths

| Path | When to use | Details |
|---|---|---|
| **CLI** (`./scripts/install.sh`) | You have a laptop with the Databricks CLI, Node.js, and `uv` installed. Faster iteration. | [docs/08-deployment-guide.md](docs/08-deployment-guide.md) |
| **Non-CLI** (workspace notebook) | Locked-down laptop, no local tooling. Entirely inside Databricks. | [docs/non-cli-install.md](docs/non-cli-install.md) |

Both paths produce the same app, call the same shared provisioning
module (`scripts/setup_workbench.py`), and are fully interoperable —
you can switch between them at any time.

## Quick Start (CLI path)

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
1. Check prerequisites (CLI, Node, Python, npm, uv)
2. Ask for your Databricks CLI profile
3. Ask for catalog (auto-discovered from your workspace)
4. Ask for SQL warehouse (auto-discovered from your workspace)
5. Ask for LLM model endpoint
6. Optionally configure MLflow tracing (creates or links an experiment)
7. Ask for Lakebase Autoscaling project name
8. Ask for app name
9. Ask whether to grant the SP access to your existing Genie Spaces
10. Write `.env.deploy` with your configuration
11. Run `scripts/deploy.sh`, which builds the app, bundle-deploys the
    optimization job, and delegates all UC/Lakebase/Apps-PATCH/Genie
    grants to `scripts/setup_workbench.py`

### 4. Lakebase (automated)

Lakebase provides persistent storage for scan history, starred spaces, and agent sessions. Without it, the app uses in-memory storage (data lost on restart).

**Lakebase setup is automated by `deploy.sh`:**
- Creates a Lakebase Autoscaling project (if it doesn't exist)
- Creates a Postgres role for the app's service principal
- Grants database permissions (CONNECT, CREATE)
- Attaches the `postgres` resource to the app

The installer asks for a Lakebase project name (defaults to the app name). The deploy script calls `scripts/setup_lakebase.py` to provision everything, then attaches the resource via the Apps API. No manual steps required.

> **Note:** The GRANT step requires `psycopg[binary]` in the project venv (installed by `uv sync`). If unavailable, the script prints the commands to run manually in the Lakebase SQL Editor.

The app automatically creates a `genie` schema and tables on first startup within the `databricks_postgres` database. Tables: `scan_results`, `starred_spaces`, `seen_spaces`, `optimization_runs`, `agent_sessions`.

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
| `GENIE_LAKEBASE_INSTANCE` | No | `<app-name>` | Lakebase Autoscaling project name (auto-provisioned by deploy) |

## Deploy Commands

```bash
./scripts/deploy.sh                           # Full deploy: create app, sync code, configure, deploy
./scripts/deploy.sh --update                  # Code-only update: sync + redeploy (faster)
./scripts/deploy.sh --destroy                 # Tear down app and clean up jobs
./scripts/deploy.sh --destroy --auto-approve  # Tear down without confirmation prompt
```

### What `--destroy` cleans up (and what it doesn't)

`--destroy` deletes the Databricks App, runtime-created jobs, and the bundle-managed optimization job. It does **not** remove:
- Lakebase data (the `genie` schema in `databricks_postgres`)
- Unity Catalog schema/tables (`<catalog>.genie_space_optimizer` and its 8 tables)
- Genie Space SP permissions granted during install
- MLflow experiments created during install
- Synced tables (if manually created)

Clean these up manually if you want a full teardown.

### What `deploy.sh` does

**Full deploy (8 steps):**

1. **Pre-flight checks** — validates tools, CLI profile, warehouse, catalog, app state
2. **Build frontend** — `npm ci` + `npm run build`
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

> For a full explanation of when OBO vs SP auth is used, see [Permissions & Authentication Model](#permissions--authentication-model) above.

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
| GSO job creation fails during deploy | Bundle deploy failed (CLI version, auth, or build issue) | Check `databricks bundle deploy -t app` output; ensure CLI >= 0.297.2 and `pip install build` |
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

## Dependency Security

All dependencies are pinned to exact versions to guard against supply chain attacks
(e.g. [CVE-2026-33634 / TeamPCP](https://www.kaspersky.com/blog/critical-supply-chain-attack-trivy-litellm-checkmarx-teampcp/55510/),
which targeted unpinned PyPI packages and GitHub Action tags).

### Lock files (always commit these)

| File | Covers | Tool |
|---|---|---|
| `uv.lock` | All root Python transitive deps with SHA256 hashes | uv |
| `packages/genie-space-optimizer/uv.lock` | GSO Python deps with SHA256 hashes | uv |
| `frontend/package-lock.json` | All frontend npm deps with SHA-512 integrity hashes | npm |
| `packages/genie-space-optimizer/bun.lock` | GSO UI deps | bun |

### Updating Python dependencies

```bash
# Upgrade one package (resolves latest compatible, updates uv.lock with new hashes)
uv lock --upgrade-package <package-name>

# Regenerate requirements.txt from the updated lock file
uv export --frozen --no-dev --no-hashes --format requirements-txt \
  | grep -v "^-e " > requirements.txt
echo "-e ./packages/genie-space-optimizer" >> requirements.txt

# Commit both
git add uv.lock requirements.txt
```

> **Do not edit `requirements.txt` manually.** It is generated from `uv.lock` and
> includes all transitive dependencies pinned to exact `==` versions. The generation
> command is documented at the top of the file.

### Updating npm dependencies

```bash
cd frontend
npm install <package>@<new-version>   # resolves and updates package-lock.json
# Then update package.json to exact version (remove the ^ prefix)
git add package.json package-lock.json  # always commit both together
```

### Why `npm ci` instead of `npm install` in deploys

`scripts/deploy.sh` uses `npm ci` for the frontend build step. Unlike `npm install`,
`npm ci`:
- Reads `package-lock.json` as the single source of truth (never updates it)
- Verifies SHA-512 integrity hashes for every installed package
- Fails loudly if `package.json` and `package-lock.json` are out of sync

If you update `frontend/package.json`, always run `npm install` locally to regenerate
`package-lock.json`, then commit both files.

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
