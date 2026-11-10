# Genie Workbench

Databricks App for creating, scoring, and optimizing Genie Agents. FastAPI backend + React/Vite frontend deployed together on Databricks Apps. It runs exclusively on the Databricks Apps platform — there is no local dev server.

## Critical Rules

- **DO NOT run `uvicorn` locally.** The app requires Databricks OBO auth, Lakebase PostgreSQL, and model-serving endpoints that are only available inside a Databricks App.
- **DO NOT run `databricks bundle init`.** It overwrites the project's `databricks.yml` and destroys the existing configuration.
- **DO NOT use `npm install` in build or deploy scripts** — always use `npm ci`. `npm install` can silently upgrade packages within `^` ranges; `npm ci` enforces the exact lockfile.
- **DO NOT edit `requirements.txt` manually.** It is generated from `uv.lock` as a pip-compatible reference but is excluded from deployment via `.databricksignore`. The platform uses `uv sync` (pyproject.toml + uv.lock) for hash-verified installs.
- **Backend logic has offline unit tests** in `backend/tests/` (`./scripts/test.sh`, which also runs the GSO suite) — run these locally. But integration/E2E testing (auth, Lakebase, serving endpoints) is done by deploying to a real Databricks workspace, not by running the server locally.
- **DO NOT run `pytest` without the dev extra.** `./scripts/test.sh` is the invocation; direct calls need `uv run --frozen --extra dev pytest`. Bare `uv run --frozen pytest` omits `pytest-asyncio`, which `asyncio_mode = "auto"` requires, and every async backend test fails on an uncollectable coroutine (look for `Unknown config option: asyncio_mode`; the count grows with the suite). Those failures are an invocation defect and must not be reported as pre-existing. Likewise never run `uv sync` from a workspace-member directory (e.g. `packages/genie-space-optimizer/`): it exact-syncs the shared root `.venv` to that member's deps and uninstalls `pytest-asyncio` while leaving `pytest`, which produces exactly that failure pattern.
- **`uv lock --check` fails structurally on this repo** — the GSO package version is dynamic (git-describe), so the resolved version changes on every commit and the check reports the lockfile as needing an update even on a clean tree. Expected, not drift. Verify the lockfile with `git status` on `uv.lock` instead, and never commit a `uv.lock` dirtied by a version-only re-resolve.

## Commands

```bash
# Python (local tooling only — not required for deploy)
uv sync --frozen --extra dev --all-packages  # Install from uv.lock (strict); repo root only — plain --frozen uninstalls pytest/pytest-asyncio
uv pip install -e .                      # Fallback: install without lock enforcement

# Frontend (from frontend/)
cd frontend && npm ci && npm run build   # Build for production (strict lockfile; runs tsc -b first)
cd frontend && npm run lint              # ESLint
cd frontend && npm test                  # Vitest (offline; CI runs this on every PR)
cd frontend && npx vitest run src/pages/SpaceList.test.tsx   # Single test file
# CI runs Node 20. On newer Node (seen on v25) the built-in localStorage shadows jsdom's and
# ~27 update-check tests fail; prefix NODE_OPTIONS=--no-experimental-webstorage to match CI.

# Deploy
./scripts/install.sh                     # First-time setup (interactive, creates .env.deploy)
./scripts/deploy.sh                      # Full deploy: build + sync + configure + redeploy
./scripts/deploy.sh --update             # Code-only update (faster, skips app creation)
./scripts/deploy.sh --destroy            # Tear down app and clean up jobs (see Gotchas for scope)
./scripts/deploy.sh --destroy --auto-approve  # Tear down without confirmation prompt
# Databricks notebook install path: clone into Databricks Git and run notebooks/install.py

# Dependency management
# requirements.txt is auto-generated from uv.lock — do not edit manually.
# After adding/bumping a Python dep in pyproject.toml:
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt > requirements.txt
python scripts/export_gso_job_requirements.py   # regenerate GSO job pins (requirements-job.txt)

# Offline unit tests (no running server) — backend/tests/ + GSO's tests/
./scripts/test.sh                 # Run BOTH suites (backend + genie-space-optimizer)
./scripts/test.sh -v              # Verbose
./scripts/test.sh -k scanner      # Run a single test file/selection (e.g. test_scanner.py)
./scripts/test.sh backend/tests   # Naming any path runs only that path
# Configured in pyproject.toml: testpaths=backend/tests, asyncio_mode=auto
#
# Use the script. Calling pytest directly needs `uv run --frozen --extra dev`:
# asyncio_mode=auto requires pytest-asyncio, and without the dev extra pytest
# warns "Unknown config option: asyncio_mode" and every async backend test fails
# on an uncollectable coroutine. Those failures are an invocation defect, not a code
# defect — do not chase them, and do not record them as pre-existing.
#
# CI (.github/workflows/test.yml) pins Python 3.12 for this suite: newer
# interpreters (3.14) have no wheel for a locked dep (whenever==0.7.3).

# Opt-in live suite (marked e2e/slow, outside testpaths, env-gated, never in CI).
# Runs real route code in-process via TestClient — no uvicorn. Runbook: scripts/e2e/mv_advisor_e2e.md
uv run --frozen --extra dev python -m pytest tests/e2e -m e2e

# Create-agent E2E against a DEPLOYED app (standalone script outside testpaths; test.sh/CI never run it).
# Sends a bearer token from `databricks auth token --profile <profile>`; no playwright needed.
GENIE_E2E_APP_URL=https://<app-url> GENIE_E2E_PROFILE=<profile> uv run --frozen python tests/test_e2e_deployed.py
```

## Architecture

```
backend/
  main.py                  # FastAPI app entry point, OBO middleware, static file serving
  models.py                # All Pydantic models (shared between routers/services)
  prompts.py               # Prompt templates for the create agent
  genie_creator.py         # Genie Agent creation logic (API calls, config assembly)
  sql_executor.py          # SQL execution via Databricks SQL warehouse
  app_version.py           # Installed version (repo-root VERSION file) + deploy-time build info
  _telemetry.py            # Databricks SDK telemetry constants (PRODUCT_VERSION is read from VERSION)
  routers/
    analysis.py            # /api/space/* (fetch, parse), /api/settings, /api/models, /api/debug/auth
    spaces.py              # /api/spaces/* (list, scan, history, star)
    admin.py               # /api/admin/* (dashboard, leaderboard, alerts)
    app_info.py            # /api/admin/version, /api/admin/update-check (app version + release-check setting)
    auth.py                # /api/auth/me
    create.py              # /api/create/* (agent chat, UC discovery, wizard)
    auto_optimize.py       # /api/auto-optimize/* (GSO engine proxy)
    vc_history.py, vc_mutations.py, vc_spaces.py  # /api/version-control/* — NOT in the static include list; see Version Control
    _validators.py         # Shared FastAPI path-param validators (e.g. RunId — uuid4-enforced)
  services/
    auth.py                # OBO auth (ContextVar), SP fallback, WorkspaceClient mgmt
    genie_client.py        # Databricks Genie API (fetch space, list spaces, query for SQL)
    scanner.py             # Rule-based IQ scoring engine (0-12, 12 checks, 3-tier maturity, UC-enriched)
    create_agent.py        # Multi-turn LLM agent for creating new Genie Agents
    create_agent_session.py # Session persistence for create agent (Lakebase)
    create_agent_tools.py  # Tool definitions for create agent (UC discovery, SQL, etc.)
    plan_builder.py        # Parallel plan generation — builds Genie Agent plans via concurrent LLM calls
    gso_lakebase.py        # GSO synced table reads from Lakebase PostgreSQL
    lakebase.py            # PostgreSQL persistence (asyncpg pool, in-memory fallback)
    llm_utils.py           # Backend LLM client (call_serving_endpoint) — OpenAI-compatible; GSO has its own
    llm_route.py           # Gateway vs classic routing for every LLM call (see LLM Calls)
    model_catalog.py       # Curated list + validation of user-selectable chat models (/api/models)
    uc_client.py           # Unity Catalog browsing (catalogs, schemas, tables)
    mv_suggest.py, mv_create.py, mv_entitlement.py  # Metric View Advisor (see below)
    join_advisor.py        # Join suggestions from UC metadata (Tables API)
    space_access.py        # Per-space access checks
    version_control/       # Version Control subsystem (contracts, ledger, coordination, platform/)
  version_control_ddl/     # Numbered SQL DDL for the Version Control Delta tables/volumes
  watch/                   # GenieWatch observability surface (see Key Patterns) — all routes under /api/watch/*
    routers/               # spaces, traffic_gaps, cost, usage, feedback, resources, settings, admin
    services/              # system_tables (SP-only reads), conversations_client, genie_client, uc_client, traffic_gaps
    models.py              # GenieWatch Pydantic models
  prompts/                 # Prompt templates for analysis
  prompts_create/          # Prompt templates for create agent (multi-file, modular)
  references/schema.md     # Genie Agent JSON schema reference
  tests/                   # Offline pytest unit suite (scanner, create agent, llm_utils, sql validation, config round trip) — run via ./scripts/test.sh
scripts/
  install.sh               # Guided first-time setup (creates .env.deploy, provisions resources)
  deploy.sh                # Build + bundle deploy (job) + app deploy (idempotent)
  deploy_lib/              # Shared Python deployment library used by notebooks/install.py
  preflight.sh             # Pre-deploy validation checks
  build.sh                 # Frontend build
  test.sh                  # Run the backend pytest suite (installs dev deps if missing)
  deploy-config.sh         # Shared deploy configuration/variables
  grant_permissions.py     # Grants required permissions for app resources
  setup_lakebase.py        # Automates Lakebase Autoscaling project, SP role, and grants
  setup_synced_tables.py   # Sets up GSO synced tables in Lakebase
notebooks/
  install.py               # Databricks-native installer using WorkspaceClient() and generated workspace source
frontend/
  src/
    App.tsx                # Root: five views — SpaceList | SpaceDetail | AdminDashboard | CreateSpace | HowItWorks
    lib/api.ts             # All API calls (fetch, SSE streaming helpers)
    types/index.ts         # TypeScript types mirroring backend Pydantic models
    components/            # UI components (analysis, optimization, etc.)
      auto-optimize/       # GSO pipeline UI (config, run history, patches, scores, etc.)
    pages/                 # SpaceList, SpaceDetail, AdminDashboard, HowItWorks, HistoryTab, IQScoreTab
    watch/                 # GenieWatch UI (own api.ts/types), lazy-loaded as AdminDashboard sub-tabs
    hooks/                 # useAnalysis, useTheme
  vite.config.ts           # Vite config with /api proxy to localhost:8000
packages/
  genie-space-optimizer/   # GSO engine: separate Python package deployed as a wheel
                           # Has its own pyproject.toml and shares the root uv.lock
                           # Has its own AGENTS.md (package layout, persistence, leakage rules) —
                           # read it before touching the package; it is not auto-loaded as CLAUDE.md
```

## Key Patterns

### Authentication (OBO)
On Databricks Apps, user identity flows via `x-forwarded-access-token` header. `OBOAuthMiddleware` in `main.py` stores the token in a `ContextVar`. All services call `get_workspace_client()` which returns the OBO client if set, otherwise the SP singleton. When an operation must run as the user and never as the SP (user-scoped reads, and OBO writes such as metric-view creation), use `require_obo_workspace_client()` instead: it raises `RuntimeError` rather than falling back. Some Genie API calls require SP auth (missing `genie` OAuth scope) — see `_is_scope_error()` fallback in `genie_client.py`.

### SSE Streaming
`/api/create/agent/chat` uses `StreamingResponse` with `text/event-stream` — a multi-turn agent with typed events (session, step, thinking, tool_call, tool_result, message_delta, message, created, updated, heartbeat, error, done) and 15s keepalive.

The frontend consumes it via manual `fetch` + `ReadableStream` in `lib/api.ts` (not EventSource). Buffer splitting on `\n\n`.

### Lakebase Persistence
`services/lakebase.py` uses asyncpg with graceful fallback to in-memory dicts when `LAKEBASE_HOST` is not set. Supports both provisioned Lakebase and Lakebase Autoscaling — for autoscaling, uses `client.postgres.get_endpoint()` to resolve DNS and `client.postgres.generate_database_credential()` for OAuth tokens. Schema and tables are created by the app at startup via `_ensure_schema()` (the SP owns everything it creates). Lakebase project, SP role, and database-level grants (CONNECT, CREATE) are automated by `scripts/setup_lakebase.py` for the local terminal path and by `scripts.deploy_lib.lakebase` for the notebook path.

### LLM Calls
There are two LLM client stacks, and you shouldn't add a third. Backend calls go through `llm_utils.call_serving_endpoint` (OpenAI-compatible). The GSO job has its own: `genie_space_optimizer/optimization/llm_client.py`, plus `optimizer_utils.py` and the embeddings in `leakage.py`. `backend/tests/test_llm_route_contract.py` allowlists the only files that may call a model directly. Both stacks pick the transport from `GENIE_LLM_ROUTE` through twin `llm_route.py` modules (`backend/services/` and GSO `optimization/`), which `backend/tests/test_llm_route_parity.py` keeps byte-identical. **`gateway` is the default** (Unity AI Gateway at `/ai-gateway/mlflow/v1/...`, with a `Databricks-Ai-Gateway-Request-Tags` header so cost shows up per component in `system.ai_gateway.usage`). On the gateway route, `gateway_model_name()` passes dotted names (UC BYOK ids, `system.ai.*`) through unchanged. It maps every other name to `system.ai.<name>`, stripping any `databricks-` prefix, so a plain custom serving-endpoint name only works on classic. Only an explicit `classic` sends chat to `/serving-endpoints/{name}/invocations` (classic embeddings keep the SDK `serving_endpoints.query`). Unknown values warn and stay on gateway, never silently falling back to classic. The route is also forwarded to every optimization run. Model configured via `LLM_MODEL` env var (default: `databricks-claude-sonnet-4-6`). `services/model_catalog.py` exposes a curated set of chat-compatible models via `/api/models` so the UI can let users override the model per Create Agent / Auto-Optimize run; `validate_chat_model()` guards those overrides (and accepts 3-level UC BYOK model ids only when `GENIE_BYOK_ENABLED=true` on the gateway route). MLflow tracing is optional — controlled by `MLFLOW_EXPERIMENT_ID`.

### GenieWatch (observability surface)
`backend/watch/` is a self-contained subsystem mounted under `/api/watch/*` (registered separately in `main.py`), surfaced in the frontend as lazy-loaded sub-tabs inside `AdminDashboard` (Spaces / Cost / Resources / Feedback / Settings). It reports per-space cost, usage, feedback, and executed-resource lineage by querying Databricks **system tables** (`system.query.history`, `system.billing.usage`, `system.access.audit`, `system.access.table_lineage`). System tables are **not OBO-readable**, so `watch/services/system_tables.py` always runs as the **service principal** (`get_service_principal_client()`) and caches results in an in-process TTL cache. The SP needs `USE CATALOG system` + schema/SELECT grants — `scripts/grant_permissions.py` is the source of truth for that grant list. The watch frontend lives in `frontend/src/watch/` with its own `api.ts` (base `/api/watch`) and types, kept namespaced to avoid colliding with the workbench API surface.

### Analysis
IQ Scan (`scanner.py`) is the only analysis path — rule-based, instant, 0-12 score with 12 checks and 3-tier maturity (Not Ready / Ready to Optimize / Trusted). Before scoring, `scan_space()` enriches the config with upstream Unity Catalog table/column descriptions so checks 2–3 reflect metadata that exists in UC even if not inlined in the Genie Agent config. `routers/analysis.py` only handles space fetching/parsing and settings — it does not perform analysis.

### Auto-Optimize
`auto_optimize.py` + the GSO engine in `packages/genie-space-optimizer/` implement the benchmark-driven optimization pipeline. (The legacy Quick Fix path — an LLM fix agent applying JSON patches from scan findings — was removed; Auto-Optimize is the only optimization path.) Runs start only via `POST /api/auto-optimize/trigger` (`TriggerRequest`); extend it rather than adding a parallel start endpoint.

GSO job invariants (enforced by tests such as `test_phase7_job_dag.py` — don't weaken them):
- The job is a **linear four-task notebook DAG**: `intake_and_snapshot → benchmark_qc_and_repair → optimize → publish_and_audit`. No extra tasks, condition tasks, `run_if` edges, or wheel entry points.
- Cross-task state passes through Delta tables keyed by `run_id` (`genie_opt_*`, defined in `optimization/ddl.py`) — **never `dbutils.jobs.taskValues`**. Schema evolution is additive only.
- Job parameters are read with `dbutils.widgets`. A new parameter must be added to **all four** places at once: root `databricks.yml`, `packages/genie-space-optimizer/databricks.yml`, `scripts/deploy_lib/gso_job.py`, and the `run_now` map in `genie_space_optimizer/backend/job_launcher.py`.
- The job is meant to run as the app **service principal**. The bundle leaves `run_as` unset, and `main.py`'s `_ensure_gso_job_run_as()` binds it at startup on a best-effort basis (a failure only logs a warning). Inference-visible text writes go through `optimization/leakage.py` (benchmark questions/expected SQL must not leak into the optimized space). Extend it rather than writing a second scanner.

### Metric View Advisor
`mv_suggest.py` / `mv_create.py` / `mv_entitlement.py` (plus GSO phases inside the `optimize` task) suggest, create, and attach UC metric views. `.cursor/rules/mv-advisor.mdc` (`alwaysApply`) is the full rulebook. It is kept byte-identical with the fenced block in `docs/design/mv-advisor-playbook.md`. Design sources: `docs/design/metric-view-suggestion-engine-pov.md` as amended by `docs/design/mv-advisor-gap-report.md` (the gap report wins on conflicts, and must be refreshed in the same commit as any code it quotes). The rules that matter most:
- `CREATE VIEW ... WITH METRICS` runs **only in the FastAPI backend under OBO** (`require_obo_workspace_client`), never in the SP-run job, and only against a recorded, re-verified consent targeting the consented schema. On any entitlement/consent mismatch, **downgrade to `suggest_only`**, never upgrade or switch schema. Don't reuse `GET /permissions/{space_id}` for entitlement (it probes the SP, not the user).
- Metric-view code in the job runs as gated, try/except-wrapped phases inside the `optimize` task: on failure, persist a status row and continue. All benchmark eval goes through the `EvalRunner` seam (`optimization/eval_runner.py`). Rollback is whole-snapshot revert (`integration/revert.py`). The job never drops a created UC view (outside sandbox mode). Dropping one is an explicit backend OBO endpoint (`POST /api/auto-optimize/mv/created/{suggestion_id}/drop`). The only automatic drop is the backend's compensating `DROP VIEW IF EXISTS` of a half-made view whose post-create check (`_confirm_metric_view`) failed during that same create (`mv_create.py`).
- Update existing metric views with `ALTER VIEW ... AS $$...$$`, never `CREATE OR REPLACE` (that drops UC grants). No literals, sample values, PII, or verbatim benchmark questions in MV metadata, DDL comments, or logs.

### Version Control
`backend/services/version_control/` (contract `VC/1.0`) snapshots Genie spaces for history/restore and cross-workspace promotion; DDL lives in `backend/version_control_ddl/`. It is **feature-gated**: `main.py` builds `app.state.vc_observe` from the `VC_OBSERVE_*` env vars and mounts the `/api/version-control/*` routers (`routers/vc_*.py`, via `platform/app_observe.mount_observe_routers`) only when that runtime resolves. `app.yaml` also declares `VC_PROMOTION_ENABLED` / `VC_OPTIMIZER_APPLY_ENABLED` / `VC_RECONCILE_ENABLED` (all `false`), but no code reads them today, so flipping them changes nothing. The current direction is observe-only: VC snapshots around optimizer runs and must not modify optimizer internals (see `version_control/OBSERVE_ONLY_PIVOT_PLAN.md`).

## Environment Variables

Defined in `app.yaml`. Many values there are `__PLACEHOLDER__` tokens (e.g. `__LLM_MODEL__`, `__GSO_JOB_ID__`) that `deploy.sh` / the notebook installer substitute at deploy time, so leave the tokens alone in the repo copy. Key ones:
- `SQL_WAREHOUSE_ID` — from app resource `sql-warehouse`
- `LLM_MODEL` — model / serving endpoint name
- `GENIE_LLM_ROUTE` — `gateway` (default) or `classic`; `GENIE_BYOK_ENABLED` — accept UC BYOK model ids (gateway only)
- `VC_OBSERVE_*` — Version Control wiring; the `VC_*_ENABLED` flags are declared but not read by any code (see Version Control)
- `LAKEBASE_HOST`, `LAKEBASE_PORT`, `LAKEBASE_DATABASE`, `LAKEBASE_INSTANCE_NAME` — Lakebase config
- `MLFLOW_EXPERIMENT_ID` — enables MLflow tracing (validated at startup, cleared if invalid)
- `GENIE_TARGET_DIRECTORY` — where new spaces are created (default `/Shared/`)
- `DEV_USER_EMAIL` — local dev only
- `GSO_CATALOG`, `GSO_SCHEMA` — Unity Catalog location for optimization tables
- `GSO_JOB_ID` — auto-injected by deploy script from bundle state
- `GSO_WAREHOUSE_ID` — SQL warehouse for GSO queries (from app resource)

Local terminal deploy config uses `.env.deploy` (created by `scripts/install.sh` from `.env.deploy.template`). The Databricks notebook installer uses widgets in `notebooks/install.py` and writes a patched `app.yaml` only into the generated workspace source folder.

## Dev/Test Workflow

There is no local dev server — all testing is done by syncing code to Databricks and redeploying:

1. Edit code locally
2. Run `./scripts/deploy.sh --update` for local terminal installs, or rerun `notebooks/install.py` for notebook installs
3. Test in the deployed Databricks App

Do NOT suggest running `uvicorn` or `npm run dev` locally. The app depends on Databricks-managed resources (OBO auth, Lakebase, serving endpoints) that aren't available outside a Databricks App environment.

## Dependency Security Policy

This project pins all direct dependencies to exact versions and resolves
transitive dependencies through lockfiles with integrity hashes.

**Policy:**

- `pyproject.toml` and `package.json` use only exact versions (`==1.2.3` for
  Python, `1.2.3` for npm). No `^`, `~`, `>=`, `<=`, `<`, `>`, `~=`, or `*`.
- The Python and frontend lockfiles below must validate (`uv lock --check`,
  `npm ci --dry-run --ignore-scripts`) before any deploy.
- `mlflow` (and `mlflow-skinny` / `mlflow-tracing`) MUST resolve to the same
  version across the workspace (today: `3.11.1`).
- `npm` install paths must succeed without `--legacy-peer-deps`. If a peer
  conflict appears, fix the manifest (bump the offending pin to a version
  inside the peer-dep range) instead of reaching for the escape-hatch flag.

**Lock files — always commit these:**

| File | Covers | Verification |
|---|---|---|
| `uv.lock` | Workspace-wide Python transitive deps | SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps | SHA-512 integrity |

The workspace uses a single root `uv.lock` for both root and
`packages/genie-space-optimizer/` — uv writes there for any
workspace-member invocation. There is intentionally no per-package `uv.lock`.

**Updating a Python dep:**

```bash
# 1. Edit the exact version in pyproject.toml (root or GSO).
# 2. Refresh the lock.
uv lock --upgrade-package <package-name>
# 3. Regenerate requirements.txt (pip-compatible reference).
uv export --frozen --no-dev --no-hashes --format requirements-txt \
  | grep -v "^-e " > requirements.txt
echo "-e ./packages/genie-space-optimizer" >> requirements.txt
# 4. Regenerate the GSO job's locked transitive pins.
python scripts/export_gso_job_requirements.py
git add pyproject.toml packages/genie-space-optimizer/pyproject.toml uv.lock requirements.txt \
  packages/genie-space-optimizer/requirements-job.txt
```

**GSO job pins (`packages/genie-space-optimizer/requirements-job.txt`):** the GSO
wheel only pins direct deps, so the serverless job installs this generated file
alongside it (`-r` in every job environment spec) to get uv.lock's transitive
versions. Without it, each run re-resolves against the live index and an upstream
release can make the install unsatisfiable (this happened with `huggingface-hub`
→ `httpx2`, surfacing as a misleading `ERROR_NO_SUCH_FILE_OR_DIRECTORY`).
Packages already installed in the serverless base environment are excluded so
the job never replaces base-image packages Spark Connect relies on; the list
comes from the committed snapshot `serverless-env-v4-base-packages.txt`. When the
job's `environment_version` changes, refresh that snapshot (`pip list
--format=freeze` in a fresh serverless notebook on the new version) and rerun
the script. Databricks also patches base images in place without bumping the
version (certifi went 2025.1.31 → 2026.10.19 on v4), so re-capture the snapshot
before using it to decide whether the job needs a package pinned. Set the
notebook's environment version explicitly: new notebooks may default to a newer
version than the job's, and `notebooks/install.py` itself runs on v5.
`backend/tests/test_gso_job_requirements.py` fails if the file drifts from uv.lock.

**Updating a frontend npm dep:**

```bash
cd frontend
npm install <package>@<exact-version> --save-exact
git add package.json package-lock.json
```

## Gotchas

- **`CLAUDE.md` is a symlink to `AGENTS.md`** — one source of truth shared by Claude Code and other agent tools. Edit `AGENTS.md`; never replace the symlink with a separate file.
- **frontend/dist/ is gitignored but NOT databricksignored** — the built React app must be synced to workspace for deployment. Build before `databricks sync`.
- **`.databricksignore` excludes `*.md`** but explicitly re-includes `backend/references/schema.md` (needed at runtime by create agent and analysis prompts).
- **OBO ContextVar and streaming** — for SSE endpoints, the ContextVar is NOT cleared after `call_next` because the response streams lazily. Streaming handlers stash the token on `request.state` and re-set it inside the generator.
- **IQ Scan is the only analysis path** — `scanner.py` runs 12 rule-based checks via `/api/spaces/{id}/scan`. `routers/analysis.py` only handles space fetching/parsing (`/api/space/fetch`, `/api/space/parse`) and settings — it does not perform analysis.
- **Vite proxy** — dev frontend at :5173 proxies `/api` to :8000. In production, FastAPI serves static files from `frontend/dist/` directly.
- **Python 3.11+** required (`pyproject.toml`). Uses `uv` for dependency management (`uv.lock` present).
- **Root `package.json`** exists solely as a build hook for Databricks Apps. `postinstall` is a no-op. `build` checks for pre-built `frontend/dist/index.html` — if present (uploaded by `deploy.sh`), skips the rebuild; if dist is missing, runs `cd frontend && npm ci && npm run build`. This keeps CLI deploy fast while allowing workspace-folder deploys from fresh clones.
- **Two install paths** — local terminal installs use `scripts/install.sh`/`scripts/deploy.sh`, with the optimization job managed by DABs (`databricks bundle deploy -t app`). Notebook installs use `notebooks/install.py`, generate source under `/Workspace/Users/<user>/.genie-workbench-deploy/<app-name>/app`, and manage the GSO job through SDK/Jobs API reset/update semantics. Do NOT run `databricks bundle deploy -t dev` for production — it creates prefixed orphan jobs.
- **Databricks CLI >= 0.297.2 required** — `preflight.sh` validates this automatically.
- **`--destroy` does not remove all resources** — it deletes the app and jobs but leaves behind: Lakebase data (`genie` schema), UC schema/tables (`<catalog>.genie_space_optimizer`), Genie Agent SP permissions, MLflow experiments, and synced tables. Clean these up manually if needed.
- **`frontend/dist/` must be explicitly uploaded** with `databricks workspace import-dir` because `databricks sync --full` only uploads non-gitignored files.
- **`requirements.txt` is databricksignored** — the platform uses `uv sync` instead of `pip install`. If you see pip dependency conflicts, verify `requirements.txt` is in `.databricksignore`.
- **`MLFLOW_EXPERIMENT_ID` is workspace-specific** — the app validates it at startup and silently disables tracing if the experiment doesn't exist.
- **Lakebase state is app-instance scoped** — keep the app name stable and update through the same install path (`./scripts/deploy.sh --update` locally, or rerun `notebooks/install.py`). If creating a new app instance, use a fresh Lakebase project; reusing an older app's Lakebase project can leave `genie` tables/sequences owned by the old app SP.

## Platform Build Strategy

The Databricks Apps platform detects `package.json` at the root and runs `npm install` then `npm run build`. To avoid cross-platform failures (macOS lockfile vs Linux container) and redundant rebuilds, the root `package.json` is configured as follows:

- **`postinstall`**: No-op. It does not invoke nested npm commands during `npm install`.
- **`build`**: Checks for pre-built `frontend/dist/index.html`. If present (uploaded by `deploy.sh`), skips the rebuild. If dist is missing, runs `cd frontend && npm ci && npm run build`.
- **`start`**: Runs uvicorn (though `app.yaml` `command` takes precedence).

Python dependencies use `uv sync` on the platform (because `requirements.txt` is excluded from `.databricksignore`). This gives a clean venv with SHA256-verified hashes, avoiding conflicts with pre-installed platform packages (dash, gradio, streamlit, etc.).

## Code Style

- Backend: Python, Pydantic models, FastAPI routers, no class-based views
- Frontend: React 19 + TypeScript + Tailwind CSS v4 + Vite 7, functional components only
- UI primitives in `frontend/src/components/ui/` (button, card, badge, etc.) using `class-variance-authority`
- Path alias `@` maps to `frontend/src/` (configured in `vite.config.ts` and `tsconfig.app.json`)
- All API routes prefixed with `/api`
- Pydantic models in `backend/models.py`, TypeScript mirrors in `frontend/src/types/index.ts` — keep in sync

## Documentation

The canonical documentation is the Docusaurus site under `docs/docs/` (see below).
It is the **only** source of user-facing documentation truth — when modifying auth, agents, or
optimization code, consult the relevant page there and update it in the same change.

There is no `docs/archives/` directory. Earlier flat-markdown snapshots and dated
one-off artifacts (run postmortems, investigations) were deleted; the site fully
carries the content worth keeping. Do not recreate a parallel copy of the docs
outside `docs/docs/` — that split is what let the two versions drift before.

`docs/design/` holds internal design records for the Metric View Advisor (POV,
gap report, playbook, exposure matrix). They are engineering contracts, not user
docs, and are not published (see Metric View Advisor above).

`docs/debug-prompt.md` is the other exception outside `docs/docs/`: the verbatim
copy-paste prompt for the GSO Run Debugger. Its SQL is guarded by
`packages/genie-space-optimizer/tests/unit/test_debug_prompt_contract.py`
(read-only statements, current tables only, no retired columns), which pins that
path — so edit that file directly.

It is **not** duplicated into the site. `docs/docs/reference/gso-run-debugger.mdx`
embeds it at build time: `docusaurus.config.ts` reads the file with `fs` and
exposes it as `customFields.debugPrompt`, which the page renders in a
`<CodeBlock>` with a copy button. A plain `import '...md?raw'` does not work —
Docusaurus's MDX loader claims every `.md` import and compiles it to JS. Editing
`docs/debug-prompt.md` updates the page on the next build; there is no second
copy to keep in sync.

### Docs site (`docs/`)

A Docusaurus 3 site (Databricks-branded, dark-default, Tailwind + DM Sans/Mono)
publishes the docs to GitHub Pages. It is a **separate Node project** with its own
`package.json` and `package-lock.json` — independent of the `frontend/` app.

```bash
cd docs
npm install            # first time (or npm ci against the lockfile)
npm start              # dev server at http://localhost:3000/databricks-genie-workbench/
npm run build          # production build into docs/build
npm run serve          # serve the production build locally
npm run typecheck      # tsc
```

- Content lives in `docs/docs/`, grouped into `getting-started/`, `features/`,
  `platform/`, `reference/`. The sidebar is autogenerated from the folder tree
  (`_category_.json` sets each category's label/order).
- `src/pages/index.tsx` — custom landing page; `src/css/custom.css` — Databricks
  color tokens and font overrides; `docusaurus.config.ts` — navbar, plugins
  (Mermaid, lunr search, image zoom, llms-txt). `url`/`baseUrl` derive from the
  `ORG`/`REPO` constants (`https://<org>.github.io/databricks-genie-workbench/`).
- **Deployment is Pages-via-Actions.** `.github/workflows/deploy-docs.yml` runs on
  every push to `main` that touches `docs/**`: it runs `npm ci && npm run build`
  in `docs/` and publishes `docs/build` with `actions/deploy-pages` (the deploy job
  uses the `databricks-solutions-protected-runner-group` runners). `npm run deploy`
  (`docusaurus deploy`, which pushes to a `gh-pages` branch) is not part of this flow.

The site originated as a port of earlier flat markdown files. Those snapshots have
been removed — `docs/docs/` is the single canonical source, so edit it directly
rather than looking for a second copy to keep in sync.

## References

**Before modifying any Genie Agent configuration, schema handling, or space creation/optimization code, you MUST `WebFetch` and read the relevant references below.**

- **Genie Agent `serialized_space` schema**: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field — authoritative field names for the Genie API. The create agent prompts (`backend/prompts.py`, `backend/prompts_create/`) and local schema reference (`backend/references/schema.md`) must match this.
  - Read before modifying: `create_agent.py`, `genie_client.py`, `references/schema.md`
- **Genie Agent validation rules**: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space — ID format (32-char lowercase hex), sorting requirements, uniqueness constraints, size limits.
  - Read before modifying: `genie_creator.py`, `create_agent_tools.py`
- **Genie Agent best practices**: https://docs.databricks.com/aws/en/genie/best-practices — official guidance on space design, table selection, instructions, and SQL snippets.
  - Read before modifying: `scanner.py` (scoring rules), `prompts_create/`, `plan_builder.py`
- **GSL instruction schema (near-term)**: `docs/docs/platform/gsl-instruction-schema.md` — section vocabulary and format rules for `instructions.text_instructions[0].content` that the Create Agent must follow. You MUST read this before modifying Create Agent prompts.
  - Read before modifying: `backend/services/plan_builder.py` (Create Agent parallel-generation prompts), `backend/prompts_create/_plan.py` (Create Agent plan-step prompt template), `backend/services/create_agent_tools.py`
