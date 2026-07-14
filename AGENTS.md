# Genie Workbench

Databricks App for creating, scoring, and optimizing Genie Spaces. FastAPI backend + React/Vite frontend deployed together on Databricks Apps. It runs exclusively on the Databricks Apps platform — there is no local dev server.

## Critical Rules

- **DO NOT run `uvicorn` locally.** The app requires Databricks OBO auth, Lakebase PostgreSQL, and model-serving endpoints that are only available inside a Databricks App.
- **DO NOT run `databricks bundle init`.** It overwrites the project's `databricks.yml` and destroys the existing configuration.
- **DO NOT use `npm install` in build or deploy scripts** — always use `npm ci`. `npm install` can silently upgrade packages within `^` ranges; `npm ci` enforces the exact lockfile.
- **DO NOT edit `requirements.txt` manually.** It is generated from `uv.lock` as a pip-compatible reference but is excluded from deployment via `.databricksignore`. The platform uses `uv sync` (pyproject.toml + uv.lock) for hash-verified installs.
- **Backend logic has offline unit tests** in `backend/tests/` (`./scripts/test.sh`) — run these locally. But integration/E2E testing (auth, Lakebase, serving endpoints) is done by deploying to a real Databricks workspace, not by running the server locally.

## Commands

```bash
# Python (local tooling only — not required for deploy)
uv sync --frozen                         # Install from uv.lock (strict)
uv pip install -e .                      # Fallback: install without lock enforcement

# Frontend (from frontend/)
cd frontend && npm ci && npm run build   # Build for production (strict lockfile)
cd frontend && npm run lint              # ESLint

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

# Backend unit tests (offline, no running server — pytest in backend/tests/)
./scripts/test.sh                 # Run full backend suite (installs dev deps if missing)
./scripts/test.sh -v              # Verbose
./scripts/test.sh -k scanner      # Run a single test file/selection (e.g. test_scanner.py)
# Configured in pyproject.toml: testpaths=backend/tests, asyncio_mode=auto

# Integration/E2E tests (require running backend at localhost:8000)
python tests/test_e2e_local.py    # E2E create agent tests
python tests/test_full_schema.py  # Schema validation
# Deployed E2E tests require: pip install playwright && playwright install chromium
python tests/test_e2e_deployed.py
```

## Architecture

```
backend/
  main.py                  # FastAPI app entry point, OBO middleware, static file serving
  models.py                # All Pydantic models (shared between routers/services)
  prompts.py               # Prompt templates for analysis/fix agent
  genie_creator.py         # Genie Space creation logic (API calls, config assembly)
  sql_executor.py          # SQL execution via Databricks SQL warehouse
  _telemetry.py            # Databricks SDK telemetry constants (PRODUCT_VERSION — keep in sync with pyproject.toml)
  routers/
    analysis.py            # /api/space/* (fetch, parse), /api/settings, /api/models, /api/debug/auth
    spaces.py              # /api/spaces/* (list, scan, history, star, fix)
    admin.py               # /api/admin/* (dashboard, leaderboard, alerts)
    auth.py                # /api/auth/me
    create.py              # /api/create/* (agent chat, UC discovery, wizard)
    auto_optimize.py       # /api/auto-optimize/* (GSO engine proxy)
    _validators.py         # Shared FastAPI path-param validators (e.g. RunId — uuid4-enforced)
  services/
    auth.py                # OBO auth (ContextVar), SP fallback, WorkspaceClient mgmt
    prompt_registry.py     # Thin re-export of genie_space_optimizer.common.prompt_registry (shared probe — see Key Patterns)
    genie_client.py        # Databricks Genie API (fetch space, list spaces, query for SQL)
    scanner.py             # Rule-based IQ scoring engine (0-12, 12 checks, 3-tier maturity, UC-enriched)
    fix_agent.py           # LLM agent (Quick Fix in UI) that generates JSON patches and applies via Genie API
    create_agent.py        # Multi-turn LLM agent for creating new Genie Spaces
    create_agent_session.py # Session persistence for create agent (Lakebase)
    create_agent_tools.py  # Tool definitions for create agent (UC discovery, SQL, etc.)
    plan_builder.py        # Parallel plan generation — builds Genie Space plans via concurrent LLM calls
    gso_lakebase.py        # GSO synced table reads from Lakebase PostgreSQL
    lakebase.py            # PostgreSQL persistence (asyncpg pool, in-memory fallback)
    llm_utils.py           # OpenAI-compatible LLM client via Databricks serving endpoints
    model_catalog.py       # Curated list + validation of user-selectable chat serving endpoints (/api/models)
    uc_client.py           # Unity Catalog browsing (catalogs, schemas, tables)
  watch/                   # GenieWatch observability surface (see Key Patterns) — all routes under /api/watch/*
    routers/               # spaces, cost, usage, feedback, resources, settings, admin
    services/              # system_tables (SP-only reads), conversations_client, genie_client, uc_client
    models.py              # GenieWatch Pydantic models
  prompts/                 # Prompt templates for analysis
  prompts_create/          # Prompt templates for create agent (multi-file, modular)
  references/schema.md     # Genie Space JSON schema reference
  tests/                   # Offline pytest unit suite (scanner, fix/create agent, llm_utils, sql validation) — run via ./scripts/test.sh
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
    components/            # UI components (analysis, optimization, fix agent, etc.)
      auto-optimize/       # GSO pipeline UI (24 components: config, run history, patches, scores, etc.)
    pages/                 # SpaceList, SpaceDetail, AdminDashboard, HowItWorks, HistoryTab, IQScoreTab
    watch/                 # GenieWatch UI (own api.ts/types), lazy-loaded as AdminDashboard sub-tabs
    hooks/                 # useAnalysis, useTheme
  vite.config.ts           # Vite config with /api proxy to localhost:8000
packages/
  genie-space-optimizer/   # GSO engine: separate Python package deployed as a wheel
                           # Has its own pyproject.toml, uv.lock, package.json, package-lock.json
```

## Key Patterns

### Authentication (OBO)
On Databricks Apps, user identity flows via `x-forwarded-access-token` header. `OBOAuthMiddleware` in `main.py` stores the token in a `ContextVar`. All services call `get_workspace_client()` which returns the OBO client if set, otherwise the SP singleton. Some Genie API calls require SP auth (missing `genie` OAuth scope) — see `_is_scope_error()` fallback in `genie_client.py`.

### SSE Streaming
Two endpoints use `StreamingResponse` with `text/event-stream`:
- `/api/spaces/{id}/fix` — fix agent patches (10s keepalive)
- `/api/create/agent/chat` — multi-turn agent with typed events (session, step, thinking, tool_call, tool_result, message_delta, message, created, updated, heartbeat, error, done) and 15s keepalive

Frontend consumes these via manual `fetch` + `ReadableStream` in `lib/api.ts` (not EventSource). Buffer splitting on `\n\n`.

### Lakebase Persistence
`services/lakebase.py` uses asyncpg with graceful fallback to in-memory dicts when `LAKEBASE_HOST` is not set. Supports both provisioned Lakebase and Lakebase Autoscaling — for autoscaling, uses `client.postgres.get_endpoint()` to resolve DNS and `client.postgres.generate_database_credential()` for OAuth tokens. Schema and tables are created by the app at startup via `_ensure_schema()` (the SP owns everything it creates). Lakebase project, SP role, and database-level grants (CONNECT, CREATE) are automated by `scripts/setup_lakebase.py` for the local terminal path and by `scripts.deploy_lib.lakebase` for the notebook path.

### LLM Calls
All LLM calls go through Databricks model serving endpoints using OpenAI-compatible API. Model configured via `LLM_MODEL` env var (default: `databricks-claude-sonnet-4-6`). `services/model_catalog.py` exposes a curated set of chat-compatible endpoints via `/api/models` so the UI can let users override the model per Create Agent / Auto-Optimize run; `validate_chat_model()` guards those overrides. MLflow tracing is optional — controlled by `MLFLOW_EXPERIMENT_ID`.

### GenieWatch (observability surface)
`backend/watch/` is a self-contained subsystem mounted under `/api/watch/*` (registered separately in `main.py`), surfaced in the frontend as lazy-loaded sub-tabs inside `AdminDashboard` (Spaces / Cost / Resources / Feedback / Settings). It reports per-space cost, usage, feedback, and executed-resource lineage by querying Databricks **system tables** (`system.query.history`, `system.billing.usage`, `system.access.audit`, `system.access.table_lineage`). System tables are **not OBO-readable**, so `watch/services/system_tables.py` always runs as the **service principal** (`get_service_principal_client()`) and caches results in an in-process TTL cache. The SP needs `USE CATALOG system` + schema/SELECT grants — `scripts/grant_permissions.py` is the source of truth for that grant list. The watch frontend lives in `frontend/src/watch/` with its own `api.ts` (base `/api/watch`) and types, kept namespaced to avoid colliding with the workbench API surface.

### Analysis
IQ Scan (`scanner.py`) is the only analysis path — rule-based, instant, 0-12 score with 12 checks and 3-tier maturity (Not Ready / Ready to Optimize / Trusted). Before scoring, `scan_space()` enriches the config with upstream Unity Catalog table/column descriptions so checks 2–3 reflect metadata that exists in UC even if not inlined in the Genie Space config. `routers/analysis.py` only handles space fetching/parsing and settings — it does not perform analysis.

### Two Separate Optimization Paths
- **Quick Fix** (`fix_agent.py`): triggered from scan findings, auto-applies JSON patches
- **Auto-Optimize** (`auto_optimize.py` + GSO engine in `packages/genie-space-optimizer/`): full benchmark-driven optimization pipeline. They're independent.

## Environment Variables

Defined in `app.yaml`. Key ones:
- `SQL_WAREHOUSE_ID` — from app resource `sql-warehouse`
- `LLM_MODEL` — serving endpoint name
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
- All three lockfiles below must validate (`uv lock --check`,
  `npm ci --dry-run --ignore-scripts`) before any deploy.
- `mlflow` (and `mlflow-skinny` / `mlflow-tracing`) MUST resolve to the same
  version across the workspace (today: `3.11.1`).
- `npm` install paths must succeed without `--legacy-peer-deps`. If a peer
  conflict appears, fix the manifest (bump the offending pin to a version
  inside the peer-dep range) instead of reaching for the escape-hatch flag.
- `uv.lock` must resolve against **public PyPI** (`https://pypi.org/simple`),
  pinned as the default index in the root `pyproject.toml` so it overrides any
  machine-level registry config. Internal registry URLs must never appear in
  the lockfile — this is a public repo. On networks that block pypi.org,
  run `uv lock` from a machine or environment with public PyPI access; do not
  re-lock through an internal proxy.

**Lock files — always commit these:**

| File | Covers | Verification |
|---|---|---|
| `uv.lock` | Workspace-wide Python transitive deps | SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps | SHA-512 integrity |
| `packages/genie-space-optimizer/package-lock.json` | GSO UI npm deps | SHA-512 integrity |

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
git add pyproject.toml packages/genie-space-optimizer/pyproject.toml uv.lock requirements.txt
```

**Updating an npm dep:**

```bash
cd <frontend|packages/genie-space-optimizer>
npm install <package>@<exact-version> --save-exact
git add package.json package-lock.json
```

## Gotchas

- **`CLAUDE.md` is a symlink to `AGENTS.md`** — one source of truth shared by Claude Code and other agent tools. Edit `AGENTS.md`; never replace the symlink with a separate file.
- **`backend/services/prompt_registry.py` is a re-export** — the real implementation lives in `genie_space_optimizer.common.prompt_registry` so the GSO job (no `backend` on its path) and the Workbench webapp share one source of truth. Import from either path; edit the GSO copy.
- **frontend/dist/ is gitignored but NOT databricksignored** — the built React app must be synced to workspace for deployment. Build before `databricks sync`.
- **`.databricksignore` excludes `*.md`** but explicitly re-includes `backend/references/schema.md` (needed at runtime by create agent and analysis prompts).
- **OBO ContextVar and streaming** — for SSE endpoints, the ContextVar is NOT cleared after `call_next` because the response streams lazily. Streaming handlers stash the token on `request.state` and re-set it inside the generator.
- **IQ Scan is the only analysis path** — `scanner.py` runs 12 rule-based checks via `/api/spaces/{id}/scan`. `routers/analysis.py` only handles space fetching/parsing (`/api/space/fetch`, `/api/space/parse`) and settings — it does not perform analysis.
- **Two separate optimization paths** — Quick Fix (`fix_agent.py`, from scan findings, auto-applies JSON patches) and Auto-Optimize (`auto_optimize.py` + GSO engine in `packages/genie-space-optimizer/`, full benchmark-driven optimization pipeline). They're independent.
- **Vite proxy** — dev frontend at :5173 proxies `/api` to :8000. In production, FastAPI serves static files from `frontend/dist/` directly.
- **Python 3.11+** required (`pyproject.toml`). Uses `uv` for dependency management (`uv.lock` present).
- **Root `package.json`** exists solely as a build hook for Databricks Apps. `postinstall` is a no-op. `build` checks for pre-built `frontend/dist/index.html` — if present (uploaded by `deploy.sh`), skips the rebuild; if dist is missing, runs `cd frontend && npm ci && npm run build`. This keeps CLI deploy fast while allowing workspace-folder deploys from fresh clones.
- **Two install paths** — local terminal installs use `scripts/install.sh`/`scripts/deploy.sh`, with the optimization job managed by DABs (`databricks bundle deploy -t app`). Notebook installs use `notebooks/install.py`, generate source under `/Workspace/Users/<user>/.genie-workbench-deploy/<app-name>/app`, and manage the GSO job through SDK/Jobs API reset/update semantics. Do NOT run `databricks bundle deploy -t dev` for production — it creates prefixed orphan jobs.
- **Databricks CLI >= 0.297.2 required** — `preflight.sh` validates this automatically.
- **`--destroy` does not remove all resources** — it deletes the app and jobs but leaves behind: Lakebase data (`genie` schema), UC schema/tables (`<catalog>.genie_space_optimizer`), Genie Space SP permissions, MLflow experiments, and synced tables. Clean these up manually if needed.
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

The canonical documentation is the Docusaurus site under `docs/` (see below).
Earlier flat-markdown docs are retained as historical snapshots under `docs/archives/`:

- `docs/archives/00-index.md` — Documentation hub and table of contents
- `docs/archives/03-authentication-and-permissions.md` — Deep dive on OBO + SP dual auth model
- `docs/archives/04-create-agent.md` — Create Agent: multi-turn tool-calling flow
- `docs/archives/07-auto-optimize.md` — GSO optimization pipeline (6-stage DAG)
- `docs/archives/appendices/A-api-reference.md` — All API endpoints with auth identity

See `docs/archives/00-index.md` for the full listing. When modifying auth, agents, or
optimization code, consult the relevant doc for design rationale.

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
- **Deployment is in flux.** There is currently no `gh-pages` branch and no docs
  CI workflow (the GitHub Actions docs-deploy workflow was removed). GitHub Pages
  is presently disabled at the org level; enabling Pages-via-Actions is a pending
  infra request. Until that lands, the site is built locally (`npm run build`) and
  previewed from the `documentation` branch. `npm run deploy` (`docusaurus deploy`)
  pushes the build to `gh-pages` once Pages is re-enabled.

**The site originated as a port of the flat docs, now archived under `docs/archives/`.**
The Docusaurus site under `docs/docs/` is the canonical source — edit it directly.
The `docs/archives/*.md` files are frozen historical snapshots and are no longer kept in
sync (e.g. `docs/docs/features/create-agent.md` supersedes
`docs/archives/04-create-agent.md`).

## References

**Before modifying any Genie Space configuration, schema handling, or space creation/optimization code, you MUST `WebFetch` and read the relevant references below.**

- **Genie Space `serialized_space` schema**: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field — authoritative field names for the Genie API. The fix agent prompt (`backend/prompts.py`) and local schema reference (`backend/references/schema.md`) must match this.
  - Read before modifying: `fix_agent.py`, `create_agent.py`, `genie_client.py`, `references/schema.md`
- **Genie Space validation rules**: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space — ID format (32-char lowercase hex), sorting requirements, uniqueness constraints, size limits. The fix agent (`backend/services/fix_agent.py`) sanitizes IDs via `_sanitize_ids()` before applying patches.
  - Read before modifying: `fix_agent.py` (`_sanitize_ids`), `genie_creator.py`, `create_agent_tools.py`
- **Genie Space best practices**: https://docs.databricks.com/aws/en/genie/best-practices — official guidance on space design, table selection, instructions, and SQL snippets.
  - Read before modifying: `scanner.py` (scoring rules), `prompts_create/`, `plan_builder.py`
- **GSL instruction schema (near-term)**: `docs/archives/gsl-instruction-schema.md` — section vocabulary and format rules for `instructions.text_instructions[0].content` that the Create Agent and Fix Agent must follow. You MUST read this before modifying Create Agent or Fix Agent prompts.
  - Read before modifying: `backend/services/plan_builder.py` (Create Agent parallel-generation prompts), `backend/prompts_create/_plan.py` (Create Agent plan-step prompt template), `backend/prompts.py` (Fix Agent prompt), `backend/services/fix_agent.py`, `backend/services/create_agent_tools.py`
