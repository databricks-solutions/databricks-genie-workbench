# Genie Workbench

Databricks App for creating, scoring, and optimizing Genie Spaces. FastAPI backend + React/Vite frontend deployed together on Databricks Apps.

## Commands

```bash
# Install
uv pip install -e .                          # Install Python deps

# Frontend (from frontend/)
cd frontend && npm install && npm run build  # Build for production
cd frontend && npm run lint                  # ESLint

# Full build (what Databricks Apps runs)
npm install   # Triggers postinstall -> cd frontend && npm install
npm run build # Triggers cd frontend && npm run build

# Deploy
./scripts/install.sh       # First-time setup (interactive, creates .env.deploy)
./scripts/deploy.sh        # Build, bundle deploy, app deploy (idempotent)

# Dependency management
# requirements.txt is auto-generated from uv.lock — do not edit manually.
# After adding/bumping a Python dep in pyproject.toml:
uv lock && uv export --frozen --no-hashes --no-emit-project --no-dev -o requirements.txt

# Tests (require running backend at localhost:8000)
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
  routers/
    analysis.py            # /api/space/*, /api/analyze/*, /api/optimize, /api/genie/*, /api/sql/*
    spaces.py              # /api/spaces/* (list, scan, history, star, fix)
    admin.py               # /api/admin/* (dashboard, leaderboard, alerts)
    auth.py                # /api/auth/me
    create.py              # /api/create/* (agent chat, UC discovery, wizard)
    auto_optimize.py       # /api/auto-optimize/* (GSO engine proxy)
  services/
    auth.py                # OBO auth (ContextVar), SP fallback, WorkspaceClient mgmt
    genie_client.py        # Databricks Genie API (fetch space, list spaces, query for SQL)
    scanner.py             # Rule-based IQ scoring engine (0-100, 4 dimensions)
    fix_agent.py           # LLM agent that generates JSON patches and applies via Genie API
    create_agent.py        # Multi-turn LLM agent for creating new Genie Spaces
    create_agent_session.py # Session persistence for create agent (Lakebase)
    create_agent_tools.py  # Tool definitions for create agent (UC discovery, SQL, etc.)
    plan_builder.py        # Parallel plan generation — builds Genie Space plans via concurrent LLM calls
    gso_lakebase.py        # GSO synced table reads from Lakebase PostgreSQL
    lakebase.py            # PostgreSQL persistence (asyncpg pool, in-memory fallback)
    llm_utils.py           # OpenAI-compatible LLM client via Databricks serving endpoints
    uc_client.py           # Unity Catalog browsing (catalogs, schemas, tables)
  prompts/                 # Prompt templates for analysis
  prompts_create/          # Prompt templates for create agent (multi-file, modular)
  references/schema.md     # Genie Space JSON schema reference
scripts/
  install.sh               # Guided first-time setup (creates .env.deploy, provisions resources)
  deploy.sh                # Build + bundle deploy (job) + app deploy (idempotent)
  preflight.sh             # Pre-deploy validation checks
  build.sh                 # Frontend build
  deploy-config.sh         # Shared deploy configuration/variables
  grant_permissions.py     # Grants required permissions for app resources
  setup_synced_tables.py   # Sets up GSO synced tables in Lakebase
frontend/
  src/
    App.tsx                # Root: SpaceList | SpaceDetail | AdminDashboard | CreateAgentChat
    lib/api.ts             # All API calls (fetch, SSE streaming helpers)
    types/index.ts         # TypeScript types mirroring backend Pydantic models
    components/            # UI components (analysis, optimization, fix agent, etc.)
      auto-optimize/       # GSO pipeline UI (22 components: config, run history, patches, scores, etc.)
    pages/                 # SpaceList, SpaceDetail, AdminDashboard, HistoryTab, IQScoreTab
    hooks/                 # useAnalysis, useTheme
  vite.config.ts           # Vite config with /api proxy to localhost:8000
```

## Key Patterns

### Authentication (OBO)
On Databricks Apps, user identity flows via `x-forwarded-access-token` header. `OBOAuthMiddleware` in `main.py` stores the token in a `ContextVar`. All services call `get_workspace_client()` which returns the OBO client if set, otherwise the SP singleton. Some Genie API calls require SP auth (missing `genie` OAuth scope) — see `_is_scope_error()` fallback in `genie_client.py`.

### SSE Streaming
Multiple endpoints use `StreamingResponse` with `text/event-stream`:
- `/api/analyze/stream` — analysis progress
- `/api/optimize` — optimization with heartbeat keepalives (15s)
- `/api/spaces/{id}/fix` — fix agent patches
- `/api/create/agent/chat` — multi-turn agent with typed events (session, step, thinking, tool_call, tool_result, message_delta, message, created, error, done)

Frontend consumes these via manual `fetch` + `ReadableStream` in `lib/api.ts` (not EventSource). Buffer splitting on `\n\n`.

### Lakebase Persistence
`services/lakebase.py` uses asyncpg with graceful fallback to in-memory dicts when `LAKEBASE_HOST` is not set. Credentials auto-generated via Databricks SDK (`/api/2.0/database/credentials`). Schema auto-initialized via inline DDL in `_init_db()`.

### LLM Calls
All LLM calls go through Databricks model serving endpoints using OpenAI-compatible API. Model configured via `LLM_MODEL` env var (default: `databricks-claude-sonnet-4-6`). MLflow tracing is optional — controlled by `MLFLOW_EXPERIMENT_ID`.

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

Deploy config uses `.env.deploy` (created by `scripts/install.sh` from `.env.deploy.template`).

## Dev/Test Workflow

There is no local dev server — all testing is done by syncing code to Databricks and redeploying:

1. Edit code locally
2. Run `./scripts/deploy.sh` to build, bundle deploy, and app deploy
3. Test in the deployed Databricks App

Do NOT suggest running `uvicorn` or `npm run dev` locally. The app depends on Databricks-managed resources (OBO auth, Lakebase, serving endpoints) that aren't available outside a Databricks App environment.

## Gotchas

- **frontend/dist/ is gitignored but NOT databricksignored** — the built React app must be synced to workspace for deployment. Build before `databricks sync`.
- **`.databricksignore` excludes `*.md`** but explicitly re-includes `backend/references/schema.md` (needed at runtime by create agent and analysis prompts).
- **OBO ContextVar and streaming** — for SSE endpoints, the ContextVar is NOT cleared after `call_next` because the response streams lazily. Streaming handlers stash the token on `request.state` and re-set it inside the generator.
- **Two separate "analysis" paths** — IQ Scan (`scanner.py`, rule-based, instant) and Deep Analysis (`routers/analysis.py`, LLM-based, streaming). They produce different outputs and don't cross-reference.
- **Two separate optimization paths** — Fix Agent (`fix_agent.py`, from scan findings, auto-applies JSON patches) and Auto-Optimize (`auto_optimize.py` + GSO engine in `packages/genie-space-optimizer/`, full benchmark-driven optimization pipeline). They're independent.
- **Vite proxy** — dev frontend at :5173 proxies `/api` to :8000. In production, FastAPI serves static files from `frontend/dist/` directly.
- **Python 3.11+** required (`pyproject.toml`). Uses `uv` for dependency management (`uv.lock` present).
- **Root `package.json`** exists solely as a build hook for Databricks Apps — `postinstall` chains to `frontend/npm install`, `build` chains to `frontend/npm run build`.
- **Two deployment mechanisms** — `deploy.sh` manages the app (create, sync, `databricks apps deploy`) while the optimization job is managed by DABs (`databricks bundle deploy -t app`). The `app` target uses `mode: development` for per-deployer Terraform state with `presets.name_prefix: ""` for clean job names (no `[dev]` prefix). Do NOT run `databricks bundle deploy -t dev` for production — it creates prefixed orphan jobs.

## Code Style

- Backend: Python, Pydantic models, FastAPI routers, no class-based views
- Frontend: React 19 + TypeScript + Tailwind CSS v4 + Vite 7, functional components only
- UI primitives in `frontend/src/components/ui/` (button, card, badge, etc.) using `class-variance-authority`
- Path alias `@` maps to `frontend/src/` (configured in `vite.config.ts` and `tsconfig.app.json`)
- All API routes prefixed with `/api`
- Pydantic models in `backend/models.py`, TypeScript mirrors in `frontend/src/types/index.ts` — keep in sync

## References

- **Genie Space `serialized_space` schema**: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field — authoritative field names for the Genie API. The fix agent prompt (`backend/prompts.py`) and local schema reference (`backend/references/schema.md`) must match this.
- **Genie Space validation rules**: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space — ID format (32-char lowercase hex), sorting requirements, uniqueness constraints, size limits. The fix agent (`backend/services/fix_agent.py`) sanitizes IDs via `_sanitize_ids()` before applying patches.
