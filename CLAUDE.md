# Genie Workbench

Databricks App for creating, scoring, and optimizing Genie Spaces. FastAPI backend + React/Vite frontend deployed together on Databricks Apps.

## Commands

```bash
# Backend (from project root)
uv pip install -e .                          # Install Python deps
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload  # Dev server

# Frontend (from frontend/)
cd frontend && npm install && npm run build  # Build for production
cd frontend && npm run dev                   # Vite dev server (port 5173, proxies /api to :8000)
cd frontend && npm run lint                  # ESLint

# Full build (what Databricks Apps runs)
npm install   # Triggers postinstall -> cd frontend && npm install
npm run build # Triggers cd frontend && npm run build

# Deploy
databricks sync --watch . /Workspace/Users/<email>/genie-workbench
databricks apps deploy <app-name> --source-code-path /Workspace/Users/<email>/genie-workbench

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
  services/
    auth.py                # OBO auth (ContextVar), SP fallback, WorkspaceClient mgmt
    genie_client.py        # Databricks Genie API (fetch space, list spaces, query for SQL)
    scanner.py             # Rule-based IQ scoring engine (0-100, 4 dimensions)
    analyzer.py            # LLM-based deep analysis against best-practices checklist
    optimizer.py           # LLM-based optimization from benchmark feedback
    fix_agent.py           # LLM agent that generates JSON patches and applies via Genie API
    create_agent.py        # Multi-turn LLM agent for creating new Genie Spaces
    create_agent_session.py # Session persistence for create agent (Lakebase)
    create_agent_tools.py  # Tool definitions for create agent (UC discovery, SQL, etc.)
    lakebase.py            # PostgreSQL persistence (asyncpg pool, in-memory fallback)
    llm_utils.py           # OpenAI-compatible LLM client via Databricks serving endpoints
    uc_client.py           # Unity Catalog browsing (catalogs, schemas, tables)
  prompts/                 # Prompt templates for analysis
  prompts_create/          # Prompt templates for create agent (multi-file, modular)
  references/schema.md     # Genie Space JSON schema reference
frontend/
  src/
    App.tsx                # Root: SpaceList | SpaceDetail | AdminDashboard | CreateAgentChat
    lib/api.ts             # All API calls (fetch, SSE streaming helpers)
    types/index.ts         # TypeScript types mirroring backend Pydantic models
    components/            # UI components (analysis, optimization, fix agent, etc.)
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
`services/lakebase.py` uses asyncpg with graceful fallback to in-memory dicts when `LAKEBASE_HOST` is not set. Credentials auto-generated via Databricks SDK (`/api/2.0/database/credentials`). Schema defined in `sql/setup_lakebase.sql`.

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

Local dev uses `.env.local` (loaded first with override) then `.env`.

## Dev/Test Workflow

There is no local dev server — all testing is done by syncing code to Databricks and redeploying:

1. Edit code locally
2. `databricks sync --watch . /Workspace/Users/<email>/genie-workbench` picks up changes automatically
3. Re-run `databricks apps deploy <app-name> --source-code-path /Workspace/Users/<email>/genie-workbench` to trigger a new deployment
4. Test in the deployed Databricks App

Do NOT suggest running `uvicorn` or `npm run dev` locally. The app depends on Databricks-managed resources (OBO auth, Lakebase, serving endpoints) that aren't available outside a Databricks App environment.

## Gotchas

- **frontend/dist/ is gitignored but NOT databricksignored** — the built React app must be synced to workspace for deployment. Build before `databricks sync`.
- **`.databricksignore` excludes `*.md`** but explicitly includes `backend/references/schema.md` (needed at runtime by the analyzer).
- **OBO ContextVar and streaming** — for SSE endpoints, the ContextVar is NOT cleared after `call_next` because the response streams lazily. Streaming handlers stash the token on `request.state` and re-set it inside the generator.
- **Two separate "analysis" paths** — IQ Scan (`scanner.py`, rule-based, instant) and Deep Analysis (`analyzer.py`, LLM-based, streaming). They produce different outputs and don't cross-reference.
- **Two separate "fix" paths** — Fix Agent (from scan findings, auto-applies patches) and Optimize flow (from benchmark labeling, produces suggestions for a new space). They're independent.
- **Vite proxy** — dev frontend at :5173 proxies `/api` to :8000. In production, FastAPI serves static files from `frontend/dist/` directly.
- **Python 3.11+** required (`pyproject.toml`). Uses `uv` for dependency management (`uv.lock` present).
- **Root `package.json`** exists solely as a build hook for Databricks Apps — `postinstall` chains to `frontend/npm install`, `build` chains to `frontend/npm run build`.

## Agent Deployment Layer

The `agents/` directory provides an optional agent deployment layer using `@app_agent` from `dbx-agent-app`. Each agent wraps existing domain logic from `backend/services/` and exposes it as a standalone Databricks agent with A2A discovery, MCP server, and eval support.

See `docs/architecture-proposal.md` for the full design and implementation roadmap.

```
agents/
  _shared/              # Auth bridge, Lakebase pool, SP fallback
  scorer/app.py         # Wraps scanner.py
  analyzer/app.py       # Wraps analyzer.py
  creator/app.py        # Wraps create_agent.py
  optimizer/app.py      # Wraps optimizer.py
  fixer/app.py          # Wraps fix_agent.py
  supervisor/proxy.py   # Frontend proxy for agent deployment mode
agents.yaml             # Multi-agent deployment config
```

## GenieRX Specification

`docs/genierx-spec.md` defines the analysis and recommendation taxonomy. Key concepts:
- **Authoritative Facts** — raw data from systems of record, safe to surface directly
- **Canonical Metrics** — governed KPIs with stable definitions
- **Heuristic Signals** — derived fields with subjective thresholds; must carry caveats

Consult the spec when working on analysis, scoring, or recommendation features.

## Code Style

- Backend: Python, Pydantic models, FastAPI routers, no class-based views
- Frontend: React 19 + TypeScript + Tailwind CSS v4 + Vite 7, functional components only
- UI primitives in `frontend/src/components/ui/` (button, card, badge, etc.) using `class-variance-authority`
- Path alias `@` maps to `frontend/src/` (configured in `vite.config.ts` and `tsconfig.app.json`)
- All API routes prefixed with `/api`
- Pydantic models in `backend/models.py`, TypeScript mirrors in `frontend/src/types/index.ts` — keep in sync
