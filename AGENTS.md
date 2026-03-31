# AGENTS.md — Genie Workbench

Genie Workbench is a **Databricks App** (FastAPI backend + React/Vite frontend) for
creating, scoring, and optimizing Databricks Genie Spaces. It runs exclusively on the
Databricks Apps platform — there is no local dev server.

## Critical Rules (read before making any changes)

- **DO NOT run `uvicorn` locally.** The app requires Databricks OBO auth, Lakebase
  PostgreSQL, and model-serving endpoints that are only available inside a Databricks
  App. Running it locally will fail silently or produce misleading errors.
- **DO NOT run `databricks bundle init`.** It overwrites the project's `databricks.yml`
  and destroys the existing configuration.
- **DO NOT use `npm install` in build or deploy scripts** — always use `npm ci`.
  `npm install` can silently upgrade packages within `^` ranges; `npm ci` enforces the
  exact lockfile.
- **DO NOT edit `requirements.txt` manually.** It is generated from `uv.lock`. See the
  Dependency Security section below.
- All testing is done by deploying to a real Databricks workspace, not by running locally.

## Build Commands

### Python (backend)

```bash
uv sync --frozen          # Install from uv.lock (strict — fails if lock is stale)
uv pip install -e .       # Fallback: install without lock enforcement
```

### Frontend

```bash
cd frontend && npm ci             # Install from package-lock.json (strict)
cd frontend && npm run build      # Production build → frontend/dist/
cd frontend && npm run lint       # ESLint
```

### Full build (equivalent to what deploy.sh runs)

```bash
cd frontend && npm ci && npm run build
```

## Deploy Workflow

```bash
./scripts/install.sh              # Guided first-time setup (creates .env.deploy)
./scripts/deploy.sh               # Full deploy: build + sync + configure + redeploy
./scripts/deploy.sh --update      # Code-only update (faster, skips app creation)
./scripts/deploy.sh --destroy     # Tear down app and clean up jobs
```

The deploy script:
1. Runs `npm ci && npm run build` (not `npm install`)
2. Syncs the repo to the Databricks workspace with `databricks sync --full`
3. Explicitly uploads `frontend/dist/` (gitignored but NOT databricksignored)
4. Patches `app.yaml` placeholders with real config values
5. Deploys the app with `databricks apps deploy`

## Architecture

```
backend/
  main.py               # FastAPI entry point, OBO middleware, static file serving
  models.py             # All Pydantic models — MUST stay in sync with frontend/src/types/index.ts
  routers/              # API route handlers (analysis, spaces, admin, auth, create, auto_optimize)
  services/             # Business logic: auth, genie_client, scanner, fix_agent,
                        #   create_agent, lakebase, llm_utils, uc_client
  prompts/              # LLM prompt templates (analysis path)
  prompts_create/       # LLM prompt templates (create agent path)
  references/schema.md  # Genie Space JSON schema — needed at runtime (re-included in .databricksignore)

frontend/src/
  App.tsx               # Root component: SpaceList | SpaceDetail | AdminDashboard | CreateAgentChat
  lib/api.ts            # All API calls + SSE streaming helpers (manual fetch, NOT EventSource)
  types/index.ts        # TypeScript mirrors of backend Pydantic models — keep in sync
  components/           # UI components (including auto-optimize/ with 22 GSO components)
  pages/                # SpaceList, SpaceDetail, AdminDashboard, HistoryTab, IQScoreTab
  hooks/                # useAnalysis, useTheme

packages/
  genie-space-optimizer/ # GSO engine: separate Python package deployed as a wheel
                         # Has its own pyproject.toml, uv.lock, package.json, bun.lock
```

## Code Style

- **Backend:** Python 3.11+, FastAPI, Pydantic models, no class-based views
- **Frontend:** React 19, TypeScript strict, Tailwind CSS v4, Vite, functional components only
- **All API routes** prefixed with `/api`
- **UI primitives** in `frontend/src/components/ui/` (button, card, badge, etc.) using
  `class-variance-authority`
- **Path alias** `@` maps to `frontend/src/` (configured in `vite.config.ts` and
  `tsconfig.app.json`)
- **Models must stay in sync:** `backend/models.py` ↔ `frontend/src/types/index.ts`

## Key Patterns

### OBO Authentication
User identity flows via `x-forwarded-access-token`. `OBOAuthMiddleware` in `main.py`
stores the token in a `ContextVar`. For SSE endpoints the ContextVar is NOT cleared after
`call_next` — streaming handlers stash it on `request.state` and re-set it inside the
generator.

### SSE Streaming
Four endpoints use `StreamingResponse` with `text/event-stream`. Frontend reads them via
manual `fetch` + `ReadableStream` in `lib/api.ts` (not `EventSource`). Buffers are split
on `\n\n`.

### Two Separate Analysis Paths
- **IQ Scan** (`scanner.py`): rule-based, instant, 0–100 score across 4 dimensions
- **Deep Analysis** (`routers/analysis.py`): LLM-based, streaming — these are independent
  and do not cross-reference each other

### Two Separate Optimization Paths
- **Fix Agent** (`fix_agent.py`): triggered from scan findings, auto-applies JSON patches
- **Auto-Optimize** (`auto_optimize.py` + GSO engine): full benchmark-driven pipeline

## Dependency Security Policy

This project pins all dependencies to exact versions following a supply chain security
hardening (see README.md § Dependency Security).

**Lock files — always commit these:**

| File | Covers |
|---|---|
| `uv.lock` | Root Python transitive deps with SHA256 hashes |
| `packages/genie-space-optimizer/uv.lock` | GSO Python deps with SHA256 hashes |
| `frontend/package-lock.json` | Frontend npm deps with SHA-512 integrity hashes |
| `packages/genie-space-optimizer/bun.lock` | GSO UI deps |

**To update a Python dependency:**

```bash
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt \
  | grep -v "^-e " > requirements.txt
echo "-e ./packages/genie-space-optimizer" >> requirements.txt
git add uv.lock requirements.txt
```

**To update an npm dependency:**

```bash
cd frontend
npm install <package>@<new-version>
# update package.json to exact version (no ^), commit both files
git add package.json package-lock.json
```

## Testing

```bash
python tests/test_e2e_local.py    # Requires backend running at localhost:8000
python tests/test_full_schema.py  # Genie Space JSON schema validation
python tests/test_e2e_deployed.py # Playwright E2E — requires:
                                  #   pip install playwright && playwright install chromium
```

## Common Gotchas

- `frontend/dist/` is **gitignored** but **NOT databricksignored** — build before syncing
- `*.md` files are excluded from Databricks sync **except** `backend/references/schema.md`,
  which is needed at runtime by the create agent and analysis prompts
- The Vite `/api` proxy (dev, port 5173 → 8000) is dev-only; in production FastAPI serves
  static files from `frontend/dist/` directly
- `MLFLOW_EXPERIMENT_ID` is workspace-specific; the app validates it at startup and
  silently disables tracing if the experiment doesn't exist
- `frontend/dist/` must be explicitly uploaded with `databricks workspace import-dir`
  because `databricks sync --full` only uploads non-gitignored files
