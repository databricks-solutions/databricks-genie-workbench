# Genie Workbench

Databricks App for creating, scoring, and optimizing Genie Spaces. FastAPI backend + React/Vite frontend deployed together on Databricks Apps. It runs exclusively on the Databricks Apps platform — there is no local dev server.

## Critical Rules

- **DO NOT run `uvicorn` locally.** The app requires Databricks OBO auth, Lakebase PostgreSQL, and model-serving endpoints that are only available inside a Databricks App.
- **DO NOT run `databricks bundle init`.** It overwrites the project's `databricks.yml` and destroys the existing configuration.
- **DO NOT use `npm install` in build or deploy scripts** — always use `npm ci`. `npm install` can silently upgrade packages within `^` ranges; `npm ci` enforces the exact lockfile.
- **DO NOT edit `requirements.txt` manually.** It is generated from `uv.lock` as a pip-compatible reference but is excluded from deployment via `.databricksignore`. The platform uses `uv sync` (pyproject.toml + uv.lock) for hash-verified installs.
- All testing is done by deploying to a real Databricks workspace, not by running locally.

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

# Dependency management
# requirements.txt is auto-generated from uv.lock — do not edit manually.
# After adding/bumping a Python dep in pyproject.toml:
uv lock --upgrade-package <package-name>
uv export --frozen --no-dev --no-hashes --format requirements-txt > requirements.txt

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
  prompts.py               # Prompt templates for analysis/fix agent
  genie_creator.py         # Genie Space creation logic (API calls, config assembly)
  sql_executor.py          # SQL execution via Databricks SQL warehouse
  routers/
    analysis.py            # /api/space/* (fetch, parse), /api/settings, /api/debug/auth
    spaces.py              # /api/spaces/* (list, scan, history, star, fix)
    admin.py               # /api/admin/* (dashboard, leaderboard, alerts)
    auth.py                # /api/auth/me
    create.py              # /api/create/* (agent chat, UC discovery, wizard)
    auto_optimize.py       # /api/auto-optimize/* (GSO engine proxy)
  services/
    auth.py                # OBO auth (ContextVar), SP fallback, WorkspaceClient mgmt
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
  setup_lakebase.py        # Automates Lakebase Autoscaling project, SP role, and grants
  setup_synced_tables.py   # Sets up GSO synced tables in Lakebase
frontend/
  src/
    App.tsx                # Root: SpaceList | SpaceDetail | AdminDashboard | CreateAgentChat
    lib/api.ts             # All API calls (fetch, SSE streaming helpers)
    types/index.ts         # TypeScript types mirroring backend Pydantic models
    components/            # UI components (analysis, optimization, fix agent, etc.)
      auto-optimize/       # GSO pipeline UI (24 components: config, run history, patches, scores, etc.)
    pages/                 # SpaceList, SpaceDetail, AdminDashboard, HistoryTab, IQScoreTab
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
`services/lakebase.py` uses asyncpg with graceful fallback to in-memory dicts when `LAKEBASE_HOST` is not set. Supports both provisioned Lakebase and Lakebase Autoscaling — for autoscaling, uses `client.postgres.get_endpoint()` to resolve DNS and `client.postgres.generate_database_credential()` for OAuth tokens. Schema and tables are created by the app at startup via `_ensure_schema()` (the SP owns everything it creates). Lakebase project, SP role, and database-level grants (CONNECT, CREATE) are automated by `scripts/setup_lakebase.py`, called from `deploy.sh` via `uv run`.

### LLM Calls
All LLM calls go through Databricks model serving endpoints using OpenAI-compatible API. Model configured via `LLM_MODEL` env var (default: `databricks-claude-sonnet-4-6`). MLflow tracing is optional — controlled by `MLFLOW_EXPERIMENT_ID`.

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

Deploy config uses `.env.deploy` (created by `scripts/install.sh` from `.env.deploy.template`).

## Dev/Test Workflow

There is no local dev server — all testing is done by syncing code to Databricks and redeploying:

1. Edit code locally
2. Run `./scripts/deploy.sh --update` to build, bundle deploy, and app deploy
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

- **frontend/dist/ is gitignored but NOT databricksignored** — the built React app must be synced to workspace for deployment. Build before `databricks sync`.
- **`.databricksignore` excludes `*.md`** but explicitly re-includes `backend/references/schema.md` (needed at runtime by create agent and analysis prompts).
- **OBO ContextVar and streaming** — for SSE endpoints, the ContextVar is NOT cleared after `call_next` because the response streams lazily. Streaming handlers stash the token on `request.state` and re-set it inside the generator.
- **IQ Scan is the only analysis path** — `scanner.py` runs 12 rule-based checks via `/api/spaces/{id}/scan`. `routers/analysis.py` only handles space fetching/parsing (`/api/space/fetch`, `/api/space/parse`) and settings — it does not perform analysis.
- **Two separate optimization paths** — Quick Fix (`fix_agent.py`, from scan findings, auto-applies JSON patches) and Auto-Optimize (`auto_optimize.py` + GSO engine in `packages/genie-space-optimizer/`, full benchmark-driven optimization pipeline). They're independent.
- **Vite proxy** — dev frontend at :5173 proxies `/api` to :8000. In production, FastAPI serves static files from `frontend/dist/` directly.
- **Python 3.11+** required (`pyproject.toml`). Uses `uv` for dependency management (`uv.lock` present).
- **Root `package.json`** exists solely as a build hook for Databricks Apps. `postinstall` is a no-op. `build` checks for pre-built `frontend/dist/index.html` — if present (uploaded by `deploy.sh`), skips the rebuild; if dist is missing, runs `cd frontend && npm ci && npm run build`. This keeps CLI deploy fast while allowing workspace-folder deploys from fresh clones.
- **Two deployment mechanisms** — `deploy.sh` manages the app (create, sync, `databricks apps deploy`) while the optimization job is managed by DABs (`databricks bundle deploy -t app`). The `app` target uses `mode: development` for per-deployer Terraform state with `presets.name_prefix: ""` for clean job names (no `[dev]` prefix). Do NOT run `databricks bundle deploy -t dev` for production — it creates prefixed orphan jobs.
- **Databricks CLI >= 0.297.2 required** — `preflight.sh` validates this automatically.
- **`--destroy` does not remove all resources** — it deletes the app and jobs but leaves behind: Lakebase data (`genie` schema), UC schema/tables (`<catalog>.genie_space_optimizer`), Genie Space SP permissions, MLflow experiments, and synced tables. Clean these up manually if needed.
- **`frontend/dist/` must be explicitly uploaded** with `databricks workspace import-dir` because `databricks sync --full` only uploads non-gitignored files.
- **`requirements.txt` is databricksignored** — the platform uses `uv sync` instead of `pip install`. If you see pip dependency conflicts, verify `requirements.txt` is in `.databricksignore`.
- **`MLFLOW_EXPERIMENT_ID` is workspace-specific** — the app validates it at startup and silently disables tracing if the experiment doesn't exist.
- **Lakebase state is app-instance scoped** — keep `GENIE_APP_NAME` stable and use `./scripts/deploy.sh --update` for normal changes. If creating a new app instance, use a fresh `GENIE_LAKEBASE_INSTANCE`; reusing an older app's Lakebase project can leave `genie` tables/sequences owned by the old app SP.

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

Comprehensive documentation lives in the `docs/` folder:

- `docs/00-index.md` — Documentation hub and table of contents
- `docs/03-authentication-and-permissions.md` — Deep dive on OBO + SP dual auth model
- `docs/04-create-agent.md` — Create Agent: multi-turn tool-calling flow
- `docs/07-auto-optimize.md` — GSO optimization pipeline (6-stage DAG)
- `docs/appendices/A-api-reference.md` — All API endpoints with auth identity

See `docs/00-index.md` for the full listing. When modifying auth, agents, or
optimization code, consult the relevant doc for design rationale.

## References

**Before modifying any Genie Space configuration, schema handling, or space creation/optimization code, you MUST `WebFetch` and read the relevant references below.**

- **Genie Space `serialized_space` schema**: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field — authoritative field names for the Genie API. The fix agent prompt (`backend/prompts.py`) and local schema reference (`backend/references/schema.md`) must match this.
  - Read before modifying: `fix_agent.py`, `create_agent.py`, `genie_client.py`, `references/schema.md`
- **Genie Space validation rules**: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space — ID format (32-char lowercase hex), sorting requirements, uniqueness constraints, size limits. The fix agent (`backend/services/fix_agent.py`) sanitizes IDs via `_sanitize_ids()` before applying patches.
  - Read before modifying: `fix_agent.py` (`_sanitize_ids`), `genie_creator.py`, `create_agent_tools.py`
- **Genie Space best practices**: https://docs.databricks.com/aws/en/genie/best-practices — official guidance on space design, table selection, instructions, and SQL snippets.
  - Read before modifying: `scanner.py` (scoring rules), `prompts_create/`, `plan_builder.py`
- **GSL instruction schema (near-term)**: `docs/gsl-instruction-schema.md` — section vocabulary and format rules for `instructions.text_instructions[0].content` that the Create Agent and Fix Agent must follow. You MUST read this before modifying Create Agent or Fix Agent prompts.
  - Read before modifying: `backend/services/plan_builder.py` (Create Agent parallel-generation prompts), `backend/prompts_create/_plan.py` (Create Agent plan-step prompt template), `backend/prompts.py` (Fix Agent prompt), `backend/services/fix_agent.py`, `backend/services/create_agent_tools.py`

## /goal Harness Contract

When a `/goal` is active in this session, Claude MUST obey the contract
in `packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/01-harness-contract.md`.

The eight invariants in that file are non-negotiable:

1. **One trial fits in one assistant message** — no mid-trial turn-ends.
   Use `Bash run_in_background: true` + `BashOutput` polling for the
   ~30-minute replay runs.
2. **The `EVIDENCE FOR EVALUATOR` block is the LAST surface of every
   assistant message** under `/goal` — re-emit it after any
   long-running tool call. Format defined in
   `packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/04-evidence-for-evaluator-protocol.md`.
3. **Trial number is read from `## Trial N — ` rows in
   `packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md`**
   (canonical tracker — H2 heading, append-only, newest row at the
   BOTTOM). Never use any session-local counter — `/goal --resume`
   resets the goal-internal counter, so all budget bounds must reference
   the persistent tracker via `grep -cE '^## Trial [0-9]+ — '`. The
   deprecated `v5/lever-loop-architecture-and-iteration-tracker.md` is
   frozen at the Trial 21 era and is no longer authoritative.
4. **Offline iteration first.** Invoke the
   `gso-offline-funnel-iterate` skill before every real trial; record
   the offline justification (`real_trial_required_reason ∈
   {genie_api, mlflow_reeval}`) in the next tracker row. Most fix
   iterations should never escalate to a real trial.
5. **Pre-trial gate is mandatory** —
   `packages/genie-space-optimizer/scripts/pretrial_gate.sh` must exit
   zero before any `./scripts/deploy.sh` invocation; the PreToolUse
   hook enforces this, but the model should also explicitly run it at
   the start of any trial turn so the failure surfaces in transcript.
6. **Generalizable fixes only** — no per-QID overfits, no re-imports
   from `_legacy/`, no closed-vocab archetypes, no hand-rolled QID
   extraction, no hardcoded anchor space-IDs in `src/`. PostToolUse
   `Edit|Write` hooks (`forbid_legacy_imports.sh` +
   `check_invariants.sh`) enforce automatically. See also "###
   Architectural Principles" below — those are the **success-bar**
   refinements of this invariant that the `/goal` evaluator enforces
   via literal-match on the Architectural Self-Assessment block.
7. **Real trials always run BOTH canonical anchor spaces** —
   airline `e94376a3-d8a6-4570-a605-9fe231e5f99c` and
   7now `d13938e7-405d-4444-833a-03f5ac9f7523` — via the
   `gso-lever-loop-replay` skill. **The active `job_id` and
   `parent_run_id` for each anchor are NEVER hardcoded here**; read them
   from the "Current parent job runs" table in
   [`packages/genie-space-optimizer/docs/architecture/canonical-anchors.md`](packages/genie-space-optimizer/docs/architecture/canonical-anchors.md)
   on every turn. That file is the single source of truth and rotates
   when a parent exhausts its 250-task-value budget (verdict
   `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` — see the gso-postmortem
   skill). If the table shows `Status = PENDING` for either anchor, the
   harness MUST trigger fresh parents via `gso-lever-loop-trigger` (or
   surface the blockage) before attempting any replay. Postmortems for
   both anchors fan out in parallel via `Task` subagents. A fix that
   passes on one but not the other is a bug class to surface, not hide.
   `gso-lever-loop-trigger` is COLD-START ONLY — do not use it for the
   outer loop except when rotating.
8. **Whack-a-mole detection** — if the funnel deepest-stage-reached
   has not advanced across three consecutive trials, end the turn
   with `verdict = WHACK_A_MOLE_DETECTED` and hand control back to
   the operator.

The canonical skill sequence per turn is documented in
`packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/01-harness-contract.md`
§"Skill call order". The pasteable goal conditions live in
`packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/conditions/`.
The operator runbook for launching `/goal` safely is in
`packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/06-operator-runbook.md`.

### Architectural Principles (success bar — non-negotiable)

The goal-achievement gate is NOT just "did accuracy hit 100%" — it is
also "is the solution that got us there architecturally sound". A fix
that wins on accuracy by adding `if qid == "<literal>": ...` hardcoded
branches is a regression even if the postmortem says
`final_accuracy_pct = 100.0`. The `/goal` evaluator literal-matches
the **Architectural Self-Assessment** block in every EVIDENCE block
to enforce these six principles; the aggregate
`architectural_principles_held = true` line must appear before the
goal is achieved.

Every assistant turn that edits code MUST pass each CLI arg below to
`scripts/emit_evidence_for_evaluator.py`. A turn that cannot truthfully
assert each principle = `true` (or `false` for principle 1) must pass
`unknown` — that keeps the goal open (not failed) but blocks goal
achievement until the principle is satisfied on a later turn.

1. **No deterministic shortcuts as fixes** (`--deterministic-shortcuts-added false`) —
   fixes come from LLM reasoning over typed schemas, validated by
   deterministic code. Hardcoded `if qid == "<literal>"`, `if space_id == "<UUID>"`,
   fixture-pinned matchers, anchor-specific branches in `src/` are all
   forbidden. `check_invariants.sh` PostToolUse hook catches the
   pattern; the self-assessment is the model's affirmative answer that
   no such pattern was introduced this turn.
2. **Generalizable solution** (`--generalizable-solution true`) —
   every fix works across the entire RCA family the tracker row
   targets, not just one anchor. Evidence: the bright-line replay
   suite includes at least one non-anchor fixture proving generality
   (Trial 24 Leg 2 is the canonical example).
3. **RCA-rooted** (`--rca-citation "<kind> -> <mechanism> (<marker>)"`) —
   every fix's tracker row cites the RCA kind, the fixing mechanism,
   and the watch-marker it lights up. No symptom-patching without RCA
   citation; if you can't cite, you're not yet ready to fix.
4. **Typed schemas at module boundaries** (`--typed-schemas-at-boundaries true`) —
   any data flowing across module boundaries (e.g., between SM stages,
   between repair phases, between optimizer and applier) uses a
   Pydantic model or dataclass, not `dict[str, Any]`. New
   cross-boundary edges added this turn must use typed schemas; if
   you must touch an existing untyped boundary, do not widen it.
5. **State-machine-resident fix** (`--sm-resident-fix true`) — edits
   apply inside the state machine (stages, transitions, repair hooks,
   slate compiler, applier), not via out-of-band branching. Evidence
   should cite `file:line` of the SM module the edit lives in.
6. **LLM as the reasoning engine** (`--llm-reasoning-used true`) —
   judgmental fixes (RCA categorization, mechanism selection,
   justification grounding, plan synthesis) go through LLM calls with
   typed prompt/output schemas. Deterministic code validates LLM
   output; it does not replace LLM judgment. A fix that turns a
   judgmental LLM call into a deterministic `if/elif` ladder is a
   principle 6 failure.

The aggregate line `architectural_principles_held = true` is
auto-derived in `emit_evidence_for_evaluator.py` and only emits `true`
when ALL six fields are at their safe value AND `rca_cited` is a
non-empty non-"unknown" citation. The conditions in
`packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/conditions/`
require that literal line for goal achievement.

### Next-Plan Playbook (Turn 1)

The `goal-execute-next-plan.txt` condition is intentionally compact and
points at this playbook. When a `/goal next-plan` is active, Turn 1
proceeds in this exact order:

1. **Read the canonical tracker end-to-end** —
   `packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md`.
   Identify every heading matching `^## Trial (\d+) — ` (H2, integer N,
   em-dash with literal spaces). Tracker is append-only — the LAST such
   heading in document order is `starting_trial`.
2. **Capture budget anchors** — `starting_trial` = N from step 1,
   `starting_trial_count` = `grep -cE '^## Trial [0-9]+ — ' tracker.md`.
   These bound the trial-budget stop condition in invariant 8.
3. **Decide in-progress** — `starting_trial` is IN-PROGRESS iff its
   `### Status` sub-section contains at least one `- [ ] ` line.
   Collect all unchecked items into `open_items`.
   - If `open_items` is empty AND every anchor postmortem shows
     `final_accuracy_pct = 100.0` → emit the EVIDENCE block with
     `next=GOAL_ACHIEVED` and stop.
   - If `open_items` is empty AND any anchor < 100% → AUTHOR a new
     `## Trial <starting_trial+1> — ...` row via the `gso-plan-next-fix`
     skill and re-enter step 1. Never bypass the tracker.
4. **Classify each open item** as LIVE-TRIAL if its body mentions any of
   these literal substrings (case-sensitive): `live`, `Live`, `deploy`,
   `Deploy`, `behavioral_diff`, `production lever_loop`,
   `fevm-prashanth`, `GSO_PATCH_OUTCOME_V1`, or any 15-digit parent-run
   id. Otherwise OFFLINE.
5. **Resolve `### Local Verification (mandatory before deploy)`** —
   extract each backtick-wrapped command into
   `local_verification_commands` in document order. Missing/empty table
   → STOP with `/goal clear` (a trial without local verification cannot
   be safely advanced).
6. **Run every `local_verification_commands` entry**, in order, each a
   single `Bash` tool call. Surface command + exit code in transcript.
   On non-zero:
   - Test-suite failure (`pytest …`) → branch to OFFLINE remediation via
     `gso-offline-funnel-iterate` (TDD: minimal red→green), re-run only
     the failing command. Never skip or weaken the test.
   - Check-only failure (e.g. `pretrial_gate.sh`) → surface output and
     STOP with `verdict = LOCAL_VERIFICATION_RED`. The harness will not
     deploy against a red gate.
7. **Local verification green → branch on `open_items`:**
   - **7a. OFFLINE-only:** invoke `gso-offline-funnel-iterate` per item
     in document order; PostToolUse hooks
     (`forbid_legacy_imports.sh` + `check_invariants.sh`) MUST exit 0;
     flip each `- [ ]` to `- [x]` via Edit with an evidence pointer.
   - **7b. At least one LIVE-TRIAL item:** read the "Current parent job
     runs" table from
     `packages/genie-space-optimizer/docs/architecture/canonical-anchors.md`
     and build `parent_runs` from the ACTIVE rows (one
     `{job_id, parent_run_id}` per anchor). If the table has any
     `PENDING` row, STOP with `verdict = ANCHOR_PARENTS_PENDING_ROTATION`
     and surface the rotation procedure — do NOT silently fall back to
     retired IDs. Then invoke `gso-lever-loop-replay` ONCE with
     `profile = fevm-prashanth`, `deploy_before_replay = true`,
     `parent_runs = <built from canonical-anchors.md>`,
     `rerun_dependent_tasks = true`, `continue_on_failure = false`.
     Wall-clock ~75 min total (10 min deploy + 30 min × 2 replays).
     **Use the FOREGROUND blocking-wait pattern, NOT a backgrounded
     watcher.** Concretely: emit a `phase=trigger` EVIDENCE block
     announcing the replay submission, then issue a SINGLE foreground
     `Bash` tool call (no `run_in_background: true`) that runs the
     wait-loop script and exits when BOTH parent runs reach a terminal
     `life_cycle_state` (`TERMINATED|INTERNAL_ERROR|SKIPPED`) or after
     a 7200-second (120 min) upper-bound timeout. The blocking call
     captures Claude's turn for the entire ~75 min — when it returns,
     Claude immediately proceeds to step 8 within the same turn.
     **Never use the passive `run_in_background: true` + `end_turn`
     pattern** — it causes the `/goal` evaluator to idle indefinitely
     because Claude never re-polls `BashOutput` after ending its turn
     (documented harness failure mode: an overnight `/goal` session
     can sit idle for 10+ hours after both parents have already
     terminated). The wait-loop script template:

```bash
#!/bin/bash
# Foreground blocking wait for both anchor parent runs.
# Exit 0 on BOTH_TERMINAL, exit 1 on timeout.
PROFILE=fevm-prashanth
AIR=<airline parent_run_id from canonical-anchors.md>
SEV=<7now parent_run_id from canonical-anchors.md>
DEADLINE=$(( $(date +%s) + 7200 ))   # 120 min upper bound
chk() {
  databricks jobs get-run "$1" --profile $PROFILE -o json 2>/dev/null \
    | python3 -c "import sys,json
try:
    d=json.load(sys.stdin); s=d.get('state',{})
    print(s.get('life_cycle_state',''))
except Exception:
    print('ERR')"
}
while true; do
  A=$(chk $AIR); S=$(chk $SEV)
  echo \"poll airline=$A 7now=$S @ $(date +%H:%M:%S)\"
  at=0; st=0
  case \"$A\" in TERMINATED|INTERNAL_ERROR|SKIPPED) at=1;; esac
  case \"$S\" in TERMINATED|INTERNAL_ERROR|SKIPPED) st=1;; esac
  if [ $at -eq 1 ] && [ $st -eq 1 ]; then
    echo \"BOTH_TERMINAL airline=$A 7now=$S\"
    exit 0
  fi
  if [ $(date +%s) -ge $DEADLINE ]; then
    echo \"TIMEOUT_2H airline=$A 7now=$S\"
    exit 1
  fi
  sleep 60
done
```

     If the script exits 1 (timeout), STOP with
     `verdict = REPLAY_HANG_TIMEOUT_2H` and quote both anchors' last
     observed `life_cycle_state`. If the script exits 0 but a parent's
     final state is `INTERNAL_ERROR` with `error_trace` containing
     `INVALID_PARAMETER_VALUE: A maximum of 250 task values per job run`,
     the verdict is `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` — that
     means the canonical-anchors.md table was stale; rotate before
     re-attempting. Otherwise proceed to step 8.
8. **After replay SUCCEEDED → fan-out two `gso-postmortem` invocations
   via `Task` subagents** (one per anchor) with `job_id` and
   `parent_run_id` taken from the same ACTIVE row of
   `canonical-anchors.md` that step 7b used, `profile = <same as above>`,
   `goal_harness_mode = true`. Each subagent writes its postmortem to
   `packages/genie-space-optimizer/docs/runid_analysis/<optimization_run_id>/postmortem.md`
   and returns the three literal `## Verdict` lines
   (`final_accuracy_pct = <X>`, `architecture_invariants_held = <bool>`,
   `verdict = <CODE>`).
9. **Evaluate each LIVE-TRIAL item against actual marker payloads** —
   if the item's positive criterion is met on BOTH anchors, flip
   `- [ ]` → `- [x]` with parent-run-id + optimization-run-id + marker
   citation. If unmet on either anchor, leave unchecked; the gap seeds
   the next trial's hypothesis.
10. **If any anchor is still < 100% after step 9 →** invoke
    `gso-plan-next-fix` to append a `## Trial <starting_trial+1> — ...`
    row at the BOTTOM of the canonical tracker. The new row must
    include: Hypothesis (citing the marker payloads from step 9),
    Workstreams table, Watch Markers, Anti-Success Markers, Local
    Verification table, Rollback, and Status checklist with at least one
    `- [ ]` item.
11. **Architectural Self-Assessment (BEFORE emitting EVIDENCE).**
    Inspect this turn's edits (use `git diff` if helpful) and assess
    each of the 6 architectural principles above. Compose the
    `emit_evidence_for_evaluator.py` invocation with the safe value
    for each principle if and only if the edits this turn truthfully
    satisfy it; otherwise pass `unknown` (goal stays open) or `false`
    (goal explicitly fails the principle and stops). For OFFLINE
    edits this turn, the rule of thumb is: each pytest you added must
    pass on a non-anchor fixture before you can claim
    `generalizable_solution=true`; each new judgmental branch must
    have a corresponding LLM call before you can claim
    `llm_reasoning_used=true`; each cross-module data edge must use
    a typed schema before you can claim
    `typed_schemas_at_boundaries=true`. For LIVE-TRIAL turns where no
    code was edited (just replay+postmortem), pass `unknown` for the
    five boolean fields and `unknown` for `rca-citation` — the
    aggregate will emit `architectural_principles_held = unknown`
    which keeps the goal open until a code-edit turn explicitly
    satisfies all six.

Every assistant message ends with the EVIDENCE FOR EVALUATOR block per
`packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/04-evidence-for-evaluator-protocol.md`.
Use the canonical anchor-name CLI args:
`--opt-run-id-airline`, `--opt-run-id-7now`,
`--deepest-stage-airline`, `--deepest-stage-7now`,
plus the six Architectural Self-Assessment args:
`--deterministic-shortcuts-added`, `--generalizable-solution`,
`--rca-citation`, `--typed-schemas-at-boundaries`, `--sm-resident-fix`,
`--llm-reasoning-used`.

**Pass `--phase` truthfully.** The emitter auto-masks all three
per-anchor verdict lines (and the `GOAL_HARNESS_STATUS_V1
architecture_invariants_held=` field) to safe placeholders on every
phase except `postmortem`, so stale on-disk postmortem verdicts
cannot trip a goal-stop on a `trigger`/`offline`/`plan`/`land`/`idle`
turn. The intermediate trigger-phase block (deploy in flight, before
fresh postmortems) MUST use `--phase trigger`; only the turn that
read fresh postmortems may use `--phase postmortem`. See
`04-evidence-for-evaluator-protocol.md` §`postmortem excerpts` for
the full rationale.

When NO `/goal` is active, this contract is dormant — operate normally.
