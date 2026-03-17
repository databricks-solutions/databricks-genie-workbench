# GSO Integration — Claude Code Playbook

> Batched, progressively-disclosed prompts for integrating the Genie Space
> Optimizer (GSO) into the Genie Workbench. Each prompt loads only the
> context it needs, produces one focused deliverable, and ends with
> concrete validation steps.
>
> **Reference docs (do NOT feed these wholesale — prompts cite specific sections):**
> - `docs/gso-architecture.md` — canonical future-state architecture
> - `docs/gso-migration-guide.md` — migration phases and setup steps
>
> **Execution order:** Prompts MUST be executed in sequence. Each batch
> builds on artifacts created by previous batches.

---

## Batch 1 — Foundation: Git Subtree + Python Dependency

### Prompt 1.1: Git Subtree Setup

```
Read these reference sections first for the subtree layout and ignore rules:
- docs/gso-architecture.md Section 6 (Code Management — Git Subtree)
- docs/gso-migration-guide.md Phase 1 (Git Subtree Setup)

Set up the GSO as a git subtree in the Workbench repo.

1. Check if the `gso` remote already exists:
   git remote -v | grep gso

2. If not, add it (use the prashsub GitHub account):
   gh auth switch --user prashsub
   git remote add gso https://github.com/prashsub/Genie_Space_Optimizer.git

3. Pull the subtree:
   git subtree add --prefix=packages/genie-space-optimizer gso main --squash

4. Add the GSO package as an editable dependency.
   The GSO makes databricks-connect an optional [spark] extra, so the
   standard install does not pull it in.

   Append this line to requirements.txt:
   -e ./packages/genie-space-optimizer

5. Verify .databricksignore does NOT exclude packages/ itself, but add
   exclusions for GSO build artifacts:
   packages/genie-space-optimizer/node_modules/
   packages/genie-space-optimizer/.build/
   packages/genie-space-optimizer/.databricks/
   packages/genie-space-optimizer/src/genie_space_optimizer/__dist__/
   packages/genie-space-optimizer/browser-test-output/
   packages/genie-space-optimizer/tests/
   packages/genie-space-optimizer/.git/

6. Verify the import works:
   pip install -e ./packages/genie-space-optimizer
   python -c "from genie_space_optimizer.integration import trigger_optimization; print('OK')"

Do NOT commit yet — we'll commit after all backend changes.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Subtree exists | `ls packages/genie-space-optimizer/src/genie_space_optimizer/` | Shows `integration/`, `optimization/`, etc. |
| 2 | Dependency added | `grep 'genie-space-optimizer' requirements.txt` | `-e ./packages/genie-space-optimizer` |
| 3 | Import works | `python -c "from genie_space_optimizer.integration import trigger_optimization; print('OK')"` | Prints `OK` |
| 4 | No Spark pulled | `pip show databricks-connect 2>&1` | `not found` or already-installed (either is fine) |

**If validation fails:**
- Subtree fails (repo access): `mkdir -p packages && git clone https://github.com/prashsub/Genie_Space_Optimizer.git packages/genie-space-optimizer`
- Import fails: Check `packages/genie-space-optimizer/pyproject.toml` has an `[project]` section with name `genie-space-optimizer`

---

## Batch 2 — Backend Data Layer: Lakebase Read Functions

### Prompt 2.1: Create `gso_lakebase.py`

```
Read these files first:
- backend/services/lakebase.py (PATTERN SOURCE — follow its exact pool
  access and fallback behavior)
- docs/gso-architecture.md Section 3.2 (gso_lakebase service spec —
  function signatures, table-to-function mapping)
- docs/gso-architecture.md Section 5.2 (Synced Tables — Delta → Lakebase
  sync flow, schema naming)

Create backend/services/gso_lakebase.py with async read functions for
the GSO synced tables in Lakebase.

PATTERNS TO FOLLOW (from lakebase.py):
- Import _pool and _lakebase_available from backend.services.lakebase
- Every function checks: if not _lakebase_available or _pool is None: return None/[]
- Use async with _pool.acquire() as conn: for queries
- Return plain dicts (not Pydantic models)
- Use $1, $2 parameterized queries (asyncpg style), NOT f-strings

Tables are in a "gso" schema in Lakebase. Functions needed:

1. load_gso_run(run_id: str) -> dict | None
   Query: SELECT * FROM gso.genie_opt_runs WHERE run_id = $1

2. load_gso_runs_for_space(space_id: str) -> list[dict]
   Query: SELECT run_id, space_id, status, started_at, completed_at,
          best_accuracy, best_iteration, convergence_reason, triggered_by
          FROM gso.genie_opt_runs WHERE space_id = $1
          ORDER BY started_at DESC

3. load_gso_stages(run_id: str) -> list[dict]
   Query: SELECT * FROM gso.genie_opt_stages WHERE run_id = $1
          ORDER BY started_at ASC

4. load_gso_iterations(run_id: str) -> list[dict]
   Query: SELECT * FROM gso.genie_opt_iterations WHERE run_id = $1
          ORDER BY iteration ASC

5. load_gso_patches(run_id: str) -> list[dict]
   Query: SELECT * FROM gso.genie_opt_patches WHERE run_id = $1
          ORDER BY iteration, lever, patch_index

6. load_gso_asi_results(run_id: str, iteration: int) -> list[dict]
   Query: SELECT * FROM gso.genie_eval_asi_results
          WHERE run_id = $1 AND iteration = $2

7. load_gso_suggestions(run_id: str) -> list[dict]
   Query: SELECT * FROM gso.genie_opt_suggestions WHERE run_id = $1
          ORDER BY created_at ASC

For fetchrow results, convert to dict(row). For fetch results, convert
to [dict(r) for r in rows]. Handle None rows gracefully.

Add a module docstring: "GSO synced table reads from Lakebase (PostgreSQL)."
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | File exists | `ls backend/services/gso_lakebase.py` | File listed |
| 2 | 7 functions | `grep -c 'async def load_gso' backend/services/gso_lakebase.py` | `7` |
| 3 | Uses pool import | `grep '_pool' backend/services/gso_lakebase.py` | Contains `from backend.services.lakebase import` |
| 4 | No f-string SQL | `grep -c "f'" backend/services/gso_lakebase.py` | `0` |
| 5 | Import works | `python -c "from backend.services.gso_lakebase import load_gso_run; print('OK')"` | `OK` |

**If validation fails:**
- Import error on `_pool`: Check `backend/services/lakebase.py` exports `_pool` and `_lakebase_available` at module level (they're globals, not wrapped in `__all__`)
- Syntax errors: Run `python -m py_compile backend/services/gso_lakebase.py`

---

## Batch 3 — Backend Router + Wiring

### Prompt 3.1: Create the Auto-Optimize Router

```
Read these files first for patterns:
- backend/routers/analysis.py (router setup, APIRouter prefix, error handling)
- backend/routers/spaces.py (CRUD patterns, HTTPException usage)
- backend/services/auth.py (get_workspace_client, get_service_principal_client)
- backend/services/gso_lakebase.py (the file you just created)
- docs/gso-architecture.md Sections 3.1, 8.4, 9.2, 9.3, 12.5, 12.6, 12.7

Create backend/routers/auto_optimize.py — a FastAPI router for the
Auto-Optimize feature. The router prefix is /api/auto-optimize.

The router imports from the GSO integration module:

  from genie_space_optimizer.integration import (
      trigger_optimization,
      apply_optimization,
      discard_optimization,
      get_lever_info,
      IntegrationConfig,
  )

10 endpoints:

1. GET /health
   - Returns { configured: true/false } based on whether GSO_CATALOG and
     GSO_JOB_ID environment variables are set and non-empty

2. POST /trigger
   - Request body: { space_id: str, apply_mode: str = "genie_config",
     levers: list[int] | None = None, deploy_target: str | None = None }
   - Auth: get_workspace_client() for OBO, get_service_principal_client() for SP
   - Build IntegrationConfig via helper _build_gso_config() from env vars:
     GSO_CATALOG, GSO_SCHEMA, GSO_WAREHOUSE_ID (fallback to SQL_WAREHOUSE_ID),
     GSO_JOB_ID
   - Call trigger_optimization() — uses warehouse-first mode (no Spark)
   - Return: { runId, jobRunId, jobUrl, status: "IN_PROGRESS" }
   - Error handling: 409 for active run, 403 for permission denied, 500 for job failure

3. GET /runs/{run_id}
   - Call load_gso_run(run_id) from gso_lakebase
   - Also call load_gso_stages(run_id) and load_gso_iterations(run_id)
   - Assemble a response with run status, pipeline steps, and scores
   - Return 404 if run not found

4. GET /runs/{run_id}/status
   - Lightweight: only load_gso_run(run_id)
   - Return: { runId, status, spaceId, startedAt, completedAt,
     baselineScore, optimizedScore, convergenceReason }

5. GET /levers
   - Call get_lever_info() from the integration module
   - Returns levers 1-5 only. Lever 0 (Proactive Enrichment) always runs
     and is NOT user-selectable.
   - Return list of { id, name, description }

6. POST /runs/{run_id}/apply
   - Auth: get_workspace_client() for OBO only — NO SP needed
   - Call apply_optimization(run_id, ws, config)
   - Return { status: "applied", runId, message }

7. POST /runs/{run_id}/discard
   - Auth: get_workspace_client() for OBO + get_service_principal_client() for SP
   - Call discard_optimization(run_id, ws, sp_ws, config)
   - Return { status: "discarded", runId, message }

8. GET /spaces/{space_id}/runs
   - Call load_gso_runs_for_space(space_id) from gso_lakebase
   - Return list of run summaries for the history table

9. GET /runs/{run_id}/iterations
   - Call load_gso_iterations(run_id) from gso_lakebase
   - Return list of iteration details

10. GET /runs/{run_id}/asi-results
    - Accept query param: iteration (int)
    - Call load_gso_asi_results(run_id, iteration) from gso_lakebase
    - Return list of ASI results

Use Pydantic models for request/response. Define them in the router file
or in backend/models.py (follow the existing pattern — check which approach
the codebase uses).
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | File exists | `ls backend/routers/auto_optimize.py` | File listed |
| 2 | 10 endpoints | `grep -cE '@router\.(get|post)' backend/routers/auto_optimize.py` | `10` |
| 3 | Health endpoint | `grep 'auto-optimize/health' backend/routers/auto_optimize.py` | Found |
| 4 | Import works | `python -c "from backend.routers.auto_optimize import router; print('OK')"` | `OK` |
| 5 | Config helper | `grep '_build_gso_config' backend/routers/auto_optimize.py` | Found |

**If validation fails:**
- Import error on `genie_space_optimizer.integration`: Re-run `pip install -e ./packages/genie-space-optimizer`
- Import error on auth: Check `backend/services/auth.py` exports `get_workspace_client` and `get_service_principal_client`

---

### Prompt 3.2: Mount Router + Add Env Vars

```
Read these files first:
- backend/main.py (existing router mounting pattern)
- app.yaml (existing env var structure)
- docs/gso-architecture.md Section 3.3 (app.yaml — GSO env var names,
  values, and valueFrom resource binding)
- docs/gso-architecture.md Section 3.4 (mounting — import and
  include_router placement)

Wire the auto_optimize router into the Workbench:

1. In backend/main.py:
   - Add import: from backend.routers.auto_optimize import router as auto_optimize_router
   - Add after the existing app.include_router() calls:
     app.include_router(auto_optimize_router)

2. In app.yaml, add these env vars under the env: section, after the
   existing Lakebase section:

  # ---------------------------------------------------------------------------
  # Auto-Optimize (GSO Engine)
  # ---------------------------------------------------------------------------
  - name: GSO_CATALOG
    value: ""
  - name: GSO_SCHEMA
    value: "genie_space_optimizer"
  - name: GSO_JOB_ID
    value: ""
  - name: GSO_WAREHOUSE_ID
    valueFrom: sql-warehouse

Do not modify any other part of main.py or app.yaml.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Router mounted | `grep 'auto_optimize_router' backend/main.py` | Import + include_router lines |
| 2 | Env vars added | `grep 'GSO_CATALOG' app.yaml` | Found |
| 3 | All 4 env vars | `grep -c 'GSO_' app.yaml` | `4` |
| 4 | App loads | `python -c "from backend.main import app; print(len(app.routes))"` | Prints a number (no crash) |

**If validation fails:**
- App won't import: Check for circular imports — `auto_optimize.py` should not import from `main.py`
- YAML syntax: Run `python -c "import yaml; yaml.safe_load(open('app.yaml'))"` to check

---

## Batch 4 — Frontend Foundation: UI Primitives + Types + API

### Prompt 4.1: Install Missing UI Primitives

```
Install the missing shadcn/ui components needed for Auto-Optimize.

cd frontend
npx shadcn@latest add checkbox table collapsible tooltip skeleton alert-dialog

Verify the new component files exist in frontend/src/components/ui/:
- checkbox.tsx
- table.tsx
- collapsible.tsx
- tooltip.tsx
- skeleton.tsx
- alert-dialog.tsx

Run: cd frontend && npx tsc --noEmit
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | 6 new files | `ls frontend/src/components/ui/{checkbox,table,collapsible,tooltip,skeleton,alert-dialog}.tsx` | All 6 listed |
| 2 | Types pass | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- shadcn prompts for config: Ensure `frontend/components.json` exists (shadcn config file)
- Type errors in new components: May need Tailwind v4 compatibility — check shadcn version

---

### Prompt 4.2: Add GSO TypeScript Types

```
Read these files first:
- frontend/src/types/index.ts (existing type organization pattern —
  interfaces grouped by domain, comment separators, exported directly)
- docs/gso-architecture.md Section 4.3 (canonical GSO TypeScript types —
  field names, optional fields, union types for apply_mode)

Add these types at the END of frontend/src/types/index.ts:

// ============================================================================
// Auto-Optimize (GSO) Types
// ============================================================================

// apply_mode supports three values: "genie_config" (default),
// "uc_artifact" (UC-level changes only), "both" (config + UC).
// The UI currently only exposes "genie_config" and disables "both".
export interface GSOTriggerRequest {
  space_id: string
  apply_mode?: "genie_config" | "uc_artifact" | "both"
  levers?: number[]
  deploy_target?: string
}

export interface GSOTriggerResponse {
  runId: string
  jobRunId: string
  jobUrl: string | null
  status: string
}

export interface GSOLeverInfo {
  id: number
  name: string
  description: string
}

export interface GSORunStatus {
  runId: string
  status: string
  spaceId: string
  startedAt: string | null
  completedAt: string | null
  baselineScore: number | null
  optimizedScore: number | null
  convergenceReason: string | null
}

export interface GSORunSummary {
  run_id: string
  space_id: string
  status: string
  started_at: string
  completed_at: string | null
  best_accuracy: number | null
  best_iteration: number | null
  convergence_reason: string | null
  triggered_by: string | null
}

export interface GSOPipelineStep {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  summary: string | null
}

export interface GSOPipelineRun {
  runId: string
  spaceId: string
  status: string
  startedAt: string
  completedAt: string | null
  baselineScore: number | null
  optimizedScore: number | null
  steps: GSOPipelineStep[]
  convergenceReason: string | null
}

export interface GSOIterationResult {
  iteration: number
  lever: number | null
  eval_scope: string
  overall_accuracy: number
  total_questions: number
  correct_count: number
  scores_json: string
  thresholds_met: boolean
}

export interface GSOQuestionResult {
  question_id: string
  judge: string
  value: string
  failure_type: string | null
  confidence: number | null
}

Do NOT modify any existing types. Only append.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Types added | `grep 'GSOTriggerRequest' frontend/src/types/index.ts` | Found |
| 2 | All 9 interfaces | `grep -c 'export interface GSO' frontend/src/types/index.ts` | `9` |
| 3 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- Duplicate identifier: Check you didn't accidentally paste types twice
- Missing export: Ensure every interface has `export` keyword

---

### Prompt 4.3: Add GSO API Functions

```
Read these files first:
- frontend/src/lib/api.ts (PATTERN SOURCE — fetchWithTimeout<T>, API_BASE,
  DEFAULT_TIMEOUT/LONG_TIMEOUT, POST request structure)
- docs/gso-migration-guide.md Section 4.9 (9 API functions listed with
  endpoints, HTTP methods, and timeout requirements)
- docs/gso-architecture.md Section 3.1 (endpoint table — all 10 backend
  routes that these functions call)

Add these functions at the END of frontend/src/lib/api.ts. Import the
GSO types you need at the top of the file from "@/types".

// --- Auto-Optimize (GSO) API ---

export async function getAutoOptimizeHealth(): Promise<{ configured: boolean }> {
  return fetchWithTimeout(`${API_BASE}/auto-optimize/health`)
}

export async function triggerAutoOptimize(request: GSOTriggerRequest): Promise<GSOTriggerResponse> {
  // POST /api/auto-optimize/trigger
  // Use LONG_TIMEOUT — trigger involves UC metadata prefetch + job submission
}

export async function getAutoOptimizeRun(runId: string): Promise<GSOPipelineRun> {
  // GET /api/auto-optimize/runs/{runId}
}

export async function getAutoOptimizeStatus(runId: string): Promise<GSORunStatus> {
  // GET /api/auto-optimize/runs/{runId}/status
}

export async function getAutoOptimizeLevers(): Promise<GSOLeverInfo[]> {
  // GET /api/auto-optimize/levers
}

export async function applyAutoOptimize(runId: string): Promise<{ status: string; runId: string; message: string }> {
  // POST /api/auto-optimize/runs/{runId}/apply
  // Use LONG_TIMEOUT
}

export async function discardAutoOptimize(runId: string): Promise<{ status: string; runId: string; message: string }> {
  // POST /api/auto-optimize/runs/{runId}/discard
}

export async function getAutoOptimizeRunsForSpace(spaceId: string): Promise<GSORunSummary[]> {
  // GET /api/auto-optimize/spaces/{spaceId}/runs
}

export async function getAutoOptimizeIterations(runId: string): Promise<GSOIterationResult[]> {
  // GET /api/auto-optimize/runs/{runId}/iterations
}

export async function getAutoOptimizeAsiResults(runId: string, iteration: number): Promise<GSOQuestionResult[]> {
  // GET /api/auto-optimize/runs/{runId}/asi-results?iteration={iteration}
}

Implement each function body following the fetchWithTimeout pattern.
Do NOT modify any existing functions.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Health function | `grep 'getAutoOptimizeHealth' frontend/src/lib/api.ts` | Found |
| 2 | All 10 functions | `grep -c 'export async function.*AutoOptimize\|export async function.*autoOptimize' frontend/src/lib/api.ts` | `10` |
| 3 | GSO imports | `grep 'GSOTriggerRequest' frontend/src/lib/api.ts` | Found in import |
| 4 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- Type import errors: Ensure GSOTriggerRequest etc. are exported from `@/types`
- Missing API_BASE: Check that `API_BASE` is defined at the top of `api.ts`

---

## Batch 5 — Frontend Layer 1: Auto-Optimize Tab

### Prompt 5.1: Create Layer 1 Components

```
Read these files first for patterns:
- frontend/src/pages/SpaceDetail.tsx (tab structure, state management)
- frontend/src/pages/IQScoreTab.tsx (example of a simple tab component)
- frontend/src/components/ui/card.tsx, button.tsx, badge.tsx (UI primitives)
- frontend/src/lib/api.ts (the GSO API functions you just added)

Read these architecture sections for design decisions:
- docs/gso-architecture.md Section 11.1-11.2 (three-layer overview,
  Layer 1 tab description)
- docs/gso-architecture.md Section 12.5 (graceful degradation — health
  check, "not configured" card, HTTP 503 behavior)
- docs/gso-architecture.md Section 12.6 (Lever 0 — Proactive Enrichment
  always runs, NOT user-selectable, levers 1-5 are DEFAULT_LEVER_ORDER)
- docs/gso-architecture.md Section 12.7 (apply_mode values — "genie_config"
  default, "both" disabled/"coming soon", "uc_artifact" not yet in UI)

For reference on the optimization config UI (levers, apply mode, start
button), look at the GSO standalone UI:
- packages/genie-space-optimizer/src/genie_space_optimizer/ui/routes/spaces/$spaceId.tsx

Create these 4 files:

1. frontend/src/components/auto-optimize/AutoOptimizeTab.tsx
   - Props: { spaceId: string }
   - State: view ("configure" | "monitoring" | "detail"), activeRunId, selectedRunId
   - On mount: call getAutoOptimizeHealth(). If configured: false,
     show an informational card: "Auto-Optimize is not configured for this
     deployment." and return early (no form, no table).
   - "configure" view: shows OptimizationConfig + RunHistoryTable
   - "monitoring" view: shows active run status card with polling (5s)
     - Use useEffect with setInterval calling getAutoOptimizeStatus(runId)
     - Stop polling when status reaches a terminal state:
       CONVERGED, STALLED, MAX_ITERATIONS, FAILED, CANCELLED, APPLIED, DISCARDED
     - Clean up interval on unmount
     - ScoreSummary component (baseline → optimized)
     - Status badge
     - "View Details" button → sets view to "detail"
   - "detail" view: shows RunDetailView (Layer 2) — import but
     it doesn't exist yet, so use a placeholder:
     {view === "detail" && selectedRunId && <div>Run detail for {selectedRunId}</div>}
   - On mount, check for active runs via getAutoOptimizeRunsForSpace()
     If an active run exists (status QUEUED/IN_PROGRESS/RUNNING),
     auto-switch to "monitoring"

2. frontend/src/components/auto-optimize/OptimizationConfig.tsx
   - Props: { spaceId: string, onStarted: (runId: string) => void, hasActiveRun: boolean }
   - Lever checkboxes (use the Checkbox component from @/components/ui/checkbox):
     5 levers (id 1-5), all checked by default. Lever 0 ("Proactive Enrichment")
     is NOT shown — it always runs automatically.
     Use a const LEVERS array:
     id 1: "Tables & Columns" — "Update table descriptions, column descriptions, and synonyms"
     id 2: "Metric Views" — "Update metric view column descriptions"
     id 3: "Table-Valued Functions" — "Remove underperforming TVFs"
     id 4: "Join Specifications" — "Add, update, or remove join relationships"
     id 5: "Genie Space Instructions" — "Rewrite global routing instructions"
   - Apply mode: "Config Only" toggle (default). "Config + UC Write Backs" shown
     but disabled with "Coming soon" badge. A third mode "uc_artifact" exists
     but is not yet exposed in the UI.
   - Start button: calls triggerAutoOptimize(), disabled when hasActiveRun or
     no levers selected. Shows "Starting..." while loading
   - On success: calls onStarted(runId) to switch parent to monitoring view
   - On error: show toast/alert with error detail

3. frontend/src/components/auto-optimize/RunHistoryTable.tsx
   - Props: { spaceId: string, onSelectRun: (runId: string) => void }
   - Fetches getAutoOptimizeRunsForSpace(spaceId) on mount
   - Renders a table (use Table, TableHeader, TableBody, TableRow, TableCell
     from @/components/ui/table): Date | Status (badge) | Baseline | Optimized | Action
   - "View Details" link calls onSelectRun(runId)
   - Empty state: "No optimization runs yet."

4. frontend/src/components/auto-optimize/ScoreSummary.tsx
   - Props: { baselineScore: number | null, optimizedScore: number | null }
   - Shows baseline → optimized with delta and arrow
   - Green if improved, red if regressed, gray if null

Use the Workbench's existing UI primitives: Card, CardHeader, CardContent,
CardTitle, Button, Badge from @/components/ui/*. Use Tailwind classes
matching the Workbench theme (bg-surface, text-primary, border-default, etc.).
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | 4 files exist | `ls frontend/src/components/auto-optimize/{AutoOptimizeTab,OptimizationConfig,RunHistoryTable,ScoreSummary}.tsx` | All 4 listed |
| 2 | Health check | `grep 'getAutoOptimizeHealth' frontend/src/components/auto-optimize/AutoOptimizeTab.tsx` | Found |
| 3 | 5 levers only | `grep -c 'id:' frontend/src/components/auto-optimize/OptimizationConfig.tsx` | `5` (no lever 0) |
| 4 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- Import errors: Ensure API functions are exported from `@/lib/api` and types from `@/types`
- Checkbox not found: Confirm `frontend/src/components/ui/checkbox.tsx` exists from Batch 4

---

### Prompt 5.2: Wire Auto-Optimize Tab into SpaceDetail

```
Read these files first:
- frontend/src/pages/SpaceDetail.tsx (existing tab system)
- docs/gso-architecture.md Section 4.1 (new tab definition — Tab type
  union, tabs array entry with Rocket icon, position between "optimize"
  and "history")

Make these targeted changes:

1. Add import at the top:
   import { Rocket } from "lucide-react"
   import { AutoOptimizeTab } from "@/components/auto-optimize/AutoOptimizeTab"

2. Extend the Tab type to include "auto-optimize":
   type Tab = "overview" | "score" | "analysis" | "optimize" | "auto-optimize" | "history"

3. Add to the tabs array (between "optimize" and "history"):
   { id: "auto-optimize", label: "Auto-Optimize", icon: <Rocket className="w-4 h-4" /> },

4. Add the conditional render (in the same section where other tabs render):
   {activeTab === "auto-optimize" && (
     <AutoOptimizeTab spaceId={spaceId} />
   )}

Do NOT modify any existing tab content or logic.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Tab type extended | `grep 'auto-optimize' frontend/src/pages/SpaceDetail.tsx` | Found in type + tabs array + render |
| 2 | Rocket icon | `grep 'Rocket' frontend/src/pages/SpaceDetail.tsx` | Found |
| 3 | Existing tabs intact | `grep -c '"overview"\|"score"\|"analysis"\|"optimize"\|"history"' frontend/src/pages/SpaceDetail.tsx` | At least 5 matches |
| 4 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- Rocket icon not found: `npm ls lucide-react` — should be installed already
- Tab not rendering: Ensure spaceId prop is available in scope where AutoOptimizeTab is used

---

## Batch 6 — Frontend Layer 2: Benchmark Evaluation Page

### Prompt 6.1: Create RunDetailView, QuestionList, QuestionDetail

```
DESIGN INTENT: Layer 2 is intentionally designed to resemble the Genie
native Benchmarks page (question list + SQL comparison), NOT the GSO's
pipeline-centric run detail page. The GSO's pipeline view is Layer 3.

Read these architecture sections for the Layer 2 design:
- docs/gso-architecture.md Section 11.3 (Layer 2 — Benchmark Evaluation
  Page: left sidebar question list, main area SQL comparison, results
  table, score banner, gear icon to Layer 3)
- docs/gso-architecture.md Section 11.8 (design intent — why Layer 2
  resembles Genie native Benchmarks, not the GSO run detail page)
- docs/gso-migration-guide.md Section 4.6 (Layer 2 wireframe layout
  with ASCII art showing the two-column structure)

Read these code files for patterns and data shapes:
- frontend/src/components/auto-optimize/AutoOptimizeTab.tsx (the parent — understand how it navigates to detail view)
- frontend/src/lib/api.ts (getAutoOptimizeRun, getAutoOptimizeIterations, getAutoOptimizeAsiResults, applyAutoOptimize, discardAutoOptimize)

For GSO data shape reference (simplify significantly):
- packages/genie-space-optimizer/src/genie_space_optimizer/ui/routes/runs/$runId.tsx

Create these 3 files:

1. frontend/src/components/auto-optimize/RunDetailView.tsx
   - Props: { runId: string, onBack: () => void }
   - Fetches getAutoOptimizeRun(runId) on mount
   - Fetches getAutoOptimizeAsiResults(runId, bestIteration) for the best iteration
   - Header: back button (calls onBack), date, status badge,
     score banner ("92% accurate (23/25)"), gear icon (Settings2 from lucide)
   - Gear icon: opens PipelineDetailsModal (Layer 3) — import but use placeholder
     for now: a state boolean showPipeline that renders a simple modal shell
   - Body: two-column layout (sidebar 1/3, main 2/3)
     - Left: QuestionList (sidebar)
     - Right: QuestionDetail (selected question)
   - Bottom: Apply/Discard buttons if run status is CONVERGED, STALLED, or MAX_ITERATIONS
     - Apply calls applyAutoOptimize(runId), then calls onBack()
     - Discard calls discardAutoOptimize(runId), then calls onBack()

2. frontend/src/components/auto-optimize/QuestionList.tsx
   - Props: { questions: GSOQuestionResult[], selectedId: string | null, onSelect: (id: string) => void }
   - Search input at top to filter by question text
   - Filter buttons: All | Passing | Failing
   - Each item: pass/fail icon (CheckCircle green / XCircle red from lucide),
     truncated question_id text
   - Selected item gets highlighted background (bg-accent/10)

3. frontend/src/components/auto-optimize/QuestionDetail.tsx
   - Props: { question: GSOQuestionResult | null }
   - If null: show "Select a question to view details"
   - Shows: question_id, judge name, assessment badge (pass=green, fail=red)
   - Value field displayed as SQL in a pre/code block with font-mono styling
   - failure_type shown if present
   - confidence shown as a percentage if present

Also update AutoOptimizeTab.tsx: replace the Layer 2 placeholder with
the real RunDetailView import:
   import { RunDetailView } from "@/components/auto-optimize/RunDetailView"
   {view === "detail" && selectedRunId && (
     <RunDetailView runId={selectedRunId} onBack={() => setView("configure")} />
   )}

Use the Workbench UI primitives throughout. Match the Workbench visual style.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | 3 files exist | `ls frontend/src/components/auto-optimize/{RunDetailView,QuestionList,QuestionDetail}.tsx` | All 3 listed |
| 2 | RunDetailView imported | `grep 'RunDetailView' frontend/src/components/auto-optimize/AutoOptimizeTab.tsx` | Found |
| 3 | Apply/Discard | `grep 'applyAutoOptimize\|discardAutoOptimize' frontend/src/components/auto-optimize/RunDetailView.tsx` | Both found |
| 4 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- ASI results shape mismatch: Check GSOQuestionResult interface matches what the backend returns
- Missing lucide icons: `grep 'CheckCircle\|XCircle\|Settings2' ...` — all from lucide-react

---

## Batch 7 — Frontend Layer 3: Pipeline Details Modal

### Prompt 7.1: Create PipelineDetailsModal + PipelineStepCard

```
Read these architecture sections for the Layer 3 design:
- docs/gso-architecture.md Section 11.4 (Layer 3 — Pipeline Details:
  6-step pipeline progress cards, iteration chart, per-judge scores,
  patch audit trail, built natively not imported from GSO)
- docs/gso-architecture.md Section 11.5 (component directory — all 9
  files listed with their layer assignments)

Read these code files for context:
- frontend/src/components/auto-optimize/RunDetailView.tsx (the parent — gear icon opens this modal)
- frontend/src/lib/api.ts (getAutoOptimizeRun — returns GSOPipelineRun with steps)

Create these 2 files:

1. frontend/src/components/auto-optimize/PipelineDetailsModal.tsx
   - Props: { runId: string, isOpen: boolean, onClose: () => void }
   - If not isOpen, return null
   - Renders a modal overlay: fixed inset-0, bg-black/50 backdrop, centered content
   - Content card: close button (X icon), title "Pipeline Details"
   - Fetches getAutoOptimizeRun(runId) on mount (reuses the same endpoint)
   - Renders a list of PipelineStepCard components for the 6 stages
   - ScoreSummary at bottom (reuse the component from Layer 1)
   - Click backdrop to close

2. frontend/src/components/auto-optimize/PipelineStepCard.tsx
   - Props: { stepNumber: number, name: string, status: string,
     durationSeconds: number | null, description: string }
   - Card with: step number in a circle, name, status badge, duration
     (formatted as "Xm Ys" or "—"), description text
   - Status styling:
     "pending" = gray bg + text
     "running" = blue bg + text + animate-pulse
     "completed" = green bg + text
     "failed" = red bg + text
   - Step descriptions (hardcoded in the modal, passed as props):
     1. Preflight: "Reads config and queries Unity Catalog for metadata"
     2. Baseline Evaluation: "Runs benchmarks through 9 judges"
     3. Proactive Enrichment: "Enriches descriptions, joins, instructions"
     4. Adaptive Optimization: "Applies optimization levers with 3-gate eval"
     5. Finalization: "Repeatability checks and model promotion"
     6. Deploy: "Deploys optimized config to target"

Also update RunDetailView.tsx: replace the modal placeholder with
the real PipelineDetailsModal import:
   import { PipelineDetailsModal } from "@/components/auto-optimize/PipelineDetailsModal"
   <PipelineDetailsModal runId={runId} isOpen={showPipeline} onClose={() => setShowPipeline(false)} />

Use Workbench UI primitives. Match the visual style.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | 2 files exist | `ls frontend/src/components/auto-optimize/{PipelineDetailsModal,PipelineStepCard}.tsx` | Both listed |
| 2 | 9 total components | `ls frontend/src/components/auto-optimize/*.tsx \| wc -l` | `9` |
| 3 | Modal wired in | `grep 'PipelineDetailsModal' frontend/src/components/auto-optimize/RunDetailView.tsx` | Found |
| 4 | 6 step descriptions | `grep -c 'description' frontend/src/components/auto-optimize/PipelineDetailsModal.tsx` | At least 6 |
| 5 | Types compile | `cd frontend && npx tsc --noEmit` | No errors |

**If validation fails:**
- Step data shape: Ensure GSOPipelineRun.steps matches PipelineStepCard props
- Modal not closing: Check onClose wired to backdrop click and X button

---

## Batch 8 — Build, Commit, Deploy

### Prompt 8.1: Build and Lint

```
Build and lint the frontend.

1. cd frontend && npm install
2. npm run lint (fix any errors, warnings are OK)
3. npm run build

If there are TypeScript errors:
- Check that all imports resolve (types from @/types, api from @/lib/api)
- Check that component props match their usage
- Fix any missing exports

If there are lint errors:
- Fix unused imports
- Fix any React hook dependency warnings
- Fix any missing key props in map() calls
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Build succeeds | `cd frontend && npm run build` | Exit code 0 |
| 2 | Dist populated | `ls frontend/dist/index.html` | File exists |
| 3 | No TS errors | `cd frontend && npx tsc --noEmit` | No errors |
| 4 | Existing tabs intact | `grep '"overview"' frontend/src/pages/SpaceDetail.tsx` | Still present |

**If validation fails:**
- Missing module: `npm install` may need re-running after shadcn additions
- Lint errors in auto-fix: Run `npm run lint -- --fix`

---

### Prompt 8.2: Commit and Deploy

```
Reference: docs/gso-migration-guide.md Phase 5 (Deploy and Validate —
build steps, sync command, deploy command, post-deployment config,
validation checklist)

Commit and deploy the GSO integration.

1. Stage all changes:
   git add -A

2. Review what's being committed:
   git status
   git diff --cached --stat

3. Commit (do NOT push yet):
   git commit -m "Integrate GSO auto-optimize into Workbench

   - Add GSO as git subtree at packages/genie-space-optimizer
   - Add backend router (auto_optimize.py) with auth bridge
   - Add Lakebase read functions (gso_lakebase.py)
   - Add Auto-Optimize tab with three-layer UI
   - Add GSO env vars to app.yaml"

4. Sync to workspace:
   databricks sync . /Workspace/Users/prashanth.subrahmanyam@databricks.com/genie-workbench \
     --profile fevm-prashanth

5. Deploy:
   databricks apps deploy genie-workbench \
     --source-code-path /Workspace/Users/prashanth.subrahmanyam@databricks.com/genie-workbench \
     --profile fevm-prashanth

6. After deploy, set env vars in the Databricks Apps UI:
   - GSO_CATALOG → the Unity Catalog name where GSO Delta tables live
   - GSO_SCHEMA → genie_space_optimizer (or your custom schema name)
   - GSO_JOB_ID → the Databricks Job ID from 'databricks jobs list | grep genie-space-optimizer'

Do NOT push to remote until manual validation is complete.
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Commit exists | `git log --oneline -1` | Shows GSO integration commit |
| 2 | Deploy starts | Check `databricks apps deploy` output | No errors |
| 3 | App loads | Navigate to app URL in browser | Workbench loads |
| 4 | Tab visible | Navigate to any Space → tabs | "Auto-Optimize" tab appears |
| 5 | Health check | `curl <app-url>/api/auto-optimize/health` | `{"configured": false}` (env vars empty by default) |

**If validation fails:**
- Deploy fails: Check `databricks apps logs genie-workbench --profile fevm-prashanth`
- Import error in logs: Re-run `pip install -e ./packages/genie-space-optimizer` in the workspace
- Tab not showing: Verify `SpaceDetail.tsx` changes were included in the sync

---

## Batch 9 — Synced Tables Setup Script

### Prompt 9.1: Create Synced Tables Setup Script

```
Read docs/gso-architecture.md Section 5.3 and docs/gso-migration-guide.md
Phase 2 for the synced table setup pattern.

Create a script at scripts/setup_synced_tables.py that sets up the
Synced Tables for GSO Delta → Lakebase replication.

The script should:

1. Accept widget parameters:
   - source_catalog (default: "")
   - source_schema (default: "genie_space_optimizer")
   - lakebase_catalog (default: "")
   - lakebase_schema (default: "gso")

2. Enable CDF on all 8 source tables (idempotent ALTER TABLE SET TBLPROPERTIES)

3. Create synced tables using the Databricks SDK:
   - Sync mode: TRIGGERED
   - Primary keys per table:
     genie_opt_runs: ["run_id"]
     genie_opt_stages: ["run_id", "stage", "started_at"]
     genie_opt_iterations: ["run_id", "iteration", "eval_scope"]
     genie_opt_patches: ["run_id", "iteration", "lever", "patch_index"]
     genie_eval_asi_results: ["run_id", "iteration", "question_id", "judge"]
     genie_opt_provenance: ["run_id", "iteration", "lever", "question_id", "judge"]
     genie_opt_suggestions: ["suggestion_id"]
     genie_opt_data_access_grants: ["grant_id"]

4. Wait for initial sync to complete (poll status every 30s)

5. Verify row counts match between source and Lakebase

The script should be runnable standalone — no imports from the GSO package.

IMPORTANT: The script must also create the gso schema in Lakebase before
creating synced tables:
  CREATE SCHEMA IF NOT EXISTS gso;
```

**Validation:**

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Script exists | `ls scripts/setup_synced_tables.py` | File listed |
| 2 | 8 tables defined | `grep -c 'genie_opt_\|genie_eval_' scripts/setup_synced_tables.py` | At least 8 |
| 3 | CDF enabled | `grep 'enableChangeDataFeed' scripts/setup_synced_tables.py` | Found |
| 4 | Schema creation | `grep 'CREATE SCHEMA' scripts/setup_synced_tables.py` | Found |
| 5 | Syntax valid | `python -m py_compile scripts/setup_synced_tables.py` | No errors |

**If validation fails:**
- SDK import error: `pip install databricks-sdk` if not installed
- Missing widget support: Script should use `argparse` for CLI or `dbutils.widgets` for notebook

---

## Post-Integration Checklist

After executing all batches, verify end-to-end:

- [ ] `packages/genie-space-optimizer/` exists with GSO code
- [ ] `backend/services/gso_lakebase.py` has 7 async read functions
- [ ] `backend/routers/auto_optimize.py` has 10 endpoints (including health)
- [ ] `backend/main.py` mounts the auto_optimize router
- [ ] `app.yaml` has GSO_CATALOG, GSO_SCHEMA, GSO_JOB_ID, GSO_WAREHOUSE_ID
- [ ] `frontend/src/types/index.ts` has 9 GSO interfaces
- [ ] `frontend/src/lib/api.ts` has 10 GSO API functions (including health)
- [ ] `frontend/src/components/ui/` has 6 new primitives (checkbox, table, etc.)
- [ ] `frontend/src/components/auto-optimize/` has 9 component files
- [ ] `frontend/src/pages/SpaceDetail.tsx` has the Auto-Optimize tab
- [ ] `frontend/dist/` builds successfully
- [ ] App deploys and the Auto-Optimize tab renders
- [ ] Health endpoint returns `configured: true` when env vars are set
- [ ] Health endpoint returns `configured: false` when env vars are empty
- [ ] No `databricks-connect` import errors in app logs
- [ ] Existing tabs (Overview, Score, Analysis, Optimize, History) are unaffected

---

## Troubleshooting

### Import errors from genie_space_optimizer

If you see `ModuleNotFoundError: No module named 'genie_space_optimizer'`,
ensure `requirements.txt` has the `-e ./packages/genie-space-optimizer`
line and that the package is installed in the Python environment.

Note: `databricks-connect` is an optional `[spark]` extra in the GSO
package. The Workbench does not need it — the integration module uses the
SQL Warehouse Statement Execution API instead of Spark Connect.

### Auth bridge failures (403/500 on trigger)

1. Check that `GSO_JOB_ID` is set and the job exists
2. Check that the Workbench SP has CAN_MANAGE on the target Genie Space
3. Check that the Workbench SP has permissions on the GSO catalog/schema
4. Check the OBO token is present (test with `curl -H "Authorization: Bearer ..."`)
5. Check `GSO_WAREHOUSE_ID` is set (or `SQL_WAREHOUSE_ID` as fallback)

### Lakebase reads return empty

1. Verify the `gso` schema exists in Lakebase: `\dn gso` via psql
2. Verify synced tables exist: check Lakebase via SQL editor
3. Verify the schema is `gso` (matching the queries in `gso_lakebase.py`)
4. Trigger a manual sync if tables are stale
5. Check that `LAKEBASE_HOST` is set in `app.yaml`

### Frontend build failures

1. Check that all GSO types are exported from `types/index.ts`
2. Check that all API functions are exported from `lib/api.ts`
3. Check that component imports use `@/components/auto-optimize/...`
4. Check that the new UI primitives are installed (checkbox, table, etc.)
5. Run `npx tsc --noEmit` for detailed type errors

### Active run conflict (409)

The GSO prevents multiple concurrent optimization runs on the same space.
If a stale run is blocking, it can be resolved via the GSO's Delta tables
(mark the run as FAILED) or by waiting for the stale-queue timeout (10 min).

### Health endpoint returns configured: false

Both `GSO_CATALOG` and `GSO_JOB_ID` must be set to non-empty values.
Set them in the Databricks Apps UI under the app's environment variables.
`GSO_WAREHOUSE_ID` is also needed but falls back to `SQL_WAREHOUSE_ID`.
