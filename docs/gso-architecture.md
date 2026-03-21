# GSO Integration — Future State Architecture

> Integrating the Genie Space Optimizer (GSO) into the Genie Workbench as a
> third feature alongside GenieRx and GenieIQ.

## 1. Current State

The Genie Workbench is a Databricks App combining two features in one
FastAPI + React deployment:

- **GenieRx** — LLM-powered deep analysis and benchmark-driven optimization
  of Genie Spaces. Ephemeral (no persistence); results are streamed to the
  frontend via SSE and optionally applied to a new space.
- **GenieIQ** — Org-wide IQ scoring (0–100, four dimensions) with Lakebase
  (PostgreSQL) persistence. Stores scan results, score history, starred
  spaces, and seen spaces.

The Genie Space Optimizer (GSO) is a **separate standalone Databricks App**
with its own repository, backend, and frontend:

- A 6-stage optimization pipeline: preflight → baseline evaluation →
  proactive enrichment → adaptive lever loop → finalize → deploy.
- 9 automated evaluation judges via `mlflow.genai.evaluate()`.
- Delta table state management (8 tables) for runs, stages, iterations,
  patches, ASI results, provenance, suggestions, and data access grants.
- Databricks Jobs for compute orchestration (each stage is a notebook task).
- MLflow experiment tracking and LoggedModel versioning.
- Its own FastAPI backend (TanStack Router frontend).

```
┌──────────────────────────────────────┐   ┌──────────────────────────────────┐
│       Genie Workbench (current)      │   │     GSO Standalone (current)     │
│                                      │   │                                  │
│  FastAPI Server                      │   │  FastAPI Server                  │
│  ├─ GenieRx Router (analysis.py)     │   │  ├─ spaces.py (list, detail,     │
│  ├─ GenieIQ Router (spaces.py,       │   │  │   trigger optimization)       │
│  │   admin.py)                       │   │  ├─ runs.py (run detail,         │
│  ├─ Auth Router (auth.py)            │   │  │   comparison, apply/discard)  │
│  └─ Create Router (create.py)        │   │  ├─ trigger.py (programmatic)    │
│                                      │   │  └─ suggestions.py               │
│  Shared Services                     │   │                                  │
│  ├─ auth.py (OBO + SP)              │   │  Optimization Engine             │
│  ├─ genie_client.py                  │   │  ├─ harness.py (6-stage DAG)     │
│  ├─ llm_utils.py                     │   │  ├─ optimizer.py (lever logic)   │
│  └─ lakebase.py (asyncpg pool)       │   │  ├─ evaluation.py (9 judges)     │
│                                      │   │  ├─ state.py (Delta read/write)  │
│  Persistence: Lakebase (PostgreSQL)  │   │  └─ ...                          │
│  ├─ scan_results                     │   │                                  │
│  ├─ starred_spaces                   │   │  Persistence: Delta Tables       │
│  └─ seen_spaces                      │   │  ├─ genie_opt_runs               │
│                                      │   │  ├─ genie_opt_stages             │
│  Frontend: React + Vite              │   │  ├─ genie_opt_iterations          │
│  └─ SpaceDetail tabs:                │   │  ├─ genie_opt_patches            │
│     Overview, Score, Analysis,       │   │  ├─ genie_eval_asi_results       │
│     Optimize, History                │   │  ├─ genie_opt_provenance         │
│                                      │   │  ├─ genie_opt_suggestions        │
└──────────────────────────────────────┘   │  └─ genie_opt_data_access_grants │
                                           │                                  │
                                           │  Compute: Databricks Jobs        │
                                           │  Frontend: React + TanStack      │
                                           └──────────────────────────────────┘
```

---

## 2. Future State Architecture

The GSO becomes the **third feature** in the Workbench, following the same
adjacency pattern as GenieRx and GenieIQ — its own router, its own
persistence path, sharing auth and the Genie client.

```
┌───────────────────────────────────────────────────────────────────────┐
│                    Unified Genie Workbench                             │
│                                                                       │
│  FastAPI Server (backend/main.py)                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │ GenieRx  │  │ GenieIQ  │  │     GSO      │  │     Shared       │ │
│  │ analysis │  │ spaces   │  │ auto_optimize│  │     auth         │ │
│  │ optimize │  │ admin    │  │  (thin router)│  │     genie_client │ │
│  │ fix_agent│  │ scanner  │  │              │  │     llm_utils    │ │
│  │          │  │ lakebase │  │ gso_lakebase │  │     create       │ │
│  └──────────┘  └──────────┘  └──────┬───────┘  └──────────────────┘ │
│                                      │                                │
│                              imports from                             │
│                                      │                                │
│  ┌───────────────────────────────────▼──────────────────────────────┐ │
│  │  packages/genie-space-optimizer/ (git subtree)                   │ │
│  │  └─ src/genie_space_optimizer/                                   │ │
│  │     ├─ optimization/ (engine — untouched)                        │ │
│  │     ├─ backend/ (GSO routes, models, job_launcher)               │ │
│  │     ├─ common/ (config, UC metadata, genie_client)               │ │
│  │     └─ jobs/ (Databricks Job notebooks)                          │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  React Frontend (frontend/src/)                                       │
│  └─ SpaceDetail tabs:                                                 │
│     Overview, Score, Analysis, Optimize, Auto-Optimize, History       │
│                                      ▲                                │
│                                      └─ NEW tab                       │
│                                                                       │
│  Persistence                                                          │
│  ┌────────────────────────────────┐  ┌──────────────────────────────┐│
│  │ Lakebase (PostgreSQL)          │  │ Delta Tables (UC)            ││
│  │                                │  │                              ││
│  │ GenieIQ (direct read/write):   │  │ GSO source of truth:        ││
│  │ ├─ scan_results                │  │ ├─ genie_opt_runs            ││
│  │ ├─ starred_spaces              │◄─│ ├─ genie_opt_stages          ││
│  │ └─ seen_spaces                 │  │ ├─ genie_opt_iterations      ││
│  │                                │  │ ├─ genie_opt_patches         ││
│  │ GSO (synced, read-only):       │  │ ├─ genie_eval_asi_results    ││
│  │ ├─ gso.genie_opt_runs         │  │ ├─ genie_opt_provenance      ││
│  │ ├─ gso.genie_opt_stages       │  │ ├─ genie_opt_suggestions     ││
│  │ ├─ gso.genie_opt_iterations   │  │ └─ genie_opt_data_access_... ││
│  │ ├─ gso.genie_opt_patches      │  │                              ││
│  │ ├─ gso.genie_eval_asi_results │  │ ▲ Written by Databricks Jobs ││
│  │ ├─ gso.genie_opt_provenance   │  └──────────────────────────────┘│
│  │ └─ gso.genie_opt_suggestions  │                                   │
│  │                                │   Synced Tables (managed DLT     │
│  │ ▲ Read by FastAPI via asyncpg  │   pipelines, Triggered mode)     │
│  └────────────────────────────────┘                                   │
│                                                                       │
│  External Compute                                                     │
│  └─ Databricks Jobs (GSO optimization engine, 6-stage pipeline)       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 3. Backend Architecture

### 3.1 Router: `backend/routers/auto_optimize.py`

A thin proxy router with 10 endpoints. It does **not** contain optimization
logic — it delegates to the GSO integration module and reads state from
Lakebase.

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/auto-optimize/health` | Returns `{configured: true/false}` based on GSO env vars |
| `POST` | `/api/auto-optimize/trigger` | Create a QUEUED run in Delta, submit a Databricks Job |
| `GET` | `/api/auto-optimize/runs/{run_id}` | Full run detail (status, stages, scores, levers) from Lakebase |
| `GET` | `/api/auto-optimize/runs/{run_id}/status` | Lightweight poll endpoint (status, scores only) |
| `GET` | `/api/auto-optimize/levers` | List available optimization levers with descriptions |
| `POST` | `/api/auto-optimize/runs/{run_id}/apply` | Confirm optimization, apply final config to space |
| `POST` | `/api/auto-optimize/runs/{run_id}/discard` | Discard optimization, rollback to pre-optimization state |
| `GET` | `/api/auto-optimize/spaces/{space_id}/runs` | List past runs for a space (history table) |
| `GET` | `/api/auto-optimize/runs/{run_id}/iterations` | Per-iteration evaluation details |
| `GET` | `/api/auto-optimize/runs/{run_id}/asi-results` | Per-judge ASI failure analysis (query param: `iteration`) |

**Imports and dependencies:**

- `genie_space_optimizer.integration` — trigger, apply, discard, lever info
  (see [gso-upstream-rfc.md](gso-upstream-rfc.md) RFC-1 for the integration
  module specification)
- `backend.services.auth` — shared OBO/SP authentication
- `backend.services.gso_lakebase` — async read functions for synced tables

### 3.2 Service: `backend/services/gso_lakebase.py`

Async read functions for the GSO synced tables. Reuses the existing
`_pool` and `_lakebase_available` from `backend/services/lakebase.py`.

| Function | Table | Returns |
|----------|-------|---------|
| `load_gso_run(run_id)` | `gso.genie_opt_runs` | Single run dict or None |
| `load_gso_runs_for_space(space_id)` | `gso.genie_opt_runs` | List of run dicts |
| `load_gso_stages(run_id)` | `gso.genie_opt_stages` | List of stage dicts |
| `load_gso_iterations(run_id)` | `gso.genie_opt_iterations` | List of iteration dicts |
| `load_gso_patches(run_id)` | `gso.genie_opt_patches` | List of patch dicts |
| `load_gso_asi_results(run_id, iteration)` | `gso.genie_eval_asi_results` | List of ASI result dicts |
| `load_gso_suggestions(run_id)` | `gso.genie_opt_suggestions` | List of suggestion dicts |

Each function follows the established pattern:

```python
async def load_gso_run(run_id: str) -> dict | None:
    if not _lakebase_available or _pool is None:
        return None
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM gso.genie_opt_runs WHERE run_id = $1", run_id
        )
        return dict(row) if row else None
```

### 3.3 Configuration: `app.yaml`

New environment variables for the GSO integration:

```yaml
# Auto-Optimize (GSO engine)
- name: GSO_CATALOG
  value: ""                    # e.g. "my_catalog"
- name: GSO_SCHEMA
  value: "genie_space_optimizer"
- name: GSO_JOB_ID
  value: ""                    # Optimization job ID (from GSO bundle deploy)
- name: GSO_WAREHOUSE_ID
  valueFrom: sql-warehouse     # Reuse the existing warehouse resource
```

### 3.4 Mounting in `main.py`

```python
from backend.routers.auto_optimize import router as auto_optimize_router
# ... existing router imports ...

app.include_router(analysis_router)
app.include_router(spaces_router)
app.include_router(admin_router)
app.include_router(auth_router)
app.include_router(create_router)
app.include_router(auto_optimize_router)   # GSO
```

---

## 4. Frontend Architecture

### 4.1 New Tab in SpaceDetail

The `SpaceDetail.tsx` tab system is extended with an "Auto-Optimize" entry:

```typescript
type Tab = "overview" | "score" | "analysis" | "optimize" | "auto-optimize" | "history"

const tabs = [
  { id: "overview",      label: "Overview",      icon: <Eye /> },
  { id: "score",         label: "Score",         icon: <BarChart2 /> },
  { id: "analysis",      label: "Analysis",      icon: <Brain /> },
  { id: "optimize",      label: "Optimize",      icon: <Settings2 /> },
  { id: "auto-optimize", label: "Auto-Optimize", icon: <Rocket /> },   // NEW
  { id: "history",       label: "History",        icon: <Clock /> },
]
```

### 4.2 Full Frontend Design

The Auto-Optimize tab uses a three-layer design with 9 components. See
**Section 11** for the complete frontend architecture, component directory,
API functions, and types.

### 4.3 Types (`types/index.ts`)

```typescript
interface GSOTriggerRequest {
  space_id: string
  apply_mode: "genie_config" | "uc_artifact" | "both"
  levers?: number[]
  deploy_target?: string
}

interface GSOTriggerResponse {
  runId: string
  jobRunId: string
  jobUrl: string | null
  status: string
}

interface GSOLeverInfo {
  id: number
  name: string
  description: string
}

interface GSORunStatus {
  runId: string
  status: string
  spaceId: string
  startedAt: string | null
  completedAt: string | null
  baselineScore: number | null
  optimizedScore: number | null
  convergenceReason: string | null
}

interface GSOPipelineStep {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  summary: string | null
}

interface GSOPipelineRun {
  runId: string
  spaceId: string
  spaceName: string
  status: string
  startedAt: string
  completedAt: string | null
  baselineScore: number | null
  optimizedScore: number | null
  steps: GSOPipelineStep[]
  convergenceReason: string | null
}
```

---

## 5. State and Persistence

### 5.1 No State Conflicts

The Workbench and GSO track completely different data dimensions:

| System | Tables | What it tracks | Keyed by |
|--------|--------|---------------|----------|
| **GenieIQ** | `scan_results`, `starred_spaces`, `seen_spaces` | IQ scores, UI preferences | `space_id` |
| **GenieRx** | (none — ephemeral) | Analysis results, optimization suggestions | N/A |
| **GSO** | `genie_opt_runs`, `genie_opt_stages`, `genie_opt_iterations`, `genie_opt_patches`, `genie_eval_asi_results`, `genie_opt_provenance`, `genie_opt_suggestions`, `genie_opt_data_access_grants` | Optimization run lifecycle, audit trail | `run_id` (references `space_id`) |

Both reference `space_id` as a foreign key to the Databricks Genie API, but
they never share rows or write to each other's tables.

The only concurrent-write concern is the **Genie API itself**: both the
Workbench Fix Agent and GSO can PATCH a space's config. The GSO handles
this via HTTP 409 (conflict) when an active run already exists for a space.

### 5.2 Synced Tables: Delta → Lakebase

The GSO optimization jobs write to **Delta tables** (source of truth). The
Workbench reads from **Lakebase** (read replica) via Synced Tables.

```
Databricks Jobs ──writes──▶ Delta Tables ──Synced Tables──▶ Lakebase
(GSO engine)                (UC, source     (managed DLT     (PostgreSQL,
                             of truth)       pipeline,        read replica)
                                             Triggered mode)
                                                                    │
FastAPI Server ◀──reads via asyncpg────────────────────────────────┘
(auto_optimize.py)
```

**Sync mode**: Triggered (incremental, on-demand). Only changes since the
last sync are applied. Can be triggered at the end of each job task or via
a table-update trigger in Lakeflow Jobs.

**Latency**: Seconds after trigger. Acceptable for the 5-second polling
interval in the frontend.

**Fallback**: If Lakebase is unavailable, the `gso_lakebase.py` functions
return `None` / empty lists (same pattern as the existing `lakebase.py`
in-memory fallback).

### 5.3 Synced Table Setup

Each of the 8 Delta tables is synced to a `gso` schema in Lakebase. The
schema must be created before the first sync:

```sql
-- Run against Lakebase (via psql or SQL editor)
CREATE SCHEMA IF NOT EXISTS gso;
```

Then create the synced tables using the Databricks SDK:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)

TABLES_TO_SYNC = [
    ("genie_opt_runs",              ["run_id"]),
    ("genie_opt_stages",            ["run_id", "stage", "started_at"]),
    ("genie_opt_iterations",        ["run_id", "iteration", "eval_scope"]),
    ("genie_opt_patches",           ["run_id", "iteration", "lever", "patch_index"]),
    ("genie_eval_asi_results",      ["run_id", "iteration", "question_id", "judge"]),
    ("genie_opt_provenance",        ["run_id", "iteration", "lever", "question_id", "judge"]),
    ("genie_opt_suggestions",       ["suggestion_id"]),
    ("genie_opt_data_access_grants",["grant_id"]),
]
```

See [Synced Tables documentation](https://docs.databricks.com/aws/en/oltp/instances/sync-data/sync-table)
for full setup instructions.

---

## 6. Code Management — Git Subtree

The GSO repository is embedded into the Workbench monorepo via **git subtree**
at `packages/genie-space-optimizer/`.

### 6.1 Directory Layout

```
databricks-genie-workbench/
├── backend/
│   ├── main.py
│   ├── routers/
│   │   ├── analysis.py          # GenieRx
│   │   ├── spaces.py            # GenieIQ
│   │   ├── admin.py             # GenieIQ
│   │   ├── auth.py              # Shared
│   │   ├── create.py            # Shared
│   │   └── auto_optimize.py     # GSO (NEW)
│   └── services/
│       ├── auth.py              # Shared
│       ├── genie_client.py      # Shared
│       ├── llm_utils.py         # Shared
│       ├── lakebase.py          # GenieIQ + shared pool
│       └── gso_lakebase.py      # GSO reads (NEW)
│
├── frontend/src/
│   ├── pages/SpaceDetail.tsx    # Add Auto-Optimize tab
│   ├── components/
│   │   ├── auto-optimize/       # GSO components (NEW)
│   │   │   ├── AutoOptimizeTab.tsx
│   │   │   ├── OptimizationConfig.tsx
│   │   │   ├── PipelineMonitor.tsx
│   │   │   └── PipelineStepCard.tsx
│   │   └── ...existing components...
│   ├── lib/api.ts               # Add GSO API functions
│   └── types/index.ts           # Add GSO types
│
├── packages/
│   └── genie-space-optimizer/   # Git subtree of GSO repo
│       ├── src/genie_space_optimizer/
│       │   ├── optimization/    # Engine (untouched)
│       │   ├── backend/         # GSO routes, models, job_launcher
│       │   ├── common/          # Config, UC metadata
│       │   └── jobs/            # Databricks Job notebooks
│       └── pyproject.toml
│
├── requirements.txt             # includes: -e ./packages/genie-space-optimizer
├── app.yaml                     # Add GSO env vars
└── ...
```

**Dependency management:** The GSO package lists `databricks-connect` as a
core dependency in its `pyproject.toml`. The Workbench does **not** need
Spark Connect — it uses the warehouse-first integration path (see
Section 8.4). After the upstream changes in
[gso-upstream-rfc.md](gso-upstream-rfc.md) RFC-4, install without the
spark extra:

```
# In requirements.txt
-e ./packages/genie-space-optimizer
```

The GSO package makes `databricks-connect` an optional extra (`[spark]`),
so the standard install does not pull it in.

### 6.2 Subtree Workflows

**Initial setup:**

```bash
git remote add gso https://github.com/prashsub/Genie_Space_Optimizer.git
git subtree add --prefix=packages/genie-space-optimizer gso main --squash
```

**Pushing GSO changes back to the standalone repo:**

```bash
git subtree push --prefix=packages/genie-space-optimizer gso <branch>
```

**Pulling upstream GSO updates into the Workbench:**

```bash
git subtree pull --prefix=packages/genie-space-optimizer gso main --squash
```

**For other Workbench contributors:** No special setup. The subtree is just
regular files. `git clone` works normally.

---

## 7. Data Flow Summary

### 7.1 Trigger Flow (user clicks "Start Optimization")

```
Frontend                    Backend                     External
────────                    ───────                     ────────
AutoOptimizeTab             auto_optimize.py            Databricks
  │                           │                           │
  ├─ POST /trigger ──────────▶│                           │
  │                           ├─ OBO auth check           │
  │                           ├─ Permission check (SP)    │
  │                           ├─ Create QUEUED run ──────▶│ Delta
  │                           ├─ submit_optimization() ──▶│ Jobs API
  │                           │   (jobs.run_now)          │
  │                           ├─ Update run to            │
  │                           │   IN_PROGRESS ───────────▶│ Delta
  ◀─ { runId, jobUrl } ──────┤                           │
  │                           │                           │
  │ (switch to monitoring)    │                           │
```

### 7.2 Monitoring Flow (polling every 5 seconds)

```
Frontend                    Backend                     External
────────                    ───────                     ────────
PipelineMonitor             auto_optimize.py
  │                           │
  ├─ GET /runs/{id}/status ──▶│
  │                           ├─ load_gso_run() ────────▶ Lakebase
  │                           ├─ load_gso_stages() ─────▶ Lakebase
  ◀─ { status, steps, ... } ─┤
  │                           │
  │ (repeat every 5s          │
  │  until terminal status)   │
```

### 7.3 Apply/Discard Flow

```
Frontend                    Backend                     External
────────                    ───────                     ────────
PipelineMonitor             auto_optimize.py
  │                           │
  ├─ POST /runs/{id}/apply ──▶│
  │                           ├─ Apply optimized config   │
  │                           │   to Genie Space ────────▶ Genie API
  │                           ├─ Update run status ──────▶ Delta
  ◀─ { status: "APPLIED" } ──┤
```

---

## 8. Initialization Bridge

The GSO standalone app performs three bootstrap steps at startup. In the
integrated Workbench, these must be handled by the thin router or at app
startup.

### 8.1 GSO Standalone Bootstrap (for reference)

| Bootstrap step | What it does | GSO implementation |
|---|---|---|
| `_DeltaTableBootstrap` | Creates 8 Delta tables via `ensure_optimization_tables(spark, catalog, schema)` if they don't exist | Runs at FastAPI lifespan startup |
| `_UCGrantBootstrap` | Grants the SP `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT` on the GSO schema + UC Volume | Runs at FastAPI lifespan startup |
| `_JobRunAsBootstrap` | Ensures the optimization job's `run_as` identity matches the current app SP | Runs at FastAPI lifespan startup |

### 8.2 Workbench Strategy: Lazy Initialization

The Workbench should NOT replicate these as startup steps (they require
Spark Connect, which adds cold-start latency). Instead, use **lazy init
on first trigger**:

```
User clicks "Start Optimization"
  │
  ▼
auto_optimize.py: POST /trigger
  │
  ├─ 1. Build GSO AppConfig from Workbench env vars (GSO_CATALOG, etc.)
  ├─ 2. Build WorkspaceClient (SP) from Workbench's get_service_principal_client()
  ├─ 3. Call do_start_optimization() — this internally calls:
  │     ├─ ensure_optimization_tables() (idempotent, creates if missing)
  │     ├─ fetch_space_config() (captures space snapshot)
  │     ├─ create_run() (writes QUEUED to Delta)
  │     └─ submit_optimization() (jobs.run_now)
  │
  ▼
  Return { runId, jobRunId, jobUrl }
```

The `do_start_optimization()` function in `spaces.py` already handles
table creation internally (via `ensure_optimization_tables` in its
`_init_spark_state()` helper). UC grants and job run_as alignment are
handled by the GSO's Databricks Asset Bundle deployment and do not need
to run in the Workbench process.

### 8.3 What the Workbench Skips

| GSO bootstrap step | Workbench equivalent | Why |
|---|---|---|
| `_DeltaTableBootstrap` | Handled inside `do_start_optimization()` | Idempotent; runs on first trigger |
| `_UCGrantBootstrap` | Handled by GSO bundle deploy (`databricks bundle deploy`) | Grants are persistent; run once during GSO setup |
| `_JobRunAsBootstrap` | Handled by GSO bundle deploy | Job config is persistent |

### 8.4 Warehouse-First Mode (Workbench Integration Path)

The Workbench does **not** install `databricks-connect` and cannot create
Spark sessions. All Delta state operations use the **SQL Statement
Execution API** via the configured SQL Warehouse (`GSO_WAREHOUSE_ID`).

The GSO provides shared warehouse helpers (`sql_warehouse_query`,
`sql_warehouse_execute`, `wh_create_run`, `wh_load_run` in
`common/warehouse.py`) that bypass Spark Connect entirely. The
integration module (`genie_space_optimizer.integration`) uses these
as the **primary path** for external callers:

```
trigger_optimization()     → warehouse-first (no Spark)
apply_optimization()       → warehouse-first (no Spark)
discard_optimization()     → warehouse read + REST rollback (no Spark)
```

The optimization engine itself (Databricks Jobs) continues to use Spark
internally — that's fine since Jobs have their own compute cluster.

---

## 9. Auth Bridge

The Workbench and GSO use the same OBO + SP pattern but with different
implementations. The thin router must bridge them.

### 9.1 Auth Implementation Comparison

| Aspect | Workbench | GSO |
|--------|-----------|-----|
| OBO token source | `x-forwarded-access-token` header | `X-Forwarded-Access-Token` header |
| OBO storage | `ContextVar` in `backend/services/auth.py` | FastAPI dependency `Dependencies.UserClient` |
| OBO client creation | `set_obo_user_token(token)` in middleware | `_get_user_ws(headers)` in `core/_defaults.py` |
| SP client | `get_service_principal_client()` (singleton) | `Dependencies.Client` (lifespan singleton) |
| SP creation | `WorkspaceClient()` (auto-resolves env) | `WorkspaceClient()` (auto-resolves env) |
| User identity | Extracted by middleware, not passed to services | `DatabricksAppsHeaders` dataclass injected per-request |

### 9.2 Bridge Pattern in `auto_optimize.py`

The thin router extracts auth from the Workbench's system and calls the
GSO integration module functions with explicit arguments:

```python
from backend.services.auth import (
    get_workspace_client,           # returns OBO client if set, else SP
    get_service_principal_client,   # always returns SP
)
from genie_space_optimizer.integration import (
    trigger_optimization,
    apply_optimization,
    discard_optimization,
    IntegrationConfig,
)

@router.post("/api/auto-optimize/trigger")
async def trigger(body: TriggerRequest, request: Request):
    ws = get_workspace_client()              # OBO client
    sp_ws = get_service_principal_client()    # SP client
    config = _build_gso_config()

    result = trigger_optimization(
        space_id=body.space_id,
        ws=ws,
        sp_ws=sp_ws,
        config=config,
        user_email=request.headers.get("x-forwarded-email"),
        user_name=request.headers.get("x-forwarded-preferred-username"),
        apply_mode=body.apply_mode,
        levers=body.levers,
        deploy_target=body.deploy_target,
    )
    return {"runId": result.run_id, "jobRunId": result.job_run_id,
            "jobUrl": result.job_url, "status": result.status}

@router.post("/api/auto-optimize/runs/{run_id}/apply")
async def apply(run_id: str):
    ws = get_workspace_client()              # OBO only — no SP needed
    config = _build_gso_config()
    result = apply_optimization(run_id, ws, config)
    return {"status": result.status, "runId": result.run_id, "message": result.message}

@router.post("/api/auto-optimize/runs/{run_id}/discard")
async def discard(run_id: str):
    ws = get_workspace_client()              # OBO client
    sp_ws = get_service_principal_client()    # SP client (for Genie API rollback)
    config = _build_gso_config()
    result = discard_optimization(run_id, ws, sp_ws, config)
    return {"status": result.status, "runId": result.run_id, "message": result.message}
```

**Note:** `apply_optimization` takes only the OBO client (no SP). The
`discard_optimization` takes both OBO and SP because the rollback needs
a Genie API client (which may use either, depending on OAuth scope
availability).

### 9.3 Config Adapter

The router constructs an `IntegrationConfig` from the Workbench's env vars.
After the upstream changes ([gso-upstream-rfc.md](gso-upstream-rfc.md)
RFC-1), this is a standard dataclass provided by the GSO package:

```python
from genie_space_optimizer.integration import IntegrationConfig

def _build_gso_config() -> IntegrationConfig:
    return IntegrationConfig(
        catalog=os.environ.get("GSO_CATALOG", ""),
        schema_name=os.environ.get("GSO_SCHEMA", "genie_space_optimizer"),
        warehouse_id=os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", ""),
        job_id=int(os.environ["GSO_JOB_ID"]) if os.environ.get("GSO_JOB_ID") else None,
    )
```

No headers adapter is needed — user identity is passed as keyword arguments
(`user_email`, `user_name`) directly to `trigger_optimization()`.

### 9.4 Permission Model

```
User (browser)
  │
  ├─ OBO token (via Databricks Apps proxy)
  │   │
  │   ├─ List Genie Spaces ──▶ Workbench's existing genie_client.py
  │   ├─ Fetch space config ──▶ Workbench's existing genie_client.py
  │   ├─ Permission check ────▶ GSO's user_can_edit_space(ws, space_id)
  │   └─ UC metadata prefetch ▶ GSO's _fetch_uc_metadata_obo()
  │
  └─ SP (app service principal)
      │
      ├─ Submit Databricks Job ▶ GSO's submit_optimization(sp_ws, ...)
      ├─ SP permission check ──▶ GSO's sp_can_manage_space(sp_ws, space_id)
      ├─ Delta table writes ───▶ GSO's create_run() / update_run_status()
      └─ Apply/discard config ─▶ GSO's applier.rollback() / patch_space_config()
```

**Required SP permissions** (same as GSO standalone):
- CAN_MANAGE on each target Genie Space
- SELECT on data schemas referenced by the space
- USE CATALOG / USE SCHEMA on the GSO catalog/schema
- Access to submit/manage the optimization Databricks Job

**Required OAuth scopes** (additive to existing Workbench scopes):
- `dashboards.genie` (already required by Workbench)
- `sql` (already required by Workbench)
- `catalog.catalogs:read`, `catalog.schemas:read`, `catalog.tables:read`
  (already required by Workbench)

No additional OAuth scopes are needed beyond what the Workbench already
configures.

---

## 10. Shared Genie Context

### 10.1 Space Config Loading

Both apps need to load Genie Space configurations. The Workbench already
does this in `backend/services/genie_client.py`:

- `get_genie_space(space_id)` — fetches and parses serialized config
- `list_genie_spaces()` — lists all spaces visible to the user
- `get_serialized_space(space_id)` — returns raw serialized space JSON

The GSO has its own `common/genie_client.py` with:

- `fetch_space_config(ws, space_id)` — same API call, different parser
- `list_spaces(ws)` — same pagination logic

### 10.2 How the Workbench Shares Context with GSO

When the user triggers an optimization from the Auto-Optimize tab, the
Workbench already has the space loaded (it's displayed in SpaceDetail).
The GSO's `do_start_optimization()` re-fetches the space config internally
as a snapshot. This is **intentional** — the GSO captures the config at
trigger time for versioning/rollback purposes.

The flow:

```
SpaceDetail (frontend)
  │ user already viewing this space
  │ (space config already loaded for Overview/Score/Analysis tabs)
  │
  ├─ POST /auto-optimize/trigger { space_id }
  │
  ▼
auto_optimize.py
  │
  ├─ Calls do_start_optimization(space_id, ws, sp_ws, ...)
  │   │
  │   ├─ fetch_space_config(ws, space_id)  ← re-fetches via OBO (intentional)
  │   ├─ Stores config_snapshot in Delta    ← for versioning
  │   ├─ _fetch_uc_metadata_obo(ws, ...)   ← UC metadata via OBO
  │   └─ submit_optimization(sp_ws, ...)   ← job via SP
  │
  ▼
  { runId, jobUrl }
```

The Workbench does NOT need to pre-load or cache the space config for the
GSO. The GSO handles its own snapshot internally. This keeps the integration
clean — the thin router only passes `space_id`, not the full config.

### 10.3 Programmatic API Trigger

The GSO standalone app exposes `POST /api/genie/trigger` for CI/CD and
script-based optimization. The Workbench equivalent is:

```
POST /api/auto-optimize/trigger
Body: {
    "space_id": "01abc...",
    "apply_mode": "genie_config",
    "levers": [1, 2, 3, 4, 5],
    "deploy_target": null
}
Response: {
    "runId": "uuid",
    "jobRunId": "12345",
    "jobUrl": "https://workspace.cloud.databricks.com/jobs/...",
    "status": "IN_PROGRESS"
}
```

This endpoint is callable from external systems (CI pipelines, notebooks,
scripts) with the same OBO/SP auth model. The Databricks Apps proxy
handles authentication — external callers send a PAT or SP token in the
Authorization header, which the proxy converts to `x-forwarded-access-token`.

---

## 11. Frontend Architecture (Three-Layer Design)

### 11.1 Layer Overview

The Auto-Optimize tab has three levels of depth:

| Layer | What | Where | Users |
|-------|------|-------|-------|
| Layer 1: Tab | Trigger optimization + run history table | `AutoOptimizeTab.tsx` | Everyone |
| Layer 2: Benchmark Eval | Question list, SQL comparison, pass/fail | `RunDetailView.tsx` | Everyone |
| Layer 3: Pipeline Details | Full GSO transparency (stages, judges, patches) | `PipelineDetailsModal.tsx` | Power users |

### 11.2 Layer 1 — Auto-Optimize Tab

Embedded in SpaceDetail as a new tab. Shows:
- Configuration card (levers, apply mode, start button)
- Current active run card (if running)
- Past optimization runs table (date, status, baseline, optimized, link)

### 11.3 Layer 2 — Benchmark Evaluation Page

Opened by clicking "View Details" on a run. Designed to resemble the
Genie native Benchmarks page:
- Left sidebar: question list with pass/fail icons, search, filter
- Main area: selected question's SQL comparison (Model vs Ground Truth)
- Results table below SQL
- Score banner at top: "92% accurate (23/25)"
- Gear icon in the banner opens Layer 3

### 11.4 Layer 3 — Pipeline Details (Power User)

Opened by clicking the gear icon. Shows the full GSO pipeline view:
- 6-step pipeline progress cards
- Iteration chart and stage timeline
- Per-judge scores
- Patch audit trail

Layer 3 components are built natively in the Workbench (Option C from
the frontend analysis), using the GSO subtree code as a reference for
data shapes and layout. Simplified initially, with full depth added
incrementally based on user demand.

### 11.5 Component Directory

```
frontend/src/components/auto-optimize/
├── AutoOptimizeTab.tsx          # Layer 1: tab container
├── OptimizationConfig.tsx       # Layer 1: levers, apply mode, start
├── RunHistoryTable.tsx          # Layer 1: past runs table
├── RunDetailView.tsx            # Layer 2: benchmark eval page
├── QuestionList.tsx             # Layer 2: sidebar question list
├── QuestionDetail.tsx           # Layer 2: SQL comparison + results
├── PipelineDetailsModal.tsx     # Layer 3: shell (gear icon opens)
├── PipelineStepCard.tsx         # Layer 3: individual step card
└── ScoreSummary.tsx             # Shared: baseline vs optimized display
```

### 11.6 Theme Compatibility

Both apps use shadcn/ui + Tailwind + CVA. The Workbench has a custom
theme defined in `frontend/src/index.css` with tokens like `--bg-primary`,
`--bg-surface`, `--color-accent`. The GSO has been provided a
`workbench-ui-kit/` package (see `docs/workbench-ui-kit/`) to adopt the
same visual identity. All Auto-Optimize components use the Workbench's
existing `components/ui/*` primitives directly.

### 11.7 Required UI Primitives

The Workbench currently has 8 shadcn/ui components: accordion, badge,
button, card, input, progress, tabs, textarea. The Auto-Optimize tab
requires additional primitives:

| Component | Used by | Purpose |
|-----------|---------|---------|
| `checkbox` | `OptimizationConfig.tsx` | Lever selection checkboxes |
| `table` | `RunHistoryTable.tsx` | Past runs table |
| `collapsible` | `OptimizationConfig.tsx` | Deploy target, advanced options |
| `tooltip` | `PipelineStepCard.tsx` | Step status hover info |
| `skeleton` | `AutoOptimizeTab.tsx` | Loading states |
| `alert-dialog` | `OptimizationConfig.tsx` | UC write-back confirmation |

Install before building components:
```bash
cd frontend && npx shadcn@latest add checkbox table collapsible tooltip skeleton alert-dialog
```

### 11.8 Layer 2 Design Intent

Layer 2 (RunDetailView) is intentionally designed to resemble the **Genie
native Benchmarks page**, not the GSO standalone run detail page. This gives
users a familiar evaluation experience within the Workbench. The GSO's
pipeline-centric view (stages, iteration explorer, TransparencyPane) is
available as Layer 3 (PipelineDetailsModal) for power users.

---

## 12. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Regression to existing features | Auto-Optimize is an additive tab. Score, Analysis, Optimize, and History tabs are unchanged. |
| Backend conflicts | New router is separate from existing routers. Only imports from GSO package and shared services. |
| Frontend conflicts | New components in their own `auto-optimize/` directory. Only `SpaceDetail.tsx` gets a tab entry. |
| GSO engine stability | Runs as Databricks Jobs exactly as designed. No modifications to optimization logic. |
| Lakebase unavailability | GSO reads return `None` / empty lists (same fallback pattern as GenieIQ). |
| Synced table lag | UI shows slightly stale data during sync delay. 5s polling naturally retries. |
| Concurrent space modifications | GSO's `do_start_optimization()` checks for active runs (HTTP 409). |
| Subtree merge conflicts | Squash merges keep history clean. Conflicts are localized to `packages/`. |
| GSO not configured | Health endpoint returns `configured: false`; UI shows informational card (Section 12.5). |

### 12.5 Graceful Degradation (GSO Not Configured)

When `GSO_CATALOG` or `GSO_JOB_ID` are empty (their defaults in
`app.yaml`), the Auto-Optimize feature cannot function. The Workbench
handles this gracefully:

1. **Health endpoint:** `GET /api/auto-optimize/health` returns
   `{"configured": true/false}` based on whether both `GSO_CATALOG` and
   `GSO_JOB_ID` are set.

2. **Frontend:** `AutoOptimizeTab` calls the health endpoint on mount. If
   not configured, it shows an informational card instead of the
   configuration form:
   > "Auto-Optimize is not configured for this deployment. Contact your
   > administrator to set GSO_CATALOG and GSO_JOB_ID."

3. **Trigger endpoint:** Returns HTTP 503 with a clear message if GSO env
   vars are missing.

### 12.6 Lever 0 (Proactive Enrichment)

The GSO's `LEVER_NAMES` dictionary includes lever 0 ("Proactive
Enrichment"), which is a preparatory stage that always runs before the
adaptive lever loop. It is **not user-selectable** and should not appear
as a checkbox in the UI.

The user-facing levers are 1-5 (`DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5]`):

| Lever | Name | Description |
|-------|------|-------------|
| 1 | Tables & Columns | Update table descriptions, column descriptions, and synonyms |
| 2 | Metric Views | Update metric view column descriptions |
| 3 | Table-Valued Functions | Remove underperforming TVFs |
| 4 | Join Specifications | Add, update, or remove join relationships between tables |
| 5 | Genie Space Instructions | Rewrite global routing instructions and add domain-specific guidance |

### 12.7 Apply Mode Values

The GSO supports three `apply_mode` values:

| Value | Description | UI |
|-------|-------------|-----|
| `"genie_config"` | Apply changes only to Genie Space configuration | Default, always available |
| `"uc_artifact"` | Apply UC-level changes only (DDL on tables/columns) | Not yet exposed in UI |
| `"both"` | Apply both Genie Space config and UC-level changes | Shown but disabled ("Coming soon") |
