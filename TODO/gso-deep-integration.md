# GSO Deep Integration Plan

## Context

GSO (Genie Space Optimizer) was originally a standalone Databricks App that got copy-pasted wholesale into `packages/genie-space-optimizer/`. It brought with it an entire standalone project's worth of artifacts: its own IDE configs (`.claude/`, `.cursor/`, `.vscode/`), deploy scripts (`deploy.sh`, `destroy.sh`, `Makefile`), frontend build system (`package.json`, `bun.lock`, `tsconfig.json`), 7 standalone doc files, test screenshots, a 1MB `uv.lock`, and its own `app.yml`. The Workbench only imports ~500-800 lines of GSO's ~30,000+ lines at runtime. The rest is either standalone app scaffolding (not needed) or the Databricks Job optimization engine (runs on separate compute, never in the app process).

**Goal**: Clean out the standalone app artifacts, absorb what the Workbench actually uses into `backend/`, and leave `packages/` containing only the optimization engine + job DAG.

---

## Phase 0: Clean Up Standalone App Artifacts (Delete Dead Weight) — DONE

Deleted all standalone app scaffolding from `packages/genie-space-optimizer/`. `backend/` and `ui/` inside `src/` deferred to Phase 1 (runtime imports depend on them).

### What was deleted:
- IDE configs: `.claude/`, `.cursor/`, `.vscode/`, `.mcp.json`
- Deploy pipeline: `app.yml`, `deploy.sh`, `deploy-config.sh`, `destroy.sh`, `Makefile`, `resources/`
- Frontend build: `package.json`, `bun.lock`, `tsconfig.json`
- Docs: `README.md`, `QUICKSTART.md`, `CHANGELOG.md`, `CODE_REVIEW.md`, `DEPLOYMENT.md`, `E2E_TESTING_GUIDE.md`, `CLAUDE.md`, `docs/`, `genie_space_optimizer/` (12 architecture markdown files)
- Scaffolding: `.gitignore`, `.python-version`, `uv.lock`
- Test artifacts: `browser-test-output/`, `scripts/`, `tests/`

### What remains after Phase 0:
```
packages/genie-space-optimizer/
  pyproject.toml              # Needed to build wheel for jobs
  databricks.yml              # Bundle config for the job resource
  src/genie_space_optimizer/
    __init__.py
    _metadata.py, _version.py # Package build metadata
    backend/                  # Standalone backend (delete in Phase 1D after absorbing utils.py)
    common/                   # Shared utilities (used by both Workbench and jobs)
    integration/              # Thin integration layer (absorb in Phase 1A)
    optimization/             # Core optimization engine (Spark-heavy, jobs only)
    jobs/                     # 6-task Databricks Job DAG notebooks
    ui/                       # Standalone frontend (delete in Phase 1D)
```

### What remains after Phase 0 + Phase 1:
```
packages/genie-space-optimizer/
  pyproject.toml              # Needed to build wheel for jobs
  databricks.yml              # Bundle config for the job resource
  src/genie_space_optimizer/
    common/                   # Shared utilities (used by both Workbench and jobs)
    optimization/             # Core optimization engine (Spark-heavy, jobs only)
    jobs/                     # 6-task Databricks Job DAG notebooks
```

---

## Phase 1: Absorb Integration Code into `backend/`

Move the thin integration layer and utils into the main app. `common/` stays in `packages/` (shared by both Workbench and jobs).

### 1A. Create `backend/services/gso/` module

| Source (packages/) | Destination (backend/services/gso/) |
|---|---|
| `integration/trigger.py` | `trigger.py` |
| `integration/apply.py` | `apply.py` |
| `integration/discard.py` | `discard.py` |
| `integration/levers.py` | `levers.py` |
| `integration/config.py` | `config.py` (IntegrationConfig dataclass) |
| `integration/types.py` | `types.py` (TriggerResult, ActionResult) |
| `backend/utils.py` | `utils.py` (safe_int, safe_float, etc.) |

These files import from `genie_space_optimizer.common.*` — those imports stay as-is since `common/` remains in `packages/`.

### 1B. Update Workbench imports
- **Files**: `backend/routers/auto_optimize.py`, `backend/services/scanner.py`
- **What**: Change `from genie_space_optimizer.integration` → `from backend.services.gso`; change `from genie_space_optimizer.backend.utils` → `from backend.services.gso.utils`. Imports from `genie_space_optimizer.common.*` stay unchanged.

### 1C. Move GSO Pydantic models to `backend/models.py`
- Move `TriggerRequest`, `SchemaAccessStatus`, `PermissionCheckResponse` from inline in `auto_optimize.py`.
- Move duplicated `LEVER_NAMES` and `_TERMINAL_RUN_STATUSES` constants from `auto_optimize.py` into `backend/services/gso/config.py` (import from `common/config.py` as source of truth).

### 1D. Delete `integration/` and `backend/` from packages
- After absorption, delete `packages/.../src/genie_space_optimizer/integration/` (now in `backend/services/gso/`)
- Delete `packages/.../src/genie_space_optimizer/backend/` (standalone app, fully replaced by Workbench)

### Verification
- `grep -r "from genie_space_optimizer.integration" backend/` returns zero results
- `grep -r "from genie_space_optimizer.backend" backend/` returns zero results
- `from genie_space_optimizer.common` imports still work (package still installed)
- All `/api/auto-optimize/*` endpoints work identically
- IQ scan checks #11/#12 still populate
- GSO Databricks Job still builds and deploys via `databricks bundle deploy`

---

## Phase 2: Frontend Constants Consolidation

### 2A. Extract `STATUS_VARIANT` and `TERMINAL_STATUSES`
- **Files**: New `frontend/src/lib/gso-constants.ts`
- **What**: `STATUS_VARIANT` duplicated in 4 component files → one shared constant.

---

## Phase 3: Bridge Score-to-Optimize Journey (Highest UX Impact)

### 3A. "Run Optimization" CTA in IQScoreTab
- **Files**: `frontend/src/pages/IQScoreTab.tsx`, `frontend/src/pages/SpaceDetail.tsx`
- **What**: Add button next to "Fix with AI Agent" → navigates to Optimize tab.

### 3B. Actionable optimization checks
- **Files**: `frontend/src/pages/IQScoreTab.tsx`
- **What**: Failed checks #11/#12 clickable → navigate to Optimize tab.

### 3C. Post-optimization re-scan prompt
- **Files**: `frontend/src/components/auto-optimize/AutoOptimizeTab.tsx`, `frontend/src/pages/SpaceDetail.tsx`
- **What**: Terminal state → "Re-scan to see updated IQ score" banner.

---

## Phase 4: Unify History Timeline

### 4A. Backend: Merge scan + optimization events
- **Files**: `backend/routers/spaces.py`, `backend/services/gso_lakebase.py`
- **What**: Extend history endpoint to include optimization runs with `type` field.

### 4B. Frontend: Unified timeline
- **Files**: `frontend/src/pages/HistoryTab.tsx`, `frontend/src/types/index.ts`
- **What**: Show optimization events as annotated markers alongside scan history.

---

## Phase 5: Lift GSO State + UI Polish

### 5A. Lift core GSO state to SpaceDetail
- **Files**: `frontend/src/pages/SpaceDetail.tsx`, `frontend/src/components/auto-optimize/AutoOptimizeTab.tsx`
- **What**: SpaceDetail tracks `activeRunId`, `runStatus`, `latestCompletedRun`. Pass as props.

### 5B. Enrich Overview tab
- Show step progress in active-run banner. Show "Last optimized" in header when completed run exists.

### 5C. Hide polling implementation detail
- Remove "Polling every 5s..." footer. Polling stays (correct for Lakeflow jobs, HTTP timeout constraint).

### 5D. Replace PipelineDetailsModal with inline expandable
- Convert modal to accordion/collapsible card matching SpaceOverview pattern.

---

## Recommended Order

1. ~~**Phase 0** — delete standalone artifacts (pure cleanup, no behavior change)~~ **DONE**
2. **Phase 1** — absorb runtime code into `backend/services/gso/`
3. **Phase 2** — frontend constants consolidation
4. **Phase 3** — Score-to-Optimize UX bridges
5. **Phase 4** — unified History timeline
6. **Phase 5** — state lifting + UI polish

## Verification

- ~~After Phase 0: `packages/genie-space-optimizer/` contains only `pyproject.toml`, `databricks.yml`, and `src/` (with `common/`, `integration/`, `optimization/`, `jobs/`). No IDE configs, docs, deploy scripts, or frontend build files.~~ **VERIFIED**
- After Phase 1: `backend/services/gso/` contains absorbed integration + utils. `packages/` further slimmed to `common/`, `optimization/`, `jobs/`. Zero `genie_space_optimizer.integration` or `genie_space_optimizer.backend` imports in `backend/`. All endpoints work. Job still deploys.
- After Phase 3: Score tab → "Run Optimization" → Optimize tab. Optimization completes → "Re-scan" → Score tab.
- After Phase 4: History tab shows scan + optimization events on one timeline.
