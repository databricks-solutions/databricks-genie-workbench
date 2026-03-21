# Code Review: Genie Space Optimizer

**Date:** 2026-02-24
**Scope:** Full-stack audit covering backend (FastAPI), frontend (React/TypeScript), jobs, configuration, and deployment artifacts.

---

## Executive Summary

The codebase is **architecturally sound** with strong patterns (DI, Suspense boundaries, persistent DAG jobs, OBO fallback). Frontend-backend integration is fully consistent: all 11 API endpoints have matching hooks, models align perfectly, and env vars flow correctly from `databricks.yml` through `AppConfig`. No TODO/FIXME markers exist.

The main areas for improvement are **code duplication** (NaN handling appears in 4 places, status constants in 2, reconciliation logic in 2) and **function complexity** (two functions exceed 200 lines). These are maintainability concerns, not correctness issues.

| Category | Verdict |
|----------|---------|
| Frontend/Backend consistency | Pass |
| API client generation sync | Pass |
| DAG task key alignment | Pass |
| Environment variable flow | Pass |
| UC permission coverage | Pass |
| Import resolution | Pass |
| Code duplication | **12 issues found** |
| Function complexity | **2 functions flagged** |
| Naming consistency | **Minor issues** |

---

## What's Working Well

### Frontend Patterns (All Routes Consistent)

Every route follows the same Suspense + ErrorBoundary pattern:

```
ErrorBoundary → Suspense (with Skeleton fallback) → Content component with useSuspense hook
```

Verified across: `index.tsx`, `settings.tsx`, `spaces/$spaceId.tsx`, `runs/$runId.tsx`, `runs/$runId/comparison.tsx`.

### API Surface Alignment

All 11 backend endpoints have matching generated hooks with correct `operation_id` values:

| Hook | Endpoint | Status |
|------|----------|--------|
| `useListSpacesSuspense` | `GET /spaces` | OK |
| `useGetActivitySuspense` | `GET /activity` | OK |
| `useGetSpaceDetailSuspense` | `GET /spaces/{space_id}` | OK |
| `useStartOptimization` | `POST /spaces/{space_id}/optimize` | OK |
| `useGetRun` | `GET /runs/{run_id}` | OK |
| `useGetComparisonSuspense` | `GET /runs/{run_id}/comparison` | OK |
| `useApplyOptimization` | `POST /runs/{run_id}/apply` | OK |
| `useDiscardOptimization` | `POST /runs/{run_id}/discard` | OK |
| `useGetDataAccessSuspense` | `GET /settings/data-access` | OK |
| `useGrantDataAccess` | `POST /settings/data-access/grant` | OK |
| `useRevokeDataAccess` | `DELETE /settings/data-access/{grant_id}` | OK |

### DAG Task Keys

`job_launcher.py` task keys match `genie_optimization_job.yml` exactly. All 6 referenced job notebooks exist in `src/genie_space_optimizer/jobs/`.

### UC Permissions

The grant script (`grant_app_uc_permissions.py`) grants exactly the privileges the app needs: `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_MODEL`, `EXECUTE`, `MANAGE`.

### Environment Variables

`databricks.yml` env vars map correctly to `AppConfig` fields:

```
GENIE_SPACE_OPTIMIZER_CATALOG      → config.catalog
GENIE_SPACE_OPTIMIZER_SCHEMA       → config.schema_name
GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID → config.warehouse_id
```

---

## Issues Found

### HIGH: Duplicated NaN/Infinity Handling (4 Implementations)

The same recursive "replace NaN/Inf with None" logic exists in four separate files:

| Function | File | Lines |
|----------|------|-------|
| `_scrub_floats()` | `backend/models.py` | 13-21 |
| `_scrub_nan()` | `backend/routes/runs.py` | 55-63 |
| `_sanitize()` | `backend/core/_factory.py` | 28-35 |
| `SafeJSONResponse.render()` | `backend/core/_factory.py` | 37-47 |

All four do the same thing:

```python
if isinstance(obj, float):
    return None if not math.isfinite(obj) else obj
if isinstance(obj, dict):
    return {k: scrub(v) for k, v in obj.items()}
if isinstance(obj, (list, tuple)):
    return [scrub(v) for v in obj]
return obj
```

Additionally, `SafeModel` (which uses `_scrub_floats`) is only inherited by *some* response models. `VersionOut` and `CurrentUserOut` skip it. While those models don't have float fields today, the inconsistent inheritance could cause issues if fields are added later.

**Recommendation:** Extract a single `scrub_nan_inf()` to a shared module (e.g., `backend/utils.py`). Make `SafeModel` the default base for all response models.

---

### HIGH: Duplicated `_safe_float()` (3 Copies)

Identical function in three route files:

| File | Lines |
|------|-------|
| `backend/routes/activity.py` | 53-60 |
| `backend/routes/spaces.py` | 755-762 |
| `backend/routes/runs.py` | ~1585 |

Plus a related variant `_finite()` in `runs.py` that returns `0.0` instead of `None` as default -- subtly different semantics.

**Recommendation:** Consolidate into the same shared module.

---

### HIGH: Duplicated Status Constants

Identical sets defined with different names:

```python
# spaces.py:33
_ACTIVE_RUN_STATUSES = {"QUEUED", "IN_PROGRESS", "RUNNING"}
_TERMINAL_JOB_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

# runs.py:1046-1047
_ACTIVE_RUN_STATUSES_LOCAL = {"QUEUED", "IN_PROGRESS", "RUNNING"}     # exact copy
_TERMINAL_JOB_STATES_LOCAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}  # exact copy
```

**Recommendation:** Define once in `backend/constants.py` or `common/config.py`.

---

### MEDIUM: Duplicated Run Reconciliation Logic

Two functions perform nearly identical job-state reconciliation:

| Function | File | Lines | Returns |
|----------|------|-------|---------|
| `_reconcile_active_runs()` | `routes/spaces.py` | 73-139 | `bool` |
| `_reconcile_single_run()` | `routes/runs.py` | 1050-1100 | modified `run_data` dict |

Both:
1. Call `sp_ws.jobs.get_run()` to check Databricks job state
2. Extract `life_cycle_state` from `run.state`
3. Compare against terminal states
4. Call `update_run_status()` on state changes

They differ only in return type and scope (batch vs single). The core logic could be a shared helper.

**Recommendation:** Extract shared reconciliation logic into a common function that both can call.

---

### MEDIUM: Duplicated SP Principal Detection

Two implementations:

```python
# settings.py:35-43 (proper helper)
def _get_sp_principal(ws: WorkspaceClient) -> str:
    client_id = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    if not client_id:
        raise HTTPException(500, "Cannot resolve SP principal")
    return client_id

# spaces.py:643-645 (inline, different fallback)
sp_id = (sp_ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "<sp-principal-id>"))
```

The `spaces.py` version uses `"<sp-principal-id>"` as fallback (a literal placeholder string) instead of raising an error.

**Recommendation:** Use the `settings.py` helper everywhere.

---

### MEDIUM: Duplicated Grant Loading

`settings.py:59-68` has `_load_grants()` to read active grants from the Delta table. `spaces.py:526-554` has `_check_sp_data_access()` which queries the same table with nearly identical SQL.

**Recommendation:** Have `_check_sp_data_access()` call `_load_grants()` instead of duplicating the query.

---

### MEDIUM: Three JSON-Parsing Helpers in runs.py

Three separate functions in the same file doing essentially the same thing:

| Function | Lines | Purpose |
|----------|-------|---------|
| `_parse_patch_command()` | 80-99 | Parse JSON with double-decode fallback |
| `_parse_detail()` | 319-329 | Parse dict or JSON string |
| `_maybe_json()` | 860-870 | Idempotent parse |

All handle the same scenario: "this value might be a JSON string or already a dict."

**Recommendation:** Consolidate into one `_safe_json_parse(val) -> dict | list | str`.

---

### MEDIUM: Job Parameter Defaults Defined in Two Places

`job_launcher.py` defines `_JOB_PARAM_DEFAULTS` (11 parameters with defaults). `genie_optimization_job.yml` defines base parameters for each task independently. These overlap but aren't identical -- the YAML file omits some parameters from certain tasks (e.g., `triggered_by`, `experiment_name`).

This isn't a bug (the Python job launcher is the runtime path; the YAML is for `databricks bundle deploy`), but it creates a risk of drift.

**Recommendation:** Add a comment in both files pointing to the other as the source of truth, or generate one from the other.

---

### LOW: Function Complexity

Two functions significantly exceed reasonable length:

| Function | File | Lines | LOC |
|----------|------|-------|-----|
| `_build_step_io()` | `routes/runs.py` | 391-673 | **283** |
| `start_optimization()` | `routes/spaces.py` | ~490-670 | **~180** |

`_build_step_io()` has 5 branches (one per pipeline step), each 40-60 lines of deeply nested dict construction. This is hard to test and modify.

**Recommendation:** Extract per-step builders:

```python
def _build_step_1_io(...) -> tuple[dict | None, dict | None]: ...
def _build_step_2_io(...) -> tuple[dict | None, dict | None]: ...
# etc.

_STEP_IO_BUILDERS = {1: _build_step_1_io, 2: _build_step_2_io, ...}
```

---

### LOW: Inconsistent Naming in API Models

Most API model fields use `camelCase` (matching JavaScript conventions), but a few use `snake_case`:

```python
# camelCase (majority)
qualityScore, baselineScore, optimizedScore, stepNumber

# snake_case (exceptions)
schema_name, target_schema, target_catalog
```

The `schema_name` / `target_*` fields come from the settings models which deal with UC objects. The inconsistency is minor but could confuse frontend developers.

**Recommendation:** Standardize to `camelCase` for all API response fields using Pydantic `alias_generator`.

---

### LOW: Silent Exception Catch in `_genie_client()`

```python
# spaces.py:40-53
def _genie_client(ws, sp_ws):
    try:
        ws.genie.list_spaces(page_size=1)
        return ws
    except PermissionDenied:
        return sp_ws
    except Exception:        # <-- catches everything silently
        return sp_ws
```

The bare `except Exception` could mask unexpected errors (network timeouts, SDK bugs, auth misconfigurations) by silently falling back to the SP client.

**Recommendation:** Log the exception type/message before falling back, or narrow the catch to known exceptions.

---

### LOW: Thread Lock May Be Ineffective

`job_launcher.py` uses `threading.Lock()` for concurrency control around job creation/update. In a production deployment with multiple uvicorn workers (configured as 2 in `app.yml`), this lock only protects within a single process. Cross-process races are still possible.

The actual impact is low because `_ensure_persistent_job()` uses idempotent create-or-update logic, but the lock gives a false sense of safety.

**Recommendation:** Either remove the lock (relying on Databricks-side idempotency) or document that it only guards single-process races.

---

## Issue Summary

| # | Severity | Issue | Files Affected |
|---|----------|-------|----------------|
| 1 | HIGH | 4 implementations of NaN/Inf scrubbing | models.py, runs.py, _factory.py |
| 2 | HIGH | 3 copies of `_safe_float()` | activity.py, spaces.py, runs.py |
| 3 | HIGH | Duplicated status constants | spaces.py, runs.py |
| 4 | MEDIUM | Duplicated reconciliation logic | spaces.py, runs.py |
| 5 | MEDIUM | Duplicated SP principal detection | settings.py, spaces.py |
| 6 | MEDIUM | Duplicated grant loading query | settings.py, spaces.py |
| 7 | MEDIUM | 3 JSON-parsing helpers in one file | runs.py |
| 8 | MEDIUM | Job param defaults in 2 places | job_launcher.py, YAML |
| 9 | LOW | `_build_step_io()` is 283 lines | runs.py |
| 10 | LOW | Mixed camelCase/snake_case in models | models.py |
| 11 | LOW | Silent `except Exception` in genie fallback | spaces.py |
| 12 | LOW | Thread lock ineffective across workers | job_launcher.py |

---

## Recommended Refactoring Plan

**Phase 1 -- Quick Wins (address HIGH items):**

1. Create `backend/utils.py` with:
   - `scrub_nan_inf(obj)` -- single NaN/Inf scrubber
   - `safe_float(val) -> float | None`
   - `safe_int(val) -> int | None`
   - `safe_json_parse(val) -> dict | list | str`
2. Create `backend/constants.py` with:
   - `ACTIVE_RUN_STATUSES`
   - `TERMINAL_JOB_STATES`
3. Update all route files to import from these shared modules.

**Phase 2 -- Consolidation (address MEDIUM items):**

4. Extract shared reconciliation logic into a helper both `spaces.py` and `runs.py` can call.
5. Reuse `_get_sp_principal()` from `settings.py` in `spaces.py`.
6. Have `_check_sp_data_access()` call `_load_grants()`.
7. Add cross-references between `job_launcher.py` and `genie_optimization_job.yml`.

**Phase 3 -- Cleanup (address LOW items):**

8. Break `_build_step_io()` into per-step functions.
9. Standardize model field naming with Pydantic `alias_generator`.
10. Narrow the exception catch in `_genie_client()`.
