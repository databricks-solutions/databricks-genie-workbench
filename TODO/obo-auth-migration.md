# Plan: Move Genie Workbench to Full OBO Auth

## Context

Genie Workbench currently uses a mix of OBO (On-Behalf-Of) user auth and App Service Principal (SP) auth. The goal is to move everything possible to OBO so that all user-facing operations respect user permissions, and SP is only used where structurally required (shared infrastructure, background jobs).

The app already declares the correct OBO scopes in `app.yaml`:
```yaml
user_api_scopes:
  - "sql"
  - "dashboards.genie"
  - "serving.serving-endpoints"
  - "catalog.catalogs:read"
  - "catalog.schemas:read"
  - "catalog.tables:read"
  - "files.files"
  - "iam.access-control:read"
```

## Current Auth Map

### Already pure OBO (no changes needed)
| Component | File | Auth |
|-----------|------|------|
| UC discovery | `backend/services/uc_client.py` | `get_workspace_client()` |
| SQL execution | `backend/sql_executor.py` | `get_workspace_client()` |
| LLM serving | `backend/services/llm_utils.py` | `get_workspace_client()` |
| Genie creation | `backend/genie_creator.py` | `get_workspace_client()` |
| Create agent tools | `backend/services/create_agent_tools.py` | `get_workspace_client()` |
| Fix agent patches | `backend/services/fix_agent.py` | `get_workspace_client()` |
| User identity | `backend/routers/auth.py` | Forwarded headers |

### OBO with unnecessary SP fallback (remove fallback)
| Component | File | Lines | Current pattern |
|-----------|------|-------|-----------------|
| `get_genie_space()` | `backend/services/genie_client.py` | 60-69 | Try OBO, catch scope error, retry with SP |
| `list_genie_spaces()` | `backend/services/genie_client.py` | 89-97 | Same |
| `get_space_detail()` | `backend/routers/spaces.py` | 116-133 | Same (inline) |

The `dashboards.genie` scope is already declared — users consent on first app visit. The SP fallback silently masks permission issues and causes `list_genie_spaces()` to return ALL org spaces instead of user-scoped ones.

### Must remain SP (no changes)
| Component | File | Why SP is required |
|-----------|------|--------------------|
| Lakebase credentials | `backend/services/lakebase.py:_generate_credential()` | Shared connection pool; Postgres username = SP's `application_id` |
| Lakebase token refresh | `backend/services/lakebase.py:_token_refresh_loop()` | Background task, no user session |
| Auto-Optimize permission check | `backend/routers/auto_optimize.py:check_permissions()` | Verifying SP's own UC/Genie permissions for the job |
| Auto-Optimize trigger | `backend/routers/auto_optimize.py:trigger()` | Job runs as SP (`sp_ws.jobs.run_now()`) |
| Auto-Optimize health | `backend/routers/auto_optimize.py:_is_configured()` | Checking SP can access the GSO job |
| Auto-Optimize run status | `backend/routers/auto_optimize.py:get_active_run()` | Job was submitted as SP; run may not be visible to user |

## Changes

### 1. Remove SP fallback from `genie_client.py`

**File**: `backend/services/genie_client.py`

- Delete `_is_scope_error()` helper (lines 22-25)
- In `get_genie_space()` (lines 60-69): Remove the try/except SP fallback. Call `_get_space_with_client(client, genie_space_id)` directly. Let errors propagate with a clear message.
- In `list_genie_spaces()` (lines 89-97): Same — remove the try/except SP fallback around `_list_spaces_with_client(client)`.
- Remove the `get_service_principal_client` import (line 15) since it will no longer be used in this file.

### 2. Remove SP fallback from `spaces.py`

**File**: `backend/routers/spaces.py`

- In `get_space_detail()` (lines 116-133): Replace the nested try/except with a direct call. Remove the `_is_scope_error` catch block that retries with SP.
- Remove unused imports from line 11-12: `get_service_principal_client` and `_is_scope_error`.

### 3. Update `auth.py` docstring

**File**: `backend/services/auth.py`

- Update `get_service_principal_client()` docstring to reflect its narrowed purpose: Lakebase infrastructure and Auto-Optimize job operations only. Remove mention of "scope error fallback."

### 4. No changes to admin router

**File**: `backend/routers/admin.py`

After step 1, `list_genie_spaces()` becomes pure OBO automatically. The admin dashboard will now show only spaces visible to the calling user. This is the correct security behavior — SP was leaking org-wide visibility to all users.

## What stays SP and why

### 1. Lakebase (Provisioned or Autoscale) — SP required

**Provisioned** (current): Shared `asyncpg` pool. `generate_database_credential()` produces a token scoped to the SP's `application_id`, which is the Postgres username. Per-user connections are impractical.

**Autoscale (Serverless)**: The credential API *technically* supports user tokens, but three blockers prevent per-user auth from an app:
- No auto-provisioned Postgres roles for app users — each needs manual `databricks_create_role()`
- `asyncpg` doesn't support per-connection credential callbacks (would need `psycopg3`)
- Apps + Lakebase user auth is still in Preview

Lakebase Autoscale does offer a **Data API** (PostgREST-based) designed for per-user bearer token auth with RLS — but it's a REST API, not wire protocol, requiring major architectural changes.

**Bottom line**: The shared SP pool is correct for this app's data (scan results, stars, sessions = shared app state, not per-user sensitive data).

### 2. Databricks Jobs (Auto-Optimize) — SP required

Jobs **cannot** run under OBO. The `run_as` identity is a job-level property, not per-run. The `run-now` API has no `run_as` parameter. Additionally:
- OBO tokens expire after ~1 hour; GSO jobs run 30-120 minutes
- Job clusters don't receive the triggering user's HTTP headers
- Each triggering user would need SELECT on all data schemas, CAN_MANAGE on Genie Spaces, and MLflow write access

The current pattern is the documented best practice: SP runs the job, OBO validates permissions at trigger time, user-level UC writes are deferred to when the user returns to the UI.

### 3. GSO package internals

`packages/genie-space-optimizer/` has its own fallback patterns in `_pick_genie_client()`. Out of scope for this PR; can be cleaned up separately.

## Verification

1. Deploy to a test workspace: `./scripts/deploy.sh`
2. Open the app as a normal user (not workspace admin)
3. Verify:
   - Space list shows only spaces the user has access to (not all org spaces)
   - Space detail loads correctly for an accessible space
   - Space detail returns 403/404 for a space the user cannot access
   - IQ Scan works on an accessible space
   - Fix Agent works on an accessible space
   - Create Agent works end-to-end
   - Admin dashboard shows stats scoped to user's visible spaces
   - Auto-Optimize permission check and trigger still work (SP paths unchanged)
4. Check logs for any "scope" errors — there should be none if consent flow is working
