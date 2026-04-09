# Authentication & Permissions

This is the authoritative reference for how identity and authorization work in Genie Workbench. The app uses a **dual-identity model**: the signed-in user's token for interactive operations and the app's Service Principal (SP) for background jobs and API fallback.

## Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        Databricks Apps                           │
│                                                                  │
│   Browser ──▶ Reverse Proxy ──▶ FastAPI Backend                  │
│                  │                    │                           │
│                  │ injects            │ reads header              │
│                  │ x-forwarded-       │ stores in ContextVar      │
│                  │ access-token       │                           │
│                  ▼                    ▼                           │
│            User's OAuth       OBO WorkspaceClient                │
│            Token               (per-request)                     │
│                                                                  │
│                               SP WorkspaceClient                 │
│                                (singleton, from                  │
│                                 platform env vars)               │
└──────────────────────────────────────────────────────────────────┘
```

## OBO (On-Behalf-Of) Authentication

### How it works

1. The Databricks Apps platform authenticates the user via SSO and forwards their OAuth access token in the `x-forwarded-access-token` HTTP header on every request.

2. `OBOAuthMiddleware` in `backend/main.py` intercepts all `/api/*` requests and calls `set_obo_user_token(token)` from `backend/services/auth.py`.

3. `set_obo_user_token()` creates a `WorkspaceClient` configured with `auth_type="pat"` using the user's token. This is stored in a Python `ContextVar` so it is scoped to the current request.

   > The explicit `auth_type="pat"` is required because the Databricks Apps environment has `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` set for the SP. Without it, the SDK would default to OAuth M2M instead of the user's token.

4. All downstream service calls use `get_workspace_client()`, which returns the OBO client if set, otherwise falls back to the SP singleton.

### SSE streaming caveat

For Server-Sent Events endpoints (fix agent, create agent), the `ContextVar` is **not** cleared after `call_next` in the middleware. This is because the response body streams lazily — the generator runs after the middleware returns. Streaming handlers stash the raw token on `request.state.user_token` and re-set it inside the generator function.

### What OBO protects

Users can only interact with resources they have permission to access:
- **Unity Catalog**: catalogs, schemas, tables visible to the user
- **Genie Spaces**: only spaces the user can manage
- **SQL Warehouse**: queries execute under the user's identity
- **Space creation/modification**: spaces are created and patched as the user

## Service Principal (SP)

### Singleton client

`get_service_principal_client()` in `backend/services/auth.py` always returns the SP singleton — a `WorkspaceClient` initialized from the platform environment variables (`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_HOST`). On Databricks Apps, this is the app's own SP.

### When SP is used

The SP is used in three scenarios:

#### 1. Genie API scope fallback

Some user OAuth tokens lack the `dashboards.genie` scope, even though it is listed in `app.yaml` `user_api_scopes`. When `get_genie_space()` or `list_genie_spaces()` in `backend/services/genie_client.py` catches a scope error, it transparently retries with the SP:

```python
def _is_scope_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "scope" in msg or "insufficient_scope" in msg
```

For this fallback to work, the SP must have **CAN_MANAGE** on each Genie Space.

#### 2. Optimization job execution

The Auto-Optimize pipeline runs as a Lakeflow Job — a long-running, multi-task DAG. Lakeflow Jobs execute in a separate environment with a fixed `run_as` identity. There is no mechanism to forward the user's short-lived OAuth token into a background job that may run for minutes.

The job is configured to `run_as` the app's SP. At startup, `_ensure_gso_job_run_as()` in `backend/main.py` verifies and updates the job's `run_as` to match the current app SP.

#### 3. GSO Delta table operations

Reads and writes to the optimizer state tables (12 Delta tables under `GSO_CATALOG.GSO_SCHEMA`) use the SP because these tables are owned by the SP and are not user-scoped.

## Optimization Trigger Flow

When a user triggers Auto-Optimize, the app uses **both** identities in a carefully sequenced flow:

```
User clicks "Optimize"
        │
        ▼
POST /api/auto-optimize/trigger
        │
        ▼
┌───────────────────────────────┐
│ 1. user_can_edit_space(OBO)   │◀── Verify user has CAN_EDIT/CAN_MANAGE
│    Reject if unauthorized     │
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 2. fetch_space_config         │◀── Try OBO first, then SP fallback
│    Snapshot the space config  │
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 3. fetch_uc_metadata(OBO)     │◀── Column/tag metadata respects user visibility
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 4. sp_can_manage_space(SP)    │◀── Verify SP has CAN_MANAGE
│    Reject if SP lacks access  │
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 5. wh_create_run(OBO)         │◀── Insert run row in Delta (user identity)
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 6. submit_optimization(SP)    │◀── jobs.run_now() as SP
│    → Lakeflow Job submitted   │
└───────────┬───────────────────┘
            ▼
┌───────────────────────────────┐
│ 7. 6-task DAG executes as SP  │
│    (preflight → baseline →    │
│     enrichment → lever_loop → │
│     finalize → deploy)        │
└───────────────────────────────┘
```

**Source:** `packages/genie-space-optimizer/src/genie_space_optimizer/integration/trigger.py` — `trigger_optimization()`

## Requested OAuth Scopes

The `user_api_scopes` in `app.yaml` request these scopes for the user's OBO token:

| Scope | Purpose |
|-------|---------|
| `sql` | Execute SQL queries via warehouses |
| `dashboards.genie` | Access Genie Space API |
| `serving.serving-endpoints` | Call model serving endpoints (LLM) |
| `catalog.catalogs:read` | Browse Unity Catalog catalogs |
| `catalog.schemas:read` | Browse Unity Catalog schemas |
| `catalog.tables:read` | Browse Unity Catalog tables |
| `files.files` | Access workspace files |
| `iam.access-control:read` | Read ACLs for permission checks |

If the workspace or user's OAuth consent doesn't grant all scopes, the app degrades gracefully — the SP fallback handles the most common gap (`dashboards.genie`).

## SP Permissions Required

### Per Genie Space

| Permission | Purpose |
|-----------|---------|
| `CAN_MANAGE` | API fallback when user token lacks Genie scope; applying optimization patches during the GSO pipeline |

Grant via the Genie Space sharing UI or the installer (`scripts/install.sh` automates this).

### Per referenced data schema

| Permission | Purpose |
|-----------|---------|
| `USE_CATALOG` | Access the catalog containing the schema |
| `USE_SCHEMA` | Access the schema |
| `SELECT` | Read table data during optimization benchmarks |

```sql
GRANT USE_CATALOG ON CATALOG <catalog> TO `<service-principal-name>`;
GRANT USE_SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`;
```

### GSO optimizer schema

The SP needs full access to the optimizer state schema (`<GSO_CATALOG>.genie_space_optimizer`):

| Permission | Purpose |
|-----------|---------|
| `USE_CATALOG` | Access the catalog |
| `USE_SCHEMA` | Access the schema |
| `SELECT` | Read optimizer state tables |
| `MODIFY` | Write optimizer state tables |
| `CREATE_TABLE` | Create state tables on first run |
| `CREATE_FUNCTION` | Create UDFs if needed |
| `CREATE_MODEL` | MLflow model registration |
| `CREATE_VOLUME` | Artifact storage |
| `EXECUTE` | Execute functions |
| `MANAGE` | Schema management |

These are granted automatically by `scripts/grant_permissions.py` during deployment.

## Complete Permission Boundary

| Operation | Identity | Code Reference | Rationale |
|-----------|----------|---------------|-----------|
| Browse Genie Spaces, UC catalogs/schemas/tables | OBO (user) | `services/uc_client.py`, `routers/create.py` | User sees only what they have access to |
| Genie API — fetch/list spaces | OBO → SP fallback | `services/genie_client.py` `_is_scope_error()` | User token may lack `dashboards.genie` scope |
| Create Agent — tools, SQL, space creation | OBO (user) | `services/create_agent.py`, `services/create_agent_tools.py` | Space created under user identity |
| Fix Agent — generate + apply patches | OBO (user) | `services/fix_agent.py` | Patches applied as the user |
| Trigger optimization — permission check | OBO (user) | `integration/trigger.py` `user_can_edit_space()` | Verify user has CAN_EDIT/CAN_MANAGE |
| Trigger optimization — SP entitlement check | SP | `integration/trigger.py` `sp_can_manage_space()` | Verify SP can manage the space |
| Optimization job submission | SP | `backend/job_launcher.py` `submit_optimization()` | `jobs.run_now()` requires SP |
| Optimization job execution (6-task DAG) | SP (run_as) | `backend/job_launcher.py` `ensure_job_run_as()` | Lakeflow Jobs have no OBO mechanism |
| GSO Delta table reads/writes | SP | `routers/auto_optimize.py` `_delta_query()` | Optimizer state tables owned by SP |
| Lakebase persistence | SP | `services/lakebase.py` | App-level storage, not user-scoped |
| IQ Scan | OBO (user) → SP for GSO data | `services/scanner.py` | Space fetch via OBO; GSO run data via SP |
| Apply optimization results | OBO (user) | `routers/auto_optimize.py` `/runs/{id}/apply` | Changes applied under user identity |

## Security Considerations

1. **Authorization before execution** — the user must have `CAN_EDIT` or `CAN_MANAGE` on the Genie Space before any optimization job is submitted. This check happens with the user's OBO token, not the SP.

2. **SP entitlement validated** — even if the user is authorized, the SP must also have `CAN_MANAGE` on the space. If the SP lacks access, the trigger is rejected with a clear error.

3. **Minimum-privilege SP** — the SP only needs read access to referenced data schemas (for benchmarking) and manage access to the GSO state schema. It does not need workspace-admin privileges.

4. **No token forwarding to jobs** — the design intentionally does not pass user tokens into background jobs. Short-lived OAuth tokens would expire during long-running DAGs, and storing user credentials in job parameters would be a security risk.

5. **Audit trail** — the triggering user's email is recorded in the optimization run metadata, providing traceability even though the job executes as the SP.

## Related Documentation

- [Architecture Overview](02-architecture-overview.md) — system components
- [Auto-Optimize](07-auto-optimize.md) — the optimization pipeline that runs under SP
- [Deployment Guide](08-deployment-guide.md) — how SP permissions are granted during deploy
- [API Reference](appendices/A-api-reference.md) — per-endpoint auth identity
