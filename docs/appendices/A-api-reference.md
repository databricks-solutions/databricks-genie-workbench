# Appendix A: API Reference

All API endpoints are prefixed with `/api` and served by FastAPI routers. This reference lists every endpoint with its HTTP method, auth identity, and purpose.

**Auth identity key:**
- **OBO** — uses the signed-in user's On-Behalf-Of token
- **SP** — uses the app's Service Principal
- **OBO → SP** — tries OBO first, falls back to SP on scope error
- **Mixed** — uses both identities for different parts of the operation

## Analysis Router (`/api`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/api/space/fetch` | OBO → SP | Fetch serialized Genie Space by ID |
| POST | `/api/space/parse` | None | Parse pasted Genie API JSON (client-side data, no auth needed) |
| GET | `/api/debug/auth` | OBO | Dev-only auth debug endpoint (404 on Databricks Apps) |
| GET | `/api/settings` | None | Read-only app settings (LLM model, warehouse, host) |

## Spaces Router (`/api`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/spaces` | OBO → SP | List Genie Spaces with IQ scores, starred sort, filters |
| GET | `/api/spaces/{space_id}` | OBO | Space metadata + latest scan + star status |
| POST | `/api/spaces/{space_id}/scan` | OBO | Run IQ scan and persist result to Lakebase |
| GET | `/api/spaces/{space_id}/history` | OBO | Scan + auto-optimize run history for a space |
| PUT | `/api/spaces/{space_id}/star` | OBO | Toggle starred status (Lakebase) |
| POST | `/api/spaces/{space_id}/fix` | OBO | **SSE** — Fix agent: stream patches and progress |

## Admin Router (`/api/admin`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/admin/dashboard` | OBO | Org-wide stats: space count, scan count, avg score, maturity distribution |
| GET | `/api/admin/leaderboard` | OBO | Top/bottom spaces by IQ score (`top_n` param) |
| GET | `/api/admin/alerts` | OBO | Spaces with "Not Ready" maturity (max 20) |

## Auth Router (`/api/auth`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/auth/me` | OBO | Current user info from OBO headers, dev env, or SDK |
| GET | `/api/auth/status` | OBO | Lightweight health check with workspace client / auth type |

## Create Router (`/api/create`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/create/discover/catalogs` | OBO | List Unity Catalog catalogs |
| GET | `/api/create/discover/schemas` | OBO | List schemas in a catalog |
| GET | `/api/create/discover/tables` | OBO | List tables in a catalog.schema |
| GET | `/api/create/discover/columns` | OBO | List columns for a table |
| POST | `/api/create/validate` | OBO | Validate serialized space config (errors/warnings) |
| POST | `/api/create` | OBO | Create Genie Space from wizard payload |
| POST | `/api/create/agent/chat` | OBO | **SSE** — Create agent conversational flow |
| GET | `/api/create/agent/sessions/{session_id}` | OBO | Load agent session for refresh/reconnect |
| DELETE | `/api/create/agent/sessions/{session_id}` | OBO | Delete agent session |

## Auto-Optimize Router (`/api/auto-optimize`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/auto-optimize/health` | SP | GSO health check: job/warehouse configuration status |
| GET | `/api/auto-optimize/permissions/{space_id}` | Mixed | Pre-check SP manage + UC read + Prompt Registry |
| POST | `/api/auto-optimize/trigger` | Mixed | Start GSO optimization job (OBO for auth, SP for job submission) |
| GET | `/api/auto-optimize/runs/{run_id}` | SP | Full run detail: stages, steps, levers, links |
| GET | `/api/auto-optimize/runs/{run_id}/status` | SP | Lightweight status poll: steps, scores |
| GET | `/api/auto-optimize/levers` | None | List optimization lever definitions |
| POST | `/api/auto-optimize/runs/{run_id}/apply` | OBO | Apply optimization results to the Genie Space |
| POST | `/api/auto-optimize/runs/{run_id}/discard` | Mixed | Discard run / rollback changes |
| GET | `/api/auto-optimize/spaces/{space_id}/active-run` | SP | Check for QUEUED/IN_PROGRESS run |
| GET | `/api/auto-optimize/spaces/{space_id}/runs` | SP | List optimization runs for a space |
| GET | `/api/auto-optimize/runs/{run_id}/iterations` | SP | Per-iteration evaluation rows |
| GET | `/api/auto-optimize/runs/{run_id}/debug-data` | SP | Diagnostics for Lakebase vs Delta data |
| GET | `/api/auto-optimize/runs/{run_id}/asi-results` | SP | ASI judge results (requires `iteration` param) |
| GET | `/api/auto-optimize/runs/{run_id}/question-results` | SP | Per-question results (requires `iteration` param) |
| GET | `/api/auto-optimize/runs/{run_id}/patches` | SP | All patches for the run |
| GET | `/api/auto-optimize/runs/{run_id}/suggestions` | SP | Strategist suggestions for the run |

## Static File Serving (`main.py`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/` | None | Serve `index.html` (React SPA) |
| GET | `/{full_path:path}` | None | Serve static assets from `frontend/dist/`, fallback to SPA |

## SSE Streaming Endpoints

Two endpoints use Server-Sent Events:

| Endpoint | Keepalive | Events |
|----------|-----------|--------|
| `POST /api/spaces/{id}/fix` | 10s | `thinking`, `patch`, `applying`, `complete`, `error` |
| `POST /api/create/agent/chat` | 15s | `session`, `step`, `thinking`, `tool_call`, `tool_result`, `message_delta`, `message`, `created`, `updated`, `heartbeat`, `error`, `done` |

The frontend consumes SSE via manual `fetch` + `ReadableStream` in `lib/api.ts` (not `EventSource`). Buffers are split on `\n\n`.

## Related Documentation

- [Authentication & Permissions](../03-authentication-and-permissions.md) — which identity is used where and why
- [Architecture Overview](../02-architecture-overview.md) — router and service structure
