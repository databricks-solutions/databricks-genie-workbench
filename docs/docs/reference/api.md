---
sidebar_position: 1
description: "Every API endpoint with its HTTP method, auth identity, and purpose."
---

# API Reference

All API endpoints are prefixed with `/api` and served by FastAPI routers. This reference lists every endpoint with its HTTP method, auth identity, and purpose.

**Auth identity key:**
- **OBO** — uses the signed-in user's On-Behalf-Of token
- **SP** — uses the app's Service Principal
- **OBO → SP** — tries OBO first, falls back to SP on scope error
- **Mixed** — uses both identities for different parts of the operation

## Analysis Router (`/api`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--info">POST</span> | `/api/space/fetch` | <span className="badge badge--info">OBO → SP</span> | Fetch serialized Genie Agent by ID |
| <span className="badge badge--info">POST</span> | `/api/space/parse` | <span className="badge badge--secondary">None</span> | Parse pasted Genie API JSON (client-side data, no auth needed) |
| <span className="badge badge--success">GET</span> | `/api/debug/auth` | <span className="badge badge--primary">OBO</span> | Dev-only auth debug endpoint (404 on Databricks Apps) |
| <span className="badge badge--success">GET</span> | `/api/settings` | <span className="badge badge--secondary">None</span> | Read-only app settings (LLM model, warehouse, host) |
| <span className="badge badge--success">GET</span> | `/api/models` | <span className="badge badge--secondary">None</span> | Curated chat serving endpoints selectable per Create Agent / Auto-Optimize run |

## Spaces Router (`/api`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/spaces` | <span className="badge badge--info">OBO → SP</span> | List Genie Agents with IQ scores, starred sort, filters |
| <span className="badge badge--success">GET</span> | `/api/spaces/{space_id}` | <span className="badge badge--primary">OBO</span> | Space metadata + latest scan + star status |
| <span className="badge badge--info">POST</span> | `/api/spaces/{space_id}/scan` | <span className="badge badge--primary">OBO</span> | Run IQ scan and persist result to Lakebase |
| <span className="badge badge--success">GET</span> | `/api/spaces/{space_id}/history` | <span className="badge badge--primary">OBO</span> | Scan + auto-optimize run history for a space |
| <span className="badge badge--warning">PUT</span> | `/api/spaces/{space_id}/star` | <span className="badge badge--primary">OBO</span> | Toggle starred status (Lakebase) |

## Admin Router (`/api/admin`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/admin/dashboard` | <span className="badge badge--primary">OBO</span> | Org-wide stats: space count, scan count, avg score, maturity distribution |
| <span className="badge badge--success">GET</span> | `/api/admin/leaderboard` | <span className="badge badge--primary">OBO</span> | Top/bottom spaces by IQ score (`top_n` param) |
| <span className="badge badge--success">GET</span> | `/api/admin/alerts` | <span className="badge badge--primary">OBO</span> | Spaces with "Not Ready" maturity (max 20) |

## Auth Router (`/api/auth`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/auth/me` | <span className="badge badge--primary">OBO</span> | Current user info from OBO headers, dev env, or SDK |
| <span className="badge badge--success">GET</span> | `/api/auth/status` | <span className="badge badge--primary">OBO</span> | Lightweight health check with workspace client / auth type |

## Create Router (`/api/create`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/create/preflight` | <span className="badge badge--primary">OBO</span> | Pre-check that the user can create Genie Agents |
| <span className="badge badge--success">GET</span> | `/api/create/discover/catalogs` | <span className="badge badge--primary">OBO</span> | List Unity Catalog catalogs |
| <span className="badge badge--success">GET</span> | `/api/create/discover/schemas` | <span className="badge badge--primary">OBO</span> | List schemas in a catalog |
| <span className="badge badge--success">GET</span> | `/api/create/discover/tables` | <span className="badge badge--primary">OBO</span> | List tables in a catalog.schema |
| <span className="badge badge--success">GET</span> | `/api/create/discover/columns` | <span className="badge badge--primary">OBO</span> | List columns for a table |
| <span className="badge badge--success">GET</span> | `/api/create/discover/search` | <span className="badge badge--primary">OBO</span> | Keyword search for candidate tables across Unity Catalog |
| <span className="badge badge--info">POST</span> | `/api/create/validate` | <span className="badge badge--primary">OBO</span> | Validate serialized space config (errors/warnings) |
| <span className="badge badge--info">POST</span> | `/api/create` | <span className="badge badge--primary">OBO</span> | Create Genie Agent from wizard payload |
| <span className="badge badge--info">POST</span> | `/api/create/agent/chat` | <span className="badge badge--primary">OBO</span> | **SSE** — Create agent conversational flow |
| <span className="badge badge--success">GET</span> | `/api/create/agent/sessions/{session_id}` | <span className="badge badge--primary">OBO</span> | Load agent session for refresh/reconnect |
| <span className="badge badge--danger">DELETE</span> | `/api/create/agent/sessions/{session_id}` | <span className="badge badge--primary">OBO</span> | Delete agent session |

## Auto-Optimize Router (`/api/auto-optimize`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/health` | <span className="badge badge--secondary">SP</span> | GSO health check: job/warehouse configuration status |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/permissions/{space_id}` | <span className="badge badge--warning">Mixed</span> | Pre-check SP manage + UC read |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/trigger` | <span className="badge badge--warning">Mixed</span> | Start GSO optimization job; `benchmark_policy` is `review_only` or `repair_allowed` (OBO for auth, SP for job submission) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}` | <span className="badge badge--secondary">SP</span> | Full run detail: stages, steps, levers, links |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/status` | <span className="badge badge--secondary">SP</span> | Lightweight status poll: steps, scores |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/levers` | <span className="badge badge--secondary">None</span> | List optimization lever definitions |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/runs/{run_id}/apply` | <span className="badge badge--primary">OBO</span> | Mark an already-published result `APPLIED` for integration compatibility; the post-run UI does not require this step |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/runs/{run_id}/discard` | <span className="badge badge--warning">Mixed</span> | Discard run / rollback changes |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/revert-options` | <span className="badge badge--warning">Mixed</span> | Preview champion/baseline availability and live-to-snapshot benchmark diffs |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/runs/{run_id}/revert` | <span className="badge badge--warning">Mixed</span> | Revert with independent `config_target=champion\|baseline` and `benchmark_target=current\|champion\|baseline` query parameters |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/spaces/{space_id}/active-run` | <span className="badge badge--secondary">SP</span> | Check for QUEUED/IN_PROGRESS run |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/spaces/{space_id}/runs` | <span className="badge badge--secondary">SP</span> | List optimization runs for a space |
| <span className="badge badge--danger">DELETE</span> | `/api/auto-optimize/runs/{run_id}/history-entry` | <span className="badge badge--warning">Mixed</span> | Hide a terminal run from Workbench history after OBO `CAN_EDIT`/`CAN_MANAGE` authorization; preserves the workflow run and GSO audit data |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/spaces/{space_id}/current-version` | <span className="badge badge--warning">Mixed</span> | Match live config and benchmarks independently to history-visible captured baselines/champions; report matched, mixed, component drift, or incomplete history (`refresh=true` bypasses the live-state cache) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/iterations` | <span className="badge badge--secondary">SP</span> | Per-iteration evaluation rows |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/loop-state` | <span className="badge badge--secondary">SP</span> | Optimizer controller loop state for the run |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/publish` | <span className="badge badge--secondary">SP</span> | Publish record and champion outcome |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/debug-data` | <span className="badge badge--secondary">SP</span> | Diagnostics for Lakebase vs Delta data |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/eval-results` | <span className="badge badge--secondary">SP</span> | Native Eval-Run rows (requires `iteration` param) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/question-results` | <span className="badge badge--secondary">SP</span> | Per-question results (requires `iteration` param) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/patches` | <span className="badge badge--secondary">SP</span> | All patches for the run |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/benchmark-changes` | <span className="badge badge--secondary">SP</span> | Benchmark mutation ledger plus QC window, structured quality findings, semantic-review coverage, and proposed repairs |

## GenieWatch Routers (`/api/watch`)

Most GenieWatch metrics read Databricks **system tables**, which are not OBO-readable,
so those routes execute as the **service principal** and use an in-process TTL cache.
The traffic-gap route is user-authorized and does not persist traffic: it requires
`CAN_MANAGE`, reads all conversation pages transiently, and fails instead of
returning partial results. It is **OBO-only** — it never falls back to the service
principal and returns `401` without user authorization. See
[OBO-only routes](/docs/platform/authentication#obo-only-routes). These routers are registered separately in `main.py`.

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/watch/spaces` | <span className="badge badge--secondary">SP</span> | List watched Genie Agents with cost/usage summaries |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}` | <span className="badge badge--secondary">SP</span> | Watch detail for one Agent |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/traffic-gaps` | <span className="badge badge--secondary">OBO only</span> | Manager-only, reviewable benchmark candidate gaps; no SP fallback, and no raw question or user identity in the response |
| <span className="badge badge--info">POST</span> | `/api/watch/spaces/refresh` | <span className="badge badge--secondary">SP</span> | Refresh the watched-space cache |
| <span className="badge badge--success">GET</span> | `/api/watch/overview` | <span className="badge badge--secondary">SP</span> | Org-wide cost overview |
| <span className="badge badge--success">GET</span> | `/api/watch/cost/top` | <span className="badge badge--secondary">SP</span> | Highest-cost Agents |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/cost` | <span className="badge badge--secondary">SP</span> | Per-Agent cost breakdown |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/cost/top-queries` | <span className="badge badge--secondary">SP</span> | Most expensive queries for an Agent |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/cost/conversations` | <span className="badge badge--secondary">SP</span> | Cost attributed per conversation |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/usage` | <span className="badge badge--secondary">SP</span> | Query volume and usage trend |
| <span className="badge badge--success">GET</span> | `/api/watch/feedback` | <span className="badge badge--secondary">SP</span> | Org-wide feedback signals |
| <span className="badge badge--success">GET</span> | `/api/watch/feedback/comments` | <span className="badge badge--secondary">SP</span> | Feedback comment text |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/feedback` | <span className="badge badge--secondary">SP</span> | Per-Agent feedback |
| <span className="badge badge--success">GET</span> | `/api/watch/spaces/{space_id}/resources` | <span className="badge badge--secondary">SP</span> | Tables actually executed by an Agent |
| <span className="badge badge--success">GET</span> | `/api/watch/resources/rollup` | <span className="badge badge--secondary">SP</span> | Executed-resource rollup |
| <span className="badge badge--success">GET</span> | `/api/watch/resources/spaces` | <span className="badge badge--secondary">SP</span> | Agents grouped by executed resource |
| <span className="badge badge--success">GET</span> | `/api/watch/resources/graph` | <span className="badge badge--secondary">SP</span> | Agent-to-resource lineage graph |
| <span className="badge badge--success">GET</span> | `/api/watch/settings/health` | <span className="badge badge--secondary">SP</span> | Watch health: system-table access and cache state |
| <span className="badge badge--info">POST</span> | `/api/watch/settings/cache/refresh` | <span className="badge badge--secondary">SP</span> | Force a cache refresh |
| <span className="badge badge--info">POST</span> | `/api/watch/admin/refresh-rollup` | <span className="badge badge--secondary">SP</span> | Rebuild usage rollups (admin-gated via `require_admin`) |

## Static File Serving (`main.py`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/` | <span className="badge badge--secondary">None</span> | Serve `index.html` (React SPA) |
| <span className="badge badge--success">GET</span> | `/{full_path:path}` | <span className="badge badge--secondary">None</span> | Serve static assets from `frontend/dist/`, fallback to SPA |

## SSE Streaming Endpoints

One endpoint uses Server-Sent Events:

| Endpoint | Keepalive | Events |
|----------|-----------|--------|
| `POST /api/create/agent/chat` | 15s | `session`, `step`, `thinking`, `tool_call`, `tool_result`, `message_delta`, `message`, `created`, `updated`, `heartbeat`, `error`, `done` |

The frontend consumes SSE via manual `fetch` + `ReadableStream` in `lib/api.ts` (not `EventSource`). Buffers are split on `\n\n`.

## Related Documentation

- [Authentication & Permissions](/docs/platform/authentication) — which identity is used where and why
- [Architecture Overview](/docs/getting-started/architecture-overview) — router and service structure
