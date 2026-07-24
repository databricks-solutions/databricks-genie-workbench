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
| <span className="badge badge--info">POST</span> | `/api/space/fetch` | <span className="badge badge--info">OBO → SP</span> | Fetch serialized Genie Space by ID |
| <span className="badge badge--info">POST</span> | `/api/space/parse` | <span className="badge badge--secondary">None</span> | Parse pasted Genie API JSON (client-side data, no auth needed) |
| <span className="badge badge--success">GET</span> | `/api/debug/auth` | <span className="badge badge--primary">OBO</span> | Dev-only auth debug endpoint (404 on Databricks Apps) |
| <span className="badge badge--success">GET</span> | `/api/settings` | <span className="badge badge--secondary">None</span> | Read-only app settings (LLM model, warehouse, host) |

## Spaces Router (`/api`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/spaces` | <span className="badge badge--info">OBO → SP</span> | List Genie Spaces with IQ scores, starred sort, filters |
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
| <span className="badge badge--success">GET</span> | `/api/create/discover/catalogs` | <span className="badge badge--primary">OBO</span> | List Unity Catalog catalogs |
| <span className="badge badge--success">GET</span> | `/api/create/discover/schemas` | <span className="badge badge--primary">OBO</span> | List schemas in a catalog |
| <span className="badge badge--success">GET</span> | `/api/create/discover/tables` | <span className="badge badge--primary">OBO</span> | List tables in a catalog.schema |
| <span className="badge badge--success">GET</span> | `/api/create/discover/columns` | <span className="badge badge--primary">OBO</span> | List columns for a table |
| <span className="badge badge--info">POST</span> | `/api/create/validate` | <span className="badge badge--primary">OBO</span> | Validate serialized space config (errors/warnings) |
| <span className="badge badge--info">POST</span> | `/api/create` | <span className="badge badge--primary">OBO</span> | Create Genie Space from wizard payload |
| <span className="badge badge--info">POST</span> | `/api/create/agent/chat` | <span className="badge badge--primary">OBO</span> | **SSE** — Create agent conversational flow |
| <span className="badge badge--success">GET</span> | `/api/create/agent/sessions/{session_id}` | <span className="badge badge--primary">OBO</span> | Load agent session for refresh/reconnect |
| <span className="badge badge--danger">DELETE</span> | `/api/create/agent/sessions/{session_id}` | <span className="badge badge--primary">OBO</span> | Delete agent session |

## Auto-Optimize Router (`/api/auto-optimize`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/health` | <span className="badge badge--secondary">SP</span> | GSO health check: job/warehouse configuration status |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/permissions/{space_id}` | <span className="badge badge--warning">Mixed</span> | Pre-check SP manage + UC read |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/trigger` | <span className="badge badge--warning">Mixed</span> | Start GSO optimization job (OBO for auth, SP for job submission) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}` | <span className="badge badge--secondary">SP</span> | Full run detail: stages, steps, levers, links |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/status` | <span className="badge badge--secondary">SP</span> | Lightweight status poll: steps, scores |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/levers` | <span className="badge badge--secondary">None</span> | List optimization lever definitions |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/runs/{run_id}/apply` | <span className="badge badge--primary">OBO</span> | Apply optimization results to the Genie Space |
| <span className="badge badge--info">POST</span> | `/api/auto-optimize/runs/{run_id}/discard` | <span className="badge badge--warning">Mixed</span> | Discard run / rollback changes |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/spaces/{space_id}/active-run` | <span className="badge badge--secondary">SP</span> | Check for QUEUED/IN_PROGRESS run |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/spaces/{space_id}/runs` | <span className="badge badge--secondary">SP</span> | List optimization runs for a space |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/iterations` | <span className="badge badge--secondary">SP</span> | Per-iteration evaluation rows |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/debug-data` | <span className="badge badge--secondary">SP</span> | Diagnostics for Lakebase vs Delta data |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/asi-results` | <span className="badge badge--secondary">SP</span> | ASI judge results (requires `iteration` param) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/question-results` | <span className="badge badge--secondary">SP</span> | Per-question results (requires `iteration` param) |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/patches` | <span className="badge badge--secondary">SP</span> | All patches for the run |
| <span className="badge badge--success">GET</span> | `/api/auto-optimize/runs/{run_id}/benchmark-changes` | <span className="badge badge--secondary">SP</span> | Benchmark mutation ledger plus QC window, structured quality findings, semantic-review coverage, and proposed repairs |

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
