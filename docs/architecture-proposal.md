# Genie Workbench → Multi-Agent Architecture

> **Status:** Proposal
> **Author:** Stuart Gano
> **Audience:** Sean Zhang (Workbench maintainer)
> **Date:** 2026-03-10

---

## Executive Summary

The Genie Workbench is a monolithic Databricks App (~10,200 lines backend) that hand-rolls OBO auth, tool-calling loops, SSE streaming, and SDK wrappers. Two FE-built libraries solve these exact problems:

- **AI Dev Kit** (`databricks-tools-core`) — pre-built Python functions for SQL execution, Unity Catalog browsing, and warehouse management
- **dbx-agent-app** — `@app_agent` decorator that auto-generates `/invocations` endpoints, agent cards, MCP servers, health checks, and handles OBO auth

This proposal refactors the Workbench into a **multi-agent system** where each capability is a separate, discoverable `@app_agent` app. The result: ~30% less code, free MCP servers, A2A discovery, and `mlflow.genai.evaluate()` support — with zero changes to the React frontend.

---

## Current Architecture (Monolith)

```
┌─────────────────────────────────────────────────┐
│  backend/main.py (FastAPI)                      │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │ OBOAuthMiddleware                        │   │
│  │ (hand-rolled ContextVar + x-forwarded-   │   │
│  │  access-token extraction)                │   │
│  └──────────────────────────────────────────┘   │
│                                                 │
│  ┌──────────┐ ┌──────────┐ ┌───────────────┐   │
│  │ routers/ │ │ routers/ │ │ routers/      │   │
│  │ spaces   │ │ analysis │ │ create        │   │
│  │ (scan,   │ │ (analyze,│ │ (UC discovery │   │
│  │  history, │ │  stream, │ │  agent chat,  │   │
│  │  star,   │ │  query,  │ │  validate,    │   │
│  │  fix)    │ │  optimize│ │  create)      │   │
│  └──────────┘ └──────────┘ └───────────────┘   │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │ services/                                │   │
│  │ scanner.py  analyzer.py  optimizer.py    │   │
│  │ fix_agent.py  create_agent.py            │   │
│  │ create_agent_tools.py (2,717 lines!)     │   │
│  │ create_agent_session.py                  │   │
│  │ uc_client.py  sql_executor.py            │   │
│  │ genie_client.py  lakebase.py  auth.py    │   │
│  └──────────────────────────────────────────┘   │
│                                                 │
│  frontend/dist/ (React SPA, static files)       │
└─────────────────────────────────────────────────┘
```

### Pain points

| Issue | Impact |
|-------|--------|
| `create_agent_tools.py` is 2,717 lines of hand-coded tool definitions + JSON schemas + dispatch table | Every new tool requires ~80 lines of boilerplate |
| OBO auth in `services/auth.py` (136 lines) uses ContextVar + middleware — breaks in streaming generators | Streaming endpoints need manual `set_obo_user_token()` re-establishment. Recent fix added `get_service_principal_client()` fallback for missing OAuth scopes |
| `genie_client.py` (244 lines) duplicates SP-fallback pattern (`_is_scope_error`) in every API call | Each new Genie API function must remember to add scope-error retry logic |
| `sql_executor.py` (220 lines) reimplements what `databricks-tools-core.sql` provides | Maintenance burden, no warehouse auto-detection improvements |
| `uc_client.py` (60 lines) reimplements what `databricks-tools-core.unity_catalog` provides | Duplicated effort |
| No agent discovery — other workspace apps can't call Workbench capabilities | Siloed functionality |
| No eval support — testing requires manual curl/browser interaction | No regression testing pipeline |
| Monolithic deployment — any change redeploys everything | Slow iteration on individual capabilities |

---

## Proposed Architecture (Multi-Agent)

```
┌─────────────────────────────────────────────────────────┐
│  genie-workbench (supervisor)                           │
│  React SPA + FastAPI shell                              │
│  Routes frontend API calls → sub-agent /invocations     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │ genie-   │  │ genie-   │  │ genie-   │             │
│  │ scorer   │  │ analyzer │  │ creator  │             │
│  │          │  │          │  │          │             │
│  │ IQ scan  │  │ LLM deep │  │ Space    │             │
│  │ scoring  │  │ analysis │  │ creation │             │
│  │ history  │  │ synthesis│  │ wizard   │             │
│  └──────────┘  └──────────┘  └──────────┘             │
│                                                         │
│  ┌──────────┐  ┌──────────┐                            │
│  │ genie-   │  │ genie-   │                            │
│  │ optimizer│  │ fixer    │                            │
│  │          │  │          │                            │
│  │ Benchmark│  │ AI fix   │                            │
│  │ labeling │  │ agent    │                            │
│  │ suggest  │  │ patches  │                            │
│  └──────────┘  └──────────┘                            │
└─────────────────────────────────────────────────────────┘
```

Each sub-agent is a standalone Databricks App with:
- **`@app_agent` decorator** — auto-generates `/invocations`, `/.well-known/agent.json`, `/health`, MCP server
- **OBO auth** — handled by `request.user_context` (replaces ContextVar middleware)
- **Tool definitions** — auto-generated from `@agent.tool()` decorated functions (replaces JSON schemas)
- **Eval support** — `app_predict_fn()` bridge to `mlflow.genai.evaluate()`

---

## Agent Decomposition

### Agent Boundaries

| Agent | Source | Tools | Needs Lakebase? | Streaming? | LLM? |
|-------|--------|-------|-----------------|------------|------|
| **genie-scorer** | `agents/scorer/` | `scan_space`, `get_history`, `toggle_star`, `list_spaces` | Yes (scores, stars) | No | No |
| **genie-analyzer** | `agents/analyzer/` | `fetch_space`, `analyze_section`, `analyze_all`, `query_genie`, `execute_sql` | No | Yes (SSE) | Yes |
| **genie-creator** | `agents/creator/` | All 16 current tools (discover_*, describe_*, profile_*, generate_config, etc.) | Yes (sessions) | Yes (SSE) | Yes |
| **genie-optimizer** | `agents/optimizer/` | `generate_suggestions`, `merge_config`, `label_benchmark` | No | No (heartbeat SSE) | Yes |
| **genie-fixer** | `agents/fixer/` | `generate_fixes`, `apply_patch` | No | Yes (SSE) | Yes |
| **supervisor** | root `app.py` | Routes to sub-agents, serves React SPA, `/api/settings`, `/api/auth` | Yes (starred) | Proxy | No |

### What moves where

```
backend/services/scanner.py        → agents/scorer/scanner.py       (as-is, domain logic)
backend/services/analyzer.py       → agents/analyzer/analyzer.py    (as-is, domain logic)
backend/services/optimizer.py      → agents/optimizer/optimizer.py  (as-is, domain logic)
backend/services/fix_agent.py      → agents/fixer/fix_agent.py     (as-is, domain logic)
backend/services/create_agent.py   → agents/creator/agent.py       (as-is, domain logic)
backend/services/create_agent_session.py → agents/creator/session.py (as-is)
backend/prompts_create/            → agents/creator/prompts/        (as-is)
backend/references/                → agents/creator/references/     (as-is)

backend/services/uc_client.py      → DELETED (replaced by databricks-tools-core)
backend/sql_executor.py            → DELETED (replaced by databricks-tools-core)
backend/routers/spaces.py          → DISSOLVED (endpoints become scorer/supervisor tools)
backend/routers/analysis.py        → DISSOLVED (endpoints become analyzer/optimizer tools)
backend/routers/create.py          → DISSOLVED (endpoints become creator tools)
```

### What stays custom (irreplaceable domain logic)

These files contain business logic specific to GenieIQ/GenieRx and move to their respective agents unchanged:

- `scanner.py` — Rule-based IQ scoring (maturity levels, dimension weights)
- `analyzer.py` — LLM checklist evaluation with session management
- `optimizer.py` — Optimization suggestion generation from labeling feedback
- `fix_agent.py` — Patch generation + application via Genie API
- `create_agent.py` — Tool-calling loop with message compaction, JSON repair, session recovery
- `create_agent_session.py` — Two-tier session persistence (memory + Lakebase)
- `prompts_create/` — Dynamic prompt assembly (9 modules: core, data_sources, requirements, plan, etc.)
- `references/schema.md` — Genie Space schema reference
- `genie_creator.py` — Genie API write operations
- `genie_client.py` — Genie API read operations (including SP-fallback for missing OAuth scopes, added in PR #7)
- `lakebase.py` — PostgreSQL persistence with in-memory fallback

---

## What Gets Replaced

### 1. Tool Definition Boilerplate → `@agent.tool()` Decorators

**Before** (create_agent_tools.py, ~80 lines per tool):
```python
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "discover_catalogs",
            "description": "List all Unity Catalog catalogs the user has access to.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    # ... 15 more tool definitions with nested JSON schemas ...
]

def handle_tool_call(name: str, arguments: dict, session_config=None) -> dict:
    handlers = {
        "discover_catalogs": _discover_catalogs,
        "discover_schemas": _discover_schemas,
        # ... 14 more entries ...
    }
    handler = handlers.get(name)
    # ... dispatch logic ...
```

**After** (auto-generated from function signatures):
```python
@creator.tool(description="List all Unity Catalog catalogs the user has access to.")
async def discover_catalogs() -> dict:
    from databricks_tools_core.unity_catalog import list_catalogs
    return {"catalogs": list_catalogs()}

@creator.tool(description="List schemas within a catalog.")
async def discover_schemas(catalog: str) -> dict:
    from databricks_tools_core.unity_catalog import list_schemas
    return {"schemas": list_schemas(catalog)}
```

**Impact:** ~580 lines of JSON schemas + 40-line dispatch table → auto-generated.

### 2. OBO Auth Middleware + SP Fallback → `request.user_context`

**Before** (main.py + auth.py + genie_client.py):
```python
# main.py — ContextVar middleware
class OBOAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        token = request.headers.get("x-forwarded-access-token", "")
        if token:
            set_obo_user_token(token)  # ContextVar
        request.state.user_token = token
        response = await call_next(request)
        if not is_streaming:
            clear_obo_user_token()
        return response

# auth.py — SP fallback for scope errors (added in PR #7)
def get_service_principal_client() -> WorkspaceClient:
    """Bypass OBO for ops requiring scopes the user token lacks."""
    return _get_default_client()

# genie_client.py — every API function repeats this pattern
try:
    return _get_space_with_client(client, genie_space_id)
except Exception as e:
    if _is_scope_error(e):
        sp_client = get_service_principal_client()
        return _get_space_with_client(sp_client, genie_space_id)

# In streaming generators:
if user_token:
    set_obo_user_token(user_token)  # Must re-establish in generator!
```

**After** (`@app_agent` handles it):
```python
@app_agent(name="genie-scorer", ...)
async def scorer(request: AgentRequest) -> AgentResponse:
    # request.user_context.access_token is automatically available
    # No ContextVar management, no SP fallback boilerplate
    ...
```

**Impact:** ~30 lines of middleware + SP fallback pattern duplicated across every API call → zero.

### 3. UC Client + SQL Executor → `databricks-tools-core`

| Current | Lines | Replacement |
|---------|-------|-------------|
| `backend/services/uc_client.py` | 60 | `from databricks_tools_core.unity_catalog import list_catalogs, list_schemas, list_tables` |
| `backend/sql_executor.py` | 220 | `from databricks_tools_core.sql import execute_sql, get_best_warehouse` |
| Warehouse auto-detection | 30 | `get_best_warehouse()` |

**Impact:** 310 lines deleted, replaced by maintained library functions.

---

## Deployment Topology

### agents.yaml

```yaml
project:
  name: genie-workbench
  workspace_path: /Workspace/Shared/apps

agents:
  - name: scorer
    source: ./agents/scorer
  - name: analyzer
    source: ./agents/analyzer
  - name: creator
    source: ./agents/creator
  - name: optimizer
    source: ./agents/optimizer
  - name: fixer
    source: ./agents/fixer
  - name: supervisor
    source: .
    depends_on: [scorer, analyzer, creator, optimizer, fixer]
    url_env_map:
      scorer: SCORER_URL
      analyzer: ANALYZER_URL
      creator: CREATOR_URL
      optimizer: OPTIMIZER_URL
      fixer: FIXER_URL
```

Each agent deploys as its own Databricks App with:
- Its own `app.yaml` defining env vars and resource bindings
- Its own service principal (for Lakebase, LLM endpoint access)
- Auto-generated `/.well-known/agent.json` for A2A discovery
- Auto-generated MCP server for tool integration

### Per-Agent app.yaml Example (scorer)

```yaml
command: ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
env:
  - name: LAKEBASE_HOST
    value: ""
  - name: LAKEBASE_INSTANCE_NAME
    value: ""
```

---

## Wire Protocol

### Frontend → Supervisor → Sub-Agents

The React SPA continues to hit the same API paths (`/api/spaces/*`, `/api/analyze/*`, `/api/create/*`). The supervisor proxies requests to sub-agents:

```
Browser → /api/spaces/scan       → supervisor → genie-scorer /invocations
Browser → /api/analyze/stream    → supervisor → genie-analyzer /invocations
Browser → /api/create/agent/chat → supervisor → genie-creator /invocations
Browser → /api/optimize          → supervisor → genie-optimizer /invocations
Browser → /api/spaces/{id}/fix   → supervisor → genie-fixer /invocations
```

The supervisor uses the Responses Agent protocol (or simple HTTP proxying) to forward requests. For streaming endpoints, the supervisor proxies SSE responses transparently.

### Agent-to-Agent (A2A) Discovery

After deployment, each agent exposes `/.well-known/agent.json`:

```json
{
  "name": "genie-scorer",
  "description": "IQ scoring for Genie Spaces",
  "url": "https://genie-workbench-scorer.cloud.databricks.com",
  "tools": [
    {"name": "scan_space", "description": "Run IQ scan on a Genie Space"},
    {"name": "get_history", "description": "Get score history"},
    {"name": "toggle_star", "description": "Toggle star on a space"}
  ]
}
```

Other workspace apps can discover and call these agents using `AgentDiscovery`.

---

## Migration Path (Phased, Backwards-Compatible)

### Phase 1: Scaffolding + Architecture Doc ← **This PR**

- Architecture proposal for review
- `agents.yaml` deployment config
- Skeleton `app.py` + `app.yaml` for each agent
- No behavior changes to existing monolith

### Phase 2: Extract genie-scorer (lowest risk)

**Why first:** No LLM calls, no streaming, no sessions — pure rule-based scoring. Validates the `@app_agent` pattern with minimal risk.

Files moved:
- `backend/services/scanner.py` → `agents/scorer/scanner.py` (as-is)
- Relevant Lakebase functions → `agents/scorer/lakebase.py`

What gets deleted from monolith:
- Scan/history/star endpoints from `backend/routers/spaces.py` (~80 lines)

### Phase 3: Extract genie-fixer (streaming + LLM, medium complexity)

**Why second:** Streaming SSE + LLM calls, but simpler than creator (no sessions, no 16 tools).

Files moved:
- `backend/services/fix_agent.py` → `agents/fixer/fix_agent.py`
- Fix prompt → `agents/fixer/prompts.py`

Validates: Streaming via async generator → SSE (auto-handled by `@app_agent`)

### Phase 4: Extract genie-analyzer (streaming + LLM, high complexity)

Files moved:
- `backend/services/analyzer.py` → `agents/analyzer/analyzer.py`
- Analysis prompts → `agents/analyzer/prompts/`

Tools: `fetch_space`, `analyze_section`, `analyze_all`, `query_genie`, `execute_sql`

### Phase 5: Extract genie-optimizer

Files moved:
- `backend/services/optimizer.py` → `agents/optimizer/optimizer.py`
- Benchmark labeling logic → `agents/optimizer/labeling.py`

Tools: `generate_suggestions`, `merge_config`, `label_benchmark`

### Phase 6: Extract genie-creator (most complex, last)

**Why last:** 16 tools, session persistence, complex tool-calling loop with message compaction. Hardest extraction.

Key change: 16 hand-coded tool definitions become `@creator.tool()` decorators:
```python
@creator.tool(description="List Unity Catalog catalogs")
async def discover_catalogs() -> dict:
    from databricks_tools_core.unity_catalog import list_catalogs
    return {"catalogs": list_catalogs()}
```

What stays custom: Dynamic prompt assembly, session persistence, message compaction, config generation/validation. These are domain logic.

What gets replaced:
- Tool definition boilerplate (~580 lines of JSON schemas → auto-generated from function signatures)
- `handle_tool_call()` dispatcher (~40 lines → auto-routing)
- OBO middleware → `request.user_context`

### Phase 7: Supervisor + Frontend

The supervisor becomes a thin shell that:
1. Serves the React SPA (static files)
2. Routes API calls to sub-agents
3. Handles settings and auth endpoints

Frontend changes: **Minimal.** API client (`frontend/src/lib/api.ts`) keeps hitting the same paths. The supervisor proxies to sub-agents transparently.

### Phase 8: AI Dev Kit Integration

Replace hand-rolled utilities with `databricks-tools-core` across all agents:

| Current | Lines | Replacement |
|---------|-------|-------------|
| `backend/services/uc_client.py` | 60 | `databricks_tools_core.unity_catalog` |
| `backend/sql_executor.py` | 220 | `databricks_tools_core.sql` |
| Warehouse auto-detection in sql_executor | 30 | `get_best_warehouse()` |

---

## Eval Story

Each agent becomes independently evaluatable via the `dbx-agent-app` bridge:

```python
from dbx_agent_app.bridge import app_predict_fn
import mlflow

predict = app_predict_fn("https://genie-workbench-scorer.cloud.databricks.com")
results = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=predict,
    scorers=[correctness_scorer, latency_scorer],
)
```

This replaces the current "manual curl and check" testing with automated, repeatable evaluation pipelines for each agent independently.

---

## Integration Challenges — Concrete Solutions

The three auth systems that need bridging:
- **Monolith auth** (`backend/services/auth.py:25`): `_obo_client` ContextVar → `WorkspaceClient`
- **`@app_agent`** (`dbx_agent_app/core/types.py:32`): `request.user_context` → `UserContext` with `.access_token`
- **`databricks-tools-core`** (`databricks_tools_core/auth.py:35-36`): `_host_ctx`/`_token_ctx` ContextVars via `set_databricks_auth()`

### 1. OBO Auth Bridge → `agents/_shared/auth_bridge.py`

**Problem:** Each agent receives `request.user_context` from `@app_agent`, but domain logic calls `get_workspace_client()` from the monolith's auth module. During migration, both patterns need to work. And `databricks-tools-core` functions use their own separate ContextVars.

**Solution:** `obo_context()` context manager that sets up all three auth systems in one `with` block:

```python
from agents._shared.auth_bridge import obo_context

@scorer.tool(description="Run IQ scan on a Genie Space")
async def scan_space(space_id: str, request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        # All of these now work:
        # - monolith's get_workspace_client() returns OBO client
        # - databricks-tools-core functions use OBO token
        result = scanner.calculate_score(space_id)
```

For streaming generators, capture the token before the generator starts and re-enter `obo_context()` per-yield (same pattern as `backend/routers/create.py:125-198`).

### 2. Complex Tool Schemas → `agents/creator/schemas.py`

**Problem:** `generate_config` has 11 parameters with 4-5 nesting levels (tables → column configs, example SQLs → parameters, etc.). `@app_agent`'s schema generator only handles primitives. The monolith defines these schemas as **~580 lines of hand-written JSON** in `create_agent_tools.py` — brittle, hard to maintain, and easy to get out of sync with the runtime code.

**Solution:** **~80 lines of Pydantic models** that auto-generate the equivalent JSON Schema via `.model_json_schema()` and double as runtime validation:

```python
from agents.creator.schemas import GenerateConfigArgs

@creator.tool(
    description="Generate a Genie Space configuration",
    parameters=GenerateConfigArgs.model_json_schema(),
)
async def generate_config(**kwargs) -> dict:
    args = GenerateConfigArgs(**kwargs)  # Validate at runtime
```

580 lines of hand-maintained JSON → 80 lines of Pydantic models. Schema and validation are always in sync because they come from the same source.

### 3. Frontend Transparency → `agents/supervisor/proxy.py`

**Problem:** The React SPA makes 28 API calls to `/api/*` that route to 5 different sub-agents after decomposition. The frontend should not change.

**Solution:** Ordered route table with prefix matching, glob support for path parameters, and SSE stream detection:

```python
ROUTE_TABLE = [
    ("/api/spaces/*/fix", "FIXER_URL"),   # specific before general
    ("/api/genie/create", "CREATOR_URL"),
    ("/api/spaces",       "SCORER_URL"),
    ("/api/analyze",      "ANALYZER_URL"),
    ("/api/create",       "CREATOR_URL"),
    # ... etc
]
```

SSE streams are detected by `content-type: text/event-stream` and forwarded as chunked bytes. OBO headers pass through automatically.

### 4. SP Fallback Decorator → `agents/_shared/sp_fallback.py`

**Problem:** The `_is_scope_error()` + retry-with-SP pattern is duplicated across `genie_client.py` and `spaces.py`. Each agent that calls Genie APIs needs this pattern.

**Solution:** `@with_sp_fallback` decorator and `genie_api_call()` convenience function:

```python
from agents._shared.sp_fallback import genie_api_call

# One-liner with automatic SP fallback
space = genie_api_call("GET", f"/api/2.0/genie/spaces/{space_id}",
                       query={"include_serialized_space": "true"})
```

### 5. Shared Lakebase Pool → `agents/_shared/lakebase_client.py`

**Problem:** Multiple agents need Lakebase (scorer for scores/stars, creator for sessions). Each runs as a separate Databricks App with its own credentials.

**Solution:** Shared pool lifecycle + idempotent DDL per agent:

```python
from agents._shared.lakebase_client import init_pool, SCORER_DDL

# At startup — creates tables if they don't exist
await init_pool(SCORER_DDL)
```

Each agent initializes its own pool from its own env vars. Domain-specific query functions stay in each agent's module. The shared client manages pool lifecycle, credential generation, and DDL only.

---

## Estimated Impact

| Metric | Before | After |
|--------|--------|-------|
| Backend Python lines | ~10,178 | ~7,100 (30% reduction from eliminating boilerplate) |
| Files deleted | 0 | 5 (routers + utility wrappers replaced by libraries) |
| Tool definition boilerplate | ~580 lines JSON schemas | 0 (auto-generated from type hints) |
| Dispatch table code | ~40 lines | 0 (auto-routing by `@app_agent`) |
| OBO auth code | ~30 lines middleware | 0 (handled by framework) |
| Auto-generated endpoints | 0 | 30+ (5 agents × 6 endpoints each: /invocations, /health, agent.json, MCP, etc.) |
| MCP servers | 0 | 5 (one per agent, free) |
| Agent discovery | None | A2A protocol, workspace-wide |
| Eval support | Manual testing | `mlflow.genai.evaluate()` via bridge |
| Deployment | Single `databricks apps deploy` | `dbx-agent-app deploy --config agents.yaml` (per-agent or all) |

---

## Verification Plan

1. **Unit tests:** Each agent's tools can be tested independently via `agent(AgentRequest(...))` — the `@app_agent` decorator makes the handler directly callable.

2. **Integration tests:** Deploy all agents locally (`uvicorn agents/scorer/app:app --port 8001`, etc.), configure supervisor with local URLs, run existing E2E tests.

3. **A2A discovery:** After deploying to Databricks Apps, verify `/.well-known/agent.json` returns correct agent cards. Use `AgentDiscovery` to scan workspace.

4. **Eval bridge:** Run `mlflow.genai.evaluate()` against each deployed agent using `app_predict_fn()`.

5. **Frontend smoke test:** Verify React SPA still works end-to-end through the supervisor proxy.

---

## Files in This PR

### New files — scaffolds + deployment
- `docs/architecture-proposal.md` — this document
- `agents.yaml` — multi-agent deployment config
- `agents/scorer/app.py` — scorer agent scaffold
- `agents/scorer/app.yaml` — scorer Databricks Apps config
- `agents/analyzer/app.py` — analyzer agent scaffold
- `agents/analyzer/app.yaml` — analyzer Databricks Apps config
- `agents/creator/app.py` — creator agent scaffold
- `agents/creator/app.yaml` — creator Databricks Apps config
- `agents/optimizer/app.py` — optimizer agent scaffold
- `agents/optimizer/app.yaml` — optimizer Databricks Apps config
- `agents/fixer/app.py` — fixer agent scaffold
- `agents/fixer/app.yaml` — fixer Databricks Apps config

### New files — integration challenge solutions
- `agents/_shared/__init__.py` — shared utilities package
- `agents/_shared/auth_bridge.py` — Challenge 1: OBO auth context manager bridging all 3 auth systems
- `agents/_shared/sp_fallback.py` — Challenge 4: SP fallback decorator for Genie API scope errors
- `agents/_shared/lakebase_client.py` — Challenge 5: Shared Lakebase pool with idempotent DDL
- `agents/creator/schemas.py` — Challenge 2: Pydantic models replacing ~580 lines of JSON schemas
- `agents/supervisor/__init__.py` — supervisor package
- `agents/supervisor/proxy.py` — Challenge 3: Frontend-transparent proxy with SSE support

### Modified files
- `agents/scorer/app.py` — wired up auth_bridge, sp_fallback, and lakebase_client imports
- `agents/creator/app.py` — uses Pydantic schema override for generate_config/present_plan
- `docs/architecture-proposal.md` — replaced placeholder challenge descriptions with concrete solutions

### No changes to existing monolith
The existing `backend/` code is untouched. All new files are additive.
