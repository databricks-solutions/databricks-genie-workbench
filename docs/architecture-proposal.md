# Agent Deployment Layer for Genie Workbench

> **Status:** Proposal
> **Author:** Stuart Gano
> **Date:** 2026-03-10

---

## Summary

The Genie Workbench now has scoring, analysis, optimization, creation, and auto-optimization all working as a Databricks App. This proposal adds an **agent deployment layer** so each capability can also be deployed as a standalone Databricks agent — enabling A2A discovery, MCP tool integration, and independent `mlflow.genai.evaluate()` testing.

The existing backend is unchanged. The agent layer wraps existing domain logic using:

- **`dbx-agent-app`** (`@app_agent` decorator) — auto-generates `/invocations` endpoints, agent cards, MCP servers, and health checks
- **AI Dev Kit** (`databricks-tools-core`) — optional drop-in replacements for UC browsing and SQL execution

**What this enables:**
- Other workspace apps can discover and call Workbench capabilities via A2A protocol
- Each agent gets a free MCP server for tool integration
- Automated eval pipelines via `mlflow.genai.evaluate()` against individual agents
- Independent deployment of individual capabilities when needed

**What this does NOT change:**
- The existing monolith deployment continues to work as-is
- The React frontend is unmodified
- All existing domain logic (scanner, analyzer, optimizer, fix agent, create agent, GSO) stays in place

---

## Architecture

The agent layer sits alongside the existing monolith. Both deployment modes work:

```
┌─────────────────────────────────────────────────────────────────┐
│  EXISTING: Monolith (unchanged)                                 │
│  backend/main.py → routers → services → frontend/dist           │
│  Deployed via: databricks apps deploy                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  NEW: Agent Layer (additive)                                    │
│  Deployed via: dbx-agent-app deploy --config agents.yaml        │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                     │
│  │ genie-   │  │ genie-   │  │ genie-   │                     │
│  │ scorer   │  │ analyzer │  │ creator  │                     │
│  │ wraps:   │  │ wraps:   │  │ wraps:   │                     │
│  │ scanner  │  │ analyzer │  │ create_  │                     │
│  │ .py      │  │ .py      │  │ agent.py │                     │
│  └──────────┘  └──────────┘  └──────────┘                     │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                     │
│  │ genie-   │  │ genie-   │  │supervisor│                     │
│  │ optimizer│  │ fixer    │  │ React SPA│                     │
│  │ wraps:   │  │ wraps:   │  │ + proxy  │                     │
│  │ optimizer│  │ fix_agent│  │          │                     │
│  │ .py      │  │ .py      │  │          │                     │
│  └──────────┘  └──────────┘  └──────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```

Each agent wraps existing domain logic and adds:
- **`@app_agent` decorator** — auto-generates `/invocations`, `/.well-known/agent.json`, `/health`, MCP server
- **OBO auth** — `request.user_context` bridges into existing auth via `obo_context()`
- **Tool definitions** — auto-generated from `@agent.tool()` decorated functions
- **Eval support** — `app_predict_fn()` bridge to `mlflow.genai.evaluate()`

---

## Agent Decomposition

### What each agent wraps

| Agent | Wraps | Tools exposed | Lakebase? | Streaming? | LLM? |
|-------|-------|---------------|-----------|------------|------|
| **genie-scorer** | `services/scanner.py` | `scan_space`, `get_history`, `toggle_star`, `list_spaces` | Yes (scores, stars) | No | No |
| **genie-analyzer** | `services/analyzer.py`, `services/genie_client.py` | `fetch_space`, `analyze_section`, `analyze_all`, `query_genie`, `execute_sql` | No | Yes (SSE) | Yes |
| **genie-creator** | `services/create_agent.py`, `services/create_agent_tools.py` | All 16 current tools (discover_*, describe_*, profile_*, generate_config, etc.) | Yes (sessions) | Yes (SSE) | Yes |
| **genie-optimizer** | `services/optimizer.py` | `generate_suggestions`, `merge_config`, `label_benchmark` | No | No (heartbeat SSE) | Yes |
| **genie-fixer** | `services/fix_agent.py` | `generate_fixes`, `apply_patch` | No | Yes (SSE) | Yes |
| **supervisor** | Existing React SPA | Routes frontend API calls to sub-agents, serves static files | Yes (starred) | Proxy | No |

Each agent imports from `backend/services/` — the domain logic stays where it is. The agent layer is a thin wrapper that exposes existing functions as agent tools with standard protocol support.

### Domain logic (unchanged)

These files contain the business logic that agents wrap. They are not modified:

- `scanner.py` — Rule-based IQ scoring (maturity levels, dimension weights)
- `analyzer.py` — LLM checklist evaluation with session management
- `optimizer.py` — Optimization suggestion generation from labeling feedback
- `fix_agent.py` — Patch generation + application via Genie API
- `create_agent.py` — Tool-calling loop with message compaction, JSON repair, session recovery
- `create_agent_session.py` — Two-tier session persistence (memory + Lakebase)
- `prompts_create/` — Dynamic prompt assembly (9 modules: core, data_sources, requirements, plan, etc.)
- `references/schema.md` — Genie Space schema reference
- `genie_client.py` — Genie API read operations (including SP-fallback for missing OAuth scopes)
- `lakebase.py` — PostgreSQL persistence with in-memory fallback
- `auto_optimize.py` + GSO package — Auto-optimization pipeline

---

## What the Agent Layer Provides

### 1. Auto-generated tool definitions from `@agent.tool()` decorators

Agent tools are defined as decorated functions — schemas, dispatch, and validation are auto-generated:

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

For tools with complex nested parameters (like `generate_config`), Pydantic models provide the schema and runtime validation in one place — see `agents/creator/schemas.py`.

### 2. OBO auth bridging via `obo_context()`

Agents receive the user's token via `request.user_context`. The `obo_context()` context manager bridges this into the existing `get_workspace_client()` pattern so domain logic works unchanged:

```python
@scorer.tool(description="Run IQ scan on a Genie Space")
async def scan_space(space_id: str, request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        # Existing scanner.py works as-is — get_workspace_client() returns OBO client
        result = scanner.calculate_score(space_id)
```

### 3. Optional AI Dev Kit integration

Where applicable, agents can use `databricks-tools-core` as drop-in replacements:

| Existing service | AI Dev Kit equivalent |
|---------|-------------|
| `backend/services/uc_client.py` | `databricks_tools_core.unity_catalog` |
| SQL execution in various services | `databricks_tools_core.sql` |
| Warehouse auto-detection | `get_best_warehouse()` |

This is optional and incremental — agents can import existing services or AI Dev Kit functions interchangeably.

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

## Implementation Roadmap

Each phase adds a working agent. The monolith continues to serve production throughout.

### Phase 1: Scaffolds + Architecture ← **This PR**

- Agent scaffolds with tool signatures and source traceability
- `agents.yaml` deployment config
- Shared modules: `auth_bridge.py`, `lakebase_client.py`, `sp_fallback.py`
- This document

### Phase 2: Wire up genie-scorer (lowest risk)

**Why first:** No LLM calls, no streaming, no sessions — pure rule-based scoring. Validates the `@app_agent` + `obo_context()` pattern end-to-end.

- Agent tool implementations call `backend/services/scanner.py` directly
- Deploy alongside monolith, verify via `/invocations` and MCP

### Phase 3: Wire up genie-fixer (streaming + LLM)

Validates SSE streaming through `@app_agent` + LLM tool calling.

### Phase 4: Wire up genie-analyzer (streaming + LLM, multi-tool)

Tools: `fetch_space`, `analyze_section`, `analyze_all`, `query_genie`, `execute_sql`

### Phase 5: Wire up genie-optimizer

Tools: `generate_suggestions`, `merge_config`, `label_benchmark`

### Phase 6: Wire up genie-creator (most complex)

16 tools, session persistence, complex tool-calling loop. Pydantic schemas (in `agents/creator/schemas.py`) replace hand-written JSON tool definitions.

### Phase 7: Supervisor + frontend proxy

Optional: if agent deployment becomes the primary mode, add a supervisor that serves the React SPA and proxies API calls to sub-agents. The frontend stays unchanged — same API paths, same behavior.

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

## Shared Modules

The agent layer includes shared utilities in `agents/_shared/` that handle the integration between `@app_agent` and existing backend services.

### 1. Auth Bridge → `agents/_shared/auth_bridge.py`

`obo_context()` is a context manager that bridges `@app_agent`'s `request.user_context` into the existing `get_workspace_client()` pattern, plus `databricks-tools-core` ContextVars. This lets agent tools call existing domain logic without modification:

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

For tools with deeply nested parameters (like `generate_config` with 11 params across 4-5 nesting levels), Pydantic models provide the JSON Schema and runtime validation in ~80 lines:

```python
from agents.creator.schemas import GenerateConfigArgs

@creator.tool(
    description="Generate a Genie Space configuration",
    parameters=GenerateConfigArgs.model_json_schema(),
)
async def generate_config(**kwargs) -> dict:
    args = GenerateConfigArgs(**kwargs)  # Validate at runtime
```

Schema and validation stay in sync because they come from the same source.

### 3. Supervisor Proxy → `agents/supervisor/proxy.py`

If agents are deployed independently, the supervisor proxies frontend API calls to the correct agent. Ordered route table with prefix matching, glob support for path parameters, and SSE stream detection:

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

### 4. SP Fallback → `agents/_shared/sp_fallback.py`

Centralizes the SP-fallback pattern for Genie API calls where the user's OBO token may lack required OAuth scopes:

```python
from agents._shared.sp_fallback import genie_api_call

# One-liner with automatic SP fallback
space = genie_api_call("GET", f"/api/2.0/genie/spaces/{space_id}",
                       query={"include_serialized_space": "true"})
```

### 5. Shared Lakebase Pool → `agents/_shared/lakebase_client.py`

Shared asyncpg pool lifecycle with idempotent DDL. Each agent initializes its own pool from its own env vars:

```python
from agents._shared.lakebase_client import init_pool, SCORER_DDL

# At startup — creates tables if they don't exist
await init_pool(SCORER_DDL)
```

Each agent initializes its own pool from its own env vars. Domain-specific query functions stay in each agent's module. The shared client manages pool lifecycle, credential generation, and DDL only.

---

## What You Get

| Capability | Today | With agent layer |
|------------|-------|-----------------|
| Auto-generated endpoints | — | 30+ (5 agents × `/invocations`, `/health`, `agent.json`, MCP, etc.) |
| MCP servers | — | 5 (one per agent, free) |
| Agent discovery | — | A2A protocol, workspace-wide |
| Eval support | Manual testing | `mlflow.genai.evaluate()` via `app_predict_fn()` bridge |
| Independent deployment | — | `dbx-agent-app deploy --config agents.yaml --agent scorer` |
| Tool definitions | Hand-written JSON schemas | Auto-generated from function signatures + Pydantic models |

---

## Verification Plan

1. **Unit tests:** Each agent's tools can be tested independently via `agent(AgentRequest(...))` — the `@app_agent` decorator makes the handler directly callable.

2. **Integration tests:** Deploy all agents locally (`uvicorn agents/scorer/app:app --port 8001`, etc.), configure supervisor with local URLs, run existing E2E tests.

3. **A2A discovery:** After deploying to Databricks Apps, verify `/.well-known/agent.json` returns correct agent cards. Use `AgentDiscovery` to scan workspace.

4. **Eval bridge:** Run `mlflow.genai.evaluate()` against each deployed agent using `app_predict_fn()`.

5. **Frontend smoke test:** Verify React SPA still works end-to-end through the supervisor proxy.

---

## Files in This PR

All files are additive. No changes to existing `backend/`, `frontend/`, `packages/`, or `scripts/`.

### Agent scaffolds
- `agents.yaml` — multi-agent deployment config
- `agents/scorer/app.py` + `app.yaml` — scorer agent (wraps scanner.py)
- `agents/analyzer/app.py` + `app.yaml` — analyzer agent (wraps analyzer.py)
- `agents/creator/app.py` + `app.yaml` — creator agent (wraps create_agent.py)
- `agents/creator/schemas.py` — Pydantic models for complex tool parameters
- `agents/optimizer/app.py` + `app.yaml` — optimizer agent (wraps optimizer.py)
- `agents/fixer/app.py` + `app.yaml` — fixer agent (wraps fix_agent.py)
- `agents/supervisor/proxy.py` — frontend-transparent proxy with SSE support

### Shared modules
- `agents/_shared/auth_bridge.py` — OBO auth context manager bridging `@app_agent` ↔ existing services
- `agents/_shared/sp_fallback.py` — SP fallback decorator for Genie API scope errors
- `agents/_shared/lakebase_client.py` — Shared Lakebase pool with idempotent DDL

### Documentation
- `docs/architecture-proposal.md` — this document
- `docs/genierx-spec.md` — GenieRX analyzer/recommender specification
