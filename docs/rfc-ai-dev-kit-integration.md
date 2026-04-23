# RFC: AI Dev Kit Integration

**Status:** Draft — requesting feedback
**Author:** Stuart Gano
**Date:** 2026-03-13

---

## Context

[AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) (`databricks-solutions/ai-dev-kit`) is the Field Engineering toolkit for coding agents. It provides:

- **`databricks-tools-core`** — Python library with high-level Databricks functions (SQL, Unity Catalog, Jobs, Genie, etc.)
- **`databricks-mcp-server`** — 80+ MCP tools wrapping `databricks-tools-core` for Claude Code, Cursor, etc.
- **`databricks-skills`** — Markdown skills teaching coding agents Databricks patterns
- **Agent Bricks** — Patterns for building Knowledge Assistants, Genie Spaces, and Multi-Agent Supervisors

Genie Workbench is a deployed Databricks App for scoring, analyzing, and optimizing Genie Spaces. It has its own hand-rolled implementations of Databricks API calls (Genie, UC, SQL) and its own LLM-driven agents (create, fix, analyze).

**The gap:** These two projects serve overlapping audiences with duplicated implementations and no cross-pollination. A coding agent using ai-dev-kit can create a Genie Space but has no way to score or optimize it. The Workbench app hand-rolls SDK calls that `databricks-tools-core` already provides.

---

## Proposal

Three integration tracks, from lowest to highest effort:

### Track 1: Genie Space Quality Skill for AI Dev Kit

**Effort:** Low (markdown + examples, no code changes to Workbench)
**Value:** High — makes maturity knowledge available to all ai-dev-kit users

Add a skill to `ai-dev-kit/.claude/skills/` that teaches coding agents how to assess and improve Genie Space quality. This skill would encode the maturity curve knowledge, scoring criteria, and optimization patterns that currently live only in the Workbench app.

#### Skill content

```
ai-dev-kit/.claude/skills/databricks-genie-quality/
  SKILL.md              # Main skill: when to use, maturity model, scoring criteria
  optimization.md       # Deep dive: common findings and how to fix them
  examples/
    score-space.py      # Example: score a space using the Genie API
    add-instructions.py # Example: programmatically add instructions
    add-joins.py        # Example: add join specifications
```

#### What the skill teaches

1. **Maturity model** — The 5-stage model (Nascent → Optimized) with scoring criteria
2. **Assessment patterns** — How to fetch a serialized space and evaluate it against criteria
3. **Optimization patterns** — Common findings and programmatic fixes:
   - Missing table/column descriptions → pull from UC comments
   - No join specs → infer from foreign keys
   - Missing instructions → generate from table schemas
   - No sample questions → generate from common query patterns
4. **Genie API gotchas** — `serialized_space` retrieval (empty PATCH), table sorting requirement, `table_identifiers` being silently ignored

#### What this enables

A developer using Claude Code + ai-dev-kit could say:
- "Score my Genie Space `sales_analytics` and tell me what to improve"
- "Add column descriptions to all tables in my space from UC metadata"
- "Generate sample SQL questions for my Genie Space"

Without needing to deploy the Workbench app.

---

### Track 2: Replace Hand-Rolled SDK Calls with `databricks-tools-core`

**Effort:** Medium (refactor backend services)
**Value:** Medium — reduces maintenance burden, stays in sync with SDK improvements

The Workbench backend has several services that duplicate what `databricks-tools-core` already provides:

| Workbench service | What it does | `databricks-tools-core` equivalent |
|---|---|---|
| `genie_client.py` → `list_genie_spaces()` | Lists all Genie Spaces | `find_genie_by_name()` + list variant |
| `genie_client.py` → `get_serialized_space()` | Fetches space config via empty PATCH | Genie tools (if available) |
| `uc_client.py` → `list_catalogs/schemas/tables()` | UC browsing for create wizard | UC tools in `databricks-tools-core` |
| `create_agent_tools.py` → `execute_sql()` | SQL execution for validation | `execute_sql()` |
| `genie_client.py` → `query_genie()` | Sends questions to Genie | Genie query tools |

#### Approach

1. **Add `databricks-tools-core` as a dependency** to `pyproject.toml`
2. **Audit overlap** — Map each Workbench SDK call to its `databricks-tools-core` equivalent
3. **Replace incrementally** — Start with UC browsing and SQL execution (lowest risk), then move to Genie API calls
4. **Keep Workbench-specific logic** — The scoring engine, LLM analysis, and config merge logic are unique to Workbench and don't belong in the shared library

#### What NOT to replace

- `scanner.py` — Scoring logic is Workbench-specific
- `analyzer.py` / `optimizer.py` — LLM-driven analysis is Workbench-specific
- `maturity_config.py` — Config management is Workbench-specific
- `lakebase.py` — Persistence layer is Workbench-specific

#### Open questions

- Does `databricks-tools-core` handle OBO auth (ContextVar-based)? The Workbench uses `x-forwarded-access-token` from Databricks Apps. If the library only supports SP or PAT auth, we'd need to wrap it or contribute OBO support upstream.
- Does the library expose `serialized_space` retrieval? This is the non-obvious empty-PATCH trick that the Workbench relies on heavily. If not, this stays hand-rolled or we contribute it.
- Version pinning — How stable is `databricks-tools-core`? Is it safe to depend on in a deployed app, or is it still iterating fast?

---

### Track 3: Workbench MCP Server

**Effort:** Medium-High (new API surface, deployment considerations)
**Value:** High — makes Workbench capabilities available to any MCP client

Expose Workbench-specific capabilities as MCP tools that coding agents can call. This is different from ai-dev-kit's MCP server which wraps raw Databricks APIs — the Workbench MCP server would expose higher-level operations.

#### Proposed tools

| Tool | Description | Input | Output |
|---|---|---|---|
| `scan_genie_space` | Score a space against the maturity model | `{space_id}` | Score, maturity stage, findings, next steps |
| `get_maturity_config` | Get active scoring configuration | — | Config with stages, criteria, weights |
| `get_space_history` | Get score history for a space | `{space_id, days?}` | Array of score data points |
| `get_org_health` | Get org-wide space health stats | — | Avg score, distribution, critical count |
| `analyze_space` | Run deep LLM analysis on a space | `{space_id}` | Section analyses, synthesis, recommendations |
| `suggest_fixes` | Get specific fix suggestions for findings | `{space_id, findings}` | Actionable patches/instructions |

#### Architecture options

**Option A: Standalone MCP server (recommended for now)**
- Runs as a separate process, talks to the Workbench API over HTTP
- Easy to register in `.mcp.json` alongside ai-dev-kit's MCP server
- Doesn't require Workbench internals — just calls the REST API
- Works for any MCP client (Claude Code, Cursor, etc.)

```
Coding Agent → MCP Client → Workbench MCP Server → Workbench REST API → Databricks
```

**Option B: Contribute tools to ai-dev-kit's MCP server**
- Add Workbench tools alongside existing Databricks tools
- Requires the Workbench app to be deployed (tools call its API)
- Tighter coupling but single MCP config for users

**Option C: Embed MCP in the Workbench app**
- FastMCP mounted directly in the FastAPI app (streamable HTTP transport)
- No separate process, but only works when the app is running
- This was the direction of the earlier `mcp_server.py` prototype

#### Open questions

- **Auth flow** — MCP tools running in Claude Code on a developer's laptop need to reach the deployed Workbench app. How does auth work? Service principal? User token forwarding?
- **Latency** — `analyze_space` and `suggest_fixes` involve LLM calls (30-60s). MCP tools typically expect fast responses. Do we need async patterns or progress callbacks?
- **Scope** — Should the MCP server also expose the create-space agent, or keep it UI-only?

---

## Sequencing

```
Track 1 (Skill)         ████░░░░░░  ← Start here, ships independently to ai-dev-kit
Track 2 (tools-core)    ░░████░░░░  ← Refactor after skill validates the integration story
Track 3 (MCP Server)    ░░░░░░████  ← Build after Tracks 1-2 establish patterns
```

**Track 1** can start immediately and doesn't require any code changes to the Workbench. It's a PR to `ai-dev-kit`, not this repo. The skill content comes directly from `docs/genie-space-maturity.md` and the patterns in `scanner.py`.

**Track 2** requires auditing `databricks-tools-core`'s API surface and auth model. Could start in parallel with Track 1 if someone wants to spike on the OBO auth question.

**Track 3** depends on having a stable Workbench API and a clear auth story. The `mcp_server.py` prototype exists but needs the auth and deployment model figured out first.

---

## Decision needed

1. **Do we start with Track 1?** It's low-cost and tests the integration story without coupling the codebases.
2. **Who owns the ai-dev-kit PR?** The skill content exists in this repo — do we author the PR here and open it against ai-dev-kit?
3. **Track 2 auth question** — Can someone with `databricks-tools-core` context confirm whether OBO auth is supported?

---

## References

- [AI Dev Kit repo](https://github.com/databricks-solutions/ai-dev-kit)
- [databricks-tools-core](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-tools-core)
- [AI Dev Kit skills](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills)
- [Genie Space Maturity Curve](docs/genie-space-maturity.md) (this repo)
- [Configurable Scoring RFC](https://github.com/databricks-solutions/databricks-genie-workbench/pull/18) (PR #18)
