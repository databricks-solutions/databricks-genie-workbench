# apx Project

Full-stack Databricks App built with apx (React + Vite frontend, FastAPI backend).

## Do's and Don'ts
- OpenAPI client auto-regenerates on code changes when dev servers are running - don't manually regenerate.
- Prefer running apx related commands via MCP server if it's available.
- Use the apx MCP `search_registry_components` and `add_component` tools to find and add shadcn/ui components.
- When using the API calls on the frontend, use error boundaries to handle errors.
- Run `apx dev check` command (via CLI or MCP) to check for errors in the project code after making changes.
- If agent has access to native browser tool, use it to verify changes on the frontend. If such tool is not present or is not working, use playwright MCP to automate browser actions (e.g. screenshots, clicks, etc.).
- Avoid unnecessary restarts of the development servers
- **Databricks SDK:** Use the apx MCP `docs` tool to search Databricks SDK documentation instead of guessing or hallucinating API signatures.

## Package Management
- **Frontend:** Use `apx bun install` or `apx bun add <dependency>` for frontend package management.
- **Python:** Always use `uv` (never `pip`)

## Component Management
- **Check configured registries first:** Before building custom components, check `[tool.apx.ui.registries]` in `pyproject.toml` for domain-specific registries (e.g. `@ai-elements` for chat/AI, `@animate-ui` for animations). Use `list_registry_components` with the registry name to browse available components.
- **Finding components:** Use MCP `search_registry_components` to search across all configured registries. Results from project-configured registries are boosted.
- **Adding components:** Use MCP `add_component` or CLI `apx components add <component> --yes` to add components
- **Component location:** If component was added to a wrong location (e.g. stored into `src/components` instead of `src/genie-space-optimizer/ui/components`), move it to the proper folder
- **Component organization:** Prefer grouping components by functionality rather than by file type (e.g. `src/genie-space-optimizer/ui/components/chat/`)

## Project Structure
Full-stack app: `src/genie-space-optimizer/ui/` (React + Vite) and `src/genie-space-optimizer/backend/` (FastAPI). Backend serves frontend at `/` and API at `/api`. API client auto-generated from OpenAPI schema.

## Dependencies & Dependency Injection

The `Dependency` class in `src/genie-space-optimizer/backend/core.py` provides typed FastAPI dependencies. **Always use these instead of manually creating clients or accessing `request.app.state`.**

| Dependency | Type | Description |
|---|---|---|
| `Dependencies.Client` | `WorkspaceClient` | Databricks client using app-level service principal credentials |
| `Dependencies.UserClient` | `WorkspaceClient` | Databricks client authenticated on behalf of the current user (requires OBO token) |
| `Dependencies.Config` | `AppConfig` | Application configuration loaded from environment variables |
| `Dependencies.Session` | `Session` | SQLModel database session, scoped to request (requires lakebase addon) |

## Models & API
- **3-model pattern:** `Entity` (DB), `EntityIn` (input), `EntityOut` (output)
- **API routes must have:** `response_model` and `operation_id` for client generation

## Frontend Rules
- **Routing:** `@tanstack/react-router` (routes in `src/genie-space-optimizer/ui/routes/`)
- **Data fetching:** Always use `useXSuspense` hooks with `Suspense` and `Skeleton` components
- **Pattern:** Render static elements immediately, fetch API data with suspense
- **Components:** Use shadcn/ui, add to `src/genie-space-optimizer/ui/components/`
- **Data access:** Use `selector()` function for clean destructuring (e.g., `const {data: profile} = useProfileSuspense(selector())`)

## MCP Tools Reference

This project is configured with the **apx MCP server** (see `.mcp.json`). Always prefer MCP tools over CLI commands — they are faster and provide structured output.

| Tool | Description |
|------|-------------|
| `start` | Start development server and return the URL |
| `stop` | Stop the development server |
| `restart` | Restart the development server (preserves port if possible) |
| `logs` | Fetch recent dev server logs |
| `check` | Check project code for errors (runs tsc and ty checks in parallel) |
| `refresh_openapi` | Regenerate OpenAPI schema and API client |
| `search_registry_components` | Search shadcn registry components using semantic search |
| `list_registry_components` | List all available components in a specific registry |
| `add_component` | Add a component to the project |
| `routes` | List all API routes with parameters, schemas, and generated hook names |
| `docs` | Search Databricks SDK documentation for code examples and API references |
| `databricks_apps_logs` | Fetch logs from deployed Databricks app using Databricks CLI |
| `get_route_info` | Get code example for using a specific API route |
| `feedback_prepare` | Prepare a feedback issue for review. Returns formatted title, body, and browser URL |
| `feedback_submit` | Submit a prepared feedback issue as a public GitHub issue |

## Development Commands

CLI equivalents (use MCP tools above when available):

| Command | Description |
|---------|-------------|
| `apx dev start` | Start all dev servers (backend + frontend + OpenAPI watcher) |
| `apx dev stop` | Stop all dev servers |
| `apx dev status` | Check running server status and ports |
| `apx dev check` | Check for TypeScript/Python errors |
| `apx dev logs` | View recent logs (default: last 10m) |
| `apx dev logs -f` | Follow/stream logs live |
| `apx build` | Build for production |

## Deployment

The optimization runner job is declared in `databricks.yml` and managed by the Databricks bundle. Use the wrapper scripts instead of raw `databricks bundle` commands:

| Command | Description |
|---------|-------------|
| `./deploy.sh` | Build, deploy bundle, resolve SP, apply UC grants, configure job `run_as` |
| `./destroy.sh` | Clean up deployment jobs, then destroy the bundle |

Environment variables for deploy/destroy (all have sensible defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `GENIE_APP_NAME` | `genie-space-optimizer` | Databricks App name |
| `GENIE_CATALOG` | `main` | Unity Catalog name |
| `GENIE_SCHEMA` | `genie_optimization` | Schema for optimization tables |
| `GENIE_WAREHOUSE_ID` | (empty) | SQL Warehouse ID |
| `GENIE_DEPLOY_PROFILE` | `DEFAULT` | Databricks CLI profile |

Extra arguments are forwarded to `databricks bundle deploy/destroy`:
```bash
./deploy.sh --var warehouse_id=abc123def
```

The bundle passes `GENIE_SPACE_OPTIMIZER_JOB_ID` to the app automatically via the app's environment variables. The app reads this at startup to trigger optimization runs on the bundle-managed job.

## Backlog-Driven Development

This project uses an incremental, multi-session agent workflow:

| File | Purpose |
|------|---------|
| `docs/backlog.yml` | Feature specs with tasks, acceptance criteria, file pointers |
| `docs/agent-progress.json` | Machine-readable task status + session log |
| `.cursor/rules/backlog-agent.mdc` | Full session protocol (Orient → Pick → Execute → Verify → Close) |

**Before starting feature work**, read `docs/agent-progress.json` and `git log --oneline -15` to understand current state. Pick exactly one task, implement it, verify against acceptance criteria, commit with the task ID prefix (e.g. `[2a] ...`), and update the progress file. See `.cursor/rules/backlog-agent.mdc` for the full protocol.

## Detailed Patterns

For backend patterns (DI, CRUD routers, AppConfig, lifespan) and frontend patterns (Suspense, mutations, selector, components), see `.claude/skills/apx/`.
