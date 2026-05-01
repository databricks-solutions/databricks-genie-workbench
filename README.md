# Genie Workbench

Genie Workbench is a Databricks App for creating, scoring, and optimizing Databricks Genie Spaces. It combines a FastAPI backend, React/Vite frontend, Databricks On-Behalf-Of auth, Lakebase persistence, and the Genie Space Optimizer (GSO) benchmark pipeline.

Use it to:

- Create Genie Spaces from business requirements and Unity Catalog data sources
- Score Genie Space quality with an instant rule-based IQ scan
- Apply quick fixes to existing spaces
- Run benchmark-driven optimization through the Auto-Optimize pipeline
- Track scan history, starred spaces, sessions, and optimization state

## Quick Start

The recommended install path is the Databricks notebook installer.

### Databricks Notebook Installer

1. Clone this repo into a Databricks Git folder.
2. Open `notebooks/install.py`.
3. Set the notebook widgets:
   - `app_name`
   - `catalog`
   - `warehouse_id`
   - `llm_model`
   - `mlflow_experiment_id`
   - `lakebase_mode`
   - `lakebase_instance`
   - `grant_genie_spaces`
4. Run the notebook from the top.

The notebook uses notebook-native `WorkspaceClient()` auth, creates a generated deployment source folder under `/Workspace/Users/<you>/.genie-workbench-deploy/<app-name>/app`, patches `app.yaml` there, provisions UC/Lakebase/GSO resources, and deploys the Databricks App from that generated source. The Git folder remains unchanged.

For widget details, prerequisites, Lakebase behavior, updates, and troubleshooting, see [docs/08-deployment-guide.md](docs/08-deployment-guide.md).

### Local Terminal Installer

The local terminal path is still supported for users who want CLI-based deployment:

```bash
git clone <repo-url>
cd databricks-genie-workbench
databricks auth login --profile <workspace-profile>
./scripts/install.sh
```

For subsequent local terminal updates:

```bash
./scripts/deploy.sh --update
```

Do not run `databricks bundle init`; this project already has its bundle configuration.

## Prerequisites

Notebook installer:

- Databricks workspace with Apps enabled
- SQL Warehouse
- Unity Catalog with permission to create the GSO schema
- Repo cloned into a Databricks Git folder
- Databricks compute that can run `%pip install`

Local terminal installer additionally requires:

- Databricks CLI v0.297.2+
- Python 3.11+
- uv
- Node.js and npm

Auto-Optimize requires MLflow Prompt Registry to be enabled in the workspace. Lakebase is optional, but without it scan history, starred spaces, and agent sessions are stored in memory only.

## Documentation

Start with [docs/00-index.md](docs/00-index.md) for the full documentation map.

Common references:

- [Deployment Guide](docs/08-deployment-guide.md): notebook and local installer flows, Lakebase setup, updates, teardown
- [Architecture Overview](docs/02-architecture-overview.md): backend, frontend, persistence, deployment design
- [Authentication & Permissions](docs/03-authentication-and-permissions.md): OBO vs service principal behavior and required grants
- [Auto-Optimize](docs/07-auto-optimize.md): GSO optimization pipeline
- [Operations Guide](docs/09-operations-guide.md): Lakebase, MLflow, app logs, GSO job operations
- [Troubleshooting](docs/appendices/B-troubleshooting.md): common install and runtime failures
- [Environment Variables](docs/appendices/C-environment-variables.md): `app.yaml`, `.env.deploy`, and notebook widget variable flow

## Development Notes

This app runs only on Databricks Apps. Do not run a local `uvicorn` server for app testing; OBO auth, Lakebase, app resources, and serving endpoints are Databricks-managed runtime dependencies.

Dependency lock files are the source of truth and should be committed when changed:

- `uv.lock`
- `packages/genie-space-optimizer/uv.lock`
- `frontend/package-lock.json`
- `packages/genie-space-optimizer/package-lock.json`

Do not edit `requirements.txt` manually. It is generated from `uv.lock` and excluded from Databricks App deployment so the platform uses `uv sync` from `pyproject.toml` and `uv.lock`.

## Help

Databricks support does not cover this project. For questions or bugs, open a GitHub issue and the team will help on a best-effort basis.

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this repository is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library | description | license | source |
|---|---|---|---|
| asyncpg | Fast PostgreSQL client for asyncio | Apache-2.0 | https://pypi.org/project/asyncpg/ |
| class-variance-authority | CSS class name composition utility | Apache-2.0 | https://github.com/joe-bell/cva |
| clsx | Utility for constructing className strings | MIT | https://github.com/lukeed/clsx |
| databricks-sdk | Databricks SDK for Python | Apache-2.0 | https://pypi.org/project/databricks-sdk/ |
| fastapi | Modern async web framework for APIs | MIT | https://pypi.org/project/fastapi/ |
| httpx | Async/sync HTTP client | BSD-3-Clause | https://pypi.org/project/httpx/ |
| lucide-react | Icon library for React | ISC | https://github.com/lucide-icons/lucide |
| mlflow | ML experiment tracking and model registry | Apache-2.0 | https://pypi.org/project/mlflow/ |
| pandas | Data manipulation and analysis | BSD-3-Clause | https://pypi.org/project/pandas/ |
| prism-react-renderer | Syntax highlighting with Prism for React | MIT | https://github.com/FormidableLabs/prism-react-renderer |
| psycopg | PostgreSQL database adapter (v3) | LGPL-3.0 | https://pypi.org/project/psycopg/ |
| pydantic | Data validation using Python type hints | MIT | https://pypi.org/project/pydantic/ |
| pydantic-settings | Settings management with Pydantic | MIT | https://pypi.org/project/pydantic-settings/ |
| python-dotenv | Load environment variables from .env files | BSD-3-Clause | https://pypi.org/project/python-dotenv/ |
| pyyaml | YAML parser and emitter | MIT | https://pypi.org/project/PyYAML/ |
| react | Library for building user interfaces | MIT | https://github.com/facebook/react |
| react-diff-viewer-continued | Text diff viewer component for React | MIT | https://github.com/aeolun/react-diff-viewer-continued |
| react-dom | React DOM rendering | MIT | https://github.com/facebook/react |
| react-markdown | Render Markdown as React components | MIT | https://github.com/remarkjs/react-markdown |
| recharts | Charting library for React | MIT | https://github.com/recharts/recharts |
| remark-gfm | GitHub Flavored Markdown support for remark | MIT | https://github.com/remarkjs/remark-gfm |
| requests | HTTP library for Python | Apache-2.0 | https://pypi.org/project/requests/ |
| sql-formatter | SQL query formatter | MIT | https://github.com/sql-formatter-org/sql-formatter |
| sqlglot | SQL parser, transpiler, and optimizer | MIT | https://pypi.org/project/sqlglot/ |
| sqlmodel | SQL databases with Python and Pydantic | MIT | https://pypi.org/project/sqlmodel/ |
| tailwind-merge | Merge Tailwind CSS classes without conflicts | MIT | https://github.com/dcastil/tailwind-merge |
| uvicorn | ASGI web server | BSD-3-Clause | https://pypi.org/project/uvicorn/ |
