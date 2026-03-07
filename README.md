# Genie Workbench

Genie Workbench combines GenieRx (LLM-powered analysis and optimization of Genie Spaces) and GenieIQ (org-wide scoring with Lakebase persistence) into a single Databricks App.

## Deployment

This app is deployed as a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/). The frontend (React/Vite) and backend (FastAPI) are built and served together — there is no separate local dev server.

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and authenticated (`databricks auth login`)
- A Databricks workspace with Apps enabled

### 1. Clone the repo

```bash
git clone <repo-url>
cd databricks-genie-workbench
```

### 2. Create the app

Create a new Databricks App via the workspace UI (**Compute > Apps > Create App**). Note the app name you choose (e.g. `genie-workbench`).

### 3. Sync local files to the workspace

```bash
databricks sync --watch . /Workspace/Users/<your-email>/genie-workbench
```

This uploads your project files to a workspace folder and watches for changes. Files listed in `.gitignore` and `.databricksignore` are excluded (e.g. `node_modules/`, `dist/`, `.env`).

### 4. Deploy the app

```bash
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<your-email>/genie-workbench
```

During deployment, Databricks Apps automatically:
1. Runs `npm install` (detects root `package.json`, which chains into `frontend/`)
2. Runs `pip install -r requirements.txt`
3. Runs `npm run build` (builds the React frontend to `frontend/dist/`)
4. Starts the app via the command in `app.yaml` (`uvicorn backend.main:app`)

### 5. Configure app resources

After deploying, grant the app's service principal access to required resources:

- **Workspace Directory** — Can Manage (for creating Genie Spaces)
- **Unity Catalog/Schema** — USE CATALOG, USE SCHEMA, SELECT
- **LLM Serving Endpoint** — Can Query
- **SQL Warehouse** — Can Use
- **Genie Space(s)** — Can Edit

See `app.yaml` for environment variable configuration (SQL warehouse, Lakebase, MLflow, etc.).

### Iterating on changes

After the initial deploy, use the sync + deploy cycle:
1. Edit code locally
2. `databricks sync --watch` picks up changes automatically
3. Re-run `databricks apps deploy` to trigger a new deployment

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
