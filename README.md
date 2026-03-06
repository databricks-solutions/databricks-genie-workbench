# REPO NAME 

```
Placeholder

Fill here a description at a functional level - what is this content doing
```

## Video Overview

Include a GIF overview of what your project does. Use a service like Quicktime, Zoom or Loom to create the video, then convert to a GIF.


## Installation

### Local Development

**1. Configure environment**

```bash
cp .env.example .env.local
# Edit .env.local — minimum required fields:
# DATABRICKS_HOST, DATABRICKS_TOKEN
```

The only required fields to get started are `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Everything else (Lakebase, MLflow, SQL warehouse) is optional — the app falls back to in-memory storage without Lakebase.

**2. Install Python deps**

```bash
uv sync
# or: pip install -e .
```

**3. Install frontend deps and build**

```bash
cd frontend
npm install
npm run build
```

**4. Run (two terminals)**

```bash
# Terminal 1 — backend (from repo root)
uv run start-server

# Terminal 2 — frontend
cd frontend
npm run dev
```

The frontend runs at `http://localhost:5173` and proxies API requests to the backend at `http://localhost:8000`.

**Notes:**
- No Lakebase needed locally — scan results are stored in-memory and reset on server restart
- `SQL_WAREHOUSE_ID` is only required if using the Optimize tab's benchmark runner
- Leave `MLFLOW_EXPERIMENT_ID` empty to skip MLflow tracing
- Alternatively, authenticate via `databricks auth login` (OAuth) and set `DATABRICKS_CONFIG_PROFILE=DEFAULT` instead of using a PAT

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
