# Appendix C: Environment Variables

## App Environment Variables (`app.yaml`)

These variables are defined in `app.yaml` and injected into the app runtime. Placeholder values (e.g., `__GSO_CATALOG__`) are patched by `deploy.sh` before deployment.

### MLflow Tracing

| Variable | Value | Description |
|----------|-------|-------------|
| `MLFLOW_TRACKING_URI` | `databricks` | MLflow tracking server (Databricks workspace) |
| `MLFLOW_REGISTRY_URI` | `databricks-uc` | MLflow model registry (Unity Catalog) |
| `MLFLOW_EXPERIMENT_ID` | `__MLFLOW_EXPERIMENT_ID__` | Experiment for tracing LLM calls. Workspace-specific; validated at startup, cleared if invalid |

### LLM Model

| Variable | Value | Description |
|----------|-------|-------------|
| `LLM_MODEL` | `__LLM_MODEL__` | Databricks model serving endpoint for analysis, fix agent, create agent. Default: `databricks-claude-sonnet-4-6` |

### SQL Warehouse

| Variable | Source | Description |
|----------|--------|-------------|
| `SQL_WAREHOUSE_ID` | `valueFrom: sql-warehouse` | SQL Warehouse ID, pulled from the app resource named `sql-warehouse` |

### Genie Space Configuration

| Variable | Value | Description |
|----------|-------|-------------|
| `GENIE_TARGET_DIRECTORY` | `/Shared/` | Where new Genie Spaces are created. Override to a specific folder if needed |

### Local Development

| Variable | Value | Description |
|----------|-------|-------------|
| `DEV_USER_EMAIL` | (empty) | User email for local dev auth. Only used when running outside Databricks Apps |

### Lakebase PostgreSQL

| Variable | Source | Description |
|----------|--------|-------------|
| `LAKEBASE_HOST` | `valueFrom: postgres` | Hostname, injected from the `postgres` app resource |
| `LAKEBASE_PORT` | `5432` | PostgreSQL port |
| `LAKEBASE_DATABASE` | `databricks_postgres` | Database name (standard Lakebase default) |
| `LAKEBASE_INSTANCE_NAME` | `__LAKEBASE_INSTANCE__` | Lakebase Autoscaling project name (patched by deploy script) |

### Auto-Optimize (GSO Engine)

| Variable | Source | Description |
|----------|--------|-------------|
| `GSO_CATALOG` | `__GSO_CATALOG__` | Unity Catalog for optimizer state tables. Patched from `.env.deploy` |
| `GSO_SCHEMA` | `genie_space_optimizer` | Schema within the catalog for GSO tables (fixed name) |
| `GSO_JOB_ID` | `__GSO_JOB_ID__` | Databricks Job ID for the optimization DAG. Patched from bundle deploy state |
| `GSO_WAREHOUSE_ID` | `valueFrom: sql-warehouse` | SQL Warehouse for GSO queries |

## Deploy Configuration Variables (`.env.deploy`)

These variables are used by `deploy.sh` and `install.sh` at deploy time. They are **not** injected into the app runtime directly — instead, deploy scripts use them to patch `app.yaml` placeholders and configure resources.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENIE_WAREHOUSE_ID` | Yes | — | SQL Warehouse ID (hex string from warehouse URL or detail page) |
| `GENIE_CATALOG` | Yes | — | Unity Catalog name (you need CREATE SCHEMA permission) |
| `GENIE_APP_NAME` | No | `genie-workbench` | Databricks App name (must be unique in your workspace) |
| `GENIE_DEPLOY_PROFILE` | No | `DEFAULT` | Databricks CLI profile name |
| `GENIE_LLM_MODEL` | No | `databricks-claude-sonnet-4-6` | LLM serving endpoint for analysis |
| `GENIE_LAKEBASE_INSTANCE` | No | empty | Lakebase Autoscaling project to use or create; keep stable for the same app, use a fresh project for a new app instance |

## How Variables Flow

```
.env.deploy                    app.yaml (template)              app.yaml (deployed)
┌──────────────────┐          ┌───────────────────┐           ┌───────────────────┐
│ GENIE_CATALOG=foo│─────────▶│ GSO_CATALOG:      │──────────▶│ GSO_CATALOG: foo  │
│ GENIE_WAREHOUSE  │          │   __GSO_CATALOG__  │           │                   │
│ GENIE_LLM_MODEL │          │ LLM_MODEL:        │           │ LLM_MODEL:        │
│ ...              │          │   __LLM_MODEL__    │           │   claude-sonnet   │
└──────────────────┘          └───────────────────┘           └───────────────────┘
                                                                       │
                                 deploy.sh patches                     │
                                 placeholders with                     ▼
                                 real values                    App Runtime
                                                               (env vars available)
```

1. `install.sh` collects values and writes `.env.deploy`
2. `deploy.sh` reads `.env.deploy` and patches `__PLACEHOLDER__` strings in `app.yaml`
3. `databricks apps deploy` uploads the patched `app.yaml`
4. The Databricks Apps platform injects env vars into the running container
5. `valueFrom` variables (e.g., `LAKEBASE_HOST`, `SQL_WAREHOUSE_ID`) are resolved from app resources at runtime

## Related Documentation

- [Deployment Guide](../08-deployment-guide.md) — deploy workflow and configuration
- [Operations Guide](../09-operations-guide.md) — MLflow and Lakebase management
