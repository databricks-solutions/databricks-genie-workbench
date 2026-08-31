#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy-config.sh — shared configuration for deploy.sh
#
# Sourced (not executed) by deploy scripts. Reads deployment settings from
# environment variables with sensible defaults and validates required values.
#
# Environment variables (set these before running deploy.sh):
#
#   GENIE_WAREHOUSE_ID       (required)  SQL Warehouse ID for query execution
#   GENIE_CATALOG            (required)  Unity Catalog name (must have CREATE SCHEMA permission)
#   GENIE_APP_NAME           (optional)  Databricks App name          [default: genie-workbench]
#   GENIE_DEPLOY_PROFILE     (optional)  Databricks CLI profile       [default: DEFAULT]
#   GENIE_LLM_MODEL          (optional)  Default LLM serving endpoint [default: databricks-claude-sonnet-4-6]
#   GENIE_LAKEBASE_INSTANCE  (optional)  Lakebase instance name       [default: none]
#   GENIE_MLFLOW_EXPERIMENT_ID (optional) MLflow experiment ID for agent tracing [default: disabled]
#
# After sourcing, the following variables are available:
#   APP_NAME, CATALOG, GSO_SCHEMA, WAREHOUSE_ID, PROFILE, LLM_MODEL, MLFLOW_EXPERIMENT_ID
# ---------------------------------------------------------------------------

# ── Load .env.deploy if present (in project root) ─────────────────────────
_PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
_DEPLOY_ENV="$_PROJECT_DIR/.env.deploy"
if [ -f "$_DEPLOY_ENV" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$_DEPLOY_ENV"
    set +a
fi

# ── Resolve config from env vars ─────────────────────────────────────────
APP_NAME="${GENIE_APP_NAME:-genie-workbench}"
CATALOG="${GENIE_CATALOG:-}"
GSO_SCHEMA="genie_space_optimizer"  # Fixed default — matches GSO convention
WAREHOUSE_ID="${GENIE_WAREHOUSE_ID:-}"
PROFILE="${GENIE_DEPLOY_PROFILE:-DEFAULT}"
LLM_MODEL="${GENIE_LLM_MODEL:-databricks-claude-sonnet-4-6}"
LAKEBASE_INSTANCE="${GENIE_LAKEBASE_INSTANCE:-}"
MLFLOW_EXPERIMENT_ID="${GENIE_MLFLOW_EXPERIMENT_ID:-}"

# ── Ontology materialize job run_as (MV-D50) ─────────────────────────────
# The identity the ontology materialize job reads system tables as, so NO app
# service-principal system-table grant is required. This is the bundle's
# `ontology_job_run_as` complex variable — which CANNOT be set via --var or
# BUNDLE_VAR_ (the CLI expects a string for those), so deploy.sh writes it to
# the sanctioned `.databricks/bundle/app/variable-overrides.json` file instead.
# Prefer a metastore-admin user; fall back to a granted SP. Empty ⇒ no override
# (the job runs as the deploy identity — the backward-compatible default).
ONTOLOGY_JOB_RUN_AS_USER="${GENIE_ONTOLOGY_JOB_RUN_AS_USER:-}"
ONTOLOGY_JOB_RUN_AS_SP="${GENIE_ONTOLOGY_JOB_RUN_AS_SP:-}"
ONTOLOGY_JOB_RUN_AS_JSON=""
if [ -n "$ONTOLOGY_JOB_RUN_AS_USER" ]; then
    ONTOLOGY_JOB_RUN_AS_JSON="{\"user_name\": \"${ONTOLOGY_JOB_RUN_AS_USER}\"}"
elif [ -n "$ONTOLOGY_JOB_RUN_AS_SP" ]; then
    ONTOLOGY_JOB_RUN_AS_JSON="{\"service_principal_name\": \"${ONTOLOGY_JOB_RUN_AS_SP}\"}"
fi

# ── Validate required values ─────────────────────────────────────────────
if [ -z "$WAREHOUSE_ID" ]; then
    echo "ERROR: GENIE_WAREHOUSE_ID is required but not set." >&2
    echo "" >&2
    echo "Set it as an environment variable:" >&2
    echo "  export GENIE_WAREHOUSE_ID=<your-sql-warehouse-id>" >&2
    echo "" >&2
    echo "Or create a .env.deploy file in the project root:" >&2
    echo "  echo 'GENIE_WAREHOUSE_ID=<your-sql-warehouse-id>' >> .env.deploy" >&2
    exit 1
fi

if [ -z "$CATALOG" ]; then
    echo "ERROR: GENIE_CATALOG is required but not set." >&2
    echo "" >&2
    echo "Set it as an environment variable:" >&2
    echo "  export GENIE_CATALOG=<your-catalog>" >&2
    exit 1
fi

# ── Print config summary ─────────────────────────────────────────────────
_print_config() {
    echo "  ┌─ Configuration ─────────────────────────────────────────┐"
    echo "  │  Profile:      $PROFILE"
    echo "  │  App name:     $APP_NAME"
    echo "  │  Catalog:      $CATALOG"
    echo "  │  GSO Schema:   ${CATALOG}.${GSO_SCHEMA}"
    echo "  │  Warehouse ID: $WAREHOUSE_ID"
    echo "  │  Default LLM:  $LLM_MODEL"
    echo "  │  Lakebase:     $LAKEBASE_INSTANCE"
    echo "  │  MLflow:       ${MLFLOW_EXPERIMENT_ID:-<disabled>}"
    echo "  │  Ont run_as:   ${ONTOLOGY_JOB_RUN_AS_USER:-${ONTOLOGY_JOB_RUN_AS_SP:-<deploy identity>}}"
    echo "  └─────────────────────────────────────────────────────────┘"
}
