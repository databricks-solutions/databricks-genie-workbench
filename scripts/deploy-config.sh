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
#                                           Set to empty string to use current-user
#                                           CLI auth (Databricks Web Terminal)
#   GENIE_LLM_MODEL          (optional)  LLM serving endpoint         [default: databricks-claude-sonnet-4-6]
#   GENIE_LAKEBASE_INSTANCE  (optional)  Lakebase instance name       [default: none]
#   GENIE_MLFLOW_EXPERIMENT_ID (optional) MLflow experiment ID for agent tracing [default: disabled]
#   GENIE_GRANT_SPACES       (optional)  Grant app SP CAN_EDIT on user's Genie Spaces (Y/N) [default: Y]
#   GENIE_UV_PROJECT_ENVIRONMENT (optional) Python venv path for uv
#
# After sourcing, the following variables are available:
#   APP_NAME, CATALOG, GSO_SCHEMA, WAREHOUSE_ID, PROFILE, LLM_MODEL,
#   LAKEBASE_INSTANCE, MLFLOW_EXPERIMENT_ID, GRANT_SPACES
# ---------------------------------------------------------------------------

# ── Load .env.deploy if present (in project root) ─────────────────────────
_PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
_DEPLOY_ENV="${GENIE_DEPLOY_ENV_FILE:-$_PROJECT_DIR/.env.deploy}"
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
if [ "${GENIE_DEPLOY_PROFILE+x}" = "x" ]; then
    PROFILE="$GENIE_DEPLOY_PROFILE"
else
    PROFILE="DEFAULT"
fi
LLM_MODEL="${GENIE_LLM_MODEL:-databricks-claude-sonnet-4-6}"
LAKEBASE_INSTANCE="${GENIE_LAKEBASE_INSTANCE:-}"
MLFLOW_EXPERIMENT_ID="${GENIE_MLFLOW_EXPERIMENT_ID:-}"
GRANT_SPACES="${GENIE_GRANT_SPACES:-Y}"

# ── Databricks CLI auth mode ────────────────────────────────────────────
# Local installs use a named profile by default. Databricks Web Terminal uses
# environment-provided current-user auth, where profile commands are not
# supported, so GENIE_DEPLOY_PROFILE="" intentionally omits --profile.
if [ -n "$PROFILE" ]; then
    PROFILE_LABEL="$PROFILE"
    DBX_PROFILE_ARGS=(--profile "$PROFILE")
else
    PROFILE_LABEL="current-user auth (no profile)"
    DBX_PROFILE_ARGS=()
fi

_dbx() {
    databricks "$@" "${DBX_PROFILE_ARGS[@]}"
}

# ── Python venv location ────────────────────────────────────────────────
# Databricks /Workspace is not a normal POSIX filesystem and can fail while
# uv expands wheels into .venv. Keep Web Terminal virtualenvs on the local
# home filesystem while leaving local CLI installs on the default .venv.
if [ -n "${GENIE_UV_PROJECT_ENVIRONMENT:-}" ]; then
    export UV_PROJECT_ENVIRONMENT="$GENIE_UV_PROJECT_ENVIRONMENT"
elif [ -z "${UV_PROJECT_ENVIRONMENT:-}" ] && [[ "$_PROJECT_DIR" == /Workspace/* ]]; then
    _UV_ENV_NAME="${APP_NAME//[^A-Za-z0-9_.-]/-}"
    export UV_PROJECT_ENVIRONMENT="${HOME:-/tmp}/.venvs/$_UV_ENV_NAME"
fi
if [ -n "${UV_PROJECT_ENVIRONMENT:-}" ]; then
    mkdir -p "$(dirname "$UV_PROJECT_ENVIRONMENT")"
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
    echo "  │  Profile:      $PROFILE_LABEL"
    echo "  │  App name:     $APP_NAME"
    echo "  │  Catalog:      $CATALOG"
    echo "  │  GSO Schema:   ${CATALOG}.${GSO_SCHEMA}"
    echo "  │  Warehouse ID: $WAREHOUSE_ID"
    echo "  │  LLM Model:    $LLM_MODEL"
    echo "  │  Lakebase:     $LAKEBASE_INSTANCE"
    echo "  │  MLflow:       ${MLFLOW_EXPERIMENT_ID:-<disabled>}"
    if [ -n "${UV_PROJECT_ENVIRONMENT:-}" ]; then
        echo "  │  Python venv:  $UV_PROJECT_ENVIRONMENT"
    fi
    echo "  └─────────────────────────────────────────────────────────┘"
}
