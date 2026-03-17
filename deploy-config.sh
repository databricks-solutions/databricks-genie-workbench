#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy-config.sh — shared configuration for deploy.sh and destroy.sh
#
# Sourced (not executed) by the deploy/destroy scripts.  Reads deployment
# settings from environment variables with sensible defaults, validates
# required values, and builds the --var flags that databricks bundle needs.
#
# Environment variables (set these before running deploy.sh / destroy.sh):
#
#   GENIE_WAREHOUSE_ID   (required)  SQL Warehouse ID for query execution
#   GENIE_CATALOG        (optional)  Unity Catalog name         [default: main]
#   GENIE_SCHEMA         (optional)  Schema for state tables    [default: genie_optimization]
#   GENIE_APP_NAME       (optional)  Databricks App name        [default: genie-space-optimizer]
#   GENIE_DEPLOY_PROFILE (optional)  Databricks CLI profile     [default: DEFAULT]
#
# After sourcing, the following variables are available:
#   APP_NAME, CATALOG, SCHEMA, WAREHOUSE_ID, PROFILE
#   BUNDLE_VAR_FLAGS  — array of --var flags to pass to databricks bundle
# ---------------------------------------------------------------------------

# ── Load .env.deploy if present ──────────────────────────────────────────
_CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_DEPLOY_ENV="$_CONFIG_DIR/.env.deploy"
if [ -f "$_DEPLOY_ENV" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$_DEPLOY_ENV"
    set +a
fi

# ── Resolve config from env vars ─────────────────────────────────────────
APP_NAME="${GENIE_APP_NAME:-genie-space-optimizer}"
CATALOG="${GENIE_CATALOG:-main}"
SCHEMA="${GENIE_SCHEMA:-genie_optimization}"
WAREHOUSE_ID="${GENIE_WAREHOUSE_ID:-}"
PROFILE="${GENIE_DEPLOY_PROFILE:-DEFAULT}"

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

# ── Build --var flags for databricks bundle ──────────────────────────────
BUNDLE_VAR_FLAGS=(
    --var "warehouse_id=$WAREHOUSE_ID"
    --var "catalog=$CATALOG"
)
if [ "$APP_NAME" != "genie-space-optimizer" ]; then
    BUNDLE_VAR_FLAGS+=(--var "app_name=$APP_NAME")
fi
if [ "$SCHEMA" != "genie_optimization" ]; then
    BUNDLE_VAR_FLAGS+=(--var "gold_schema=$SCHEMA")
fi

# ── Print config summary ─────────────────────────────────────────────────
_print_config() {
    echo "  ┌─ Configuration ─────────────────────────────────────────┐"
    echo "  │  Profile:      $PROFILE"
    echo "  │  App name:     $APP_NAME"
    echo "  │  Catalog:      $CATALOG"
    echo "  │  Schema:       $SCHEMA"
    echo "  │  Warehouse ID: $WAREHOUSE_ID"
    echo "  └─────────────────────────────────────────────────────────┘"
}
