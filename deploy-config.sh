#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy-config.sh — shared configuration for deploy.sh
#
# Sourced (not executed) by deploy scripts. Reads deployment settings from
# environment variables with sensible defaults, validates required values,
# and builds the --var flags that databricks bundle needs.
#
# Environment variables (set these before running deploy.sh):
#
#   GENIE_WAREHOUSE_ID       (required)  SQL Warehouse ID for query execution
#   GENIE_CATALOG            (required)  Unity Catalog name (must have CREATE SCHEMA permission)
#   GENIE_APP_NAME           (optional)  Databricks App name          [default: genie-workbench]
#   GENIE_DEPLOY_PROFILE     (optional)  Databricks CLI profile       [default: DEFAULT]
#   GENIE_LLM_MODEL          (optional)  LLM serving endpoint         [default: databricks-claude-sonnet-4-6]
#   GENIE_DEPLOY_TARGET      (optional)  Bundle target                [auto-detected: dev or dev-lakebase]
#
# After sourcing, the following variables are available:
#   APP_NAME, CATALOG, GSO_SCHEMA, WAREHOUSE_ID, PROFILE, LLM_MODEL,
#   BUNDLE_VAR_FLAGS
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
APP_NAME="${GENIE_APP_NAME:-genie-workbench}"
CATALOG="${GENIE_CATALOG:-}"
GSO_SCHEMA="genie_space_optimizer"  # Fixed default — matches GSO convention
WAREHOUSE_ID="${GENIE_WAREHOUSE_ID:-}"
PROFILE="${GENIE_DEPLOY_PROFILE:-DEFAULT}"
LLM_MODEL="${GENIE_LLM_MODEL:-databricks-claude-sonnet-4-6}"

# Auto-detect deploy target: use dev-lakebase when the databricks.yml defines
# a postgres_projects resource under that target (Lakebase persistence).
# Explicit GENIE_DEPLOY_TARGET always wins.
if [ -n "${GENIE_DEPLOY_TARGET:-}" ]; then
    DEPLOY_TARGET="$GENIE_DEPLOY_TARGET"
elif grep -q 'postgres_projects:' "$_CONFIG_DIR/databricks.yml" 2>/dev/null; then
    DEPLOY_TARGET="dev-lakebase"
else
    DEPLOY_TARGET="dev"
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

# ── Build bundle flags ───────────────────────────────────────────────────
BUNDLE_TARGET_FLAGS=(--target "$DEPLOY_TARGET")

# ── Build --var flags for databricks bundle ──────────────────────────────
BUNDLE_VAR_FLAGS=(
    --var "warehouse_id=$WAREHOUSE_ID"
    --var "catalog=$CATALOG"
)
if [ "$APP_NAME" != "genie-workbench" ]; then
    BUNDLE_VAR_FLAGS+=(--var "app_name=$APP_NAME")
fi
if [ "$LLM_MODEL" != "databricks-claude-sonnet-4-6" ]; then
    BUNDLE_VAR_FLAGS+=(--var "llm_model=$LLM_MODEL")
fi

# ── Print config summary ─────────────────────────────────────────────────
_print_config() {
    echo "  ┌─ Configuration ─────────────────────────────────────────┐"
    echo "  │  Profile:      $PROFILE"
    echo "  │  App name:     $APP_NAME"
    echo "  │  Catalog:      $CATALOG"
    echo "  │  GSO Schema:   ${CATALOG}.${GSO_SCHEMA}"
    echo "  │  Warehouse ID: $WAREHOUSE_ID"
    echo "  │  LLM Model:    $LLM_MODEL"
    echo "  │  Target:       $DEPLOY_TARGET"
    echo "  └─────────────────────────────────────────────────────────┘"
}
