#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# install.sh — Guided installer for Genie Workbench
#
# Interactive script that:
#   1. Checks prerequisites (databricks CLI, node, python)
#   2. Asks for Databricks profile
#   3. Asks for catalog (with auto-discovery)
#   4. Asks for SQL Warehouse (with auto-discovery)
#   5. Asks for LLM model
#   6. Lakebase info (attach manually via Apps UI after deploy)
#   7. Asks for app name
#   8. Writes .env.deploy
#   9. Runs deploy.sh
#  10. Resolves app service principal
#  11. Optionally grants SP access to Genie Spaces
#  12. Prints summary with automated/manual sections
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

_info()  { echo -e "${BLUE}ℹ${NC} $*"; }
_ok()    { echo -e "${GREEN}✓${NC} $*"; }
_warn()  { echo -e "${YELLOW}⚠${NC} $*"; }
_error() { echo -e "${RED}✗${NC} $*" >&2; }
_header() { echo -e "\n${BOLD}${CYAN}── $* ──${NC}\n"; }

_prompt() {
    local varname="$1"
    local prompt_text="$2"
    local default="${3:-}"
    local result

    if [ -n "$default" ]; then
        echo -en "  ${prompt_text} ${BOLD}[$default]${NC}: "
    else
        echo -en "  ${prompt_text}: "
    fi
    read -r result
    result="${result:-$default}"
    eval "$varname=\"$result\""
}

_prompt_yn() {
    local varname="$1"
    local prompt_text="$2"
    local default="${3:-Y}"
    local result

    echo -en "  ${prompt_text} [${default}]: "
    read -r result
    result="${result:-$default}"
    case "$result" in
        [Yy]*) eval "$varname=Y" ;;
        *)     eval "$varname=N" ;;
    esac
}

# ══════════════════════════════════════════════════════════════════════════
# Step 0: Banner
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║            Genie Workbench — Guided Installer                ║${NC}"
echo -e "${BOLD}║                                                              ║${NC}"
echo -e "${BOLD}║  Creates and configures:                                     ║${NC}"
echo -e "${BOLD}║    • Databricks App (with OAuth scopes)                      ║${NC}"
echo -e "${BOLD}║    • GSO Optimization Job (6-stage pipeline)                 ║${NC}"
echo -e "${BOLD}║    • Lakebase database (PostgreSQL)                          ║${NC}"
echo -e "${BOLD}║    • Unity Catalog permissions                               ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════════
# Step 1: Check prerequisites
# ══════════════════════════════════════════════════════════════════════════
_header "Step 1: Checking prerequisites"

MISSING=()

if command -v databricks &>/dev/null; then
    DB_VERSION=$(databricks --version 2>/dev/null || echo "unknown")
    _ok "databricks CLI ($DB_VERSION)"
else
    MISSING+=("databricks CLI — https://docs.databricks.com/dev-tools/cli/install.html")
fi

if command -v node &>/dev/null; then
    NODE_VERSION=$(node --version 2>/dev/null || echo "unknown")
    _ok "Node.js ($NODE_VERSION)"
else
    MISSING+=("Node.js — https://nodejs.org/")
fi

if command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 --version 2>/dev/null || echo "unknown")
    _ok "Python ($PY_VERSION)"
else
    MISSING+=("Python 3.11+ — https://python.org/")
fi

if command -v npm &>/dev/null; then
    _ok "npm ($(npm --version 2>/dev/null))"
else
    MISSING+=("npm — installed with Node.js")
fi

if [ ${#MISSING[@]} -gt 0 ]; then
    echo ""
    _error "Missing prerequisites:"
    for dep in "${MISSING[@]}"; do
        echo "    - $dep"
    done
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 2: Databricks profile
# ══════════════════════════════════════════════════════════════════════════
_header "Step 2: Databricks profile"

# Show available profiles
_info "Available profiles:"
databricks auth profiles 2>/dev/null | head -20 || echo "  (could not list profiles)"
echo ""

_prompt PROFILE "Databricks CLI profile" "DEFAULT"

# Validate the profile
if databricks current-user me --profile "$PROFILE" -o json &>/dev/null; then
    DEPLOYER=$(databricks current-user me --profile "$PROFILE" -o json \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
    _ok "Authenticated as $DEPLOYER"
else
    _error "Could not authenticate with profile '$PROFILE'."
    _info "Run: databricks configure --profile $PROFILE"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 3: Catalog
# ══════════════════════════════════════════════════════════════════════════
_header "Step 3: Unity Catalog"

_info "The optimizer stores state tables in a schema called 'genie_space_optimizer'"
_info "inside the catalog you choose below."
echo ""
_warn "You must have CREATE SCHEMA permission on this catalog."
_info "The deploy script will create the schema automatically if it doesn't exist."
echo ""

_info "Available catalogs:"
databricks catalogs list --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys,json
try:
    data = json.load(sys.stdin)
    cats = data if isinstance(data, list) else data.get('catalogs', [])
    for c in cats[:20]:
        name = c.get('name','') if isinstance(c, dict) else str(c)
        print(f'    {name}')
except: pass
" 2>/dev/null || echo "  (could not list catalogs)"
echo ""

_prompt CATALOG "Catalog name" ""

if [ -z "$CATALOG" ]; then
    _error "Catalog is required." >&2
    echo "" >&2
    echo "  The optimizer needs a Unity Catalog to store state tables," >&2
    echo "  benchmarks, and prompt artifacts." >&2
    echo "" >&2
    echo "  Example:" >&2
    echo "    export GENIE_CATALOG=my_catalog" >&2
    exit 1
fi

GSO_SCHEMA="genie_space_optimizer"
_ok "Will use schema: ${CATALOG}.${GSO_SCHEMA}"

# ══════════════════════════════════════════════════════════════════════════
# Step 4: SQL Warehouse
# ══════════════════════════════════════════════════════════════════════════
_header "Step 4: SQL Warehouse"

_info "Available SQL warehouses:"
databricks warehouses list --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys,json
try:
    data = json.load(sys.stdin)
    whs = data if isinstance(data, list) else data.get('warehouses', [])
    for w in whs[:15]:
        wid = w.get('id','')
        name = w.get('name','')
        state = w.get('state','')
        print(f'    {wid}  {name}  ({state})')
except: pass
" 2>/dev/null || echo "  (could not list warehouses)"
echo ""

_prompt WAREHOUSE_ID "SQL Warehouse ID" ""

if [ -z "$WAREHOUSE_ID" ]; then
    _error "Warehouse ID is required."
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 5: LLM Model
# ══════════════════════════════════════════════════════════════════════════
_header "Step 5: LLM Model"

_prompt LLM_MODEL "LLM serving endpoint" "databricks-claude-sonnet-4-6"

# ══════════════════════════════════════════════════════════════════════════
# Step 6: Lakebase
# ══════════════════════════════════════════════════════════════════════════
_header "Step 6: Lakebase (PostgreSQL)"

APP_NAME_DEFAULT="genie-workbench"

_info "Lakebase provides persistent storage for scan history and starred spaces."
_info "Without it, the app uses in-memory storage (data lost on restart)."
echo ""
_info "If you have a Lakebase instance, enter its name below."
_info "Leave blank to skip (in-memory fallback — data lost on restart)."
echo ""

_prompt LAKEBASE_INSTANCE "Lakebase instance name" "$APP_NAME_DEFAULT"

echo ""
_info "After deploy, attach a Lakebase resource in the Databricks Apps UI:"
_info "  Apps → $APP_NAME_DEFAULT → Resources → + Add → PostgreSQL (Lakebase)"
_info "  Name it 'postgres' with CAN_CONNECT_AND_CREATE permission."

# ══════════════════════════════════════════════════════════════════════════
# Step 7: App name
# ══════════════════════════════════════════════════════════════════════════
_header "Step 7: App name"

_prompt APP_NAME "Databricks App name" "$APP_NAME_DEFAULT"

# ══════════════════════════════════════════════════════════════════════════
# Step 8: Write .env.deploy
# ══════════════════════════════════════════════════════════════════════════
_header "Step 8: Writing configuration"

ENV_FILE="$PROJECT_DIR/.env.deploy"
cat > "$ENV_FILE" <<EOF
# Genie Workbench — Deployment Configuration
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

GENIE_WAREHOUSE_ID=$WAREHOUSE_ID
GENIE_CATALOG=$CATALOG
GENIE_APP_NAME=$APP_NAME
GENIE_DEPLOY_PROFILE=$PROFILE
GENIE_LLM_MODEL=$LLM_MODEL
GENIE_LAKEBASE_INSTANCE=$LAKEBASE_INSTANCE
EOF

_ok "Configuration written to .env.deploy"
echo ""
echo "  ┌─ Configuration Summary ───────────────────────────────────┐"
echo "  │  Profile:      $PROFILE"
echo "  │  App name:     $APP_NAME"
echo "  │  Catalog:      $CATALOG"
echo "  │  GSO Schema:   ${CATALOG}.${GSO_SCHEMA} (default)"
echo "  │  Warehouse ID: $WAREHOUSE_ID"
echo "  │  LLM Model:    $LLM_MODEL"
echo "  │  Lakebase:     ${LAKEBASE_INSTANCE:-<none>}"
echo "  └───────────────────────────────────────────────────────────┘"

# ══════════════════════════════════════════════════════════════════════════
# Step 9: Deploy
# ══════════════════════════════════════════════════════════════════════════
_header "Step 9: Deploying"

_prompt_yn DO_DEPLOY "Run deploy now?" "Y"

if [ "$DO_DEPLOY" = "Y" ]; then
    "$SCRIPT_DIR/deploy.sh"
else
    _info "Skipping deploy. Run ./scripts/deploy.sh when ready."
    exit 0
fi

# Track what was automated for the summary
AUTOMATED=()
AUTOMATED_FAIL=()

# ══════════════════════════════════════════════════════════════════════════
# Step 10: Resolve app service principal
# ══════════════════════════════════════════════════════════════════════════
_header "Step 10: Resolving app service principal"

SP_CLIENT_ID=$(
    databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id','') or d.get('service_principal_name',''))" \
    2>/dev/null || true
)

# Resolve human-readable SP name for the summary
SP_DISPLAY_NAME=""
if [ -n "$SP_CLIENT_ID" ]; then
    SP_DISPLAY_NAME=$(
        databricks service-principals list --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "
import sys, json
sp_id = '$SP_CLIENT_ID'
try:
    data = json.load(sys.stdin)
    sps = data if isinstance(data, list) else data.get('Resources', data.get('service_principals', []))
    for sp in sps:
        if sp.get('applicationId','') == sp_id or sp.get('application_id','') == sp_id:
            print(sp.get('displayName','') or sp.get('display_name',''))
            break
except: pass
" 2>/dev/null || true
    )
    _ok "SP: ${SP_DISPLAY_NAME:-$SP_CLIENT_ID} (${SP_CLIENT_ID})"
else
    _warn "Could not resolve app service principal. Skipping automated grants."
    _warn "You can grant permissions manually after the app is fully deployed."
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 11: Genie Space permissions (optional)
# ══════════════════════════════════════════════════════════════════════════
_header "Step 11: Genie Space access"

_info "The app uses On-Behalf-Of (OBO) auth, so users see their own spaces."
_info "However, the service principal needs explicit grants for fallback access."
echo ""

GENIE_SPACES_GRANTED=0

_prompt_yn GRANT_SPACES "Grant the app access to all Genie Spaces you can edit?" "Y"

if [ "$GRANT_SPACES" = "Y" ] && [ -n "$SP_CLIENT_ID" ]; then
    _info "Discovering your Genie Spaces..."

    # List Genie Spaces and grant SP access
    GENIE_SPACES_GRANTED=$(python3 -c "
import json, subprocess, sys

profile = '$PROFILE'
sp_id = '$SP_CLIENT_ID'

# List all Genie Spaces visible to the deploying user
try:
    result = subprocess.run(
        ['databricks', 'api', 'get', '/api/2.0/genie/spaces', '--profile', profile, '-o', 'json'],
        capture_output=True, text=True, check=True,
    )
    data = json.loads(result.stdout)
    spaces = data if isinstance(data, list) else data.get('spaces', data.get('genie_spaces', []))
except Exception as e:
    print(f'Could not list Genie Spaces: {e}', file=sys.stderr)
    spaces = []

if not spaces:
    print('0')
    sys.exit(0)

granted = 0
for space in spaces:
    space_id = space.get('id') or space.get('space_id', '')
    space_name = space.get('title') or space.get('name', space_id)
    if not space_id:
        continue

    try:
        perm_payload = json.dumps({
            'access_control_list': [
                {
                    'service_principal_name': sp_id,
                    'permission_level': 'CAN_EDIT',
                }
            ]
        })
        subprocess.run(
            ['databricks', 'api', 'put', f'/api/2.0/permissions/dashboards.genie/{space_id}',
             '--profile', profile, '--json', perm_payload],
            capture_output=True, text=True, check=True,
        )
        print(f'Granted CAN_EDIT on: {space_name} ({space_id})', file=sys.stderr)
        granted += 1
    except Exception as e:
        print(f'Could not grant on {space_name}: {e}', file=sys.stderr)

print(granted)
" 2>/dev/null || echo "0")

    if [ "$GENIE_SPACES_GRANTED" -gt 0 ] 2>/dev/null; then
        _ok "Granted access to $GENIE_SPACES_GRANTED Genie Space(s)."
        AUTOMATED+=("Genie Space SP access ($GENIE_SPACES_GRANTED spaces)")
    else
        _warn "No Genie Spaces were granted. You can grant them manually later."
    fi
elif [ -z "$SP_CLIENT_ID" ]; then
    _warn "Skipped — no SP resolved."
else
    _info "Skipping Genie Space grants. You can grant them manually later."
fi

# ══════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}${BOLD}  Installation complete!${NC}"
echo ""

# Try to get the app URL
APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('url',''))" 2>/dev/null || true)

echo "  App:       $APP_NAME"
echo "  Catalog:   $CATALOG"
echo "  Schema:    ${CATALOG}.${GSO_SCHEMA}"
echo "  SP:        ${SP_DISPLAY_NAME:-${SP_CLIENT_ID:-<unknown>}}"
echo ""
if [ -n "$APP_URL" ]; then
    echo -e "  ${BOLD}URL: ${CYAN}${APP_URL}${NC}"
else
    echo "  URL: https://${APP_NAME}-*.databricksapps.com (available shortly)"
fi

# ── Automated (done) ─────────────────────────────────────────────────────
echo ""
echo -e "  ${GREEN}${BOLD}Automated (done):${NC}"
echo -e "    ${GREEN}✓${NC} OAuth scopes (configured in app.yaml)"
echo -e "    ${GREEN}✓${NC} GSO optimization job (bundle-managed)"
echo -e "    ${GREEN}✓${NC} UC grants on ${CATALOG}.${GSO_SCHEMA}"

if [ ${#AUTOMATED[@]} -gt 0 ]; then
    for item in "${AUTOMATED[@]}"; do
        echo -e "    ${GREEN}✓${NC} $item"
    done
fi

if [ ${#AUTOMATED_FAIL[@]} -gt 0 ]; then
    echo ""
    echo -e "  ${YELLOW}${BOLD}Attempted but failed (grant manually):${NC}"
    for item in "${AUTOMATED_FAIL[@]}"; do
        echo -e "    ${YELLOW}⚠${NC} $item"
    done
fi

# ── Remaining manual steps ───────────────────────────────────────────────
SP_NAME_FOR_DISPLAY="${SP_DISPLAY_NAME:-${SP_CLIENT_ID:-<app-service-principal>}}"

echo ""
echo -e "  ${YELLOW}${BOLD}Remaining manual steps:${NC}"
echo ""
echo -e "    ${BOLD}1. Attach Lakebase (PostgreSQL) for persistent storage${NC}"
echo "       Without Lakebase, scan results and stars are lost on restart."
echo "       To enable persistent storage:"
echo ""
echo "       a) Open the Databricks Apps UI → ${APP_NAME} → Resources"
echo "       b) Click '+ Add resource' → PostgreSQL (Lakebase)"
echo "       c) Name it 'postgres' and select 'CAN_CONNECT_AND_CREATE'"
echo "       d) Save and redeploy the app (or just refresh the page —"
echo "          the app auto-retries schema creation every 30 seconds)"
echo ""
echo -e "    ${BOLD}2. Create GSO synced tables (for Auto-Optimize history)${NC}"
echo "       Synced tables replicate GSO Delta tables to Lakebase for"
echo "       fast reads in the app. They must be created via Catalog Explorer UI."
echo ""
echo "       For each of these 8 tables in ${CATALOG}.${GSO_SCHEMA}:"
echo "         genie_opt_runs, genie_opt_stages, genie_opt_iterations,"
echo "         genie_opt_patches, genie_eval_asi_results, genie_opt_provenance,"
echo "         genie_opt_suggestions, genie_opt_data_access_grants"
echo ""
echo "       a) Navigate to the source table in Catalog Explorer"
echo "       b) Click 'Create' → 'Synced table'"
echo "       c) Name: <table_name>_synced (same schema)"
echo "       d) Database type: Lakebase Serverless (Autoscaling)"
echo "       e) Project: ${APP_NAME}-db, Branch: production"
echo "       f) Sync mode: Triggered"
echo ""
echo "       Then verify:"
echo -e "       ${CYAN}python3 scripts/setup_synced_tables.py --source-catalog ${CATALOG} --warehouse-id \$WAREHOUSE_ID --profile \$PROFILE --verify-only${NC}"
echo ""
echo -e "    ${BOLD}3. Genie Space data access${NC}"
echo "       The SP needs SELECT on schemas your Genie Spaces reference."
echo "       Open the app → Auto-Optimize → Settings to see which schemas"
echo "       need grants, then run:"
echo -e "       ${CYAN}GRANT SELECT ON SCHEMA <catalog>.<schema> TO \`${SP_NAME_FOR_DISPLAY}\`${NC}"
echo ""
echo -e "    ${BOLD}4. Future Genie Spaces${NC}"
echo "       Spaces created after install need SP grants. Open the space"
echo "       sharing dialog and add '${SP_NAME_FOR_DISPLAY}' with CAN_MANAGE."
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
