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
#   6. MLflow tracing (optional — experiment ID for agent observability)
#   7. Lakebase info (attach manually via Apps UI after deploy)
#   8. Asks for app name
#   9. Writes .env.deploy
#  10. Runs deploy.sh
#  11. Resolves app service principal
#  12. Optionally grants SP access to Genie Spaces
#  13. Prints summary with automated/manual sections
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

    local yn_hint="[Y/N]"
    echo -en "  ${prompt_text} ${yn_hint}: "
    read -r result
    result="${result:-$default}"
    case "$result" in
        [Yy]*) eval "$varname=Y" ;;
        *)     eval "$varname=N" ;;
    esac
}

# Usage: _select_from VARNAME "Prompt text" [default_idx] item1 item2 item3 ...
# Optional default_idx (a plain integer) sets a default choice — Enter accepts it.
# Sets VARNAME to the selected item. Caller must handle empty result.
_select_from() {
    local varname="$1"
    local prompt_text="$2"
    shift 2

    # If first remaining arg is a plain integer, treat it as the default index (1-based)
    local default_idx=0
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        default_idx="$1"
        shift
    fi

    local items=("$@")

    if [ ${#items[@]} -eq 0 ]; then
        printf -v "$varname" '%s' ""
        return
    fi

    local i
    for i in "${!items[@]}"; do
        if [ "$((i+1))" -eq "$default_idx" ]; then
            echo "    $((i+1))) ${items[$i]}  (default)"
        else
            echo "    $((i+1))) ${items[$i]}"
        fi
    done
    echo ""

    local choice result
    local range_hint="[1-${#items[@]}]"
    [ "$default_idx" -gt 0 ] && range_hint="[1-${#items[@]}, Enter for $default_idx]"

    while true; do
        echo -en "  ${prompt_text} ${range_hint}: "
        read -r choice
        if [ -z "$choice" ] && [ "$default_idx" -gt 0 ]; then
            result="${items[$((default_idx-1))]}"
            break
        fi
        if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#items[@]}" ]; then
            result="${items[$((choice-1))]}"
            break
        fi
        echo "  Please enter a number between 1 and ${#items[@]}."
    done
    printf -v "$varname" '%s' "$result"
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

if command -v uv &>/dev/null; then
    _ok "uv ($(uv --version 2>/dev/null))"
else
    MISSING+=("uv — https://docs.astral.sh/uv/  (curl -LsSf https://astral.sh/uv/install.sh | sh)")
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

_info "Discovering configured Databricks profiles..."
# Profile names are ini section headers and cannot contain spaces — $1 is safe
PROFILE_LINES=$(databricks auth profiles 2>/dev/null | tail -n +2 | awk '{print $1}' | grep -v '^$')
PROFILES_EXIT=$?

PROFILE_NAMES=()
while IFS= read -r line; do
    [ -n "$line" ] && PROFILE_NAMES+=("$line")
done <<< "$PROFILE_LINES"

if [ ${#PROFILE_NAMES[@]} -eq 0 ]; then
    if [ "$PROFILES_EXIT" -ne 0 ]; then
        _warn "Could not list profiles (databricks CLI error). Falling back to DEFAULT."
    else
        _warn "No profiles configured. Falling back to DEFAULT."
    fi
    PROFILE_NAMES=("DEFAULT")
fi

_info "Available profiles:"
_select_from PROFILE "Select a profile" "${PROFILE_NAMES[@]}"
echo ""

# Validate the profile
if databricks current-user me --profile "$PROFILE" -o json &>/dev/null; then
    DEPLOYER=$(databricks current-user me --profile "$PROFILE" -o json \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
    _ok "Authenticated as $DEPLOYER (profile: $PROFILE)"
else
    _error "Could not authenticate with profile '$PROFILE'."
    _info "Run: databricks configure --profile $PROFILE"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 3: Unity Catalog
# ══════════════════════════════════════════════════════════════════════════
_header "Step 3: Unity Catalog"

_info "Genie Workbench stores run history, benchmark results, and optimization"
_info "state in a schema called 'genie_space_optimizer' inside the catalog you"
_info "choose here. The deploy script creates this schema automatically."
echo ""
_warn "You must have CREATE SCHEMA permission on the selected catalog."
echo ""

_info "Discovering available catalogs..."
CATALOG_NAMES=()
while IFS= read -r name; do
    [ -n "$name" ] && CATALOG_NAMES+=("$name")
done < <(
    databricks catalogs list --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    cats = data if isinstance(data, list) else data.get('catalogs', [])
    for c in cats[:30]:
        name = c.get('name','') if isinstance(c, dict) else str(c)
        if name:
            print(name)
except: pass
" 2>/dev/null
)

if [ ${#CATALOG_NAMES[@]} -eq 0 ]; then
    _warn "Could not list catalogs. Enter a catalog name manually."
    _prompt CATALOG "Catalog name" ""
else
    _info "Available catalogs:"
    _select_from CATALOG "Select the catalog to use" "${CATALOG_NAMES[@]}"
fi

if [ -z "$CATALOG" ]; then
    _error "Catalog is required."
    exit 1
fi

GSO_SCHEMA="genie_space_optimizer"
_ok "Will create schema: ${CATALOG}.${GSO_SCHEMA}"

# ══════════════════════════════════════════════════════════════════════════
# Step 4: SQL Warehouse
# ══════════════════════════════════════════════════════════════════════════
_header "Step 4: SQL Warehouse"

_info "The warehouse runs SQL queries for the optimizer and catalog discovery."
echo ""
_info "Discovering available SQL warehouses..."

# Build parallel arrays: display labels and raw IDs
WH_LABELS=()
WH_IDS=()
while IFS='|' read -r wid wlabel; do
    [ -n "$wid" ] && WH_IDS+=("$wid") && WH_LABELS+=("$wlabel")
done < <(
    databricks warehouses list --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    whs = data if isinstance(data, list) else data.get('warehouses', [])
    for w in whs[:20]:
        wid   = w.get('id','')
        name  = w.get('name','Unnamed')
        state = w.get('state','UNKNOWN')
        if wid:
            label = f'{name}  ({state})  — ID: {wid}'
            print(f'{wid}|{label}')
except: pass
" 2>/dev/null
)

if [ ${#WH_LABELS[@]} -eq 0 ]; then
    _warn "Could not list warehouses. Enter the warehouse ID manually."
    _prompt WAREHOUSE_ID "SQL Warehouse ID" ""
else
    _info "Available SQL warehouses:"
    _select_from WH_LABEL "Select a warehouse" "${WH_LABELS[@]}"
    # Reverse-lookup the ID for the selected label
    WAREHOUSE_ID=""
    for i in "${!WH_LABELS[@]}"; do
        if [ "${WH_LABELS[$i]}" = "$WH_LABEL" ]; then
            WAREHOUSE_ID="${WH_IDS[$i]}"
            break
        fi
    done
fi

if [ -z "$WAREHOUSE_ID" ]; then
    _error "Warehouse ID is required."
    exit 1
fi

_ok "Selected warehouse: $WAREHOUSE_ID"

# ══════════════════════════════════════════════════════════════════════════
# Step 5: LLM Model
# ══════════════════════════════════════════════════════════════════════════
_header "Step 5: LLM Model"

_info "Choose the foundation model Genie Workbench will use to create and"
_info "optimize Genie Spaces, generate SQL instructions, and explain findings."
echo ""

CURATED_MODELS=(
    "Claude Sonnet 4.6  (Recommended — databricks-claude-sonnet-4-6)"
    "GPT-5.4            (Databricks — databricks-gpt-5-4)"
    "Other              (browse all serving endpoints or enter a name manually)"
)

LLM_MODEL=""
_select_from MODEL_CHOICE "Select a model" 1 "${CURATED_MODELS[@]}"

case "$MODEL_CHOICE" in
    Claude*)
        LLM_MODEL="databricks-claude-sonnet-4-6" ;;
    GPT*)
        LLM_MODEL="databricks-gpt-5-4" ;;
    Other*)
        echo ""
        echo "    1) Browse all serving endpoints in my workspace"
        echo "    2) Enter endpoint name manually"
        echo ""
        OTHER_CHOICE=""
        while true; do
            echo -en "  [1-2]: "
            read -r OTHER_CHOICE
            case "$OTHER_CHOICE" in
                1|2) break ;;
                *) echo "  Please enter 1 or 2." ;;
            esac
        done
        if [ "$OTHER_CHOICE" = "1" ]; then
            _info "Fetching all serving endpoints..."
            ALL_ENDPOINTS=()
            while IFS= read -r ep; do
                [ -n "$ep" ] && ALL_ENDPOINTS+=("$ep")
            done < <(
                databricks serving-endpoints list --profile "$PROFILE" -o json 2>/dev/null \
                | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    eps = data if isinstance(data, list) else data.get('endpoints', [])
    for e in eps:
        print(e.get('name',''))
except: pass
" 2>/dev/null
            )
            if [ ${#ALL_ENDPOINTS[@]} -eq 0 ]; then
                _warn "No serving endpoints found. Enter endpoint name manually."
                _prompt LLM_MODEL "Endpoint name" ""
            else
                _select_from LLM_MODEL "Select endpoint" "${ALL_ENDPOINTS[@]}"
            fi
        else
            _prompt LLM_MODEL "Endpoint name" ""
        fi
        ;;
esac

if [ -z "$LLM_MODEL" ]; then
    _error "No LLM model selected."
    exit 1
fi

_ok "LLM model: $LLM_MODEL"

# ══════════════════════════════════════════════════════════════════════════
# Step 6: MLflow Tracing (optional)
# ══════════════════════════════════════════════════════════════════════════
_header "Step 6: MLflow Tracing (optional)"

_info "MLflow tracing records every LLM call the Create Agent and Fix Agent make:"
_info "inputs, outputs, token counts, and latency — viewable in the MLflow"
_info "Experiments UI. Useful for debugging agent behavior; adds minor overhead."
_info "You can enable or disable this later by editing .env.deploy."
echo ""

MLFLOW_EXPERIMENT_ID=""
_prompt_yn ENABLE_MLFLOW "Enable MLflow tracing for agents?" "Y"

if [ "$ENABLE_MLFLOW" = "Y" ]; then
    _prompt_yn HAS_EXPERIMENT "Do you already have an MLflow experiment?" "N"

    if [ "$HAS_EXPERIMENT" = "Y" ]; then
        _info "Discovering MLflow experiments..."
        EXP_LABELS=()
        EXP_IDS=()
        while IFS='|' read -r eid elabel; do
            [ -n "$eid" ] && EXP_IDS+=("$eid") && EXP_LABELS+=("$elabel")
        done < <(
            databricks api post /api/2.0/mlflow/experiments/search \
                --profile "$PROFILE" --json '{"max_results": 50}' -o json 2>/dev/null \
            | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for e in data.get('experiments', []):
        eid  = e.get('experiment_id','')
        name = e.get('name','Unnamed')
        if eid:
            print(f'{eid}|{name}  (ID: {eid})')
except: pass
" 2>/dev/null
        )

        if [ ${#EXP_LABELS[@]} -eq 0 ]; then
            _warn "No MLflow experiments found. Enter an experiment ID manually."
            _prompt MLFLOW_EXPERIMENT_ID "MLflow experiment ID" ""
        else
            _info "Available MLflow experiments:"
            _select_from EXP_LABEL "Select an experiment" "${EXP_LABELS[@]}"
            MLFLOW_EXPERIMENT_ID=""
            for i in "${!EXP_LABELS[@]}"; do
                if [ "${EXP_LABELS[$i]}" = "$EXP_LABEL" ]; then
                    MLFLOW_EXPERIMENT_ID="${EXP_IDS[$i]}"
                    break
                fi
            done
        fi

        if [ -z "$MLFLOW_EXPERIMENT_ID" ]; then
            _warn "No experiment selected. MLflow tracing will be disabled."
        else
            _ok "MLflow tracing enabled (experiment: $MLFLOW_EXPERIMENT_ID)"
        fi
    else
        _info "Creating MLflow experiment..."
        _info "Press Enter to accept the default path, or type a custom one."
        _prompt EXPERIMENT_PATH "Experiment path" "/Shared/genie-workbench-agent-tracing"
        # Try to create the experiment
        MLFLOW_EXPERIMENT_ID=$(
            databricks api post /api/2.0/mlflow/experiments/create \
                --profile "$PROFILE" \
                --json "{\"name\": \"$EXPERIMENT_PATH\"}" -o json 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('experiment_id',''))" 2>/dev/null || true
        )
        # If creation failed (e.g. already exists), look it up by name
        if [ -z "$MLFLOW_EXPERIMENT_ID" ]; then
            MLFLOW_EXPERIMENT_ID=$(
                databricks api post /api/2.0/mlflow/experiments/search \
                    --profile "$PROFILE" \
                    --json '{"max_results": 100}' -o json 2>/dev/null \
                | python3 -c "
import sys, json
path = '$EXPERIMENT_PATH'
try:
    for e in json.load(sys.stdin).get('experiments', []):
        if e.get('name','') == path:
            print(e.get('experiment_id',''))
            break
except: pass
" 2>/dev/null || true
            )
        fi
        if [ -n "$MLFLOW_EXPERIMENT_ID" ]; then
            _ok "MLflow experiment ready: $EXPERIMENT_PATH (ID: $MLFLOW_EXPERIMENT_ID)"
        else
            _warn "Could not create or find MLflow experiment. Tracing will be disabled."
            _info "You can create one manually and add the ID to .env.deploy later."
        fi
    fi
else
    _info "MLflow tracing disabled. You can enable it later in .env.deploy."
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 7: Lakebase (PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════
_header "Step 7: Lakebase (PostgreSQL)"

_info "Lakebase provides persistent PostgreSQL storage for scan history, starred"
_info "spaces, and Create Agent sessions. Without it, the app uses in-memory"
_info "storage and all history is lost every time the app restarts."
echo ""
_info "If you don't have a Lakebase instance yet, you can skip this step and"
_info "attach one later via Apps UI → Resources → + Add → PostgreSQL (Lakebase)."
echo ""

_info "Discovering available Lakebase instances..."
LB_NAMES=()
while IFS= read -r name; do
    [ -n "$name" ] && LB_NAMES+=("$name")
done < <(
    databricks api get /api/2.0/database/instances --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    instances = data if isinstance(data, list) else data.get('instances', [])
    for inst in instances:
        name = inst.get('name','') if isinstance(inst, dict) else str(inst)
        if name:
            print(name)
except: pass
" 2>/dev/null
)

LB_OPTIONS=()
if [ ${#LB_NAMES[@]} -gt 0 ]; then
    LB_OPTIONS+=("${LB_NAMES[@]}")
fi
LB_OPTIONS+=("Skip — use in-memory fallback (history lost on restart)")

_info "Available Lakebase instances:"
_select_from LB_CHOICE "Select a Lakebase instance" "${LB_OPTIONS[@]}"

if [[ "$LB_CHOICE" == "Skip — use in-memory fallback (history lost on restart)" ]]; then
    LAKEBASE_INSTANCE=""
    _warn "Skipping Lakebase. Scan history and stars will not persist across restarts."
else
    LAKEBASE_INSTANCE="$LB_CHOICE"
    _ok "Lakebase instance: $LAKEBASE_INSTANCE"
fi

# ══════════════════════════════════════════════════════════════════════════
# Step 8: App name
# ══════════════════════════════════════════════════════════════════════════
_header "Step 8: App name"

_info "This is the name of the Databricks App that will be created in your workspace."
_info "Only lowercase letters, numbers, and hyphens are allowed."
echo ""

APP_NAME=""
while true; do
    echo -en "  Name your Databricks app (e.g. genie-workbench): "
    read -r APP_NAME_INPUT
    if [ -z "$APP_NAME_INPUT" ]; then
        echo "  Please enter an app name."
        continue
    fi

    # Auto-fix: lowercase, replace spaces and underscores with hyphens, strip disallowed chars
    APP_NAME_FIXED=$(echo "$APP_NAME_INPUT" | tr '[:upper:]' '[:lower:]' | tr ' _' '-' | tr -cd 'a-z0-9-')

    if [ "$APP_NAME_FIXED" = "$APP_NAME_INPUT" ]; then
        # Already valid
        APP_NAME="$APP_NAME_INPUT"
        break
    elif [ -z "$APP_NAME_FIXED" ]; then
        _warn "That name contains no valid characters. Please try again."
    else
        _warn "App names may only contain lowercase letters, numbers, and hyphens. Suggested fix: ${BOLD}${APP_NAME_FIXED}${NC}"
        _prompt_yn USE_FIXED "Use '$APP_NAME_FIXED' as your app name?" "Y"
        if [ "$USE_FIXED" = "Y" ]; then
            APP_NAME="$APP_NAME_FIXED"
            break
        fi
    fi
done

_ok "App name: $APP_NAME"

# ══════════════════════════════════════════════════════════════════════════
# Step 9: Write .env.deploy
# ══════════════════════════════════════════════════════════════════════════
_header "Step 9: Writing configuration"

ENV_FILE="$PROJECT_DIR/.env.deploy"
cat > "$ENV_FILE" <<EOF
# Genie Workbench — Deployment Configuration
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

GENIE_WAREHOUSE_ID="$WAREHOUSE_ID"
GENIE_CATALOG="$CATALOG"
GENIE_APP_NAME="$APP_NAME"
GENIE_DEPLOY_PROFILE="$PROFILE"
GENIE_LLM_MODEL="$LLM_MODEL"
GENIE_LAKEBASE_INSTANCE="$LAKEBASE_INSTANCE"
GENIE_MLFLOW_EXPERIMENT_ID="$MLFLOW_EXPERIMENT_ID"
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
echo "  │  MLflow:       ${MLFLOW_EXPERIMENT_ID:-<disabled>}"
echo "  └───────────────────────────────────────────────────────────┘"

# ══════════════════════════════════════════════════════════════════════════
# Step 10: Deploy
# ══════════════════════════════════════════════════════════════════════════
_header "Step 10: Deploying"

_info "This will build the frontend, sync code to your workspace, deploy the"
_info "optimization job, and start the app (typically 3-5 minutes)."
echo ""
_prompt_yn DO_DEPLOY "Deploy now?" "Y"

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
# Step 11: Resolve app service principal
# ══════════════════════════════════════════════════════════════════════════
_header "Step 11: Resolving app service principal"

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
# Step 12: Genie Space permissions (optional)
# ══════════════════════════════════════════════════════════════════════════
_header "Step 12: Genie Space access"

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
