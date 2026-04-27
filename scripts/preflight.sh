#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# preflight.sh — reusable pre-flight validation functions for deploy.sh
#
# Sourced (not executed) by deploy.sh. Each function prints a clear error
# with remediation steps and exits non-zero if the check fails.
# ---------------------------------------------------------------------------

_preflight_check_tools() {
    echo "  Checking required tools..."
    local missing=()
    command -v databricks &>/dev/null || missing+=("databricks")
    command -v python3 &>/dev/null    || missing+=("python3")
    command -v node &>/dev/null       || missing+=("node")
    command -v npm &>/dev/null        || missing+=("npm")
    command -v uv &>/dev/null         || missing+=("uv")
    if [ ${#missing[@]} -gt 0 ]; then
        echo ""
        echo "  ✗ Missing required tools: ${missing[*]}"
        echo ""
        if [[ " ${missing[*]} " == *" uv "* ]]; then
            echo "  Install uv:"
            echo "    curl -LsSf https://astral.sh/uv/install.sh | sh"
            echo "    or: brew install uv"
            echo ""
        fi
        echo "  Remediation: install the missing tools and re-run scripts/deploy.sh"
        exit 1
    fi
    echo "  ✓ All required tools available (databricks, python3, node, npm, uv)"

    local node_version
    node_version=$(node --version 2>/dev/null || echo "unknown")
    if ! node -e '
const [major, minor] = process.versions.node.split(".").map(Number);
const supported = (major === 20 && minor >= 19) || (major === 22 && minor >= 12) || major > 22;
process.exit(supported ? 0 : 1);
'; then
        echo ""
        echo "  ✗ Node.js $node_version is not supported by the frontend toolchain."
        echo ""
        echo "  Vite requires Node.js ^20.19.0 or >=22.12.0."
        echo ""
        echo "  Remediation: install Node.js 22 LTS or newer, then re-run scripts/deploy.sh"
        exit 1
    fi
    echo "  ✓ Node.js version $node_version"

    # Verify Databricks CLI version meets minimum for bundle app/job support
    local cli_version min_cli_version="0.297.2"
    cli_version=$(databricks --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    if [ -n "$cli_version" ]; then
        local lowest
        lowest=$(printf '%s\n%s\n' "$min_cli_version" "$cli_version" | sort -V | head -1)
        if [ "$lowest" != "$min_cli_version" ]; then
            echo ""
            echo "  ✗ Databricks CLI version $cli_version is too old (minimum: $min_cli_version)."
            echo ""
            echo "  Remediation:"
            echo "    brew upgrade databricks"
            echo "    or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
            exit 1
        fi
        echo "  ✓ Databricks CLI version $cli_version"
    fi
}

_preflight_check_venv() {
    echo "  Syncing Python venv (uv sync --frozen)..."
    if uv sync --frozen --quiet; then
        echo "  ✓ Python venv ready (pinned dependencies)"
    else
        echo ""
        echo "  ✗ uv sync --frozen failed. The Python venv could not be created."
        echo ""
        echo "  See uv's error output above for the root cause."
        echo "  Common causes:"
        echo "    - Internal PyPI mirror missing a package (unset UV_INDEX_URL or set to https://pypi.org/simple)"
        echo "    - Python 3.11+ not available (try: uv python install 3.11)"
        echo "    - Corrupt .venv (try: rm -rf .venv && uv sync --frozen)"
        exit 1
    fi
}

_preflight_check_npm_lockfiles() {
    echo "  Checking npm lockfiles for private registry URLs..."
    local private_host="npm-proxy.dev.databricks.com"
    local lockfiles=(
        "$PROJECT_DIR/package-lock.json"
        "$PROJECT_DIR/frontend/package-lock.json"
        "$PROJECT_DIR/packages/genie-space-optimizer/package-lock.json"
    )
    local offenders=()
    local lockfile

    for lockfile in "${lockfiles[@]}"; do
        if [ -f "$lockfile" ] && grep -q "$private_host" "$lockfile"; then
            offenders+=("${lockfile#$PROJECT_DIR/}")
        fi
    done

    if [ ${#offenders[@]} -gt 0 ]; then
        echo ""
        echo "  ✗ Committed npm lockfiles contain private Databricks registry URLs:"
        local offender
        for offender in "${offenders[@]}"; do
            echo "    - $offender"
        done
        echo ""
        echo "  Lockfiles must be registry-neutral so both internal and external users can install."
        echo ""
        echo "  Remediation:"
        echo "    1. Keep your preferred npm registry in user/global npm config only:"
        echo "       Databricks internal: npm config set registry https://npm-proxy.dev.databricks.com/"
        echo "       External/customer:  npm config set registry https://registry.npmjs.org/"
        echo "    2. Regenerate lockfiles with omit-lockfile-registry-resolved=true"
        echo "       or normalize private resolved URLs to https://registry.npmjs.org/"
        echo "    3. Commit the registry-neutral lockfiles"
        exit 1
    fi
    echo "  ✓ npm lockfiles are registry-neutral"
}

_preflight_check_npm_registry() {
    echo "  Checking npm registry connectivity..."
    local registry
    registry=$(npm config get registry 2>/dev/null | sed 's|/$||')
    registry="${registry:-https://registry.npmjs.org}"
    if curl -s -o /dev/null -w "" --connect-timeout 5 "${registry}/react" 2>/dev/null; then
        echo "  ✓ npm registry ($registry) is reachable"
    else
        echo ""
        echo "  ✗ Cannot reach npm registry ($registry)."
        echo ""
        echo "  The frontend install requires downloading npm packages."
        echo ""
        echo "  Remediation:"
        echo "    1. Check your internet connection"
        echo "    2. Use a registry reachable from your network:"
        echo "       Databricks internal: npm config set registry https://npm-proxy.dev.databricks.com/"
        echo "       External/customer:  npm config set registry https://registry.npmjs.org/"
        echo "    3. If using an HTTP proxy: npm config set proxy <proxy-url>"
        echo ""
        exit 1
    fi
}

_preflight_check_profile() {
    local profile="$1"
    echo "  Checking CLI profile '$profile'..."
    if ! databricks current-user me --profile "$profile" -o json &>/dev/null; then
        echo ""
        echo "  ✗ Cannot authenticate with profile '$profile'."
        echo ""
        echo "  Remediation:"
        echo "    1. Run: databricks configure --profile $profile"
        echo "    2. Or set GENIE_DEPLOY_PROFILE to a valid profile name"
        echo ""
        exit 1
    fi
    echo "  ✓ CLI profile is valid"
}

_preflight_check_warehouse() {
    local warehouse_id="$1"
    local profile="$2"
    echo "  Checking SQL warehouse '$warehouse_id'..."
    local wh_output
    if ! wh_output=$(databricks warehouses get "$warehouse_id" --profile "$profile" -o json 2>&1); then
        echo ""
        echo "  ✗ SQL warehouse '$warehouse_id' is not accessible."
        echo ""
        echo "  Remediation:"
        echo "    1. Verify the warehouse ID is correct"
        echo "    2. Ensure your user/SP has CAN_USE permission on the warehouse"
        echo "    3. Check the warehouse exists: databricks warehouses list --profile $profile"
        echo ""
        exit 1
    fi
    local wh_state
    wh_state=$(echo "$wh_output" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
    echo "  ✓ SQL warehouse exists (state: $wh_state)"
}

_preflight_check_catalog() {
    local catalog="$1"
    local profile="$2"
    echo "  Checking catalog '$catalog'..."
    if ! databricks catalogs get "$catalog" --profile "$profile" -o json &>/dev/null; then
        echo ""
        echo "  ✗ Catalog '$catalog' is not accessible."
        echo ""
        echo "  Remediation:"
        echo "    1. Verify the catalog name is correct"
        echo "    2. Ensure you have USE CATALOG and CREATE SCHEMA permissions"
        echo "    3. List catalogs: databricks catalogs list --profile $profile"
        echo ""
        exit 1
    fi
    echo "  ✓ Catalog exists"
}

_preflight_check_app_state() {
    local app_name="$1"
    local profile="$2"
    echo "  Checking app state for '$app_name'..."

    local app_output
    if app_output=$(databricks apps get "$app_name" --profile "$profile" -o json 2>/dev/null); then
        # App exists — check if it's in a cleanup/deleted state
        local app_status
        app_status=$(echo "$app_output" | python3 -c "
import sys, json
d = json.load(sys.stdin)
status = d.get('status', {}).get('state', d.get('compute_status', {}).get('state', 'UNKNOWN'))
print(status)
" 2>/dev/null || echo "UNKNOWN")

        if echo "$app_status" | grep -qi "delet\|cleanup"; then
            echo ""
            echo "  ⚠ App '$app_name' exists but is in '$app_status' state."
            echo ""
            echo "  Remediation:"
            echo "    1. Wait for cleanup to complete (can take 5-10 minutes)"
            echo "    2. Then re-run scripts/deploy.sh"
            echo ""
            exit 1
        fi
        echo "  ✓ App exists (state: $app_status) — bundle will update it"
    else
        echo "  ✓ App does not exist yet — deploy will create it"
    fi
}
