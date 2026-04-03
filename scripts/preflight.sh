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

    # Verify Databricks CLI version meets minimum for bundle app/job support
    local cli_version
    cli_version=$(databricks --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    if [ -n "$cli_version" ]; then
        local minor
        minor=$(echo "$cli_version" | cut -d. -f2)
        if [ "$(echo "$cli_version" | cut -d. -f1)" -eq 0 ] && [ "$minor" -lt 239 ]; then
            echo ""
            echo "  ✗ Databricks CLI version $cli_version is too old (minimum: 0.239.0)."
            echo ""
            echo "  Remediation:"
            echo "    pip install --upgrade databricks-cli"
            echo "    or: brew upgrade databricks"
            exit 1
        fi
        echo "  ✓ Databricks CLI version $cli_version"
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
