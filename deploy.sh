#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy.sh — deploy Genie Workbench bundle (app + GSO optimization job)
# and apply post-deploy grants/permissions.
#
# Two modes:
#   Full deploy (default):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Clean stale wheels from workspace
#     4. Bundle deploy (terraform — creates/updates job + app resources)
#     5. Resolve app SP + Grant UC permissions
#     6. Resolve job ID + Grant job permissions
#     7. Redeploy app (apps deploy --source-code-path)
#     8. Verify deployment
#
#   Update mode (--update):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Sync files to workspace (no terraform)
#     4. Resolve app SP + Grant UC permissions
#     5. Redeploy app (apps deploy --source-code-path)
#     6. Verify deployment
#     Skips bundle deploy, wheel cleanup, and job permissions.
#     Use for code-only changes when the app already exists.
#
# Usage:
#   export GENIE_WAREHOUSE_ID=<your-warehouse-id>   # required
#   export GENIE_CATALOG=my_catalog                  # required
#   ./deploy.sh                                      # full deploy
#   ./deploy.sh --update                             # code-only update
#
# Or use a .env.deploy file (see deploy-config.sh for all options).
# Any extra flags are forwarded to `databricks bundle deploy`.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Parse --update flag ──────────────────────────────────────────────────
UPDATE_ONLY=false
EXTRA_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--update" ]; then
        UPDATE_ONLY=true
    else
        EXTRA_ARGS+=("$arg")
    fi
done
set -- "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"

# shellcheck source=deploy-config.sh
source "$SCRIPT_DIR/deploy-config.sh"

# shellcheck source=scripts/preflight.sh
source "$SCRIPT_DIR/scripts/preflight.sh"

if [ "$UPDATE_ONLY" = "true" ]; then
    TOTAL_STEPS=7
    DEPLOY_LABEL="Code Update"
else
    TOTAL_STEPS=8
    DEPLOY_LABEL="Bundle Deploy"
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Genie Workbench — $DEPLOY_LABEL$(printf '%*s' $((37 - ${#DEPLOY_LABEL})) '')║"
echo "╚══════════════════════════════════════════════════════════════╝"
_print_config

# ── Step 1: Pre-flight checks ─────────────────────────────────────────
echo ""
echo "▸ Step 1/$TOTAL_STEPS: Pre-flight checks..."
_preflight_check_profile "$PROFILE"

# Resolve deployer email (needed for workspace paths)
DEPLOYER=$(databricks current-user me --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
WS_BUNDLE_PATH="/Workspace/Users/$DEPLOYER/.bundle/genie-workbench/dev/files"

_preflight_check_warehouse "$WAREHOUSE_ID" "$PROFILE"
_preflight_check_catalog "$CATALOG" "$PROFILE"
_preflight_check_app_state "$APP_NAME" "$PROFILE"
echo "  ✓ All pre-flight checks passed"

# ── Step 2: Build frontend ─────────────────────────────────────────────
STEP=2
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Building frontend..."
(cd "$SCRIPT_DIR/frontend" && npm install --silent && npm run build --silent)
echo "  ✓ Frontend built"

if [ "$UPDATE_ONLY" = "true" ]; then
    # ── Step 3 (update): Sync files to workspace ──────────────────────
    STEP=3
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Syncing files to workspace..."
    databricks sync . "$WS_BUNDLE_PATH" --profile "$PROFILE" --full
    echo "  ✓ Files synced to $WS_BUNDLE_PATH"
else
    # ── Step 3 (full): Clean stale wheels from workspace ──────────────
    STEP=3
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Cleaning stale wheels from workspace..."
    STALE_COUNT=0
    for whl in $(databricks workspace list "$WS_BUNDLE_PATH/.build" --profile "$PROFILE" 2>/dev/null \
        | grep "\.whl" | awk '{print $NF}'); do
        echo "  Deleting: $(basename "$whl")"
        databricks workspace delete "$whl" --profile "$PROFILE" 2>/dev/null || true
        STALE_COUNT=$((STALE_COUNT + 1))
    done
    if [ "$STALE_COUNT" -gt 0 ]; then
        echo "  ✓ Removed $STALE_COUNT stale wheel(s)"
    else
        echo "  ✓ No stale wheels found"
    fi

    # ── Step 4 (full): Deploy the bundle ──────────────────────────────
    STEP=4
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Deploying bundle..."
    databricks bundle deploy "${BUNDLE_VAR_FLAGS[@]}" "$@"
    echo "  ✓ Bundle deployed"
fi

# ── Resolve app SP + Grant UC permissions ────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Resolving app SP and granting UC permissions..."
SP_CLIENT_ID=$(
    databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id','') or d.get('service_principal_name',''))"
)
if [ -z "$SP_CLIENT_ID" ]; then
    echo "  ✗ Could not resolve SP for app '$APP_NAME'. Is the app created?"
    exit 1
fi
echo "  ✓ SP client ID: $SP_CLIENT_ID"

python3 "$SCRIPT_DIR/scripts/grant_permissions.py" \
    --profile "$PROFILE" \
    --app-name "$APP_NAME" \
    --catalog "$CATALOG" \
    --schema "$GSO_SCHEMA" \
    --warehouse-id "$WAREHOUSE_ID"
echo "  ✓ UC grants applied"

# ── Resolve job ID + Grant job permissions ───────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Resolving job ID and configuring permissions..."

if [ "$UPDATE_ONLY" = "true" ]; then
    # In update mode, find the job by name (bundle state may be stale)
    JOB_ID=$(
        databricks jobs list --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
jobs = data if isinstance(data, list) else data.get('jobs', [])
for j in jobs:
    name = (j.get('settings') or {}).get('name', '')
    if 'gso-optimization' in name.lower():
        print(j.get('job_id', ''))
        break
" 2>/dev/null || true
    )
else
    JOB_ID=$(
        databricks bundle summary "${BUNDLE_VAR_FLAGS[@]}" -o json \
        | python3 -c "
import sys, json
summary = json.load(sys.stdin)
job = summary.get('resources',{}).get('jobs',{}).get('gso-optimization-runner',{})
print(job.get('id',''))
"
    )
fi

if [ -z "$JOB_ID" ]; then
    echo "  ⚠ Could not find optimization job."
    echo "  The job will be created on the next full bundle deploy."
else
    # Validate the job actually exists on the workspace
    if ! databricks jobs get "$JOB_ID" --profile "$PROFILE" &>/dev/null; then
        echo "  ✗ Job $JOB_ID does not exist on workspace."
        echo ""
        echo "  Remediation:"
        echo "    1. Run a full deploy (without --update) to recreate the job"
        echo "    2. Or manually create the job and update GSO_JOB_ID"
        exit 1
    fi
    echo "  ✓ Job ID: $JOB_ID (verified on workspace)"

    PERM_PAYLOAD=$(python3 -c "
import json
acl = [
    {'user_name': '$DEPLOYER', 'permission_level': 'IS_OWNER'},
    {'group_name': 'users', 'permission_level': 'CAN_VIEW'},
    {'service_principal_name': '$SP_CLIENT_ID', 'permission_level': 'CAN_MANAGE'},
]
print(json.dumps({'access_control_list': acl}))
")
    if databricks api put "/api/2.0/permissions/jobs/$JOB_ID" --profile "$PROFILE" --json "$PERM_PAYLOAD" 2>/dev/null; then
        echo "  ✓ Job permissions updated (owner=$DEPLOYER, SP=CAN_MANAGE, users=CAN_VIEW)"
    else
        echo "  ✗ Could not set job permissions on job $JOB_ID."
        echo "  The app SP will not be able to trigger optimization runs."
        exit 1
    fi
fi

# ── Redeploy app (ensures freshest code) ─────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Redeploying app with freshest code..."

# Patch app.yaml on workspace with real GSO values before apps deploy.
# apps deploy reads app.yaml and uses it as the complete env config,
# overwriting whatever the bundle set. So we must inject the real values.
echo "  Patching app.yaml on workspace with GSO config..."
PATCHED_APP_YAML="/tmp/app.yaml.patched"
cp "$SCRIPT_DIR/app.yaml" "$PATCHED_APP_YAML"
sed -i.bak "s|__GSO_CATALOG__|$CATALOG|" "$PATCHED_APP_YAML"
if [ -n "$JOB_ID" ]; then
    sed -i.bak "s|__GSO_JOB_ID__|$JOB_ID|" "$PATCHED_APP_YAML"
fi
rm -f "${PATCHED_APP_YAML}.bak"

# Validate all placeholders were resolved
if grep -q '__GSO_' "$PATCHED_APP_YAML"; then
    echo "  ✗ app.yaml still contains unresolved placeholders:"
    grep '__GSO_' "$PATCHED_APP_YAML" | sed 's/^/      /'
    echo ""
    echo "  Remediation: Ensure GSO_CATALOG and GSO_JOB_ID are set."
    rm -f "$PATCHED_APP_YAML"
    exit 1
fi

databricks workspace import "$WS_BUNDLE_PATH/app.yaml" \
    --profile "$PROFILE" --file "$PATCHED_APP_YAML" --format AUTO --overwrite 2>/dev/null && \
echo "  ✓ app.yaml patched (GSO_CATALOG=$CATALOG, GSO_JOB_ID=${JOB_ID:-<none>})" || \
echo "  ⚠ Could not patch app.yaml — GSO config may not be set"

# Ensure app compute is running before deploying
APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")
if [ "$APP_STATE" != "ACTIVE" ]; then
    echo "  ℹ App compute is $APP_STATE — starting..."
    databricks apps start "$APP_NAME" --profile "$PROFILE" --no-wait 2>/dev/null || true
    echo "  Waiting for app compute to reach ACTIVE state..."
    for i in $(seq 1 30); do
        sleep 10
        APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")
        if [ "$APP_STATE" = "ACTIVE" ]; then
            echo "  ✓ App compute is ACTIVE"
            break
        fi
        echo "    ... $APP_STATE (attempt $i/30)"
    done
    if [ "$APP_STATE" != "ACTIVE" ]; then
        echo "  ⚠ App compute did not reach ACTIVE state after 5 minutes."
        echo "  Proceeding with deploy anyway — it may start on deployment."
    fi
else
    echo "  ✓ App compute is already ACTIVE"
fi

databricks apps deploy "$APP_NAME" --profile "$PROFILE" \
    --source-code-path "$WS_BUNDLE_PATH" --no-wait
echo "  ✓ App deployment triggered from $WS_BUNDLE_PATH"

# ── Verify deployment ────────────────────────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Verifying deployment..."
VERIFY_OK=true

# Check wheel exists on workspace
WHL_LIST=""
for CHECK_PATH in "$WS_BUNDLE_PATH/.build" "$WS_BUNDLE_PATH"; do
    WHL_LIST=$(databricks workspace list "$CHECK_PATH" --profile "$PROFILE" 2>/dev/null \
        | grep "\.whl" || true)
    if [ -n "$WHL_LIST" ]; then
        echo "  ✓ Wheel on workspace: $(echo "$WHL_LIST" | awk '{print $NF}' | head -1)"
        break
    fi
done
if [ -z "$WHL_LIST" ]; then
    echo "  ⚠ No wheel found on workspace"
    VERIFY_OK=false
fi

# Check app deployment status
APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('url',''))" 2>/dev/null || true)
APP_DEPLOY_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
ad = d.get('active_deployment',{}) or d.get('pending_deployment',{})
print(ad.get('status',{}).get('state','UNKNOWN'))
" 2>/dev/null || echo "UNKNOWN")
echo "  ✓ App deployment state: $APP_DEPLOY_STATE"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Deploy complete!"
echo "  App:      $APP_NAME"
echo "  Job:      ${JOB_ID:-<not found>}"
echo "  SP:       $SP_CLIENT_ID"
echo "  Deployer: $DEPLOYER"
echo ""
if [ -n "$APP_URL" ]; then
    echo "  URL: $APP_URL"
else
    echo "  URL: https://${APP_NAME}-*.databricksapps.com (available shortly)"
fi
echo ""
if [ "$VERIFY_OK" = "true" ]; then
    echo "  Status: All checks passed ✓"
else
    echo "  Status: Some checks had warnings — review output above"
fi
echo "═══════════════════════════════════════════════════════════════"
