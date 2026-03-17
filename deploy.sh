#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy.sh — deploy the Genie Space Optimizer bundle and apply post-deploy
# grants/permissions that require the app (and its SP) to exist.
#
# Usage:
#   export GENIE_WAREHOUSE_ID=<your-warehouse-id>   # required
#   export GENIE_CATALOG=my_catalog                  # optional (default: main)
#   ./deploy.sh                                      # that's it
#
# Or use a .env.deploy file (see deploy-config.sh for all options).
# Any extra flags are forwarded to `databricks bundle deploy`.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=deploy-config.sh
source "$SCRIPT_DIR/deploy-config.sh"

TOTAL_STEPS=6

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Genie Space Optimizer — Bundle Deploy                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
_print_config

# ── Step 1: Deploy the bundle (creates app + job via Terraform) ──────────
echo ""
echo "▸ Step 1/$TOTAL_STEPS: Deploying bundle..."
databricks bundle deploy "${BUNDLE_VAR_FLAGS[@]}" "$@"
echo "  ✓ Bundle deployed"

# ── Step 2: Resolve the app's service principal ──────────────────────────
echo ""
echo "▸ Step 2/$TOTAL_STEPS: Resolving app service principal..."
SP_CLIENT_ID=$(
    databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id','') or d.get('service_principal_name',''))"
)
if [ -z "$SP_CLIENT_ID" ]; then
    echo "  ✗ Could not resolve SP for app '$APP_NAME'. Is the app created?"
    exit 1
fi
echo "  ✓ SP client ID: $SP_CLIENT_ID"

# ── Step 3: Grant UC permissions to the SP ───────────────────────────────
echo ""
echo "▸ Step 3/$TOTAL_STEPS: Granting Unity Catalog permissions..."
uv run python "$SCRIPT_DIR/resources/grant_app_uc_permissions.py" \
    --profile "$PROFILE" \
    --app-name "$APP_NAME" \
    --catalog "$CATALOG" \
    --schema "$SCHEMA" \
    --warehouse-id "$WAREHOUSE_ID"
echo "  ✓ UC grants applied"

# ── Step 4: Resolve bundle-managed job ID from Terraform state ─────────
echo ""
echo "▸ Step 4/$TOTAL_STEPS: Resolving bundle-managed job ID..."
JOB_ID=$(
    databricks bundle summary "${BUNDLE_VAR_FLAGS[@]}" -o json \
    | python3 -c "
import sys, json
summary = json.load(sys.stdin)
job = summary.get('resources',{}).get('jobs',{}).get('genie-space-optimizer-runner',{})
print(job.get('id',''))
"
)
if [ -z "$JOB_ID" ]; then
    echo "  ⚠ Could not find bundle-managed job in Terraform state."
    echo "  Skipping job permission and app.yml patching."
    exit 0
fi
echo "  ✓ Job ID: $JOB_ID"

# ── Step 5: Patch app.yml with job ID and redeploy ───────────────────────
echo ""
echo "▸ Step 5/$TOTAL_STEPS: Injecting job ID into app.yml and redeploying..."

BUILD_DIR="$SCRIPT_DIR/.build"
DEPLOYER=$(databricks current-user me --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
WS_BUILD_PATH="/Workspace/Users/$DEPLOYER/.bundle/genie-space-optimizer/dev/files/.build"

uv run python "$SCRIPT_DIR/resources/patch_app_yml.py" "$BUILD_DIR/app.yml" \
    --catalog "$CATALOG" --schema "$SCHEMA" --warehouse-id "$WAREHOUSE_ID" \
    --job-id "$JOB_ID"

databricks workspace import "$WS_BUILD_PATH/app.yml" \
    --profile "$PROFILE" --file "$BUILD_DIR/app.yml" --format AUTO --overwrite

APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")
if [ "$APP_STATE" != "ACTIVE" ]; then
    echo "  ℹ App is $APP_STATE — starting..."
    databricks apps start "$APP_NAME" --profile "$PROFILE" --no-wait 2>/dev/null || true
    echo "  Waiting for app to reach RUNNING state..."
    for i in $(seq 1 30); do
        sleep 10
        APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")
        if [ "$APP_STATE" = "ACTIVE" ]; then break; fi
        echo "    ... $APP_STATE (attempt $i/30)"
    done
fi

databricks apps deploy "$APP_NAME" --profile "$PROFILE" --source-code-path "$WS_BUILD_PATH"
echo "  ✓ App redeployed with GENIE_SPACE_OPTIMIZER_JOB_ID=$JOB_ID"

# ── Step 6: Grant CAN_MANAGE on the job to the SP ────────────────────────
echo ""
echo "▸ Step 6/$TOTAL_STEPS: Configuring job permissions..."

PERM_PAYLOAD=$(python3 -c "
import json
acl = [
    {'user_name': '$DEPLOYER', 'permission_level': 'IS_OWNER'},
    {'group_name': 'users', 'permission_level': 'CAN_VIEW'},
    {'service_principal_name': '$SP_CLIENT_ID', 'permission_level': 'CAN_MANAGE'},
]
print(json.dumps({'access_control_list': acl}))
")
databricks api put "/api/2.0/permissions/jobs/$JOB_ID" --profile "$PROFILE" --json "$PERM_PAYLOAD"
echo "  ✓ Job permissions updated (owner=$DEPLOYER, SP=CAN_MANAGE, users=CAN_VIEW)"
echo "  ℹ Job run_as will be set to SP automatically on first app startup"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Deploy complete!"
echo "  App:  $APP_NAME"
echo "  Job:  $JOB_ID (run_as set on app startup)"
echo "  SP:   $SP_CLIENT_ID"
echo "  URL:  https://${APP_NAME}-*.aws.databricksapps.com"
echo "═══════════════════════════════════════════════════════════════"
