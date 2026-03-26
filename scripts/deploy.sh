#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy.sh — deploy Genie Workbench app and configure permissions.
#
# Three modes:
#   Full deploy (default):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Create app (if not exists)
#     4. Full-sync files to workspace
#     5. Resolve app SP + Grant UC permissions (+ enable CDF on GSO tables)
#     6. Resolve job ID + Grant job permissions
#     7. Redeploy app (apps deploy --source-code-path)
#     8. Verify deployment
#
#   Update mode (--update):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Sync files to workspace
#     4. Resolve app SP + Grant UC permissions (+ enable CDF on GSO tables)
#     5. Resolve job ID + Grant job permissions
#     6. Redeploy app (apps deploy --source-code-path)
#     7. Verify deployment
#     Skips app creation.
#     Use for code-only changes when the app already exists.
#
#   Destroy mode (--destroy):
#     1. Clean up runtime-created jobs
#     2. Delete the app
#
# Usage:
#   export GENIE_WAREHOUSE_ID=<your-warehouse-id>   # required
#   export GENIE_CATALOG=my_catalog                  # required
#   ./scripts/deploy.sh                              # full deploy
#   ./scripts/deploy.sh --update                     # code-only update
#   ./scripts/deploy.sh --destroy                    # destroy everything
#   ./scripts/deploy.sh --destroy --auto-approve     # destroy without confirmation
#
# Or use a .env.deploy file (see deploy-config.sh for all options).
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Parse flags ────────────────────────────────────────────────────────────
UPDATE_ONLY=false
DESTROY_MODE=false
AUTO_APPROVE=false
for arg in "$@"; do
    case "$arg" in
        --update)       UPDATE_ONLY=true ;;
        --destroy)      DESTROY_MODE=true ;;
        --auto-approve) AUTO_APPROVE=true ;;
    esac
done

# shellcheck source=deploy-config.sh
source "$SCRIPT_DIR/deploy-config.sh"

# shellcheck source=preflight.sh
source "$SCRIPT_DIR/preflight.sh"

# ═══════════════════════════════════════════════════════════════════════════
# DESTROY MODE
# ═══════════════════════════════════════════════════════════════════════════
if [ "$DESTROY_MODE" = "true" ]; then
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  Genie Workbench — Destroy                                   ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    _print_config

    # ── Step 1: Clean up runtime-created jobs ──────────────────────────
    echo ""
    echo "▸ Step 1/2: Cleaning up runtime-created jobs..."
    RUNTIME_JOBS=$(
        databricks jobs list --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "
import sys, json
jobs = json.load(sys.stdin)
for j in (jobs if isinstance(jobs, list) else jobs.get('jobs', [])):
    tags = (j.get('settings') or {}).get('tags', {})
    if tags.get('app') == '$APP_NAME' and (
        tags.get('pattern') == 'deployment-job' or
        tags.get('managed-by') == 'backend-job-launcher'
    ):
        print(j['job_id'])
" 2>/dev/null || true
    )

    if [ -z "$RUNTIME_JOBS" ]; then
        echo "  No runtime-created jobs found."
    else
        DELETED=0
        for JID in $RUNTIME_JOBS; do
            echo "  Deleting runtime job $JID..."
            if databricks jobs delete "$JID" --profile "$PROFILE" 2>/dev/null; then
                DELETED=$((DELETED + 1))
            else
                echo "  ⚠ Could not delete job $JID (may already be deleted)"
            fi
        done
        echo "  ✓ Cleaned up $DELETED runtime job(s)"
    fi

    # ── Step 2: Delete the app ───────────────────────────────────────
    echo ""
    echo "▸ Step 2/2: Deleting app '$APP_NAME'..."
    if databricks apps get "$APP_NAME" --profile "$PROFILE" &>/dev/null; then
        if [ "$AUTO_APPROVE" != "true" ]; then
            echo "  This will permanently delete the app and its compute."
            echo -n "  Continue? [y/N]: "
            read -r confirm
            if [[ ! "$confirm" =~ ^[Yy] ]]; then
                echo "  Cancelled."
                exit 0
            fi
        fi
        if databricks apps delete "$APP_NAME" --profile "$PROFILE" 2>/dev/null; then
            echo "  ✓ App '$APP_NAME' deleted"
        else
            echo "  ✗ Could not delete app '$APP_NAME'."
            echo "  Try deleting manually via the Databricks Apps UI."
            exit 1
        fi
    else
        echo "  App '$APP_NAME' does not exist — nothing to delete."
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Destroy complete."
    echo "═══════════════════════════════════════════════════════════════"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════
# DEPLOY / UPDATE MODE
# ═══════════════════════════════════════════════════════════════════════════
if [ "$UPDATE_ONLY" = "true" ]; then
    TOTAL_STEPS=7
    DEPLOY_LABEL="Code Update"
else
    TOTAL_STEPS=8
    DEPLOY_LABEL="Full Deploy"
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Genie Workbench — $DEPLOY_LABEL$(printf '%*s' $((37 - ${#DEPLOY_LABEL})) '')║"
echo "╚══════════════════════════════════════════════════════════════╝"
_print_config

# ── Step 1: Pre-flight checks ─────────────────────────────────────────
echo ""
echo "▸ Step 1/$TOTAL_STEPS: Pre-flight checks..."
_preflight_check_tools
_preflight_check_profile "$PROFILE"

# Resolve deployer email (needed for workspace paths)
DEPLOYER=$(databricks current-user me --profile "$PROFILE" -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
WS_PATH="/Workspace/Users/$DEPLOYER/$APP_NAME"

_preflight_check_warehouse "$WAREHOUSE_ID" "$PROFILE"
_preflight_check_catalog "$CATALOG" "$PROFILE"
_preflight_check_app_state "$APP_NAME" "$PROFILE"
echo "  ✓ All pre-flight checks passed"

# ── Step 2: Build frontend ─────────────────────────────────────────────
STEP=2
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Building frontend..."
if ! (cd "$PROJECT_DIR/frontend" && npm install --silent && npm run build --silent); then
    echo "  ✗ Frontend build failed (npm returned non-zero exit code)."
    exit 1
fi
if [ ! -f "$PROJECT_DIR/frontend/dist/index.html" ]; then
    echo "  ✗ Frontend build failed — frontend/dist/index.html not found."
    exit 1
fi
echo "  ✓ Frontend built"

if [ "$UPDATE_ONLY" = "true" ]; then
    # ── Step 3 (update): Sync files to workspace ──────────────────────
    STEP=3
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Syncing files to workspace..."
    # Clean sync: delete workspace dir first so deleted local files don't linger.
    # databricks sync --full only uploads — it never removes stale remote files.
    echo "  Cleaning stale workspace files..."
    databricks workspace delete "$WS_PATH" --profile "$PROFILE" --recursive 2>/dev/null || true
    databricks sync "$PROJECT_DIR" "$WS_PATH" --profile "$PROFILE" --full
    # frontend/dist/ is gitignored so databricks sync skips it — upload explicitly
    echo "  Uploading frontend build artifacts..."
    databricks workspace import-dir "$PROJECT_DIR/frontend/dist" \
        "$WS_PATH/frontend/dist" --profile "$PROFILE" --overwrite
    echo "  ✓ Files synced to $WS_PATH"
else
    # ── Step 3 (full): Create app if not exists ──────────────────────
    STEP=3
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Creating app (if not exists)..."
    if databricks apps get "$APP_NAME" --profile "$PROFILE" &>/dev/null; then
        echo "  ✓ App '$APP_NAME' already exists"
    else
        echo "  Creating app '$APP_NAME'..."
        APP_CREATE_JSON=$(python3 -c "import json; print(json.dumps({'name': '$APP_NAME', 'description': 'Genie Workbench - Create, score, and optimize Genie Spaces'}))")
        if databricks apps create --json "$APP_CREATE_JSON" --profile "$PROFILE" --no-wait 2>/dev/null; then
            echo "  ✓ App created (compute starting in background)"
        else
            echo "  ✗ Could not create app '$APP_NAME'."
            echo ""
            echo "  Remediation:"
            echo "    1. Check if the app name is available"
            echo "    2. Ensure you have permission to create apps"
            echo "    3. Try creating manually in the Databricks Apps UI"
            exit 1
        fi
    fi

    # ── Step 4 (full): Full-sync files to workspace ───────────────────
    STEP=4
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Syncing files to workspace..."
    # Clean sync: delete workspace dir first so deleted local files don't linger.
    # databricks sync --full only uploads — it never removes stale remote files.
    echo "  Cleaning stale workspace files..."
    databricks workspace delete "$WS_PATH" --profile "$PROFILE" --recursive 2>/dev/null || true
    databricks sync "$PROJECT_DIR" "$WS_PATH" --profile "$PROFILE" --full
    # frontend/dist/ is gitignored so databricks sync skips it — upload explicitly
    echo "  Uploading frontend build artifacts..."
    databricks workspace import-dir "$PROJECT_DIR/frontend/dist" \
        "$WS_PATH/frontend/dist" --profile "$PROFILE" --overwrite
    echo "  ✓ Full sync complete"
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

python3 "$SCRIPT_DIR/grant_permissions.py" \
    --profile "$PROFILE" \
    --app-name "$APP_NAME" \
    --catalog "$CATALOG" \
    --schema "$GSO_SCHEMA" \
    --warehouse-id "$WAREHOUSE_ID"
echo "  ✓ UC grants applied"

# Grant deployer "Service Principal User" role on the app SP.
# Required for setting the job's run_as to the SP.
SP_USER_PAYLOAD=$(python3 -c "
import json
print(json.dumps({'access_control_list': [
    {'user_name': '$DEPLOYER', 'permission_level': 'CAN_USE'},
]}))
")
if databricks api patch "/api/2.0/permissions/authorization/serviceprincipals/$SP_CLIENT_ID" \
    --profile "$PROFILE" --json "$SP_USER_PAYLOAD" 2>/dev/null; then
    echo "  ✓ Deployer granted Service Principal User role"
else
    echo "  ⚠ Could not grant SP User role — job run_as may fail. Ask a workspace admin."
fi

# ── Set up GSO optimization job ──────────────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Setting up optimization job..."

# Find existing job or create one (builds wheel, uploads notebooks, creates job)
if JOB_ID=$(python3 "$SCRIPT_DIR/ensure_gso_job.py" \
    --profile "$PROFILE" \
    --catalog "$CATALOG" \
    --schema "$GSO_SCHEMA" \
    --app-name "$APP_NAME" \
    --project-dir "$PROJECT_DIR" \
    --sp-client-id "$SP_CLIENT_ID"); then

    # Grant job permissions — SP owns the job (run_as identity), deployer can manage
    PERM_PAYLOAD=$(python3 -c "
import json
acl = [
    {'user_name': '$DEPLOYER', 'permission_level': 'CAN_MANAGE'},
    {'group_name': 'users', 'permission_level': 'CAN_VIEW'},
    {'service_principal_name': '$SP_CLIENT_ID', 'permission_level': 'IS_OWNER'},
]
print(json.dumps({'access_control_list': acl}))
")
    if databricks api put "/api/2.0/permissions/jobs/$JOB_ID" --profile "$PROFILE" --json "$PERM_PAYLOAD" 2>/dev/null; then
        echo "  ✓ Job permissions updated (SP=IS_OWNER, deployer=CAN_MANAGE, users=CAN_VIEW)"
    else
        echo "  ⚠ Could not set job permissions — SP may not be able to trigger optimization runs."
    fi
else
    echo "  ⚠ Could not set up optimization job. Auto-Optimize will not be available."
    echo "  You can retry by re-running: ./scripts/deploy.sh --update"
    JOB_ID=""
fi

# ── Redeploy app (ensures freshest code) ─────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Redeploying app with freshest code..."

# Patch app.yaml on workspace with real GSO values before apps deploy.
# apps deploy reads app.yaml and uses it as the complete env config,
# overwriting whatever was previously set. So we must inject the real values.
echo "  Patching app.yaml on workspace with GSO config..."
PATCHED_APP_YAML="/tmp/app.yaml.patched"
cp "$PROJECT_DIR/app.yaml" "$PATCHED_APP_YAML"
sed -i.bak "s|__GSO_CATALOG__|$CATALOG|" "$PATCHED_APP_YAML"
sed -i.bak "s|__LAKEBASE_INSTANCE__|$LAKEBASE_INSTANCE|" "$PATCHED_APP_YAML"
sed -i.bak "s|__LLM_MODEL__|$LLM_MODEL|" "$PATCHED_APP_YAML"
sed -i.bak "s|__MLFLOW_EXPERIMENT_ID__|$MLFLOW_EXPERIMENT_ID|" "$PATCHED_APP_YAML"
if [ -n "$JOB_ID" ]; then
    sed -i.bak "s|__GSO_JOB_ID__|$JOB_ID|" "$PATCHED_APP_YAML"
fi

rm -f "${PATCHED_APP_YAML}.bak"

# Validate all placeholders were resolved
UNRESOLVED=$(grep -c '__[A-Z_]*__' "$PATCHED_APP_YAML" || true)
if [ "$UNRESOLVED" -gt 0 ]; then
    echo "  ⚠ app.yaml has $UNRESOLVED unresolved placeholder(s):"
    grep '__[A-Z_]*__' "$PATCHED_APP_YAML" | sed 's/^/      /'
fi

databricks workspace import "$WS_PATH/app.yaml" \
    --profile "$PROFILE" --file "$PATCHED_APP_YAML" --format AUTO --overwrite 2>/dev/null && \
echo "  ✓ app.yaml patched (GSO_CATALOG=$CATALOG, GSO_JOB_ID=${JOB_ID:-<none>}, LAKEBASE_INSTANCE=$LAKEBASE_INSTANCE, LLM_MODEL=$LLM_MODEL, MLFLOW=${MLFLOW_EXPERIMENT_ID:-<disabled>})" || \
echo "  ⚠ Could not patch app.yaml — config may not be set"

# Sync _metadata.py — gitignored so sync skips it,
# but required at runtime for the genie_space_optimizer package to import.
METADATA_SRC="$PROJECT_DIR/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
METADATA_DST="$WS_PATH/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
if [ -f "$METADATA_SRC" ]; then
    databricks workspace import "$METADATA_DST" \
        --profile "$PROFILE" --file "$METADATA_SRC" --format AUTO --overwrite 2>/dev/null && \
    echo "  ✓ _metadata.py synced" || \
    echo "  ⚠ Could not sync _metadata.py"
fi

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

# ── Set app scopes + resources, then deploy ──────────────────────────────
# Merge existing resources (e.g. manually-added Lakebase) with required ones.
echo "  Configuring app scopes and resources..."
EXISTING_RESOURCES=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin).get('resources',[])))" 2>/dev/null || echo "[]")

PATCH_PAYLOAD=$(python3 -c "
import json
scopes = ['sql', 'dashboards.genie', 'serving.serving-endpoints',
           'catalog.catalogs:read', 'catalog.schemas:read',
           'catalog.tables:read', 'files.files']

# Start with existing resources that have full config (not empty stubs).
# The PATCH API replaces all resources, so we must include everything.
# Empty stubs like {'name': 'postgres'} are rejected — skip them.
existing = json.loads('$EXISTING_RESOURCES')
by_name = {}
for r in existing:
    has_config = any(k for k in r if k != 'name')
    if has_config:
        by_name[r['name']] = r

# Ensure sql-warehouse is set with the correct ID
by_name['sql-warehouse'] = {'name': 'sql-warehouse', 'sql_warehouse': {'id': '$WAREHOUSE_ID', 'permission': 'CAN_USE'}}

print(json.dumps({'user_api_scopes': scopes, 'resources': list(by_name.values())}))
")
databricks api patch "/api/2.0/apps/$APP_NAME" \
    --profile "$PROFILE" --json "$PATCH_PAYLOAD" 2>/dev/null && \
    echo "  ✓ App scopes and resources configured" || \
    echo "  ⚠ Could not configure app scopes/resources"

databricks apps deploy "$APP_NAME" --profile "$PROFILE" \
    --source-code-path "$WS_PATH" --no-wait
echo "  ✓ App deployment triggered from $WS_PATH"

# ── Verify deployment ────────────────────────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Verifying deployment..."
VERIFY_OK=true

# Check critical files exist on workspace
echo "  Checking critical files on workspace..."
CRITICAL_FILES=(
    "$WS_PATH/backend/main.py"
    "$WS_PATH/backend/__init__.py"
    "$WS_PATH/requirements.txt"
    "$WS_PATH/frontend/dist/index.html"
    "$WS_PATH/app.yaml"
)
MISSING_FILES=()
for f in "${CRITICAL_FILES[@]}"; do
    if ! databricks workspace get-status "$f" --profile "$PROFILE" &>/dev/null; then
        MISSING_FILES+=("$(basename "$f")")
    fi
done
if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    echo "  ✗ Missing critical files on workspace: ${MISSING_FILES[*]}"
    echo ""
    echo "  Remediation: re-run deploy or use --update mode."
    VERIFY_OK=false
else
    echo "  ✓ All critical files present on workspace"
fi

# Wait for deployment to settle and check status
echo "  Waiting for app deployment to settle..."
DEPLOY_STATE="IN_PROGRESS"
for i in $(seq 1 18); do
    sleep 10
    APP_JSON=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null)
    DEPLOY_STATE=$(echo "$APP_JSON" | python3 -c "
import sys,json
d=json.load(sys.stdin)
ad = d.get('pending_deployment',{}) or d.get('active_deployment',{})
print(ad.get('status',{}).get('state','UNKNOWN'))
" 2>/dev/null || echo "UNKNOWN")
    if [ "$DEPLOY_STATE" != "IN_PROGRESS" ]; then
        break
    fi
    echo "    ... $DEPLOY_STATE (attempt $i/18)"
done

APP_URL=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || true)
APP_STATUS=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('app_status',{}).get('state','UNKNOWN'))" 2>/dev/null || true)

if [ "$DEPLOY_STATE" = "SUCCEEDED" ]; then
    echo "  ✓ App deployment SUCCEEDED"

    # Wait for app to finish restarting and reach RUNNING state
    if [ "$APP_STATUS" != "RUNNING" ]; then
        echo "  Waiting for app to reach RUNNING state..."
        for i in $(seq 1 12); do
            sleep 10
            APP_JSON=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null)
            APP_STATUS=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('app_status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
            APP_URL=$(echo "$APP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || true)
            if [ "$APP_STATUS" = "RUNNING" ]; then
                break
            fi
            if [ "$APP_STATUS" = "CRASHED" ] || [ "$APP_STATUS" = "UNAVAILABLE" ]; then
                break
            fi
            echo "    ... app is $APP_STATUS (attempt $i/12)"
        done
    fi

    if [ "$APP_STATUS" = "RUNNING" ]; then
        echo "  ✓ App is RUNNING"
    elif [ "$APP_STATUS" = "CRASHED" ] || [ "$APP_STATUS" = "UNAVAILABLE" ]; then
        echo "  ✗ App status: $APP_STATUS"
        echo "  Check logs:  databricks apps logs $APP_NAME --profile $PROFILE"
        VERIFY_OK=false
    else
        echo "  ℹ App is still $APP_STATUS — it may need more time to start."
        echo "  Check status:  databricks apps get $APP_NAME --profile $PROFILE"
    fi
elif [ "$DEPLOY_STATE" = "FAILED" ]; then
    DEPLOY_MSG=$(echo "$APP_JSON" | python3 -c "
import sys,json; d=json.load(sys.stdin)
ad = d.get('pending_deployment',{}) or d.get('active_deployment',{})
print(ad.get('status',{}).get('message','unknown error'))
" 2>/dev/null || echo "unknown")
    echo "  ✗ App deployment FAILED: $DEPLOY_MSG"
    echo ""
    echo "  Remediation:"
    echo "    1. Check logs:  databricks apps logs $APP_NAME --profile $PROFILE"
    echo "    2. Common causes:"
    echo "       - Missing Python dependencies (check requirements.txt)"
    echo "       - Import errors (check backend/main.py and its imports)"
    echo "       - Missing frontend/dist/ (gitignored, must be built + uploaded)"
    echo "    3. Fix the issue and re-run: ./scripts/deploy.sh --update"
    VERIFY_OK=false
elif [ "$DEPLOY_STATE" = "IN_PROGRESS" ]; then
    echo "  ℹ App deployment still IN_PROGRESS after 3 minutes"
    echo "  Check status:  databricks apps get $APP_NAME --profile $PROFILE"
else
    echo "  ℹ App deployment state: $DEPLOY_STATE"
fi

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
if [ "$VERIFY_OK" != "true" ]; then
    echo "  Status: DEPLOY FAILED — review errors above"
    echo ""
    echo "  Quick debug:"
    echo "    databricks apps logs $APP_NAME --profile $PROFILE"
elif [ "$APP_STATUS" = "RUNNING" ]; then
    echo "  Status: App is RUNNING ✓"
else
    echo "  Status: Deploy succeeded, app is $APP_STATUS"
    echo "  The app may need a minute to finish starting."
fi
echo ""
echo "  NOTE: If you see 'Failed to list spaces' in the app, attach a"
echo "  Lakebase PostgreSQL resource named 'postgres' in the Apps UI"
echo "  with CAN_CONNECT_AND_CREATE permission. The app will auto-retry"
echo "  schema creation — no redeploy needed."
echo "═══════════════════════════════════════════════════════════════"
