#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy.sh — deploy Genie Workbench bundle (app + GSO optimization job)
# and apply post-deploy grants/permissions.
#
# Three modes:
#   Full deploy (default):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Clean stale wheels from workspace
#     4. Bundle deploy (terraform — creates/updates job + app resources)
#     5. Full-sync files to workspace (ensures complete codebase)
#     6. Resolve app SP + Grant UC permissions
#     7. Resolve job ID + Grant job permissions
#     8. Redeploy app (apps deploy --source-code-path)
#     9. Verify deployment (including critical file checks)
#
#   Update mode (--update):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Sync files to workspace (no terraform)
#     4. Resolve app SP + Grant UC permissions
#     5. Resolve job ID + Grant job permissions
#     6. Redeploy app (apps deploy --source-code-path)
#     7. Verify deployment
#     Skips bundle deploy and wheel cleanup.
#     Use for code-only changes when the app already exists.
#
#   Destroy mode (--destroy):
#     1. Clean up runtime-created jobs
#     2. Destroy the bundle (Terraform-managed app + job)
#
# Usage:
#   export GENIE_WAREHOUSE_ID=<your-warehouse-id>   # required
#   export GENIE_CATALOG=my_catalog                  # required
#   ./deploy.sh                                      # full deploy
#   ./deploy.sh --update                             # code-only update
#   ./deploy.sh --destroy                            # destroy everything
#   ./deploy.sh --destroy --auto-approve             # destroy without confirmation
#
# Or use a .env.deploy file (see deploy-config.sh for all options).
# Any extra flags are forwarded to `databricks bundle deploy`.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Parse flags ────────────────────────────────────────────────────────────
UPDATE_ONLY=false
DESTROY_MODE=false
EXTRA_ARGS=()
for arg in "$@"; do
    case "$arg" in
        --update)  UPDATE_ONLY=true ;;
        --destroy) DESTROY_MODE=true ;;
        *)         EXTRA_ARGS+=("$arg") ;;
    esac
done
set -- "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"

# shellcheck source=deploy-config.sh
source "$SCRIPT_DIR/deploy-config.sh"

# shellcheck source=scripts/preflight.sh
source "$SCRIPT_DIR/scripts/preflight.sh"

# ═══════════════════════════════════════════════════════════════════════════
# DESTROY MODE
# ═══════════════════════════════════════════════════════════════════════════
if [ "$DESTROY_MODE" = "true" ]; then
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  Genie Workbench — Bundle Destroy                          ║"
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

    # ── Step 2: Destroy the bundle ─────────────────────────────────────
    echo ""
    echo "▸ Step 2/2: Destroying bundle (app + runner job)..."

    # Ensure .build dir exists (bundle validate needs it for sync.include)
    BUILD_STUB=false
    if [ ! -d "$SCRIPT_DIR/.build" ]; then
        mkdir -p "$SCRIPT_DIR/.build"
        touch "$SCRIPT_DIR/.build/.keep"
        BUILD_STUB=true
    fi

    databricks bundle destroy "${BUNDLE_VAR_FLAGS[@]}" "${BUNDLE_TARGET_FLAGS[@]}" --profile "$PROFILE" "$@"

    # Clean up stub if we created it
    if [ "$BUILD_STUB" = "true" ]; then
        rm -rf "$SCRIPT_DIR/.build"
    fi

    echo "  ✓ Bundle destroyed"
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
    TOTAL_STEPS=9
    DEPLOY_LABEL="Bundle Deploy"
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
WS_BUNDLE_PATH="/Workspace/Users/$DEPLOYER/.bundle/genie-workbench/$DEPLOY_TARGET/files"

_preflight_check_warehouse "$WAREHOUSE_ID" "$PROFILE"
_preflight_check_catalog "$CATALOG" "$PROFILE"
_preflight_check_app_state "$APP_NAME" "$PROFILE"
echo "  ✓ All pre-flight checks passed"

# ── Step 2: Build frontend ─────────────────────────────────────────────
STEP=2
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Building frontend..."
(cd "$SCRIPT_DIR/frontend" && npm install --silent && npm run build --silent)
if [ ! -f "$SCRIPT_DIR/frontend/dist/index.html" ]; then
    echo "  ✗ Frontend build failed — frontend/dist/index.html not found."
    exit 1
fi
echo "  ✓ Frontend built"

if [ "$UPDATE_ONLY" = "true" ]; then
    # ── Step 3 (update): Sync files to workspace ──────────────────────
    STEP=3
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Syncing files to workspace..."
    # Full sync respects .databricksignore (which includes frontend/dist/)
    databricks sync . "$WS_BUNDLE_PATH" --profile "$PROFILE" --full
    # frontend/dist/ is gitignored so databricks sync skips it — upload explicitly
    echo "  Uploading frontend build artifacts..."
    databricks workspace import-dir "$SCRIPT_DIR/frontend/dist" \
        "$WS_BUNDLE_PATH/frontend/dist" --profile "$PROFILE" --overwrite
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
    databricks bundle deploy "${BUNDLE_VAR_FLAGS[@]}" "${BUNDLE_TARGET_FLAGS[@]}" --profile "$PROFILE" "$@"
    echo "  ✓ Bundle deployed"

    # NOTE: Lakebase resource wiring moved to post-deploy PATCH step (runs in both modes)

    # ── Step 5 (full): Full-sync files to workspace ───────────────────
    # Bundle deploy only uploads changed files (incremental). On a fresh
    # workspace or after a destroy, most source files are missing. Do a
    # full sync + explicit frontend/dist upload to guarantee completeness.
    STEP=5
    echo ""
    echo "▸ Step $STEP/$TOTAL_STEPS: Full-syncing source files to workspace..."
    databricks sync . "$WS_BUNDLE_PATH" --profile "$PROFILE" --full
    # frontend/dist/ is gitignored so databricks sync skips it — upload explicitly
    echo "  Uploading frontend build artifacts..."
    databricks workspace import-dir "$SCRIPT_DIR/frontend/dist" \
        "$WS_BUNDLE_PATH/frontend/dist" --profile "$PROFILE" --overwrite
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
        databricks bundle summary "${BUNDLE_VAR_FLAGS[@]}" "${BUNDLE_TARGET_FLAGS[@]}" --profile "$PROFILE" -o json \
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
UNRESOLVED=$(grep -c '__[A-Z_]*__' "$PATCHED_APP_YAML" || true)
if [ "$UNRESOLVED" -gt 0 ]; then
    echo "  ⚠ app.yaml has $UNRESOLVED unresolved placeholder(s):"
    grep '__[A-Z_]*__' "$PATCHED_APP_YAML" | sed 's/^/      /'
fi

databricks workspace import "$WS_BUNDLE_PATH/app.yaml" \
    --profile "$PROFILE" --file "$PATCHED_APP_YAML" --format AUTO --overwrite 2>/dev/null && \
echo "  ✓ app.yaml patched (GSO_CATALOG=$CATALOG, GSO_JOB_ID=${JOB_ID:-<none>})" || \
echo "  ⚠ Could not patch app.yaml — config may not be set"

# Sync _metadata.py — gitignored so bundle sync/databricks sync skip it,
# but required at runtime for the genie_space_optimizer package to import.
METADATA_SRC="$SCRIPT_DIR/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
METADATA_DST="$WS_BUNDLE_PATH/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
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

# If there's a Lakebase postgres project, include it as a resource
# This handles the case where Lakebase was added via UI but shows as
# an empty stub in the GET response — we reconstruct from the project.
if 'postgres' not in by_name and '$DEPLOY_TARGET' == 'dev-lakebase':
    import subprocess, sys
    try:
        result = subprocess.run(
            ['databricks', 'api', 'get',
             '/api/2.0/postgres/projects/${APP_NAME}-db/branches/production/databases',
             '--profile', '$PROFILE'],
            capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            dbs = json.loads(result.stdout).get('databases', [])
            if dbs:
                by_name['postgres'] = {
                    'name': 'postgres',
                    'postgres': {
                        'branch': dbs[0].get('parent', ''),
                        'database': dbs[0].get('name', ''),
                        'permission': 'CAN_CONNECT_AND_CREATE'
                    }
                }
    except Exception:
        pass

print(json.dumps({'user_api_scopes': scopes, 'resources': list(by_name.values())}))
")
databricks api patch "/api/2.0/apps/$APP_NAME" \
    --profile "$PROFILE" --json "$PATCH_PAYLOAD" 2>/dev/null && \
    echo "  ✓ App scopes and resources configured" || \
    echo "  ⚠ Could not configure app scopes/resources"

databricks apps deploy "$APP_NAME" --profile "$PROFILE" \
    --source-code-path "$WS_BUNDLE_PATH" --no-wait
echo "  ✓ App deployment triggered from $WS_BUNDLE_PATH"

# ── Verify deployment ────────────────────────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Verifying deployment..."
VERIFY_OK=true

# Check critical files exist on workspace
echo "  Checking critical files on workspace..."
CRITICAL_FILES=(
    "$WS_BUNDLE_PATH/backend/main.py"
    "$WS_BUNDLE_PATH/backend/__init__.py"
    "$WS_BUNDLE_PATH/requirements.txt"
    "$WS_BUNDLE_PATH/frontend/dist/index.html"
    "$WS_BUNDLE_PATH/app.yaml"
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
    echo "  This typically happens when bundle deploy does an incremental sync"
    echo "  on a fresh workspace. The full-sync step should have fixed this."
    echo ""
    echo "  Remediation: re-run deploy or use --update mode."
    VERIFY_OK=false
else
    echo "  ✓ All critical files present on workspace"
fi

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

# Wait for deployment to settle and check status
echo "  Waiting for app deployment to settle..."
DEPLOY_STATE="IN_PROGRESS"
for i in $(seq 1 18); do
    sleep 10
    APP_JSON=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null)
    DEPLOY_STATE=$(echo "$APP_JSON" | python3 -c "
import sys,json
d=json.load(sys.stdin)
ad = d.get('active_deployment',{}) or d.get('pending_deployment',{})
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
elif [ "$DEPLOY_STATE" = "FAILED" ]; then
    DEPLOY_MSG=$(echo "$APP_JSON" | python3 -c "
import sys,json; d=json.load(sys.stdin)
ad = d.get('active_deployment',{}) or d.get('pending_deployment',{})
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
    echo "    3. Fix the issue and re-run: ./deploy.sh --update"
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
if [ "$VERIFY_OK" = "true" ]; then
    echo "  Status: All checks passed ✓"
else
    echo "  Status: DEPLOY FAILED — review errors above"
    echo ""
    echo "  Quick debug:"
    echo "    databricks apps logs $APP_NAME --profile $PROFILE"
fi
echo "═══════════════════════════════════════════════════════════════"
