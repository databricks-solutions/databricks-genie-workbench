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
#     5. Deploy optimization job via bundle (databricks bundle deploy -t app)
#     6. Wait for app compute to reach ACTIVE
#     7. Run setup_workbench.py (UC + Lakebase + Apps PATCH + app.yaml +
#        GSO job perms + bundle-dir perms + Genie Space grants)
#     8. Redeploy app (apps deploy --source-code-path)
#     9. Verify deployment
#
#   Update mode (--update):
#     1. Pre-flight checks
#     2. Build frontend
#     3. Sync files to workspace
#     4. Deploy optimization job via bundle
#     5. Wait for app compute to reach ACTIVE
#     6. Run setup_workbench.py
#     7. Redeploy app (apps deploy --source-code-path)
#     8. Verify deployment
#     Skips app creation.
#     Use for code-only changes when the app already exists.
#
#   Destroy mode (--destroy):
#     1. Clean up runtime-created jobs
#     2. Destroy bundle-managed optimization job
#     3. Delete the app
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

    if [ "$AUTO_APPROVE" != "true" ]; then
        echo ""
        echo "  This will permanently delete the app, optimization job, and all bundle state."
        echo -n "  Continue? [y/N]: "
        read -r confirm
        if [[ ! "$confirm" =~ ^[Yy] ]]; then
            echo "  Cancelled."
            exit 0
        fi
    fi

    # ── Step 1: Clean up runtime-created jobs ──────────────────────────
    echo ""
    echo "▸ Step 1/3: Cleaning up runtime-created jobs..."
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

    # ── Step 2: Destroy bundle-managed optimization job ───────────────
    echo ""
    echo "▸ Step 2/3: Destroying bundle-managed optimization job..."
    if (cd "$PROJECT_DIR" && databricks bundle destroy -t app \
        --var="catalog=${CATALOG}" \
        --var="warehouse_id=${WAREHOUSE_ID:-placeholder}" \
        --profile "$PROFILE" --auto-approve 2>&1 | sed 's/^/  /'); then
        echo "  ✓ Bundle resources destroyed"
    else
        echo "  ⚠ Bundle destroy failed or no bundle state found (OK on first deploy)"
    fi

    # ── Step 3: Delete the app ───────────────────────────────────────
    echo ""
    echo "▸ Step 3/3: Deleting app '$APP_NAME'..."
    if databricks apps get "$APP_NAME" --profile "$PROFILE" &>/dev/null; then
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
    TOTAL_STEPS=8
    DEPLOY_LABEL="Code Update"
else
    TOTAL_STEPS=9
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
_preflight_check_venv
_preflight_check_npm_registry
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
if ! (cd "$PROJECT_DIR/frontend" && npm ci && npm run build); then
    echo "  ✗ Frontend build failed (npm returned non-zero exit code)."
    echo ""
    echo "  See npm's error output above for the root cause."
    echo "  Common causes:"
    echo "    - Internal npm mirror missing a package or hash-mismatching"
    echo "      (npm config set registry https://registry.npmjs.org/)"
    echo "    - Rollup platform binary missing after npm ci"
    echo "      (rm -rf frontend/node_modules && cd frontend && npm ci)"
    echo "    - Node version too old (require >= 18)"
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
    databricks sync "$PROJECT_DIR" "$WS_PATH" --profile "$PROFILE" --full \
        --exclude-from "$PROJECT_DIR/.databricksignore"
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
    databricks sync "$PROJECT_DIR" "$WS_PATH" --profile "$PROFILE" --full \
        --exclude-from "$PROJECT_DIR/.databricksignore"
    # frontend/dist/ is gitignored so databricks sync skips it — upload explicitly
    echo "  Uploading frontend build artifacts..."
    databricks workspace import-dir "$PROJECT_DIR/frontend/dist" \
        "$WS_PATH/frontend/dist" --profile "$PROFILE" --overwrite
    echo "  ✓ Full sync complete"
fi

# ── Deploy optimization job via bundle ────────────────────────────────────
# Must run before setup_workbench.py so the job exists when we wire app.yaml.
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Deploying optimization job via bundle..."

# databricks bundle deploy -t app:
#   - Builds the GSO wheel (artifacts block)
#   - Syncs job notebooks to workspace
#   - Creates/updates the optimization job (Terraform-managed)
# run_as is NOT set in the bundle — the app self-heals it at startup
# via _ensure_gso_job_run_as() in backend/main.py (avoids needing
# servicePrincipal.user role on the deployer).

# Force full file sync on every deploy. The bundle CLI uses local snapshot
# files to do incremental uploads; if the snapshot drifts from the workspace
# (e.g. interrupted upload, workspace cleanup) notebooks silently go missing
# and the job fails at runtime. Deleting the snapshots is cheap — the wheel
# upload dominates deploy time, not the ~350 small file uploads.
rm -f "$PROJECT_DIR/.databricks/bundle/app/sync-snapshots/"*.json 2>/dev/null || true

set +e
BUNDLE_OUTPUT=$(cd "$PROJECT_DIR" && databricks bundle deploy -t app \
    --var="catalog=$CATALOG" \
    --var="warehouse_id=$WAREHOUSE_ID" \
    --profile "$PROFILE" 2>&1)
BUNDLE_EXIT=$?
set -e
echo "$BUNDLE_OUTPUT" | sed 's/^/  /'

if [ "$BUNDLE_EXIT" -ne 0 ]; then
    echo ""
    echo "  ✗ Bundle deploy failed (exit code $BUNDLE_EXIT)."
    echo ""
    echo "  Remediation:"
    echo "    1. Check the error output above"
    echo "    2. Common causes:"
    echo "       - Databricks CLI too old (need >= 0.297.2)"
    echo "       - Auth issue with profile '$PROFILE'"
    echo "       - GSO wheel build failure (missing 'build' package)"
    echo "       - Terraform state conflict (try: databricks bundle deploy -t app --force-lock)"
    echo "    3. Fix the issue and re-run: ./scripts/deploy.sh --update"
    exit 1
fi

# Verify critical job notebooks actually landed on the workspace.
# The bundle's incremental sync can silently skip files if the local snapshot
# diverges from workspace reality. This catches that failure at deploy time
# rather than at job runtime.
_PREFLIGHT_NB="/Workspace/Users/$DEPLOYER/.bundle/genie-workbench/app/files/packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_preflight"
if ! databricks workspace get-status "$_PREFLIGHT_NB" --profile "$PROFILE" -o json >/dev/null 2>&1; then
    echo ""
    echo "  ✗ FATAL: Bundle file sync failed — job notebook not found at:"
    echo "    $_PREFLIGHT_NB"
    echo ""
    echo "  Remediation:"
    echo "    1. Delete stale sync snapshots:"
    echo "       rm -f .databricks/bundle/app/sync-snapshots/*.json"
    echo "    2. Re-run: ./scripts/deploy.sh --update"
    exit 1
fi
echo "  ✓ Job notebooks verified on workspace"

JOB_ID=$(cd "$PROJECT_DIR" && databricks bundle summary -t app \
    --var="catalog=$CATALOG" \
    --var="warehouse_id=$WAREHOUSE_ID" \
    --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys, json
s = json.load(sys.stdin)
print(s['resources']['jobs']['gso-optimization-runner']['id'])
" 2>/dev/null) || true

if [ -z "$JOB_ID" ]; then
    echo "  ✗ Bundle deployed but could not resolve job ID from Terraform state."
    echo ""
    echo "  Remediation:"
    echo "    1. Run: databricks bundle summary -t app --profile $PROFILE -o json"
    echo "    2. Check if resources.jobs.gso-optimization-runner.id exists"
    echo "    3. Re-run: ./scripts/deploy.sh --update"
    exit 1
fi

echo "  ✓ Optimization job deployed: $JOB_ID"

# Clean up legacy jobs created by the old ensure_gso_job.py script.
# These have name "genie-space-optimizer-job" and tag "persistent-dag"
# but are NOT the bundle-managed job (different ID).
LEGACY_JOBS=$(databricks jobs list --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "
import sys, json
bundle_id = '$JOB_ID'
jobs = json.load(sys.stdin)
for j in (jobs if isinstance(jobs, list) else jobs.get('jobs', [])):
    tags = (j.get('settings') or {}).get('tags', {})
    name = (j.get('settings') or {}).get('name', '')
    jid = str(j.get('job_id', ''))
    if (tags.get('pattern') == 'persistent-dag'
        and tags.get('app') in ('genie-workbench', 'genie-space-optimizer')
        and jid != bundle_id):
        print(jid)
" 2>/dev/null || true)

for OLD_JID in $LEGACY_JOBS; do
    echo "  ℹ Found legacy optimization job $OLD_JID — deleting..."
    databricks jobs delete "$OLD_JID" --profile "$PROFILE" 2>/dev/null && \
        echo "  ✓ Legacy job $OLD_JID deleted" || \
        echo "  ⚠ Could not delete legacy job $OLD_JID — delete it manually"
done

# Sync _metadata.py — required at runtime for the genie_space_optimizer
# package to import.  Previously gitignored (generated by apx build), now
# checked in as a static stub.  Fail hard if missing or upload fails so
# the optimization trigger doesn't crash with "No module named _metadata".
METADATA_SRC="$PROJECT_DIR/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
METADATA_DST="$WS_PATH/packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py"
if [ ! -f "$METADATA_SRC" ]; then
    echo "  ✗ FATAL: _metadata.py not found at:"
    echo "    $METADATA_SRC"
    echo ""
    echo "  This file is required at runtime.  It should exist in the repo."
    echo "  If missing after a fresh clone, recreate it:"
    echo ""
    echo "    cat > packages/genie-space-optimizer/src/genie_space_optimizer/_metadata.py << 'PYEOF'"
    echo "    from pathlib import Path"
    echo "    app_name: str = \"genie-space-optimizer\""
    echo "    app_slug: str = \"genie_space_optimizer\""
    echo "    api_prefix: str = \"/api/genie\""
    echo "    dist_dir: Path = Path(__file__).resolve().parent / \"__dist__\""
    echo "    PYEOF"
    exit 1
fi
databricks workspace import "$METADATA_DST" \
    --profile "$PROFILE" --file "$METADATA_SRC" --format AUTO --overwrite || {
    echo "  ✗ FATAL: Could not upload _metadata.py to workspace."
    echo "  The optimization trigger will fail without this file."
    exit 1
}
echo "  ✓ _metadata.py synced"

# ── Wait for app compute to reach ACTIVE (needed before apps deploy) ─────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Waiting for app compute..."
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

# ── Provision resources via shared module ────────────────────────────────
# setup_workbench absorbs: UC schema/tables/grants, Lakebase project/role/grants,
# Lakebase DB resolution, Apps PATCH (scopes + resources), GSO job permissions,
# bundle-directory SP grants, app.yaml placeholder substitution, and optional
# Genie Space SP grants. Pure SDK — no databricks CLI subprocess calls.
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Provisioning UC / Lakebase / app resources..."

SETUP_ARGS=(
    --app-name "$APP_NAME"
    --catalog "$CATALOG"
    --warehouse-id "$WAREHOUSE_ID"
    --llm-model "$LLM_MODEL"
    --mlflow-experiment-id "$MLFLOW_EXPERIMENT_ID"
    --workspace-folder "$WS_PATH"
    --gso-job-id "$JOB_ID"
    --gso-schema "$GSO_SCHEMA"
    --profile "$PROFILE"
    --deployer-email "$DEPLOYER"
)
if [ -n "${LAKEBASE_INSTANCE:-}" ]; then
    SETUP_ARGS+=(--lakebase-project "$LAKEBASE_INSTANCE")
fi
if [ "${GRANT_SPACES:-Y}" != "Y" ]; then
    SETUP_ARGS+=(--skip-genie-grants)
fi

if ! (cd "$PROJECT_DIR" && uv run python -m scripts.setup_workbench "${SETUP_ARGS[@]}"); then
    echo ""
    echo "  ✗ setup_workbench.py failed — see errors above."
    echo "  Remediation:"
    echo "    1. Re-run: ./scripts/deploy.sh --update"
    echo "    2. If UC grants failed, ask a catalog owner to grant USE_CATALOG on '$CATALOG'"
    echo "    3. If Lakebase failed, verify GENIE_LAKEBASE_INSTANCE is a valid Autoscaling project"
    exit 1
fi
echo "  ✓ Provisioning complete"

# ── Redeploy app (ensures freshest code) ─────────────────────────────────
STEP=$((STEP + 1))
echo ""
echo "▸ Step $STEP/$TOTAL_STEPS: Redeploying app with freshest code..."
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
    "$WS_PATH/pyproject.toml"
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
    echo "       - Missing Python dependencies (check pyproject.toml and uv.lock)"
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

# Resolve SP for summary line (setup_workbench already used/printed it, but we
# display it here too for the final banner)
SP_CLIENT_ID=$(
    databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id','') or d.get('service_principal_name',''))" \
    2>/dev/null || true
)

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Deploy complete!"
echo "  App:      $APP_NAME"
echo "  Job:      ${JOB_ID:-<not found>}"
echo "  SP:       ${SP_CLIENT_ID:-<unknown>}"
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
