#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# destroy.sh — clean up runtime-created resources, then destroy the bundle.
#
# The bundle-managed runner job is cleaned up automatically by Terraform.
# This script additionally removes per-space deployment jobs that were
# created at runtime (tagged with app=genie-space-optimizer).
#
# Usage:
#   export GENIE_WAREHOUSE_ID=<your-warehouse-id>   # required
#   ./destroy.sh                                     # interactive
#   ./destroy.sh --auto-approve                      # skip confirmation
#
# Or use a .env.deploy file (see deploy-config.sh for all options).
# Any extra flags are forwarded to `databricks bundle destroy`.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=deploy-config.sh
source "$SCRIPT_DIR/deploy-config.sh"

APP_TAG="genie-space-optimizer"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Genie Space Optimizer — Bundle Destroy                     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
_print_config

# ── Step 1: Clean up runtime-created jobs (deployment + stale runners) ──
echo ""
echo "▸ Step 1/2: Cleaning up runtime-created jobs..."

RUNTIME_JOBS=$(
    databricks jobs list --profile "$PROFILE" -o json \
    | python3 -c "
import sys, json
jobs = json.load(sys.stdin)
for j in jobs:
    tags = j.get('settings', {}).get('tags', {})
    if tags.get('app') == '$APP_TAG' and (
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

# ── Step 2: Destroy the bundle ───────────────────────────────────────────
echo ""
echo "▸ Step 2/2: Destroying bundle (app + runner job)..."
databricks bundle destroy "${BUNDLE_VAR_FLAGS[@]}" "$@"
echo "  ✓ Bundle destroyed"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Destroy complete."
echo "═══════════════════════════════════════════════════════════════"
