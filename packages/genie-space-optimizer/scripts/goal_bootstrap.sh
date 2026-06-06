#!/usr/bin/env bash
# goal_bootstrap.sh — wire the /goal harness into the repo.
#
# Idempotent. Safe to re-run. Exits 0 only when every prerequisite for
# launching `/goal` is satisfied.
#
# What this script does:
#   1. Verifies required binaries (claude >= 2.1.139, jq, rg, databricks) are on PATH.
#   2. Symlinks the 7 GSO skills from docs/skills/ into .claude/skills/
#      so Claude Code's skill discovery picks them up.
#   3. Merges the harness's PreToolUse + PostToolUse hook block into
#      .claude/settings.json without disturbing any existing entries
#      (e.g. SessionStart).
#   4. Smoke-tests the evidence emitter + the pretrial gate's hook-mode
#      no-op path. Both must succeed for /goal to function.
#   5. Probes Databricks CLI auth. Without a working token, every
#      gso-lever-loop-replay invocation would fail at runtime; better to
#      fail bootstrap loudly so the operator re-authenticates BEFORE any
#      /goal run wastes Claude tokens spinning on auth errors.
#
# Usage:
#   bash packages/genie-space-optimizer/scripts/goal_bootstrap.sh
#
# Exit codes:
#   0  — BOOTSTRAP_READY
#   1  — required binary missing
#   2  — claude CLI too old
#   3  — skill source directory missing
#   4  — settings.json merge failed
#   5  — smoke test failed
#   6  — Databricks CLI auth broken (run `databricks auth login --host <HOST>`)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

# Databricks CLI profile that the /goal harness will exercise.
# The canonical profile is pinned to `fevm-prashanth` in AGENTS.md (the
# anchor parent runs that the /goal harness replays against live in
# that workspace under job_id 488860692117207). The CURRENT
# parent_run_id per anchor is NOT hardcoded here — it lives in
# `packages/genie-space-optimizer/docs/architecture/canonical-anchors.md`
# (Current parent job runs table) and rotates when a parent exhausts
# the 250-task-value Databricks platform budget (verdict
# PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250). Original anchor parents
# 501649560474489 (airline) and 807620338215711 (7now) were retired
# on 2026-06-06 after exhaustion; see canonical-anchors.md for the
# audit trail and the current ACTIVE parents.
#
# Operators on a different workspace can override at launch time:
#   GSO_DATABRICKS_PROFILE=my-profile bash .../goal_kick.sh next-plan
GSO_DATABRICKS_PROFILE="${GSO_DATABRICKS_PROFILE:-fevm-prashanth}"

fail() { echo "BOOTSTRAP_FAIL: $1" >&2; exit "${2:-1}"; }
info() { echo "  bootstrap: $1"; }
ok()   { echo "  bootstrap: $1  ✓"; }

# --- 1. Required binaries -------------------------------------------------

info "verifying required binaries"
command -v claude     >/dev/null || fail "claude CLI not on PATH" 1
command -v jq         >/dev/null || fail "jq not installed (brew install jq)" 1
command -v databricks >/dev/null || fail "databricks CLI not on PATH (https://docs.databricks.com/aws/en/dev-tools/cli/install)" 1

# ripgrep is required by the PostToolUse hooks (forbid_legacy_imports.sh,
# check_invariants.sh). Without it the hooks silently skip their checks
# and the harness loses architecture-invariant enforcement. Try to install
# automatically via brew before failing.
if ! command -v rg >/dev/null; then
  if command -v brew >/dev/null; then
    info "ripgrep missing; installing via brew (one-time, ~30s)"
    brew install ripgrep >&2 \
      || fail "brew install ripgrep failed; install it manually then re-run" 1
    command -v rg >/dev/null \
      || fail "ripgrep installed but 'rg' still not on PATH; restart your shell and re-run" 1
    ok "ripgrep installed via brew"
  else
    fail "ripgrep not installed and brew not available; install ripgrep manually then re-run" 1
  fi
fi

# Parse claude version; accept anything >= 2.1.139.
CLAUDE_VER_RAW=$(claude --version 2>&1 | head -1)
CLAUDE_VER=$(echo "$CLAUDE_VER_RAW" | awk '{print $1}')
# Compare semver by sorting; if the input version compares >= 2.1.139 we accept.
IS_OK=$(printf '%s\n2.1.139\n' "$CLAUDE_VER" | sort -V | tail -1)
[ "$IS_OK" = "$CLAUDE_VER" ] || fail "claude $CLAUDE_VER is below 2.1.139 (required by /goal)" 2
ok "claude $CLAUDE_VER (>= 2.1.139)"

# --- 2. Skill symlinks ----------------------------------------------------

info "wiring 7 GSO skills into .claude/skills/"
mkdir -p .claude/skills

SKILLS=(
  gso-emit-evidence-for-evaluator
  gso-offline-funnel-iterate
  gso-lever-loop-trigger
  gso-plan-next-fix
  gso-postmortem
  gso-lever-loop-run-analysis
  gso-lever-loop-replay
)

for s in "${SKILLS[@]}"; do
  src="$REPO_ROOT/packages/genie-space-optimizer/docs/skills/$s"
  dst=".claude/skills/$s"
  [ -d "$src" ] || fail "missing source skill directory: $src" 3
  [ -f "$src/SKILL.md" ] || fail "missing $src/SKILL.md" 3
  # ln -sfn is idempotent for symlinks; if dst already exists as a real dir,
  # we leave it alone (operator may have copied instead of symlinked).
  if [ -L "$dst" ] || [ ! -e "$dst" ]; then
    ln -sfn "$src" "$dst"
  fi
done
ok "skills wired ($(printf '%s ' "${SKILLS[@]}"))"

# --- 3. Hook merge into .claude/settings.json -----------------------------

info "merging PreToolUse + PostToolUse hooks into .claude/settings.json"
mkdir -p .claude
[ -f .claude/settings.json ] || echo '{}' > .claude/settings.json

# Validate existing settings.json is parseable; if not, refuse to clobber.
jq -e . .claude/settings.json >/dev/null 2>&1 \
  || fail ".claude/settings.json is not valid JSON; refusing to clobber. Fix it manually first." 4

# The merge replaces ONLY our two managed hook keys (PreToolUse, PostToolUse)
# with the harness-required entries. All other settings keys, and any
# SessionStart / Notification / Stop / etc. hooks, are preserved verbatim.
HARNESS_PRETOOL='[{
  "matcher": "Bash",
  "hooks": [{
    "type": "command",
    "command": "bash packages/genie-space-optimizer/scripts/pretrial_gate.sh --hook-mode",
    "timeout": 120
  }]
}]'

HARNESS_POSTTOOL='[{
  "matcher": "Edit|Write",
  "hooks": [
    {"type": "command", "command": "bash packages/genie-space-optimizer/scripts/forbid_legacy_imports.sh", "timeout": 10},
    {"type": "command", "command": "bash packages/genie-space-optimizer/scripts/check_invariants.sh",      "timeout": 15}
  ]
}]'

jq --argjson pre "$HARNESS_PRETOOL" --argjson post "$HARNESS_POSTTOOL" '
  . as $orig
  | (.hooks // {}) as $h
  | .hooks = ($h | .PreToolUse = $pre | .PostToolUse = $post)
' .claude/settings.json > .claude/settings.json.tmp \
  || fail "jq merge into settings.json failed" 4
mv .claude/settings.json.tmp .claude/settings.json
ok "hooks merged"

# --- 4. Smoke tests -------------------------------------------------------

info "smoke-testing emit_evidence_for_evaluator.py"
python3 packages/genie-space-optimizer/scripts/emit_evidence_for_evaluator.py \
  --trial 0 --phase idle \
  --opt-run-id-airline none --opt-run-id-7now none \
  --next bootstrap_smoke >/dev/null \
  || fail "emit_evidence_for_evaluator.py failed; aborting" 5
ok "evidence emitter green"

info "tracker-shape self-test (catches '## Trial N — ...' format drift)"
# --self-test inspects the CANONICAL iteration tracker at
# docs/architecture/lever-loop-iteration-tracker.md and verifies:
#   (a) at least one canonical '## Trial N — ' row exists,
#   (b) no canonical trial id appears more than once,
#   (c) the latest-row extraction returns non-empty content,
#   (d) the latest trial row contains a '### Status' sub-section.
# It also WARNS (non-fatal) if the tracker still references legacy
# anchor IDs (dc89d1a9 / 98ec8950) instead of the canonical
# e94376a3 (airline) / d13938e7 (7now). Future tracker drift fails
# bootstrap loudly here, before /goal can be launched against a
# malformed tracker.
if ! python3 packages/genie-space-optimizer/scripts/emit_evidence_for_evaluator.py --self-test 2>&1 \
       | sed 's/^/    /' >&2; then
  fail "tracker shape self-test failed; see SELF_TEST_FAIL line above for details" 5
fi
ok "tracker shape self-test green"

info "smoke-testing pretrial_gate.sh --hook-mode no-op"
# In --hook-mode the script reads $CLAUDE_TOOL_INPUT and only runs the
# full gate when that variable contains the literal substring "deploy.sh".
# A harmless 'ls' command must produce exit 0 immediately.
CLAUDE_TOOL_INPUT='ls -la' \
  bash packages/genie-space-optimizer/scripts/pretrial_gate.sh --hook-mode \
  || fail "pretrial_gate.sh --hook-mode no-op path failed" 5
ok "pretrial_gate hook-mode no-op green"

# --- 5. Databricks CLI auth probe ----------------------------------------
#
# `gso-lever-loop-replay` invokes `databricks jobs runs repair` which
# requires a valid OAuth/PAT token. The most common failure mode is an
# expired refresh token in the cache after a long gap between sessions
# (manifests as: "stored credentials from older CLI versions are no
# longer used" or "refresh token is invalid"). Detecting this here saves
# multi-minute Claude turns that would only discover the auth break
# inside a skill call.
info "probing Databricks CLI auth for profile '$GSO_DATABRICKS_PROFILE'"
DB_AUTH_OUT=$(databricks --profile "$GSO_DATABRICKS_PROFILE" current-user me 2>&1)
DB_AUTH_EXIT=$?
if [ $DB_AUTH_EXIT -ne 0 ]; then
  # Pull the host from ~/.databrickscfg [$GSO_DATABRICKS_PROFILE] so the
  # `databricks auth login` hint is exact. Fall back to a placeholder if
  # the profile block is missing entirely (in which case the operator
  # needs to add it before re-running).
  DB_HOST=""
  PROFILE_PRESENT="no"
  if [ -f "$HOME/.databrickscfg" ]; then
    DB_HOST=$(awk -v p="$GSO_DATABRICKS_PROFILE" '
      BEGIN { hdr = "[" p "]" }
      $0 == hdr     { in_p = 1; next }
      /^\[/         { in_p = 0 }
      in_p && /^[[:space:]]*host[[:space:]]*=/ {
        sub(/^[^=]*=[[:space:]]*/, "")
        print
        exit
      }
    ' "$HOME/.databrickscfg")
    if grep -qE "^\[${GSO_DATABRICKS_PROFILE}\]" "$HOME/.databrickscfg"; then
      PROFILE_PRESENT="yes"
    fi
  fi
  DB_HOST="${DB_HOST:-<YOUR_WORKSPACE_HOST>}"

  echo "" >&2
  echo "BOOTSTRAP_FAIL: Databricks CLI auth probe failed for profile '$GSO_DATABRICKS_PROFILE'." >&2
  echo "" >&2
  echo "  reason  : $DB_AUTH_OUT" >&2
  echo "" >&2
  if [ "$PROFILE_PRESENT" = "no" ]; then
    echo "  diagnosis: profile [$GSO_DATABRICKS_PROFILE] is NOT in ~/.databrickscfg." >&2
    echo "             Either add it manually, run \`databricks auth login\` with" >&2
    echo "             --profile $GSO_DATABRICKS_PROFILE, or override the profile:" >&2
    echo "               GSO_DATABRICKS_PROFILE=<your-profile> bash $0" >&2
  else
    echo "  fix     : re-authenticate this profile (opens a browser):" >&2
    echo "" >&2
    echo "              databricks auth login --host $DB_HOST --profile $GSO_DATABRICKS_PROFILE" >&2
  fi
  echo "" >&2
  echo "  verify  : after login, run:" >&2
  echo "              databricks --profile $GSO_DATABRICKS_PROFILE current-user me" >&2
  echo "            you should see JSON, not an error." >&2
  echo "" >&2
  echo "  why this matters: every gso-lever-loop-replay call passes" >&2
  echo "  '--profile $GSO_DATABRICKS_PROFILE' to the databricks CLI (per" >&2
  echo "  AGENTS.md §'/goal Harness Contract'). Without a valid token the" >&2
  echo "  /goal run would burn Claude tokens spinning on auth errors." >&2
  echo "" >&2
  exit 6
fi
ok "databricks auth green for profile '$GSO_DATABRICKS_PROFILE'"

# --- 6. Done --------------------------------------------------------------

echo ""
echo "BOOTSTRAP_READY"
echo ""
echo "Next step:"
echo "  bash packages/genie-space-optimizer/scripts/goal_kick.sh next-plan"
echo ""
echo "Other goal keys:"
echo "  A                  end-to-end Goal A (advance funnel on both anchors)"
echo "  B                  end-to-end Goal B (100% accuracy + invariants held)"
echo "  next-plan          execute the topmost in-progress trial plan (recommended)"
echo "  stage:<NAME>       per-funnel-stage micro-goal (e.g. stage:applyable)"
