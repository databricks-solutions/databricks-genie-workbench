#!/usr/bin/env bash
# Pre-trial gate for the GSO /goal harness.
#
# Runs the seven offline pytest checks documented in
# packages/genie-space-optimizer/docs/architecture/sm-cutover-trial-protocol.md
# §"Pre-trial local checks". Non-zero exit blocks any deploy initiated
# through Claude Code (via PreToolUse hook).
#
# Modes:
#   no args        : always run the full gate.
#   --hook-mode    : only run the gate if $CLAUDE_TOOL_INPUT contains
#                    the literal substring "deploy.sh"; otherwise exit
#                    0 immediately. Used by the PreToolUse hook so it
#                    can match all `Bash` invocations cheaply.
#   --list         : print the seven tests and exit.
#
# This script is intentionally dependency-free apart from `uv` and pytest.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
GSO_DIR="$REPO_ROOT/packages/genie-space-optimizer"

TESTS=(
  "tests/unit/test_eval_row_access_handles_production_shapes.py"
  "tests/integration/test_databricks_request_contract_golden.py"
  "tests/integration/test_sm_tape_replay_diagnose_success.py"
  "tests/integration/test_sm_forward_pipeline_to_proposed.py"
  "tests/integration/test_sm_forward_pipeline_failure_modes.py"
  "tests/integration/test_sm_forward_pipeline_to_normalized.py"
  "tests/integration/test_sm_forward_pipeline_to_applyable.py"
)

case "${1:-}" in
  --list)
    printf 'pretrial_gate.sh tests:\n'
    for t in "${TESTS[@]}"; do printf '  %s\n' "$t"; done
    exit 0
    ;;
  --hook-mode)
    # Cheap path for the PreToolUse Bash hook: only run the gate if the
    # tool input looks like a deploy invocation. Match a few common
    # spellings.
    INPUT="${CLAUDE_TOOL_INPUT:-}"
    case "$INPUT" in
      *deploy.sh*) ;;  # fall through to the full gate
      *) exit 0 ;;
    esac
    ;;
esac

echo "[pretrial_gate] running 7 offline tests under $GSO_DIR" >&2

cd "$GSO_DIR"

if ! command -v uv >/dev/null 2>&1; then
  echo "[pretrial_gate] FAIL: uv not found on PATH" >&2
  exit 3
fi

# Run all seven tests in a single pytest invocation. Quiet output; short
# traceback to keep transcript noise low; -x to stop on the first
# failure so the surfaced error is the actionable one.
if uv run pytest "${TESTS[@]}" -q --tb=short -x; then
  echo "[pretrial_gate] PASS — 7/7 offline checks green" >&2
  exit 0
fi

EXIT=$?
echo "[pretrial_gate] FAIL — one of the 7 offline checks is red (exit $EXIT)" >&2
echo "[pretrial_gate]   See packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/05-hook-and-gate-config.md" >&2
exit "$EXIT"
