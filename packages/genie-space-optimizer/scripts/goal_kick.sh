#!/usr/bin/env bash
# goal_kick.sh — single-command /goal launcher for the GSO harness.
#
# Calls goal_bootstrap.sh (idempotent), resolves the requested goal key
# to its condition file under
# packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/conditions/,
# and launches `claude -p "/goal <condition>"`.
#
# Usage:
#   bash packages/genie-space-optimizer/scripts/goal_kick.sh <goal-key> [--background] [--print-only]
#
# goal-key values:
#   next-plan          execute the topmost in-progress trial plan (recommended)
#   A                  end-to-end Goal A (advance funnel on both anchors)
#   B                  end-to-end Goal B (100% accuracy + invariants held)
#   stage:<NAME>       per-funnel-stage micro-goal
#                      where <NAME> ∈ {diagnosed, clustered, proposed,
#                                      normalized, applyable, applied,
#                                      evaluated, accepted}
#
# Flags:
#   --background       launch via nohup, redirect output to a timestamped
#                      log file under /tmp, and return the pid. Useful for
#                      multi-hour goal runs without keeping a terminal open.
#   --print-only       print the resolved `claude -p ...` command without
#                      running it. Useful for inspection.
#
# Exit codes:
#   0     foreground/background launch succeeded
#   2     bad usage (unknown goal-key or flag)
#   3     missing condition file (likely a malformed stage:<NAME>)
#   4     bootstrap failed; nothing was launched
#   *     other (claude exit code in foreground mode)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

usage() {
  # Print the leading comment block (everything from line 2 up to the
  # first non-comment line). This keeps usage in sync with the file
  # header without us having to maintain a second copy.
  awk '
    NR == 1 { next }
    /^#/    { sub(/^# ?/, ""); print; next }
    { exit }
  ' "$0"
}

if [ "$#" -lt 1 ] || [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

GOAL_KEY="$1"
shift || true

MODE="foreground"
for arg in "$@"; do
  case "$arg" in
    --background) MODE="background" ;;
    --print-only) MODE="print" ;;
    *)
      echo "ERROR: unknown flag: $arg" >&2
      usage
      exit 2
      ;;
  esac
done

CONDITIONS_DIR="$REPO_ROOT/packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/conditions"

case "$GOAL_KEY" in
  next-plan)
    FILE="$CONDITIONS_DIR/goal-execute-next-plan.txt"
    ;;
  A)
    FILE="$CONDITIONS_DIR/goal-A-advance-funnel.txt"
    ;;
  B)
    FILE="$CONDITIONS_DIR/goal-B-end-state-accuracy.txt"
    ;;
  stage:*)
    STAGE="${GOAL_KEY#stage:}"
    FILE="$CONDITIONS_DIR/micro-goal-stage-${STAGE}.txt"
    ;;
  *)
    echo "ERROR: unknown goal-key: $GOAL_KEY" >&2
    usage
    exit 2
    ;;
esac

if [ ! -f "$FILE" ]; then
  echo "ERROR: condition file not found: $FILE" >&2
  echo "       (available files in $CONDITIONS_DIR:)" >&2
  ls -1 "$CONDITIONS_DIR" | sed 's/^/         /' >&2
  exit 3
fi

cat <<EOF
=== /goal kick ===
goal key:       $GOAL_KEY
condition file: $FILE
mode:           $MODE
repo root:      $REPO_ROOT

EOF

# Always bootstrap first (idempotent, ~3s).
echo "=== bootstrap ==="
if ! bash "$(dirname "$0")/goal_bootstrap.sh"; then
  echo "ERROR: bootstrap failed; not launching /goal" >&2
  exit 4
fi
echo ""

CONDITION="$(cat "$FILE")"

case "$MODE" in
  print)
    echo "=== resolved command (not executed) ==="
    printf 'claude -p %q\n' "/goal $CONDITION"
    ;;
  foreground)
    echo "=== launching /goal interactively ==="
    echo "  (Ctrl+C to interrupt; the goal will resume on next \`claude --resume\`)"
    echo ""
    exec claude -p "/goal $CONDITION"
    ;;
  background)
    SAFE_KEY="${GOAL_KEY//[\/:]/_}"
    LOG="/tmp/goal-${SAFE_KEY}-$(date +%Y%m%d-%H%M%S).log"
    echo "=== launching /goal in background ==="
    echo "  log file: $LOG"
    nohup claude -p "/goal $CONDITION" --output-format json \
      >"$LOG" 2>&1 &
    PID=$!
    disown "$PID" 2>/dev/null || true
    echo "  pid:      $PID"
    echo ""
    echo "Follow progress:    tail -F $LOG"
    echo "Stop the run:       kill $PID"
    ;;
esac
