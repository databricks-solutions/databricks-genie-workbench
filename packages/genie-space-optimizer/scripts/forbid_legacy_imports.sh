#!/usr/bin/env bash
# Reject re-imports of quarantined `_legacy/` modules from non-`_legacy/`
# code under packages/genie-space-optimizer/src/.
#
# Invoked as a PostToolUse Edit|Write hook by Claude Code. See
# packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/05-hook-and-gate-config.md
# for the exact settings.json wiring.
#
# Exit codes:
#   0 — no violation
#   2 — violation present (Claude Code treats >= 2 as a hard reject)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TARGET_DIR="$REPO_ROOT/packages/genie-space-optimizer/src/genie_space_optimizer"

if [ ! -d "$TARGET_DIR" ]; then
  exit 0
fi

if ! command -v rg >/dev/null 2>&1; then
  echo "[forbid_legacy_imports] warn: ripgrep (rg) not found; skipping check" >&2
  exit 0
fi

# Matches any line of the form:
#   from <something>_legacy.<...> import <...>
#   import <something>_legacy.<...>
# but only in files OUTSIDE any path component named "_legacy".
VIOLATIONS=$(
  rg --type py \
     --glob '!**/_legacy/**' \
     'from\s+\S*_legacy[\.\s]|import\s+\S*_legacy[\.\s]' \
     "$TARGET_DIR" \
     2>/dev/null \
  || true
)

if [ -n "$VIOLATIONS" ]; then
  echo "BLOCK: legacy import detected outside _legacy/" >&2
  echo "" >&2
  echo "$VIOLATIONS" >&2
  echo "" >&2
  echo "Rule:    no module under packages/genie-space-optimizer/src/genie_space_optimizer/" >&2
  echo "         may re-import a name from a path with a \"_legacy\" component," >&2
  echo "         unless the importer itself lives under a \"_legacy\" path." >&2
  echo "Source:  packages/genie-space-optimizer/docs/llmdrivenarchitecture/v5/sm-cutover-deletion-first_bbfafbf1.plan.md" >&2
  exit 2
fi

exit 0
