#!/usr/bin/env bash
# Run the offline unit suites: backend/tests + packages/genie-space-optimizer/tests.
# Usage: ./scripts/test.sh [pytest args...]
#   ./scripts/test.sh                          # run both suites
#   ./scripts/test.sh -v                       # verbose
#   ./scripts/test.sh -k scanner               # run only scanner tests
#   ./scripts/test.sh backend/tests            # run one suite
#
# Runs through `uv run --frozen --extra dev` so the lockfile is enforced and the
# dev extra is present. The `--extra dev` is NOT optional: the root pytest config
# sets `asyncio_mode = "auto"`, which needs pytest-asyncio. Without the extra,
# pytest warns "Unknown config option: asyncio_mode" and every async backend test
# fails on an uncollectable coroutine — 12 failures that look like code defects
# and are not. Invoke the suites through this script rather than calling pytest
# directly, so that flag cannot be forgotten.

set -euo pipefail
cd "$(dirname "$0")/.."

# Root pyproject pins testpaths to backend/tests only, so name both suites unless
# the caller chose paths of their own. Keyed on "did an argument name something
# that exists on disk", not on "were there any arguments" — otherwise a bare flag
# like -q suppresses the defaults and silently runs the backend suite alone.
caller_chose_paths=0
for arg in "$@"; do
    if [ -e "$arg" ]; then
        caller_chose_paths=1
        break
    fi
done

# Expanded as two separate branches rather than an array that may be empty:
# bash 3.2 (still the /bin/bash on macOS) treats "${empty[@]}" as an unbound
# variable under `set -u`.
if [ "$caller_chose_paths" -eq 1 ]; then
    exec uv run --frozen --extra dev python -m pytest "$@"
fi
exec uv run --frozen --extra dev python -m pytest \
    backend/tests packages/genie-space-optimizer/tests "$@"
