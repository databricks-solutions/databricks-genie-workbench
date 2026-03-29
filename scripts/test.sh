#!/usr/bin/env bash
# Run the backend test suite.
# Usage: ./scripts/test.sh [pytest args...]
#   ./scripts/test.sh            # run all tests
#   ./scripts/test.sh -v         # verbose
#   ./scripts/test.sh -k scanner # run only scanner tests

set -euo pipefail
cd "$(dirname "$0")/.."

# Install dev deps if pytest is not available
PYTHON="${PYTHON:-python3}"

if ! "$PYTHON" -c "import pytest" 2>/dev/null; then
    echo "Installing dev dependencies..."
    uv pip install -e ".[dev]" --system --quiet
fi

"$PYTHON" -m pytest "$@"
