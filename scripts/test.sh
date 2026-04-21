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

if ! "$PYTHON" -c "import pytest, genie_space_optimizer" 2>/dev/null; then
    echo "Installing dev dependencies..."
    # Root package pulls in genie-space-optimizer via [tool.uv.sources] when
    # resolved through `uv sync`, but `uv pip install -e .` can't see the
    # workspace mapping — install the workspace member explicitly so
    # `from genie_space_optimizer...` imports resolve under --system.
    uv pip install -e ".[dev]" -e packages/genie-space-optimizer --system --quiet
fi

"$PYTHON" -m pytest "$@"
