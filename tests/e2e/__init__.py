# MV Advisor live E2E suite (Prompt 15). Opt-in, env-gated, serialized.
# This package is intentionally OUTSIDE the offline testpaths (backend/tests +
# packages/genie-space-optimizer/tests), so neither the default pytest run nor
# ./scripts/test.sh collects it and the 638+1452 offline baseline is untouched.
# Run it explicitly: `uv run --frozen --extra dev pytest -m e2e tests/e2e`.
