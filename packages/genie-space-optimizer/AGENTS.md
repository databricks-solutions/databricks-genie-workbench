# AGENTS.md — genie-space-optimizer

The `genie-space-optimizer` (GSO) package is the Auto-Optimize engine for Genie Workbench.
It is a self-contained FastAPI + React app that runs as both a Databricks App and a
Databricks Job (the benchmark-driven optimization pipeline).

For top-level project context see the root `AGENTS.md` and `README.md`.

## Build Commands

### Python

```bash
uv sync --frozen                  # Install deps from uv.lock (strict)
uv build                          # Build the distributable wheel
uv run pytest                     # Run tests
```

### Frontend

```bash
npm ci                            # Install from package-lock.json (strict)
npm run build                     # Production build
```

> This package standardizes on **npm** (matches the repo-wide convention
> used by `frontend/` and `scripts/build.sh`). An older Bun lockfile was
> removed in PR #79. Don't reintroduce one — either add new dependencies
> via `npm install --save-exact` or open a discussion first.

## Package Layout

```
src/genie_space_optimizer/
  backend/              # FastAPI app for the GSO service
  optimization/         # Benchmark-driven optimization pipeline (6 stages)
  jobs/                 # Databricks Job notebooks/tasks
  ui/                   # React frontend (Vite + npm)
  genie_optimizer_skills/ # Excluded from type-checking (see pyproject.toml)
```

## Shared Modules (imported by the Workbench backend)

`common/prompt_registry.py` is the single source of truth for the MLflow
Prompt Registry probe used by both `/permissions` (read) and preflight
(write). Two invariants live here:

1. **Probe–workload parity.** The read probe MUST call the same MLflow
   SDK symbol the job uses (`mlflow.genai.search_prompts`). Do NOT call
   `WorkspaceClient.api_client.do("GET", "/api/2.0/mlflow/...")`. When
   Databricks moves the endpoint, MLflow ships the fix and our probe
   tracks it automatically.
2. **Closed-world classifier.** Any `error_code` we haven't explicitly
   mapped becomes `reason=vendor_bug / actionable_by=platform`. Adding a
   new code is a one-line entry in `_classify_exception` (or the
   `_VENDOR_BUG_CODES` / `_SCOPE_CODES` frozensets) + a matching branch
   in `PermissionAlert.tsx` + a test. See the runbook in the root
   `AGENTS.md` ("Runbook — new Prompt Registry error code appears in
   prod").

## Key Differences from Root Package

- Uses **Bun** (not npm) for the frontend
- Python deps resolved from `https://pypi-proxy.dev.databricks.com/simple/` (internal
  Databricks PyPI proxy), not raw PyPI — this reduces (but does not eliminate) supply
  chain risk
- Dynamic versioning via `uv-dynamic-versioning` (reads from git tags)
- The `_metadata.py` file is generated at build time and gitignored — it is produced
  by the bundle's `artifacts.default.build` step and then picked up by sync

## Dependency Security Policy

All dependencies are pinned to exact versions. Lock files must be committed.

**To update a Python dependency:**

```bash
uv lock --upgrade-package <package-name>
git add uv.lock
```

**To update a Bun dependency:**

```bash
npm install <package>@<version> --save-exact
# package.json must record the exact version (no ^ or ~)
git add package.json package-lock.json
```

## Testing

```bash
uv run pytest                           # Python unit tests (pyproject.toml)
npx vitest run                          # UI helper unit tests (vitest.config.ts)
```

### Auto-Optimize invariant tests (Bug #1, #2, #3)

Three customer-visible contracts are protected by dedicated tests. See the
root `AGENTS.md` section "Auto-Optimize Invariants" for the full contract.
Before touching any of these files, run the corresponding tests:

| Contract | Implementation files | Tests |
|---|---|---|
| **Bug #1** — Prompt Registry gate | `common/prompt_registry.py`, `backend/routes/trigger.py`, `jobs/run_preflight.py`, `optimization/preflight.py` | `tests/unit/test_prompt_registry_probe.py` |
| **Bug #2** — `evaluated_count` denominator | `optimization/evaluation.py` (`ArbiterAdjustedResult`), `optimization/ddl.py`, `optimization/state.py` (`write_iteration`), `backend/routes/runs.py` (`_resolve_eval_counts`) | `tests/unit/test_arbiter_adjusted_accuracy.py`, `tests/unit/test_iteration_api_contract.py`, `tests/unit/test_write_iteration_schema.py` |
| **Bug #3** — Stable exclusion reason codes | `optimization/evaluation.py` (`EXCLUSION_*` constants + `RowExclusion`), `ui/lib/transparency-api.ts` (`ExclusionReasonCode`), `ui/lib/exclusions.ts` (`humanizeExclusionReason`) | `tests/unit/test_arbiter_adjusted_accuracy.py::test_exclusions_carry_stable_reason_codes`, `src/genie_space_optimizer/ui/lib/exclusions.test.ts` |

**When adding a new exclusion reason code:**
1. Add the constant in `optimization/evaluation.py` (Python)
2. Extend `ExclusionReasonCode` union in `ui/lib/transparency-api.ts` (TS)
3. Add a `case` in `humanizeExclusionReason` in `ui/lib/exclusions.ts`
4. Add coverage to `test_arbiter_adjusted_accuracy.py` and `exclusions.test.ts`
5. The server and UI must ship the new code in the same PR — otherwise old
   clients render a raw snake_case string (acceptable fallback, not ideal).

**When adding a new iteration-count field:**
1. Add the column to `optimization/ddl.py` AND `_migrate_add_columns` in `state.py`
2. Plumb through `write_iteration()`
3. Extend `_resolve_eval_counts()` in `backend/routes/runs.py`
4. Mirror on both Pydantic (`backend/models.py`) and TS (`ui/lib/transparency-api.ts`) models
5. Add a test in `test_write_iteration_schema.py` AND `test_iteration_api_contract.py`
