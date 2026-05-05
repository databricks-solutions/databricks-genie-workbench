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

### Auto-Optimize invariant tests (Bug #1, #2, #3, #4)

Four customer-visible contracts are protected by dedicated tests. See the
root `AGENTS.md` section "Auto-Optimize Invariants" for the full contract.
Before touching any of these files, run the corresponding tests:

| Contract | Implementation files | Tests |
|---|---|---|
| **Bug #1** — Prompt Registry gate | `common/prompt_registry.py`, `backend/routes/trigger.py`, `jobs/run_preflight.py`, `optimization/preflight.py` | `tests/unit/test_prompt_registry_probe.py` |
| **Bug #2** — `evaluated_count` denominator | `optimization/evaluation.py` (`ArbiterAdjustedResult`), `optimization/ddl.py`, `optimization/state.py` (`write_iteration`), `backend/routes/runs.py` (`_resolve_eval_counts`) | `tests/unit/test_arbiter_adjusted_accuracy.py`, `tests/unit/test_iteration_api_contract.py`, `tests/unit/test_write_iteration_schema.py` |
| **Bug #3** — Stable exclusion reason codes | `optimization/evaluation.py` (`EXCLUSION_*` constants + `RowExclusion`), `ui/lib/transparency-api.ts` (`ExclusionReasonCode`), `ui/lib/exclusions.ts` (`humanizeExclusionReason`) | `tests/unit/test_arbiter_adjusted_accuracy.py::test_exclusions_carry_stable_reason_codes`, `src/genie_space_optimizer/ui/lib/exclusions.test.ts` |
| **Bug #4** — Benchmark leakage firewall | `optimization/leakage.py`, `optimization/afs.py` (P2), `optimization/optimizer.py` (`_DEPRECATED_mine_benchmark_example_sqls_verbatim`, `_resolve_lever5_llm_result`, `_BUG4_COUNTERS`), `optimization/harness.py` (`_apply_proactive_example_sqls`, `_seed_new_sql_snippets`, `_merge_bug4_counters`), `common/genie_client.py` (`publish_benchmarks_to_genie_space`) | `tests/unit/test_leakage_firewall.py` |

#### Bug #4 — benchmark leakage invariants

The optimizer is evaluated on a held-out benchmark corpus. Historically
two paths copied those benchmarks' `expected_sql` into the space being
evaluated, contaminating the training signal and inflating reported
accuracy:

1. Primary: `_mine_benchmark_example_sqls` in `optimization/optimizer.py`
   iterated benchmarks and emitted `add_example_sql` proposals with the
   verbatim `expected_sql`. **Closed** — function renamed to
   `_DEPRECATED_mine_benchmark_example_sqls_verbatim` and gated behind
   `GSO_ALLOW_VERBATIM_MINING=1`. All call sites removed.
2. Secondary: `_resolve_lever5_llm_result` forced an `add_example_sql`
   patch using `representative["question"]` / `representative["expected_sql"]`
   when the LLM returned `text_instruction` for a SQL-pattern root cause.
   **Closed** — the verbatim-copy branch now just bumps the
   `secondary_mining_blocked` counter and falls through to the regular
   text-instruction path.

The firewall in `optimization/leakage.py` is the last-mile guard on every
write path. Before adding a new path that persists inference-visible
content:

1. Decide whether the new patch type should be in `_PATCH_TEXT_FIELDS`
   (it should if its text fields can plausibly echo benchmark content).
2. Wire `is_benchmark_leak(proposal, patch_type, corpus)` at the write
   boundary. On rejection call `_incr_bug4_counter("firewall_rejections")`
   and suppress the proposal.
3. Add a parametrized case to `test_firewall_per_patch_type` in
   `tests/unit/test_leakage_firewall.py`.

The full test suite covers: canonicalized-SQL fingerprint matches,
n-gram near-verbatim catches, per-patch-type shape dispatch,
empty-corpus safety, the deprecated-mining runtime error, the closed
secondary-mining path, and the `publish_benchmarks_to_genie_space` merge
+ dedup + `[auto-optimize]` tagging + example-sql mirror guard.

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

## IQ Scan Integration (Epic #179)

The Optimizer consumes IQ Scan signals at two phases of a run and persists a
pair of snapshots for post-hoc delta analysis. Implementation lives in
`iq_scan/` (pure scoring) and `optimization/scan_snapshots.py` (UC Delta
persistence). The backend `scanner` service now delegates to
`iq_scan.scoring.calculate_score` to avoid cross-package coupling.

### Preflight sub-step — `preflight_run_iq_scan`

Inserted between `preflight_fetch_config` and `preflight_collect_uc_metadata`
in `optimization/preflight.py`. Behavior:

- **Check 1 (data sources exist) hard-blocks** the run with a user-friendly
  `RuntimeError` before any UC metadata work is done.
- **Check 10 (10+ benchmark questions) warn-only.** `MIN_VALID_BENCHMARKS = 5`
  at `optimization/preflight.py` is the authoritative post-validation gate;
  synthetic benchmark generation still tops up fresh spaces.
- Writes `phase='preflight'` to `genie_opt_scan_snapshots` BEFORE the
  hard-block so failed preflights are auditable.
- Computes `recommended_levers` = union of the CTA-supplied lever list
  (from the "Fix with Optimization" deep-link) and levers implied by
  failing/warning checks via `SCAN_CHECK_TO_LEVERS` in
  `common/config.py`.

### Strategist signals (four narrow categories)

The strategist sees a condensed summary — not the full 12-check result — via
`iq_scan_findings` in `_build_context_data`:

1. `score` / `maturity` — readiness headline
2. `ceilings` — space-wide warnings (data-source count > 12, entity-matching
   approaching the 120/space cap, text-instruction length)
3. `rls_tables` — tables flagged as RLS-governed (entity matching is
   silently disabled on these, so the strategist should prefer other levers)
4. `coverage_gaps` — short list of failing check findings
5. `recommended_levers` — lever IDs implied by failing/warning checks

IQ scan findings are **prompt context, not proposals** — the Bug #4 benchmark
leakage firewall does not apply.

### Ranking tiebreaker — `rank_clusters`

`optimization/optimizer.py::rank_clusters` accepts an optional
`recommended_levers: set[int] | None`. When two clusters have impact scores
within `_RANK_TIEBREAK_THRESHOLD = 1.0`, the cluster whose implied lever
(derived via `_map_to_lever`) overlaps with `recommended_levers` wins.
Absolute ranking by impact score is preserved outside the threshold.

### Postflight hook

`run_postflight_scan` in `optimization/scan_snapshots.py` fires immediately
before each terminal status write in `harness.py` (CONVERGED, STALLED,
MAX_ITERATIONS). Soft-failing: every exception is caught and logged so a
failed scan never blocks the terminal status write. Writes
`phase='postflight'` keyed on `(run_id, phase)` so retries are idempotent.

### Feature flags + rollout

Two env vars gate the integration:

| Flag | Scope | Default |
|---|---|---|
| `GSO_ENABLE_IQ_SCAN_PREFLIGHT` | Preflight sub-step + postflight hook | `false` |
| `GSO_ENABLE_IQ_SCAN_STRATEGIST` | Strategist context block + tiebreaker | `false` |

Both parse the same truthy shape as the rest of the package
(`{"1","true","yes","on"}`, case-insensitive). Postflight deliberately shares
the preflight flag — a postflight row without a matching preflight row would
break the delta view.

**Four-release rollout sequence:**

1. **Release N.** All 7 PRs merged. Both flags default `false`. Behavior
   identical to today. Unit and parity tests verify no regression.
2. **Release N+1.** `GSO_ENABLE_IQ_SCAN_PREFLIGHT=true` by default after
   observing scan latency (`PREFLIGHT_IQ_SCAN_COMPLETE` stage timing) for
   ≥1 week. At this point preflight Check 1 hard-blocks and the snapshot
   table starts filling with preflight+postflight pairs. Strategist flag
   stays off — the Strategist LLM still sees the same 13 context blocks as
   today.
3. **Release N+2.** `GSO_ENABLE_IQ_SCAN_STRATEGIST=true` by default after
   A/B comparing strategist lever-selection accuracy with and without the
   new context block + tiebreaker. The 14th context block (`iq_scan_findings`)
   becomes part of the strategist prompt and the tiebreaker activates.
4. **Release N+3.** Remove both flags entirely. `calculate_score` runs
   unconditionally on every run.

### Files owning the contract

| File | Role |
|---|---|
| `iq_scan/scoring.py` | Pure scoring — shared with backend scanner via delegation |
| `common/config.py` | `SCAN_CHECK_TO_LEVERS`, `TABLE_SCAN_SNAPSHOTS` |
| `optimization/scan_snapshots.py` | UC Delta writer + postflight helper |
| `optimization/preflight.py` | `preflight_run_iq_scan` sub-step |
| `optimization/optimizer.py` | `_format_iq_scan_findings`, `_build_context_data` hook, `rank_clusters` tiebreaker |
| `optimization/harness.py` | Plumbs `iq_scan_recommended_levers` + `iq_scan_summary` through `_run_preflight` → `_run_lever_loop`; fires postflight hook |
| `optimization/applier.py` | `_table_has_rls` / `_column_has_rls` — skips entity matching on RLS-governed tables so the scan's RLS warning matches the applier's actual behavior |

### Out of scope for this package (tracked by other teams)

- Frontend Fix-Agent UI removal and the "Fix with Optimization" CTA
  (owned by the frontend team; GSO only exposes the backend contract).
- Run-detail pre/post IQ delta widget (GSO guarantees two rows per run;
  the widget itself is a frontend deliverable).
- Repo-root `docs/` audit and the fate of `docs/06-fix-agent.md`.
- End-to-end validation in `tests/test_e2e_deployed.py`.
