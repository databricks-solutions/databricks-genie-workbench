# Changelog — genie-space-optimizer

All notable GSO-package changes should be logged here. Follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The package is
versioned dynamically from git tags (`uv-dynamic-versioning`), so entries
here group by release rather than by explicit version number.

## [Unreleased]

### Added — IQ Scan integration (Epic #179)

- **`iq_scan/` sub-package** — moves `calculate_score` and helpers
  (`get_maturity_label`, `_check`, `_SQL_IN_TEXT_RE`, `CONFIG_CHECK_COUNT`)
  out of `backend/services/scanner.py` and into
  `iq_scan/scoring.py` so the optimizer can import scoring without a
  cross-package dependency on `backend.services.lakebase`. The backend
  scanner is now a thin wrapper that re-exports these symbols.
- **`optimization/scan_snapshots.py`** — new `genie_opt_scan_snapshots` UC
  Delta table + `write_scan_snapshot` writer keyed idempotently on
  `(run_id, phase)`, plus `run_postflight_scan` helper used by the harness
  hook.
- **`common/config.py`** — adds `TABLE_SCAN_SNAPSHOTS` constant and
  `SCAN_CHECK_TO_LEVERS` mapping (failing IQ-Scan checks → recommended
  optimizer levers).
- **`optimization/preflight.py`** — new `preflight_run_iq_scan` sub-step,
  inserted between `preflight_fetch_config` and
  `preflight_collect_uc_metadata`. Hard-blocks on Check 1 (data sources
  exist), warns on Check 10 (10+ benchmark questions — `MIN_VALID_BENCHMARKS`
  remains the authoritative gate), persists a `phase='preflight'` snapshot,
  and emits a narrowed four-signal summary for the strategist.
  Flag-gated by `GSO_ENABLE_IQ_SCAN_PREFLIGHT` (default `false`).
- **`optimization/optimizer.py`** — new `_format_iq_scan_findings` helper,
  a 14th context block (`iq_scan_findings`) threaded into
  `_build_context_data` and `_call_llm_for_adaptive_strategy`, and a
  `recommended_levers` tiebreaker inside `rank_clusters` (within
  `_RANK_TIEBREAK_THRESHOLD = 1.0`). Flag-gated by
  `GSO_ENABLE_IQ_SCAN_STRATEGIST` (default `false`).
- **`optimization/harness.py`** — plumbs `iq_scan_recommended_levers` and
  `iq_scan_summary` from `_run_preflight` through `_run_lever_loop` to
  `rank_clusters` and `_call_llm_for_adaptive_strategy`. Fires
  `run_postflight_scan` immediately before each terminal status write
  (CONVERGED / STALLED / MAX_ITERATIONS). Soft-fail — the postflight scan
  never blocks the terminal status write.
- **`ui/components/how-it-works/stages/FailureAnalysisStage.tsx`** — adds
  the 14th context-block tile (`iq_scan_findings`) to the strategist
  context-block grid.

### Fixed

- **RLS-aware entity matching** in `optimization/applier.py`
  (`auto_apply_prompt_matching`). New `_table_has_rls` / `_column_has_rls`
  helpers detect row-level-security or column-mask governance at the table
  and column level; `enable_entity_matching` is skipped on governed columns
  where the platform silently disables it anyway. `enable_format_assistance`
  is still applied where applicable. Each skipped column logs at INFO. This
  aligns the applier's behavior with the IQ-Scan's RLS warning.

### Rollout sequence

Four-release flag flip (see `AGENTS.md` → "IQ Scan Integration" for full
context):

1. **Release N.** All 7 PRs merged. Both flags default `false`. No user-
   visible change.
2. **Release N+1.** `GSO_ENABLE_IQ_SCAN_PREFLIGHT=true` by default after
   observing `PREFLIGHT_IQ_SCAN_COMPLETE` stage timing for ≥1 week. Scan
   snapshots begin filling.
3. **Release N+2.** `GSO_ENABLE_IQ_SCAN_STRATEGIST=true` by default after
   A/B validation. The strategist consumes the new context block and the
   rank-cluster tiebreaker activates.
4. **Release N+3.** Remove flags entirely.
