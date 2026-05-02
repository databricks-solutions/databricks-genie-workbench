# Phase A Burn-Down Log

This is the append-only ledger of Phase A burn-down cycles for the airline
benchmark fixture. Every fresh lever-loop run that produces a new
`airline_real_v1.json` adds one row to the table in
[Cycle history](#cycle-history). The CI budget pinned by
`tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget`
is the load-bearing companion: each cycle either holds or tightens it,
never grows it without explicit triage.

Plan: [`2026-05-02-run-replay-per-iteration-fix-plan.md`](./2026-05-02-run-replay-per-iteration-fix-plan.md).

## 2026-05-02 — `run_replay` per-iteration fix (Cycle 7 fixture)

| Phase | Violations | Notes |
|---|---|---|
| Before fix | 328 | ~320 cross-iteration `X -> X` self-transitions (validator was called once on the flattened 5-iteration event list); ~8 intra-iter signal that survived. |
| After fix  | 44  | All `illegal_transition`. Composition by detail: `soft_signal -> already_passing`=30, `evaluated -> post_eval`=5, `clustered -> soft_signal`=5, `applied -> post_eval`=3, `ag_assigned -> rolled_back`=1. |

**Decision:** Documented residuals (44 > 5 plan threshold) and proceeded to
Phase 4.5 (data-capture widening) + Phase 5 (per-cycle runbook). The 44 are
real intra-iteration patterns, not the cross-iter noise that motivated this
plan, but burning each one to zero requires harness-side triage that is
out of scope for the per-iteration fix landing here.

**Initial CI budget:** `BURNDOWN_BUDGET = 44`. Tightens cycle by cycle as
the harness/exporter signal cleans up.

**Top hypotheses** (one line each — to be confirmed in subsequent cycles):
- `soft_signal -> already_passing` (30): replay engine's `_classify_eval_rows`
  partitions a qid as `already_passing` (rc=yes, arbiter=both_correct), but
  the same qid is also listed in `soft_clusters[*].question_ids`, so the
  fixture-soft-promotion at `lever_loop_replay.py:74-82` adds it to the
  `soft` partition without removing it from `already_passing`. Both events
  fire for the same qid in the same iteration.
- `evaluated -> post_eval` (5): qid evaluated with rc=no/arbiter=neither_correct
  (or similar) is bucketed into `hard` but no cluster claims it; harness-side
  emit gap.
- `clustered -> soft_signal` (5): qid is in both a hard cluster and a soft
  cluster within the same iteration; the fixture's cluster boundary is fuzzy.
- `applied -> post_eval` (3): AG outcome event missing for a qid that received
  a patch — strategist response or `ag_outcomes` map is incomplete.
- `ag_assigned -> rolled_back` (1): proposed/applied missing in the path; the
  rollback fired before the patch survived to applied.

Reference: `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` Phase 4.

## Cycle history

| Cycle | Date | Iters | Violations | Composition (by_kind) | Notes |
|---|---|---|---|---|---|
| 7 | 2026-05-02 | 5 | 44 | `{illegal_transition: 44}` | Per-iter validator landed; Track D `_baseline_row_qid` fix in place; cross-iter noise eliminated. Initial CI budget set to 44. |
