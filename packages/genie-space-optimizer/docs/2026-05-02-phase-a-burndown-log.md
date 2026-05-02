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
| 8 | 2026-05-02 | 5 | **0** | `{}` (clean) | **Phase A airline burn-down hard-closed.** Cycle 8 measured 45 violations pre-fix — every single one was the `soft_signal -> already_passing` replay-engine double-emit (9 overlap qids × 5 iters). Fix at `lever_loop_replay.py:80-86` (commit `abd0716`) demotes `already_passing` and `gt_corr` qids when the fixture's `soft_clusters` claim them. Post-fix Cycle 8 measures **0 violations**. Cycle 7 also retroactively dropped from 44 → 11 under the same fix; the residual 11 (5 `evaluated->post_eval`, 5 `clustered->soft_signal`, 1 `ag_assigned->rolled_back`) were genuine harness/exporter emit gaps that Cycle 8's harness-side improvements fully resolved. CI budget tightened from 44 → 0. |

## 2026-05-02 — Replay-engine `soft_signal -> already_passing` fix (Cycle 8 dominant pattern)

The Cycle 7 burn-down log (above) hypothesised that `soft_signal -> already_passing` (30 violations) was a replay-engine bug at `lever_loop_replay.py:74-82` where the fixture-soft-promotion added a qid to the `soft` partition without removing it from `already_passing`. Cycle 8 confirmed the hypothesis exactly: every Cycle 8 violation was this pattern (9 overlap qids × 5 iters = 45). The fix (commit `abd0716`) is a two-line addition to the promotion block:

```python
if fixture_soft_qids:
    soft.update(fixture_soft_qids)
    hard -= fixture_soft_qids
    already_passing -= fixture_soft_qids   # <-- NEW
    gt_corr -= fixture_soft_qids           # <-- NEW (symmetric, defensive)
```

`_classify_eval_rows` returns mutually-exclusive row-level partitions, but a fixture-level soft cluster can claim any qid regardless of its row classification — the cluster wins. Verified by `test_run_replay_demotes_already_passing_when_qid_in_soft_cluster` and the new `synthetic_already_passing_in_soft_cluster.json` fixture.

**Retroactive impact on Cycle 7 baseline:**

| Cycle 7 violation class | Pre-fix | Post-fix | Δ |
|---|---:|---:|---:|
| `soft_signal -> already_passing` | 30 | 0 | −30 |
| `evaluated -> post_eval` | 5 | 5 | 0 |
| `clustered -> soft_signal` | 5 | 5 | 0 |
| `applied -> post_eval` | 3 | 0 | −3 |
| `ag_assigned -> rolled_back` | 1 | 1 | 0 |
| **Total** | **44** | **11** | **−33** |

The unexpected `applied -> post_eval` drop (−3) was the same overlap qids reaching `applied` later in the journey under the old (broken) emit order; eliminating the double-emit fixed both transitions simultaneously.

## 2026-05-02 — L4a: per-iteration `JourneyValidationReport` persistence

Strict-additive widening of the per-iteration data-capture surface; no loop decision-logic changed.

**Persisted to:**
- Replay fixture: `iterations[N].journey_validation = report.to_dict()`
- MLflow artifact: `phase_a/journey_validation/iter_<N>.json` per iteration
- MLflow tags: `journey_validation.iter_<N>.violations`, `journey_validation.iter_<N>.is_valid`

**Deferred (out of scope for L4a):**
- Delta column on `genie_opt_iterations`. Schema migration is invasive; backfill story for historical runs is non-trivial. Re-evaluate when L4b/L4c lands and a real query workload exists.

**Verified by:**
- `tests/unit/test_question_journey_contract.py::test_journey_validation_report_to_dict_round_trip`
- `tests/unit/test_validate_journeys_at_iteration_end.py` (2 tests)
- `tests/unit/test_journey_fixture_exporter.py::test_exporter_passes_journey_validation_field_through`
- `tests/unit/test_journey_fixture_exporter.py::test_exporter_handles_missing_journey_validation_field`
- Cycle 8 intake (Phase 5 Task 14 Step 4) will confirm MLflow artifact + tags appear post-run.

Reference: `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` Phase 4.5.
