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
| 10 | 2026-05-03 | 4 | **46** | `{illegal_transition: 46}` | **REGRESSION — cycle aborted, canonical baseline reverted, no commit.** Source run `opt_run_id 407772af-9662-4803-be6b-f00a368c528a` (job 1036606061019898 / run 526124065145154). 46 violations vs budget 0. Top patterns: `proposed -> proposed` (19) and `applied -> applied` (19) on `gs_009` — AG_DECOMPOSED_H001 emitted 12 alternative `add_join_spec` proposals plus the H001 `add_sql_snippet_measure` proposal in iter 2; the proposal-stage and applied-stage emitters fire once per proposal/patch instead of once per qid, double-firing the journey state. Residual `evaluated -> post_eval` (6, all on `gs_013`/`gs_022`) and `clustered -> soft_signal` (2, on `gs_016`) are the same harness emit gaps that survived Cycle 8's fix on hard qids that re-cluster as soft mid-iteration. **Action:** open a fix branch addressing the proposal/patch-stage de-duplication (cluster the per-proposal events into a single per-qid emit) before re-attempting cycle 10. See `docs/runid_analysis/cycle10_intake.md` and the postmortem at `docs/runid_analysis/407772af-9662-4803-be6b-f00a368c528a/postmortem.md`. |

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

## 2026-05-04 — Control-plane invariant repairs: `assert_soft_cluster_currency`

A 7Now lever-loop run (`opt_run_id 2423b960-16e8-41d4-a0cb-74c563378e05`,
job `195836514612090`) halted on the post-enrichment iteration with:

```
AssertionError: soft-cluster currency drift: currently-passing qids appear
in soft clusters: [gs_001, gs_004, gs_006, gs_008, gs_010, gs_017, gs_018,
gs_022, gs_030]
```

**Symptom.** Nine 7Now qids that were genuinely passing post-enrichment
(`arbiter=both_correct`, `result_correctness=yes`) showed up in a soft
cluster on the same iteration, tripping the invariant.

**Root cause — invariant/semantics mismatch, not stale data.** The helper
asserted `soft_qids ∩ currently_passing_qids == ∅`. The harness call site
defined `currently_passing_qids = _all_eval_qids − _live_hard_qids` (any
qid not in a *hard* cluster). But the soft pile is populated by
`has_individual_judge_failure` (`evaluation.py:143`), whose docstring
explicitly says: "the arbiter rescued the row (or `result_correctness=yes`)
but individual judges still flagged suboptimal patterns." Those qids are,
by construction, simultaneously not-hard (so "passing" by the call site's
definition) and in the soft pile. The replay layer already canonicalizes
this overlap as legal via `synthetic_already_passing_in_soft_cluster.json`
and `test_run_replay_demotes_already_passing_when_qid_in_soft_cluster`.
The runtime assertion was the only disagreeing voice, and on this 7Now
run it fired the moment the strategy lane admitted a soft cluster (the
`clusters_for_strategy` window opens when the hard set is small —
exactly the post-enrichment shape).

The May-01 23:04 7Now reproducer the helper was *originally* meant to
catch (a just-fixed target re-emitted by a clusterer reading stale ASI)
was a different thing: rows where the **fresh** eval shows zero judge
complaints, yet the soft cluster still lists the qid. The old set-
intersection invariant happened to catch that case, and also kept
catching the legitimate Case A pattern as a false positive.

**Fix.** Replaced the invariant with a row-grounded predicate
(`control_plane.py::assert_soft_cluster_currency`):

> Every qid emitted in any soft cluster must, on the same rows the
> clusterer saw, exhibit at least one row where
> `has_individual_judge_failure(row) == True`. If not, the clusterer is
> reading stale ASI / cached rows that no longer reflect the latest eval.

The call site at `harness.py` now plumbs `failure_rows` out of
`_analyze_and_distribute` (new return-dict key) and passes it as
`current_eval_rows`, so the assertion sees the exact rows the soft pile
was built from (no Delta re-read skew). The `_all_eval_qids`,
`_live_hard_qids`, `_live_passing_qids` locals on the soft path are
removed; the audit path (`assert_quarantine_attribution_sound`) still
uses its own `_for_audit` set and is untouched.

`:vN` benchmark-suffix variants are normalized on both sides (mirroring
`_is_quarantined_qid`) via a small `_base_qid` helper.

**Replay impact.** None. The new accept set is a strict super-set of the
old one on the legitimate Case A pattern (the in-tree fixture
`synthetic_already_passing_in_soft_cluster.json` exercises this), and
strictly more discriminating on the original stale-ASI case. The full
replay suite (13 passed, 2 unrelated skips) holds; the airline burndown
budget is unchanged.

**Tests.** Six unit tests in
`tests/unit/test_convergence_quarantine_attribution.py`:

- `test_assert_soft_cluster_currency_rejects_stale_asi_drift` —
  May-01 reproducer (qid in soft cluster, fresh rows clean) raises.
- `test_assert_soft_cluster_currency_accepts_arbiter_rescued_judge_failure` —
  Case A (arbiter rescued, judge=no) is allowed; the 7Now false-positive
  no longer fires.
- `test_assert_soft_cluster_currency_rejects_qid_with_no_row_in_current_eval` —
  source-skew failure mode (qid in soft cluster, no row in current eval).
- `test_assert_soft_cluster_currency_accepts_empty_soft_cluster` —
  short-circuit on no soft clusters.
- `test_assert_soft_cluster_currency_normalizes_vN_qid_suffixes` —
  pins `:vN` normalization.
- `test_assert_soft_cluster_currency_ignores_info_only_judge_failures` —
  `repeatability` / `previous_sql` `no` values do not satisfy the
  invariant.

**Files touched:**
- `src/genie_space_optimizer/optimization/control_plane.py` —
  rewrote `assert_soft_cluster_currency`, added `_base_qid`.
- `src/genie_space_optimizer/optimization/harness.py` — added
  `failure_rows` to `_analyze_and_distribute` return, updated call site
  to use the new signature, removed soft-path passing-set derivation.
- `tests/unit/test_convergence_quarantine_attribution.py` — replaced
  the two old tests with six row-grounded ones.

Reference: planned in
`.cursor/plans/fix_soft-cluster_currency_invariant_*.plan.md`. Tagged
`control-plane invariant repairs` for the iteration ledger.

## 2026-05-04 — Cycle 2: Proposal survival and safety-gate hardening

Inspired by airline run `2afb0be2-88b6-4832-99aa-c7e78fbc90f7` which
exhausted 5 iterations at 87.5% with zero accepted action groups,
despite generating directionally-causal patches every iteration.

| Defect | Stage | Fix | Flag (default ON) | Test file |
|---|---|---|---|---|
| Intra-AG body duplicates with mismatched `patch_type` | proposal generation / gates | New `_run_intra_ag_dedup` keyed on body-only fingerprint | `GSO_INTRA_AG_PROPOSAL_DEDUP` | `tests/unit/test_intra_ag_proposal_dedup.py` |
| Shared-cause hard failures classified as collateral | `proposal_grounding.patch_blast_radius_is_safe` | `shared_cause_collateral_warning` downgrade when `outside ⊆ live_hard_qids` | `GSO_SHARED_CAUSE_BLAST_RADIUS` | `tests/unit/test_blast_radius_shared_cause.py` |
| DOA ledger blind to empty applied-patch signatures | `harness._record_dead_on_arrival_signature` | Parallel ledger keyed on selected-proposal-ID signatures | `GSO_DOA_SELECTED_PROPOSAL_SIGNATURE` | `tests/unit/test_doa_selected_proposal_signature.py` |
| Single-question shape RCAs routed to space-wide lever 6 | `stages.action_groups` | `recommended_levers_for_cluster` returns `(3, 5)` when `q_count==1` and root_cause ∈ `_QUESTION_SHAPE_ROOT_CAUSES` | `GSO_QUESTION_SHAPE_LEVER_PREFERENCE` | `tests/unit/test_question_shape_lever_preference.py` |

**Replay impact:** none — every change is flag-gated default-on but
byte-equivalent on the airline replay fixture (which does not
exercise the affected paths in a way that changes its output shape).
Replay byte-stability gate (`tests/replay/test_phase_f_h_wireup_byte_stable.py`)
stays green across every commit.

**Plan:** [`2026-05-04-cycle-2-optimizer-improvement-plan.md`](./2026-05-04-cycle-2-optimizer-improvement-plan.md).
**Iteration ledger:** Cycle 2 in
[`2026-05-05-optimizer-iteration-ledger.md`](./2026-05-05-optimizer-iteration-ledger.md).
