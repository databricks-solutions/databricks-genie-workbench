# Phase A — Contract Burn-Down Close Summary

## Summary

Phase A (journey-contract burn-down + real-run replay fixture) is closed.
The airline corpus's journey-contract validation count was burned from
**328 violations** (cycle 7, on the original flat-events `run_replay`)
through **44 violations** (cycle 7, after the per-iteration validator fix),
through **45 violations** (cycle 8, raw — same dominant pattern),
to **0 violations** (cycle 8, after the soft-cluster demotion fix).

`airline_real_v1.json` is committed as the byte-stable regression target
for Phases B/C/D, with `expected_canonical_journey` pinning a 365-event
canonical ledger.

## Cycles

| Cycle | Date | Iters | Pre-fix violations | Notes |
|---|---|---|---|---|
| 1–6 | (Phase A pre-burn-down) | various | n/a | Each cycle was a different fixture-shape bug; see `2026-05-02-cycle7-reconstruction-postmortem.md` for the per-cycle root-cause table. Cumulatively spent ~14 hours of real-Genie wall clock. |
| 7 | 2026-05-02 | 5 | 328 → 44 | Per-iteration validator landed (fix `2db6fcd`), eliminating ~320 cross-iteration `X -> X` self-transitions. 44 residuals were real intra-iter signal. Track D `_baseline_row_qid` fix already in place. Initial CI budget set to 44. |
| 8 | 2026-05-02 | 5 | **45 → 0** | Soft-cluster demotion fix (commit `abd0716`) at `lever_loop_replay.py:80-86`. Eliminated the dominant `soft_signal -> already_passing` double-emit pattern (9 overlap qids × 5 iters = 45). Cycle 7 also dropped from 44 → 11 retroactively under the same fix; the residual 11 patterns were resolved by cycle 8's harness-side improvements. **CI budget tightened from 44 → 0 — Phase A airline burn-down hard-closed.** |

See [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md)
for the per-cycle technical detail (violation histograms, fix diffs,
retroactive impact tables).

## Wirings applied during Phase A

| Helper | Hook point | Eliminates |
|---|---|---|
| `_emit_eval_entry_journey` | After `_analyze_and_distribute(...)` | All `missing_qid` violations |
| `_emit_ag_assignment_journey` | Before `_journey_emit("proposed", ...)` | `clustered → proposed` illegal-transition |
| `_emit_gate_drop_journey` (×5) | After grounding, normalize, applyability, alignment, reflection gates | `proposed → post_eval` illegal-transitions for dropped proposals |
| `_emit_ag_outcome_journey` | At `_audit_emit(... gate_name="full_eval_acceptance" ...)` accept and rollback paths | `applied → post_eval` illegal-transitions |
| `_emit_post_eval_journey` | Before `_validate_journeys_at_iteration_end` at iteration end | All `no_terminal_state` violations |

## Key fixes landing in this PR

| Commit | Subject | Layer |
|---|---|---|
| `d9f8caa` | `fix(gso): _baseline_row_qid prefers canonical qid sources over trace IDs` | Track D — eval-row qid extractor |
| `2db6fcd` | `fix(replay): validate_question_journeys per iteration, not flattened` | Replay engine — per-iter validation |
| `abd0716` | `fix(replay): demote already_passing/gt_corr qids when in soft_clusters` | Replay engine — soft-cluster classification authority |
| `a8320a5` | `feat(replay): recognize skipped_no_applied_patches and skipped_dead_on_arrival` | Replay engine — cycle 8 forward-compat |

## Fixture provenance

| Field | Value |
|---|---|
| Path | `packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json` |
| Captured during | Cycle 7 (reconstructed from MLflow trace tags via `notebooks/reconstruct_cycle7_fixture.py`) |
| Fixture ID | `airline_real_v1_run_8b121949-a3dd-416b-a4b1-0550bc52b39e` |
| MLflow run id | from `fixture_id` suffix |
| Capture date | 2026-05-02 |
| Iterations captured | 5 |
| Canonical ledger events | 365 |
| Canonical ledger size | 38 706 bytes (compact JSON) |

## Exit criterion confirmation

- [x] `_validate_journeys_at_iteration_end` reports zero violations on the
      airline corpus run that produced this fixture (verified via
      `test_run_replay_airline_real_v1_within_burndown_budget` with
      `BURNDOWN_BUDGET = 0`).
- [x] `airline_real_v1.json` committed and contains
      `expected_canonical_journey` (365 events, 38 706 bytes).
- [x] `pytest tests/replay/` passes 11 tests:
      - 3 existing `airline_5cluster` tests
      - 3 synthetic per-iter tests (`two_iter_clean`, `two_iter_one_intra_violation`, `single_iter_5cluster`)
      - 1 budgeted regression (`airline_real_v1_within_burndown_budget`, budget = 0)
      - 1 soft-cluster demotion test (`demotes_already_passing_when_qid_in_soft_cluster`)
      - 1 skipped-AG outcome test (`recognizes_skipped_ag_outcomes`)
      - 2 new airline_real_v1 tests (byte-stability, 30s budget)
- [x] No accuracy regression observed vs the prior baseline (eyeball — no
      formal variance band yet; that comes in Phase B's scoreboard).

## Linked plans

- [`2026-05-01-burn-down-to-merge-roadmap.md`](./2026-05-01-burn-down-to-merge-roadmap.md) — strategic narrative.
- [`2026-05-01-phase-a-contract-burndown-plan.md`](./2026-05-01-phase-a-contract-burndown-plan.md) — original burn-down plan; Tasks 15–20 closed by this log.
- [`2026-05-02-run-replay-per-iteration-fix-plan.md`](./2026-05-02-run-replay-per-iteration-fix-plan.md) — the Cycle 7→8 burn-down plan that mechanically drove violations to 0.
- [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md) — detailed per-iter ledger; this log is the high-level summary.
- [`2026-05-02-cycle7-reconstruction-postmortem.md`](./2026-05-02-cycle7-reconstruction-postmortem.md) — cycles 1–7 fixture-shape autopsy.

## Next phase

**Phase B — Operator Scoreboard.** Replay-only, requires zero real-Genie
cycles, can begin immediately. Detailed plan to be drafted as
`2026-05-XX-operator-scoreboard-plan.md`.
