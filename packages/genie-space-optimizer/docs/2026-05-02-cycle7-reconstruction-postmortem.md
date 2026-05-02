# Cycle 7 Reconstruction Postmortem

**Date:** 2026-05-02
**Scope:** Phase A burn-down Task 13 (real-fixture capture)
**Outcome:** Track D `_baseline_row_qid` fix and reconstruction-script
scaffolding landed in code and unit tests. The actual cycle 7 fixture
reconstruction (Phase 3 Tasks 7-8) remains an operator step requiring a
Databricks notebook attached to the workspace where cycle 7 ran.

## Summary

Cycles 1-7 of the Phase A burn-down all failed in different ways:

| Cycle | Failure mode | Root cause |
|---|---|---|
| 1-3 | empty `eval_rows` | `_latest_eval_result` carrier refresh tied to accept-only path |
| 4 | empty rows persisted but seed silent-failed | baseline seed never logged failures |
| 5 | rows present but no qids extracted | `_baseline_row_qid` only checked `question_id`/`id` |
| 6 | rows + qids present, but qids were MLflow trace IDs | `client_request_id` fallback returned trace IDs as if they were canonical qids |
| 7 | same as 6 (false-green validator) | same; validator passed because all event emitters used the same wrong qid namespace |

Each cycle costs ~2 hours of real-Genie wall clock. By cycle 7 we had
spent ~14 hours on what is fundamentally a fixture-shape problem.

## What we changed instead of running cycle 8

1. **Reconstructed cycle 7 offline** using `mlflow.search_traces` (which
   already tagged every span with the canonical `question_id`) to invert
   the trace-id-to-qid map and rewrite eval_rows. The reconstruction
   script lives at
   `src/genie_space_optimizer/scripts/reconstruct_airline_real_v1_fixture.py`
   and exposes:
   - `substitute_trace_ids_with_canonical_qids(raw_iter, trace_to_canonical)`
   - `reconstruct_fixture(raw_fixture, trace_maps_by_iter)`
   - `load_fixture` / `save_fixture` for I/O
   - `fetch_trace_map_for_iteration` (MLflow primary path)
   - `fetch_trace_map_for_iteration_via_delta` (Delta fallback)
   - `assert_canonical_overlap(fixture)` — final invariant gate
   - `main(...)` end-to-end orchestrator
2. **Fixed `_baseline_row_qid`** to prefer canonical qid sources
   (`question_id` / `id` / `inputs/question_id` / `inputs.question_id` /
   `request.kwargs.question_id` including JSON-string parsing) over
   trace-id-shaped aliases (`client_request_id` / `request_id`).
3. **Added 8 reconstruction-script tests** + **5 Track D regression
   tests** that pin the real cycle 7 row shape, so the next time someone
   changes `_baseline_row_qid` the test suite breaks before any 2-hour
   cycle does.

## Operator steps still required (Phase 3)

The reconstruction script is callable but cannot run from a local laptop
— it needs `mlflow.search_traces` against the workspace where cycle 7
ran, and it needs the Spark Delta fallback path. To produce
`tests/replay/fixtures/airline_real_v1.json`, an operator must:

1. Save the cycle 7 raw stderr fixture to
   `tests/replay/fixtures/airline_real_v1_cycle7_raw.json` (Phase 1 Task 1).
   *(Already committed at `8950077`.)*
2. Sync the branch into the workspace and open
   `packages/genie-space-optimizer/notebooks/reconstruct_cycle7_fixture.py`
   as a Databricks notebook. It is a thin wrapper around `main(...)` with
   widget parameters, dep installs, and pre/post sanity checks.
3. Set the widgets (`experiment_id`, `optimization_run_id` defaults to the
   cycle 7 UUID, `catalog`, `schema`, `repo_root`) and run all cells.
4. Download the corrected fixture from
   `<repo_root>/packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json`
   and commit it.

The Track D fix shipped in this PR ensures cycle 8 (or any future cycle)
will write canonical qids directly — no reconstruction needed
post-merge. The reconstruction script is therefore a one-shot tool kept
for forensic value.

## Detection going forward

Add a fixture lint to CI (next phase):
- Reject any committed fixture under `tests/replay/fixtures/` where any
  `eval_rows[*].question_id` starts with `tr-`.
- Reject any fixture where cluster qids are not a subset of eval qids in
  any iteration.

This is the same as the canonical-overlap assertion in
`reconstruct_airline_real_v1_fixture.assert_canonical_overlap`. Lifting
it into a pytest collection hook or pre-commit check would have caught
cycles 6-7 in seconds.

## Lessons

- **Treat real-Genie cycles as integration tests, not debug-print
  loops.** Each is a 2-hour test cycle with high noise. Fixture-shape
  bugs belong in unit tests with synthetic data.
- **MLflow trace tags are a free fixture builder.** We had every byte of
  canonical-qid metadata persisted across cycles 5-7; we just weren't
  reading it.
- **Silent fallbacks are bug factories.** `_baseline_row_qid` returning
  a trace ID as a "qid" is what made cycles 6-7 look superficially
  healthy. Hard-fail or warn loudly when a fallback path runs.
