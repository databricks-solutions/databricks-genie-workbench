# 7Now iter02 / action_group_selection — Forbidden-AG No-Op Loop Closure Fixture

## Source

7Now run 960148942255012 (C14-V T1 corpus anchor) — iteration 2.

The postmortem bundle (`docs/runid_analysis/3b050ec5...`) captured
`action_group_selection` only for iter_01 (iter_02 bundle has only
`cluster_formation`). This fixture is a CONTRACT FIXTURE, not a
direct replay of captured MLflow stage I/O.

## What it encodes

**The forbidden-AG no-op loop**: on the real 7Now run, AG1 was
selected on iter_01 but produced zero proposals. Under the pre-C15
code, iter_02 selected AG1 again (the forbidden set was not checked
at the stage level). C15 Phase 3 closes this at the stage contract
level: when `stage_handlers_chunk_b_enabled()` is on and the harness
threads `forbidden_ags=(ForbiddenAG("AG1", ForbiddenReason.NO_PROPOSALS),)`
into `ActionGroupsInput`, `select()` populates `admission_trace` with
AG1 → DENIED/no_proposals and does NOT select AG1 as the output AG.

## Why no input.json

The real iter_01 `input.json` from the postmortem bundle is 2.5 MB
(contains full RCA grounding terms). Vendoring it would require PII
audit and truncation. The `test_iter02_does_not_reselect_forbidden_ag1`
test in `test_chunk_b_replay.py` only reads `expected_output.json` —
it is a CONTRACT test, not a full replay test.

## Full replay note

A full round-trip replay test (input → execute → compare output) would
require either: (a) a scrubbed/truncated input.json, or (b) a synthetic
minimal input. This is deferred to a follow-up task. The contract test
here is the regression rail; replay byte-stability is covered by the
unit tests in `test_action_groups_contract.py`.
