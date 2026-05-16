# Trial-5 Postmortem Fixtures

Captured from the two Lever Loop runs on 2026-05-16. See
`packages/genie-space-optimizer/docs/prompt_improvements/2026-05-16-trial-5-results.md`
for the source MLflow run IDs and the original bundle locations.

Each `replay_fixture.json` is the postmortem-bundle replay fixture for
the run, copied verbatim from
`/tmp/trial5_postmortem/run{A,B}/.../replay_fixture.json`. Fields the
regression tests rely on:

- `iterations[*].iteration` — 1-indexed iteration number
- `iterations[*].decision_records[]` — every typed decision record;
  the regression tests query by `decision_type`, `reason_code`,
  `root_cause`, `next_action`, `ag_id`.
- `iterations[*].ag_outcomes` — `{ag_id: terminal_outcome_string}` map.
- `iterations[*].strategist_response.action_groups[]` — the
  post-projection AGs (each carrying a `patches` list with
  canonical `patch_type` keys — this is what the *downstream*
  consumer sees, not the raw Stage-2 proposals).

These fixtures characterize the *broken* Trial-5 behaviour. The
regression tests in `tests/integration/test_trial5_regression_replay.py`
assert what must remain visible until the corresponding plan-phase
fix lands; the unit tests added by Phases 1a-3 then prove the broken
shape cannot recur.
