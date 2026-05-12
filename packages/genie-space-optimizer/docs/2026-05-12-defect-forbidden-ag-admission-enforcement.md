# Defect: Enforce forbidden/no-action AG admission + force materially different strategy after target_qids_not_improved

> **Status:** stub. Surfaced by the RCO-4b consolidating-trial run
> `ccf1d60d-d686-467b-bafa-1640131b4393` (May 12, 2026). Task
> breakdown to be filled in.

## Verdict

`MERGE_GATE_GAP / NO_ACCEPTED_PROGRESS`

## Source postmortem

`packages/genie-space-optimizer/docs/runid_analysis/ccf1d60d-d686-467b-bafa-1640131b4393/postmortem.md`

## One-line summary

Iteration 1 produced a real `AG1` candidate that improved aggregate
score (87.0% → 91.3%), but rollback was **correct**: target `gs_026`
stayed hard, `target_fixed_qids=[]`, and `gs_012` regressed
out-of-target. Iterations 2–5 then stalled re-admitting the same
forbidden no-action `AG1` family despite
`GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` reporting
`would_admit_with_admit_no_action_on=true`.

## Driving evidence

- `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` fires with
  `behavior_flag_on=true`, `rollback_reason=no_proposals`,
  `would_admit_with_admit_no_action_on=true` — and the AG still
  proceeds to `Proposals (0 total)`.
- Iteration-1 `GSO_FULL_EVAL_V1`: `target_fixed_qids=[]`,
  `target_still_hard_qids=[gs_026]`,
  `out_of_target_regressed_qids=[gs_012]`,
  `accidentally_improved_qids=[]`.
- Iterations 2–5 each follow the same pattern: same `AG1` family,
  `Proposals (0 total)`, `Valid proposals: 0 of 0`.

## Recommended next steps (verbatim from postmortem F1–F8)

1. **Enforce forbidden/no-action AG admission.**
   - Modules: `optimization/strategist_constraints.py`,
     `optimization/reflection_retry.py`,
     `optimization/rca_next_action.py`,
     `optimization/harness.py`.
   - Smallest change: when a prior AG has
     `rollback_reason=no_proposals` or
     `target_qids_not_improved`, block the same
     `(target_qids, blame_set, lever_set)` unless RCA evidence or
     lever family changes.

2. **Promote `target_qids_not_improved` into a patch-family change
   requirement.**
   - Modules: `optimization/control_plane.py`,
     `optimization/rca_next_action.py`,
     `optimization/cluster_driven_synthesis.py`.

3. **Build a target-scoped structural repair for `gs_026`** (plural
   top-N collapse + wrong table routing: `zone_vp_name` vs
   `zone_combination`).
   - Modules: `optimization/cluster_driven_synthesis.py`,
     `optimization/stages/proposals.py`,
     `optimization/proposal_shape.py`,
     `optimization/patch_applyability.py`.

4. **Add regression-aware collateral constraints from `gs_012`**
   (it regressed in iteration 1).
   - Modules: `optimization/stages/gates.py`,
     `optimization/per_question_regression.py`,
     `optimization/cluster_driven_synthesis.py`.

5. **Make zero-proposal iterations produce a non-empty retry
   signature** so they retire immediately.
   - Modules: `optimization/reflection_retry.py`,
     `optimization/stages/proposals.py`,
     `optimization/harness.py`.

6. **Fix journey replay state transitions around soft-signal
   reclassification** (5 `clustered → soft_signal` violations for
   `gs_021`; Cycle-17 carry-over).
   - Modules: `optimization/question_journey.py`,
     `optimization/journey_fixture_exporter.py`,
     `optimization/invariants.py`.
   - **Note:** This is the named blocker for RCO-6 (replay/journey
     parity). Fixing it here unblocks RCO-6's named-anchor work.

7. **Restore Phase H per-iteration artifact totality** (same
   orthogonal gap as the airline run).
   - Modules: `optimization/run_output_bundle.py`,
     `optimization/stage_io_capture.py`,
     `tools/evidence_bundle.py`.

8. **Fix Databricks ID resolution in run manifest emission** (same
   orthogonal gap as the airline run).

## Out of scope

This defect plan is **independent** of the RCO-4b consolidating-trial
plan. The trial infrastructure (markers, contract-health,
pure-helper extraction) fired correctly on this run. The
acceptance/rollback decision itself was correct in iteration 1 — the
gap is the loop's inability to learn from that rollback and force a
materially different strategy.
