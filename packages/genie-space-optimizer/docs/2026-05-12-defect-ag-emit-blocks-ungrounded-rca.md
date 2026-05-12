# Defect: AG emission must block on ungrounded RCA + skipped_no_applied_patches must update retry memory

> **Status:** SUPERSEDED by
> `docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md`
> (2026-05-12). All seven items in the "Recommended next steps"
> section below are addressed by that plan (items 1, 4, 5) or
> consciously deferred to Defect Plan 2 (item 6 — Phase H totality)
> and the bundle-status wiring fix (item 7 in spirit). Items 2-3
> (structural SQL repairs for gs_009 / gs_024) are LLM/prompt-domain
> work explicitly out of roadmap-closeout scope per
> `docs/2026-05-10-roadmap-closeout.md`.

## Verdict

`MERGE_GATE_GAP / NO_APPLIED_PATCHES`

## Source postmortem

`packages/genie-space-optimizer/docs/runid_analysis/31ecd96f-5d56-4b5a-af8e-38e9e5c549af/postmortem.md`

## One-line summary

The Databricks job succeeded, but the optimizer made zero progress
(91.7% → 91.7%): all five iterations ended `skipped_no_applied_patches`,
alternating `AG_DECOMPOSED_H001` for `gs_009` and `AG_DECOMPOSED_H002`
for `gs_024`. Every attempt died before candidate evaluation.

## Driving evidence

- `gs_009` and `gs_024` repeatedly show `rca_formed
  outcome=unresolved reason=rca_ungrounded`, yet `AG_DECOMPOSED_H001`
  and `AG_DECOMPOSED_H002` continue to emit.
- Contract-health invariant violation:
  `cluster H001/H002 reached AG-emit with no fit RCA card and no
  cluster_blocked_no_rca record`.
- Selected patch signature on `skipped_no_applied_patches` rows is
  empty (`()`). The loop has no key to retire the no-op pattern, so
  it retries the same AG family indefinitely.

## Recommended next steps (verbatim from postmortem F1–F8)

1. **Block AG emission when RCA is ungrounded** unless an explicit
   diagnostic repair path is created.
   - Modules: `optimization/rca_execution.py`,
     `optimization/stages/action_groups.py`,
     `optimization/strategist_constraints.py`,
     `optimization/invariants.py`.
   - Smallest change: route ungrounded clusters to
     `cluster_blocked_no_rca` or a typed RCA-regeneration stage
     before AG emission; add an invariant test for
     `open_cluster_ungrounded_at_ag_emit`.

2. **Add a structural SQL repair path that survives applyability for
   top-N aggregation defects** (e.g. `gs_009` needs `ROW_NUMBER()` or
   `LIMIT 10`).
   - Modules: `optimization/cluster_driven_synthesis.py`,
     `optimization/stages/proposals.py`,
     `optimization/patch_applyability.py`,
     `optimization/sql_shape_quality.py`.

3. **Add a narrow payment-filter repair path for `gs_024`** with
   dependency-aware collateral checks.
   - Modules: `optimization/cluster_driven_synthesis.py`,
     `optimization/stages/gates.py`,
     `optimization/proposal_shape.py`,
     `optimization/patch_applyability.py`.

4. **Treat `skipped_no_applied_patches` as a first-class
   retry/retirement signal.**
   - Modules: `optimization/reflection_retry.py`,
     `optimization/rca_next_action.py`,
     `optimization/strategist_constraints.py`,
     `optimization/harness.py`.
   - Smallest change: persist a no-op retry signature using
     `(ag_id, target_qids, root_cause, gate_drop_reasons)` even when
     patch IDs are empty, then retire or deprioritize that AG until
     RCA or lever family changes.

5. **Make blast-radius drops actionable** rather than terminal.
   - Modules: `optimization/stages/gates.py`,
     `optimization/rca_next_action.py`,
     `optimization/cluster_driven_synthesis.py`.

6. **Restore Phase H iteration-summary and per-iteration artifact
   totality** (orthogonal but surfaced by the same run).
   - Modules: `optimization/run_output_bundle.py`,
     `optimization/stage_io_capture.py`,
     `tools/evidence_bundle.py`.

7. **Fix Databricks ID resolution in run manifest emission**
   (`GSO_RUN_MANIFEST_V1/V2` reports `unknown` for job, parent run,
   task run).
   - Modules: `optimization/harness.py`,
     `optimization/run_output_bundle.py`,
     `tools/evidence_bundle.py`.

## Out of scope

This defect plan is **independent** of the RCO-4b consolidating-trial
plan. The trial infrastructure (markers, contract-health,
pure-helper extraction) fired correctly on this run. The optimizer
correctness issues above are pre-existing gaps surfaced (not caused)
by the trial.
