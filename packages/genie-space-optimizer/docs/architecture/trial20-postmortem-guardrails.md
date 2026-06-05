# Trial 20 — Postmortem Guardrails

This document supplements the GSO postmortem skill with the new guardrails
introduced by Trial 20. Trial 20 closes the contract splits surfaced by the
Trial 19 postmortems (airline `519131527536322`, 7now `766686021706995`) by
aligning every outer decision rail with the inner state-machine contracts
established in Trials 16–19.

Each guardrail names:

- The **anti-success marker** to search for in MLflow traces / lever-loop
  stdout.
- The **expected positive marker** that should appear once Trial 20 is
  active.
- The **root-cause band** the marker resolves to (which workstream owns it).
- The flag that supersedes any pre-Trial-20 guardrails the new one closes.

## Guardrail Catalogue

### G-T20-A: PRE_ARBITER_VETO_OF_POST_ARBITER_GAIN

A full-eval acceptance was vetoed by
`decide_pre_arbiter_regression_guardrail` with `target_fixed_qids=()` even
though sliced eval showed a positive arbiter delta on the AG's target.

| Aspect | Value |
|---|---|
| Anti-success marker | `target_fixed_qids=()` + arbiter delta >0 on AG target |
| Expected positive marker | `GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1`, `GSO_TRIAL20_SHADOW_DECISION_V1` |
| Workstream | A (pre-arbiter veto fix) |
| Sub-flag | `GSO_TRIAL20_PRE_ARBITER_VETO_FIX` |
| Supersedes | none (new guardrail) |

Postmortem action: read the shadow-decision marker to see whether today's
veto would have flipped under the A2 fix. If `decision_today != decision_with_fix`,
the regression band is closed by Trial 20.

### G-T20-B: NO_APPLIED_PATCHES_AFTER_KEPT_INSUFFICIENT

An iteration emitted `GSO_ITERATION_TERMINAL_DECIDED_V1
terminal_reason="no_applied_patches"` even though one or more QIDs
reached `accepted.decision == "kept_insufficient"` in the state machine.

| Aspect | Value |
|---|---|
| Anti-success marker | `GSO_ITERATION_TERMINAL_DECIDED_V1 terminal_reason="no_applied_patches"` AND any SM final state with `accepted.decision="kept_insufficient"` |
| Expected positive marker | `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL_V1` + `terminal_reason="kept_insufficient"` |
| Workstream | B (terminal-taxonomy unification) |
| Sub-flag | `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL` |
| Supersedes | pre-Trial-20 `NO_APPLIED_PATCHES` catch-all when SM saw kept_insufficient |

Postmortem action: the terminal selector now reads the SM final state. If
the selector still emits `no_applied_patches` while the SM marked
`kept_insufficient`, the precedence rule has regressed — file a Plan 12
pivot routing bug rather than a stage-3 silence bug.

### G-T20-C: DEGENERATE_PIVOT_SAME_FAMILY

Plan 12 recommended a pivot to the SAME `patch_family` that the cluster
just terminated on, because the legacy `_PIVOT_FROM_FAMILY_AFTER_FAILURE`
constant was a single-element fixed string rather than a cycle.

| Aspect | Value |
|---|---|
| Anti-success marker | `GSO_PLAN12_PIVOT_RECOMMENDED_V1` where `prior_patch_family == recommended_patch_family` |
| Expected positive marker | `recommended_patch_family == _PIVOT_GRAPH[prior_patch_family]` (always a different family in the 5-family cycle) |
| Workstream | C (pivot graph) |
| Sub-flag | `GSO_TRIAL20_FAMILY_PIVOT_GRAPH` |
| Supersedes | `_PIVOT_FROM_FAMILY_AFTER_FAILURE` constant pivot |

Postmortem action: if the marker still names the same family on both
sides, the pivot graph either was not consulted (flag off) or
`prior_patch_family` was empty and the inference fallback (C2) did not
fire. Cross-check the kept_insufficient signature for the cluster.

### G-T20-D1: SINGLE_LEVER_WHEN_BUNDLE_REQUIRED

A cluster with non-empty `insufficient_repair_signatures` emitted a
single-lever proposal instead of a mandatory multi-lever bundle, OR an
iteration-1 single-lever proposal arrived without
`single_lever_justification`.

| Aspect | Value |
|---|---|
| Anti-success marker | single-lever proposal + non-empty insufficient signatures, OR iteration-1 single-lever without `single_lever_justification` |
| Expected positive marker | `GSO_TRIAL20_BUNDLE_EMITTED_V1` (mandatory bundle) OR `GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1` (justified single-lever) |
| Workstream | D (bundle defaults) |
| Sub-flag | `GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT` |
| Supersedes | Trial 17 `bundle_id (optional)` prompt phrasing |

Postmortem action: read the justification text. If the LLM's reasoning
does not engage with the named multi-axis failure mode, surface as a
prompt-tuning task (not a structural Plan 12 pivot).

### G-T20-D3: SOLE_LEVER_REUSES_REJECTED_FAMILY

The strategist admitted a sole-lever proposal whose lever family matches
the family of the most recent `rejected_insufficient_repeat` admission
signature.

| Aspect | Value |
|---|---|
| Anti-success marker | sole-lever proposal post-admission with family == rejected_signature.family |
| Expected positive marker | strategist refusal + Plan 12 pivot to a different family |
| Workstream | D (strategist hardening) |
| Sub-flag | `GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT` |
| Supersedes | none (new hard rule) |

### G-T20-E: BLAST_RADIUS_UNSTAMPED

A proposal reached `blast_radius_batch._assess_blast_radius` without
`passing_dependents` in its `patch_body`. Pre-Trial-20 this defaulted to
`safe=True`; Trial 20 defaults to `safe=False` so the existing
`narrow_replacement_gate` cycle (already wired in
`registry.py:35-66`) is actually exercised.

| Aspect | Value |
|---|---|
| Anti-success marker | `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1` |
| Expected positive marker | `passing_dependents` present on every Stage 3 proposal patch_body; no unstamped marker |
| Workstream | E (blast-radius mandatory) |
| Sub-flag | `GSO_TRIAL20_BLAST_RADIUS_MANDATORY` |
| Supersedes | safe-by-default fallback at `proposal_grounding.py:557-558` |

Postmortem action: if the unstamped marker fires in production, E1
(state-machine ctx plumbing of the counterfactual scanner) has a gap.
File against the harness counterfactual scanner replication path, NOT
the narrow_replacement_gate transformer.

### G-T20-F1: ADD_SQL_SNIPPET_WITHOUT_VALIDATION_PASSED

An `add_sql_snippet_*` patch reached applyability without
`validation_passed=True` stamped by `validate_sql_snippet`.

| Aspect | Value |
|---|---|
| Anti-success marker | applier rejection `validation_passed missing` on `add_sql_snippet_*` |
| Expected positive marker | `GSO_TRIAL20_SQL_SNIPPET_VALIDATED_V1` |
| Workstream | F1 (validator stamping audit) |
| Sub-flag | not gated (hot fix) |
| Supersedes | none |

### G-T20-F2: UNRESOLVED_CANONICAL_TARGET

A metadata patch (typically `add_column_description`) was emitted against
an unresolved table target (`tkt_payment`, `mv_7now_store_sales`, etc.).

| Aspect | Value |
|---|---|
| Anti-success marker | `target_table` contains unqualified or unknown identifier; applier rejects with `target_unresolved` |
| Expected positive marker | `GSO_TRIAL20_CANONICAL_TARGET_RESOLVED_V1` with `resolved_target` populated |
| Workstream | F2 (canonical target resolution) |
| Sub-flag | not gated (hot fix) |
| Supersedes | none |

## How To Read These Guardrails In A Postmortem

1. Start with the `GSO_ITERATION_TERMINAL_DECIDED_V1` markers. If
   `terminal_reason == "kept_insufficient"`, look for the matching Plan 12
   pivot marker — if the recommended family matches `_PIVOT_GRAPH[prior]`,
   the C1 cycle did its job.
2. For each accepted candidate in full-eval, look for
   `GSO_TRIAL20_SHADOW_DECISION_V1`. If `decision_today != decision_with_fix`,
   the A2 fix flipped the call.
3. On Stage 3 inspection, every proposal patch_body MUST carry
   `passing_dependents`. The absence of `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1`
   is the success criterion (we want the marker to never fire post-Trial-20).
4. Single-lever proposals MUST carry `single_lever_justification` on iteration
   1; bundles MUST be emitted when prior insufficient signatures are present.
   Read the `GSO_TRIAL20_BUNDLE_EMITTED_V1` / `GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1`
   pair to attribute reasoning.

## Trial 19 Guardrails Superseded By Trial 20

| Trial 19 guardrail | Trial 20 replacement |
|---|---|
| `NO_APPLIED_PATCHES catch-all on kept_insufficient` (terminal taxonomy) | G-T20-B (KEPT_INSUFFICIENT terminal precedence) |
| `_PIVOT_FROM_FAMILY_AFTER_FAILURE` constant | G-T20-C (PIVOT_GRAPH cycle) |
| Optional bundle prompt phrasing | G-T20-D1 (bundle-default with justification field) |
| safe-by-default blast radius fallback | G-T20-E (unsafe-by-default + marker) |

When postmortem evidence still names a Trial 19 guardrail by its old
label, treat it as a Trial 20 regression: either the master flag was
disabled in the run, or the specific sub-flag was overridden.

## Rollback

Single env-var: `GSO_TRIAL20_ENFORCE=0` then redeploy. Every guardrail
above returns to Trial 19 behaviour byte-for-byte. The 500-seed workbench
sweep with the master flag OFF is the byte-stable contract.
