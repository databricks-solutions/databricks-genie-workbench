# Lever Loop — Architecture & Iteration Tracker

A running ledger of architectural trials applied to the lever-loop
optimizer. Each entry captures the hypothesis being tested, the watch
markers, the anti-success markers, the local verification gates, and
the RCA bands the trial is intended to close.

This document follows the Trial 19 / Trial 20 cutover protocol: trials
are merged default-ON behind a master flag with a single
emergency-rollback knob. The flag is the contract — when OFF, the trial
must be byte-stable on the 500-seed workbench sweep.

---

## Trial 20 — Close the Outer Rails

**Master flag:** `GSO_TRIAL20_ENFORCE` (default ON).

**Sub-flags (all default ON when master ON):**

- `GSO_TRIAL20_PRE_ARBITER_VETO_FIX`
- `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL`
- `GSO_TRIAL20_FAMILY_PIVOT_GRAPH`
- `GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT`
- `GSO_TRIAL20_BLAST_RADIUS_MANDATORY`

### Hypothesis

Trials 16–19 fixed the inner loop (state-machine acceptance, admission
gate, LLM-first RCA). Three outer rails remained on legacy semantics,
causing arbiter accuracy gains to flatline on the lever-loop runs
following Trial 19:

- Pre-arbiter regression guardrail vetoed an arbiter-positive iteration
  even though sliced eval rescued the AG target (airline
  `519131527536322`).
- Iteration-terminal taxonomy emitted `no_applied_patches` while the SM
  recorded `kept_insufficient` (7now `766686021706995`).
- Plan 12 pivot table was a single-element constant — it could recommend
  the same family that just failed.

Trial 20's discipline is "align outer rails with existing inner
contracts". Zero net new architectural surface area — only contract
realignment.

### Watch Markers (positive — these SHOULD appear)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1` | A1 | Offline root-cause replay identified the actual veto cause |
| `GSO_TRIAL20_SHADOW_DECISION_V1` | A3 | Side-by-side comparison of today's decision vs. with-fix |
| `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL_V1` | B | SM final state precedence over catch-all |
| `GSO_TRIAL20_BUNDLE_EMITTED_V1` | D4 | Multi-lever bundle emitted when required |
| `GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1` | D4 | LLM's free-text justification for single-lever choice |
| `GSO_TRIAL20_SQL_SNIPPET_VALIDATED_V1` | F1 | `validate_sql_snippet` stamped `validation_passed=True` |
| `GSO_TRIAL20_CANONICAL_TARGET_RESOLVED_V1` | F2 | Raw target resolved to canonical fully-qualified identifier |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-20)

| Marker | Workstream | Means we regressed |
|---|---|---|
| `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1` | E1 | Counterfactual scanner not plumbed into SM ctx; gate is a no-op |
| `GSO_ITERATION_TERMINAL_DECIDED_V1 terminal_reason="no_applied_patches"` with any `accepted.decision="kept_insufficient"` in the iteration | B2 | Terminal-selector precedence regression |
| `GSO_PLAN12_PIVOT_RECOMMENDED_V1` where `prior == recommended` family | C1 | Pivot graph not consulted |
| Single-lever proposal with non-empty `insufficient_repair_signatures` | D1 | Bundle-default directive not honored |
| `add_sql_snippet_*` patch reaching applier without `validation_passed=True` | F1 | `validate_sql_snippet` not stamping |

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 20 unit suite | `pytest tests/unit/optimization/test_trial20_*.py tests/unit/test_plan12_patch_family_pivot_after_no_applied.py tests/unit/test_terminal_reason.py tests/unit/test_repair_proposal_dataclass.py tests/unit/test_llm_repair_proposal_output_schema.py -q` |
| Integration replay (airline + 7now) | `pytest tests/integration/test_trial20_postmortem_replay.py -q` |
| Workbench 500-seed ON | `GSO_TRIAL20_ENFORCE=1 python -m local_lever_workbench.fuzzer --iterations 500 --seed 0 --mode mixed` |
| Workbench 500-seed OFF (byte-stable) | `GSO_TRIAL20_ENFORCE=0 python -m local_lever_workbench.fuzzer --iterations 500 --seed 0 --mode mixed` |

### RCA Bands Closed

| Pre-Trial-20 band | Closed by | New attribution |
|---|---|---|
| Airline rollback with `target_fixed_qids=()` | A1+A2+A3 | `GSO_TRIAL20_SHADOW_DECISION_V1` shows fix flips the call |
| 7now `no_applied_patches` masking `kept_insufficient` | B2 | Iteration-terminal selector reads SM final state |
| Plan 12 same-family pivot loop | C1+C2 | `_PIVOT_GRAPH` cycle + prior-family inference |
| Single-lever proposal where the diagnosis is multi-axis | D1+D3 | Bundle mandatory after insufficient; strategist refuses same-family sole-lever |
| Blast-radius safe-by-default masking missing counterfactual | E1+E2 | Default-unsafe + `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1` |
| `add_sql_snippet_*` rejected at applier for missing `validation_passed` | F1 | Validator stamps unconditionally |
| Stage 3 metadata patches against unresolved targets (`tkt_payment`, `mv_7now_store_sales`) | F2 | Canonical target resolution + drop fallback |

### Rollback

`export GSO_TRIAL20_ENFORCE=0` then redeploy. Every Trial 20 surface
reverts byte-for-byte to Trial 19 behaviour. Verified by the 500-seed
workbench sweep with the master flag OFF.

### Status

- [x] Workstream A (A1, A2, A3) — pre-arbiter veto fix
- [x] Workstream B (B1, B2, B3) — terminal taxonomy unification
- [x] Workstream C (C1, C2) — pivot graph
- [x] Workstream D (D1, D2, D3, D4) — bundle defaults
- [x] Workstream E (E1, E2, E3) — blast-radius mandatory
- [x] Workstream F (F1, F2) — applier plumbing hot fixes
- [x] Workbench v2.1 invariants K1–K5
- [x] 500-seed workbench sweep ON + OFF pass 500/500
- [x] Per-workstream unit tests
- [x] Synthetic tape fixtures + integration replay

---

## Trial 21 — Evidence Actuator Cutover

### Hypothesis

Six observe-only P4 gates emit useful markers but never drop a proposal.
Run A (`919039845318742`) and Run B (`452249357578743`) both shipped
candidates that should have been blocked at synthesis time. Trial 21
collapses the per-gate marker pipeline into a single decision boundary —
the **Evidence Actuator** in `proposal_slate_compiler.py` — and
graduates every gate from "observe" to "drop with typed
:class:`DropReason`".

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W1 | Postmortem-replay regression suite (7 bright-line assertions, frozen marker fixtures) | `tests/integration/postmortem_replay/` | enforce |
| W2 | `proposal_slate_compiler.compile_slate`: 7-step pipeline, typed `DropReason`, `SlateCompilerResult`. `NO_APPLIED_PATCHES` is NEVER emitted on empty slate. | `proposal_slate_compiler.py` | enforce |
| W3 + C8 | `slice_segments` allocates the 40k cap across the three Stage 3 prompt regions; `observe_only=false`. Run B's 104k payload reports `over_cap=false` post-slice with `sub_cluster_split_needed=true`. | `stage3_prompt_sizer.py` | enforce |
| W4 + C3 | Declined snippets drop at the producer (`stages/synthesize.py`) with `GSO_TRIAL21_PRODUCER_DROP_V1`. Actuator carries a defensive duplicate via `snippet_validator_verdict_by_proposal_id`. | `producer_snippet_validator.py`, `stages/synthesize.py` | enforce |
| W5 + C2 + C5 | `behavior_delta_hash` over `(rca_kind, behavioral_diff)` is the canonical fingerprint; `_check_mechanism_repeat` short-circuits when fingerprints diverge. | `proposal_slate_compiler.py`, `patch_mechanism.py` | enforce |
| W6 + C1 | Per-patch-family asset gate table `_REQUIRED_ASSET_TABLE` + `required_assets_for_patch_family()`. `add_instruction` requires justification; description families require implicated assets; `add_example_sql` requires sql_shape_delta. | `repair_diagnosis.py` | enforce |
| W7 | `metadata_target_resolver.validate_and_stamp_metadata_patch_target` canonicalises bare table names against the deployed Genie metadata snapshot before declaring `missing_table`. | `metadata_target_resolver.py` | enforce |
| W8 + C7 | `classify_run_outcome_from_aggregates`: scalar-input outcome classifier. `accepted_with_attribution_drift` now admitted to the aggregate-gain branch so target-debt is detected on attribution-drift accepts. | `state_machine/outcome.py` | enforce |
| W9 | `compute_deploy_eligibility` returns `DeployEligibilityVerdict(optimizer_task_status, candidate_deploy_eligible, deploy_skip_reason)`. Optimizer task stays SUCCESS; deploy task skips on `contract_health_blocked` / `no_candidate`. | `harness.py` | enforce |

### Feature Flag

`GSO_TRIAL21_ACTUATOR` (default ON). Set to `0` / `false` / `no` /
`off` to revert to the P4 observe-only path; the producer's `compile_slate`
call is short-circuited and all proposals flow unchanged.

### Watch Markers (positive — these SHOULD appear)

* `GSO_SLATE_COMPILER_DECISION_V1` — emitted per dropped proposal AND
  per Actuator summary (with `is_summary=true`). Carries `drop_reason`,
  `failing_check`, `proposal_id`, `qids`, `cluster_id`, `iteration`.
* `GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1` — now with
  `observe_only=false`, `sub_cluster_split_needed`,
  `user_prompt_tokens_pre_slice`.
* `GSO_TRIAL21_PRODUCER_DROP_V1` — emitted when a snippet validator
  decline drops the proposal at the producer.

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-21)

| Anti-marker | Where it surfaced | Trial 21 fix |
|---|---|---|
| `terminal_reason=no_applied_patches` on empty slate | Lever loop | Empty slates now carry a typed `DropReason` (mapped via `drop_reason_to_terminal_reason`); `NO_APPLIED_PATCHES` is no longer emitted by `compile_slate`. |
| `over_cap=true` with `observe_only=true` | Stage 3 prompt sizer | `slice_segments` enforces with `observe_only=false`. |
| `validator outcome=declined` followed by `applier_outcome=applied` | Producer→applier pipeline | W4 drops at producer with `GSO_TRIAL21_PRODUCER_DROP_V1`. |
| Same `(qid, patch_type, lever)` triple appearing as `kept_insufficient` more than once with the same `behavior_delta_hash` | Lever loop | W5 fingerprint memory short-circuits via `_check_mechanism_repeat`. |
| `add_column_description` / `add_table_description` with empty `implicated_assets` reaching applier | Stage 3 | W6 `MISSING_IMPLICATED_ASSETS` drop. |
| `add_instruction` shipped without `single_lever_justification` | Stage 3 | W6 `UNJUSTIFIED_SINGLE_LEVER` drop. |
| `OPTIMIZER_TRIED_INSUFFICIENT_GAIN` on attribution_drift accept with `target_still_hard_qids` non-empty | Outcome classifier | W8 reclassifies as `OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT`. |
| Deploy task FAILS because merge_gate_blocked or assembly_failed | Job DAG | W9 splits: optimizer task stays SUCCESS, deploy task skips with `contract_health_blocked`. |

### Local Verification (mandatory before deploy)

| Step | Command |
|---|---|
| Trial 21 actuator unit tests | `pytest tests/unit/test_proposal_slate_compiler.py tests/unit/test_required_assets_for_patch_family.py tests/unit/test_deploy_eligibility.py tests/unit/test_stage3_prompt_sizer.py tests/unit/state_machine/test_outcome_aggregates.py -q` |
| Trial 21 postmortem replay (the merge gate) | `pytest tests/integration/postmortem_replay/test_trial21_postmortem_replay.py -q` |

Both suites MUST pass before bumping any module status from
`observe-only` to `enforce`.

### Rollback

`export GSO_TRIAL21_ACTUATOR=0` then redeploy. Each Actuator decision
short-circuits to "admit"; the P4 observe-only markers continue to
emit so the postmortem-replay suite can still audit them.

### Status

- [x] W1 — postmortem-replay regression suite (7 bright-line tests + W7 positive case = 8 tests green)
- [x] W2 — Evidence Actuator (`proposal_slate_compiler.py`) + flag + wire-in
- [x] W3 + C8 — `slice_segments` enforce-mode
- [x] W4 + C3 — producer drop on declined snippets
- [x] W5 + C2 + C5 — behavior-delta fingerprint memory
- [x] W6 + C1 — per-patch-family asset gate table
- [x] W7 — metadata target resolver canonicalises bare table names
- [x] W8 + C7 — `classify_run_outcome_from_aggregates` admits attribution_drift to aggregate-gain branch
- [x] W9 — `compute_deploy_eligibility` splits task status from deploy eligibility
- [x] All 8 postmortem-replay tests green
- [x] Per-workstream unit tests

---

## Trial 22 — Slate Compiler Repair + Ledger Reconciliation

**Master flag:** `GSO_TRIAL22_SLATE_REPAIR` (default ON).

**Sub-flags (all default ON; each is an independent rollback knob):**

- `GSO_TRIAL22_BUNDLE_GROUP_CHECK` — W2 group-level bundle invariant
- `GSO_TRIAL22_BUNDLE_COHESION_SWEEP` — W2 Phase 1.5 atomic cascade
- `GSO_TRIAL22_TERMINAL_REASON_HELPER` — W4 terminal-verdict helper
- `GSO_TRIAL22_ASSET_GATE` — W6 `_check_required_assets` gate
- `GSO_TRIAL22_SUBCLUSTER_SLICE` — W7 RCA-subcluster prompt slicing
- `GSO_TRIAL22_LINEAGE_INVARIANT` — W5.1 full-eval lineage enforcement
- `GSO_TRIAL22_DEPLOY_GATE` — W8 candidate deploy-eligibility gate

### Hypothesis

Trial 21 created the right Evidence Actuator boundary but scoped the
bundle invariant **per proposal** instead of **per bundle group**.
Production emits a bundle as N sibling proposals sharing one
`bundle_id`, each carrying a single lever, so
`effective_selected_levers()` was length-1 on every sibling and every
bundle was dropped `bundle_invariant_violated` — d139 stayed flat (no
proposal survived) and e943 reported an accepted full-eval (95.8%) that
the state-machine ledger contradicted with `OPTIMIZER_NO_CANDIDATES`.

Trial 22 fixes the actuator boundary (bundle validity is a group
property), makes the terminal-reason ledger tell the truth when the
compiler drops proposals, and reconciles the two ledgers so no
full-eval acceptance can disagree with optimizer outcome. No new RCA
logic.

### Watch Markers (positive — these SHOULD appear)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_SLATE_COMPILER_DECISION_V1 drop_reason=bundle_member_dropped_cascade` | W2 | Phase 1.5 atomically dropped a bundle after one member failed an earlier check |
| `GSO_TRIAL22_RETRY_FEEDBACK_V1` | W3 | Compiler drop summary fed back into the next Stage 3 prompt |
| `GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1` | W7 | Oversized RCA-subcluster prompt partitioned into token-budgeted sub-batches |
| `GSO_TRIAL22_LINEAGE_KEY_AUDIT_V1` | W5.0 | Canonical lineage key `(optimization_run_id, ag_id, iteration)` audited (always emitted) |
| `GSO_TRIAL22_LINEAGE_VIOLATION_V1` | W5.1 | Accepted full-eval with no matching patch_outcome+admission stamped `orphan_acceptance` |
| `GSO_TRIAL22_DEPLOY_SKIPPED_V1` | W8 | Deploy task skipped an ineligible candidate; parent job stays SUCCESS |
| `GSO_TRIAL22_BUNDLE_DISSOLVED_V1` | W2.1 | A bundle reduced to one surviving member was dissolved into a solo proposal (lone valid member proceeds instead of dropping) |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-22)

| Anti-marker | Where it surfaced | Trial 22 fix |
|---|---|---|
| Every Stage 3 proposal dropped `bundle_invariant_violated` with `survivor_count=0` | Slate compiler (d139) | W2 group-aware `_check_bundle_invariants_group` evaluates the union of levers across the `bundle_id` group |
| `terminal_reason=stage3_returned_none` while Stage 3 DID return proposals the compiler then dropped | Lever loop (d139) | W4 `SLATE_COMPILER_EMPTY` + `IterationTerminalVerdict{top_drop_reason, drop_reason_counts}` |
| Closed-vocab enum mixed with dynamic `:<reason>` suffix | Terminal taxonomy | W4 enum stays closed; the reason lives in structured fields, never the enum string |
| W6 asset drop of one bundle member silently re-triggers W2 on the surviving sibling | Compiler ordering | W2 Phase 1.5 cohesion sweep drops the whole bundle atomically (`BUNDLE_MEMBER_DROPPED_CASCADE`) |
| Lone surviving bundle member dropped `bundle_invariant_violated` after an UPSTREAM producer drop (snippet validator) reduced the bundle to one member — reproduces the d139 flatline (caught in live `fevm-prashanth` run) | Slate compiler Phase 2 | W2.1 singleton-bundle dissolution: the lone member's `bundle_id` is cleared and it proceeds as a solo proposal (`GSO_TRIAL22_BUNDLE_DISSOLVED_V1`) |
| `GSO_FULL_EVAL_V1.accepted=true` with zero matching patch_outcome/admission AND `scoreboard.best_accuracy != baseline` | Finalize (e943) | W5.1 excludes orphan acceptances from `best_accuracy` |
| Working e943 H001 Stage 3 prompt (~6-7k) gets split | Stage 3 sizer | W7 split scoped to the RCA-subcluster builder; H001 positive-control bright-line #5 stays GREEN |
| `databricks apps deploy` invoked when `candidate_deploy_eligible=False` | Job DAG | W8 deploy task reads the task value and skips with `GSO_TRIAL22_DEPLOY_SKIPPED_V1` |

### Bright-Line Replay Suite (the merge gate)

`tests/integration/postmortem_replay/test_trial22_postmortem_replay.py`
— 8 bright-lines, all GREEN (no `xfail` remaining):

1. Bundle survives when the group carries two distinct levers (d139)
2. Bundle cohesion cascade drops siblings atomically (d139)
3. Typed drop round-trips into the harness terminal verdict
4. Closed-vocab terminal reason (no colon suffix; structured fields)
5. H001 positive control — e943 stays under cap, no split fires
6. RCA-subcluster oversize payload slices into sub-batches (d139)
7. Lineage orphan stamping + positive control (e943)
8. Retry feedback durability on the iteration terminal-state ledger

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 22 replay (merge gate) | `pytest tests/integration/postmortem_replay/test_trial22_postmortem_replay.py -q` |
| Compiler + terminal + deploy units | `pytest tests/unit/test_proposal_slate_compiler.py tests/unit/test_terminal_reason.py tests/unit/test_deploy_eligibility.py tests/unit/test_stage3_prompt_sizer.py -q` |
| Trial 21 replay (no regression) | `pytest tests/integration/postmortem_replay/test_trial21_postmortem_replay.py -q` |
| Live LLM workbench (production-replay) — patches survive the compiler | `cli.py run --input <bundle> --llm-mode live-llm-only --profile <profile>` → expect `deepest_stage_reached: accepted`, `recorded_patches>0`, `GSO_TRIAL22_BUNDLE_DISSOLVED_V1` |

### Rollback

`export GSO_TRIAL22_SLATE_REPAIR=0` (or any individual sub-flag `=0`)
then redeploy. The per-workstream sub-flags allow surgical rollback of
a single behavior (e.g. `GSO_TRIAL22_LINEAGE_INVARIANT=0` disables only
the finalize lineage enforcement while keeping the bundle repair;
`GSO_TRIAL22_BUNDLE_DISSOLVE=0` restores the strict pre-fix behavior
where a singleton bundle drops as `bundle_invariant_violated`).

### Status

- [x] W1 — production-shape fixtures + 8 bright-line replay tests
- [x] W2 — three-phase compiler pipeline (per-proposal → cohesion sweep → group invariant); `BUNDLE_MEMBER_DROPPED_CASCADE`
- [x] W2.1 — singleton-bundle dissolution (live `fevm-prashanth` finding): a bundle reduced to one surviving member by an upstream producer drop is dissolved into a solo proposal instead of dropping as `bundle_invariant_violated` (`GSO_TRIAL22_BUNDLE_DISSOLVE` flag, default ON; `GSO_TRIAL22_BUNDLE_DISSOLVED_V1` marker)
- [x] W3 — durable retry feedback (`compiler_drop_summary` on `IterationCandidateLedgerEntry`; ledger-backed read path)
- [x] W4 — `SLATE_COMPILER_EMPTY`/`STAGE3_RETURNED_NONE`/`APPLIER_NO_OUTCOMES` enum + `IterationTerminalVerdict`
- [x] W5.0 — canonical lineage-key audit + admission marker key plumbing
- [x] W5.1 — `enforce_full_eval_lineage` orphan stamping + `best_accuracy` exclusion (gated finalize hook)
- [x] W6 — diagnosis assets plumbed into `SlateCompilerContext`; asset gate active
- [x] W7 — RCA-subcluster prompt slicing with H001 positive-control guard
- [x] W8 — `deploy_eligibility_from_loop_out` task values + deploy-task skip marker
- [x] All 8 Trial 22 postmortem-replay tests green; Trial 21 suite still green
- [ ] Production lever_loop run observed end-to-end with non-zero `GSO_PATCH_OUTCOME_V1` rows (requires live workspace deploy)

---

## Trial 23 — Optimizer Loop Repair: Correct at Source, Repair Not Drop, Authoritative Loop

**Master flag:** `GSO_TRIAL23_LOOP_REPAIR` (default ON). When OFF, every
Trial 23 sub-flag is forced OFF regardless of its own env var.

**Sub-flags (all default ON when master ON, except where noted):**

| Flag | Workstream |
|---|---|
| `GSO_TRIAL23_KEPT_INSUFFICIENT_AUTHORITATIVE` | W1 — kept_insufficient is the authoritative terminal |
| `GSO_TRIAL23_TARGET_HONEST_ACCEPTANCE` | W2 — demote attribution-drift accept with unresolved target debt |
| `GSO_TRIAL23_PIVOT_INPUTS` | W3 — populate prior_patch_family / prior_lever_set |
| `GSO_TRIAL23_RCA_MECHANISM_ROUTING` | W4 — RCA-kind → mechanism routing |
| `GSO_TRIAL23_ASSET_GROUNDING` | W5 — pre-generation asset grounding (observe-and-ground) |
| `GSO_TRIAL23_ASSET_GROUNDING_BLOCKING` | W5 — flip repair-diagnosis gate to blocking (**default OFF**; enable only after W7–W9 verified) |
| `GSO_TRIAL23_SUBCLUSTER_REAL_SLICE` | W6 — real partitioned re-dispatch of oversized RCA-subcluster prompts |
| `GSO_TRIAL23_SNIPPET_REPAIR` | W7 — repair invalid snippets before dropping |
| `GSO_TRIAL23_PIVOT_DESTINATION` | W8 — give the sole-lever pivot a destination |
| `GSO_TRIAL23_BUNDLE_REPAIR` | W9 — recompose same-lever bundles instead of dropping |
| `GSO_TRIAL23_PHASE_H_CONTRACT_GATE` | W10 — Phase H upload/render failure + stale scoreboard → non-deployable |

### Hypothesis

Trials 20–22 fixed patch *delivery*; the only mechanism that survives
every gate is `add_example_sql`, which is behaviorally inert for the
real RCAs. Trial 23 fixes generation *quality*, *recovery*, *loop
authority*, and *honest acceptance* under two laws: **correct-at-source**
(the patch is right the first time) and **repair-not-drop** (a wrong
patch is repaired or redirected, never silently dropped). The central
sequencing constraint: observe-only gates may only be promoted to
blocking (W5 pre-gen block, W10 deploy gate) *after* the repair/redirect
paths (W7–W9) exist, or the loop regresses to the all-dropped flatline.

### Watch Markers (positive — these SHOULD appear)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL23_KEPT_INSUFFICIENT_AUTHORITATIVE_V1` | W1 | Applied + kept_insufficient → terminal forced to `kept_insufficient` |
| `GSO_TRIAL23_TARGET_HONEST_ACCEPTANCE_V1` | W2 | Drift accept with unresolved target debt demoted to non-deployable |
| `GSO_TRIAL23_PIVOT_INPUT_RECOVERED_V1` | W3 | Terminal signature carries a real `prior_patch_family` / `prior_lever_set` |
| `GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1` | W5 | Resolved table/column slice injected before synthesis |
| `GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1` | W6 | Oversized RCA-subcluster prompt sliced into N real LLM calls |
| `GSO_TRIAL23_SNIPPET_REPAIR_V1` (`outcome=repaired`) | W7 | Invalid snippet repaired and re-validated before drop |
| `GSO_TRIAL23_PIVOT_DESTINATION_V1` (`outcome=pivot_landed`) | W8 | Sole-lever drop replaced by a landed multi-lever bundle |
| `GSO_TRIAL23_BUNDLE_RECOMPOSED_V1` | W9 | Same-lever multi-member bundle recomposed to solos instead of dropped |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-23)

| Marker / signal | Workstream | Means we regressed |
|---|---|---|
| `GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1` | W4 | RCA fell back to inert `add_example_sql` for a kind it cannot fix |
| `GSO_TRIAL23_SNIPPET_REPAIR_V1` (`outcome=repair_failed`) high-rate | W7 | Snippets dropped without a successful repair |
| `GSO_TRIAL23_PIVOT_DESTINATION_V1` (`outcome=pivot_emptied_slate`) | W8 | Pivot replacement also emptied the slate |
| `accepted_with_unresolved_target_debt` (deployable) | W2 | Deployable accept while target debt non-empty |
| Deploy ships on `upload_failed` / `render_failed` / stale scoreboard | W10 | A busted/stale Phase H bundle reached the deploy task |

### Per-phase replay gates

- **Phase 1 (W1–W3) gate:** d139 terminates `kept_insufficient` (not
  `no_applied_patches`); e943's 95.8% shows as `net_win_non_deployable`
  with `unresolved_target_debt`. Pinned in
  `tests/integration/postmortem_replay/test_trial23_postmortem_replay.py`.
- **W10 Phase H gate (e943 anchor):** the production e943 attempt
  (`339587654249993`) reported `bundle_status=assembly_failed` +
  `phase_h_upload_status=upload_failed` + stale scoreboard yet returned task
  SUCCESS and deploy ran. Pinned by two bright-lines in the same replay file:
  `deploy_eligibility_from_loop_out` keeps the task SUCCESS but marks the
  candidate non-deployable (`deploy_skip_reason=contract_health_blocked`), and
  an isolation case proves the upload-failure + stale-scoreboard coverage is
  new behind `GSO_TRIAL23_PHASE_H_CONTRACT_GATE` (flag-off restores legacy).
  Replay suite now 6 tests green; 0 regressions vs flag-off baseline.

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 23 replay (Phase 1 gate) | `pytest tests/integration/postmortem_replay/test_trial23_postmortem_replay.py -q` |
| W7/W8/W9 repair units | `pytest tests/unit/optimization/test_trial23_w7_snippet_repair.py tests/unit/optimization/test_trial23_w8_pivot_destination.py tests/unit/optimization/test_trial23_w9_bundle_repair.py -q` |
| W10 deploy gate + bundle crash fix | `pytest tests/unit/test_deploy_eligibility.py tests/unit/test_run_output_bundle.py -q` |
| Trial 21/22 replay (no regression) | `pytest tests/integration/postmortem_replay/test_trial21_postmortem_replay.py tests/integration/postmortem_replay/test_trial22_postmortem_replay.py -q` |
| Live LLM workbench (production-replay, `fevm-prashanth`) — a corrective mechanism that is **not** `add_example_sql` survives to the applier on a target QID with `behavioral_diff != unchanged` | `devtools/local_lever_workbench` run with `--llm-mode live-llm-only` |

### Rollback

`export GSO_TRIAL23_LOOP_REPAIR=0` then redeploy for a full emergency
rollback to pre-Trial-23 behaviour. Each sub-flag above also rolls back
surgically (e.g. `GSO_TRIAL23_PHASE_H_CONTRACT_GATE=0` restores the
legacy Trial 21/22 deploy posture where only `assembly_failed` /
`merge_gate_blocked` block deploy; `GSO_TRIAL23_BUNDLE_REPAIR=0` restores
the strict `bundle_invariant_violated` drop for same-lever bundles).

### Status

- [x] W0 — observe-only audit (`docs/architecture/trial-23-observe-only-audit.md`); re-prioritised phases
- [x] W1 — `kept_insufficient` authoritative (helper branch + harness B2 ordering + flag + tests)
- [x] W2 — target-honest acceptance: `NET_WIN_NON_DEPLOYABLE` demotion + control-plane drift demotion (legacy preserved under rollback)
- [x] W3 — reliable pivot inputs: `_infer_prior_family_from_signatures` + Plan 12 wiring
- [x] Phase 1 replay gate green (d139 kept_insufficient; e943 net_win_non_deployable)
- [x] W4 — RCA-kind → mechanism routing (routing module + runtime detector + prompt/skill guidance)
- [x] W5 — `asset_grounding.py` resolver + synthesis injection (blocking promotion stays default-OFF)
- [x] W6 — `subcluster_redispatch.py` + real N-call fan-out (flag-gated, RCA-subcluster scoped)
- [x] W7 — snippet repair loop (`snippet_repair.py` + C3 repair-before-drop)
- [x] W8 — pivot with a destination (`pivot_destination.py` + reactive re-prompt via `llm_response_override` re-entry, recursion-guarded)
- [x] W9 — bundle repair over drop (recompose same-lever multi-member bundle to solos; `GSO_TRIAL23_BUNDLE_RECOMPOSED_V1`)
- [x] W10 — Phase H contract boundary: list-safe `build_decision_trace_all` / `build_journey_validation_all` crash fix + `compute_deploy_eligibility` / `deploy_eligibility_from_loop_out` gate `upload_failed` / `render_failed` / stale scoreboard non-deployable (flag-gated, legacy preserved)
- [ ] Live LLM workbench proves a non-`add_example_sql` corrective mechanism survives to the applier on a target QID (requires live workspace / `fevm-prashanth` profile)

### Faithful e943 replay finding (2026-06-01) — next gap identified

Captured the real e943 target QID fresh from MLflow (parent run
`501649560474489`, task `339587654249993`) via
`local_lever_workbench capture --task-key lever_loop` and replayed it live
(`fevm-prashanth`, `live-llm-only`, real airline space
`01f143dfbeec15a3a0e87ced8662f4ed`, 407 schema cols injected). Flag-on vs
flag-off both run on the genuine `airline_ticketing_and_fare_analysis_gs_009`
hard case.

What works: Stage 1 diagnoses `extra_defensive_filter` (high confidence,
actionable) and W4 routing proposes a corrective `add_instruction` alongside
`add_example_sql` (`MECHANISM_COVERAGE` covers `instruction_text`) — i.e. the
loop no longer defaults to the inert example-SQL alone.

Why the positive criterion still isn't met (and it is **not** a Trial 23
regression — flag-on and flag-off fail identically here): the synthesizer
emits a single-lever (lever-5) bundle whose lead member (`add_instruction`)
is dropped by the slate compiler as `unjustified_single_lever` /
`failing_check=required_assets` (the corrective instruction names no grounded
implicated asset), and the sibling `add_example_sql` then cascades out as
`bundle_member_dropped_cascade` / `bundle_cohesion` → `survivor_count=0` →
`stage3_returned_none`. None of the Trial 23 repair hooks cover this drop
family: W7 fires only on a *declined* snippet (this snippet was `stamped`),
W8 fires only on the strategist `sole_lever_in_rejected_family` gate (this
empty came from the slate compiler, not the strategist), and W9 recomposes
only `bundle_invariant_violated` (this was `unjustified_single_lever`). W5
asset grounding resolves assets into the prompt but, observe-only, does not
populate the proposal's `required_assets`, so the slate contract still drops
it.

Next gap (candidate for a follow-up workstream, outside the Trial 23 plan):
an `unjustified_single_lever` / `required_assets` + `bundle_member_dropped_cascade`
repair path — either ground the corrective instruction's `required_assets`
from the resolved asset slice (promote W5 from prompt-injection to
proposal-field population) before the slate `required_assets` check, or treat
the cohesion-cascade drop of an otherwise-valid sibling as a recompose-to-solo
(extend W9 beyond `bundle_invariant_violated`). Reproducible fixtures:
`devtools/local_lever_workbench/runs/trial23_e943_real{,_on,_off}/`.

This Trial 23 follow-on gap became **Trial 24**.

---

## Trial 24 — Kit at Source for Example-SQL-Insufficient RCAs

**Master flag:** `GSO_TRIAL24_KIT_AT_SOURCE` (default ON). ANDs over every
sub-flag. `=0` restores exact pre-Trial-24 behaviour.

**Sub-flags (all default ON when master ON):**

| Flag | Workstream |
|---|---|
| `GSO_TRIAL24_REQUIRED_ASSETS_KIT_WAIVER` | W24.3 — kit-aware justification waiver |
| `GSO_TRIAL24_MECHANISM_AWARE_KIT` | FA — `patch_type`→mechanism kit detection when LLM mis-tags levers |
| `GSO_TRIAL24_FILTER_REMOVAL_SOLO` | FB — `extra_defensive_filter` reclassified as instruction solo |
| `GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING` | RR Phase 3 — generalize justification grounding beyond `_TRIAL24_KIT_FOR_RCA` allowlist |

**Standalone design doc:** [`trial-24-kit-at-source.md`](./trial-24-kit-at-source.md)
(rich rationale, mermaid flow, follow-on history, replay-readiness Phases 0–3).
This section is the canonical-tracker entry; the standalone doc is the
detailed plan.

### Hypothesis

The faithful e943 replay (`docs/runid_analysis/e94376a3-d8a6-4570-a605-9fe231e5f99c`,
target `airline_ticketing_and_fare_analysis_gs_009`, RCA `extra_defensive_filter`)
showed the loop correctly proposing a corrective `add_instruction` (not the inert
`add_example_sql`) — but it died upstream of every Trial 23 repair hook:

- the instruction was emitted as a lone single lever and dropped by
  `_check_required_assets` as `unjustified_single_lever`;
- its bundle sibling cascaded out via `bundle_member_dropped_cascade` →
  `survivor_count=0` → `stage3_returned_none`;
- flag-on and flag-off failed identically — not a Trial 23 regression.

Trial 24 makes Trial 23's `RCA_KIND_TO_FIXING_MECHANISMS` routing authoritative as
a **kit**: the corrective patch is born as a ≥2-lever-family kit that survives both
the slate `required_assets` and bundle-invariants contracts, AND adds a kit-aware
**justification waiver** so an instruction shipped inside a kit is not dropped as
`unjustified_single_lever` (the structural companion lever IS the justification).
Follow-ons A and B then handle two real-world live-LLM behaviours observed on the
e943 anchor: lever mis-tagging (FA, mechanism-aware kit detection) and the
filter-removal companion that is properly a solo instruction (FB).

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W24.1 | `_TRIAL24_KIT_FOR_RCA` extension (`extra_defensive_filter`→{5a,6}; `top_n_cardinality_collapse`→{6,1}); merged into `kit_for_rca_violation_reason` + `next_companion_family_from_kit` via `_kit_for_rca_companions`, flag-gated. | `stages/action_groups.py` | enforce |
| W24.2 | Mandatory-kit clause added to BOTH Stage 3 builders + plan11_synthesize SKILL item 10. | `stages/synthesize.py`, `skills/plan11_synthesize/SKILL.md` | enforce |
| W24.3 | Kit-aware `required_assets` waiver: `in_multi_lever_kit` on `required_assets_for_patch_family`; bundle-derived `kit_member_intent_ids` in `compile_slate`. | `repair_diagnosis.py`, `proposal_slate_compiler.py` | enforce |
| W24.4 | Snippet member grounding verified (`_t22_assets_by_intent_id` ← `effective_blame_set`); kit-survives-slate test. | `stages/synthesize.py` | enforce |
| W24.5 | `trial24_flags.py` + `GSO_TRIAL24_KIT_FORCED_V1` audit marker (RCA + companion set + emitted levers + `kit_satisfied`). | `trial24_flags.py`, `stages/synthesize.py` | enforce |
| W24.6 | Deterministic replay + unit gates; 0 regressions. Live e943 proof. | tests below | deterministic enforce, live ✅ after FA+FB |
| FA | Mechanism-aware kit detection: `_bundle_distinct_mechanisms` + OR-acceptance in W24.3 pre-scan and Phase 2 bundle invariant. | `proposal_slate_compiler.py` | enforce |
| FB | Filter-removal solo: `extra_defensive_filter` dropped from forced-kit map, synthesis justification fallback, `snippet_noop_suppression` decline + synthesis degrade-to-solo, Stage 3 prompt + SKILL update. | `stages/action_groups.py`, `stages/synthesize.py`, `producer_snippet_validator.py`, `llm_abstain.py` | enforce |
| RR Phase 3 | Generalize FB2 grounding to any `INSTRUCTION_TEXT` solo proposal (not just `_TRIAL24_KIT_FOR_RCA` allowlist), gated by `GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING`. Plus Phase 0–2/1 test-debt cleanup so the regression gate is trustworthy. | `trial24_flags.py`, `stages/synthesize.py` | enforce |

### Watch Markers (positive — these SHOULD appear)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL24_KIT_FORCED_V1` | W24.5 | Forced kit fired for an allowlist RCA; payload carries `rca_kind`, companion set, emitted levers, `kit_satisfied` |
| `GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1` | FB grounded solo | A corrective `add_instruction` lands as a justified solo (justification grounded from `expected_behavioral_change`→`rationale`) instead of dropping `unjustified_single_lever` |
| `GSO_PATCH_OUTCOME_V1` (mechanism != `add_example_sql`) on a target QID with `behavioral_diff != unchanged` | W24.6 live proof | The behaviorally-impactful corrective mechanism actually lands and changes question behavior — the long-owed live promotion bar |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-24)

| Anti-marker | Where it surfaced | Trial 24 fix |
|---|---|---|
| `unjustified_single_lever` drop of a corrective `add_instruction` whose companion was dropped earlier in the same bundle | Slate compiler (e943) | W24.3 kit-aware waiver pre-scan; FA mechanism-aware OR; FB synthesis-time justification grounding |
| `bundle_member_dropped_cascade` → `survivor_count=0` → `stage3_returned_none` when a corrective instruction was emitted | Slate compiler (e943) | W24.2 mandatory-kit clause in Stage 3; W24.3 + FA keep both members alive |
| `singleton` hard-reject of an `extra_defensive_filter` instruction solo | KIT_FOR_RCA | FB reclassification removes it from forced-kit map; degrade-to-solo is intentional |
| No-op suppression snippet (`WHERE 1=1` / `WHERE TRUE`) reaching the slate | Producer validator | FB `snippet_noop_suppression` decline + Stage 3 SKILL guidance |
| Trial 24 ON regresses any prior trial's bright-line | Cross-trial | W24.6 full sweep `9636 passed / 0 failed`; Trial 20/21/22/23 replay suites all stay green |

### Bright-Line Replay Suite (the merge gate)

`tests/integration/postmortem_replay/test_trial24_postmortem_replay.py` and
`test_trial24_general_grounding_replay.py` — bright-lines, all GREEN:

1. Trial 24 kit-at-source replay: kit survives flag-on; instruction drops flag-off (deterministic on e943 fixture)
2. Follow-on B filter-removal solo replay: grounded instruction lands solo flag-on; ungrounded drops
3. General grounding non-allowlist fixture (Leg 2): solo corrective instruction on a non-allowlist RCA survives flag-on, drops flag-off

### Local Verification (mandatory before deploy)

These rows are the local pre-flight gates the harness must run (and pass)
before any deploy. The live-LLM behavioral-diff proof is NOT a local
verification step — it is the open LIVE-TRIAL item itself (see `### Status`
below) and runs through `gso-lever-loop-replay` AFTER deploy.

| Check | Command |
|---|---|
| Trial 24 replay (merge gate) | `pytest tests/integration/postmortem_replay/test_trial24_postmortem_replay.py tests/integration/postmortem_replay/test_trial24_general_grounding_replay.py -q` |
| Kit + required-assets + slate + snippet units | `pytest tests/unit/test_kit_for_rca.py tests/unit/test_required_assets_for_patch_family.py tests/unit/test_trial24_kit_survives_slate.py tests/unit/test_trial24_mechanism_aware_kit.py tests/unit/test_trial24_followons_filter_removal_solo.py tests/unit/test_producer_snippet_validator.py -q` |
| Trial 21/22/23 replay (no regression) | `pytest tests/integration/postmortem_replay/ -q` |
| Full authoritative suite (replay readiness) | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

Note: the last row expects ~9636 tests passing with 0 failed at the time
this trial was authored. The harness should treat any FAILED count > 0 as
LOCAL_VERIFICATION_RED; a drift in the PASSED count (more tests added)
is acceptable.

### Rollback

`export GSO_TRIAL24_KIT_AT_SOURCE=0` then redeploy for a full emergency
rollback to pre-Trial-24 behaviour. Each sub-flag also rolls back
surgically (e.g. `GSO_TRIAL24_REQUIRED_ASSETS_KIT_WAIVER=0` keeps the forced kit
but restores the strict per-proposal justification gate;
`GSO_TRIAL24_FILTER_REMOVAL_SOLO=0` returns `extra_defensive_filter` to the
forced-kit map). Base `KIT_FOR_RCA` constant is never mutated, so flag-off is
byte-stable.

### Status

- [x] W24.1 — `_TRIAL24_KIT_FOR_RCA` extension + `_kit_for_rca_companions` plumbing
- [x] W24.2 — mandatory-kit clause in both Stage 3 builders + plan11_synthesize SKILL
- [x] W24.3 — kit-aware `required_assets` waiver (bundle-derived `kit_member_intent_ids`)
- [x] W24.4 — snippet member grounding (`_t22_assets_by_intent_id` ← `effective_blame_set`)
- [x] W24.5 — `trial24_flags.py` + `GSO_TRIAL24_KIT_FORCED_V1` marker
- [x] W24.6 — deterministic replay + unit gates green, 0 regressions
- [x] FA — mechanism-aware kit detection (`_bundle_distinct_mechanisms`)
- [x] FB — filter-removal solo (reclassify + ground + decline no-op snippet + Stage 3 SKILL)
- [x] RR Phase 0–2 — full unit/integration suite green (`9636 passed / 0 failed`)
- [x] RR Phase 3 — generalized `INSTRUCTION_TEXT` justification grounding flag-gated; Leg 1 e943 re-confirmed; Leg 2 deterministic non-allowlist bright-line pinned
- [ ] Live `behavioral_diff != unchanged` applier proof on BOTH canonical
  anchors (`e94376a3` airline, `d13938e7` 7now) via `gso-lever-loop-replay`
  after deploy. This is the long-owed live promotion bar; default-ON was
  accepted as a monitored ship ahead of this live signal. **Original parent
  runs `501649560474489` (airline) and `807620338215711` (7now) are RETIRED**
  — see "Live Verification Status" below. Trial 25 rotates to fresh parents.

### Live Verification Status (2026-06-06)

Two replay attempts were made on the original parent runs (airline
`501649560474489`, 7now `807620338215711`) during the overnight
`/goal next-plan` session. Both attempts ran the GSO state machine
**end-to-end successfully** (28.5 min airline, 25.0 min 7now;
`loop_out` fully populated) but the final post-loop "Publishing Task
Values" cell crashed with the Databricks platform error:

```
INVALID_PARAMETER_VALUE: A maximum of 250 task values per job run is allowed
```

The verdict is `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` (new code,
classification `D_PLATFORM_INFRA`) — not a Trial 24 regression. The
Databricks Jobs platform tracks `dbutils.jobs.taskValues.set` writes
cumulatively per parent job run; each lever-loop replay publishes ~11-12
values, layered on ~45 values per full anchor replay (preflight +
baseline_eval + lever_loop + enrichment + finalize). After ~5-6 anchor
replays a parent crosses the 250 ceiling and every subsequent retry
crashes on the first `setJson("scores", …)` call inside the same parent
run. Both original anchor parents are now **permanently dead**:

| Anchor | Parent run | Replay attempts | Latest failed task_run_id | Status |
|---|---|---|---|---|
| airline (`e94376a3`) | `501649560474489` | 22 (last 2 failed identically) | `502350180589777` | RETIRED — task-value budget exhausted |
| 7now (`d13938e7`)    | `807620338215711` | 21 (last 2 failed identically) | `12690262598700`  | RETIRED — task-value budget exhausted |

**What we know about Trial 24's actual live behaviour** (updated
2026-06-06 after MLflow marker extraction from driver stdout — see
`docs/runid_analysis/<task_run_id>/trial24_behavioral_diff_summary.json`
and `…/trial24_runtime_markers.txt` per anchor): the GSO loop ran
4 iterations on each anchor before exhausting its iteration budget,
and the platform-side bookkeeping fault only hit the final publish
cell — the Stage 1-5 markers and per-iteration acceptance records
were all written to MLflow during the run. The extracted evidence
shows Trial 24's live promotion bar is **NOT held** on either anchor:

| Anchor | iters completed | `TRIAL24_KIT_FORCED_V1` count | `KEPT_INSUFFICIENT` records | `behavioral_diff != "unchanged"` | best_accuracy | optimizer_outcome |
|---|---|---|---|---|---|---|
| airline | 4 / 5 | **0** | 6 | **0**       | 87.5% | `OPTIMIZER_TRIED_INSUFFICIENT_GAIN` |
| 7now    | 4 / 5 | **0** | 6 | **0**       | 86.96% | `OPTIMIZER_TRIED_INSUFFICIENT_GAIN` |

**Trial 24's kit-at-source gate never fired on either anchor**
(`TRIAL24_KIT_FORCED_V1 = 0` on both), so the trial's central
mechanism was not exercised in production. The reasons differ per
anchor:

- **Airline**: the live RCA pipeline produced kinds
  `wrong_aggregation`, `wrong_column`, and `plural_top_n_collapse` —
  **none of which are in Trial 24's kit map**
  `{extra_defensive_filter, top_n_cardinality_collapse}`. The map
  is too narrow to match the actual airline RCA distribution.
- **7now**: the live RCA pipeline emitted English-label values like
  `"Top-N cardinality collapse via spurious RANK()=1 filter"` that
  **never normalised to the canonical key**
  `top_n_cardinality_collapse`. The cluster *described* exactly the
  failure mode Trial 24's map targets but the kind-normalisation
  layer in `repair_diagnosis.py` (or upstream) didn't reduce the
  English label to the canonical key.

Trial 20 grounded-solo (`GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1`)
fired twice on 7now during iter 1, but both times on `add_example_sql`
mechanisms — which the Trial 24 promotion bar explicitly excludes
("behaviorally-impactful corrective mechanism != `add_example_sql`").

**Additional finding (separate bug — candidate for its own trial):**
`add_sql_snippet_filter` mechanism patches fail at the applier with
`outcome=applyability_rejected`, `outcome_reason=apply_failed:Invalid
serialized_space: Unknown field 'name'`. This blocked iter 4 on
airline (both targets) and iter 2 on 7now (both targets). That is a
real applier bug independent of Trial 24's kit gate — when fixed it
may resurface as a Trial 24 kit-gate dependency (the
`add_sql_snippet_filter` mechanism is part of the corrective kit for
`extra_defensive_filter`). File as a Trial-25-or-26-adjacent fix
candidate.

**Trial 24's `[ ]` live-verification status remains open**, but
re-running the same Trial-24 code under cold-start would produce the
**same NOT_PROVEN result** — the kit gate's failure to fire is a
structural design gap in the kit map + RCA-kind normalisation, not a
non-determinism that another run would fix. The harness blockers
(parents dead, AGENTS.md hardcoding the dead IDs, /goal watcher
wakeup flaw) are addressed by **Trial 25** below; the
kit-gate-doesn't-fire structural gap is addressed by **Trial 26**
(authored as a follow-on; see below).

## Trial 25 — Replay-Budget Scalability for the Anchor Parent Runs

**Master flag:** `GSO_TRIAL25_HANDOFF_COMPACT` (default ON). ANDs over
every sub-flag. `=0` restores the pre-Trial-25 per-key `taskValues.set`
fan-out so consumers can roll back if a downstream consumer regresses.

**Sub-flags (all default ON when master ON):**

| Flag | Workstream |
|---|---|
| `GSO_TRIAL25_LEVER_LOOP_JSON_BLOB` | W25.1 — consolidate `run_lever_loop.py` post-loop publish into a single JSON blob |
| `GSO_TRIAL25_PREFLIGHT_JSON_BLOB`  | W25.2 — consolidate `run_preflight.py` publish |
| `GSO_TRIAL25_BASELINE_JSON_BLOB`   | W25.3 — consolidate `run_baseline.py` publish |
| `GSO_TRIAL25_FINALIZE_JSON_BLOB`   | W25.4 — consolidate `run_finalize.py` publish |
| `GSO_TRIAL25_REPLAY_BUDGET_GATE`   | W25.5 — pre-replay guardrail in `gso-lever-loop-replay` |

### Hypothesis

`PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` is a **harness
scalability** failure, not a GSO-logic failure. The current
per-anchor replay budget is ~5-6 attempts on a single parent run, but
the long-running optimisation funnel pattern requires many more
replays per anchor (Trials 18-24 have already produced ~20 replays
each on `501649560474489` and `807620338215711`). Two architectural
levers compound to a >40× extension of the practical budget without
changing GSO logic or consumer semantics:

1. **Compact every job-notebook publish into ≤2 `taskValues.set`
   calls** (one structured JSON blob keyed `<task>_outputs`, plus an
   optional small `<task>_status` for fast-path consumers). The
   existing `HandoffSource.TASK_VALUES` / `DELTA_FALLBACK` abstraction
   in `jobs/_handoff.py` already isolates consumers from the
   underlying schema, so a typed `LeverLoopOutputs` / `PreflightOutputs`
   /etc. dataclass at the publish boundary plus a parallel typed read
   at every consumer site is a generalizable, RCA-rooted, SM-resident
   refactor with bounded blast radius. Going from ~45 values per
   anchor replay to ≤10 lifts the per-parent budget from ~5-6 to
   ~25+ replays. Combined with rotating each anchor to a fresh parent
   when a new trial campaign opens, the practical headroom is
   effectively unbounded.

2. **Pre-trigger budget guardrail in `gso-lever-loop-replay`.** Before
   scheduling a replay, query the parent run's accumulated task-value
   count via `databricks jobs get-run --include-resolved-values` and
   refuse to schedule when `count > 200` with an actionable
   `PARENT_RUN_NEAR_BUDGET_CEILING_<count>` verdict that names the
   rotation skill (`gso-lever-loop-trigger`). This converts a silent,
   end-of-replay platform crash into an explicit, pre-replay
   harness-side rejection that the operator (and `/goal`) can route
   around.

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W25.1 | `LeverLoopOutputs` dataclass + single `lever_loop_outputs` JSON publish; consumers in `_handoff.py` switch to typed read; rollback path preserves per-key writes when flag off | `jobs/run_lever_loop.py`, `jobs/_handoff.py`, `tests/unit/test_handoff_lever_loop_*.py` | plan |
| W25.2 | `PreflightOutputs` dataclass + single `preflight_outputs` JSON publish | `jobs/run_preflight.py`, `jobs/_handoff.py`, tests | plan |
| W25.3 | `BaselineOutputs` dataclass + single `baseline_outputs` JSON publish | `jobs/run_baseline.py`, `jobs/_handoff.py`, tests | plan |
| W25.4 | `FinalizeOutputs` dataclass + single `finalize_outputs` JSON publish | `jobs/run_finalize.py`, `jobs/_handoff.py`, tests | plan |
| W25.5 | Pre-trigger budget gate in `gso-lever-loop-replay` SKILL.md + helper script `scripts/check_parent_task_value_budget.py` | `docs/skills/gso-lever-loop-replay/SKILL.md`, `scripts/` | plan |
| W25.6 | AGENTS.md harness fixes: (a) parent_run_id rotation policy with named "current anchor parent runs" config, (b) replace `/goal` passive-watcher pattern with active-poll-with-periodic-EVIDENCE (or foreground blocking wait + heartbeat EVIDENCE) | `AGENTS.md`, `docs/llmdrivenarchitecture/goalMode/*.md` | plan |
| W25.7 | `gso-postmortem` SKILL.md entry for `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` (and helper script to detect it from `jobs get-run-output` JSON) | `docs/skills/gso-postmortem/SKILL.md` | plan |
| W25.8 | Rotate anchor parent runs to fresh `gso-lever-loop-trigger`-issued runs; update AGENTS.md anchor constants; re-run Trial 24's live behavioral_diff proof on the fresh parents | `gso-lever-loop-trigger` invocation + AGENTS.md | plan |

### Watch Markers (positive — these SHOULD appear after Trial 25)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL25_HANDOFF_COMPACT_PUBLISH_V1{task,blob_keys,blob_bytes,prior_key_count}` | W25.1-4 | Single-blob publish replaced per-key fan-out for this task attempt |
| `GSO_TRIAL25_HANDOFF_COMPACT_READ_V1{task,blob_keys,reader}` | W25.1-4 | A downstream consumer successfully read the typed blob |
| `GSO_TRIAL25_BUDGET_GATE_PASSED_V1{parent_run_id,task_value_count,threshold}` | W25.5 | Pre-replay guardrail allowed the replay |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-25)

| Anti-marker | Where it surfaced | Trial 25 fix |
|---|---|---|
| `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` (Py4J `setJson` crash inside `Publishing Task Values` cell) | `run_*.py` post-loop publish on long-lived anchor parents | W25.1-4 compact publish + W25.5 pre-trigger gate + W25.8 fresh parents |
| Downstream consumer fails because `taskValues.get(key="scores")` returns `None` after flag-on | `_handoff.py`, `run_finalize.py`, etc. | W25.1 typed read path must be wired before W25.1 publish flips |
| `/goal` session sits idle for >30 min because the watcher script doesn't wake the agent | `/goal` end_turn + backgrounded poll loop never surfaces `BOTH_TERMINAL` | W25.6 active-poll-with-periodic-EVIDENCE pattern |

### Bright-Line Replay Suite (the merge gate)

- All Trial 18-24 replay suites stay green (no GSO-logic regression)
- New unit tests: `tests/unit/test_trial25_handoff_compact_publish.py`,
  `tests/unit/test_trial25_handoff_compact_read.py`,
  `tests/unit/test_trial25_replay_budget_gate.py`
- New integration test: `tests/integration/test_trial25_lever_loop_handoff_roundtrip.py`
  (publish → read across the in-process `_handoff.py` abstraction
  with both flag states)

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 25 handoff units | `pytest tests/unit/test_trial25_handoff_compact_publish.py tests/unit/test_trial25_handoff_compact_read.py tests/unit/test_trial25_replay_budget_gate.py -q` |
| Trial 25 roundtrip | `pytest tests/integration/test_trial25_lever_loop_handoff_roundtrip.py -q` |
| Trial 24 deterministic replay (no regression) | `pytest tests/integration/postmortem_replay/test_trial24_postmortem_replay.py tests/integration/postmortem_replay/test_trial24_general_grounding_replay.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL25_HANDOFF_COMPACT=0` then redeploy for a full
emergency rollback to per-key `taskValues.set` publish. Each sub-flag
also rolls back surgically per task (e.g.
`GSO_TRIAL25_LEVER_LOOP_JSON_BLOB=0` only restores `run_lever_loop.py`
publish, keeping the other three tasks compact). Consumer typed reads
auto-fall-back to per-key reads when the blob key is absent.

### Status

- [x] W25.1 — `LeverLoopOutputs` single-blob publish + typed read — landed 2026-06-06; `publish_task_outputs` helper in `_handoff.py`; `run_lever_loop.py` SKIP path (5 keys) + happy path (8 keys) + Trial 22 deploy gate (2 keys) + `debug_info` (1 key) all flow into ONE ``lever_loop_outputs`` blob; consumer side reads via 3-step lookup (blob → per-key → default) with per-(dbutils, taskKey) caching; flag `GSO_TRIAL25_LEVER_LOOP_JSON_BLOB` (default ON) for surgical rollback; covered by 34 new tests in `tests/unit/test_trial25_handoff_compact_{publish,read}.py` + `tests/unit/test_trial25_replay_budget_gate.py` + `tests/integration/test_trial25_lever_loop_handoff_roundtrip.py`. Used dict-of-values at the call site rather than a parallel `LeverLoopOutputs` dataclass — the typed consumer `get_lever_loop_outputs()` already enforces the schema and a publisher-side dataclass would have been redundant scaffolding.
- [x] W25.2 — `PreflightOutputs` compact publish + read — landed 2026-06-06; 15 sets → 1 blob; flag `GSO_TRIAL25_PREFLIGHT_JSON_BLOB`
- [x] W25.3 — `BaselineOutputs` compact publish + read — landed 2026-06-06; 6 sets → 1 blob; flag `GSO_TRIAL25_BASELINE_EVAL_JSON_BLOB` (note: task name in env var matches the `_handoff.py` taskKey of `baseline_eval`, not the human-friendly `BaselineOutputs` name)
- [x] W25.4 — `FinalizeOutputs` compact publish + read — landed 2026-06-06; 5-10 sets (UC promotion branch) → 1 blob; flag `GSO_TRIAL25_FINALIZE_JSON_BLOB`
- [x] W25.5 — pre-trigger budget gate in `gso-lever-loop-replay` — landed 2026-06-06; new `scripts/check_parent_task_value_budget.py` CLI (exit 0=pass, 10=blocked, 11=cli-failure-treat-as-unsafe); `gso-lever-loop-replay/SKILL.md` Step 1a invokes it before any Genie/Delta mutation and the dry-run summary block now quotes the verdict line verbatim; default threshold 200 leaves 50-entry safety margin under the 250-entry platform cap. **NOT in scope for Trial 25:** `run_enrichment.py` still emits 3-7 per-key sets per replay (W25.1-4 covered only preflight/baseline/lever_loop/finalize per tracker scope). Per-replay budget cost is now ~10-12 sets (down from ~45+), so practical replay headroom per parent is ~20-25 (up from 5-6) even without compacting enrichment. Follow-up to compact enrichment can be filed as Trial 25.x if budget pressure re-emerges.
- [x] W25.6 — AGENTS.md harness fixes (parent rotation policy + foreground-blocking watcher) — landed 2026-06-06; see AGENTS.md invariant 7 + step 7b + new `packages/genie-space-optimizer/docs/architecture/canonical-anchors.md`
- [x] W25.7 — `gso-postmortem` SKILL.md entry for `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` — landed 2026-06-06
- [ ] W25.8 — rotate anchor parents (`canonical-anchors.md` Current table) AND deploy with W25.1-5 active before re-running any live verification. **NOTE 2026-06-06:** rotation alone won't surface new Trial 24 evidence because the Trial 24 kit gate doesn't fire on the live RCA population (see Trial 24 Live Verification Status above and Trial 26 below). Defer this workstream until Trial 26's RCA-normalization fix lands.

## Trial 26 — RCA-Kind Normalisation + Kit-Map Coverage So Trial 24 Can Actually Fire

**Master flag:** `GSO_TRIAL26_KIT_GATE_REACHABLE` (default ON). ANDs over
every sub-flag. `=0` restores pre-Trial-26 normalization + kit-map
coverage exactly.

**Sub-flags (all default ON when master ON):**

| Flag | Workstream |
|---|---|
| `GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE` | W26.1 — English label → canonical RCA-kind key via LLM-validated typed normaliser |
| `GSO_TRIAL26_KIT_MAP_EXPANDED`             | W26.2 — extend `_TRIAL24_KIT_FOR_RCA` to cover the live airline RCA kinds (`wrong_aggregation`, `wrong_column`, `plural_top_n_collapse`) |
| `GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX`     | W26.3 — fix `add_sql_snippet_filter` applier emitting `name` field on serialized_space (root cause of `Invalid serialized_space: Unknown field 'name'`) |

### Hypothesis

Trial 24's kit-at-source synthesis is architecturally correct — it
produces ≥2-lever kits that survive the slate `required_assets`
gate and the bundle-invariants contract, and the FB grounded-solo
path handles the filter-removal case. **But the gate that decides
"is this RCA a Trial 24 candidate?" never fires on the live RCA
population on either canonical anchor** (`TRIAL24_KIT_FORCED_V1=0`
in both 502350180589777 and 12690262598700 MLflow marker streams).
Two structural gaps are responsible:

1. **The kit map is too narrow** —
   `_TRIAL24_KIT_FOR_RCA = {extra_defensive_filter, top_n_cardinality_collapse}`
   does not cover the actual RCA distribution observed on airline
   (`wrong_aggregation`, `wrong_column`, `plural_top_n_collapse`).
   Even with perfect normalisation, zero airline RCAs would map
   into the kit. The map needs to span the live distribution; the
   FA mechanism-aware detection added in Trial 24 is necessary but
   not sufficient.

2. **RCA-kind normalisation drift** — on 7now, the live RCA pipeline
   emits English-label values like
   `"Top-N cardinality collapse via spurious RANK()=1 filter"` that
   match the *intent* of the canonical key
   `top_n_cardinality_collapse` but do not normalise to it. The
   canonical key is the dictionary lookup the kit map uses, so any
   label that fails to reduce keeps the kit gate shut. The
   normalisation layer in `repair_diagnosis.py` (and any upstream
   RCA-kind producer in `stages/diagnose.py`) needs a typed,
   LLM-validated reduction step: deterministic code maps to a
   bounded enum where possible, LLM judgment classifies free-form
   English into the same enum, and an aligned scorer (offline
   evaluator) gates that the classification is doing what the
   downstream kit map expects.

3. **`add_sql_snippet_filter` applier bug (separate, blocking)** —
   2/8 patch attempts across both anchors failed with
   `outcome=applyability_rejected`,
   `outcome_reason=apply_failed:Invalid serialized_space: Unknown
   field 'name'`. This means the applier is emitting a `name` field
   on a serialized_space object that the Databricks Genie API
   rejects. Even if the kit gate fires post-Trial-26, the kit's
   `add_sql_snippet_filter` member will continue to be rejected
   until this is fixed. Bundled into Trial 26 because it's part of
   the same "Trial 24 kit can never land in production" failure
   chain.

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W26.1 | Typed RCA-kind canonical normaliser landed as `optimization/rca_kind_canonical.py` with four-tier ladder: `deterministic` (input already a canonical key) → `alias` (curated shorthand table — `top_n_collapse`, `plural_top_n_collapse`, `defensive_filter`, `col_disambig`) → `keyword` (12 curated regex patterns covering every kit-map key + inflection-tolerant — the production `"Top-N cardinality collapse via spurious RANK()=1 filter"` reaches `top_n_cardinality_collapse` via this tier) → optional `llm` (only when `w` is provided AND the W26.1 sub-flag is ON; clamped to the canonical set; current `_invoke_llm_tier` hook raises `NotImplementedError` and the wiring is owed by W26.4 as a typed `LlmReasoningCall`). The canonical key set is the single-source-of-truth union of `KIT_FOR_RCA ∪ _TRIAL24_KIT_FOR_RCA ∪ _TRIAL26_KIT_FOR_RCA ∪ {"unknown_kind"}`. Results are typed (`RcaKindCanonical(canonical_key, confidence, via, raw_label)`), per-process memoised, and every call emits `GSO_TRIAL26_RCA_CANONICAL_V1{raw_label,canonical_key,confidence,via}`. Integrated into `stages/action_groups._normalize_rca_kind` (the single chokepoint every kit-map lookup funnels through) — so every kit-gate decision now sees the canonical key. Sub-flag OFF falls back to legacy `.strip().lower()` byte-stably; master flag OFF forces sub-flag OFF. Defense in depth: a raising LLM tier → `via="llm_error"` → `canonical_key="unknown_kind"`; an off-canonical LLM return → `via="llm_invalid"` → clamped to `unknown_kind`. | `optimization/rca_kind_canonical.py` (new), `stages/action_groups._normalize_rca_kind` (integration) | completed (33 unit tests; full unit suite 9780/9780 green) |
| W26.2 | Extend `_TRIAL24_KIT_FOR_RCA` to include `wrong_aggregation`, `wrong_column`, `plural_top_n_collapse` (each with its corrective mechanism family). The kit composition follows the same shape as the existing Trial 24 entries (≥2-lever kit; matched companion families). Mechanism families derive from the existing `RCA_KIND_TO_FIXING_MECHANISMS` Trial 23 routing — Trial 26 wires those into Trial 24's kit-at-source synthesis path. | `stages/action_groups.py`, `proposal_slate_compiler.py` | plan |
| W26.3 | Fix `add_sql_snippet_filter` applier emitting `name` on serialized_space. Locate the applier dispatcher in `applier/` (or wherever `add_sql_snippet_filter` is translated to a serialized_space mutation), remove or rename the offending `name` field per the canonical `serialized_space` schema (`backend/references/schema.md`), add a deterministic typed builder + unit test covering both the happy path and the regression on the airline iter-4 / 7now iter-2 patch payloads. | `applier/`, `tests/unit/test_trial26_applier_snippet_name_fix.py` | plan |
| W26.4 | Bright-line merge gate landed in three pieces: (1) `tests/eval/test_rca_kind_canonical_normaliser_alignment.py` — 26-row curated corpus mined from `tests/replay/anchors/fixtures/`, `tests/integration/postmortem_replay/fixtures/`, `tests/fixtures/trial19_postmortem/`, and the Trial 26 plan; pins both RESOLVE (every label must canonicalise) and UNKNOWN (false-positives banned) at the tracker's ≥95% threshold. (2) `tests/integration/postmortem_replay/test_trial26_kit_map_coverage_replay.py` — 8 tests proving the kit gate fires end-to-end for every Trial 26 label form (canonical key, alias, English keyword) AND that the observability markers (`GSO_TRIAL26_KIT_MAP_EXPANDED_V1` for W26.2, `GSO_TRIAL26_RCA_CANONICAL_V1` for W26.1) carry well-formed payloads. (3) The W26.3 applier test (`tests/unit/test_trial26_applier_snippet_name_fix.py`) includes the typed end-to-end check against `genie_schema.SqlSnippetFilter` proving the regression payload from airline iter-4 / 7now iter-2 no longer hits `Unknown field 'name'`. Trial 18-24 replay suites confirmed green (`tests/integration/postmortem_replay/` 64 tests, 0 regressions; `tests/eval/` 29 tests, 0 regressions; full unit 9865 passed, 0 failures). LLM tier 4 wire-up (typed `LlmReasoningCall`) deferred until W26.5 live evidence shows a false-negative the deterministic ladder cannot catch. | new `tests/eval/__init__.py`, `tests/eval/test_rca_kind_canonical_normaliser_alignment.py`, `tests/integration/postmortem_replay/test_trial26_kit_map_coverage_replay.py` | completed (37 new tests; full unit + replay + eval 9865 / 0) |
| W26.5 | Re-run Trial 24 live verification on fresh parent runs (Trial 25 W25.8) after Trial 26 lands. Acceptance criterion: at least one `GSO_TRIAL24_KIT_FORCED_V1` marker on each anchor AND at least one accepted patch with `behavioral_diff != "unchanged"` AND mechanism not in `{add_example_sql}`. | live verification (no code) | plan |
| W26.6 | **Map-driven kit-at-source synthesis prompt (the real W26.5 unblock).** W26.2 expanded the validator's `KIT_FOR_RCA` map but the Stage 3 synthesis prompt (`stages/synthesize.py`) still hard-coded only the two original Trial 24 kinds, so the producer was never told to emit a kit for `wrong_aggregation`/`wrong_column` — every such proposal died as `kit_for_rca_violation:rca=…:singleton` → `empty_synthesis` → `stage3_returned_none`. This desync silently red-lined the **entire** forward-pipeline integration suite (`to_proposed`/`to_normalized`/`to_applyable`/`to_applied`/real-production-row), which was outside the declared local-verification list. Fix: derive the kit-mandate enumeration from a new typed `action_groups.active_kit_for_rca_map()` (the same merged map the validator reads), so any map expansion is mirrored into the producer prompt with no further edit. Forward-pipeline mechanics tests diagnose a kit-FREE RCA (`KIT_FREE_RCA_KIND`) so the single-lever vehicle stays focused on funnel mechanics; kit-at-source coverage moves to `test_trial26_synthesis_kit_prompt` (producer prompt is map-driven) + `test_trial26_kit_map_coverage_replay` (gate enforcement rejects singleton / admits companion kit for every W26.2 kind). | `stages/synthesize.py`, `stages/action_groups.py` (`active_kit_for_rca_map`) | completed (offline; +6 tests; full unit+replay+eval 9842/29 green, all integration 437 green, pretrial gate 66/66) |

### Watch Markers (positive — these SHOULD appear after Trial 26)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL26_RCA_CANONICAL_V1{raw_label,canonical_key,confidence,via}` | W26.1 | RCA-kind normaliser reduced a free-form English label to a canonical kit-map key (`via` ∈ `deterministic|llm|allowlist`) |
| `GSO_TRIAL24_KIT_FORCED_V1` count `> 0` on a live anchor | W26.1+W26.2 | Trial 24's kit-at-source synthesis fired in production; the long-owed live promotion bar is now reachable |
| `GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1{fields,kept,dropped}` | W26.3 | Applier dispatcher logged the canonical field set it emitted to the genie API (`name` MUST NOT appear in `kept` post-Trial-26) |

### Anti-Success Markers (negative — these MUST NOT appear post-Trial-26)

| Anti-marker | Where it surfaced | Trial 26 fix |
|---|---|---|
| `GSO_TRIAL24_KIT_FORCED_V1` count `= 0` on a live anchor whose RCA distribution contains any kit-map key (Trial 24 + 26 combined map) | live trial postmortem | W26.1 normaliser bug |
| `outcome_reason=apply_failed:Invalid serialized_space: Unknown field 'name'` | applier | W26.3 |
| `GSO_TRIAL26_RCA_CANONICAL_V1.canonical_key="unknown_kind"` rate `> 30%` across a live anchor's iterations | W26.1 alignment regression | tighten normaliser via additional alignment data or expand map |

### Bright-Line Replay Suite (the merge gate)

- All Trial 18-24 replay suites stay green (no regression on existing fixtures)
- `tests/eval/test_rca_kind_canonical_normaliser_alignment.py` reports `≥95%` exact match on the curated (English-label, canonical-key) dataset
- `test_trial26_kit_map_coverage_replay.py` proves the kit gate fires for each newly-added kind on its anchor fixture
- `test_trial26_applier_snippet_name_fix.py` proves the canonical builder produces a serialized_space mutation the Genie API accepts (mocked) and the regression payload from airline iter 4 / 7now iter 2 no longer hits `Unknown field 'name'`

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 26 RCA normaliser alignment | `pytest tests/eval/test_rca_kind_canonical_normaliser_alignment.py -q` |
| Trial 26 kit-map coverage replay | `pytest tests/integration/postmortem_replay/test_trial26_kit_map_coverage_replay.py -q` |
| Trial 26 applier fix unit | `pytest tests/unit/test_trial26_applier_snippet_name_fix.py -q` |
| Trial 24 replay (no regression) | `pytest tests/integration/postmortem_replay/test_trial24_postmortem_replay.py tests/integration/postmortem_replay/test_trial24_general_grounding_replay.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL26_KIT_GATE_REACHABLE=0` then redeploy for emergency
rollback. Each sub-flag also rolls back surgically (e.g.
`GSO_TRIAL26_KIT_MAP_EXPANDED=0` shrinks the kit map back to Trial-24
coverage but keeps the normaliser; `GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX=0`
restores the broken `name` field if the applier fix regresses).

### Status

- [x] W26.1 — RCA-kind canonical normaliser (deterministic + alias + keyword tiers; LLM tier hook owed by W26.4) wired into `_normalize_rca_kind`
- [x] W26.2 — kit-map expansion to cover live airline RCA distribution (`wrong_aggregation`, `wrong_column`, `plural_top_n_collapse` alias)
- [x] W26.3 — `add_sql_snippet_filter` applier `name`-field fix (`display_name` emission in three producer paths)
- [x] W26.4 — bright-line replay suite + offline alignment test (LLM tier wire-up deferred to post-W26.5 if evidence demands it)
- [x] W26.6 — map-driven kit-at-source synthesis prompt (`stages/synthesize.py` reads `action_groups.active_kit_for_rca_map()`); closes the W26.2 producer/validator desync that stranded `wrong_aggregation`/`wrong_column` proposals as `kit_for_rca_violation:…:singleton`. Caught a suite-wide forward-pipeline regression (5 integration tests outside the declared local-verification list). +6 tests; full unit+replay+eval 9842/29 green, all integration 437 green, pretrial gate 66/66. This is the offline prerequisite that makes W26.5's live acceptance reachable.
- [x] W26.7 — **Trial 25 W25.2 cold-start handoff reader-migration regression — FIXED offline (compact-aware readers).** Routed every cross-task read in `run_baseline.py`/`run_enrichment.py`/`run_deploy.py`/`run_finalize.py` through `_handoff._tv_get` (compact `<task>_outputs` blob → per-key fallback → default); `run_deploy` bool parse fixed (`bool("False")` was truthy). Added source-scan guard `tests/unit/test_trial26_w26_7_coldstart_handoff_readers.py` (fails on any raw `dbutils.jobs.taskValues.get(` in `run_*.py`). Verified: all 9 `run_*.py` zero raw cross-task reads; +2 guard tests; existing handoff 33 green; full unit+replay 9844; pretrial gate 66/66. (Original diagnosis below.) After the W26.6 deploy, cold-start parents `335104024979293` (airline) / `631994889494024` (7now) both died `INTERNAL_ERROR` at `baseline_eval` (preflight SUCCESS, lever_loop SKIPPED-upstream-failed — so the W26.6 kit fix never ran). RCA: `run_preflight.py:624` publishes the compact `preflight_outputs` blob via `publish_task_outputs` (Trial 25 W25.2), but `run_baseline.py:223-236`, `run_enrichment.py:92-112`, `run_deploy.py:164/244-249`, `run_finalize.py:198` read cross-task values with RAW `dbutils.jobs.taskValues.get(taskKey=…, key=…)` instead of the compact-aware `_handoff._tv_get`. Latent under replay (baseline_eval is skipped) → only cold-start exposes it. `baseline_eval` error: `ValueError: No task values with key "run_id" were found`. Fix (planned, offline-first): route every cross-task read in the 4 job files through `_tv_get` (compact blob → per-key fallback → default), and add a source-scan guard test (check_invariants-style) so a raw cross-task read can never regress again. Then redeploy + re-trigger fresh parents + re-poll + postmortem. | `jobs/run_baseline.py`, `jobs/run_enrichment.py`, `jobs/run_deploy.py`, `jobs/run_finalize.py` + offline guard test | plan
- [ ] W26.5 — live re-verification. **RAN end-to-end after W26.7 (both anchors `TERMINATED SUCCESS`, full DAG green) — acceptance UNMET, two NEW blockers found.** Parents: airline `450001766723999` (final 95.65%, `verdict=LEVER_LOOP_SKIPPED_POST_ENRICHMENT_MEETS_THRESHOLDS` — baseline 91.3% already `thresholds_met`, enrichment→95.65%, Starting Point Gate skipped lever_loop so the kit gate never ran), 7now `517826776610889` (final 91.3%, `verdict=PLAN11_STAGE3_PROMPT_TOO_LARGE_RUN_STARVED` — 20/24 `plan11_synthesize` declined `prompt_too_large` 73170>40000, no behavior-changing patch; both hard QIDs reached `accepted` but every applied lever `behavioral_diff=unchanged`/`kept_insufficient`). **Positively verified:** W26.7 compact handoff readers fired (`GSO_TRIAL25_HANDOFF_COMPACT_READ_V1`), W26.6 crash symptom GONE (zero `kit_for_rca_violation:singleton`/`empty_synthesis`/`stage3_returned_none`), no deterministic per-QID shortcut. `GSO_TRIAL24_KIT_FORCED_V1` = 0 on both (kit gate never reached: airline skip-gate, 7now synthesis starvation). New blockers seed Trial 27. Postmortems: `runid_analysis/unresolved_450001766723999/`, `runid_analysis/d13938e7-405d-4444-833a-03f5ac9f7523__parent_517826776610889/`.

## Trial 27 — Stage 3 Synthesis De-Starvation (prompt_too_large) + Kit-Gate Reachability on a Sub-Threshold Anchor

**Master flag:** `GSO_TRIAL27_STAGE3_DESTARVE` (default ON). `=0` restores
pre-Trial-27 Stage 3 prompt assembly + verification-anchor behaviour.

### Hypothesis

Trial 26 landed W26.6 (map-driven kit-at-source prompt) and W26.7
(compact cold-start handoff readers); both verified working live (W26.6
crash symptom gone, W26.7 handoff confirmed). But the W26.5 live run
showed neither anchor can yet demonstrate the kit gate firing, for two
distinct, newly-dominant reasons:

1. **7now — Stage 3 synthesis starvation (code-actionable, primary).**
   `verdict=PLAN11_STAGE3_PROMPT_TOO_LARGE_RUN_STARVED`: 20/24
   `plan11_synthesize` calls declined with `prompt_too_large`
   (73170 tokens vs the 40000 cap; `GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1`
   reported `over_cap=true`/`sub_cluster_split_needed`). With synthesis
   declined, no behavior-changing structural lever was ever produced →
   every applied lever recorded `behavioral_diff=unchanged` /
   `kept_insufficient`, the kit gate (`GSO_TRIAL24_KIT_FORCED_V1`) never
   fired, and the run stayed at baseline 91.3%. The Stage 3 prompt
   assembly must fit under the cap (sub-cluster split when
   `sub_cluster_split_needed`, and/or trim the cacheable static blocks —
   archetype catalog + lever menu — that dominate `cacheable_block_tokens`).
2. **airline — Starting Point Gate skips lever_loop (anchor/trial-design).**
   `verdict=LEVER_LOOP_SKIPPED_POST_ENRICHMENT_MEETS_THRESHOLDS`:
   baseline 91.3% already `thresholds_met=true`, enrichment→95.65%, so
   the lever loop never runs and the kit gate is unverifiable on airline
   at its current threshold. This is NOT a `src/` fix — it is a
   verification-anchor / threshold lever owned by the harness
   (`gso-lever-loop-replay` threshold params or a verification-only
   Starting-Point-Gate flag). Document, do not overfit.
3. **7now — RCA canonicaliser coverage gap (secondary, code-actionable).**
   5/7 `GSO_TRIAL26_RCA_CANONICAL_V1` labels resolved to `unknown_kind`
   (Stage 2 free-text routing narratives aren't covered by the
   deterministic/keyword tiers). Even once synthesis fits, an
   `unknown_kind` label can't match the kit map, so the kit gate stays
   shut. Extend the canonicaliser keyword tier (and/or wire the owed
   W26.1 LLM tier) so live Stage 2 narratives reduce to canonical keys.

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W27.1 | Stage 3 prompt fits the 40000-token cap on production-shaped clusters by extending Trial 23 W6 partitioned re-dispatch (already shipped for subcluster-id builders) to fire on ANY cluster with `sub_cluster_split_needed=true`. Bright-line #5 (H001 single-call path) preserved by the existing `if len(_w6_parts) > 1:` guard which falls through to single-call when the partition cannot split further. New marker `GSO_TRIAL27_W6_EXTENDED_V1` isolates the W27.1-attributable population for postmortems. Sub-flag `GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER` (default ON under master `GSO_TRIAL27_STAGE3_DESTARVE`) gives surgical rollback to the pre-Trial-27 subcluster-only gate. | `optimization/stages/synthesize.py` (W6 gate relaxation), `optimization/subcluster_redispatch.py` (`trial27_w6_extended_marker`), `optimization/trial27_flags.py`. Tests: `tests/unit/optimization/test_trial27_w6_extend_nonsubcluster.py` (5 tests: extension fires on non-subcluster, flag-OFF byte-stable, master-OFF byte-stable, partition len==1 falls through preserves bright-line #5, subcluster regression). | done |
| W27.2 | RCA canonicaliser keyword/LLM-tier coverage for live Stage 2 routing narratives so the `unknown_kind` rate on a live anchor drops below the Trial 26 anti-marker 30% bar; mined corpus from the 7now run's 7 `RCA_CANONICAL_V1` payloads. | `optimization/rca_kind_canonical.py` | plan |
| W27.3 | Verification-anchor / threshold lever so the kit gate is reachable on anchors whose baseline already meets thresholds (airline today). Pure decision function `should_skip_starting_point_gate` extracted from the `run_lever_loop` notebook so the gate is unit-testable, with per-run boolean signal `force_lever_loop` (set by the harness as a job parameter — `gso-lever-loop-replay` and `gso-lever-loop-trigger` skills both updated) and deploy-time capability flag `GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE` (default ON, kill-switch when OFF). When both signals fire AND `thresholds_met=true`, the gate emits observability marker `GSO_TRIAL27_FORCE_LEVER_LOOP_V1{would_have_skipped_reason, accuracy_source, post_enrichment_accuracy, baseline_accuracy}` and DOES NOT skip. No `src/` per-anchor / per-QID / per-space_id hardcode anywhere. | `optimization/starting_point_gate.py` (new), `jobs/run_lever_loop.py` (gate wiring + widget), `docs/skills/gso-lever-loop-{replay,trigger}/SKILL.md`, `optimization/trial27_flags.py`. Tests: `tests/unit/optimization/test_trial27_starting_point_gate.py` (10 tests covering thresholds-not-met no-skip, baseline/post-enrichment skip parity with the pre-Trial-27 notebook string, override engaged with both signals, capability-OFF kills override, master-OFF kills capability, marker payload shape, baseline-only no-post-enrichment-accuracy variant). | done |
| W27.4 | Re-run live verification on fresh parents after W27.1-2-3 land. Acceptance: ≥1 `GSO_TRIAL24_KIT_FORCED_V1` AND ≥1 accepted patch `behavioral_diff != "unchanged"` (mechanism != `add_example_sql`) on at least one anchor whose lever loop actually runs. For airline, trigger with `force_lever_loop=true` (the new W27.3 knob) since baseline 91.3% already reports `thresholds_met=true`. | live verification (no code) | plan |

### Watch Markers (positive)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1{over_cap:false}` on production clusters | W27.1 | Stage 3 prompt now fits the cap; synthesis no longer declines |
| `GSO_PLAN11_STAGE3_SYNTHESIS_V1{outcome:"proposed"}` with non-empty `bundle_ids` | W27.1 | synthesis produced a proposal where it previously declined |
| `GSO_TRIAL26_RCA_CANONICAL_V1{canonical_key != "unknown_kind"}` rate ≥ 70% on a live anchor | W27.2 | canonicaliser now resolves live Stage 2 narratives |
| `GSO_TRIAL27_W6_EXTENDED_V1` count ≥ 1 per live anchor whose Stage 3 saw `sub_cluster_split_needed=true` on a non-subcluster cluster | W27.1 | the W27.1 extension engaged in production (separates W27.1 dispatch from pre-existing subcluster-only W6 dispatch) |
| `GSO_TRIAL27_FORCE_LEVER_LOOP_V1` count ≥ 1 on the airline anchor (or any anchor whose `thresholds_met=true` would otherwise block lever_loop) | W27.3 | the verification override engaged and the lever loop ran; airline kit gate now exercisable |
| `GSO_TRIAL24_KIT_FORCED_V1` count `> 0` on a live anchor whose lever loop ran | W27.1+W27.2+W27.3 | the long-owed kit-gate production proof finally reachable |

### Anti-Success Markers (negative)

| Anti-marker | Meaning |
|---|---|
| `prompt_too_large` decline rate `> 10%` of `plan11_synthesize` calls on a live anchor | W27.1 incomplete |
| `GSO_TRIAL26_RCA_CANONICAL_V1.canonical_key="unknown_kind"` rate `> 30%` on a live anchor | W27.2 incomplete |
| any `if qid == …` / `if space_id == …` hardcode introduced to force the gate | Architectural Principle #1 violation — forbidden |

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Stage 3 prompt-size de-starvation unit/replay | `pytest tests/unit/stages/ tests/integration/test_sm_forward_pipeline_to_proposed.py -q` |
| RCA canonicaliser alignment (≥95%) | `pytest tests/eval/test_rca_kind_canonical_normaliser_alignment.py -q` |
| Cold-start handoff guard (no regression) | `pytest tests/unit/test_trial26_w26_7_coldstart_handoff_readers.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL27_STAGE3_DESTARVE=0` then redeploy.

### Status

- [x] W27.1 — Stage 3 prompt fits the 40000 cap on production clusters via Trial 23 W6 extension to non-subcluster builders; 5 offline tests + zero regressions across 9875-test suite. Sub-flag `GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER` (default ON), marker `GSO_TRIAL27_W6_EXTENDED_V1`. Bright-line #5 preserved by `len(_w6_parts) > 1` guard.
- [ ] W27.2 — RCA canonicaliser coverage so live `unknown_kind` rate < 30%
- [x] W27.3 — verification-anchor / threshold lever: pure decision function `should_skip_starting_point_gate` extracted into `optimization/starting_point_gate.py`; per-run signal `force_lever_loop` (harness job parameter) gated by deploy-time capability `GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE` (default ON); marker `GSO_TRIAL27_FORCE_LEVER_LOOP_V1` on engagement; 10 offline tests + zero regressions. `gso-lever-loop-replay` and `gso-lever-loop-trigger` skills updated to plumb the per-anchor knob. No `src/` per-anchor / per-QID / per-space_id hardcode.
- [ ] W27.4 — live re-verification: ≥1 `GSO_TRIAL24_KIT_FORCED_V1` + ≥1 accepted behavior-changing patch on an anchor whose lever loop runs (airline triggered with `force_lever_loop=true` via W27.3)

### Live Verification Results (Trial 27 replay — 2026-06-06)

Replayed both anchors after deploying W27.1+W27.3 (parents:
airline `450001766723999` REPAIR `320352026705603`, 7now
`517826776610889` REPAIR `132079122490923`; both `TERMINATED/SUCCESS`).
Postmortems: `docs/runid_analysis/e94376a3-…/postmortem.md` (airline,
`OPTIMIZER_TRIED_INSUFFICIENT_GAIN`) and `docs/runid_analysis/d13938e7-…/postmortem.md`
(7now, `STRUCTURAL_LEVER_NOT_REACHED_MECHANISM_DOES_NOT_COVER_BEHAVIOR_DELTA`).

- **W27.3 CONFIRMED live** — `GSO_TRIAL27_FORCE_LEVER_LOOP_V1` count=1 on
  airline; the lever loop ran 4 iterations despite `thresholds_met=true`
  (post-enrichment 95.65%), deepest_stage=`accepted`. The override works.
- **W27.1 PARTIAL** — in-loop Stage-3 synthesis (iters 1–4) all fit the
  40000 cap (0% `over_cap`, prior `PLAN11_STAGE3_PROMPT_TOO_LARGE_RUN_STARVED`
  did NOT recur in the loop). BUT the iteration-0 SEED pass still declines
  `prompt_too_large` (65k–74k tokens; W27.1's non-subcluster gate only
  fires in-loop). `GSO_TRIAL27_W6_EXTENDED_V1` count=0 on both (in-loop
  clusters fit WITHOUT needing the split, so the extension path wasn't
  exercised).
- **W27.2 STILL OPEN — confirmed dominant blocker.** RCA `unknown_kind`
  rate = 66.7% (airline) / 71.4% (7now), both > 30% anti-marker →
  `GSO_TRIAL24_KIT_FORCED_V1` count=0 on BOTH → structural lever never
  selected → mechanism-binding gate rejected re-proposals as
  `mechanism_does_not_cover_behavior_delta` → `behavioral_diff=unchanged`
  every iteration → 0 accuracy gain (airline stayed 95.65%, 7now 91.3%).
- **W27.4 UNMET** — `GSO_TRIAL24_KIT_FORCED_V1`=0 and
  behavioral_diff-changed patches=0 on both anchors. Leave unchecked.

Both postmortems report `architecture_invariants_held = false` (driven by
the open W27.2 unknown_kind rate + bundle-completeness/persistence gaps,
NOT a regression introduced by W27.1/W27.3). The gap → Trial 28.

## Trial 28 — Wire the owed RCA-canonicaliser LLM tier (the confirmed kit-gate blocker) + iter-0 Stage-3 de-starvation

**Master flag:** `GSO_TRIAL28_KIT_REACHABILITY` (default ON). `=0` restores
pre-Trial-28 canonicaliser tiers + seed-pass synthesis assembly.

### Hypothesis

Trial 27's live replay proved W27.3 (force_lever_loop) and W27.1 (in-loop
Stage-3 de-starvation) work, and isolated the dominant remaining blocker
with live marker payloads: the RCA canonicaliser leaves the majority of
live Stage-2 routing narratives at `unknown_kind` (66.7% airline / 71.4%
7now, both > the 30% anti-marker), so `_kit_for_rca_companions` returns
`None`, `GSO_TRIAL24_KIT_FORCED_V1` never fires (count=0 on both), the
structural lever is never selected, and the mechanism-binding gate
rejects every re-proposal as `mechanism_does_not_cover_behavior_delta` →
`behavioral_diff=unchanged` → 0 gain. The 5 live `unknown_kind` narratives
(mined from `docs/runid_analysis/unresolved_517826776610889/evidence/lever_loop_clean.txt`)
are free-text routing PROSE (e.g. "Cluster H001 root cause is 'SQL shape:
example SQL needed for ranking/comparison patterns' — the routing table
maps this structural pattern gap to lever-5b-example-sql…"), which a
keyword regex cannot robustly categorise without overfitting. The owed
tier-4 LLM call (`rca_kind_canonical._invoke_llm_tier`, currently
`raise NotImplementedError`) is the principled fix: an LLM categorises the
narrative against the closed `RCA_CANONICAL_KEY_SET` enum; deterministic
code clamps the output to the canonical set. Secondary: the iteration-0
seed Stage-3 pass still starves (`prompt_too_large`), which W27.1 left
untouched.

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W28.1 | Wire the owed RCA-canonicaliser tier-4 LLM call so live Stage-2 routing narratives reduce to canonical keys. Reuse the existing `LlmReasoningCall` + `LlmReasoningRequest` + `build_response_format` typed-LLM infra with a closed-enum output schema over `RCA_CANONICAL_KEY_SET ∪ {unknown_kind}`; deterministic code clamps the LLM output to the canonical set (LLM reasons, code validates). New sub-flag `GSO_TRIAL28_RCA_LLM_TIER` (default ON under master). Lazily acquire the workspace client via `make_workspace_client()` inside `_invoke_llm_tier` when `w` is None AND the flag is ON, so the kit-gate call site (`_normalize_rca_kind`) does NOT need `w` threaded through its ~15 call sites. No per-QID / per-anchor / per-space_id literal in `src/` — the LLM generalises over ANY narrative. | `optimization/rca_kind_canonical.py`, `optimization/trial28_flags.py`, new skill `skills/plan11_rca_canonicalise/` (SKILL.md system prompt + closed-enum `output_schema.py`). Tests: monkeypatched LLM tier resolves the 5 mined live `unknown_kind` narratives to canonical keys on a NON-anchor fixture; alignment ≥95% unchanged; flag-OFF byte-stable; off-canonical LLM output clamps to `unknown_kind` (`via=llm_invalid`). | plan |
| W28.2 | Extend the W27.1 Stage-3 partitioned re-dispatch to the iteration-0 SEED synthesis pass so the pre-loop pass also fits the 40000 cap (currently 65k–74k tokens → 100% `prompt_too_large` at iter 0). Marker `GSO_TRIAL28_SEED_REDISPATCH_V1`. Sub-flag `GSO_TRIAL28_SEED_DESTARVE` (default ON under master). | `optimization/stages/synthesize.py` (seed path), `optimization/trial28_flags.py`. Tests: forward-pipeline seed-pass row with `sub_cluster_split_needed=true` fits the cap; bright-line single-call preserved when partition len==1. | plan |
| W28.3 | airline `gs_024` `POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS` — the post-apply eval slice has `benchmarks_count=0`, so applied patches can never be scored and always `reject_loss`. Diagnose the per-QID benchmark-slice resolution; if `gs_024`/`gs_009` are in the `pending_gt_review` / `hard_qids_already_correct` subset they are not genuine failures — document, do not overfit. | `optimization/` eval-slice resolution (TBD by RCA); live verification. | plan |
| W28.4 | Live re-verification on the same parents after W28.1-2 land. Acceptance: RCA `unknown_kind` rate < 30% AND `GSO_TRIAL24_KIT_FORCED_V1` count ≥ 1 AND ≥1 accepted patch `behavioral_diff != "unchanged"` on at least one anchor whose lever loop runs. | live verification (no code) | plan |

### Watch Markers (positive)

| Marker | Workstream | Meaning |
|---|---|---|
| `GSO_TRIAL26_RCA_CANONICAL_V1{via:"llm", canonical_key != "unknown_kind"}` count ≥ 1 on a live anchor | W28.1 | the LLM tier resolved a live routing narrative the deterministic tiers could not |
| `GSO_TRIAL26_RCA_CANONICAL_V1{canonical_key != "unknown_kind"}` rate ≥ 70% on a live anchor | W28.1 | `unknown_kind` rate now under the 30% anti-marker |
| `GSO_TRIAL24_KIT_FORCED_V1` count > 0 on a live anchor | W28.1 | kit gate finally fires — the long-owed production proof |
| `GSO_TRIAL28_SEED_REDISPATCH_V1{over_cap:false}` at iteration 0 | W28.2 | the seed Stage-3 pass now fits the cap |

### Anti-Success Markers (negative)

| Anti-marker | Meaning |
|---|---|
| `GSO_TRIAL26_RCA_CANONICAL_V1.canonical_key="unknown_kind"` rate > 30% on a live anchor | W28.1 incomplete |
| `prompt_too_large` at iteration 0 on a live anchor | W28.2 incomplete |
| any `if rca_kind == …` / keyword pinned to a specific 7now string in `src/` | Architectural Principle #1 violation — the LLM tier must generalise |

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| RCA canonicaliser LLM tier (mocked) resolves mined narratives | `pytest tests/unit/optimization/test_trial28_rca_llm_tier.py -q` |
| RCA canonicaliser alignment (≥95%, deterministic-tier unchanged) | `pytest tests/eval/test_rca_kind_canonical_normaliser_alignment.py -q` |
| Stage 3 seed-pass de-starvation | `pytest tests/unit/stages/ tests/integration/test_sm_forward_pipeline_to_proposed.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL28_KIT_REACHABILITY=0` then redeploy.

### Status

- [x] W28.1 — **offline-complete.** RCA-canonicaliser tier-4 LLM wire-up landed: `_invoke_llm_tier` now uses `LlmReasoningCall` + `LlmReasoningRequest` with a typed `LLMOutputContract` `result_cls` (`canonical_key`+`confidence`) over the closed `RCA_CANONICAL_KEY_SET`; deterministic clamp keeps off-canonical answers → `unknown_kind`/`via=llm_invalid`. Lazy `make_workspace_client()` acquisition under sub-flag `GSO_TRIAL28_RCA_LLM_TIER` (default ON, master `GSO_TRIAL28_KIT_REACHABILITY`), pytest-guarded so the 9885-test suite stays byte-stable (no kit-gate network calls offline). 9 new tests in `tests/unit/optimization/test_trial28_rca_llm_tier.py` (generic non-anchor narrative); alignment ≥95% unchanged; full suite 9885 green; pretrial gate exit 0. No new `check_invariants` violations. **Live `unknown_kind` < 30% still to be confirmed by W28.4 replay.**
- [ ] W28.2 — iteration-0 seed Stage-3 de-starvation (forward-pipeline fits the cap)
- [ ] W28.3 — diagnose airline `gs_024` zero-benchmark eval slice
- [ ] W28.4 — live re-verification: **PARTIAL (2 of 3 criteria met).** `unknown_kind` < 30% ✅ (0.0% on BOTH anchors, down from 66.7%/71.4%) + `GSO_TRIAL24_KIT_FORCED_V1` ≥ 1 ✅ (count=2 on 7now — kit gate fired, deepest-ever `accepted`) — but ≥1 behavior-changing patch ❌ (`behavioral_diff_changed_count=0` on both). Leave unchecked: the behaviour criterion is unmet.

### Live Verification Results (Trial 28 W28.1 replay — 2026-06-07)

Replayed both anchors after deploying W28.1 (2nd repairs: airline REPAIR
`838289694458133` `force_lever_loop=true`, 7now REPAIR `1067408292431508`;
both `TERMINATED/SUCCESS`; latest lever_loop tasks `992800754335144` /
`45506293787891`). Postmortems overwrote `docs/runid_analysis/e94376a3-…/`
and `docs/runid_analysis/d13938e7-…/postmortem.md`.

- **W28.1 CONFIRMED live** — the owed tier-4 LLM canonicaliser fired
  (`via="llm"`, 1 narrative each) and **`unknown_kind` dropped 66.7%→0.0%
  (airline) and 71.4%→0.0% (7now)**, both far under the 30% anti-marker.
- **Kit gate REACHED on 7now** — `GSO_TRIAL24_KIT_FORCED_V1` count=2
  (`kit_satisfied=true`), patches applied end-to-end, deepest_stage=`accepted`
  (deepest-ever for 7now). The long-owed kit-gate production proof is met.
- **NEW DOMINANT BLOCKER — behavioural inertness.** Every kit-forced /
  accepted patch recorded `behavioral_diff="unchanged"` →
  `OPTIMIZER_TRIED_INSUFFICIENT_GAIN` (7now stayed 91.3%). The
  `add_example_sql` / `add_column_description` levers the kit selects for
  `wrong_column` / `top_n_cardinality_collapse` RCAs do not shift Genie's
  NL→SQL. → Trial 29 W29.1.
- **airline kit gate STILL shut — upstream, not canonicaliser.** Stage-1
  diagnose declined gs_009 8× (`context_token_budget_exceeded`) → no
  grounded RCA card; Plan-11 dispatch input-projection drift dropped
  gs_024 in all 4 iters. → Trial 29 W29.2.
- **W28.2 still owed** — iter-0 seed Stage-3 still over cap (59k–68k tok);
  in-loop synthesis healthy (0% over cap).
- Both postmortems `architecture_invariants_held = false` (bundle
  incomplete: 29 missing artifacts; medium-tier I7/I4) and note the hard
  QIDs are `pending_gt_review` / `hard_qids_already_correct` — the 100%
  ceiling is partly **GT-review-bound**, not purely optimizer-addressable.

## Trial 29 — Behaviour-changing structural lever for kit-forced RCAs + airline Stage-1/Plan-11 upstream de-starvation

**Master flag:** `GSO_TRIAL29_BEHAVIOR_DELTA` (default ON). `=0` restores
pre-Trial-29 lever selection + Stage-1 budget assembly.

### Hypothesis

Trial 28 W28.1 made the kit gate REACHABLE (unknown_kind 0%, kit fired on
7now) but exposed the next blocker with live payloads: the kit-forced
levers (`add_example_sql`, `add_column_description`) APPLY but leave
`behavioral_diff="unchanged"` (0 behaviour-changing patches on either
anchor), so accuracy never moves. For `wrong_column` /
`top_n_cardinality_collapse` RCAs the fix must be a STRUCTURAL lever
(lever-6 SQL-snippet / sql-shape) that demonstrably shifts Genie's
NL→SQL, gated by a post-apply behaviour check that rejects inert patches
BEFORE they consume the iteration budget. Separately, airline's kit gate
is starved upstream (Stage-1 diagnose `context_token_budget_exceeded` on
gs_009 ×8; Plan-11 dispatch input-projection drift drops gs_024) — the
canonicaliser never gets a chance because no grounded RCA card is
produced. NOTE: both anchors' residual gap to 100% is partly
GT-review-bound (`pending_gt_review`/`hard_qids_already_correct`); a
parallel ground-truth-review pass may be required for literal 100%.

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W29.1 | Post-apply behaviour gate + structural-lever routing for kit-forced RCAs: when a kit-forced patch yields `behavioral_diff="unchanged"`, the acceptance gate emits a new `kit_forced_inert_reroute` decision (sibling of `kept_insufficient`), records the rejected mechanism on `AcceptanceDecisionRecord.rejected_mechanism`, harvests it into `InertMechanismHistory` (typed accumulator threaded through `TransformerContext` + `Stage2BatchInput`), and Stage 3 synthesis renders a per-(qid, rca_kind) AVOID section into the prompt so the LLM picks from `_structural_fix_mechanisms(rca) - rejected_mechanism` next iteration. Forensic evidence persists as `Trial29InertPatchDiagnostic` JSONL. Kit-forced detection is dynamically derived via `_kit_for_rca_companions(rca_kind) is not None` (no schema change to `AppliedRecord`). Gated by `GSO_TRIAL29_BEHAVIOR_DELTA` (master) + `GSO_TRIAL29_INERT_REROUTE` (sub); both default ON, byte-stable rollback to `kept_insufficient` when OFF. | `optimization/trial29_flags.py`, `optimization/inert_mechanism_history.py`, `optimization/inert_patch_diagnostic.py`, `optimization/state_machine/records.py` (`AcceptanceDecisionRecord`), `optimization/state_machine/verdict.py` (`TransformerContext`), `optimization/state_machine/transformers/acceptance_gate.py` (new lane), `optimization/state_machine/transformers/cluster_batch.py` (`Stage2BatchInput`), `optimization/stages/synthesize.py` (`render_inert_mechanism_history_section`) | implemented (offline) |
| W29.2 | airline upstream de-starvation: Stage-1 diagnose `context_token_budget_exceeded` on gs_009 (compact the diagnose prompt / raise budget) + Plan-11 dispatch input-projection drift dropping gs_024 (fix the SM→dispatch projection so every clustered QID reaches dispatch). | `optimization/stages/diagnose.py`, Plan-11 dispatch projection | plan |
| W29.3 | W28.2 carry-over: iteration-0 seed Stage-3 de-starvation (extend W27.1 W6 re-dispatch to the seed pass). | `optimization/stages/synthesize.py` (seed path) | plan |
| W29.4 | Live re-verification. Acceptance: ≥1 accepted patch `behavioral_diff != "unchanged"` AND a measurable accuracy gain on at least one anchor whose lever loop runs. | live verification | plan |
| W29.5 | Decomposed architecture invariants: split the monolithic `architecture_invariants_held: bool` into three per-domain sub-invariants (`rca_invariants_held` / `lever_lattice_invariants_held` / `bundle_completeness_invariants_held`) so an orthogonal infra gap (e.g. evidence-bundle persistence) does not mask RCA / lever-lattice progress. `all_held` preserves the legacy single-bool contract for harness reads; `legacy_architecture_invariants_held` is a free-function alias. `render_postmortem_section` emits each sub-invariant + the aggregate so the /goal harness parser keeps working. | `optimization/architecture_invariants.py` (`ArchitectureInvariants` typed model + `render_postmortem_section`) | implemented (offline) |

### Watch Markers (positive)

| Marker | Workstream | Meaning |
|---|---|---|
| accepted patch with `behavioral_diff != "unchanged"` on a live anchor | W29.1 | a kit-forced lever finally shifted Genie's NL→SQL |
| `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` count > 0 on a live anchor | W29.1 | the new acceptance lane fired (kit-forced patch was inert → routed for re-synthesis with rejected mechanism recorded) |
| `GSO_TRIAL24_KIT_FORCED_V1` count > 0 on airline | W29.2 | airline's upstream starvation cleared; kit gate reachable |
| final accuracy strictly > post-enrichment baseline on a live anchor | W29.1+W29.2 | the loop produced a real gain |

### Anti-Success Markers (negative)

| Anti-marker | Meaning |
|---|---|
| `behavioral_diff="unchanged"` on 100% of accepted patches on a live anchor | W29.1 incomplete |
| Stage-1 `context_token_budget_exceeded` on any airline hard QID | W29.2 incomplete |
| any `if rca_kind == …` / per-QID lever hardcode in `src/` | Architectural Principle #1 violation |

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 29 W29.1 unit + W29.5 (`trial29_flags` + `inert_mechanism_history` + `inert_patch_reroute` + `inert_patch_diagnostic` + `architecture_invariants` + Stage 3 prompt render) | `pytest tests/unit/optimization/test_trial29_*.py tests/unit/stages/test_trial29_synthesis_inert_history_prompt.py -q` (42 tests) |
| Trial 29 W29.1 end-to-end replay (gate → harvest → persist → prompt → extend across two iterations) | `pytest tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py -q` |
| State-machine regression (gate / records / verdict / transformers) — must remain at 291 passing | `pytest tests/unit/state_machine/ tests/integration/postmortem_replay/ -q` |
| Post-apply behaviour gate + structural routing unit | `pytest tests/unit/optimization/ -q -k behavior_delta` |
| Stage-1 diagnose budget / Plan-11 projection | `pytest tests/unit/stages/ tests/integration/test_sm_forward_pipeline_to_proposed.py -q` |
| RCA canonicaliser alignment (≥95%, unchanged) | `pytest tests/eval/test_rca_kind_canonical_normaliser_alignment.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL29_BEHAVIOR_DELTA=0` then redeploy (master kill-switch; forces every Trial 29 sub-flag OFF). Fine-grained: `export GSO_TRIAL29_INERT_REROUTE=0` to disable just W29.1 (falls back to `kept_insufficient` for byte-stable behaviour). W29.5 is an offline schema decomposition with `all_held` preserving the legacy aggregate — no flag required.

### Status

- [x] W29.1 — post-apply behaviour gate + structural-lever routing for kit-forced RCAs (offline: inert patch → structural re-route; behaviour gate rejects unchanged via `kit_forced_inert_reroute` lane; rejected mechanism harvested into `InertMechanismHistory` and surfaced in Stage 3 prompt; `Trial29InertPatchDiagnostic` JSONL persistence) — implemented 2026-06-07, 42 unit + 1 integration replay + 291 SM regression GREEN, byte-stable when flag OFF
- [ ] W29.2 — airline Stage-1 diagnose budget + Plan-11 dispatch projection (gs_009 grounded RCA card; gs_024 reaches dispatch)
- [ ] W29.3 — iteration-0 seed Stage-3 de-starvation (carry-over from W28.2)
- [~] W29.4 — live re-verification **PARTIAL** (2026-06-07, both anchors). The W29.1 lane **fired live on both anchors** (airline: 1× `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` on `gs_009`/`top_n_cardinality_collapse`/rejected `lever-5`; 7now: 2× on `gs_013`/`wrong_column` + `gs_026`/`top_n_cardinality_collapse`, both rejected `lever-5`) and accuracy **held** at each post-enrichment baseline (airline 95.65%, 7now 91.3% — criterion 4 PASS, criterion 1 PASS). BUT **no patch ever produced `behavioral_diff != "unchanged"`** (criterion 3 FAIL on both) and the mechanism-switch (criterion 2) was never exercised: on 7now Stage 3 **re-emitted the same rejected `(lever-5, add_example_sql)` mechanism** in iters 2/3/4 (never selected the `lever-6` fallback); on airline `gs_009` was **dropped from the iter-3/4 Stage-3 `target_qids_union`** so the strategist never had to re-pick. Postmortems: `runid_analysis/d13938e7-…/postmortem.md` (7now), `runid_analysis/e94376a3-…/postmortem_653857084564329.md` (airline). **Root cause → W30.1** (below): the W29.1 feedback channel carries the rejected signature but does NOT *force* Stage 3 to choose a different structural mechanism — the LLM is free to re-emit it, and the kept-QID projection drops the rerouted QID before the next synthesis. Repairs: airline `1068216194302846` (task `653857084564329`), 7now `990840611944843` (task `1075987815374793`), both TERMINATED/SUCCESS.
- [x] W29.5 — decomposed `ArchitectureInvariants` typed model (rca / lever-lattice / bundle-completeness sub-invariants + `all_held` backwards-compat aggregate + postmortem render) — implemented 2026-06-07, 6 unit tests GREEN, byte-stable single-bool path preserved

### W29.1 Test Files & Modules (implementation evidence)

| Module | Test file | Tests | Purpose |
|---|---|---|---|
| `optimization/trial29_flags.py` | `tests/unit/optimization/test_trial29_flags.py` | 12 | Master + sub-flag with default-ON, opt-out, master kill-switch semantics |
| `optimization/inert_mechanism_history.py` (`InertMechanismHistory`, `harvest_sm_…`, `extend_sm_…`) | `tests/unit/optimization/test_trial29_inert_mechanism_history.py` | 8 | Typed accumulator round-trip, harvest from `AcceptanceDecisionRecord`, cumulative-extend dedup, `TransformerContext` + `Stage2BatchInput` plumbing |
| `optimization/inert_patch_diagnostic.py` (`Trial29InertPatchDiagnostic`, `persist_…`, `load_…`) | `tests/unit/optimization/test_trial29_inert_patch_diagnostic.py` | 4 | JSONL persistence round-trip, missing-dir/file handling |
| `optimization/state_machine/records.py` (`AcceptanceDecisionRecord.decision` literal + `rejected_mechanism` field) + `optimization/state_machine/transformers/acceptance_gate.py` (new `KIT_FORCED_INERT_REROUTE` lane) | `tests/unit/optimization/test_trial29_inert_patch_reroute.py` | 8 | Lane fires positive + 4 negative-rollback paths (non-kit-forced RCA, target_fixed, score-delta non-zero, behavior != "unchanged") + 3 typed-record extension tests |
| `optimization/stages/synthesize.py` (`render_inert_mechanism_history_section`) | `tests/unit/stages/test_trial29_synthesis_inert_history_prompt.py` | 3 | Per-QID AVOID section rendering, empty-history byte-stability, multi-QID accumulation |
| **E2E across W29.1 surface** | `tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py` | 1 | Two-iteration 7now-shaped replay: gate → harvest → persist → prompt → extend → reload, asserts cumulative rejected-mechanism dedup |

### W29.5 Test Files & Modules

| Module | Test file | Tests | Purpose |
|---|---|---|---|
| `optimization/architecture_invariants.py` (`ArchitectureInvariants`, `all_held`, `legacy_architecture_invariants_held`, `render_postmortem_section`) | `tests/unit/optimization/test_trial29_architecture_invariants.py` | 6 | Per-domain decomposition, `all_held` conjunction, postmortem section format compatibility with /goal harness parser |

## Trial 30 — Force the structural-mechanism switch (close the W29.4 gap) + W29.1 evidence-bundle completeness

> **Why this trial exists.** Trial 29 W29.4 proved the W29.1 detection
> half works live on both anchors — the `kit_forced_inert_reroute`
> lane fired (airline 1×, 7now 2×) and rejected the inert mechanism —
> but the *correction* half did not change behaviour: Stage 3 either
> re-emitted the same rejected mechanism (7now, all of iters 2-4 stayed
> on `lever-5`/`add_example_sql`, never selected the `lever-6`
> fallback) or never had to re-pick because the rerouted QID was
> dropped from the next iteration's synthesis target set (airline
> `gs_009` fell out of `target_qids_union` in iters 3-4). The feedback
> channel **informs** the LLM of the rejected mechanism but does not
> **constrain** it, and the kept-QID projection can drop the very QID
> the reroute was about. The result on both anchors:
> `behavioral_diff="unchanged"` on 100 % of accepted patches —
> the Trial 29 anti-success marker.

### Cross-anchor root cause (from the two W29.4 postmortems)

| Symptom | airline (`653857084564329`) | 7now (`1075987815374793`) | Shared mechanism |
|---|---|---|---|
| reroute fired | 1× (`gs_009`, `top_n_cardinality_collapse`, rejected `lever-5`) | 2× (`gs_013`/`wrong_column`, `gs_026`/`top_n_cardinality_collapse`, both rejected `lever-5`) | W29.1 detection works |
| next-iter mechanism | n/a — `gs_009` dropped from Stage-3 `target_qids_union` | re-emitted `lever-5`/`add_example_sql` (never picked `lever-6` fallback) | **W29.1 feedback is advisory, not enforced** |
| behavioural delta | none (`unchanged`×1) | none (`unchanged`×2) | criterion 3 FAIL both |
| accuracy | held 95.65 % | held 91.3 % | criterion 4 PASS both (no regression — the W29.2 `mechanism_does_not_cover_behavior_delta` re-apply gate blocked the inert re-apply safely) |

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W30.1 | **Force the structural switch.** Convert the W29.1 feedback channel from advisory to enforced: when Stage 3 synthesises for a `(qid, rca_kind)` that has a non-empty `InertMechanismHistory`, the lever-selection MUST pick from `_structural_fix_mechanisms(rca_kind) - rejected_mechanisms` and the synthesised proposal MUST be **rejected at validation** if it re-emits a rejected mechanism (deterministic post-LLM guard, not just prompt text). The lever lattice must expose a non-`add_example_sql` structural mechanism for `top_n_cardinality_collapse` and `wrong_column` (today the only fallback for those kits appears to be `lever-6`, which the strategist never selected — verify the fallback is actually reachable and structurally distinct). | `optimization/stages/synthesize.py` (post-LLM mechanism guard), `optimization/stages/action_groups.py` (`_structural_fix_mechanisms` coverage for top_n / wrong_column), `optimization/state_machine/transformers/cluster_batch.py` (ensure rerouted QID stays in the next `target_qids_union`) | **done** |
| W30.2 | **Keep the rerouted QID in the next synthesis set.** Airline `gs_009` was dropped from iters 3-4 `target_qids_union` after the reroute, so the strategist never re-picked. The kept-QID projection must treat a `kit_forced_inert_reroute` QID as "still open" (same class as `kept_insufficient`) so it is carried into the next iteration's Stage-3 target set until a behaviour delta is observed or the lattice is exhausted. | Stage-3 target-set projection (kept-QID carry-forward) | **done** |
| W30.3 | **W29.1 evidence-bundle completeness (closes `bundle_completeness_invariants_held`).** Two persistence gaps surfaced live: (a) **no `Trial29InertPatchDiagnostic` JSONL was persisted** on either anchor despite 3 total reroutes — the persistence call is implemented + unit-tested but is not wired into the live acceptance path; (b) **`kit_forced_inert_reroute` decisions did not project to `genie_eval_lever_loop_decisions`** (both postmortems had to fall back to log-grep for the count because the decisions table returned 0 rows). Wire both so the W29.5 `bundle_completeness_invariants_held` sub-invariant can actually go green. | acceptance→persistence wiring for `Trial29InertPatchDiagnostic`; decisions-table projection for the new decision literal | **done** |
| W30.4 | **Airline observability gaps (non-blocking but they force `architecture_invariants_held=false`).** (a) `GSO_TRIAL24_KIT_FORCED_V1` was suppressed because the *cluster-level* RCA was `extra_defensive_filter` even though the per-QID `gs_009` RCA was `top_n_cardinality_collapse` — the kit marker should reflect the per-QID kit decision, not just the cluster headline; (b) `terminal_reason="unknown"` appeared 4× — every terminal path should carry a typed reason; (c) the Trial-16 `POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS` regression returned on `gs_024` (the W29.2 Plan-11 dispatch projection work is the likely fix). | kit-marker per-QID source; typed `terminal_reason`; `gs_024` zero-benchmark slice (overlaps W29.2) | **done** (b+c; a deferred to live-verify) |
| W30.5 | **Live re-verification.** Same acceptance bar as W29.4 but now expecting it to PASS: ≥1 accepted patch `behavioral_diff != "unchanged"` AND a measurable accuracy gain on at least one anchor whose lever loop runs, AND every reroute persists a `Trial29InertPatchDiagnostic` + projects to the decisions table. | live verification | plan |

### Watch Markers (positive)

| Marker | Workstream | Meaning |
|---|---|---|
| Stage-3 proposal rejected at validation for re-emitting a rejected mechanism | W30.1 | the enforced guard fired (advisory → enforced) |
| a rerouted `(qid, rca_kind)` appears in the NEXT iteration's Stage-3 `target_qids_union` | W30.2 | rerouted QID carried forward, not dropped |
| `genie_eval_lever_loop_decisions` has ≥1 row with `decision='kit_forced_inert_reroute'` on a live anchor | W30.3 | decisions-table projection wired |
| ≥1 `Trial29InertPatchDiagnostic` JSONL persisted per reroute on a live anchor | W30.3 | evidence-bundle completeness closed |
| accepted patch with `behavioral_diff != "unchanged"` on a live anchor | W30.1+W30.2 | the structural switch finally shifted Genie's NL→SQL (the W29.4 anti-success marker cleared) |

### Anti-Success Markers (negative)

| Anti-marker | Meaning |
|---|---|
| Stage 3 re-emits a rejected mechanism for a `(qid, rca_kind)` with non-empty inert history | W30.1 incomplete (still advisory) |
| a rerouted QID is absent from the next iteration's Stage-3 target set | W30.2 incomplete |
| `behavioral_diff="unchanged"` on 100 % of accepted patches on a live anchor (carried from W29.4) | W30.1+W30.2 incomplete |
| `genie_eval_lever_loop_decisions` returns 0 `kit_forced_inert_reroute` rows while the log shows ≥1 marker | W30.3 incomplete (projection gap) |
| any `if rca_kind == …` / per-QID lever hardcode in `src/` | Architectural Principle #1 violation |

### Status

- [x] W30.1 — force the structural-mechanism switch (advisory → enforced post-LLM guard + structurally-distinct fallback coverage) — implemented 2026-06-07. W30.1a wires the W29.1 `InertMechanismHistory` from the harness harvest → `TransformerContext` → Stage 3 prompt (it was unwired in production, the root of the W29.4 PARTIAL). W30.1b adds the deterministic post-LLM guard `enforced_switch_survivors` (compares on `PatchMechanism`, not lever-id strings, so lever-5/5a/5b aliasing cannot let a re-emit slip through; drops a re-emit ONLY when a structural fallback survives in the same QID's slate, else keeps it + flags `no_fallback` — never zeroes a QID). Gated by `GSO_TRIAL30_ENFORCED_SWITCH` (master) + `GSO_TRIAL30_INERT_HARVEST_WIRE` / `GSO_TRIAL30_ENFORCE_GUARD` (sub-flags); byte-stable when OFF. 33 Trial-30 unit tests + 3 end-to-end replay + 931 stages/optimization regression GREEN.
- [x] W30.2 — keep the rerouted QID in the next Stage-3 target set (kept-QID carry-forward parity with `kept_insufficient`) — implemented 2026-06-07. (a) member_qids unioned into the synthesized `target_qids_union`; (b) `kit_forced_inert_reroute` added to `_TERMINATIONS_REQUIRING_PIVOT`; (c) same-iteration live bucket written so the rerouted QID stays visible to the next synthesis without a persistence round-trip.
- [x] W30.3 — W29.1 evidence-bundle completeness (persist `Trial29InertPatchDiagnostic` + project `kit_forced_inert_reroute` to decisions table) — implemented 2026-06-08. New pure typed projection module `optimization/trial30_inert_projection.py` (`build_inert_patch_diagnostics` → typed `Trial29InertPatchDiagnostic` tuple; `build_inert_decision_rows` → typed `InertDecisionRow` with `.to_decision_row()` adapter to the existing `write_lever_loop_decisions` dict sink). Wired into `_run_lever_loop_sm_first` as a sibling of the W30.1a harvest block (harness.py:~20819): reads the same `_sm_final_states`, persists each diagnostic JSONL to `GSO_RUN_ARTIFACT_ROOT` and projects each reroute row to `genie_eval_lever_loop_decisions`. Gated by new sub-flag `GSO_TRIAL30_BUNDLE_COMPLETENESS` (master-killable; byte-stable OFF). Tests: `tests/unit/optimization/test_trial30_inert_projection.py` (4, incl. cross-(qid,rca) generality), `tests/unit/optimization/test_trial30_bundle_completeness_flag.py` (3). trial30 suite 44 GREEN, stages+optimization regression 938 GREEN, pretrial_gate 66/7-of-7 GREEN.
- [x] W30.4 — airline observability gaps — invariant-gating sub-items DONE 2026-06-08: (b) typed `terminal_reason` + (c) `gs_024` zero-benchmark eval-slice. (a) per-QID kit marker DEFERRED to live-verify (rca-domain observability; `rca_invariants_held` already true after W28.1 — re-assess against the fresh W30.5 postmortem invariants table).
  - [x] (b) typed `terminal_reason` — implemented 2026-06-08. The three harness no-candidate paths that emitted the raw `"unknown"` string (postmortem terminal-reason taxonomy gap, SKILL.md L2084) now emit typed `TerminalReason` members: `INFRASTRUCTURE_PRE_AG_SNAPSHOT_FAILED` (pre-AG snapshot capture failed), `INFRASTRUCTURE_APPLIER_FAILED` (Genie API rejected the PATCH payload), `SLICE_OR_P0_GATE_REGRESSION_ROLLBACK` (slice/p0 gate post-apply rollback). New members added to `optimization/terminal_reason.py` + routed in `iteration_terminal_policy._ROUTING_TABLE` to the SAME `("skip_productive", True)` the raw `"unknown"` got via default (observability-only; zero retry/forbid behaviour drift). Harness emission sites (marker + `_iter_terminal_reason` mirror + reflection-signature) updated; reflection-write whitelist refreshed for the W30.3+W30.4b line drift (1:1 mapping verified by terminal reason). Tests: `tests/unit/test_trial30_w30_4_typed_terminal_reasons.py` (3). trial30 suite 47 GREEN, regression 938 GREEN, terminal family 196 GREEN, pretrial_gate PASS.
  - [~] (a) per-QID kit marker — **DEFERRED to live-verify** 2026-06-08. The `GSO_TRIAL24_KIT_FORCED_V1` marker keys on the cluster headline RCA; reflecting a per-QID kit decision requires threading per-QID `DiagnosisRecord`s into the synthesis stage (the cluster IS the RCA grouping unit, so `RepairProposal` carries no distinct per-proposal source RCA). Payoff is low: `rca_invariants_held` is **already true after W28.1**, so this is rca-domain observability, not an invariant-gating gap. Re-assess against the fresh postmortem's invariants table after W30.5; only thread per-QID diagnoses if it shows a concrete ❌ row.
  - [x] (c) `gs_024` `POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS` slice — implemented 2026-06-08. Root cause was a namespace-vs-canonical qid mismatch in the post-apply eval slice: `extract_question_id(b)[0]` returns the NAMESPACED qid (`airline_..._gs_024`) but `requested` (from `inp.eval_qids`) carried canonical `gs_024` (or vice versa) → empty slice even with the benchmark present. Fix: new pure helper `stages/evaluation.slice_benchmarks_to_eval_qids` matches namespace-insensitively via the shared `canonical_eval_row._split_namespaced_qid` canonicaliser (NOT hand-rolled) + exact-match short-circuit for byte stability; the `PostApplyEvalEmptySliceError` fail-fast is preserved for a genuine no-match. This is the postmortem SKILL.md L158 recommendation (canonical-extractor join on both sides). Tests: `tests/unit/stages/test_trial30_w30_4c_eval_slice_namespace.py` (6, incl. cross-namespace/non-anchor generality). regression 944 GREEN, trial30 53 GREEN, pretrial_gate PASS.
- [ ] W30.5 — live re-verification: ≥1 `behavioral_diff != "unchanged"` patch + accuracy gain + diagnostic/decision persistence per reroute

### Local Verification (mandatory before deploy)

| Scope | Command |
|---|---|
| Trial 30 W30.1 + W30.2 full suite (flags + mechanism normalization + harvest wire + ctx→prompt thread + guard helper + guard wiring + union member_qids + pivot membership + live bucket) | `pytest tests/ -k trial30 -q --ignore=tests/replay/test_ccf1d60d_safe_subset_isolation.py` (33 tests) |
| Trial 30 W30.1 end-to-end replay (gate → harvest → **enforce**: re-emit dropped w/ fallback, kept w/o fallback, on canonical `gs_026`) | `pytest tests/integration/postmortem_replay/test_trial30_w30_1_enforced_switch_replay.py -q` (3 tests) |
| Stages + optimization regression (no behavioural drift from the guard/threading) | `pytest tests/unit/stages/ tests/unit/optimization/ -q --ignore=tests/replay/test_ccf1d60d_safe_subset_isolation.py` (931 tests) |

> Pre-existing unrelated collection error: `tests/replay/test_ccf1d60d_safe_subset_isolation.py` imports a removed `acceptance_tier` module (predates Trial 30; ignore until that stale test is repaired).

### Rollback

`export GSO_TRIAL30_ENFORCED_SWITCH=0` then redeploy (master kill-switch; forces every Trial 30 sub-flag OFF → byte-stable W29 behaviour). Fine-grained: `GSO_TRIAL30_ENFORCE_GUARD=0` disables only the W30.1b drop (history still threaded to the prompt as advisory, i.e. W29 behaviour); `GSO_TRIAL30_INERT_HARVEST_WIRE=0` stops harvesting the history entirely.

### W30.1 + W30.2 Test Files & Modules (implementation evidence)

| Layer | Test file | Module under test |
|---|---|---|
| Flags | `tests/unit/optimization/test_trial30_flags.py` | `optimization/trial30_flags.py` |
| Mechanism normalization | `tests/unit/optimization/test_trial30_mechanism_normalization.py` | `optimization/rca_mechanism_routing.py` (`mechanisms_for_rejected_levers`) |
| W30.1a harvest wire | `tests/unit/optimization/test_trial30_inert_harvest_wire.py` | `optimization/harness.py` (extractors + harvest), `optimization/inert_mechanism_history.py` |
| W30.1a ctx → Stage 3 prompt thread | `tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py` | `optimization/optimizer.py`, `optimization/stages/synthesize.py`, `state_machine/transformers/synthesize_llm.py` |
| W30.1b guard helper (pure) | `tests/unit/optimization/test_trial30_enforced_switch_guard.py` | `optimization/enforced_mechanism_switch.py` (`enforced_switch_survivors`) |
| W30.1b guard wiring | `tests/unit/stages/test_trial30_synthesize_guard_integration.py` | `optimization/stages/synthesize.py` (post-loop guard block) |
| W30.2(b) pivot membership | `tests/unit/test_trial30_pivot_membership.py` | `optimization/stages/action_groups.py` (`_TERMINATIONS_REQUIRING_PIVOT`) |
| W30.2(c) live bucket | `tests/unit/optimization/test_trial30_kit_forced_live_bucket.py` | reroute → same-iteration live bucket |
| **E2E (W29 → W30 closure)** | `tests/integration/postmortem_replay/test_trial30_w30_1_enforced_switch_replay.py` | gate → harvest → `enforced_switch_survivors` on `gs_026` |
| W30.3 projection (pure) | `tests/unit/optimization/test_trial30_inert_projection.py` | `optimization/trial30_inert_projection.py` (`build_inert_patch_diagnostics`, `build_inert_decision_rows`, `InertDecisionRow`) |
| W30.3 bundle-completeness flag | `tests/unit/optimization/test_trial30_bundle_completeness_flag.py` | `optimization/trial30_flags.py` (`trial30_bundle_completeness_enabled`) |
| W30.4(b) typed terminal reasons | `tests/unit/test_trial30_w30_4_typed_terminal_reasons.py` | `optimization/terminal_reason.py` (3 new members), `optimization/iteration_terminal_policy.py` (`_ROUTING_TABLE`), `optimization/harness.py` (3 emission sites) |
| W30.4(c) namespace-insensitive eval slice | `tests/unit/stages/test_trial30_w30_4c_eval_slice_namespace.py` | `optimization/stages/evaluation.py` (`slice_benchmarks_to_eval_qids`, `_canonical_qid_for_match`) |

## Trial 31 — Land the structural fallback the enforced switch demands (airline L6-decline) + RCA-groundedness funnel de-death (7now) + invariant/slice consistency

> **Why this trial exists.** Trial 30 W30.5 deployed W30.1–W30.4 and replayed both anchors live (airline parent `450001766723999` repair `805804379736305` task `810030146155257`; 7now parent `517826776610889` repair `507988473098231` task `64875616766479`, both TERMINATED/SUCCESS). The enforced-switch DETECTION half (W30.1a/W30.2/W30.3/W30.4b) is confirmed working live, but the goal was not met: **airline held 95.65 %, 7now held 91.3 %, 0 accepted patches on either, `architecture_invariants_held=false` on both.** Two NEW, distinct blockers — deeper than the W29.4 advisory-vs-enforced gap that W30.1 closed (so NOT whack-a-mole; the failure family shifted):
> - **airline:** the W30.1 guard correctly DROPS the re-emitted inert mechanism and the anti-success detector recommends `sql_snippet` (×2), but the **forced-L6 structural generator DECLINES** (`lever6_force_llm_declined`), so no structural patch is synthesised; the applied patches stay inert (`add_example_sql`/`add_instruction`) → terminal `contract_failed: rca_mechanism_defaulted_to_instruction_text` and `OPTIMIZER_INVARIANT_VIOLATION` emitted **while the task reports SUCCESS** (consistency bug).
> - **7now:** the funnel **died upstream, pre-apply** — both hard QIDs (`gs_013`, `gs_026`) stalled at `proposed` via `rca_ungrounded → no_causal_target → proposal_generation_empty → no_applied_patches` despite the RCA card naming concrete causal assets, so the inert-switch was never exercised. This regressed 7now's deepest stage (W29.4 reached `accepted`) and is the still-unimplemented W29.2 RCA-grounding/dispatch issue.

### Cross-anchor root cause (from the two W30.5 postmortems)

| Symptom | airline (`810030146155257`) | 7now (`64875616766479`) | Shared mechanism |
|---|---|---|---|
| W30.1 enforced switch | FIRED ×2, dropped inert re-emit (✅ detection) | not exercised (no prior applied inert patch) | detection deployed |
| structural fallback landed? | NO — `lever6_force_llm_declined` | NO — funnel died at `proposed` | **structural mechanism never reaches a live applied patch** |
| deepest stage | applied-but-inert (0 accepted) | `proposed` (0 applied) | apply→accept boundary never crossed productively |
| `architecture_invariants_held` | false | false (regressed from W29.4 true) | structural-actuation + RCA-grounding gaps |

### Workstreams

| Workstream | Description | Module owner | Status |
|---|---|---|---|
| W31.1 | **Make the mandated structural fallback land (airline `lever6_force_llm_declined`).** When the enforced switch mandates a structural mechanism and forced-L6 synthesis declines, the system MUST NOT silently default to inert instruction-text. Generalizable fix: strengthen the L6 forced-synthesis grounding/prompt so it produces a structural patch for `top_n_cardinality_collapse`/`wrong_column`, and when it still declines, terminate with a typed `no_structural_candidate` reason instead of applying an inert patch. LLM-reasoned (the structural synthesis is an LLM call), validated deterministically. | `optimization/stages/synthesize.py` (forced-L6 path), `optimization/rca_mechanism_routing.py` | plan |
| W31.2 | **7now RCA-groundedness funnel de-death (closes the deferred W29.2).** `gs_013`/`gs_026` die at `rca_ungrounded → no_causal_target` even though the RCA card names concrete causal assets — the SM→dispatch RCA-grounding projection drops them. Fix the grounding/dispatch handoff so a card that names causal assets reaches structural synthesis. | `optimization/stages/diagnose.py`, RCA-grounding → dispatch projection | plan |
| W31.3 | **Invariant↔task-result consistency.** `OPTIMIZER_INVARIANT_VIOLATION` was emitted while the parent task reported SUCCESS. A run that records an optimizer invariant violation must surface a non-SUCCESS optimizer outcome (typed), so the postmortem and the task agree. | `optimization/harness.py` (optimizer-outcome ↔ task-result), `optimization/state_machine/outcome.py` | plan |
| W31.4 | **W30.4(c) follow-up — already-correct QID empty-slice.** The namespace fix cleared the legacy mismatch, but a distinct `post_apply_eval_empty_slice_for_requested_qid` now fires for an already-correct QID (`gs_024`) that legitimately has no benchmark row. The slice/fail-fast must exclude already-correct / no-benchmark QIDs from the requested set before raising. | `optimization/stages/evaluation.py` (`slice_benchmarks_to_eval_qids` caller / eval_qids construction) | plan |
| W31.5 | **Live re-verification.** Acceptance: ≥1 accepted patch `behavioral_diff != "unchanged"` on at least one anchor AND `architecture_invariants_held=true` on both, AND no anchor regresses below its W30.5 baseline. | live verification | plan |

### Watch Markers (positive)

| Marker | Workstream | Meaning |
|---|---|---|
| a forced-L6 structural patch is synthesised + applied (no `lever6_force_llm_declined` terminal) on airline | W31.1 | the mandated structural fallback finally lands |
| `gs_013`/`gs_026` advance past `proposed` to `applyable`/`applied` on 7now | W31.2 | RCA-grounding funnel de-death |
| optimizer outcome is non-SUCCESS whenever `OPTIMIZER_INVARIANT_VIOLATION` is emitted | W31.3 | invariant↔task consistency |
| no `post_apply_eval_empty_slice_for_requested_qid` for an already-correct QID | W31.4 | already-correct QIDs excluded from the slice |
| accepted patch with `behavioral_diff != "unchanged"` on a live anchor | W31.1+W31.2 | the structural switch shifted Genie's NL→SQL |

### Anti-Success Markers (negative)

| Anti-marker | Meaning |
|---|---|
| `lever6_force_llm_declined` terminal on airline with an inert applied patch | W31.1 incomplete |
| `gs_013`/`gs_026` still die at `proposed`/`rca_ungrounded` on 7now | W31.2 incomplete |
| `OPTIMIZER_INVARIANT_VIOLATION` co-emitted with a SUCCESS optimizer outcome | W31.3 incomplete |
| any `if rca_kind == …` / per-QID lever hardcode in `src/` | Architectural Principle #1 violation |

### Local Verification (mandatory before deploy)

| Scope | Command |
|---|---|
| Trial 31 W31.1 forced-L6 structural synthesis + decline-handling | `pytest tests/ -k trial31 -q --ignore=tests/replay/test_ccf1d60d_safe_subset_isolation.py` |
| SM forward pipeline (RCA-grounding → proposed → applyable, W31.2) | `pytest tests/integration/test_sm_forward_pipeline_to_applyable.py tests/integration/test_sm_forward_pipeline_to_proposed.py -q` |
| Stages + optimization regression (no drift) | `pytest tests/unit/stages/ tests/unit/optimization/ -q --ignore=tests/replay/test_ccf1d60d_safe_subset_isolation.py` |

### Rollback

Per-workstream flag-gated (mirror the Trial 30 `GSO_TRIAL30_*` pattern); each W31 sub-flag default-ON under a master `GSO_TRIAL31_*`, byte-stable when OFF.

### Status

- [ ] W31.1 — land the mandated structural fallback (airline `lever6_force_llm_declined` → structural patch synthesised+applied, or typed `no_structural_candidate` terminal instead of inert default)
- [ ] W31.2 — 7now RCA-groundedness funnel de-death (`gs_013`/`gs_026` reach `applyable`; closes deferred W29.2)
- [ ] W31.3 — optimizer-outcome ↔ task-result consistency (no `OPTIMIZER_INVARIANT_VIOLATION` co-emitted with SUCCESS)
- [ ] W31.4 — W30.4(c) follow-up: exclude already-correct / no-benchmark QIDs from the post-apply eval slice before fail-fast
- [ ] W31.5 — live re-verification (≥1 `behavioral_diff != "unchanged"` + `architecture_invariants_held=true` on both, no regression)
