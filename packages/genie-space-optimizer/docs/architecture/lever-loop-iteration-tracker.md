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

- [ ] W25.1 — `LeverLoopOutputs` dataclass + single-blob publish + typed read
- [ ] W25.2 — `PreflightOutputs` compact publish + read
- [ ] W25.3 — `BaselineOutputs` compact publish + read
- [ ] W25.4 — `FinalizeOutputs` compact publish + read
- [ ] W25.5 — pre-trigger budget gate in `gso-lever-loop-replay`
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
| W26.1 | Typed RCA-kind canonical normaliser: LLM call (`rca_kind_canonicalise`) with typed input (free-form RCA kind string + cluster context) and typed enum output (canonical kit-map key or `unknown_kind`); deterministic post-validation that the output is in the canonical set; aligned offline evaluator (`tests/eval/test_rca_kind_canonical_normaliser_alignment.py`) over the airline + 7now RCA distributions; integration with `repair_diagnosis.py` and `stages/diagnose.py` so every RCA kind that reaches the kit map is canonical. | `stages/diagnose.py`, `repair_diagnosis.py`, new `rca_kind_canonical.py`, alignment test | plan |
| W26.2 | Extend `_TRIAL24_KIT_FOR_RCA` to include `wrong_aggregation`, `wrong_column`, `plural_top_n_collapse` (each with its corrective mechanism family). The kit composition follows the same shape as the existing Trial 24 entries (≥2-lever kit; matched companion families). Mechanism families derive from the existing `RCA_KIND_TO_FIXING_MECHANISMS` Trial 23 routing — Trial 26 wires those into Trial 24's kit-at-source synthesis path. | `stages/action_groups.py`, `proposal_slate_compiler.py` | plan |
| W26.3 | Fix `add_sql_snippet_filter` applier emitting `name` on serialized_space. Locate the applier dispatcher in `applier/` (or wherever `add_sql_snippet_filter` is translated to a serialized_space mutation), remove or rename the offending `name` field per the canonical `serialized_space` schema (`backend/references/schema.md`), add a deterministic typed builder + unit test covering both the happy path and the regression on the airline iter-4 / 7now iter-2 patch payloads. | `applier/`, `tests/unit/test_applier_add_sql_snippet_filter.py` | plan |
| W26.4 | Local verification: extend `tests/integration/postmortem_replay/test_trial24_postmortem_replay.py` with a non-anchor fixture proving the new kit map + normaliser cover at least one English-label cluster (Leg 3); add `test_trial26_rca_kind_canonical_normaliser_alignment.py` (offline LLM alignment) using a tracker-curated dataset of (English label → canonical key) pairs from past runid_analysis bundles. | tests | plan |
| W26.5 | Re-run Trial 24 live verification on fresh parent runs (Trial 25 W25.8) after Trial 26 lands. Acceptance criterion: at least one `GSO_TRIAL24_KIT_FORCED_V1` marker on each anchor AND at least one accepted patch with `behavioral_diff != "unchanged"` AND mechanism not in `{add_example_sql}`. | live verification (no code) | plan |

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
- `test_applier_add_sql_snippet_filter.py` proves the canonical builder produces a serialized_space mutation the Genie API accepts (mocked) and the regression payload from airline iter 4 / 7now iter 2 no longer hits `Unknown field 'name'`

### Local Verification (mandatory before deploy)

| Check | Command |
|---|---|
| Trial 26 RCA normaliser alignment | `pytest tests/eval/test_rca_kind_canonical_normaliser_alignment.py -q` |
| Trial 26 kit-map coverage replay | `pytest tests/integration/postmortem_replay/test_trial26_kit_map_coverage_replay.py -q` |
| Trial 26 applier fix unit | `pytest tests/unit/test_applier_add_sql_snippet_filter.py -q` |
| Trial 24 replay (no regression) | `pytest tests/integration/postmortem_replay/test_trial24_postmortem_replay.py tests/integration/postmortem_replay/test_trial24_general_grounding_replay.py -q` |
| Full authoritative suite | `pytest tests/unit/ tests/integration/postmortem_replay/ --ignore=tests/unit/_legacy -q` |

### Rollback

`export GSO_TRIAL26_KIT_GATE_REACHABLE=0` then redeploy for emergency
rollback. Each sub-flag also rolls back surgically (e.g.
`GSO_TRIAL26_KIT_MAP_EXPANDED=0` shrinks the kit map back to Trial-24
coverage but keeps the normaliser; `GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX=0`
restores the broken `name` field if the applier fix regresses).

### Status

- [ ] W26.1 — RCA-kind canonical normaliser + offline alignment test
- [ ] W26.2 — kit-map expansion to cover live airline RCA distribution
- [ ] W26.3 — `add_sql_snippet_filter` applier `name`-field fix
- [ ] W26.4 — bright-line replay suite + offline alignment test
- [ ] W26.5 — live re-verification on Trial-25-rotated parent runs: ≥1 `GSO_TRIAL24_KIT_FORCED_V1` per anchor AND ≥1 accepted patch with `behavioral_diff != "unchanged"` AND mechanism != `add_example_sql` per anchor
