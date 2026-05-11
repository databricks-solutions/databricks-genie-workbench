# GSO Run Output Contract — Letter-and-Spirit Compliance Roadmap

> **Purpose.** Achieve full compliance with the GSO Run Output Contract defined in [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) — both **letter** (every stage emits the contract-required typed records / markers) and **spirit** (the four inter-stage arrows that make the loop a closed scientific process actually fire).
>
> **Status.** Roadmap (multi-cycle). Each cycle is one or more small implementation plans, drafted and shipped independently. Slots into the [Optimizer Iteration Ledger](./2026-05-05-optimizer-iteration-ledger.md) as Cycles 12 → 17 plus sibling Cycle 14B (parallel ship to Cycle 14). Cycle 11 (Honest Loop Pilot) is shipped; this roadmap picks up where it ended.
>
> **North-star claim.** Genie space accuracy is non-deterministic. The loop's **process** is what we control. After this roadmap ships, every failed run produces a non-repeated next experiment, and every successful run is reproducible from one record. Accuracy will follow.

> **Last revision (2026-05-08).** Cycle 12 expanded from T1-T4 to **T1-T5** after drafting the T1, T2, T3 plans surfaced two structural discoveries: (a) C12-T2's root cause is not "validator unwired" — it is *wired but silent* (broad `try/except` swallow + listing-before-upload false-positives), so the fix is typed observability + `self_write_paths` exclusion, not fresh wiring; (b) C12-T3 has two gap layers — 5 parent-level paths with no producer at all (Layer A, fixed by T3) and ~7-per-iteration paths uploaded under legacy `phase_a/`/`phase_b/` prefixes (Layer B, scoped as the new **C12-T5**). C12 sizing revised from ~5 to ~12 working days; sequencing and self-check tables updated. Plan refs: [`2026-05-08-cycle-12-t1-run-manifest-v2-plan.md`](./2026-05-08-cycle-12-t1-run-manifest-v2-plan.md), [`2026-05-08-cycle-12-t2-phase-h-validator-wiring-plan.md`](./2026-05-08-cycle-12-t2-phase-h-validator-wiring-plan.md), [`2026-05-08-cycle-12-t3-bundle-assembler-fix-plan.md`](./2026-05-08-cycle-12-t3-bundle-assembler-fix-plan.md).

> **Revision (2026-05-09).** New anchor evidence — postmortem refreshed to lever-loop attempt 7, task `76457773587391` — shifts the dominant bottleneck. The original anchor (attempt 5) showed "loop produces no candidate." The new anchor shows the opposite: the loop produces a `+17.4pp` candidate (`78.3% → 95.7%`, every threshold met, only `gs_018` failed) and then **fully discards it** because (1) target `gs_026` records `target_fixed_qids=()` AND `target_still_hard_qids=()` simultaneously — an impossible state proving per-QID delta computation can return `unknown` for evaluated targets (new postmortem F2); (2) one out-of-target soft→hard regression triggers full-AG rollback with no partial-harvest fallback (new postmortem F3). Two new tasks added: **C14-T0** (canonical per-QID target-delta computation; pure-helper + invariant **I13**; prerequisite to C14-T2's render) and sibling **Cycle 14B** (partial harvest with bounded regression debt; new `RollbackClass.ACCEPTED_WITH_DEBT` + `RegressionDebtPolicy`). Old anchor F1, F4-F9 still apply and remain the canonical citations for Cycles 12, 13, 14-T1/T2/T3/T4, 15, 16, 17. Sequencing now: `C12-T1 → {C13, C14-T0} → C14 (T1+T2+T3+T4) → {C14B, C15} → C16 → C17`. Cycle counts and self-check table updated below.

> **Revision (2026-05-09 #4) — Cycle 14-V scoreboard from corpus evidence; Cycle 14-W defect sweep #2.** First post-Cycle-14-V corpus pilot (7Now task `960148942255012`, attempt 11, MERGE_GATE_GAP; airline task `1105451933925748`, attempt 13, READY_TO_MERGE_WITH_ATTRIBUTION_DRIFT) produced fresh evidence on every C14-V-registered defect. **Validated:** D-1 (C14-V T1's `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` fires on 5/5 NO_ACTION reflections — corpus-validated, ready for default-flip in Cycle 14-W T4). **Untested:** D-2 (neither anchor hit C14B-T3's canonical trigger). **Partial:** D-3 (T3 fixed `target_fixed_qids`/`target_still_hard_qids` derivation but `SOFT_PASSING` is not represented in any bucket field — 7Now F2: `gs_026=soft_passing` in `target_delta_states` while both bucket fields are empty AND Phase H reports `target_resolution_failed`). **Regressed in production:** D-4 (`AttributeError: 'list' object has no attribute 'get'` still raises on airline anchor 13 F7 — `_normalize_stage_capture` exists but isn't called at every `.get()` site) + D-5 (manifest `databricks_*` IDs still blank cross-space — `_databricks_ids_from_env` shipped but the production code path doesn't reach it OR dbutils tag names differ). **New defects** from the same evidence: D-6 (Phase H acceptance writer says `outcome=rolled_back/missing_pre_rows` while stdout says ACCEPTED on airline iter 1 — sibling of D-3 in a different writer); D-7 (iteration-summary totality broken — airline emits 1 of 3 expected `GSO_ITERATION_SUMMARY_V1`; `iter_record_counts=[46,54,48,47]` for 3-iter run); D-8 (replay-vs-Phase-H journey-validator drift — 7Now local replay says 25 violations, Phase H says 0). **Headline learning:** airline iter 1 accepted `+12.5pp` via `accepted_with_attribution_drift` (`83.3% → 95.8%`, thresholds met; `target_qids=gs_024` remained still-hard but `AG_DECOMPOSED_H004` improved aggregate without regressions). This is the first in-production demonstration of keep-the-win behavior on a target-drift case — formalized in queued **Cycle 14-C: First-Class Attribution-Drift Partial Harvest** (next plan after 14-W). Two new institutional disciplines promoted from this evidence: **Discipline A** — regressed-defect closures require end-to-end fixture-replay tests, not just unit tests (C14-V D-4/D-5 unit tests passed but production behaviour didn't change). **Discipline B** — multi-path resolvers ship typed `_RESOLVED_V1` tracing markers (D-5's regression was invisible because `_databricks_ids_from_env`'s internal resolution path wasn't traced). All eight defects (D-1 through D-8) tracked in the Defect Registry below; D-3/D-4/D-5 status flipped to `partial/regressed`; D-6/D-7/D-8 registered as new entries. Plan ref: [`2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md`](./2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md). Cycle 14-W is observability + correctness + one default-flip (T4); replay byte-stable. Sequencing now: `C12-T1 → {C13, C14-T0, C14B} → C14-V → C14-W (shipped) → C14-W hardening delta (shipped) → **C14-C** → [combined Tier-3 corpus pilot] → C14-T3 → {C16-T3, C14-T4} → C15 → C16-T1+T2 → C16-T4+T5+T6 → C17`. Open Q#10 (Databricks IDs blank) status reverted from RESOLVED to OPEN — closing again in 14-W T3 with the new tracing discipline.

> **Revision (2026-05-09 #3).** Two post-redeploy lever-loop pilots produced concrete in-production evidence: 7Now task `338386531912450` (attempt 10, MERGE_GATE_GAP) and airline task `833709971504406` (attempt 12, READY_TO_MERGE_WITH_REGRESSION_DEBT). Three big positives: (i) **C14B partial-harvest works in production** — airline AG2 fixed `gs_024`, accepted with regression debt on `gs_016`, `+12.5pp` net accepted (`83.3% → 95.8%`, thresholds met); (ii) **C14-T1+T2 contract progress is real** — `canonical_acceptance_render=true`, `GSO_FULL_EVAL_V1` emitted, `GSO_PHASE_B_END_V1.total_records` populated on both anchors (was zero pre-T1+T2); (iii) **C14-T0's decision logic is correct** — both anchors emit the right `reason_code` (`target_qids_not_improved` for 7Now's aggregate-only gain; `target_fixed_offset_by_regression` for airline's AG3 debt-repayment attempts). Three "shipped-but-silent" defects: (a) `_compute_forbidden_ag_set` runs the C13 admission predicate correctly but the behavior flag `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` is default-off, so 7Now iters 2-5 still re-emit AG1 with 0 proposals — corpus measurement is impossible without flipping defaults (which would change replay byte-stability); (b) `_maybe_run_patch_isolation_orchestrator` runs the C14B-T3 attribution correctly but the behavior flag `GSO_PATCH_SUBSET_ISOLATION` is default-off, so airline AG3's two canonical triggers emit zero diagnostic markers — same corpus-measurement gap; (c) `format_full_eval_marker_payload` derives `target_delta_states` from C14-T0's total function while simultaneously emitting `target_still_hard_qids` / `unknown_to_hard_regressed_qids` from legacy bucket fields — they disagree, producing same-payload contradictions (`gs_026` rendered as `soft_to_hard` AND `target_still_hard_qids`; `gs_021` baseline-hard rendered as `unknown_to_hard`). Plus two ledger-cleanup items: bundle assembler `AttributeError` on list-valued stage capture (anchor 2 F7); run-manifest `databricks_*` IDs blank cross-space (Open Q#10 confirmed). All five defects are scoped to **Cycle 14-V — Shipped-Cycle Defect Sweep** (precedent: Cycle 6) following the iteration-ledger discipline. Plan ref: [`2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md`](./2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md). Cycle 14-V is **observability-only** — no behavior-flag flips, no decision-sequence changes; replay byte-stable with all flags off. Sequencing now: `C12-T1 → {C13, C14-T0, C14B} → C14-V → C14-T3 → {C16-T3, C14-T4} → C15 → C16-T1+T2 → C16-T4+T5+T6 → C17`. After C14-V lands and a fresh corpus run produces shadow-marker evidence, the roadmap's open questions Q#9 (C14B policy tuning) and Q#11 (L6 narrow-replacement promotion) become evidence-driven decisions instead of speculative ones.

> **Revision (2026-05-10 #2) — Cycle 15 rescoped to absorb the deferred stage-migration.** The original Cycle 15 scope (RCA enforcement / Stage 2→4 arrow) ships inside Cycle 15 Phase 2 as a typed-output consumer (`StrategistContextOutput.rca_cards_grounded_only` strips ungrounded RCAs at the type level), not as a standalone wiring change. Cycle 15 also adds two new stages (`run_manifest`, `bundle_assembly` — closes D-4 and D-5 contract surface), one new pre-strategist stage (`strategist_context` — closes the user's "judges→strategist deterministic" goal), and ships the four chunk flags (`GSO_STAGE_HANDLERS_CHUNK_{A,B,C,D}`). Plan ref: [`2026-05-10-cycle-15-stage-contracts-and-boundary-fixtures-plan.md`](./2026-05-10-cycle-15-stage-contracts-and-boundary-fixtures-plan.md). Total stage count goes from 9 to 12 (`evaluation_state`, `cluster_formation`, `rca_evidence`, `strategist_context` (new), `action_group_selection`, `proposal_generation`, `safety_gates`, `applied_patches`, `acceptance_decision`, `learning_next_action`, `bundle_assembly` (new), `run_manifest` (new)). Cycle 15's PR sequence: P0 (scaffolding refresh) → P1 (Chunk D) → P1 default-flip → P2 (Chunk A) → P2 default-flip → P3 (Chunk B) → P3 default-flip → P4 (Chunk C) → P4 default-flip → P5 (transcript transparency + roadmap revision). Replay byte-stability holds with all four chunk flags off; each PR is independently revertable.

> **Revision (2026-05-09 #2).** Two complementary postmortems landed *after* C14-T0 and C14B (T1+T2+T3) merged but *before* the wheel was redeployed to the lever-loop bundle: 7Now task `337676694173049` (attempt 8) and airline task `294637253025289` (attempt 10, run dir `1099b152-...`). Both ran on **pre-T0/pre-C14B production code**, so the headline findings (gs_026 still in `(none)/(none)` impossible state on 337; Phase B/H still rendering `rolled_back/missing_pre_rows` on 294 despite a 100% accepted journey ledger) are **expected legacy behaviour, not new bugs** — they confirm what the upstream cycles will close once redeployed. They do, however, surface **five net-new learnings** on top of already-shipped/drafted work: (1) C14B's pilot 10pp aggregate floor would still reject 337's `+8.7pp` candidate after redeploy → defer policy tuning to a post-redeploy corpus measurement (C14B Risks). (2) **L6 narrow-replacement gap is double-confirmed across both runs** — 337 drops `add_sql_snippet_expression` (causal H002 fix), 294 drops `add_sql_snippet_measure`; both fall through `narrow_not_applicable` with `reason=unrecognized_patch_type`. This is the highest-leverage accuracy unlock the roadmap currently doesn't sequence ahead of process work; raised as **Open Question #11** for explicit promotion-vs-defer decision. (3) Regression-bucket mis-classification (`gs_021` baseline-hard counted as `unknown_to_hard`; `gs_007`/`gs_030` soft signals also counted as `unknown_to_hard`) **inflates C14B's `regression_debt_qids` input** — C16-T3 promoted from "post-hoc cleanup" to **prerequisite for C14B telemetry trustworthiness**. (4) New typed bundle-assembler error class `GSO_BUNDLE_ASSEMBLY_FAILED_V1` with `AttributeError: 'list' object has no attribute 'get'` plus `91/13 missing` while `artifact_index.json` resolves the paths — added to C12-T3 Risks (list-valued stage capture normalization + manifest/index path consistency). (5) Two specific journey transitions (`clustered → already_passing`, `evaluated → post_eval`) fired on a 100%-accepted run as "illegal" — added to C17-T1 as concrete (c)-class entries (state-machine extensions for resolved-success terminal states), with C14-T3's I9 invariant scope updated to exclude these legitimate transitions. No cycle-count or sequencing change in this revision; C14-T0 and C14B remain the immediate post-redeploy verification targets.

## How to read this doc

1. The audit table maps each of the 11 contract stages to **what shipped**, **what's missing**, and the **target cycle** that closes the gap.
2. The seven proposed cycles (12 → 17, with **Cycle 14B as a sibling parallel-shipping cycle to Cycle 14**) are ordered by dependency. Each cycle has:
   - **Inspiration run** — the anchor evidence justifying the cycle.
   - **What this cycle closes** — the user-visible improvement.
   - **Stage(s) closed** — mapping to the 11-stage contract.
   - **Current state (audited)** — what's already in the codebase.
   - **What changes** — the per-task plan.
   - **Binary success criteria** — verifiable assertions, one per task.
   - **Dependencies** — upstream prerequisites by cycle and (where it matters) by specific task.
   - **Sizing**, **Flag(s)**, **Risks** — operational concerns.
   - **Downstream coordination points** — where later cycles consume this cycle's outputs. A reader of any single cycle sees both directions of the data flow without having to reconstruct it.
3. Implementation plans for individual cycles are **deferred** — they will be drafted as dated plan docs (e.g. `2026-05-08-cycle-12-...-plan.md`) following the [`writing-plans`](../../../.cursor/skills-cursor/writing-plans/SKILL.md) skill.
4. Each cycle ships **one or two behavior flags maximum** behind the existing flag-discipline (Cycle 16 ships three because two are warn-only pilots and one is a default-flip; Cycle 12 ships two behavior flags plus two emit-only typed-observability markers, which are not flags). Replay byte-stability holds with all behavior flags off.
5. **Invariant IDs** (`I1`-`I13`) are canonical: registered in `optimization/invariants.py` under exactly those identifiers. C16-T4's contract-health summary reads invariants by canonical ID, so the upstream cycle that introduces an invariant (**C14-T0 → I13**; C14-T3 → I9; C14-T4 → I10; C16-T1/T2 → I11; C17-T3 → I12) must register it under the canonical ID, not a free-form name.

## Anchor evidence

This roadmap is grounded in **eight** lever-loop attempts across two optimization-run directories — five sequential attempts on 7Now and three on the airline workspace. Anchors #5 and #6 are the post-redeploy pilots that motivated **Cycle 14-V** (now shipped); anchors #7 and #8 are the **post-Cycle-14-V** pilots that motivate **Cycle 14-W** (defect sweep #2) and **Cycle 14-C** (first-class attribution-drift partial harvest):

1. **Original 7Now anchor — task `534010336956422` (attempt 5, `TERMINATED/SUCCESS` with `MERGE_GATE_GAP`).** Five iterations on the 7Now space ([`runid_analysis/3b050ec5-...`](./runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097/postmortem.md)). Iteration 1 ran a real loop; iterations 2-5 were empty-proposal repeats of the same AG. Replay fixture emitted 85 invariant violations (I3, I4, I7) but the loop did not stop. Original postmortem findings F1-F9 are the canonical citations for **Cycles 12, 13, 15, 16, 17 and Cycle 14 tasks T1-T4**.
2. **Acceptance-stage 7Now anchor — task `76457773587391` (attempt 7, `MERGE_GATE_GAP`; postmortem refreshed 2026-05-09).** Iteration 1 produced a `+17.4pp` candidate (`78.3% → 95.7%`, all configured thresholds met, only `gs_018` failed). The candidate was fully rolled back because target `gs_026` recorded `target_fixed_qids=()` AND `target_still_hard_qids=()` simultaneously (impossible state — new postmortem F2), and one out-of-target soft→hard regression (`gs_018`) triggered full-AG discard with no partial-harvest fallback (new postmortem F3). Iterations 2-5 still empty-proposal (new postmortem F5 — Cycle 13 still relevant). New postmortem findings F2 + F3 motivate the two new tasks added on 2026-05-09: **Cycle 14-T0** (per-QID delta correctness) and **Cycle 14B** (partial harvest with regression debt).
3. **Pre-redeploy confirmation 7Now anchor — task `337676694173049` (attempt 8, `MERGE_GATE_GAP`; postmortem 2026-05-09 #2).** Five iterations on 7Now after C14-T0 + C14B merged but *before* the wheel was redeployed to the lever-loop bundle. Candidate accuracy `78.3% → 87.0%` (`+8.7pp`); rolled back with reason `target_qids_not_improved` (legacy reason, not T0's `target_resolution_failed`); no `GSO_PATCH_ISOLATION_DIAGNOSTIC_V1` markers emitted. **Confirms the deploy state, not new bugs.** Surfaces three net-new learnings: (a) C14B's pilot 10pp aggregate floor would still reject this candidate post-redeploy → policy-tuning corpus measurement deferred (C14B Risks); (b) regression-bucket mis-classification — `gs_021` (baseline-hard H004) bucketed as `unknown_to_hard`; `gs_007`/`gs_030` (soft signals S001) also bucketed as `unknown_to_hard` → C16-T3 promoted to prerequisite for C14B telemetry; (c) `add_sql_snippet_expression` for the H002 causal fix `lost_at:applyability` with `narrow_not_applicable, reason=unrecognized_patch_type` (new postmortem F5/F6).
4. **First 100%-accepted airline anchor — task `294637253025289` (attempt 10 in [`runid_analysis/1099b152-...`](./runid_analysis/1099b152-8655-4f1e-ab43-1240a9400280/postmortem.md), `READY_TO_MERGE_WITH_CONTRACT_GAPS`; postmortem 2026-05-09 #2).** Single iteration on the airline space. Baseline `83.3%` → final accepted `100.0%`; one accepted AG (`AG_DECOMPOSED_H004` targeting `gs_024`); zero rolled-back AGs; thresholds met. **First concrete F8 instance with full evidence:** stdout final summary + journey ledger say accepted, while Phase B/H stage output and operator transcript both render `acceptance_decided outcome=rolled_back reason=missing_pre_rows`. Surfaces three net-new learnings on top of #3's: (d) `GSO_BUNDLE_ASSEMBLY_FAILED_V1` with `AttributeError: 'list' object has no attribute 'get'` and manifest reporting `91/13 missing` while `artifact_index.json` resolves them → added to C12-T3 Risks; (e) `add_sql_snippet_measure` (`SUM(tkt_payment.PAYMENT_AMT)`) dropped by blast-radius with `narrow_not_applicable, reason=unrecognized_patch_type` → confirms #3(c) is a multi-patch-type problem, not a one-off; (f) two specific trunk transitions (`clustered → already_passing` for `gs_007`, `evaluated → post_eval` for `gs_016`) fire as "illegal" on a 100%-accepted run → C17-T1 (c)-class entries; C14-T3's I9 must distinguish stale-illegal from post-resolution-legitimate.

5. **Post-redeploy 7Now anchor — task `338386531912450` (attempt 10 in `runid_analysis/3b050ec5-...`, `MERGE_GATE_GAP`; postmortem 2026-05-09 #3).** First lever-loop run on 7Now after C14-T0, C14B (T1+T2+T3), C14-T1+T2, and C13 *code* shipped. Five iterations: iter 1 produced a `+8.7pp` candidate (`78.3% → 87.0%`), correctly rolled back because `gs_026` remained hard (`reason_code=target_qids_not_improved`); iters 2-5 repeated AG1 with `Proposals (0 total)`. **Confirms three things:** C14-T1 (Phase B totality, `total_records=372`), C14-T2 (canonical render, `GSO_FULL_EVAL_V1` + `canonical_acceptance_render=true`), and C14-T0 acceptance logic (decision sequence is correct). **Surfaces three "shipped-but-silent" defects** (all scoped to Cycle 14-V): (i) C13 admission predicate cannot be measured because `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` is default-off; (ii) canonical render emits `gs_026` as both `soft_to_hard` (in `target_delta_states`) and `target_still_hard_qids` simultaneously — same-payload contradiction; (iii) `gs_021` (baseline-hard cluster H004) misclassified as `unknown_to_hard_regressed_qids`. Also confirms Open Q#10 (manifest `databricks_*` IDs blank).
6. **Post-redeploy airline anchor — task `833709971504406` (attempt 12 in `runid_analysis/1099b152-...`, `READY_TO_MERGE_WITH_REGRESSION_DEBT`; postmortem 2026-05-09 #3).** **First in-production demonstration of C14B partial harvest accepting a candidate with bounded regression debt.** Four iterations: iter 1 accepted AG1 fixing `gs_013` (`83.3% → 87.5%`); iter 2 accepted AG2 with regression debt on `gs_016` (`87.5% → 95.8%`, fixed `gs_024`); iters 3-4 attempted AG3 to repay the `gs_016` debt and rolled back twice with `target_fixed_offset_by_regression` (fixed `gs_016`, regressed `gs_007`/`gs_009`/`gs_024`); convergence at `95.8%`, `thresholds_met=true`, `reason=plateau_unresolved_hard_failures_quarantined`. **Confirms four things:** C14B-T1+T2 partial harvest works in production (`+12.5pp` net accepted with explicit debt accounting); C14-T1 (Phase B totality, `total_records=323`); C14-T2 (canonical render); the `target_fixed_offset_by_regression` reason code is correctly emitted by the gate. **Surfaces two "shipped-but-silent" defects** (both scoped to Cycle 14-V): (i) C14B-T3 diagnostic-only orchestrator cannot be measured because `GSO_PATCH_SUBSET_ISOLATION` is default-off — neither `GSO_PATCH_ISOLATION_DIAGNOSTIC_V1` nor `OUTCOME_V1` markers emitted on either canonical-trigger iteration; (ii) canonical render emits `gs_016` as `unknown_to_hard_regressed_qids` despite known journey state. Also confirms anchor #4's `GSO_BUNDLE_ASSEMBLY_FAILED_V1` `AttributeError: 'list' object has no attribute 'get'` (anchor 2 F7) is reproducible across attempts — added to Cycle 14-V T5.

7. **Post-Cycle-14-V 7Now anchor — task `960148942255012` (attempt 11 in `runid_analysis/3b050ec5-...`, `MERGE_GATE_GAP`; postmortem 2026-05-09 #4).** First lever-loop run on 7Now after C14-V code shipped. Five iterations: iter 1 produced a `+8.1pp` candidate (`78.3% → 86.4%`), correctly rolled back because target `gs_026` was rendered as `soft_passing` (partial improvement, not fixed); iters 2-5 repeated AG1 with `Proposals (0 total)`. **Validates C14-V T1:** `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` fires across iters 2-5 with `would_admit_with_admit_no_action_on=true` + `suppressed_by_admit_no_action_off=true` — corpus evidence ready for default-flip in **Cycle 14-W T4**. **Surfaces three defects:** (i) D-3 extension — `target_delta_states=[["gs_026", "soft_passing"]]` while `target_fixed_qids=[]` AND `target_still_hard_qids=[]` AND Phase H separately reports `target_resolution_failed` (3-way contradiction; `SOFT_PASSING` not represented in any bucket field; closing in C14-W T1); (ii) D-5 regression — manifest IDs still blank (closing in C14-W T3); (iii) D-8 (new) — local replay reports 25 journey violations; Phase H `journey_validation_all.json` reports 0 — same defect class as D-3 but in a different writer (Phase H journey validator; closing in C14-W T6).
8. **Post-Cycle-14-V airline anchor — task `1105451933925748` (attempt 13 in `runid_analysis/1099b152-...`, `READY_TO_MERGE_WITH_ATTRIBUTION_DRIFT`; postmortem 2026-05-09 #4).** **First in-production demonstration of `accepted_with_attribution_drift` keep-the-win acceptance.** Three iterations: iter 1 accepted `AG_DECOMPOSED_H004` with `reason_code=accepted_with_attribution_drift`, improving `83.3% → 95.8%` and reducing failed set to `[gs_024]` only — even though `target_qids=gs_024` remained `still_hard` (target attribution drift). Iter 2 + iter 3 rolled back correctly (regressed `gs_007`; no gain). `GSO_CONVERGENCE_V1.thresholds_met=true`, `reason=plateau_unresolved_hard_failures_quarantined`. **Confirms three things:** the `accepted_with_attribution_drift` reason code (control_plane.py:1118, 1293) is real and works in production; the gate correctly rejects later regression-causing AG2/AG3; convergence gate correctly quarantines `gs_024` as the remaining hard cluster. **Surfaces five defects:** (iv) D-4 regression — `GSO_BUNDLE_ASSEMBLY_FAILED_V1` still raises `AttributeError: 'list' object has no attribute 'get'` (C14-V T5 fix didn't reach this call site; closing in C14-W T2); (v) D-5 regression confirmed cross-space; (vi) D-6 (new) — Phase H `iterations/iter_01/stages/09_acceptance_decision/output.json` says `outcome=rolled_back, reason_code=missing_pre_rows` while stdout says ACCEPTED — Phase H acceptance writer drift (closing in C14-W T6); (vii) D-7 (new) — `GSO_PHASE_B_END_V1.iter_record_counts=[46,54,48,47]` (4 buckets) for 3-iter run; only 1 of 3 expected `GSO_ITERATION_SUMMARY_V1` emitted (totality break; closing in C14-W T5); (viii) **headline learning** — `accepted_with_attribution_drift` is real and emergent but the gain is attributed to still-hard `gs_024` rather than reattributed to actually-improved QIDs; needs first-class formalization in queued **Cycle 14-C**.

Anchors #1-3 + #5 + #7 share the 7Now optimization-run directory because they are sequential attempts in the same lever-loop sequence; #2 supersedes #1 as the primary evidence for acceptance-stage scope but #1's findings on RCA grounding (F6/F7), strategist learning (F5), causal continuity, regression bucketing, and contract-health observability remain the canonical citations for the cycles they motivated. #3 confirms what #2 surfaced and is the citation for the three "deploy-state" learnings (C14B policy tuning, C16-T3 promotion, L6 narrow-replacement). #4 is from a different workspace and is the canonical citation for the cross-space confirmations: F8 projection drift (now with two-surface evidence), the bundle-assembler `AttributeError`, and the journey-state taxonomy gap. **#5 and #6 are the post-redeploy pilots** — both demonstrate that the *code* shipped in C12-T1, C13, C14-T0, C14-T1+T2, and C14B is working as designed, and surface the five "shipped-but-silent" defects that Cycle 14-V closes (defect registry below).

---

## Defect registry — single source of truth for shipped-but-silent defects

> **Purpose.** Every "shipped" cycle that subsequently exhibits an in-production defect is registered here with its anchor evidence, root cause classification, and closing cycle. The goal is **whack-a-mole prevention**: a defect cannot drift between roadmap, ledger, troubleshooting guide, and postmortem narratives because all four cite this registry. A defect leaves the registry only when its closing cycle ships AND a fresh corpus run produces zero re-emissions of its associated regression-rail marker.

| ID | Defect | Cycle that shipped the code | Anchor | Root cause class | Closing cycle | Status | Regression rail |
|---|---|---|---|---|---|---|---|
| D-1 | C13 admission predicate not corpus-measurable (NO_ACTION reflections invisible under flag-default-off) | C13 (forbidden-AG admission) | Anchor #5 (7Now run 338, F7); validated by Anchor #7 F5 | corpus-measurement gap (flag default-off + no shadow-mode emission) | C14-V T1 (shadow); **C14-W T4** (default-flip promotion) | **corpus-validated** by C14-V T1; closing default-flip in C14-W T4 | `GSO_FORBIDDEN_AG_ADMISSION_BYPASSED_V1` (C14-V T4) — must stay silent post-flip |
| D-2 | C14B-T3 diagnostic orchestrator not corpus-measurable (canonical reason codes invisible under flag-default-off) | C14B-T3 (patch-subset isolation) | Anchor #6 (airline run 833, F3) | corpus-measurement gap (flag default-off + no shadow-mode emission) | **C14-V T2** | shipped pending corpus trigger — neither anchor #7 nor #8 hit C14B-T3's canonical trigger; awaiting future pilot evidence | `GSO_PATCH_ISOLATION_TRIGGER_NOT_ENGAGED_V1` (C14-V T4) |
| D-3 | Canonical render contradicts itself within the same payload (target_delta_states vs legacy bucket fields) | C14-T2 (canonical render) | Anchors #5 + #6 (F4 in each); extended by Anchor #7 F2 | implementation bug (parallel derivation paths in `format_full_eval_marker_payload`); extended scope: `SOFT_PASSING` representation + `unknown_to_hard` derivation | C14-V T3 (partial); **C14-W T1** (extension to `target_soft_passing_qids`) | **partial** — C14-V T3 fixed `fixed`/`still_hard`; C14-W T1 closes `soft_passing` | `GSO_CANONICAL_RENDER_INVARIANT_V1` (C14-V T4) |
| D-4 | Bundle assembler raises `AttributeError: 'list' object has no attribute 'get'` on list-valued stage capture | C12-T3 (bundle assembler) | Anchor #6 (airline run 833, F7); Anchor #4; **regressed in Anchor #8 F7** | implementation bug (missing list normalization); regression: function exists but not called at every `.get()` site | C14-V T5 (unit-tested only); **C14-W T2** (call-site coverage + airline fixture replay) | **regressed in production** — closing properly in C14-W T2 with Discipline A integration test | `GSO_BUNDLE_ASSEMBLY_FAILED_V1` (existing); diagnostic `GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1` |
| D-5 | `GSO_RUN_MANIFEST_V2` emits with blank `databricks_job_id` / `lever_loop_task_run_id` | C12-T1 (run manifest V2) | Anchor #5 F9 + Anchor #6 F8; **regressed in Anchors #7 F8 + #8 F8** | implementation bug (env-fed population not wired at call site); regression: `_databricks_ids_from_env` exists but production code path returns blank rather than sentinel | C14-V T6 (unit-tested only); **C14-W T3** (resolution-path tracing + Jobs-runtime integration test) | **regressed in production** — closing properly in C14-W T3 with Discipline B tracing marker | `GSO_DATABRICKS_IDS_RESOLVED_V1` (C14-W T3 diagnostic) |
| D-6 | Phase H acceptance writer says `outcome=rolled_back/missing_pre_rows` while stdout says ACCEPTED on the same iteration | C12-T3 (Phase H acceptance writer); C14-T2 (canonical render scope did not extend to Phase H writer) | Anchor #8 F8 | implementation bug (parallel derivation in Phase H acceptance writer; sibling of D-3 in a different writer) | **C14-W T6** | new (registered 2026-05-09 #4) | `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1` (C14-W T6) |
| D-7 | Iteration-summary totality broken — fewer `GSO_ITERATION_SUMMARY_V1` emitted than attempted iterations; `iter_record_counts` cardinality drifts | C14-T1 (Phase B totality) | Anchor #8 F7 | implementation bug (summary emitted only on accepted-iteration code path; should emit per attempted iteration) | **C14-W T5** | new (registered 2026-05-09 #4) | `GSO_ITERATION_SUMMARY_TOTALITY_V1` (C14-W T5) |
| D-8 | Replay-vs-Phase-H journey-validator drift (local replay reports N violations; Phase H `journey_validation_all.json` reports 0) | C12-T3 (Phase H journey validator); C17-T1 (state-machine extensions) | Anchor #7 F8 | implementation bug (parallel derivation in Phase H journey validator; sibling of D-3 in a third writer) | **C14-W T6** | new (registered 2026-05-09 #4) | `GSO_PHASE_H_JOURNEY_DRIFT_V1` (C14-W T6) |

**Closing protocol (revised 2026-05-10 — three-tier closure model).**
A defect's `Status` carries the tier it has reached. Movement
between tiers is one-way:

| Tier | Status name | Evidence required | Cost |
|---|---|---|---|
| 1 | `closed-local` | Unit tests + replay-fixture integration tests green; production code path exercised via vendored fixtures; flags-off byte-stability preserved. | Free (local CI) |
| 2 | `closed-runtime` | Tier-1 evidence + a one-off diagnostic micro-job in the production runtime returns the expected resolution-path trace. | ~30 seconds of cluster time per anchor |
| 3 | `closed-corpus` | Tier-1 + Tier-2 evidence + a fresh lever-loop pilot on each anchor space confirms the regression rail stays silent across multiple iterations. | ~30+ minutes of cluster time per anchor; non-deterministic (LLM proposal + arbiter variance) |

**Tier selection rules:**
- **Tier 1 sufficient** for: observability-only cycles (C12-T2/T3, C14-V T1+T2+T3+T4, C14-W hardening); pure-helper correctness fixes (canonical render contradictions, normalisation guards); default-flips with prior-cycle shadow-corpus validation (D-1 / C13 promotion in C14-W T4).
- **Tier 2 required** when: a defect's closure depends on a runtime-only fact unavailable to local fixtures (env-var resolution behaviour, cluster-tag presence, DBUtils API shape, network-time secrets — D-5 is the canonical example).
- **Tier 3 required** when: the cycle introduces new behaviour that fixtures cannot validate (new attribution accounting in C14-C, new lever payload shapes in C14-T3, new control-plane reflection in C15, new burn-down telemetry in C16-T3, new state-machine transitions in C17-T1).

**Movement protocol:**
1. `open` → `closed-local`: closing cycle ships AND its replay-fixture integration tests pass on vendored anchor fixtures AND the regression rail emits zero false positives in flags-off byte-stability replay.
2. `closed-local` → `closed-runtime`: a diagnostic micro-job in the production runtime returns the expected resolution-path trace (Discipline D below). Required only if the defect's closure depends on a runtime-only fact (Tier-2 selection rule).
3. `closed-runtime` → `closed-corpus`: a fresh lever-loop pilot on each anchor space confirms the regression rail stays silent. Required only if the cycle introduces new behaviour fixtures cannot validate (Tier-3 selection rule).

If the regression-rail marker fires at any tier check, register a **new** defect ID (D-N+1) citing the regression; do NOT demote the original ID. The original closure stays valid at its tier; the new defect is a downstream regression with its own anchor evidence.

**Per-defect Status (after C14-W hardening delta lands):**

| ID | Status |
|---|---|
| D-1 | `closed-local` (corpus-validated by C14-V; default-flipped by C14-W T4; replay-fixture parity in C14-W hardening T1) |
| D-2 | `closed-local pending Tier-3` (untested in C14-V/W corpus pilots — neither anchor hit C14B-T3's canonical trigger; will close at Tier-3 when a future anchor exercises the orchestrator) |
| D-3 ext | `closed-local` (replay-fixture parity in C14-W hardening T1) |
| D-4 | `closed-local` (assemble_bundle_for_replay end-to-end in C14-W hardening T2) |
| D-5 | `closed-local pending Tier-2` (T6 diagnostic micro-job confirms cross-space behaviour) |
| D-6 | `closed-local` (production-wired in C14-W hardening T4) |
| D-7 | `closed-local` (production-wired in C14-W hardening T3) |
| D-8 | `closed-local` (production-wired in C14-W hardening T5) |

**Per-defect Status (after C15 Phase 1 lands):**

| ID | Status |
|---|---|
| D-1 | `closed-corpus` (corpus-validated by C14-V; default-flipped by C14-W T4; replay-fixture parity in C14-W hardening T1) |
| D-2 | `closed-local pending Tier-3` |
| D-3 ext | `closed-local` (canonical `EvalRow.is_passing()` predicate in C15-P1.2; replay-fixture parity in C15-P1.8) |
| D-4 | `closed-local` (typed `BundleAssemblyInput` with `StageCaptureNormalized` in C15-P1.5; replay-fixture parity in C15-P1.8) |
| D-5 | `closed-local pending Tier-2` (typed `RunManifestOutput` with `ResolutionPath` enum in C15-P1.6; Tier-2 confirmation in next corpus pilot) |
| D-6 | `closed-local` (Phase H acceptance writer consumes `AgOutcome` in C15-P1.10) |
| D-7 | `closed-local` (totality emitted from `LearningOutput.iteration_summaries` in C15-P1.11) |
| D-8 | `closed-local` (Phase H journey writer consumes `LearningOutput` in C15-P1.10) |

**Anti-pattern guard.** Defects D-1 and D-2 are *not* implementation bugs — both shipped cycles' code is correct. They are corpus-measurement gaps caused by the warn-only-pilot-first discipline applied to a behavior flag. Cycle 14-V's pattern (shadow-mode observability under a separate default-on observe flag) preserves the warn-only-pilot discipline while making the underlying admission/orchestrator measurable on real corpus runs. **Anchor #7 F5 corpus-validates the pattern** (5/5 NO_ACTION reflections traced; rail silent). Future cycles that introduce a new behavior flag with default-off should follow the same shadow-mode pattern from day one to avoid registering a fresh D-N entry on the next pilot. **Open Q#12 promoted from "provisional" to "standard discipline"** in iteration-ledger plan revision #4 (Cycle 14-W closeout artifact).

**Discipline A (regressed-defect closures require integration tests, not just unit tests; promoted 2026-05-09 #4).** D-4 and D-5 regressed in production despite C14-V T5/T6 unit tests passing. Root cause: unit tests didn't exercise the production code path. Cycle 14-W T2 (D-4) and T3 (D-5) ship anchor-fixture-replay integration tests. Future regressed-defect closures must include an end-to-end integration test that replays the actual production failure shape; if the unit test was already passing pre-defect-closure, the unit test is insufficient and must be extended.

**Discipline B (multi-path resolvers ship typed `_RESOLVED_V1` tracing markers; promoted 2026-05-09 #4).** D-5's regression was invisible because `_databricks_ids_from_env`'s internal resolution path (env / dbutils / sentinel) wasn't traced. Future multi-path resolver functions should emit a typed marker recording which path fired, so corpus measurement catches "function reached but wrong path" failures.

**Discipline E (perimeter typing; promoted 2026-05-10 #2 in Cycle 15).** Every stage's `execute` perimeter takes one frozen Input and returns one frozen Output. Internal helpers may use dicts, but cross-stage data flow is typed. New stages must add `from_<source_stage>(...)` constructor methods, not `dict.get(...)` calls in the next stage's body. Enforced by `tests/unit/test_stage_conformance_jsonio.py`.

**Discipline F (boundary fixtures from production; promoted 2026-05-10 #2 in Cycle 15).** Boundary fixtures are vendored from real anchor runs, never synthesised. New stages ship at minimum two fixtures (one airline, one 7Now). Refreshing a fixture requires explicit signed approval (PR title token `[fixture-refresh]`) and a paired `expected_output.json` regeneration justified in the PR description. Enforced by `tests/integration/test_chunk_*_replay.py`.

**Discipline G (transcript renders typed I/O; promoted 2026-05-10 #2 in Cycle 15).** Every Input/Output dataclass implements `to_pretty()`. Operator transcript renders stage-by-stage I/O in a fixed format. Free-form transcript text is not added; new diagnostic information becomes a typed field on a stage Output. Enforced by `tests/integration/test_operator_transcript_snapshot.py`.

### Combined C14-W + C14-C Tier-3 corpus pilot protocol (2026-05-10)

After both [C14-W hardening delta](./2026-05-10-cycle-14-w-hardening-delta-plan.md)
and [C14-C](./2026-05-10-cycle-14-c-first-class-attribution-drift-partial-harvest-plan.md)
PRs ship to main, run **one combined Tier-3 corpus pilot** to
ratify both cycles together. Two lever-loop runs:

1. **Airline anchor pilot** — task `_target_space=airline`. Five
   iterations. Pass criteria:
   - C14-W D-5 Tier-2 sanity: `GSO_DATABRICKS_IDS_RESOLVED_V1`
     emits with `resolution_path ∈ {env, dbutils, mixed}` AND
     `fields_resolved == fields_total`. (D-5 status flips to
     `closed`.)
   - C14-W rails silent on clean iterations: every drift / totality
     marker introduced by the hardening delta emits zero false
     positives.
   - C14-C: iter 1 must reproduce `accepted_with_attribution_drift`
     with non-empty `accidentally_improved_qids` and
     `unresolved_target_debt_qids = [gs_024]`. Marker
     `GSO_ATTRIBUTION_DRIFT_V1` fires exactly once.
   - C14-C strategist behaviour: with
     `GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST=1` set as a corpus-pilot
     override (NOT as a default flip), iter 2's strategist input
     contains the `unresolved_target_debt_qids` slot. The pilot
     captures whether the strategist subsequently re-targets
     `gs_024` — a binary observation, not a pass/fail gate.

2. **7Now anchor pilot** — task `_target_space=7now`. Five
   iterations. Pass criteria:
   - C14-W rails silent (this anchor exercises the post-fix
     totality, journey-drift, and acceptance-drift surfaces).
   - C14-C does NOT trigger the drift branch on iter 1 (7Now's
     iter-1 candidate stays below thresholds; the branch's
     `thresholds_met=True` precondition is not satisfied).
     `GSO_ATTRIBUTION_DRIFT_V1` emits zero times.
   - The strategist input on every iteration is byte-identical to
     pre-pilot when `GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST=0`
     (i.e., default-off mode) — confirms the flag's invariant B.

Both pilots must complete cleanly for the combined ratification.
Failure modes:

- A C14-W rail fires unexpectedly → register as D-9 in the Defect
  Registry; do NOT reopen the closed-local entries.
- A C14-C reattribution invariant fails → register as a new C14-C
  defect with anchor evidence; revert C14-C if invariant A or B is
  violated.
- The strategist behavioural observation reveals zero re-targeting
  effect → not a failure; informs the follow-up cycle scope.

Expected total cluster cost: ~60 minutes (one airline run + one
7Now run, ~30 minutes each). This is the next time lever-loop
budget is justified after C14-V's validation pilot. Any future
defect closures stay at Tier-1 / Tier-2 unless they introduce
behaviour fixtures cannot validate.

---

## Audit: 11 stages × current state

| # | Stage (contract key) | Letter | Spirit | What's shipped | What's missing | Target cycle |
|---|---|---|---|---|---|---|
| 1 | `evaluation_state` | partial | partial | Baseline accuracy + hard/soft enumeration emitted; `eval_classified` decision records exist | Phase B per-iter aggregator runs only on happy path; producer exceptions bypass it → `GSO_PHASE_B_END_V1.total_records=0` while replay holds records (F9) | **Cycle 14** |
| 2 | `rca_evidence` | violated | violated | `rca_formed` decision records emitted when RCA succeeds; `I7` invariant fires when AGs reach emit ungrounded | No `cluster_blocked_no_rca` emit on the alternative path; I7 is observational, not a gate (F7) | **Cycle 15** |
| 3 | `cluster_formation` | adheres | partial | `cluster_selected` decision records emitted; H001-H005 enumerated | Clusters reach AG-emit without grounded RCA cards (downstream of Stage 2) | auto-closes when Cycle 15 ships |
| 4 | `action_group_selection` | adheres | violated | `strategist_ag_emitted`, `_compute_forbidden_ag_set`, AG collision guard | Forbidden set only admits `CONTENT_REGRESSION` reflections with non-empty `lever_set`; `no_proposals` doesn't qualify on either count → strategist re-emits same AG (F6, I4 fired ×4) | **Cycle 13** |
| 5 | `proposal_generation` | partial | partial | `proposal_generated` records when proposals exist | No typed `proposals_empty` decision record; the empty-result reflection entry is text-only and carries `levers=[]` (F6) | **Cycle 13** |
| 6 | `safety_gates` | adheres | violated | Blast-radius gate; `no_causal_applyable_halt` flag (production-locked); `l6_narrow_replacement_on_hcrf_enabled` (default-on); `narrow_replacement_for_expression` (default-off, semantically wrong for metric views) | When the *structural* causal patch is dropped but RCA-grounded *non-structural* patches survive, halt-on-no-causal-applyable does not fire because some causal patches still survive. Branch A `query_id`-in-CASE form is wrong for metric views (no `query_id` column). Branch C (L5 example SQL) not implemented. (F2, F3) | **Cycle 16** |
| 7 | `applied_patches` | partial | partial | `patch_applied` records use `expanded_patch_id` in canonical paths; `patch_selection.py` and `decision_emitters.py` prefer it | Split L5 instruction sections still emit bare `P001#1..#4` in some call sites; identity is non-injective downstream (F8) | **Cycle 14** |
| 8 | `post_patch_evaluation` | partial | partial | `qid_resolution` records exist; four canonical regression buckets shipped (`soft_to_hard`, `passing_to_hard`, `unknown_to_hard`, `regression_debt`); P1 disjoint-union invariant default-on | (a) Per-QID delta computation can return `unknown` for evaluated target QIDs → `target_fixed_qids=()` AND `target_still_hard_qids=()` simultaneously (new F2); (b) no explicit `existing_hard_still_hard_outside_target` bucket → already-hard `gs_021` lands in `unknown_to_hard` bucket (old F5) | **Cycle 14-T0** (delta totality) **+ Cycle 16-T3** (bucket completeness) |
| 9 | `acceptance_decision` | violated | violated | Three different acceptance objects: `acceptance_policy.AcceptanceDecision` (delta-based), `control_plane.ControlPlaneAcceptance` (per-QID buckets), `lever_loop_stdout_parser.AcceptanceDecision` (parser dataclass) | (a) No single canonical render path → stdout `GSO_FULL_EVAL_V1.reason=target_qids_not_improved` while replay `acceptance_decided.reason_code=missing_pre_rows` for the same event (old F4 / new F8); (b) no partial-harvest policy → a candidate that fixes ≥1 hard cluster but introduces bounded out-of-target debt is fully discarded (new F3) | **Cycle 14** (canonical render) **+ Cycle 14B** (partial-harvest policy) |
| 10 | `learning_next_action` | violated | violated | Reflection buffer + `_build_reflection_entry` + Cycle 11 `learning_next_action` typed record per iteration | `no_proposals` reflection entry sets `levers=[]` and rollback class `OTHER`; learning is write-only (F6) | **Cycle 13** |
| 11 | `contract_health` | observational | violated | Loop invariants I1-I8 + strict mode (default-on for CI/replay); Phase H manifest strict validation wired-but-silent (default-on flag, broad `try/except` swallows exit status); 359 decision-validation issues, 25 illegal trunk transitions, 127/163 manifest paths missing on the anchor run (5 parent-level have no producer, ~122 are per-iteration prefix mismatches) | No typed `GSO_CONTRACT_HEALTH_V1` summary; no `MERGE_GATE_BLOCKED` exit contract; strict mode in production not enforced; journey-validation invalidity does not block AND its producers emit illegal transitions (producer bug, not just gate gap); 5 parent-level bundle paths have no producer; ~7 per-iteration paths × N iterations are written under legacy `phase_a/`/`phase_b/` prefixes instead of contract paths; run manifest missing experimental-setup fields (`wheel_sha`, `git_sha`, `effective_flags`, `python_version`) | **Cycle 12 (T1-T5) + Cycle 15 (typed-stage transparency) + Cycle 16 + Cycle 17** |

## Audit: 4 inter-stage arrows that make the loop scientific

| Arrow | Required so that... | Currently | Closed by |
|---|---|---|---|
| `Stage 2 → Stage 4` | RCA constrains AG emit | I7 observational; no enforcement | **Cycle 15** |
| `Stage 9 → Stage 10` | Learning reads one decision, not two | Stdout vs replay disagree (F4) | **Cycle 14** |
| `Stage 10 → Stage 4` | Strategist sees what is now forbidden | I4 fires ×4 in same run; loop ignores it | **Cycle 13** |
| `Stage 11 → run exit` | Contract health gates merge | Run completes SUCCESS with 359 validation issues | **Cycle 16** |

---

## Cycle 12 — Discipline plumbing

**Inspiration runs.** `3b050ec5-...` (postmortem F9), `1099b152-...` (the prior run where we couldn't tell which wheel/flags shipped).

**What this cycle closes.** Five things make every subsequent cycle measurable and contract-letter compliant on artifact production:
1. The run manifest carries enough experimental-setup metadata that any postmortem can answer "what code/flags actually ran?" from one record.
2. Phase H strict-validation is *typed and observable*: the validator's status (flag / listing / validator exit codes + exception class) reaches stdout on every run, so the "ran-but-silent" failure mode is no longer possible. Its `manifest_path_missing` output reaches `manifest.missing_pieces` without false-flagging the assembler's own self-write paths.
3. Five missing parent-level producers (`decision_trace_all`, `journey_validation_all`, `replay_fixture`, `scoreboard`, `failure_buckets`) are wired, so the assembler actually materializes every declared parent-bundle path (was 4/9; will be 9/9). A post-upload `assembler_completeness_check` enumerates any remaining gap as a typed marker.
4. Per-iteration paths are migrated from their legacy `phase_a/`/`phase_b/` prefixes to the contract-declared `gso_postmortem_bundle/iterations/iter_NN/...` layout, closing the "Layer B" gap surfaced by T3's completeness check.
5. The three "assumed shipped" closeout audits (`mlflow_audit.gso_postmortem_bundle`, operator-transcript per-iteration firing, exit-JSON pointer completeness) are run and either confirm compliance or produce a typed gap record.

**Stage(s) closed.** Stage 11 partial:
- *experimental-setup-record subgoal* — closed by T1.
- *strict-validator typed-observability subgoal* (newly identified during T2 drafting; was previously implicit under "validator wiring") — closed by T2.
- *storage-contract artifact production at the parent-bundle layer* — closed by T3.
- *closeout audit subgoal* (`mlflow_audit.gso_postmortem_bundle` + per-iteration transcript firing + exit-JSON pointer set) — closed by T4.
- *storage-contract artifact production at the per-iteration layer* — closed by T5.

Merge-gate enforcement (`Stage 11 → run exit` arrow) is Cycle 16; producer correctness for replay validity is Cycle 17.

**Current state (audited; refined after drafting T1-T3 plans).**
- `run_manifest_marker` exists in `optimization/run_analysis_contract.py:36`. Fields today: `optimization_run_id`, `databricks_job_id`, `databricks_parent_run_id`, `lever_loop_task_run_id`, `mlflow_experiment_id`, `space_id`, `event`. Emitted at lever-loop start and end (`harness.py:22985`).
- `phase_h_manifest_strict_validation_enabled` exists in `common/config.py:5660`, default-on. The validator block at `harness.py:23080-23165` walks declared paths AND is correctly wired into `_missing_pieces`. **Updated finding (T2 plan drafting):** the anchor run's empty `manifest.missing_pieces` is *not* "validator unwired"; it is "validator silent" — a single broad `try/except` swallows every exception class with `logger.debug` only. Additionally, the artifact listing happens *before* the parent-bundle uploads (`harness.py:23271-23295`), so 4 self-write paths (manifest, run_summary, artifact_index, operator_transcript) get false-flagged and the entire validator block likely raised inside the listing path on the anchor run. T2 fix is typed observability + narrow handlers + `self_write_paths` exclusion, not fresh wiring.
- `bundle_artifact_paths` declares 9 parent-level + ~8-per-iteration paths under `gso_postmortem_bundle/`. **Updated finding (T3 plan drafting):** only 4 of the 9 parent-level paths are uploaded today (`manifest`, `artifact_index`, `run_summary`, `operator_transcript`); the other 5 (`decision_trace_all`, `journey_validation_all`, `replay_fixture`, `scoreboard`, `failure_buckets`) have *no producer at all*. Per-iteration paths are written under legacy `phase_a/`/`phase_b/` prefixes (e.g. `phase_b/decision_trace/iter_N.json`), not the contract-declared `gso_postmortem_bundle/iterations/iter_NN/...` layout. The anchor run's "127/163 missing" splits into Layer A (5 missing producers, fixed by T3) and Layer B (per-iteration prefix mismatch, scoped as T5).
- `mlflow_audit.audit_parent_bundle` exists in `tools/mlflow_audit.py:264-315` but only checks `manifest.json` exists (not the full 9-path parent set). T3 extends it.
- `operator_process_transcript.render_process_transcript` exists; per-iteration call-site location vs `_finalize_iteration_summary` is unverified (T4).
- `dbutils.notebook.exit(...)` JSON contents (LLM contract pointers) on the anchor run is unverified (T4).

**What changes.**
- **C12-T1.** Extend `run_manifest_marker` payload with `wheel_sha`, `git_sha`, `effective_flags` (snapshot of every `*_enabled()` accessor in `common/config.py` via introspection at runtime), `python_version`, `domain`. Emit at run start and run end. New marker version: `GSO_RUN_MANIFEST_V2` (preserves V1 compatibility for the parser). **Drafted:** [`2026-05-08-cycle-12-t1-run-manifest-v2-plan.md`](./2026-05-08-cycle-12-t1-run-manifest-v2-plan.md).
- **C12-T2.** Replace the broad `try/except` in the harness validator block with three narrow handlers (flag check / MLflow listing / validator call), each with its own typed status. Add `self_write_paths` parameter to `validate_phase_h_manifest_paths` so the four assembler-imminent paths are not false-flagged. Emit a new `GSO_PHASE_H_STRICT_VALIDATION_V1` marker recording `flag_enabled`, `declared_count`, `materialized_count`, `self_write_count`, `missing_count`, `listing_status`, `validator_status`, `exception_class`. Refactor inline block into a pure helper `_run_phase_h_strict_validation` so the five exit paths (ok / listing-failed / validator-failed / flag-off / no-anchor) are testable without booting a lever loop. **Drafted:** [`2026-05-08-cycle-12-t2-phase-h-validator-wiring-plan.md`](./2026-05-08-cycle-12-t2-phase-h-validator-wiring-plan.md).
- **C12-T3.** Wire 5 missing parent-level producers via new pure builders in `optimization/run_output_bundle.py` (`build_decision_trace_all`, `build_journey_validation_all`, `build_scoreboard`, `build_failure_buckets`, `aggregate_per_iteration_artifacts`). Add 5 corresponding `_client_phase_h.log_text(...)` calls inside the existing parent-bundle upload block. Add `assembler_completeness_check(declared_paths, materialized_paths)` pure helper that runs *post-upload* (not pre-upload as originally drafted — pre-upload always shows self-writes as missing) and categorizes any gap as `parent_level_missing` (assembler bug) vs `unmigrated_per_iteration_missing` (Layer B, T5 scope). Emit `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` with the categorized gap when non-empty. Extend `mlflow_audit.audit_parent_bundle` from "manifest.json present" to "all 9 parent-level paths present" with a new `missing_parent_paths` field (preserves `has_manifest` for back-compat). **Drafted:** [`2026-05-08-cycle-12-t3-bundle-assembler-fix-plan.md`](./2026-05-08-cycle-12-t3-bundle-assembler-fix-plan.md).
- **C12-T4.** Closeout audit checklist (one short plan): on a fresh run after T1-T3 ship, verify (a) `audit_parent_bundle.missing_parent_paths == ()`, (b) every iteration's `operator_transcript.md` is present in the bundle (means it rendered every iteration, including iterations that hit producer exceptions — must live in `_finalize_iteration_summary`'s finally arm if not already), (c) `dbutils.notebook.exit(...)` JSON carries the full LLM-contract pointer set per contract Acceptance Criteria. Each "no" outcome becomes a dated follow-up task on the iteration ledger.
- **C12-T5 *(new — surfaced by T3's completeness check).*** Migrate per-iteration path producers from their legacy `phase_a/journey_validation/iter_N.json` and `phase_b/{decision_trace,operator_transcript}/iter_N.json` locations to the contract-declared `gso_postmortem_bundle/iterations/iter_NN/{journey_validation,decision_trace,operator_transcript,summary,rca_ledger,proposal_inventory,patch_survival}.json` layout. Two implementation options: (5a) write to the contract path *in addition to* the legacy path (dual-write, larger artifact volume but back-compat preserved); (5b) write to the contract path *instead of* the legacy path and add a one-shot tool that retro-renames legacy artifacts on existing runs. Prefer 5a behind a default-on flag; revisit 5b after one corpus measurement. The completeness-check marker from T3 makes the gap measurable, so T5 ships when the volume justifies the migration cost. T5 is sequenced after T1-T4 because T4's audit defines whether the per-iteration path layer needs to ship inside Cycle 12 or can spill into a successor cycle; if T4 returns green on (a)-(c) and the only remaining gap is the per-iteration backlog, T5 ships as a Cycle-12 close-out; otherwise it spills.

**Binary success criteria.**
- **T1.** Every postmortem can answer "what wheel/git_sha/flags/python ran?" by reading exactly one `GSO_RUN_MANIFEST_V2` record. Verified by replay-fixture parser test on a clean run.
- **T2.** Every run emits exactly one `GSO_PHASE_H_STRICT_VALIDATION_V1` marker, regardless of validator outcome. The five exit paths (ok / listing-failed / validator-failed / flag-off / no-anchor) are distinguishable by reading `MarkerLog.phase_h_strict_validation`. On the anchor run replayed against the new code, the marker carries either `validator_status=ok` with `manifest.missing_pieces` matching the path-gap count, or `validator_status≠ok` with the named exception class — never silent.
- **T3.** For a fresh run after T3 ships: `audit_parent_bundle.missing_parent_paths == ()` (all 9 parent-level paths materialized) AND either no `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` marker emits OR the marker carries only `unmigrated_per_iteration_missing` (Layer B, T5 scope) — never `parent_level_missing`. Verified by integration test.
- **T4.** Closeout audit returns green on all three "assumed shipped" items, or each gap produces a typed follow-up task on the iteration ledger.
- **T5.** For a fresh run after T5 ships: `assembler_completeness_check.complete == True` (zero `unmigrated_per_iteration_missing`); the legacy `phase_a/`/`phase_b/` artifacts continue to materialize during the dual-write window (5a). Verified by integration test on a 3-iteration corpus run.

**Dependencies.** None.

**Sizing.** Five plans, dependency-ordered.

| Plan | Status | Tasks | Working days |
|---|---|---|---|
| C12-T1 | drafted | 10 | ~2 |
| C12-T2 | drafted | 6 | ~2 |
| C12-T3 | drafted | 11 | ~3-4 |
| C12-T4 | pending | ~3-4 | ~1 |
| C12-T5 | scoped, not yet drafted | TBD (~10) | ~3-4 |
| **Total** | | | **~11-13** |

T1, T2, T3 are independent and can ship in any order (or in parallel). T4 depends on T1+T2+T3 having shipped (audit consumes their typed markers). T5 depends on T4 having returned green on the parent-level closeout (so we know per-iteration is the only remaining layer).

**Flag(s).**
- `GSO_RUN_MANIFEST_V2` (default-on; V1 emit preserved for back-compat) — T1.
- `GSO_PHASE_H_STRICT_VALIDATION_V1` and `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` — T2/T3 emit-only markers, no behavior flags. Add `MarkerLog.phase_h_strict_validation` and `MarkerLog.bundle_assembly_incomplete` parser surfaces.
- T4 introduces no flags.
- T5 ships `GSO_PER_ITERATION_BUNDLE_PATHS_DUAL_WRITE` (default-off → on after corpus measurement; flag-off path retains legacy-only writes for replay byte-stability).

**Risks.**
- `effective_flags` introspection might serialize many flags, bloating stdout. Mitigation: enumerate only `*_enabled()` accessors via `inspect.getmembers(config, callable)`; T1's plan emits all flags without truncation but adds a defensive size test that fires if the payload exceeds an envelope (current envelope 4KB).
- `wheel_sha` requires reading the deployed wheel manifest; in dev mode we may need a fallback (e.g. `git_sha` only). T1's `read_wheel_sha` returns empty on failure rather than raising.
- ~~T3's assembler fix may surface additional shipped-but-unwired contract paths~~ **(confirmed during plan drafting):** five parent-level paths had no producer (`decision_trace_all`, `journey_validation_all`, `replay_fixture`, `scoreboard`, `failure_buckets`). T3 ships minimal-but-honest builders for all five rather than removing them from the contract. The richer schemas (e.g. full `LoopSnapshot`-based scoreboard) become Cycle-18+ scope if a postmortem skill needs richer data.
- **List-valued stage capture `AttributeError` (surfaced by airline anchor #4, postmortem 2026-05-09 #2).** The legacy assembler emitted `GSO_BUNDLE_ASSEMBLY_FAILED_V1` with `AttributeError: 'list' object has no attribute 'get'` — an internal stage capture is a list in some code paths and a dict in others, and the assembler calls `.get()` unconditionally. **T3's plan must normalize list-valued stage captures before `.get()` access** (e.g. `_normalize_stage_capture(c)` returns the first dict element of a list-typed capture, or an empty dict, never the raw list). Add a unit test fixture exercising the list path. Without this fix, T3's new producers (`build_decision_trace_all`, etc.) inherit the same crash mode the moment any upstream stage capture changes shape.
- **Manifest/index path inconsistency (surfaced by airline anchor #4).** On run 294, the bundle materialized 91 files and `artifact_index.json` resolved every parent-level path, but `manifest.json` still listed 13 missing pieces. The validator and the index resolver use different path-construction rules. **T3's plan must require the manifest validator to consume the same path set `artifact_index.json` writes, not a parallel re-derivation.** Concretely: the manifest writer and the index writer should share one pure helper `_canonical_bundle_paths(declared, materialized) -> {present, missing}`, and both surfaces render from its output. Without this, T3's `audit_parent_bundle.missing_parent_paths` and `manifest.missing_pieces` will continue to disagree on the same run, undermining the binary success criterion.
- ~~T4 closeout audit may surface a fourth gap class~~ **(confirmed during T3 drafting):** the per-iteration path layer is the surfaced gap. Now scoped as T5; if T4 surfaces a *fifth* gap class, that becomes a follow-up cycle.
- T5's dual-write window doubles per-iteration artifact volume during the rollout. Mitigation: corpus-measure on a 3-iteration run before defaulting on; if volume is acceptable, retain legacy writes for one cycle and remove in the cycle after (giving postmortem skills time to migrate readers).
- T5 may surface that some "per-iteration path" producers are themselves missing (not a prefix issue but no producer at all). Mitigation: T5 first runs a scoped audit (analogous to T2's typed observability) to enumerate which iteration-level paths have producers under any prefix, then migrates only the producible ones; missing producers go to a successor cycle.

---

## Cycle 14-V — Shipped-cycle defect sweep (observability restoration + canonical-render fix)

**Inspiration runs.** Anchor #5 (7Now task `338386531912450`, attempt 10) and Anchor #6 (airline task `833709971504406`, attempt 12) — the post-redeploy lever-loop pilots that demonstrated C13, C14-T0, C14-T1+T2, and C14B code is shipped and partially working in production, but exhibit five "shipped-but-silent" defects (registered as D-1 → D-5 above).

**What this cycle closes.** Cycle 14-V is the **Cycle 6 precedent applied to the post-2026-05-09 cycles**: a targeted defect sweep that makes shipped-cycle code corpus-measurable without flipping behavior-flag defaults. Five surgical fixes:

1. **D-1.** Shadow-mode observability for C13's forbidden-AG admission predicate (`GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` typed marker). Emits on every NO_ACTION reflection processed by `_compute_forbidden_ag_set`, regardless of `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` value. Postmortems can now prove the admission predicate's behavior on the current corpus before the behavior-flag default flips.
2. **D-2.** Shadow-mode observability for C14B-T3's diagnostic-only orchestrator (`GSO_PATCH_ISOLATION_OBSERVE_V1` typed marker). Emits on every acceptance decision whose `reason_code` is in the canonical isolation-eligible set, regardless of `GSO_PATCH_SUBSET_ISOLATION` value.
3. **D-3.** Single-source canonical render — `format_full_eval_marker_payload` now derives `target_fixed_qids` and `target_still_hard_qids` from `target_delta_states` (C14-T0's total function) when populated, eliminating the same-payload contradiction surfaced by both anchors. Pre-T0 fixtures fall through to legacy fields verbatim (back-compat).
4. **D-4.** `_normalize_stage_capture(value)` safely converts list-valued stage captures to dict before any `.get()` access in the bundle assembler. Eliminates the `AttributeError: 'list' object has no attribute 'get'` surfaced on every airline run.
5. **D-5.** Run-manifest `databricks_*` ID population from environment + dbutils tag-resolver, with the literal sentinel `'unknown'` falling through (NEVER blank). Resolves Open Q#10.

Plus three loud-failure regression-rail markers (`GSO_FORBIDDEN_AG_ADMISSION_BYPASSED_V1`, `GSO_PATCH_ISOLATION_TRIGGER_NOT_ENGAGED_V1`, `GSO_CANONICAL_RENDER_INVARIANT_V1`) that stay silent on clean runs and emit only when a future change reintroduces the closed defect — these are the canonical regression-prevention rails.

**Stage(s) closed.** None (no new compliance — defect sweep only). However, Cycle 14-V *unblocks* the corpus-measurement evidence that several future cycles depend on:

- C13 → corpus measurement of admission impact (was blocked by D-1).
- C14B-T3 LIVE-arm flip → corpus measurement of attribution accuracy (was blocked by D-2).
- C14-T3 (I9 byte-equality) → render fields self-consistent before invariant fires (was blocked by D-3).
- C12-T4 closeout audit → bundle assembly + manifest stable (was blocked by D-4 + D-5).

**Current state (audited).** Confirmed by reading the live code:

- `_reflection_admitted_to_forbidden_set` (`harness.py:9785-9841`) and `_compute_forbidden_ag_set` (`harness.py:9844-9889`) are correctly implemented per C13 plan; the flag is read at line 9869.
- `_maybe_run_patch_isolation_orchestrator` (`harness.py:2000-2100+`) is correctly implemented per C14B-T3 plan; the flag is read at line 2023.
- `format_full_eval_marker_payload` (`control_plane.py:851-915`) emits both `target_delta_states` and legacy bucket fields as parallel derivations.
- `run_output_bundle.py` calls `.get(...)` on stage-capture values without list normalization.
- The run-manifest emission site (around `harness.py:22985`) does not read `DATABRICKS_*` environment variables.

**What changes.** Six surgical tasks (T0 setup, T1-T6 implementation, T7 integration test, T8 self-review). Plan ref: [`2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md`](./2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md).

**Binary success criteria.**
- **T1.** `_compute_forbidden_ag_set` emits `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` for every NO_ACTION reflection on the canonical 7Now anchor; the `forbidden` set returned is byte-identical to pre-14-V code under `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0`.
- **T2.** `_maybe_run_patch_isolation_orchestrator` emits `GSO_PATCH_ISOLATION_OBSERVE_V1` for both AG3 iter 3+4 on the canonical airline anchor; the existing diagnostic + outcome markers stay gated on `patch_subset_isolation_enabled()`.
- **T3.** Replay of either anchor's fixture under Cycle 14-V code produces a `format_full_eval_marker_payload` output where no QID appears in two contradicting fields (e.g., simultaneously in `target_delta_states=soft_to_hard` AND `target_still_hard_qids`).
- **T4.** All three regression-rail markers stay silent on both anchors; corpus pilot post-Cycle 14-V emits zero rail markers.
- **T5.** Bundle assembly does not raise `AttributeError` on either anchor; `GSO_BUNDLE_ASSEMBLY_FAILED_V1` is silent.
- **T6.** `GSO_RUN_MANIFEST_V2` emits with all three `databricks_*` fields populated (either resolved IDs or the literal sentinel `'unknown'`); never blank.

**Dependencies.** None. Cycle 14-V can ship any time after C12-T1, C13, C14-T0, C14-T1+T2, C14B-T1+T2, and C14B-T3 (all already merged). Sequencing-wise, 14-V ships **before** the next round of plan drafting (C14-T3, C14-T4, C16-T3) so the post-14-V corpus pilot produces clean evidence to drive those plans.

**Sizing.** One plan, 1.5-2 working days for one engineer; ~1 day with 3 parallel sub-agents (T1/T2/T3 are independent; T5/T6 are independent; T7/T8 sequential).

**Flag(s).** Three observability-only flags, all default-on; no behavior change when off. Replay byte-stable with all flags off (preserves the warn-only-pilot discipline for C13/C14B/C16).

| Flag | Default | Surface |
|---|---|---|
| `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE` | on | shadow emission inside `_compute_forbidden_ag_set` |
| `GSO_PATCH_ISOLATION_OBSERVE` | on | shadow emission inside `_maybe_run_patch_isolation_orchestrator` |
| `GSO_CANONICAL_RENDER_INVARIANT` | on | self-check inside `format_full_eval_marker_payload` |

**Risks.**
- Shadow-mode emission could create stdout volume on the canonical NO_ACTION + isolation reason-code corpus paths. Mitigation: each marker is one line per emission; on a 5-iteration run with 4 NO_ACTION reflections + 2 isolation triggers, total volume is 6 marker lines — negligible.
- The single-source render fix (D-3) might surface a *new* same-QID contradiction class not enumerated in T3. Mitigation: T4's `GSO_CANONICAL_RENDER_INVARIANT_V1` enumerates three violation classes (`fixed_and_still_hard_overlap`, `target_in_out_of_target_set`, `delta_state_disagrees_with_bucket`); if a fourth class surfaces, register it as D-N+1 and ship a follow-up.

**Downstream coordination points.**
- **C14-T3** (I9 byte-equality invariant) reads the post-14-V canonical render; without 14-V T3, I9 would invariant-check a contradicting render.
- **C14B-T3 LIVE-arm flip** reads the post-14-V observe markers as the corpus-measurement input that justifies flipping `GSO_PATCH_SUBSET_ISOLATION` default to on.
- **C13 default flip** reads the post-14-V observe markers as the corpus-measurement input that justifies flipping `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` default to on.
- **C16-T3** (regression bucket completeness) reads the post-14-V D-3 fix as a cleaner upstream — the `unknown_to_hard_regressed_qids` bucket no longer leaks target QIDs, so C16-T3's `existing_hard_still_hard_outside_target` extension lands on a contradiction-free substrate.
- **C16-T4** (contract-health summary) reads the new T4 regression-rail markers as the canonical "shipped-cycle defect emerged" signal in the HIGH severity tier.

---

## Cycle 14-W — Post-Cycle-14-V defect sweep #2 + C13 default-flip promotion (recursive Cycle 6 / Cycle 14-V precedent)

**Inspiration run.** Anchors #7 (7Now run `960148942255012`, attempt 11) + #8 (airline run `1105451933925748`, attempt 13) — first post-Cycle-14-V corpus pilot. Plan ref: [`2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md`](./2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md). Also see Revision (2026-05-09 #4) above for the C14-V scoreboard.

**What this cycle closes.** Cycle 14-W applies the **Cycle 6 / Cycle 14-V precedent recursively**: the post-Cycle-14-V corpus pilot validated D-1 (C13 admission shadow marker fires correctly across the corpus, ready for default-flip), but it ALSO surfaced that two C14-V-registered defects regressed in production (D-4 + D-5), one is partial (D-3), and three new defects (D-6, D-7, D-8) emerged from the same evidence. Six surgical defect closures plus one corpus-validated default-flip:

1. **D-3 extension (T1)** — render `target_soft_passing_qids` as a first-class bucket field derived from `target_delta_states` when state is `SOFT_PASSING`. Closes the 7Now F2 contradiction (gs_026 in `target_delta_states` but absent from every bucket).
2. **D-4 production-shape fix (T2)** — audit every `.get()` consumer of stage-capture state, route through `_normalize_stage_capture`; ship an airline-fixture-replay integration test (Discipline A).
3. **D-5 production-shape fix (T3)** — instrument `_databricks_ids_from_env` with a `GSO_DATABRICKS_IDS_RESOLVED_V1` tracing marker; ship a Jobs-runtime integration test exercising the dbutils tag-resolver path (Discipline A + B).
4. **D-1 corpus-validated default-flip (T4)** — promote `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` from default-off to default-on; regression-rail invariant `GSO_FORBIDDEN_AG_ADMISSION_BYPASSED_V1` must stay silent post-flip.
5. **D-7 closure (T5)** — emit exactly one `GSO_ITERATION_SUMMARY_V1` per attempted iteration regardless of acceptance outcome; new invariant `GSO_ITERATION_SUMMARY_TOTALITY_V1` fires if cardinality drifts.
6. **D-6 + D-8 closure (T6)** — migrate Phase H acceptance writer + journey-validator writer to consume canonical `ControlPlaneAcceptance` / journey-log objects; new alarm markers `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1` + `GSO_PHASE_H_JOURNEY_DRIFT_V1`.

**Stage(s) closed.** None (no new compliance — defect sweep + default-flip). Cycle 14-W *unblocks* the next sequenced cycles:
- C14-T3 (acceptance-trace replay validity) depends on Phase H writer consuming canonical decisions (T6).
- C14-C (first-class attribution-drift partial harvest, queued next) depends on canonical render extending to `target_soft_passing_qids` (T1).
- C16-T3 (regression bucket completeness) reads the cleaner post-T1 `target_soft_passing_qids` as a substrate.

**Current state (audited).**
- D-3 ext: `format_full_eval_marker_payload` (control_plane.py:887-921) derives `FIXED`/`STILL_HARD` from `target_delta_states`; `SOFT_PASSING` is not represented in any bucket field. `ControlPlaneAcceptance` dataclass has no `target_soft_passing_qids` slot.
- D-4: `_normalize_stage_capture` exists at `run_output_bundle.py:24-49` but airline anchor 13 F7 still raises `AttributeError`. Call-site coverage gap.
- D-5: `_databricks_ids_from_env` exists at `harness.py:215-274` with env-var → dbutils tag-resolver → sentinel chain. Both anchors return blank rather than sentinel — production code path returns blank from a different code path OR dbutils tags differ. Resolution path not traced.
- D-1: `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` default-off in `config.py:5480-5497`. C14-V T1 shadow marker corpus-validated on 7Now anchor 11 F5 (5/5 NO_ACTION reflections; rail silent).
- D-7: `GSO_ITERATION_SUMMARY_V1` emitted only on accepted-iteration code path. Phase B `iter_record_counts` cardinality drifts from `iteration_counter`.
- D-6 + D-8: Phase H acceptance writer and journey-validator writer both have parallel-derivation paths from runtime state. Local replay validator and Phase H journey validator do not share a `validate_journey()` implementation.

**What changes.** Six surgical tasks (T1-T6 substantive; T0 setup, T7 integration test, T8 self-review). Plan ref: [`2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md`](./2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md).

**Binary success criteria.**
- **T1.** Replay of either anchor under 14-W code emits `target_soft_passing_qids` populated when `target_delta_states` contains a `SOFT_PASSING` entry; no QID appears in two of `{fixed, still_hard, soft_passing}`.
- **T2.** `tests/integration/test_bundle_assembler_airline_fixture_replay` is green; replay of airline anchor 13 fixture emits zero `GSO_BUNDLE_ASSEMBLY_FAILED_V1` markers.
- **T3.** `tests/integration/test_databricks_ids_in_jobs_runtime` is green; replay with mocked dbutils tags populates manifest IDs; `GSO_DATABRICKS_IDS_RESOLVED_V1` records the correct `resolution_path` field.
- **T4.** With default-on flag, `GSO_FORBIDDEN_AG_ADMISSION_BYPASSED_V1` rail stays silent on both anchors; replay byte-stability preserved by explicit fixture flag-set.
- **T5.** Replay emits exactly one `GSO_ITERATION_SUMMARY_V1` per attempted iteration; `phase_b_iter_record_counts` length equals `iteration_counter`; `GSO_ITERATION_SUMMARY_TOTALITY_V1` rail stays silent.
- **T6.** Phase H acceptance writer JSON outputs match canonical `GSO_FULL_EVAL_V1` payloads byte-for-byte on iter-decision fields; local-replay journey violation count equals Phase H `journey_validation_all.json` violation count; `GSO_PHASE_H_*_DRIFT_V1` rails stay silent.

**Dependencies.** Cycle 14-V (all six tasks). Cycle 14-W can ship any time after 14-V code is deployed and the corpus pilot has produced anchors #7 + #8 evidence (already done).

**Sizing.** One plan, 2-3 working days for one engineer; ~1.5 days with 3 parallel sub-agents (T1/T5/T6 each touch independent code paths; T2/T3 share `harness.py` so sequential).

**Flag(s).** Three new observability-only flags (default-on) plus one behavior-flag default-flip:

| Flag | Default | Surface |
|---|---|---|
| `GSO_DATABRICKS_IDS_RESOLUTION_TRACE` | on | resolution-path trace inside `_databricks_ids_from_env` |
| `GSO_ITERATION_SUMMARY_TOTALITY` | on | invariant alarm at finalize-stage |
| `GSO_PHASE_H_CANONICAL_CONSUMER` | on | Phase H writers consume canonical decision/state |
| `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` | **flipped on** (was off) | C13 admission predicate behavior — corpus-validated by C14-V T1 |

**Risks.**
- T4's default-flip changes behavior on production runs. Mitigation: replay fixture is explicitly set to the legacy off value (one-time fixture migration); regression rail validates corpus silence post-flip.
- T2's call-site audit might miss a `.get()` site that *does* sometimes consume list-shaped values but with dict-shaped values in the test corpus. Mitigation: AST-walk-based static-analysis test (`test_bundle_assembler_call_site_coverage`) catches every `<*stage*>.get(...)` and `<*capture*>.get(...)` regardless of fixture coverage.
- T6 might surface a third class of Phase H writer drift not enumerated. Mitigation: `GSO_PHASE_H_*_DRIFT_V1` alarm enumerates the writer + the field that drifted; if a fourth writer is found, register as D-9+ and ship Cycle 14-X.

**Downstream coordination points.**
- **C14-T3** (acceptance-trace replay validity) reads the post-14-W canonical Phase H acceptance writer.
- **C14-C** (first-class attribution-drift partial harvest, next plan) reads the post-14-W `target_soft_passing_qids` field as substrate for proper attribution accounting.
- **C16-T3** (regression bucket completeness) reads the post-14-W cleaner `target_soft_passing_qids` derivation.
- **C16-T4** (contract-health summary) reads `GSO_DATABRICKS_IDS_RESOLVED_V1` + `GSO_PHASE_H_*_DRIFT_V1` + `GSO_ITERATION_SUMMARY_TOTALITY_V1` as canonical contract-health signals.

---

## Cycle 14-C — First-class attribution-drift partial harvest (queued — drafts after 14-W ships)

**Inspiration run.** Anchor #8 (airline run `1105451933925748`, attempt 13) — first in-production demonstration of `accepted_with_attribution_drift` keep-the-win acceptance. AG_DECOMPOSED_H004 improved aggregate `83.3% → 95.8%` (`+12.5pp`, thresholds met) even though `target_qids=gs_024` remained `still_hard`. Existing code path: `control_plane.py:1118, 1290-1297` — emergent behavior, no plan.

**What this cycle closes.** Reattribute accepted gains to the QIDs/clusters that actually improved (rather than to the still-hard target), record the still-hard `target_qids` as **unresolved-target-debt** (new acceptance-decision sub-record sibling of `accepted_with_partial_harvest_debt`), and emit a typed marker with both attribution and debt for postmortem traceability.

**Stage(s) closed.** Stage 9 (`acceptance_decision`) — extended behavior; new debt-classification sub-record. Stage 10 (`learning_next_action`) — debt-aware learning input.

**Current state (audited).**
- `accepted_with_attribution_drift` reason code exists at `control_plane.py:1118`; fires when `thresholds_met=True` + zero collateral regressions + no causal target fix. Anchor #8 confirms it works in production.
- Attribution is recorded against still-hard `target_qids` rather than reattributed to actually-improved QIDs.
- `partial_harvest_with_debt=false` and `patch_subset_isolation=false` in anchor #8 manifest — disjoint from C14B path.
- No regression rail asserts attribution never points at a still-hard QID.

**What changes (provisional — finalized post-14-W; deferred until 14-W lands).**

1. **T1: Reattribute accepted gain.** Compute `improved_qids = baseline_failed ∩ candidate_passing` and assign acceptance to those QIDs/clusters; record original `target_qids` separately as unresolved debt.
2. **T2: New `unresolved_target_debt` sub-record.** Sibling of `regression_debt_qids`; tracks targets that didn't move on an accepted iteration.
3. **T3: Typed marker `GSO_ATTRIBUTION_DRIFT_V1`.** Records `improved_qids`, `improved_clusters`, `unresolved_target_qids`, `pre_arbiter_delta_pp`, `post_arbiter_delta_pp`.
4. **T4: Regression rail `GSO_ATTRIBUTION_POINTS_AT_HARD_V1`.** Fires if attribution lists a QID still in `target_still_hard_qids` or `target_soft_passing_qids` (which depend on 14-W T1 deriving them).

**Binary success criteria.**
- **T1.** Replay of airline anchor 13 emits attribution pointing at `gs_013` / `gs_024`-cluster QIDs that actually improved, NOT at still-hard `gs_024`.
- **T2.** Acceptance decision JSON has both `attribution_qids` and `unresolved_target_debt` populated.
- **T3.** `GSO_ATTRIBUTION_DRIFT_V1` emitted on every `accepted_with_attribution_drift` acceptance.
- **T4.** `GSO_ATTRIBUTION_POINTS_AT_HARD_V1` rail silent on every clean attribution.

**Dependencies.** Cycle 14-W (all six tasks). Cycle 14-W T1 introduces `target_soft_passing_qids`; the attribution-points-at-hard rail (T4) requires this field to derive correctly.

**Sizing.** One plan, 3-4 working days for one engineer; depends on 14-W T1's render extension landing first.

**Flag(s).** One new behavior flag `GSO_ATTRIBUTION_DRIFT_FIRST_CLASS` (default-off pilot per warn-only-pilot discipline; promoted to default-on after corpus measurement following Discipline A integration test). Plus one observability flag `GSO_ATTRIBUTION_DRIFT_OBSERVE` (default-on per Open Q#12 promotion).

**Risks.** Reattribution might surface QIDs that were soft-improving (not flipped from hard to passing) but contributed to the aggregate gain. Mitigation: `improved_qids` derivation uses canonical `pre_rows`/`post_rows` state; soft-only contributors register as `soft_improvement` in attribution rather than `hard_fix`.

---

## Cycle 13 — Close arrow `Stage 10 → Stage 4`

**Inspiration run.** `3b050ec5-...` postmortem F6 (I4 fired ×4 across iters 2-5 with `Proposals (0 total)`; AG1 re-emitted four times despite zero output the prior iteration).

**What this cycle closes.** The strategist's input now reflects what its prior outputs proved unworkable. This is the single highest-leverage move in the roadmap: every other process improvement depends on the loop being able to learn from itself.

**Stage(s) closed.**
- Stage 5 (`proposal_generation`) — letter and spirit.
- Stage 10 (`learning_next_action`) — letter and spirit.
- Stage 4 (`action_group_selection`) — letter and spirit (closes arrow `10 → 4`).

**Current state (audited).**
- `_build_reflection_entry` (`harness.py:8818`) builds reflection entries with `lever_set` derived from the `levers` parameter.
- The `no_proposals` call site (`harness.py:17974-17982`) explicitly passes `levers=[]`. The reflection entry therefore stores empty `lever_set`.
- `classify_rollback_reason` (`rollback_class.py:106-107`) maps `"no_proposals"` to `RollbackClass.OTHER`.
- `_compute_forbidden_ag_set` (`harness.py:9459`) admits only `CONTENT_REGRESSION` *and* requires non-empty `lever_set`. Both filters drop `no_proposals` reflections.
- Result: AG re-emission is unconstrained after a no-op iteration.

**What changes.**
- **C13-T1.** Add `NO_ACTION` value to `RollbackClass` enum and update `classify_rollback_reason` so `"no_proposals"` and `"ag_collision_with_forbidden_set"` map to `NO_ACTION` (was `OTHER`).
- **C13-T2.** Update the `no_proposals` reflection call site (`harness.py:17974`) to pass `levers=lever_keys` (the AG's intended lever set) instead of `levers=[]`. Same fix at the `ag_collision_with_forbidden_set` call site (`harness.py:16640`).
- **C13-T3.** Extend `_compute_forbidden_ag_set` to admit `NO_ACTION` reflections in addition to `CONTENT_REGRESSION`, retaining the non-empty-`lever_set` requirement.
- **C13-T4.** Emit a typed `proposal_generated` decision record with `outcome=DROPPED` and `reason_code=NO_PROPOSALS_GENERATED` at the empty-proposal call site so Stage 5 has a record (currently it has only the reflection entry).

**Binary success criteria.**
- I4 (`consecutive_empty_proposals_same_ag`) cannot fire twice in one run by construction. New structural unit test: `test_i4_cannot_fire_twice_in_one_run_after_cycle_13`.
- For every iteration where proposals=0, exactly one `proposal_generated[NO_PROPOSALS_GENERATED]` record exists in the iteration trace.
- The next iteration's `_compute_forbidden_ag_set` returns a set containing the prior iteration's AG signature.

**Dependencies.** Cycle 12-T1 specifically (`effective_flags` traceability — so the postmortem can confirm `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` was on for the run we're claiming closed I4). C12-T2 / T3 / T4 / T5 are *not* on the critical path; this cycle can ship as soon as T1 lands, in parallel with the rest of C12.

**Sizing.** One plan, ~2 days.

**Flag(s).** `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` (default-off → on after one pilot). Replay byte-stability requires the flag-off path to keep current behavior.

**Risks.**
- The strategist may run out of eligible AGs entirely after one iteration. Mitigation: if every AG is forbidden, terminate with reason `no_eligible_ag_after_forbidden` instead of looping. This is the correct contract behavior; it surfaces the lack of viable strategy honestly.
- Existing tests that depend on AG-re-emission behavior may break. Mitigation: TDD; flag-off path retained.

**Downstream coordination points (consumed by later cycles).**
- *C14-T2 (canonical render).* `RollbackClass.NO_ACTION` must be rendered by C14-T2's `format_full_eval_marker_payload`. C13 ships first; C14-T2's helper test suite includes a `NO_ACTION` fixture. If C14 lands the helper before C13 ships `NO_ACTION`, the helper's enum dispatch must default to `OTHER` for unknown values so C13's later addition is byte-stable.
- *C16-T2 (structural causal dropped).* C13's `_compute_forbidden_ag_set` is extended again by C16-T2 to admit `structural_causal_dropped` reflections. C13-T3's admission predicate should be a single function `_reflection_admitted_to_forbidden_set(entry) -> bool` that C16-T2 extends with one new clause (not a re-implementation). C13-T3's plan should explicitly leave this hook in place.
- *C16-T2 (reflection-entry shape).* For C16-T2's propagation to actually fire, its reflection entries must carry `levers=ag.lever_set` and a `RollbackClass` value that the predicate above admits. C16-T2's plan should mirror C13-T2's call-site fix verbatim.

---

## Cycle 14 — Single source of truth for decisions

**Inspiration runs.** Original anchor `534010336956422` postmortem F4 (stdout vs replay disagreement on rollback reason), F8 (split-L5 patch ID non-injectivity), F9 (`phase_b.total_records=0` while replay records exist). **New anchor `76457773587391` postmortem F2** (target QID `gs_026` recorded as neither `target_fixed` nor `target_still_hard` — impossible state, proving per-QID delta computation can return `unknown` for evaluated targets).

**What this cycle closes.** Four independent data-quality gaps that share one root cause: multiple producers writing the same conceptual record into different code paths, plus one producer (per-QID delta computation) returning structurally illegal output. After this cycle, every contract record has exactly one canonical producer AND that producer cannot return `unknown` state for evaluated QIDs.

**Stage(s) closed.**
- Stage 1 (`evaluation_state`) — letter and spirit (Phase B aggregator authoritative).
- Stage 7 (`applied_patches`) — letter and spirit (`expanded_patch_id` everywhere).
- Stage 8 (`post_patch_evaluation`) — *delta-computation correctness sub-criterion* (T0; per-QID delta state is total over evaluated targets, never `unknown`). Stage 8's bucket-taxonomy completeness remains Cycle 16's scope.
- Stage 9 (`acceptance_decision`) — letter and spirit (one canonical render, fed by correct deltas). Acceptance-policy completeness (partial-harvest with debt) is sibling Cycle 14B's scope.

**Current state (audited).**
- **Per-QID delta computation (new finding from new anchor F2).** `control_plane.compute_acceptance_buckets` and `state.update_target_progress` consume baseline-rows and candidate-rows independently. When a target QID is missing from one set (e.g. baseline lookup hits the wrong row index, or the candidate's failed-question list excludes a now-passing target without a corresponding `target_fixed` record), the resulting `target_state` falls through every if-branch and is silently treated as `unknown`. The new anchor's `gs_026` lands in this hole: candidate failed-question list shows only `gs_018`, so `gs_026` should resolve to `fixed`, but the per-QID delta function never records it as such — yielding `target_fixed_qids=()` AND `target_still_hard_qids=()`.
- **Phase B aggregator.** `_phase_b_iter_record_counts` is appended inside the iteration body's happy path (`harness.py:22399`). Producer exceptions in earlier code paths skip the whole block. `_finalize_iteration_summary` is called from `try/finally` (Cycle 11 work) but the Phase B accounting is *not* inside `_finalize_iteration_summary` yet.
- **Acceptance disagreement.** `acceptance_policy.AcceptanceDecision` (delta-only) and `control_plane.ControlPlaneAcceptance` (full per-QID buckets) are populated independently. Stdout `GSO_FULL_EVAL_V1` and replay `acceptance_decided` render from each separately.
- **`expanded_patch_id`.** Used in `patch_selection.py` and `decision_emitters.py`. Cycle 6 F-4 fixed the canonical case for L1/L6. Split-L5 sections still leak bare `P001#1..#4` in caps and survival tables (old anchor F8).

**What changes.**
- **C14-T0 *(new — added 2026-05-09 from new anchor F2; prerequisite for T2 and Cycle 14B).*** Build a single pure helper `compute_target_delta_states(target_qids, baseline_rows, candidate_rows, candidate_failed_qids) -> dict[QID, DeltaState]` in `optimization/control_plane.py`. The function must be **total over `target_qids`**: every target QID lands in exactly one of `{fixed, still_hard, soft_to_hard, soft_passing, regressed_to_unknown, lookup_failed}`. The new `lookup_failed` bucket is the explicit "I could not resolve this target's state" answer — never `unknown`. Migrate `compute_acceptance_buckets` and `state.update_target_progress` to consume this helper. Add invariant **I13**: for every target QID, `delta_state ≠ unknown` at decision time AND `lookup_failed` count contributes to a typed `target_resolution_failed` rollback class rather than silently rendering as "not improved." Severity HIGH.
- **C14-T1.** Move Phase B per-iter accounting (the `_phase_b_iter_record_counts` / `_phase_b_iter_violation_counts` / `_phase_b_no_records_iterations` block) into `_finalize_iteration_summary` so producer exceptions cannot bypass it. Same emitter-resilience pattern as Cycle 11 Bug B fix.
- **C14-T2.** Promote `control_plane.ControlPlaneAcceptance` to the canonical acceptance object. Add a render helper `format_full_eval_marker_payload(decision)` that produces both the stdout `GSO_FULL_EVAL_V1` payload and the replay `acceptance_decided` record's contents from one source. Replace the two existing renderers with calls to this helper. **C14-T2 consumes T0's helper output** — `format_full_eval_marker_payload` reads `decision.target_delta_states` rather than recomputing.
- **C14-T3.** Add invariant **I9**: `GSO_FULL_EVAL_V1` and replay `acceptance_decided` must agree byte-for-byte on `reason_code`, `target_still_hard_qids`, `out_of_target_regressed_qids` after canonical normalization (sorted, lowercase). Add to invariant suite as HIGH severity. **Scope addendum (2026-05-09 #2; surfaced by airline anchor #4):** I9 must distinguish *stale-illegal* trunk transitions (the producer path C17 fixes) from *post-resolution-legitimate* terminal transitions on a successful run. Two concrete transitions surfaced on run 294 (100% accepted) — `clustered → already_passing` (e.g. `gs_007`: clustered as related-but-not-target, remained passing after the AG_DECOMPOSED_H004 fix) and `evaluated → post_eval` (e.g. `gs_016`: evaluated and entered post-eval terminal state after AG acceptance) — are **legitimate** state-machine transitions, not producer bugs. I9 must not trip on these. C14-T3's plan registers the legal-terminal transitions explicitly in I9's allow-list and cross-references C17-T1 (which performs the producer-side audit and ships the corresponding state-machine extensions); if C17-T1's classification produces additional legal terminals beyond these two, I9's allow-list extends in lockstep.
- **C14-T4.** Audit the ~5 remaining sites that emit bare `P001#N` for split L5 sections (`harness.py`, `patch_selection.py`, `applier.py`, `applier_audit.py`). Replace with `expanded_patch_id` resolution. Add invariant **I10**: every applied-patch record's `proposal_id` must resolve uniquely to a `proposal_generated` record's `proposal_id` in the same iteration.

**Binary success criteria.**
- **T0.** For every successful run, every target QID's `delta_state` is non-`unknown`. Verified by I13. The new anchor replayed against post-T0 code: `gs_026` resolves to `fixed` (candidate's failed-question set excludes it) and the resulting `target_fixed_qids=(gs_026,)`. No simultaneous-empty bug possible by construction.
- **T1.** For every successful run, `GSO_PHASE_B_END_V1.total_records == len(replay_fixture.decision_records)`. Add as I1 follow-up assertion.
- **T2.** `GSO_FULL_EVAL_V1` and replay `acceptance_decided` byte-equal on the three target fields. Verified by I9.
- **T4.** Every applied-patch ID resolves to exactly one proposal section. Verified by I10.

**Dependencies.** Cycle 12-T1 specifically (`effective_flags` traceability for the new render-helper flags). Cycle 13 (so Phase B records include the typed `proposal_generated[NO_PROPOSALS_GENERATED]` records this cycle aggregates). C12-T2 / T3 / T4 / T5 are *not* on the critical path. Cycle 14's T1 (Phase B accounting in `_finalize_iteration_summary`) does land in the same call site that C12-T4's audit verifies for per-iteration transcript firing — coordinate the two changes (sequential, not parallel) to avoid two simultaneous edits to `_finalize_iteration_summary`. **T0 is the prerequisite for T2 within this cycle and for Cycle 14B externally** (T2 reads T0's output; C14B's policy reads T0's `DeltaState` enum); T0 ships first.

**Sizing.** Five plans (T0 sequenced first; T1-T4 parallelizable after T0 lands).

| Plan | Status | Tasks | Working days |
|---|---|---|---|
| C14-T0 | drafted ([`2026-05-09-cycle-14-t0-target-delta-correctness-plan.md`](./2026-05-09-cycle-14-t0-target-delta-correctness-plan.md)) | 8 | ~1-2 |
| C14-T1 | scoped, not yet drafted | ~4 | ~1-2 |
| C14-T2 | scoped, not yet drafted | ~5 | ~2 |
| C14-T3 | closed-local pending corpus | ~3 | ~1 | (closed by [Cycle 15.1 — Compliance Ratchet](./2026-05-10-cycle-15-1-compliance-ratchet-plan.md))
| C14-T4 | closed-local pending corpus | ~5 | ~1-2 | (closed by [Cycle 15.1 — Compliance Ratchet](./2026-05-10-cycle-15-1-compliance-ratchet-plan.md))
| **Total** | | | **~6-9** |

**Flag(s).** `GSO_TARGET_DELTA_STRICT` (default-on; defensive, asserts `delta_state != unknown` at decision time; flag-off path is an explicit migration ramp removed in the next cycle). Other tasks ship byte-stable on existing default-on infrastructure.

**Risks.**
- **T0.** Migrating `state.update_target_progress` may surface call sites that consumed the legacy `unknown` fall-through as a sentinel. Mitigation: T0's audit step lists every consumer; each consumer's behavior under `lookup_failed` is documented before migration. If any consumer needs `unknown` semantics, route through a typed adapter rather than reverting the helper signature.
- Promoting `ControlPlaneAcceptance` may require migrating call sites of the narrower `acceptance_policy.AcceptanceDecision`. Mitigation: keep `acceptance_policy.AcceptanceDecision` as a delta-only sub-object inside `ControlPlaneAcceptance`.
- Byte-stable replay may detect minor stdout reorderings from the new render helper. Mitigation: snapshot the existing render bytes on the canonical replay fixture, write the helper to reproduce them exactly, only then refactor the existing call sites.

**Downstream coordination points (consumed by later cycles).**
- *C14-T0 ⇄ C14-T2.* `format_full_eval_marker_payload` reads `decision.target_delta_states` produced by T0's helper. T0 ships first; T2's render directly consumes T0's `DeltaState` enum.
- *C14-T0 ⇄ C14B.* Cycle 14B's partial-harvest policy reads `delta_state` per target QID (specifically `fixed` count, `still_hard` count, and `lookup_failed` count) when deciding whether bounded debt is acceptable. C14B cannot ship before T0; T0's helper signature is C14B's input contract.
- *C13 ⇄ C14-T2.* `format_full_eval_marker_payload` must render `RollbackClass.NO_ACTION` (introduced by C13-T1). Sequencing: C13 ships first per the roadmap, so C14-T2's helper directly handles `NO_ACTION`. If C14 ever ships before C13, C14-T2's helper defaults unknown rollback classes to the `OTHER` render path so C13's addition is byte-stable.
- *C15 ⇄ C14-T1.* `cluster_blocked_no_rca` decision records (C15-T1) flow through Phase B aggregation when C14-T1's `_finalize_iteration_summary` accounting lands. C14-T1's plan should not hard-code the set of decision-record types it counts; it should aggregate every typed record present in the iteration's trace.
- *C16-T3 ⇄ C14-T0 + T2.* C16-T3 adds `existing_hard_still_hard_outside_target` as a value of T0's `DeltaState` enum (not as a separate field on `ControlPlaneAcceptance`). C14-T0's enum must accept open-set extension via `StrEnum` so C16-T3's addition is non-breaking. C14-T2's `format_full_eval_marker_payload` must surface every enum value in both the stdout payload AND the replay record. C14-T3's I9 byte-equality assertion automatically covers the new value once added.
- *C16-T4 ⇄ C14-T0 + T3 + T4.* C16-T4's HIGH severity tier reads I9 (from C14-T3), I10 (from C14-T4), AND **I13 (new, from C14-T0)**. C14-T0's plan must register I13 under that canonical ID in `optimization/invariants.py`; C16-T4's lookup table includes I13 in the HIGH tier.
- *C17 ⇄ C14.* C17 reads journey events from `question_journey.py` / `lever_loop_replay.py` (in-memory) but its replay-validity audit fixture (C17-T4) replays the full decision-record stream that C14's canonical producer emits. If C14's canonical render reorders fields, C17's anchor-run replay fixture must be regenerated post-C14 — not a behavioral coupling but a fixture-regeneration ordering.

---

## Cycle 14B — Partial harvest with bounded regression debt

**Inspiration run.** New anchor `76457773587391` postmortem F1 (candidate scored `+17.4pp`, all thresholds met, only `gs_018` failed) + F3 (rollback discarded all progress because no partial-harvest policy exists). The new postmortem's "Recommended Next Actions" #1 explicitly cites this gap: *"the optimizer can produce a high-accuracy candidate; the missing piece is preserving or reattributing that progress safely instead of rolling it all back."*

**What this cycle closes.** A new capability the contract assumes but does not currently implement: when a candidate fixes ≥1 hard cluster AND aggregate accuracy improves AND out-of-target regression debt is under policy, the candidate is accepted with explicit debt accounting rather than fully discarded. When debt is over policy AND a single patch can be identified as the regression cause, the optimizer attempts patch-subset isolation (re-evaluate without that patch) before falling back to full discard.

**Stage(s) closed.**
- Stage 9 (`acceptance_decision`) — *partial-harvest policy sub-criterion*. Adds new rollback class `ACCEPTED_WITH_DEBT` and new `ControlPlaneAcceptance` field `regression_debt_qids`.
- Stage 10 (`learning_next_action`) — *patch-subset isolation arm*. When full-AG accept is policy-disallowed but a per-patch isolation produces a clean accept, the loop records the isolation as a `learning_next_action` constraint (the unsafe patch's signature is now forbidden in future expansions).

**Current state (audited).**
- `control_plane.ControlPlaneAcceptance` produces `out_of_target_regressed_qids`, `unknown_to_hard_regressed_qids`, etc. The acceptance gate's downstream policy in `harness.py` and `acceptance_policy.py` checks these fields against thresholds and returns `ROLLBACK` (full discard) on any non-empty out-of-target regression.
- No `ACCEPTED_WITH_DEBT` rollback class. No `regression_debt_qids` field carrying through to `learning_next_action` records.
- `patch_survival.json` per-iteration record exists in declared form but its producer is part of Cycle 12-T5's per-iteration migration. The substrate for patch-subset isolation needs T5 to land first.

**What changes.**
- **C14B-T1.** Define `RegressionDebtPolicy` dataclass in `optimization/acceptance_policy.py` with fields: `max_debt_qids` (int), `allowed_debt_buckets` (frozenset of `DeltaState` values, e.g. `{soft_to_hard}` only), `min_aggregate_improvement_pp` (float), `min_target_clusters_fixed` (int), `min_threshold_pass_rate` (float in [0,1]), `cumulative_debt_max` (int — caps total accepted debt across the run). Initially flag-gated with `max_debt_qids=0` so behavior is byte-stable on flag-off.
- **C14B-T2.** Add `RollbackClass.ACCEPTED_WITH_DEBT` and field `regression_debt_qids: tuple[QID, ...]` on `ControlPlaneAcceptance`. When candidate satisfies all four policy thresholds AND debt-under-policy, return `accept_with_debt` and emit a typed `acceptance_with_debt` decision record. The reflection-buffer entry for this iteration carries `levers=ag.lever_set` and `RollbackClass.ACCEPTED_WITH_DEBT` so C13-T3's forbidden-AG admission predicate can include it explicitly. **Default behavior: include `ACCEPTED_WITH_DEBT` in the forbidden-set** (the AG that produced debt is not retried unconditionally; the loop must mutate the lever family or move to a new cluster). Override is a separate flag for the rare "debt-causing AG produced enough win to warrant retry with a narrowed scope" case.
- **C14B-T3.** Patch-subset isolation: when policy-disallows full-AG accept AND `patch_survival.json` identifies a single patch as the regression cause, re-evaluate the candidate with only the survivors and route through the same accept-with-debt path. If multiple patches contribute to the regression, halt with `multi_patch_regression` rollback class (no isolation attempted; surfaces honestly that the regression is shared across patches). Behind a separate flag from T2's accept-with-debt; T3 ships only after C12-T5 lands AND T5's per-iteration completeness check returns green for `patch_survival.json`.

**Binary success criteria.**
- **T1.** `RegressionDebtPolicy` instance is constructible from `common/config.py` accessor; flag-off default produces byte-stable rollback behavior on existing replay fixtures.
- **T2.** New anchor `76457773587391` replayed against post-T2 code with policy `max_debt_qids=1, allowed_debt_buckets={soft_to_hard}, min_aggregate_improvement_pp=10, min_target_clusters_fixed=1`: the candidate accepts with `regression_debt_qids=(gs_018,)`, accepted accuracy lands at `≈ 91% to 95.7%` depending on whether dependent metrics shift, `learning_next_action` record carries `regression_debt_qids=(gs_018,)`, and the AG signature appears in next-iter `forbidden_ag_set`. Verified by integration test.
- **T3.** When policy-disallows full-AG accept AND single-patch isolation produces an under-policy candidate, the isolation candidate is accepted with debt and the rejected patch's `expanded_patch_id` appears in the `learning_next_action.unsafe_patches` list. Verified by integration test against a synthetic single-patch-regression fixture.

**Dependencies.**
- **Hard prerequisites.** Cycle 14-T0 (`compute_target_delta_states` and `DeltaState` enum is C14B's policy-input contract). Cycle 14-T2 (canonical render must surface `regression_debt_qids` and `RollbackClass.ACCEPTED_WITH_DEBT`). Cycle 13-T3 (forbidden-AG admission predicate is the hook C14B-T2's reflection entry rides).
- **Soft prerequisite (T3 only).** Cycle 12-T5 (`patch_survival.json` producer at the contract path) — required for T3 only; T1+T2 can ship without T5.

**Sizing.** Two plans (T1+T2 / T3), ~3-4 days total. T1+T2 is the minimum-viable accept-with-debt path; T3 is the optimization that recovers more candidates when regressions are single-patch.

*T1+T2 plan drafted and shipped: [`2026-05-09-cycle-14b-t1-t2-partial-harvest-with-debt-plan.md`](./2026-05-09-cycle-14b-t1-t2-partial-harvest-with-debt-plan.md) (10 tasks, ~2 working days). Closeout: see Cycle 14B-T1+T2 row in [`2026-05-05-optimizer-iteration-ledger.md`](./2026-05-05-optimizer-iteration-ledger.md).*

*T3 plan drafted and shipped (diagnostic-only mode): [`2026-05-09-cycle-14b-t3-patch-subset-isolation-plan.md`](./2026-05-09-cycle-14b-t3-patch-subset-isolation-plan.md) (8 tasks; pure helpers + typed markers + diagnostic-only orchestrator ship today; live re-eval arm gated on C12-T5). Closeout: see Cycle 14B-T3 row in [`2026-05-05-optimizer-iteration-ledger.md`](./2026-05-05-optimizer-iteration-ledger.md).*

**Flag(s).**
- `GSO_PARTIAL_HARVEST_WITH_DEBT` (default-off → warn-only → enforce). Flag-off path retains current full-discard behavior.
- `GSO_PATCH_SUBSET_ISOLATION` (default-off → on after T2 has been live for one corpus measurement). Independent of T2's flag — operators can enable accept-with-debt without enabling isolation, but enabling isolation requires T2's accept-with-debt path to be live (the isolated candidate routes through it).

**Risks.**
- Accepting bounded debt may compound across iterations: iteration 1 accepts `gs_018` debt, iteration 2 adds `gs_004` debt, etc. Mitigation: `RegressionDebtPolicy.cumulative_debt_max` caps total accepted debt across the run; once hit, subsequent iterations cannot accept debt until a debt-clearing iteration (a candidate that fixes a previous debt QID).
- Patch-subset isolation may surface `patch_survival.json` producer correctness bugs (e.g., a patch is mis-attributed as the regression source). Mitigation: T3 ships only after C12-T5's `patch_survival.json` producer has been corpus-validated; until then, T3 stays default-off.
- Accept-with-debt may regress aggregate accuracy in subsequent iterations if the debt patch interacts with later patches. Mitigation: T2's `regression_debt_qids` flow into the next iteration's strategist input as `dont_break_qids`; the strategist must avoid AGs that touch debt-related lever families.
- The new anchor's `gs_018` regression may not be representative — many real regressions could be hard-to-hard (not soft-to-hard) and policy-disallowed by default. Mitigation: the policy thresholds are flag-tunable; corpus measurement after T2 ships informs the default values for the next ledger row. C14B's "What changes" deliberately ships with conservative defaults rather than pre-tuning.
- **Pilot policy floor (`min_aggregate_improvement_pp=10.0`) likely too conservative (anchor #3 evidence).** 7Now run `337676694173049` (attempt 8) produced a `+8.7pp` candidate (`78.3% → 87.0%`) that, even after C14B redeploys, would still be rejected by the 10pp floor. Mitigation: do **not** pre-tune the floor; ship the conservative default and capture the next 3-5 post-redeploy runs as a corpus measurement. Specifically capture, per run, the candidate aggregate-delta distribution and the `regression_debt_qids` count; if the +8.7pp class of candidate appears repeatedly with bounded debt, lower the floor in a tracked ledger row. Pre-tuning would change the policy on n=1 evidence and would also be unreliable until **C16-T3** lands (see the next risk).
- **C14B telemetry input contamination from regression-bucket mis-classification (anchor #3 evidence).** On run `337676694173049`, three QIDs (`gs_021` baseline-hard H004; `gs_007`/`gs_030` soft-signal S001) all landed in `unknown_to_hard_regressed_qids`. C14B's policy reads `regression_debt_qids` derived from the bucketed inputs — so `max_debt_qids` and `allowed_debt_buckets` evaluate against an **inflated and mis-typed** count. Mitigation: C16-T3 (regression-bucket completeness) is now an explicit prerequisite for *trustworthy* C14B telemetry, not just a post-hoc cleanup. C14B can ship its policy-evaluation code path before C16-T3 (the policy infrastructure is correct on its own), but pilot policy tuning (the previous risk) must wait until C16-T3 lands so the debt count reflects the actual disjoint-union taxonomy. Until then, treat C14B accept-with-debt outcomes as warn-only diagnostics, not as evidence for floor adjustment.

**Downstream coordination points (consumed by later cycles).**
- *C14B-T2 ⇄ C13-T3.* The `ACCEPTED_WITH_DEBT` reflection-entry uses C13-T3's admission predicate. Default behavior: forbid the producing AG from retry (treat as `NO_ACTION`-equivalent for forbidden-set purposes). C13-T3's predicate must accept `ACCEPTED_WITH_DEBT` as an admission key; if C13-T3 has not yet shipped that hook when C14B lands, C14B-T2's plan adds the admission clause as part of T2 (rather than blocking on a C13 follow-up).
- *C14B ⇄ C16-T3.* C16-T3's `existing_hard_still_hard_outside_target` enum value is the disjoint-union complement to C14B's `regression_debt_qids` (debt = newly-introduced regressions; existing-hard = pre-existing baseline failures). They must not double-count: a QID in `regression_debt_qids` must not appear in `existing_hard_still_hard_outside_target` for the same iteration. P1 invariant updated by C16-T3 to enforce this disjointness.
- *C14B ⇄ C16-T4.* The `accepted_with_debt` decision is a *successful* run outcome with non-zero debt. C16-T4's contract-health summary must distinguish "ran clean" from "accepted with debt" — debt is a postmortem-relevant signal, not a merge-gate trip. Suggested addition to C16-T4's marker payload: `acceptance_class ∈ {accept, accept_with_debt, rollback, halt, merge_gate_blocked}`. C14B-T2's plan should explicitly leave room in the contract-health marker schema for this field.
- *C14B-T3 ⇄ C12-T5.* `patch_survival.json` is the substrate. T3 cannot ship before T5 lands AND T5's per-iteration completeness check returns green for `patch_survival.json`. C14B-T3's plan should verify this before drafting and explicitly reference T5's binary success criterion.

---

## Cycle 15 — RCA-first discipline (close arrow `Stage 2 → Stage 4`)

**Inspiration run.** `3b050ec5-...` postmortem F7 (I7 `open_cluster_ungrounded_at_ag_emit` fired for H001-H005, AG emission still proceeded).

**What this cycle closes.** The contract's letter requirement: *"the optimizer must diagnose why each question failed before it proposes changes."* No AG emits without a grounded RCA card.

**Stage(s) closed.**
- Stage 2 (`rca_evidence`) — letter and spirit.
- Stage 3 (`cluster_formation`) — auto-closes (every cluster either emits a grounded AG or a `cluster_blocked_no_rca` record).

**Current state (audited).**
- I7 (`check_i7_rca_grounding` in `invariants.py:313`) fires when AGs reach emit without a fit RCA card.
- `loop_invariants_strict()` is default-on for CI/replay (`config.py:5677`). Production override path is documented but not enforced.
- The harness has no pre-emit gate that consults the same logic; I7 detects the violation post-hoc.
- No `cluster_blocked_no_rca` typed record producer exists.

**What changes.**
- **C15-T1.** Add a pre-AG-emit gate that consults the same logic as I7. When a cluster fails the RCA-grounding check, emit a typed `cluster_blocked_no_rca` decision record with `reason_code` (`no_fit_rca_card`, `rca_evidence_exhausted`, etc.) and exclude the cluster from this iteration's AG pool. This is the "graceful degradation" arm — the run continues, just without the ungrounded AG.
- **C15-T2.** Promote I7 to require either a grounded AG or a `cluster_blocked_no_rca` record per cluster reaching AG-emit consideration. Failure of *both* is the new invariant trip.
- **C15-T3.** When all clusters end up blocked-no-rca, terminate the iteration with reason `no_grounded_clusters` instead of empty-iteration. This becomes the new visible failure mode (informative, not silent).

**Binary success criteria.**
- Zero AG emissions without `rca_card_id` in the AG decision record. Structural assertion via I7.
- For every cluster reaching AG-emit consideration: exactly one of `{cluster_emitted_ag, cluster_blocked_no_rca}` is recorded.
- Run terminates with `no_grounded_clusters` rather than silently looping when all clusters are ungrounded.

**Dependencies.** Cycle 14 (so the cluster_blocked_no_rca record flows into Phase B and the canonical decision stream cleanly).

**Sizing.** One plan, ~2-3 days, with a two-step warn → enforce rollout.

**Flag(s).** `GSO_RCA_GROUNDING_PRE_EMIT_GATE` (default-off → warn-only on for one pilot → enforce). Strict mode for I7 is governed by existing `loop_invariants_strict` flag; this cycle does not change strict-mode defaults (Cycle 16 does).

**Risks.**
- May block runs that today pass partial-RCA AGs and sometimes succeed. Mitigation: warn-only pilot first; the diagnostic-AG path (Cycle 1 AG-1-F, already shipped) becomes the safety valve when RCA is missing.
- Diagnostic AGs themselves should not be subject to this gate. Mitigation: explicit allowlist by `ag.is_diagnostic` flag.

**Downstream coordination points (consumed by later cycles).**
- *C16-T4 (contract-health summary).* C15-T3's terminal reason `no_grounded_clusters` is an honest termination, not a violation — it should NOT trigger a HIGH/MEDIUM severity in C16-T4. C16-T4's tier table treats terminal reasons separately from health-violation inputs; C15-T3's plan should add a dedicated `terminal_reason` field to `GSO_LEARNING_NEXT_ACTION_V1` (or the equivalent) so C16-T4 can read it without confusion.
- *C16-T2 (no-structural-alternative recovery).* C15-T1's `cluster_blocked_no_rca` records and C16-T2's `structural_causal_dropped` records are separate failure modes (cluster-level vs patch-level) and should both surface as distinct typed records — not collapsed into one "halt" path. C15-T1's plan should leave room in the decision-record schema for C16-T2 to add a sibling type without breaking C15's tests.
- *C17-T4 (anchor-run replay fixture).* If C15's pre-emit gate flips to enforce, the anchor run replayed against post-C15 code will produce a different decision-record stream (some clusters now blocked, no AG emit). C17-T4's fixture regeneration must run *after* C15 has shipped to enforce; otherwise the fixture captures the legacy stream and silently passes I12 against the wrong baseline.

---

## Cycle 16 — Causal continuity + merge gate (close arrow `Stage 11 → run exit`)

**Inspiration run.** `3b050ec5-...` postmortem F2 (causal H002 SQL patch dropped at blast-radius), F3 (causal-first cap kept metadata + routing patches, not the structural patch), F5 (regression accounting mixes baseline-hard with newly-hard), F9 (`MERGE_GATE_GAP` final status with 359 validation issues but `TERMINATED/SUCCESS`).

**What this cycle closes.** Two arrows in one cycle, both depending on the upstream three:
1. Causal continuity through safety gates: when the structural causal patch is dropped, the optimizer either synthesizes a narrower causal alternative (L5 example SQL) or halts honestly. Never silently degrades to non-structural-only.
2. Contract Health gates run exit: the run cannot exit `SUCCESS` while contract-health is `MERGE_GATE_BLOCKED`.

**Stage(s) closed.**
- Stage 6 (`safety_gates`) — spirit.
- Stage 8 (`post_patch_evaluation`) — letter (regression bucket completeness).
- Stage 11 (`contract_health`) — letter and spirit (closes arrow `11 → run exit`).

**Current state (audited).**
- `l6_narrow_replacement_for_expression_enabled()` (`config.py:5606`) exists, default-off. Branch A `query_id`-in-CASE form is **semantically wrong for metric views** — there is no `query_id` column to filter on in metric-view DDL. Branch C (L5 question-scoped example SQL) is the correct shape but is not implemented.
- `no_causal_applyable_halt_enabled()` is production-locked on (`config.py:5338`). It only fires when *every* RCA-grounded patch is dropped; non-structural patches that inherit the AG's RCA still survive when the structural patch is dropped.
- `ControlPlaneAcceptance` has `soft_to_hard_regressed_qids`, `passing_to_hard_regressed_qids`, `unknown_to_hard_regressed_qids` and a P1 disjoint-union invariant. There is no `existing_hard_still_hard_outside_target` bucket — already-hard QIDs that remain hard but are out-of-target leak into `unknown_to_hard`.
- No `GSO_CONTRACT_HEALTH_V1` summary marker. No `MERGE_GATE_BLOCKED` exit contract. Strict mode default in production needs verification.

**What changes.**
- **C16-T1.** Implement Branch C of the narrow structural fallback: when a `add_sql_snippet_expression` / `add_sql_snippet_measure` is dropped at blast-radius, synthesize a question-scoped Lever 5 `add_example_sql` patch instead. Replace the (incorrect) `query_id`-in-CASE Branch A with this. Default-off; promote to default-on after corpus measurement on the anchor run.
- **C16-T2.** Add a "structural causal patch dropped" detector: when a structural-shape patch (L6 expression/measure) is dropped while non-structural causal patches survive, emit a `structural_causal_dropped` decision record and route to C16-T1's narrow synthesis. If synthesis fails, halt with `no_structural_alternative` and let strategist recovery (Cycle 13's forbidden-set propagation) carry forward the constraint. **Concrete coordination with C13:** the `structural_causal_dropped` reflection entry must mirror C13-T2's call-site fix verbatim — `levers=ag.lever_set` (non-empty) and `RollbackClass.NO_ACTION` (or a new dedicated `RollbackClass.STRUCTURAL_DROP` if C13's enum has shipped without the broader scope). C13-T3's admission predicate is the single function the propagation rides; C16-T2 must add its admission clause via that hook, not a parallel re-implementation.
- **C16-T3.** *(Status: closed-local pending corpus.)* Add `existing_hard_still_hard_outside_target_qids` bucket to `ControlPlaneAcceptance`. Update bucket attribution: a candidate-failed QID that was hard at baseline AND is not in the AG's target set goes into this new bucket, never `unknown_to_hard`. Update P1 invariant accordingly. **Promoted scope (2026-05-09 #2; surfaced by anchor #3):** C16-T3 is now a **prerequisite for C14B's debt-policy telemetry trustworthiness**, not a post-hoc cleanup. Run `337676694173049` showed three QIDs landing in `unknown_to_hard_regressed_qids` that should not have: `gs_021` (baseline-hard H004, belongs in `existing_hard_still_hard_outside_target_qids`), `gs_007`/`gs_030` (soft signals S001, belong in `soft_to_hard_regressed_qids`). C14B's policy reads `regression_debt_qids` derived from these inputs, so until C16-T3 lands, every C14B accept-with-debt outcome is reading a contaminated debt count. C16-T3's bucket attribution must therefore also enforce that **soft-signal baselines route to `soft_to_hard_regressed_qids`, never `unknown_to_hard`** — extend C16-T3 with a second sub-task that distinguishes baseline `soft_signal` from baseline `unknown` at attribution time. Coordinate with C14-T0: T0's `DeltaState` enum already provides `soft_to_hard` and (newly added by C16-T3) `existing_hard_still_hard_outside_target` and the unknown-residual class. (closed by [Cycle 15.1 — Compliance Ratchet](./2026-05-10-cycle-15-1-compliance-ratchet-plan.md))
- **C16-T4.** Define a typed `GSO_CONTRACT_HEALTH_V1` summary marker emitted at run end. **Inputs (consumed from the typed observability layer the upstream cycles establish):**
  - From C12 — `phase_h_strict_validation.validator_status` / `.exception_class` (T2); `bundle_assembly_incomplete.parent_level_missing_count` / `.unmigrated_per_iteration_missing_count` (T3).
  - From C14 — `phase_b_records_match_replay` (T1's Phase B aggregator equality with replay), I9 (T3, acceptance byte-equality), I10 (T4, applied-patch ID injectivity).
  - From C16 itself — I11 (T1+T2, structural causal continuity); P1 (T3 update, regression-bucket disjoint-union).
  - From C17 — I12 (replay validity / zero illegal trunk transitions). **C16-T4 reserves the I12 slot in the HIGH tier even before C17 ships;** until C17 lands, the field defaults to "true" if no journey-validator runs, "false" if any illegal transition is detected, so the contract-health marker is C17-ready out of the box.
  - Pre-existing — `invariant_violation_counts_by_severity`, `journey_validation_violation_count`, `manifest_path_missing_count`, `replay_validity`.

  **Severity tiers:**
  - **HIGH** — `I3`, `I4`, `I7`, `I8`, `I9`, `I10`, **`I11`** (causal continuity), **`I12`** (replay validity, reserved for C17), **`I13`** (per-QID delta totality, from C14-T0); `replay_validity=false`; `phase_b records ≠ replay records`; **P1 invariant violation** (regression-bucket sum ≠ candidate-failed-QID count); `phase_h_strict_validation.validator_status` ∈ `{listing_failed, validator_failed}` *(silent-but-wired failure surfaced by C12-T2)*; `bundle_assembly_incomplete.parent_level_missing_count > 0` *(producer didn't run, surfaced by C12-T3)*.
  - **MEDIUM** — `I5`, `I6`, `manifest_path_missing > 0`, `bundle_assembly_incomplete.unmigrated_per_iteration_missing_count > 0` (until C12-T5 ships, after which this tier promotes to HIGH).
  - **LOW** — `I1` warnings; `phase_h_strict_validation.flag_enabled=false` *(only legitimate when explicitly disabled for an experiment)*.
  - **Excluded from severity** (terminal reasons, not violations) — `terminal_reason` ∈ `{no_grounded_clusters, no_eligible_ag_after_forbidden, no_structural_alternative}`. These surface in the marker payload for postmortem inspection but do not contribute to the merge-gate trip count.

  **Invariant ID stability.** I9 / I10 / I11 / I12 / I13 are registered in `optimization/invariants.py` under exactly those canonical IDs. **C14-T0 ships I13**; C14-T3 / T4 ship I9 and I10; C16-T1 / T2 ship I11; C17-T3 ships I12. Each upstream plan's TDD test suite asserts the canonical ID alongside the behaviour test, so C16-T4's lookup table cannot drift.

  Verification: round-trip C16-T4 against the C12-T2/T3 markers via `MarkerLog.phase_h_strict_validation` + `MarkerLog.bundle_assembly_incomplete` (both fields are wired by C12). Round-trip the I9-I12 inputs against `MarkerLog.invariant_violations` (existing field).
- **C16-T5.** When any HIGH severity violation present, lever-loop exits via `dbutils.notebook.exit({"status": "MERGE_GATE_BLOCKED", "blockers": [...]})` (or the local equivalent on non-Databricks runs). The exit message points to specific blocking violation(s) and the bundle path.
- **C16-T6.** Promote `loop_invariants_strict()` default to enforcing in production after one warn-only pilot of C16-T4 / T5.

**Binary success criteria.**
- For every iteration where `causal_dropped > 0`: either `narrow_synthesized > 0` OR `no_structural_alternative` record present. Verified by new I11.
- Sum of regression-bucket counts equals total candidate-failed QIDs. Existing P1 invariant updated.
- Every run with HIGH severity violation exits non-success with `MERGE_GATE_BLOCKED`. Verified by integration test.
- Anchor run `534010336956422` replayed with Cycle 13-16 ships fails the merge gate (because the historical run had violations); a fresh run on the same space converges or terminates honestly with non-`SUCCESS` and a typed reason.

**Dependencies.** Cycles 12, 13, 14, 15. Cycle 16 enforces what the prior four make truthful.

**Sizing.** Three plans (T1+T2 / T3 / T4+T5+T6), ~2 days each. T6 is a flag flip after a successful T4/T5 pilot.

**Flag(s).**
- `GSO_L6_NARROW_REPLACEMENT_BRANCH_C` (default-off → on after pilot). Replaces the misnamed `_FOR_EXPRESSION` flag (which was Branch A); document the rename in the migration note.
- `GSO_CONTRACT_HEALTH_MERGE_GATE` (default-off → warn-only → enforce).
- `GSO_LOOP_INVARIANTS_STRICT` already exists; T6 is a default-flip, not a new flag.

**Risks.**
- The merge gate may block all currently-acceptable runs when first turned on. Mitigation: warn-only pilot, gradual severity tightening, document the rollout in the iteration ledger.
- Branch C requires a question-scoped example-SQL synthesis path that may not exist in `cluster_driven_synthesis.py`. Mitigation: scope Branch C to leverage existing L5 example-SQL templates rather than synthesizing from scratch; explicit fallback to `no_structural_alternative` if synthesis is not feasible.
- T4's severity tiers depend on C12-T2/T3 marker payload field names. Mitigation: C12-T2/T3 plan-drafted marker payloads are the canonical specs; if those names change before C12 ships, update C16-T4 in lockstep. Verified by an integration test that round-trips `MarkerLog.phase_h_strict_validation` + `MarkerLog.bundle_assembly_incomplete` through the new contract-health marker.
- After C12-T5 ships, the `unmigrated_per_iteration_missing_count > 0` tier promotes from MEDIUM to HIGH (the per-iteration backlog is no longer a justified gap). Mitigation: track the promotion explicitly in the iteration ledger when T5 lands; the tier change is a one-line config edit, not a behavior flag.

---

## Cycle 17 — Journey-validation producer fix

**Inspiration run.** `3b050ec5-...` postmortem F9 (25 illegal trunk transitions on local replay: `clustered → soft_signal` ×15 and `clustered → already_passing` ×10).

**What this cycle closes.** After Cycle 16's merge gate ships, every run with these illegal transitions exits `MERGE_GATE_BLOCKED` — that is contract-honest behavior, but it is not contract compliance. The journey-event producer is itself broken. This cycle fixes the producer so replay validity becomes *achievable*, not just *gate-blockable*.

**Stage(s) closed.** Stage 11 (`contract_health`) — the journey-validation completeness sub-criterion. After this cycle, `replay_validity=true` is reachable on the corpus.

**Current state (audited).**
- `optimization/question_journey.py` and `optimization/lever_loop_replay.py` produce trunk transition events. The local replay validator rejects `clustered → soft_signal` and `clustered → already_passing` because the journey state machine declares those transitions illegal at trunk granularity.
- Cycle 6 F-5 attempted producer-side dedup for consecutive `soft_signal` trunks (Cycle ledger row 6); Cycle 4 N1 was the original journey-contract work. The audit in Cycle 6 noted N1's producer-side dedup did not fully land.
- The 25 illegal transitions on the anchor run indicate at least two distinct producer paths emit these transitions: the iteration-end candidate-state projection (`clustered → already_passing` after a partial improvement) and the soft-cluster-drift recovery path (`clustered → soft_signal` after a partial regression).

**What changes.**
- **C17-T1.** Audit every emit site of `trunk` journey events in `question_journey.py` and `lever_loop_replay.py`. For each site, classify: (a) emits a contract-legal transition, (b) emits an illegal transition that is the producer's fault, (c) emits an illegal transition because the journey state machine itself does not model the situation. (a) requires no change; (b) is fixed; (c) requires extending the state machine's legal-transitions table with documented rationale. **Concrete (c)-class entries surfaced by anchor #4 (run 294, 100% accepted):** (i) `clustered → already_passing` — fired for `gs_007`, a row clustered as related-but-not-target that *remained passing* after `AG_DECOMPOSED_H004` fixed `gs_024`. The state machine treats `clustered → already_passing` as illegal but the transition is legitimate when the row's failure-conditioning attribute is resolved by a fix elsewhere. T1 ships state-machine extension `clustered → already_passing` legal under predicate `target_resolved_elsewhere_in_same_iteration`. (ii) `evaluated → post_eval` — fired for `gs_016` after AG acceptance. The state machine has no `post_eval` terminal state for protected rows that pass after the iteration completes. T1 ships state-machine extension introducing `post_eval` as a terminal state and `evaluated → post_eval` as legal under predicate `iteration_terminal_with_acceptance`. Both extensions are unconditional (not flag-gated) — they are corrections to the legal-transitions table, not behaviour changes. C14-T3's I9 invariant allow-list mirrors these two new transitions exactly; the two surfaces ship in lockstep.
- **C17-T2.** Implement the (b) producer fixes. Specifically: when a candidate evaluation moves a hard-clustered QID to soft-pass or full-pass, the trunk event must be `clustered → resolved_candidate_pass` (a legal transition) or routed through `diagnostic_ag` (already legal), not directly to `already_passing` / `soft_signal`. When a soft-cluster-drift recovery produces a soft-signal QID, the trunk event must be `<prior_state> → soft_signal` where `prior_state` is the actual prior journey state, not always `clustered`.
- **C17-T3.** Add invariant **I12**: `replay_validity == true` (zero illegal trunk transitions). Severity HIGH. C16-T4 reserves I12 in its HIGH tier ahead of C17 shipping (see C16-T4 invariant ID stability note); C17-T3 registers the canonical `I12` ID in `optimization/invariants.py` and the merge gate picks it up automatically — no C16 edit required at C17 ship time.
- **C17-T4.** Replay the anchor run fixture against the fixed producers; expect zero illegal transitions. Add the anchor run as a regression fixture in `tests/replay/fixtures/`. **Forward dependency for future cycles:** after C17-T4 lands, the anchor run becomes the canonical replay regression fixture — every future cycle (Cycle 18+) that touches the decision-record stream, journey events, or the canonical render path must re-run this fixture and assert byte-stability. The fixture is regenerated only when an upstream cycle intentionally changes the canonical stream (e.g., C13-T1 adds a `RollbackClass`, C16-T3 adds a bucket field); each such regeneration is documented in the iteration ledger row that ships it.

**Binary success criteria.**
- For the anchor run replayed against new code: `replay_validity == true`, illegal-transition count == 0.
- For a fresh corpus run on the same space: same.
- I12 enforced by the merge gate (Cycle 16) means a run with any illegal transition exits `MERGE_GATE_BLOCKED`. Combined with the producer fix, this means production runs cannot ship illegal transitions silently AND cannot ship them loudly either — they cannot occur.

**Dependencies.** Cycle 14 (single source of truth for decisions and journey events; before Cycle 14, multiple producers may emit competing journey-event streams that confound this audit). Cycle 16 (merge-gate enforcement gives I12 teeth).

**Sizing.** One plan, ~2-3 days. T1 audit is the longest task; T2 producer fixes are typically one-line scope-correction edits per site.

**Flag(s).** `GSO_JOURNEY_PRODUCER_STRICT` (default-off → on after pilot). Replay byte-stability requires the flag-off path to keep current behavior. The flag exists primarily to allow staged rollout; the underlying state-machine extensions (T1-c) are unconditional.

**Risks.**
- T1's classification may reveal that some "illegal" transitions are legitimate state-machine extensions, not producer bugs. Mitigation: T1's output is a written audit checklist; each entry is reviewed before T2 touches code.
- After this cycle, runs that previously only failed silently may now also fail at the merge gate. Mitigation: this is the desired behavior; document the rollout in the iteration ledger and pair with corpus measurement so the rate of new merge-gate failures is tracked.
- C17-T4 anchor-run regeneration may capture decision-record reorderings introduced by C13-T1 / C14-T2 / C16-T3 if the regeneration runs before all those cycles ship. Mitigation: regenerate the fixture exactly once, after C16 has shipped its reserved fields (T3's bucket; T4's I-IDs); the fixture's commit message explicitly cites the upstream cycle hashes used for regeneration.

---

## Sequencing

```text
Critical path (must ship in order):

  C12-T1 ──> {C13, C14-T0, C14B} ──> C14-V ──> C14-W ──> C14-C ──> C14-T3 ──> {C16-T3, C14-T4}
                                       │                                  ──> C15 ──> C16-T1+T2
                                       │                                  ──> C16-T4+T5+T6 ──> C17
                                       │
  (C14-V is the post-redeploy defect sweep; ships before C14-W)
  (C14-W is the post-Cycle-14-V defect sweep #2 + C13 default-flip;
   ships before C14-T3/C14-C so their evidence base is contradiction-free)
  (C14-C is the first-class attribution-drift partial harvest;
   ships before C14-T3 because attribution accounting feeds replay validity)
  (effective_flags traceability ─ all downstream cycles cite C12-T1 for postmortem)

Parallel branches:

  After C12-T1 lands:
    C12-T2/T3 ──┐
                ├─> consumed by C16-T4 as typed observability inputs
    C12-T4    ──┘    (phase_h_strict_validation, bundle_assembly_incomplete)
    C12-T5 ─────> consumed by C16-T4 (MEDIUM→HIGH severity promotion)
                      └─> substrate for C14B-T3 (patch_survival.json)

  After C14-T0 lands (T1-T4 parallel; T2 reads T0 output):
    C14-T1 ──┐
    C14-T2 ──┤
    C14-T3 ──┤
    C14-T4 ──┘
             └─> all consumed by Cycles 15-17

  After C14-T0 + C13-T3 land (C14B is sibling to C14, parallel ship):
    C14B-T1 + T2 ──> consumed by C13's forbidden-set + C16-T4 acceptance_class
    C14B-T3      ──> additionally requires patch_survival.json (C12-T5 substrate)

Cross-cycle data flows (each upstream cycle's "Downstream coordination points"
section names the consumer; each downstream cycle's "Dependencies" cites the
producer):

  C13-T1 (RollbackClass.NO_ACTION)             ─> C14-T2 render helper
  C13-T3 (forbidden-AG admission predicate)    ─> C16-T2 reflection-entry shape
                                                   ─> C14B-T2 ACCEPTED_WITH_DEBT
  C14-T0 (DeltaState enum + I13)               ─> C14-T2 render helper
                                                   ─> C14B-T1 policy input
                                                   ─> C16-T3 enum extension
                                                   ─> C16-T4 HIGH severity (I13)
  C14-T1 (Phase B aggregator in finalize)      ─> C12-T4 audit (same call site)
                                                   ─> C15 (cluster_blocked_no_rca records)
  C14-T2 (canonical render helper)             ─> C16-T3 bucket field
                                                   ─> C14B-T2 regression_debt_qids surface
  C14-T3 / T4 (I9 / I10 canonical IDs)         ─> C16-T4 HIGH severity tier
  C14B-T2 (acceptance_with_debt record)        ─> C13-T3 forbidden-set extension
                                                   ─> C16-T4 acceptance_class field
  C14B-T3 (patch-subset isolation)             ─> requires C12-T5 (patch_survival.json)
  C16-T1 / T2 (I11 canonical ID)               ─> C16-T4 (self-reference)
  C17-T3 (I12 canonical ID)                    ─> C16-T4 (slot reserved pre-C17)
  C17-T4 (anchor-run regression fixture)       ─> all future cycles (Cycle 18+)
```

| Cycle | Working days | Plans | Flags introduced |
|---|---|---|---|
| 12 | ~11-13 | 5 (T1, T2, T3 drafted; T4, T5 pending) | `GSO_RUN_MANIFEST_V2` (default-on with V1 fallback); `GSO_PER_ITERATION_BUNDLE_PATHS_DUAL_WRITE` (off → on after corpus measurement); plus 2 emit-only markers (`GSO_PHASE_H_STRICT_VALIDATION_V1`, `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1`) |
| 13 | ~2 | 1 | `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` (off → on) |
| 14 | ~6-9 (T0 first, then T1-T4 parallel) | 5 (T0 + T1 + T2 + T3 + T4) | `GSO_TARGET_DELTA_STRICT` (default-on, defensive) |
| 14-V | ~1.5-2 | 1 (shipped) | 3 observability-only flags, all default-on (`GSO_FORBIDDEN_AG_ADMISSION_OBSERVE`, `GSO_PATCH_ISOLATION_OBSERVE`, `GSO_CANONICAL_RENDER_INVARIANT`); 4 emit-only markers; 2 regression-rail markers |
| 14-W | ~2-3 | 1 (drafted) | 3 new observability-only flags, all default-on (`GSO_DATABRICKS_IDS_RESOLUTION_TRACE`, `GSO_ITERATION_SUMMARY_TOTALITY`, `GSO_PHASE_H_CANONICAL_CONSUMER`); 1 default-flip (`GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` off → on, corpus-validated by C14-V T1); 4 new typed markers (`GSO_DATABRICKS_IDS_RESOLVED_V1`, `GSO_ITERATION_SUMMARY_TOTALITY_V1`, `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1`, `GSO_PHASE_H_JOURNEY_DRIFT_V1`); extends `GSO_FULL_EVAL_V1` payload with `target_soft_passing_qids` |
| 14-C | ~3-4 | 1 (queued; drafts post-14-W) | `GSO_ATTRIBUTION_DRIFT_FIRST_CLASS` (off → warn → enforce); `GSO_ATTRIBUTION_DRIFT_OBSERVE` (default-on per Q#12 promotion); 2 typed markers (`GSO_ATTRIBUTION_DRIFT_V1`, `GSO_ATTRIBUTION_POINTS_AT_HARD_V1`) |
| 14B | ~3-4 | 2 (T1+T2 / T3) | `GSO_PARTIAL_HARVEST_WITH_DEBT` (off → warn → enforce); `GSO_PATCH_SUBSET_ISOLATION` (off → on after corpus measurement) |
| 15 | ~3 | 1 | `GSO_RCA_GROUNDING_PRE_EMIT_GATE` (off → warn → enforce) |
| Cycle 15.1 — Compliance Ratchet | closed-local pending corpus | I9 (acceptance render byte-equality), I10 (applied-patch ID injectivity), C16-T3 (existing-hard-outside-target bucket + soft-signal routing lock-in). No behaviour change; observability + bookkeeping closures. | [2026-05-10-cycle-15-1-compliance-ratchet-plan.md](./2026-05-10-cycle-15-1-compliance-ratchet-plan.md) |
| 16 | ~6 | 3 | `GSO_L6_NARROW_REPLACEMENT_BRANCH_C` (off → on); `GSO_CONTRACT_HEALTH_MERGE_GATE` (off → warn → enforce); `GSO_LOOP_INVARIANTS_STRICT` (default flip) |
| 17 | ~3 | 1 | `GSO_JOURNEY_PRODUCER_STRICT` (off → on after pilot) |
| **Total** | **~41-48 working days** | **20 plans** | **17 new flags + 2 default flips + 8 emit-only markers + 4 regression-rail markers** |

## What this roadmap does *not* do

- It does not introduce a "Cycle 12 omnibus" plan. Cycle 12 has five tasks (T1-T5) but each ships as an independent dated plan.
- It does not bundle unrelated stages into one cycle. Cycle 14's five tasks (T0-T4) are conceptually one (data-quality unification — T0 makes per-QID delta state total; T1 makes Phase B aggregation total; T2 makes acceptance render canonical; T3-T4 add invariants enforcing that totality) but ship as independent plans. Cycle 14B is sibling to Cycle 14 (acceptance-policy completeness — partial-harvest with debt) and ships in parallel after T0 lands. Cycle 17 is sibling to Cycle 14 too (producer-side data-quality for journey events) but sequenced after Cycle 16 because its acceptance test needs the merge-gate enforcement to be wired.
- It does not promote any new gate to enforcing without a warn-only pilot.
- It does not propose any new architecture (no Reflexion Controller, no new run-output contract version V2). Every change is a minimum-leverage closure of an existing audit gap against the existing contract.
- It does not introduce a new MLflow contract version. All changes ride existing markers (extending payloads where backward-compatible) or add narrowly-scoped new markers.
- It does not preempt the iteration ledger's discipline. After each cycle ships, append one row to [`2026-05-05-optimizer-iteration-ledger.md`](./2026-05-05-optimizer-iteration-ledger.md) using the established cycle template.

## Self-check: does shipping this roadmap deliver letter + spirit compliance?

For each contract stage:

| Stage | Letter after roadmap | Spirit after roadmap | Closing cycle(s) |
|---|---|---|---|
| 1. Evaluation State | ✓ Phase B aggregator authoritative on every iteration | ✓ One source for evaluation-state records | 14-T1 |
| 2. RCA Evidence | ✓ Every cluster has either grounded RCA or `cluster_blocked_no_rca` | ✓ RCA strictly precedes AG selection | 15 |
| 3. Cluster Formation | ✓ Every cluster has a typed status record | ✓ Grounded clusters proceed, ungrounded clusters block | auto via 15 |
| 4. Action Group Selection | ✓ AG records carry `forbidden_ag_set` reference | ✓ Strategist input includes prior outcomes as constraints | 13 |
| 5. Proposal Generation | ✓ Empty proposals emit typed record | ✓ Stage cannot fail silently | 13 |
| 6. Safety Gates | ✓ Causal-drop emits typed record | ✓ Causal continuity preserved or honest halt | 16 |
| 7. Applied Patches | ✓ Every applied ID is `expanded_patch_id` | ✓ Identity injective | 14-T4 |
| 8. Post-Patch Evaluation | ✓ Per-QID delta state total over targets (no `unknown` fall-through); all buckets + `existing_hard_still_hard_outside_target` | ✓ Delta computation is a total function; bucket assignment is a function, not a heuristic | 14-T0 + 16-T3 |
| 9. Acceptance / Rollback | ✓ One canonical decision fed by correct deltas; one render path; `accept_with_debt` rollback class for bounded-debt candidates | ✓ Stdout and replay agree; high-quality candidates with bounded debt are preserved, not discarded | 14 + 14B |
| 10. Learning / Next Action | ✓ Reflection entries carry full identity (`lever_set`, `next_action`); `regression_debt_qids` flows to next-iter strategist | ✓ Strategist reads reflections as constraints; debt-causing patches inform future AG selection | 13 + 14B |
| 11. Contract Health | ✓ Typed health summary marker; bundle assembler completeness (parent + per-iteration paths); closeout audits run | ✓ HIGH violations block run exit; replay validity achievable (not just gate-blockable); validator state typed-observable (no more silent-but-wired failures) | 12 (T1-T5) + 16 + 17 |

For each inter-stage arrow:

| Arrow | Closed by |
|---|---|
| `Stage 2 → Stage 4` | Cycle 15 (RCA-first gate) |
| `Stage 9 → Stage 10` | Cycle 14 (canonical AcceptanceDecision render) |
| `Stage 10 → Stage 4` | Cycle 13 (forbidden-set admits no-action) |
| `Stage 11 → run exit` | Cycle 16 (merge gate enforcement) + Cycle 17 (producer-side fix so the gate is achievable) |

For each storage-contract artifact (per contract Task 5 + Task 8):

| Artifact path | Producer cycle | Audit cycle |
|---|---|---|
| `gso_postmortem_bundle/manifest.json` | already shipped (legacy) | Cycle 12-T2 (validator wiring) + T3 (`audit_parent_bundle`) + T4 (closeout) |
| `gso_postmortem_bundle/run_summary.json` | already shipped (legacy) | Cycle 12-T3 (audit extension) |
| `gso_postmortem_bundle/artifact_index.json` | already shipped (legacy) | Cycle 12-T3 |
| `gso_postmortem_bundle/operator_transcript.md` | already shipped (legacy) | Cycle 12-T3 + T4 (per-iteration firing) |
| `gso_postmortem_bundle/decision_trace_all.json` | **Cycle 12-T3** (`build_decision_trace_all`) | Cycle 12-T3 |
| `gso_postmortem_bundle/journey_validation_all.json` | **Cycle 12-T3** (`build_journey_validation_all`) | Cycle 12-T3 + Cycle 17 (producer correctness) |
| `gso_postmortem_bundle/replay_fixture.json` | **Cycle 12-T3** (reuses existing `_replay_fixture_json` from `harness.py:22914`) | Cycle 12-T3 |
| `gso_postmortem_bundle/scoreboard.json` | **Cycle 12-T3** (`build_scoreboard` minimal-but-honest) | Cycle 12-T3 |
| `gso_postmortem_bundle/failure_buckets.json` | **Cycle 12-T3** (`build_failure_buckets` minimal-but-honest) | Cycle 12-T3 |
| `iterations/iter_NN/{summary,rca_ledger,proposal_inventory,patch_survival}.json` | **Cycle 12-T5** (new producers under contract path; today these have no producer at all) | Cycle 12-T5 |
| `iterations/iter_NN/{decision_trace,journey_validation,operator_transcript}.json/.md` | **Cycle 12-T5** (migrate from legacy `phase_a/`/`phase_b/` prefixes; producers exist but write to wrong path) | Cycle 12-T5 |
| `mlflow_audit.gso_postmortem_bundle` (contract Task 8) | already shipped (`tools/mlflow_audit.audit_parent_bundle`); Cycle 12-T3 extends from `manifest.json` only to all 9 parent paths | Cycle 12-T4 verifies green on a fresh run |

**Verdict.** Shipping Cycles 12 → 17 plus sibling Cycle 14B AND **the Cycle 14-V defect sweep, the Cycle 14-W defect sweep #2, and the Cycle 14-C first-class attribution-drift partial harvest** in the sequenced order above, with each cycle's binary criteria met and each registered defect (D-1 → D-8) closed-and-corpus-validated under the closing protocol (Discipline A integration tests for regressed closures + Discipline B tracing markers for multi-path resolvers + corpus-pilot regression-rail silence), brings every contract stage to letter-and-spirit compliance, closes every inter-stage arrow the contract requires, materializes every contract-declared storage-contract artifact, makes replay validity achievable rather than only merge-gate-blockable, AND gives the optimizer the policy machinery to preserve high-quality candidates with bounded regression debt AND properly attribute keep-the-win acceptances to the QIDs that actually improved (rather than to the still-hard target). Genie space accuracy is downstream and non-deterministic; the loop's process discipline is what we control, and after this roadmap, every failed run produces a non-repeated next experiment, every successful run is reproducible from one record, every near-success run preserves what it earned, AND every shipped-cycle defect is corpus-measurable before behavior-flag promotion.

## Open questions / out of scope

These are intentionally not addressed by this roadmap. They are honest follow-ups, not gaps. (Items previously listed here as #1 "`mlflow_audit.py` parent bundle audit" and the journey-validation producer fix are now upgraded into Cycle 12-T3 + Cycle 12-T4 + Cycle 17 above. Item formerly listed as "Cycle-12 audit may surface a fourth gap class" is now scoped as Cycle 12-T5 — the per-iteration path migration. The 2026-05-09 revision retired two further implicit items: "target-resolution accounting" is now scoped as **Cycle 14-T0**, and "partial harvest with regression debt" is now scoped as **Cycle 14B**.)

1. **Strategist non-determinism.** Even after Cycle 13 closes the forbidden-AG arrow, the strategist's *initial* AG selection is LLM-dependent. The contract does not require determinism here; the loop only requires that the *evolution* of selections be a function of recorded outcomes. This is by design.
2. **Diagnostic AG quality.** Cycle 15 makes diagnostic AGs the safety valve for ungrounded clusters. Whether they then *succeed* in grounding is a separate corpus question. If after Cycle 15 we observe diagnostic AGs running 5 iterations without grounding any cluster, that's a Cycle 18+ scope.
3. **Cycle 12 `effective_flags` introspection.** If `inspect.getmembers(config, callable)` becomes fragile (e.g., a new flag is added but not picked up because it has unusual signature), we should consider a flag registry. Deferred until Cycle 12 ships and we observe the failure mode.
4. **Branch C synthesis fidelity.** The L5 question-scoped example-SQL synthesis in Cycle 16 may not always produce a high-quality alternative. The fallback `no_structural_alternative` is the honest answer when synthesis underperforms; whether to escalate to a different lever family (L1+L2 combo, etc.) is a Cycle 18+ corpus question.
5. **Richer schemas for `scoreboard.json` and `failure_buckets.json`.** Cycle 12-T3 ships minimal-but-honest builders for both (per-iteration counts, summary metrics). The richer `LoopSnapshot`-based scoreboard (8 metrics + dominant-signal classification) and the per-bucket QID lists already exist in `optimization/scoreboard.py` and `optimization/failure_bucketing.py` as in-memory data structures, but require harness wiring to flow through to the parent-bundle artifact. Becomes a Cycle-18+ task only if a postmortem skill needs richer data.
6. **Cycle 17 state-machine extensions (T1-c).** If the audit shows some "illegal" transitions are legitimate state-machine extensions (e.g., a previously-undocumented `clustered → resolved_candidate_pass` transition is needed), those extensions ship inside Cycle 17. If extensions are themselves multi-cycle work (e.g., a richer journey state machine), they spill into a follow-up cycle named explicitly in the iteration ledger.
7. **Cross-run learning.** This roadmap covers within-run learning (reflection buffer, forbidden-AG set, learning-next-action records). It does not address learning across runs — e.g., remembering that a specific AG signature failed in a prior run and excluding it from this run's strategist input. Becomes a Cycle 18+ task; sequenced after Cycle 17 because cross-run reflections need trustworthy within-run data first (`replay_validity=true` is a precondition for storing reflections that future runs can rely on).
8. **"Exists-but-silent" pattern audit.** C12-T2 surfaced that the Phase H validator was correctly wired but silent — its broad `try/except` swallowed every exception class. There may be other instances of this pattern in the codebase (RCA grounding, proposal generation, lever loop iteration body). A targeted audit applying T2's typed-observability pattern to other harness blocks is candidate Cycle-18+ work; only do it if Cycle 12-T4 closeout surfaces evidence of additional silent-failure modes, not preemptively.
9. **Cycle 14B policy tuning.** C14B ships with conservative policy defaults (`max_debt_qids=0` flag-off; `max_debt_qids=1, allowed_debt_buckets={soft_to_hard}` flag-on). Whether the optimal policy across the corpus is "0 debt allowed" (current default), "1 debt allowed if soft→hard only" (proposed pilot), or something richer (per-bucket debt quotas, debt-decay across iterations, policy-by-domain) is a corpus measurement question. Becomes a Cycle-18+ task after C14B has been live for at least one corpus pilot. Distinct from #4 — Branch C synthesis is about *generating alternatives*; this is about *accepting outcomes*.
10. **C12-T1 job/task-id payload bug surfaced by new anchor — REOPENED (was RESOLVED by Cycle 14-V T6; regressed in production; reclosing in Cycle 14-W T3).** The new postmortem's F8 reports the run manifest emits with blank `databricks_job_id` / `task_id` fields. C12-T1 marker shape ships correctly, but the job-context fetch wiring is incomplete. **Cross-space confirmation (2026-05-09 #2):** airline anchor #4 (run 294) F8 reports the same blank-IDs symptom on a different optimizer run on a different space, which rules out 7Now-specific bundle config and confirms it's a wiring bug at the C12-T1 marker emission site. **Re-confirmed by anchors #5 + #6 (2026-05-09 #3)** — both post-redeploy pilots still emit blank IDs. **Status update (2026-05-09 #4):** Cycle 14-V T6 shipped `_databricks_ids_from_env` with env→dbutils-tag→sentinel chain. Anchors #7 + #8 (post-Cycle-14-V) confirm BOTH still report blank IDs — production code path either doesn't reach the resolver OR dbutils tag names differ in this Jobs runtime. Defect **D-5** flipped from `closed` to `regressed in production` and is closing again in **Cycle 14-W T3** under Discipline B (multi-path resolvers ship typed `_RESOLVED_V1` tracing markers + Discipline A integration test exercising the dbutils path with mocked Jobs context).

11. **L6 narrow-replacement promotion vs. defer (2026-05-09 #2).** Both anchor #3 (7Now run 337, `add_sql_snippet_expression` for the H002 `gs_026` causal fix `lost_at:applyability`) and anchor #4 (airline run 294, `add_sql_snippet_measure` for `SUM(tkt_payment.PAYMENT_AMT)` dropped by blast-radius) confirm that **two distinct Lever 6 patch types** are unrecognised by the narrow-replacement builder, both routing through `narrow_not_applicable, reason=unrecognized_patch_type`. On 7Now this has now blocked the H002 hard cluster across **8+ consecutive attempts** (the optimizer keeps generating the right structural patch; the gates keep dropping it; the narrow fallback can't transform it). C16-T1 currently sequences after C12+C13+C14+C14B+C15 — i.e., it is the *last* cycle ahead of the merge gate. The open question: should L6 narrow-replacement be **promoted ahead of** at least C13/C15, or stay sequenced after them? Arguments in favour of promotion: (a) it is the highest-leverage *accuracy* unlock the roadmap currently has (the only path to fixing 7Now's H001-H005 hard clusters); (b) it is independent of C13/C14/C14B/C15 — modules `optimization/stages/gates.py`, `optimization/sql_shape_quality.py`, `optimization/cluster_driven_synthesis.py` don't share call sites with the acceptance/RCA work; (c) without L6 surviving, C13/C14/C14B/C15 deliver process correctness on a body of work the loop cannot fundamentally improve. Arguments in favour of defer: (a) C13/C14B both close *learning* arrows that prevent the same wasted-iteration patterns C16-T1 would enable more of; (b) C16-T1's binary success criterion ("Branch C synthesis produces accepted candidates on the corpus") is itself contingent on C14-T0 (delta correctness) and C14B (partial harvest) being live, otherwise a successful Branch C alternative would still be discarded at acceptance. Resolution path: defer the decision to the post-redeploy lever loop — re-run 7Now with C14-T0 + C14B live and observe whether the +8.7pp class of candidate becomes accept-with-debt or still rolls back; if accept-with-debt fires AND H002 still doesn't move, promote C16-T1 ahead of C15 in a tracked ledger row. Not promoted unconditionally because anchor #3's evidence pre-dates the redeploy. **Update (2026-05-09 #3):** anchor #5 (post-redeploy 7Now run 338) shows the +8.7pp candidate STILL rolled back with `target_qids_not_improved` (not `accepted_with_partial_harvest_debt`), because `gs_026` remains hard *as a target* — partial harvest only protects out-of-target debt, not the target itself. This is correct C14B behavior on this evidence. The promotion decision now depends on C14-V's shadow markers: if `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` and `GSO_PATCH_ISOLATION_OBSERVE_V1` evidence (post-Cycle 14-V) shows the loop is making productive progress on the *non-target* debt clusters with C13/C14B-T3 logic enabled, then C16-T1 can stay sequenced after C15. If the post-14-V corpus measurement still shows H002 stuck across multiple attempts despite admission + isolation working, promote C16-T1 ahead of C15.

12. **Shadow-mode pattern as institutional discipline — RESOLVED (promoted to standard discipline 2026-05-09 #4).** Cycle 14-V's T1 + T2 introduced a "shadow-mode observe under a separate default-on flag" pattern. **Anchor #7 F5 corpus-validates the pattern**: `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` fired on 5/5 NO_ACTION reflections; rail silent; no replay byte-stability impact; total stdout volume = 5 marker lines on a 5-iteration run (negligible). Discipline promoted from "provisional" to "standard" in iteration-ledger plan revision #4. Going forward, every cycle that introduces a behavior flag with default-off MUST also ship a sibling default-on observability flag emitting a `GSO_<FLAG>_OBSERVE_V1` marker on the canonical-trigger code path. Cycle 14-W applies this discipline to Cycle 14-C's behavior flag (`GSO_ATTRIBUTION_DRIFT_FIRST_CLASS` + `GSO_ATTRIBUTION_DRIFT_OBSERVE`).

13. **Discipline A check on regressed-defect closures (new 2026-05-09 #4).** D-4 and D-5 regressed in production despite C14-V T5/T6 unit tests passing. Root cause: unit tests didn't exercise the production code path. Cycle 14-W T2 + T3 ship anchor-fixture-replay integration tests (Discipline A). Open question: should every defect closure ship an integration test, or only regressed defects? Resolution path: defer to C14-W closeout. If the integration-test pattern adds <30% incremental test cost and consistently catches production-shape gaps that unit tests miss, promote to standard discipline (every closure requires integration test). Until then, the discipline applies only to regressed defects.

14. **Discipline B check on multi-path resolvers (new 2026-05-09 #4).** D-5's regression was invisible because `_databricks_ids_from_env`'s internal resolution path wasn't traced. Cycle 14-W T3 introduces typed `GSO_DATABRICKS_IDS_RESOLVED_V1` tracing. Open question: which other multi-path resolvers in the codebase need the same treatment? Candidates: `_resolve_genie_space_id`, `_resolve_metric_view_catalog`, `_resolve_baseline_evaluation_run`. Audit deferred to C14-W closeout — if the corpus pilot post-14-W shows multi-path-resolver regressions in a different function, register the audit as Cycle 18+ scope.

## References

- [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) — the canonical contract this roadmap is auditing against.
- [`2026-05-05-optimizer-iteration-ledger.md`](./2026-05-05-optimizer-iteration-ledger.md) — append one ledger row per cycle as it ships; canonical Defect Registry status feeds this ledger's "Status" column row-by-row.
- [`2026-05-05-optimizer-iteration-ledger-plan.md`](./2026-05-05-optimizer-iteration-ledger-plan.md) — ledger structure spec; updated 2026-05-09 #4 to promote shadow-mode pattern from "provisional" to "standard discipline" (Q#12 closure) + introduce Discipline A (regressed-defect integration tests) + Discipline B (multi-path resolver tracing markers).
- [`2026-05-05-optimizer-iteration-and-troubleshooting-guide.md`](./2026-05-05-optimizer-iteration-and-troubleshooting-guide.md) — operator dev-loop guide; updated 2026-05-09 #4 with three new "shipped-cycle-regressed" diagnostic signatures (D-6 / D-7 / D-8) under §2.3 "classic symptom → likely stage map" + "regressed defect" diagnostic protocol.
- [`2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md`](./2026-05-09-cycle-14-w-post-cycle-14-v-defect-sweep-plan.md) — **Cycle 14-W plan (drafted 2026-05-09 #4); closes regressed/partial D-3/D-4/D-5, new D-6/D-7/D-8, promotes corpus-validated D-1.**
- [`2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md`](./2026-05-09-cycle-14-v-shipped-cycle-defect-sweep-plan.md) — Cycle 14-V plan (shipped 2026-05-09 #3); closes defects D-1 → D-5.
- [`2026-05-09-cycle-13-forbidden-ag-admission-plan.md`](./2026-05-09-cycle-13-forbidden-ag-admission-plan.md) — Cycle 13 plan; corpus measurement enabled by Cycle 14-V T1.
- [`2026-05-09-cycle-14-t0-target-delta-correctness-plan.md`](./2026-05-09-cycle-14-t0-target-delta-correctness-plan.md) — Cycle 14-T0 plan; total `compute_target_delta_states` consumed by Cycle 14-V T3's render fix.
- [`2026-05-09-cycle-14-t1-t2-phase-b-totality-and-canonical-acceptance-render-plan.md`](./2026-05-09-cycle-14-t1-t2-phase-b-totality-and-canonical-acceptance-render-plan.md) — Cycle 14-T1+T2 plan; defines `format_full_eval_marker_payload` that Cycle 14-V T3 fixes.
- [`2026-05-09-cycle-14b-t1-t2-partial-harvest-with-debt-plan.md`](./2026-05-09-cycle-14b-t1-t2-partial-harvest-with-debt-plan.md) — Cycle 14B-T1+T2 plan; **confirmed working in production** by anchor #6.
- [`2026-05-09-cycle-14b-t3-patch-subset-isolation-plan.md`](./2026-05-09-cycle-14b-t3-patch-subset-isolation-plan.md) — Cycle 14B-T3 plan; corpus measurement enabled by Cycle 14-V T2.
- [`runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097/postmortem.md`](./runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097/postmortem.md) — 7Now anchor evidence: original anchor `534010336956422` (attempt 5), acceptance-stage anchor `76457773587391` (attempt 7), pre-redeploy confirmation anchor `337676694173049` (attempt 8), post-redeploy anchor `338386531912450` (attempt 10, anchor #5; D-1/D-3/D-5 citations), and **post-Cycle-14-V anchor `960148942255012` (attempt 11, anchor #7; canonical citation for D-1 corpus-validation + D-3 ext + D-5 regression + D-8)** — all sequential lever-loop attempts in the same optimization-run directory.
- [`2026-05-05-cycle-11-honest-loop-pilot-plan.md`](./2026-05-05-cycle-11-honest-loop-pilot-plan.md) — the prior cycle that shipped invariants and producer-exception records (the foundation this roadmap builds on).
- [`runid_analysis/1099b152-8655-4f1e-ab43-1240a9400280/postmortem.md`](./runid_analysis/1099b152-8655-4f1e-ab43-1240a9400280/postmortem.md) — airline anchor evidence: anchor #4 (run `294637253025289`, attempt 10), anchor #6 (run `833709971504406`, attempt 12; first in-production C14B partial-harvest accept), and **post-Cycle-14-V anchor #8 (run `1105451933925748`, attempt 13; canonical citation for D-4 regression + D-5 cross-space confirmation + D-6 + D-7 + first in-production `accepted_with_attribution_drift` evidence motivating Cycle 14-C).** Earlier attempts in this directory surfaced Cycle 11 stability fixes (Bugs A/B).
