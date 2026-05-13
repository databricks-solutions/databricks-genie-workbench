# Roadmap Closeout — Execution Spine + Process Spine

> Status: scoped after self-review against the current codebase on 2026-05-10.
> Purpose: prevent scope creep by naming the finite set of remaining plans needed to close the project spine before shifting effort to LLM prompt/model optimization.

## Goal

Close the deterministic optimizer spine so the non-LLM portions of the lever loop are accurate, repeatable, auditable, and enforceable.

The closeout is complete when:

1. The **execution spine** is one deterministic pipeline with typed stage inputs/outputs, stable object identities, canonical ordering, and gate-by-gate tests.
2. The **process spine** consumes those deterministic outputs through invariants and health checks, then blocks bad runs instead of silently producing confusing postmortems.
3. Remaining LLM work is isolated to prompt quality, not compensating for deterministic-stage drift.

## Spine Definitions

### Execution Spine

The deterministic journey around the LLM calls:

1. `evaluation_state`
2. `rca_evidence`
3. `cluster_formation`
4. `action_group_selection`
5. `proposal_generation`
6. `safety_gates`
7. `applied_patches`
8. `post_patch_evaluation`
9. `acceptance_decision`
10. `learning_next_action`

LLM calls are allowed between these stages, but every handoff into and out of deterministic code must be typed, fixture-backed, and reproducible.

### Process Spine

The enforcement layer over the execution spine:

1. Typed markers and bundle artifacts.
2. Canonical invariants (`I1`-`I13`, especially `I9`-`I13`).
3. Contract-health summary.
4. Merge-gate / strict-mode run exit.
5. Pilot-gated default flips so production runs one intended codepath.

## Self-Review Corrections

The earlier closeout list was directionally right but stale in a few places. This section is the codebase-verified correction set.

| Prior assumption | Current codebase evidence | Closeout decision |
|---|---|---|
| `C12-T2` / `GSO_PHASE_H_STRICT_VALIDATION_V1` still needs implementation. | Present: `phase_h_strict_validation_marker` in `optimization/run_analysis_contract.py`, parsed into `MarkerLog.phase_h_strict_validation` in `tools/marker_parser.py`, with unit and integration tests. | Drop as a new plan. Treat as already implemented; only consume it in contract health. |
| Bundle assembly producers are missing wholesale. | Mostly present: `build_decision_trace_all`, `build_journey_validation_all`, `build_scoreboard`, and `build_failure_buckets` exist in `optimization/run_output_bundle.py`. Harness also wires parent-bundle builders. Gap: `assemble_bundle_for_replay` returns decision trace, journey validation, scoreboard, artifact index, iteration summaries, but does not return `failure_buckets`. | Narrow to a replay-assembler parity closeout, not a full C12-T3 redo. |
| `GSO_CONTRACT_HEALTH_V1` exists or is partly wired. | No marker string found. `contract_health` exists as a run-output stage and operator transcript stage. Invariants strict mode exists in config, but `jobs/run_lever_loop.py` sets `GSO_LOOP_INVARIANTS_STRICT=0` by default for production warn-and-degrade. | Keep as a keystone plan: add health marker, consume existing markers/invariants, and decide the production strict-mode flip explicitly. |
| `GSO_RCA_GROUNDING_PRE_EMIT_GATE` is a staged flag to flip. | No flag or symbol with that name found. RCA grounding exists through invariant `I7` and proposal/patch grounding signals. | Do not include a named flag flip. Close RCA grounding through I7 + contract health/merge gate, unless a future implementation introduces an actual flag. |
| Default-flip backlog includes five known flags. | Verified default-off flags: `GSO_PARTIAL_HARVEST_WITH_DEBT`, `GSO_PATCH_SUBSET_ISOLATION`, `GSO_L6_NARROW_REPLACEMENT_BRANCH_C`, `GSO_JOURNEY_PRODUCER_STRICT`. `GSO_LOOP_INVARIANTS_STRICT` is default-on in config but overridden to `0` by the lever-loop job. | Scope default-flip work to the four behavior flags plus the lever-loop strict-mode override. |
| Acceptance-object duplication is hypothetical. | Three distinct types exist: `acceptance_policy.AcceptanceDecision`, `control_plane.ControlPlaneAcceptance`, and `tools/lever_loop_stdout_parser.AcceptanceDecision`. | Keep as a structural execution-spine closeout plan, sequenced after the enforcement keystone. |
| Stage-6 pure-helper extraction may already be complete. | `optimization/stages/gates.py` explicitly says the real production gate logic remains in `harness.py`; the stage module provides typed surface and minimal field-driven logic for unit tests. | Keep as an execution-spine closeout plan after Cycle 16 lands. |
| Replay-production journey parity may already be enforced. | Shared validation exists, and Cycle 17 applies strict producer logic in replay and shared journey helpers. No dedicated test asserts byte-equal production vs replay journey streams for the same anchor. | Keep as a targeted parity plan. |
| Canonical sort at LLM boundaries is one missing central utility. | Sorting exists in many places (`sort_keys=True`, sorted journey JSON, sorted grounding terms), but there is no single boundary audit. | Keep as a bounded audit, not a new abstraction by default. |

## Closeout Plan List

### RCO-0 — Finish In-Flight Foundation

**Status:** in flight, not a new plan.

**Includes:**

- Cycle 16: L5 narrow replacement / Branch C + I11.
- Cycle 17: journey-validation producer fix + I12.

**Exit criteria:**

- Cycle 16 and Cycle 17 are implemented, tests pass, and their plan docs are updated to `closed-local pending corpus`.
- No new roadmap scope is added until RCO-0 is complete unless it is file-disjoint and already listed below.

### RCO-1 — Bundle Replay-Assembler Parity Closeout

**Status:** closed-local pending corpus — `assemble_bundle_for_replay` now returns `failure_buckets` and is parity-locked against `bundle_artifact_paths` via `tests/unit/test_replay_assembler_parent_bundle_parity.py`. The airline-fixture integration test (`tests/integration/test_bundle_assembler_airline_fixture_replay.py`) extends the same contract to a production-shape fixture when vendored. Corpus confirmation pending the next airline run.

**Why this exists:** C12-T3 producers mostly exist, but replay bundle assembly does not mirror the complete parent bundle if `failure_buckets` is absent from `assemble_bundle_for_replay`.

**Primary files:**

- `optimization/run_output_bundle.py`
- `tests/integration/test_bundle_assembler_airline_fixture_replay.py`
- Existing bundle completeness tests under `tests/integration/`

**Work:**

- Add `failure_buckets` to `assemble_bundle_for_replay`.
- Add a regression test proving the replay assembler returns every parent-level contract artifact that the harness terminate path writes.
- Keep this narrow. Do not redesign the bundle assembler.

**Exit criteria:**

- Replay bundle assembly includes `manifest`, `run_summary`, `decision_trace_all`, `journey_validation_all`, `scoreboard`, `failure_buckets`, `artifact_index`, and iteration summaries.
- Integration test proves production-shape fixtures survive the assembler without `GSO_BUNDLE_ASSEMBLY_FAILED_V1`.

### RCO-2 — Contract Health + Merge Gate Keystone

**Split into two phases (RCO-X-Phase-A / RCO-X-Phase-B shape, matches RCO-4 and RCO-4b).**

- **RCO-2a — Marker + Summary half** — `closed-local pending corpus`.
  Ships ``GSO_CONTRACT_HEALTH_V1`` marker, parser, ``ContractHealthSummary``,
  ``build_contract_health_summary`` pure builder, all three merge-gate
  categories (``healthy`` / ``warn`` / ``merge_gate_blocked``), default-on
  emission flag, harness end-of-run wiring, operator transcript renderer.
  Production posture remains warn-and-degrade. See
  ``2026-05-12-rco-2a-contract-health-marker-and-summary-plan.md``.

- **RCO-2b — Production posture flip** — ✅ landed (2026-05-13).
  Named blocker cleared on the May-12 consolidating trial; two
  captured ``GSO_CONTRACT_HEALTH_V1`` payloads validated the marker
  emission pipeline. The merge-gate enforcement is now wired
  (``enforce_merge_gate(loop_out)`` in ``run_lever_loop.py`` raises
  ``MergeGateBlockedError`` on ``merge_gate_blocked``), and the
  ``GSO_LOOP_INVARIANTS_STRICT=0`` setdefault override is removed.
  See ``2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md``
  and ``2026-05-12-rco-2b-deferral.md`` (Disposition section).

**Why this exists:** The deterministic spine is only useful if a bad run cannot silently pass. Current code has invariant records and strict mode, but no `GSO_CONTRACT_HEALTH_V1` marker and the lever-loop job defaults strict invariants to warn-and-degrade.

**Primary files:**

- `optimization/run_analysis_contract.py`
- `tools/marker_parser.py`
- `optimization/harness.py`
- `jobs/run_lever_loop.py`
- `optimization/invariants.py`
- Contract-health tests under `tests/unit/` and `tests/integration/`

**Work:**

- Add `GSO_CONTRACT_HEALTH_V1` typed marker.
- Parse it into `MarkerLog`.
- Build the health summary from existing evidence:
  - `phase_h_strict_validation`
  - `bundle_assembly_incomplete`
  - invariant violations by canonical ID (`I9`, `I10`, `I11`, `I12`, `I13`)
  - replay validity
  - manifest / bundle completeness
- Add a merge-gate result that distinguishes:
  - `healthy`
  - `warn`
  - `merge_gate_blocked`
- Decide and implement the production behavior for `GSO_LOOP_INVARIANTS_STRICT` in `jobs/run_lever_loop.py`.

**Exit criteria:**

- Every HIGH-tier violation is rendered in the health marker with a specific blocker reason.
- Local/integration tests prove HIGH-tier failures block the run path or produce the agreed `MERGE_GATE_BLOCKED` terminal status.
- The job-level strict-mode default is intentional and documented. No hidden `setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")` contradiction remains.

### RCO-3 — Pilot-Gated Default-Flip Closeout

**Why this exists:** Shipped features behind default-off flags create multiple production spines. The closeout goal is one intended codepath.

**Flags in scope:**

- `GSO_PARTIAL_HARVEST_WITH_DEBT`
- `GSO_PATCH_SUBSET_ISOLATION`
- `GSO_L6_NARROW_REPLACEMENT_BRANCH_C`
- `GSO_JOURNEY_PRODUCER_STRICT`
- `GSO_LOOP_INVARIANTS_STRICT` production/job behavior from RCO-2

**Not in scope:**

- `GSO_RCA_GROUNDING_PRE_EMIT_GATE` unless a real flag with that name is implemented later.

**Work:**

- For each flag, document current default, intended default, pilot evidence required, and rollback.
- Flip only after the relevant corpus or replay evidence exists.
- Prefer one closeout PR with one ledger row per flag.

**Exit criteria:**

- There is no default-off behavior flag remaining for already-shipped spine work unless it has a named owner and a future removal date.
- The iteration ledger states why each default flip is safe.

**2026-05-13 default-on flip (lever-loop-free):**

Three observability-only feature flags landed default-OFF and were silent on
the 2314bb2c trial despite their code being deployed. Flipped to default-ON
via `2026-05-13-feature-flag-default-on-flip-and-phase-h-canonical-consumer-wiring-plan.md`:

- `GSO_PROPOSAL_FAILURE_DECIDED` — Plan P-F producers + iteration coverage invariant.
- `GSO_STAGE4_CONTEXT_PERSISTENCE` — Plan P-G strategist boundary persistence.
- `GSO_PATCH_SUBSET_ISOLATION` — Cycle 14B-T3 diagnostic-only attribution marker.

Each flip preserves a `GSO_*=0` rollback escape hatch (per the canonical
`_flag_default_on` pattern). Out of scope for that plan (deferred to a
post-Phase-0 evidence-bound follow-up):

- `GSO_PARTIAL_HARVEST_WITH_DEBT` — adds new acceptance branch.
- `GSO_PATCH_SUBSET_ISOLATION_LIVE` — performs live re-eval.
- `GSO_L6_NARROW_REPLACEMENT_BRANCH_C` — synthesizes new L5 patches.

**2026-05-13 Phase H canonical-consumer cleanup:**

- `GSO_PHASE_H_CANONICAL_CONSUMER` accessor removed — orphan with zero
  production call sites since C15 Phase 1 Task 1.10 deleted the legacy
  parallel writer. The drift detectors (`detect_phase_h_acceptance_drift`,
  `detect_phase_h_journey_drift`) remain wired and gated by
  `GSO_PHASE_H_DRIFT_OBSERVE`.
- F5 contract bug fixed: the acceptance-path `acceptance_decision` dict in
  `harness.py:16382` now carries an explicit `"accepted": True` field
  (parity with the rejection-path builder at `:16189`). This removes the
  false-positive `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1` markers observed on
  every accepted iteration of the 2314bb2c trial.

**2026-05-13 Phase 1 — acceptance gate redesign (lever-loop-free):**

Phase 0 surfaced four design questions about the partial-harvest acceptance
tier (`2026-05-12-phase-0-offline-acceptance-policy-replay-results.md:45-49`).
The Phase 1 plan
(`2026-05-13-acceptance-gate-redesign-phase-1-plan.md`) lands a sibling
acceptance tier — `accepted_with_attribution_drift_and_debt` — gated by
the new `GSO_ATTRIBUTION_DRIFT_WITH_DEBT` flag (default-OFF).

- Design decisions locked in `2026-05-13-acceptance-gate-redesign-design-record.md`.
- New policy factory `attribution_drift_policy_pilot_default()` with
  `min_target_clusters_fixed=0`, `min_aggregate_improvement_pp=4.0`, and
  debt buckets `{SOFT_TO_HARD, LOOKUP_FAILED}`.
- New branch in `decide_control_plane_acceptance` after the existing
  partial-harvest branch.
- `policy_replay` CLI extended with `--policy-name` argument + registry.
- Phase 0.2 replay produces 3/3 exact matches against the captured fixtures
  (see `2026-05-12-phase-0-offline-acceptance-policy-replay-results.md`
  Phase 0.2 section).

**Remaining work to flip the new flag default-on:**

1. One lever-loop trial with `GSO_ATTRIBUTION_DRIFT_WITH_DEBT=1` against a
   ccf1d60d-shaped anchor — confirm the new reason code appears in the FULL
   EVAL banner and `GSO_FULL_EVAL_V1` marker.
2. Replay-parity check against captured fixtures (RCO-6 input).
3. Follow-up plan flipping the accessor body to `_flag_default_on`.

`GSO_PARTIAL_HARVEST_WITH_DEBT` remains deferred — Phase 0 showed it is
mis-shaped for the corpus; the partial-harvest tier needs its own pilot
policy update before flipping. The attribution-drift tier does not change
that conclusion; it is a sibling, not a substitute.

**2026-05-13 Phase 3 — directive-to-proposal obligation (lever-loop-free):**

Adds a per-(ag_id, lever_key) outcome ledger so every directive an AG carries
maps to exactly one closed-vocabulary `DirectiveOutcomeCode`. Closes the
silent-AG-budget-burn pattern from 2314bb2c iter 2-5 where AG2 had L5/L6
directives that produced zero proposals with no per-lever attribution in
the trace.

- New closed vocabulary (6 entries): `proposal_emitted`,
  `no_structural_candidate`, `force_llm_declined`, `applyability_rejected`,
  `collateral_rejected`, `lever_not_proposal_generating`.
- New stdout marker `GSO_DIRECTIVE_OUTCOME_V1` (per AG per iteration).
- New invariant `check_directive_outcome_coverage` (warn-and-degrade).
- New flag `GSO_DIRECTIVE_OUTCOME_COVERAGE` (default-ON; falsy rollback).
- Distinct from P-F: P-F is iteration-level + recovery-decision; Phase 3 is
  per-AG-per-directive + attribution-only. The two invariants run
  independently; neither subsumes the other.

**Unblocks Phase 2** (recovery dispatcher): Phase 2's per-directive recovery
actions read this plan's closed vocabulary. Phase 2 is a separate plan.

**Enforcement (raise/block) deferred** to a follow-up plan after one trial
confirms zero false positives on the corpus.

**2026-05-13 Phase 3 followup — precise L6 attribution (lever-loop-free):**

Closes the executor's documented structural uncertainty from Phase 3 Task 8.
The shipped Task 8 wiring used conservative-zero values for
`force_llm_declined` / `applyability_drop_count` / `collateral_drop_count`
because the named `optimizer._*` accumulators do not exist in the codebase.
That made the classifier emit `NO_STRUCTURAL_CANDIDATE` for every
zero-proposal L6 lever — including the AG2 case where the L6 force-LLM
declined.

- One pure helper `reconcile_outcome_from_records` in `directive_outcome.py`
  reads the canonical `lever6_force_llm_declined` signal from
  `iter_inputs["decision_records"]` and upgrades the classifier's
  `NO_STRUCTURAL_CANDIDATE` to `FORCE_LLM_DECLINED` when the
  `(ag_id, iteration)` matches.
- Harness wiring runs reconciliation after the per-lever loop and before
  the `GSO_DIRECTIVE_OUTCOME_V1` marker emits. Pure, observability-only.
- Real L5 `structural_gate_drop_count` is now sourced from
  `optimizer.get_lever5_gate_drops()` (informational; does not change
  classification).
- 13 new tests (10 helper unit + 3 production-path integration).

**Still deferred** (separate follow-up plan):
- Per-cap-loop applyability/collateral attribution. The cap loop pools
  proposals across levers and per-drop records do not carry an
  `originating_lever_key` tag today. Precise attribution there requires
  stamping `_originating_lever_key` on each proposal inside
  `generate_proposals_from_strategy` and reading the stamp at the cap-loop
  drop-record build site. Producer-side refactor, larger scope.

**2026-05-13 B2 — DecisionRecord invariant input shape (lever-loop-free defect fix):**

Closes 18 medium-tier `I_CHECK_FAILED` violations on 2314bb2c iter 1 where
`check_i7_rca_grounding` and `check_i14_l6_decline_dedup` raised
`AttributeError("'DecisionRecord' object has no attribute 'get'")` because
some emit sites append the dataclass directly to
`current_iter_inputs["decision_records"]` while invariants assume Mapping.

- One pure helper `_record_to_mapping` in `invariant_projection.py`
  normalizes any record (dataclass with `to_dict()`, Mapping, or other) to
  a dict; non-coercible entries are dropped silently.
- One wire-site change in `_project_iteration` so every record reaches the
  invariants as a dict.
- No invariant body changes. No producer-side changes. Strictly additive
  defense at the projection boundary.
- 16 new tests (8 helper + 5 projection + 3 integration) including a
  2314bb2c-shaped end-to-end assertion that `I_CHECK_FAILED` count is zero.

**Companion:** `2026-05-13-b5-replay-fixture-attribution-drift-emission-plan.md`
addresses the related dataclass-vs-dict mismatch in the replay-fixture
serializer.

**2026-05-13 B5 — replay-fixture attribution-drift emission (RCO-6 input):**

Closes the 2314bb2c gap where the lever loop completed
`READY_TO_MERGE_WITH_ATTRIBUTION_DRIFT` but Phase H wrote
`replay_fixture.json = {}` and stderr `PHASE_A_REPLAY_FIXTURE_JSON` markers
were absent. Root cause: a `DecisionRecord` dataclass in
`iterations_data[i]["decision_records"]` made `_strip_dict`'s `k in d`
check raise `TypeError`, which the harness's outer try-except swallowed
silently, producing an empty fixture.

Three changes:

1. `_coerce_record_to_dict` helper in `journey_fixture_exporter.py`
   normalizes dataclass / Mapping / other to dict (mirrors B2's
   `_record_to_mapping`).
2. Per-iteration try/except wrapper in `_build_fixture` — one malformed
   iteration cannot bring down the whole serialization.
3. New diagnostic marker `GSO_REPLAY_FIXTURE_EMPTY_V1` emitted from the
   Phase A block when `iterations_data` had entries but the resulting
   fixture is semantically empty (zero iterations or any iteration with
   zero `eval_rows`).

Test coverage: 17 tests (5 strip-iteration + 5 resilience + 3 marker +
4 integration). The integration test replays the 2314bb2c iter 1 shape
(accepted-with-attribution-drift + real DecisionRecord in decision_records)
and asserts the fixture is non-empty and the emptiness marker does NOT fire.

**Companion:** `2026-05-13-b2-decisionrecord-invariant-input-shape-plan.md`
fixes the related dataclass-vs-dict mismatch in the invariant projection.

**Unblocks RCO-6** (replay/journey parity closeout): with the fixture now
reliably non-empty on attribution-drift runs, the next layer of work — a
`bundle://<run_id>` round-trip assertion through `tools/evidence_bundle.py`
— can begin. Tracked separately under RCO-6.

### RCO-4 — Stage-6 Gate Pure-Helper Extraction

**Status:** closed-local pending corpus — three of six conceptual gates extracted into pure helpers in ``optimization/stages/gates.py`` (``run_blast_radius_production_gate``, ``resolve_narrow_replacement``, ``run_applyability_gate``) behind default-off flags ``GSO_STAGE6_BLAST_RADIUS_PURE`` / ``GSO_STAGE6_NARROW_REPL_PURE`` / ``GSO_STAGE6_APPLYABILITY_PURE``. The remaining three (alignment / reflection / cap) are deferred to RCO-4b with named blockers documented in ``docs/2026-05-11-rco-4-deferred-gates.md`` and a full gate-to-code mapping in ``docs/2026-05-11-rco-4-gate-inventory.md``. Production firing order pinned by ``tests/unit/test_rco4_sequencing_grep_guard.py``. Parity fixtures under ``tests/unit/fixtures/rco4/``. Corpus confirmation + simultaneous flag flip pending the next airline + 7Now run (the flag flip belongs in RCO-3's pilot batch).

**Why this exists:** `optimization/stages/gates.py` documents that the production gate logic still lives in `harness.py`. Stage 6 remains the most complex deterministic region and should be gate-by-gate testable.

**Primary files:**

- `optimization/harness.py`
- `optimization/stages/gates.py`
- New or extended pure helper modules under `optimization/stages/`
- Gate-focused unit tests
- C15-style boundary fixtures for gate inputs/outputs

**Work:**

- Extract the production gate sequence into pure helpers with typed input/output contracts.
- Cover at least:
  - blast-radius gate
  - applyability gate
  - alignment gate
  - reflection gate
  - cap gate
  - narrow-replacement / Branch C gate
- Add a sequencing test that proves the gate order is stable.
- Add fixture tests for the production shapes that motivated Cycle 16.

**Exit criteria:**

- Stage 6 can be reasoned about as a pipeline of typed pure helpers.
- Harness orchestration is thin and mostly passes typed inputs to helpers.
- A failing patch can be traced to exactly one gate decision and one typed reason.

### RCO-4b — `_run_gate_checks` Decomposition (Phased)

**Status:** `landed` — all five phases A–E shipped. Every conceptual gate-stage inside `_run_gate_checks` has a pure-helper extraction in `optimization/stages/eval_gates.py`.

**Phase A plan:** `docs/2026-05-12-rco-4b-phase-a-run-gate-checks-decomposition-plan.md`.

**Phase B plan:** `docs/2026-05-12-rco-4b-phase-b-slice-gate-extraction-plan.md`.

**Phase C plan:** `docs/2026-05-12-rco-4b-phase-c-p0-gate-extraction-plan.md`.

**Phase D plan:** `docs/2026-05-12-rco-4b-phase-d-asi-extraction-and-baseline-drift-plan.md`.

**Phase E plan:** `docs/2026-05-12-rco-4b-phase-e-full-eval-acceptance-plan.md`.

**Trial submission plan:** `docs/2026-05-13-rco-4b-consolidating-trial-submission-plan.md`.

**Trial runbook (long-lived):** `docs/2026-05-13-rco-4b-trial-runbook.md`.

**Trial status:** submitted 2026-05-12 against two substitute anchors (`airline_trial_2026_05_12_31ecd96f`, `seven_now_trial_2026_05_12_ccf1d60d`); F9-3b050ec5 / AIRLINE-clean not yet captured. Postflight test passes 8/8 against both runs. Marker infrastructure validated. Two MERGE_GATE_GAP defects surfaced (orthogonal to RCO-4b extraction work):

- `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md` (NO_APPLIED_PATCHES; airline run).
- `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md` (NO_ACCEPTED_PROGRESS; 7now run; contains the named RCO-6 `gs_021` carry-over fix).

Deferred-RCO unblocking from this trial: RCO-2b ✅ (landed 2026-05-13; see `docs/2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md`), RCO-3 ✅, RCO-4c ⚠️ partial, RCO-6 ❌ blocked on the second defect plan. Re-trial against the original F9/AIRLINE anchors expected after the defect plans land. Full disposition in `docs/2026-05-13-rco-4b-trial-runbook.md` "Trial disposition (2026-05-12)" section.

- **RCO-2b (2026-05-13):** Production posture flipped. Merge gate raises ``MergeGateBlockedError`` on ``merge_gate_blocked``; ``GSO_LOOP_INVARIANTS_STRICT=0`` override removed from ``run_lever_loop.py``. Two captured trial payloads promoted to byte-stable fixtures under ``tests/unit/fixtures/rco2b/``.

### Re-trial unblock progress (2026-05-12)

| Defect plan | Plan file | Trial-blocking | Status |
|---|---|---|---|
| Defect 1 — AG grounding + cluster-signature admission | `docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md` | Yes | ✅ Landed (2026-05-12) |
| Defect 2 — Stable retry signature for no-progress iterations | `docs/2026-05-12-defect-no-applied-patches-retry-signature-plan.md` | Yes | ✅ Landed (2026-05-12) |
| Defect 3 — RCO-6 carve-out (gs_021 clustered → soft_signal) | `docs/2026-05-12-defect-gs021-journey-producer-strict-default-flip-plan.md` | No (blocks RCO-6, not the re-trial) | ✅ Landed (2026-05-12) |
| Bundle-status wiring fix (micro-plan) | `docs/2026-05-12-bundle-status-wiring-fix-plan.md` | No (de-risks RCO-2b) | ✅ Landed (2026-05-12) |

Re-trial against F9-3b050ec5 + AIRLINE-clean is now unblocked (Defect 1 + Defect 2 landed; bundle-status fix landed; RCO-2b strict-mode flip landed). Re-trial submission is the next operator action.

- **Bundle-status wiring fix (2026-05-12) — landed.** `contract_health.bundle_status` now reflects `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` and `GSO_BUNDLE_ASSEMBLY_FAILED_V1` markers emitted in the same run. Closes the contradiction surfaced by the May-12 trial (both runs reported `bundle_status="complete"` while `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` reported `missing_count=40`). Incidental win: `_phase_h_marker_payload` is now also visible to the relocated emission, so `phase_h_listing_status` / `phase_h_validator_status` will reflect actual Phase H state instead of always reporting `skipped`. See `docs/2026-05-12-bundle-status-wiring-fix-plan.md`.

- **Run-end replay-fixture validation wired (2026-05-12) — landed.** The post-Phase-H `GSO_CONTRACT_HEALTH_V1` marker now reports `replay_is_valid` / `replay_violation_count` based on `run_replay(serialized_fixture)` inside the existing Phase A try block. Closes the last `read-locals-before-assigned` surface from the bundle-status wiring fix's out-of-scope table. RCO-6's journey-replay defect carve-out gets a stable end-of-run measurement to track regressions against. See `docs/2026-05-12-run-end-replay-validation-plan.md`.

**Phase roadmap:** `docs/2026-05-12-rco-4b-phase-roadmap.md`.

**Stage inventory:** `docs/2026-05-12-rco-4b-gate-stage-inventory.md`.

**Phases:**
- A — Foundation + propagation_wait extraction. ✅ landed.
- B — Slice gate extraction. ✅ closed-local pending corpus
  (three pure helpers: `decide_slice_gate_should_run`,
  `compute_slice_gate_effective_tolerance`,
  `decide_slice_gate_post_eval`; default-off behind
  `GSO_GATE_CHECKS_SLICE_PURE`).
- C — P0 gate extraction. ✅ closed-local pending corpus
  (two pure helpers: `decide_p0_gate_should_run`,
  `decide_p0_gate_post_eval`; default-off behind
  `GSO_GATE_CHECKS_P0_PURE`).
- D — ASI extraction forwarder + baseline-drift diagnostic. ✅
  closed-local pending corpus (two observability-only pure helpers:
  `forward_asi_extraction_audit`, `build_baseline_drift_diagnostic`;
  default-off behind `GSO_GATE_CHECKS_ASI_EXTRACTION_PURE` and
  `GSO_GATE_CHECKS_BASELINE_DRIFT_PURE`).
- E — Full-eval-acceptance verdict extraction. ✅ closed-local
  pending corpus (one pure helper: `decide_full_eval_acceptance`;
  default-off behind `GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE`;
  three audit-emission sites pinned by sequence-guard).
- F (RCO-4c) — Unblock RCO-4 deferred gates. Pending.

**RCO-4b consolidating trial:** With every gate-stage now extracted,
the next milestone is a single consolidating trial run that enables
all six gate-flags simultaneously and captures
`GSO_CONTRACT_HEALTH_V1`, audit rows, journey events, and bundle
artifacts. The trial's outputs simultaneously unblock RCO-2b
(strict-mode default-flip), RCO-3 (pilot-gated default-flip),
RCO-4c (deferred-gate unblock), and RCO-6 (replay/production
parity).

**Closeout signal:** RCO-4b is "closed-local pending corpus" when Phases A-E have all landed and the sequence-guard test passes with every per-stage feature flag flippable to true with parity verified.

**Why this exists:** RCO-4 extracted three of six conceptual gates. The remaining three (alignment, reflection, cap) are entangled with the 800-line ``_run_gate_checks`` function in ``optimization/harness.py``. Decomposing that function is its own multi-task plan; doing it inside RCO-4 would have pushed RCO-4 past a single-PR scope.

**Primary files:**

- ``optimization/harness.py:_run_gate_checks`` (line ~12732, ~800 LOC)
- ``optimization/cumulative_regression_debt.py``
- ``optimization/reflection_retry.py``
- Existing eval-acceptance / debt-cap tests

**Work:**

- Extract slice / P0 / full-eval-acceptance / pre-arbiter-regression / baseline-drift gates into typed pure helpers using ``predict_fn`` / ``scorers`` as injected dependencies.
- Once decomposed, RCO-4's deferred gates (alignment, reflection, cap) become directly addressable.

**Exit criteria:**

- ``_run_gate_checks`` body is reduced to typed orchestration + helper dispatch.
- Alignment, reflection, and cap gates can be added to the RCO-4 pattern without re-entangling ``_run_gate_checks``.

**Scheduling note:** Not in Tier 2. RCO-4b runs after the keystone (RCO-2) lands, because the alignment gate's halting decision should be readable from the contract-health summary.

### RCO-5 — Acceptance Object Consolidation

**Status:** closed-local pending corpus — ``optimization.control_plane.ControlPlaneAcceptance`` is canonical; ``optimization.acceptance_policy.AcceptanceDecision`` renamed to ``GainGateDecision``; ``tools.lever_loop_stdout_parser.AcceptanceDecision`` renamed to ``ParsedAcceptanceView`` with new ``parsed_view_to_control_plane`` projection. Structural guard at ``tests/unit/test_rco5_acceptance_structural_guard.py``. Policy doc at ``docs/2026-05-11-rco-5-acceptance-consolidation-policy.md``. I9 still the runtime render-drift guard. No semantic changes; corpus confirmation pending the next airline + 7Now run.

**Why this exists:** Stage 9 still has multiple acceptance representations. I9 catches render drift, but drift should be structurally hard to create.

**Primary files:**

- `optimization/control_plane.py`
- `optimization/acceptance_policy.py`
- `tools/lever_loop_stdout_parser.py`
- Acceptance render tests
- I9 invariant tests

**Work:**

- Make `ControlPlaneAcceptance` the canonical runtime decision object unless implementation review finds a better owner.
- Convert `acceptance_policy.AcceptanceDecision` into a narrow gain-gate helper or adapter.
- Convert `tools/lever_loop_stdout_parser.AcceptanceDecision` into a parsed view that explicitly maps to the canonical shape instead of owning its own semantics.
- Keep I9 as a regression guard.

**Exit criteria:**

- Only one object owns acceptance semantics.
- Stdout, replay, Phase H, and decision records are views over the same canonical fields.
- A new acceptance bucket can be added in one semantic location, not three.

### RCO-6 — Replay/Production Journey Producer Parity

**Why this exists:** Replay validity is only useful if replay and production produce equivalent journey streams for the same inputs.

**Primary files:**

- `optimization/lever_loop_replay.py`
- `optimization/question_journey.py`
- `optimization/harness.py`
- New integration test under `tests/integration/`
- Existing replay fixtures under `tests/replay/fixtures/`

**Work:**

- Pick one anchor fixture after Cycle 17 lands.
- Drive the replay producer and the production journey emit path with equivalent inputs.
- Compare canonical journey JSON, or compare a stable normalized event list if direct byte equality is too brittle.

**Exit criteria:**

- There is one test that fails if replay and production disagree on journey events for the same anchor.
- Any intentional difference is explicitly documented as a view-layer difference, not an implicit producer mismatch.

### RCO-7 — Canonical Sort at LLM-to-Deterministic Boundaries

**Status:** closed-local pending corpus — four LLM ingestion boundaries (strategist response, AG stage, proposal/patch selection, arbiter verdict) now route through ``optimization/llm_boundary_sort.py`` with canonical sort-by-key. Shuffle-equivalent regression tests cover each site (``tests/unit/test_rco7_*``) plus a harness-side grep guard. Corpus confirmation pending the next airline run.

**Why this exists:** LLM outputs may be semantically identical but list-ordered differently. Deterministic stages should not depend on incidental LLM ordering.

**Primary files:**

- Strategist-response ingestion in `optimization/harness.py`
- Proposal and patch selection call sites
- Judge / arbiter verdict ingestion call sites
- Existing canonical JSON helpers and tests

**Work:**

- Audit each LLM output boundary.
- Add a stable sort by canonical key where ordering should not matter:
  - AG ID
  - proposal ID / expanded patch ID
  - QID
  - lever ID
  - reason code
- Add tests proving shuffled equivalent input produces the same deterministic output.

**Exit criteria:**

- Reordering equivalent LLM outputs does not change deterministic-stage results.
- No new global abstraction is introduced unless the audit finds meaningful duplication.

### RCO-8 — Sub-Stage Production-Shape Boundary Fixtures

**Status:** closed-local pending corpus — production-shape fixture pairs are pinned for the three C14-V / C14-W anchored helpers (`_normalize_stage_capture`, `_databricks_ids_from_env`, `parse_markers`) under `tests/unit/fixtures/rco8/`. Floor tests in each of `tests/unit/test_rco8_*_production_fixtures.py` enforce the minimum-case set. No production code changed. Corpus confirmation pending the next airline run.

**Why this exists:** Cycle 14-V showed fixes passing unit tests while failing in production because synthetic fixtures did not match production shapes.

**Primary targets:**

- Helpers involved in C14-V D-4 / D-5 style regressions:
  - stage-capture normalization
  - Databricks ID resolution
  - marker payload normalization
  - any helper that reads dict-or-list production captures

**Work:**

- Add helper-level `input.json` / `expected_output.json` fixtures for production shapes below the stage boundary.
- Keep these fixtures small and anchored to real-run evidence.
- Do not add new behavior unless a fixture fails.

**Exit criteria:**

- The known production-shape regression pattern is covered below the stage level.
- A helper cannot be marked closed-local without a production-shape fixture when its defect came from production-shape mismatch.

### RCO-9 — Final Closeout Audit

**Why this exists:** After the execution and process spines are closed, run one audit plan to remove stale roadmap entries, dead flags, and obsolete transitional docs.

**Work:**

- Update `2026-05-07-contract-spirit-compliance-roadmap.md`.
- Update the optimizer iteration ledger.
- Mark which flags are production-locked, which are removed, and which are explicitly deferred.
- Confirm no pending "gap" is just a stale doc statement.

**Exit criteria:**

- Roadmap says exactly what remains, if anything.
- No "pending" item exists without an owner, trigger, and target artifact.
- Prompt/model optimization can start without open deterministic-spine ambiguity.

## Execution Order

### Tier 0 — Already In Flight

1. RCO-0: Finish Cycle 16 and Cycle 17.

### Tier 1 — Can Draft/Execute Now

2. RCO-1: Bundle replay-assembler parity closeout. ✅ closed-local pending corpus.
3. RCO-7: Canonical sort audit at LLM boundaries. ✅ closed-local pending corpus.
4. RCO-8: Sub-stage production-shape boundary fixtures. ✅ closed-local pending corpus.

These are relatively low-conflict and do not require the contract-health keystone to exist first.

### Tier 2 — After Cycle 16/17 Land

5. RCO-4: Stage-6 gate pure-helper extraction. ✅ closed-local pending corpus (3 of 6 conceptual gates; alignment/reflection/cap deferred to RCO-4b).
6. RCO-6: Replay/production journey producer parity.

These should wait until Cycle 16/17 finish because they overlap with the gate and journey surfaces those cycles are actively changing.

### Tier 3 — Keystone

7. RCO-2: Contract health + merge gate keystone.
   - RCO-2a (marker + summary half): ✅ landed (2026-05-12).
   - RCO-2b (production posture flip): ✅ landed (2026-05-13) — named blocker cleared on the May-12 consolidating trial; merge-gate enforcement wired and STRICT=0 override removed.

This should land after Cycle 17's I12 and RCO-1's bundle parity are available, because it consumes both.

### Tier 4 — Pilot-Gated Closeout

8. RCO-3: Default-flip closeout.

This follows the keystone and corpus evidence. Do not flip defaults speculatively.

### Tier 5 — Structural Simplification + Final Audit

9. RCO-5: Acceptance object consolidation. ✅ closed-local pending corpus.
10. RCO-9: Final closeout audit.

Acceptance consolidation can happen before the keystone if execution capacity is available, but it is safest after the health/merge-gate contract is stable because it touches Stage 9 semantics.

## Scope Control Rules

1. No new LLM prompt/model optimization work enters this closeout.
2. No new heuristic detector enters this closeout unless it maps to one of the RCO plans above and has a failing fixture or pilot finding.
3. No new behavior flag enters this closeout unless it has:
   - default state,
   - pilot trigger,
   - rollback plan,
   - ledger row,
   - explicit removal or production-lock target.
4. If a gap is only observable in docs but not in code, update the docs rather than adding implementation scope.
5. Every closeout plan must name which spine it closes:
   - execution spine,
   - process spine,
   - or both.

## Milestones

### Milestone A — Process Spine Complete

Required:

- RCO-0
- RCO-1
- RCO-2
- RCO-3

Result:

- HIGH-tier invariant and contract-health violations cannot silently pass.
- Production has one intended strictness posture.

### Milestone B — Execution Spine Deterministic-Complete

Required:

- RCO-4
- RCO-5
- RCO-6
- RCO-7
- RCO-8

Result:

- Stage 6 is gate-by-gate testable.
- Stage 9 has one canonical acceptance object.
- Replay and production journey streams have parity coverage.
- LLM-output ordering no longer affects deterministic results.
- Production-shape helper fixtures prevent the C14-V regression pattern.

### Milestone C — Project Closeout

Required:

- RCO-9

Result:

- Roadmap and ledger reflect the actual current state.
- Remaining work, if any, is explicitly outside deterministic-spine closeout.
- The project is ready to shift focus to LLM prompt/model optimization.

## Recommended Next Plan

Draft and execute **RCO-1 — Bundle Replay-Assembler Parity Closeout** next.

Reasoning:

- It is small and codebase-verified.
- It corrects the stale "C12-T3 is missing" assumption without reopening a broad bundle-assembly plan.
- It feeds the contract-health keystone with reliable parent-bundle evidence.
- It has low overlap with Cycle 16 and Cycle 17.

