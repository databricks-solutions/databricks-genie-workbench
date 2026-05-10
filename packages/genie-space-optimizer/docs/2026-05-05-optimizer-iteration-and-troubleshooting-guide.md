# Iterating, Troubleshooting, and Improving the Optimizer

> **Audience:** maintainers and contributors working on the Genie Space Optimizer (GSO) lever loop after the burn-down-to-merge roadmap has fully landed (Phases A → H, including v2.1 wire-up and the Phase H completion plan).
>
> **Goal:** explain the dev-loop you should run when a real run misbehaves, and where to make changes for each class of improvement, so the optimizer stays tunable without breaking byte-stability or its self-describing artifact contract.
>
> **Pairs with:** `docs/2026-05-01-burn-down-to-merge-roadmap.md` (the strategic narrative), `docs/2026-05-07-contract-spirit-compliance-roadmap.md` (the post-merge compliance roadmap and **canonical Defect Registry** for shipped-but-silent defects D-1 → D-N), `docs/2026-05-05-optimizer-iteration-ledger.md` (the per-cycle behavioral record), and the three skills under `docs/skills/` (the operational runbooks).
>
> **Defect Registry pointer.** When a postmortem encounters a known shipped-but-silent defect class (e.g. C13 admission predicate inert under flag-default-off, or a regressed defect that previously closed), do NOT diagnose it from first principles — look it up in the [Defect Registry](./2026-05-07-contract-spirit-compliance-roadmap.md#defect-registry--single-source-of-truth-for-shipped-but-silent-defects). Each defect ID (D-1 → D-8 as of 2026-05-09 #4) carries Anchor / Root-cause-class / Closing-cycle / Status / Regression-rail-marker columns. Section 2.3 below ("classic symptom → likely stage map") includes one row per registered defect. Section 3.K covers the "regressed defect" diagnostic protocol introduced 2026-05-09 #4 (Discipline A) and the "multi-path resolver" protocol (Discipline B).

---

## 1. Mental Model

After Phase H landed, the lever loop has a single canonical shape that every artifact, test, and skill is built around.

### 1.1 The 9-stage pipeline (per iteration)

The 11-section process order from `optimization/run_output_contract.py:53` is what an operator sees in the transcript. Two of those sections (`post_patch_evaluation`, `contract_health`) reuse other stages or are informational; the **9 executable stages** each have a typed module under `optimization/stages/`:

| # | Process Stage | Module | Role |
|---|---|---|---|
| 1 | `evaluation_state` | `stages/evaluation.py` | Run baseline / per-iteration full eval, classify rows |
| 2 | `rca_evidence` | `stages/rca_evidence.py` | Build per-qid evidence dict from judge + ASI metadata |
| 3 | `cluster_formation` | `stages/clustering.py` | Group failing qids into causal clusters |
| 4 | `action_group_selection` | `stages/action_groups.py` | Strategist picks AGs (cluster scope + lever mix) |
| 5 | `proposal_generation` | `stages/proposals.py` | Concrete candidate patches |
| 6 | `safety_gates` | `stages/gates.py` | Lever-5 / groundedness / blast-radius / dedup / DOA |
| 7 | `applied_patches` | `stages/application.py` | Patches actually applied to the candidate space |
| 8 | `post_patch_evaluation` | re-uses `stages/evaluation.py` | Re-evaluate the patched space |
| 9 | `acceptance_decision` | `stages/acceptance.py` | Control plane decides accept vs. rollback |
| 10 | `learning_next_action` | `stages/learning.py` | Retire AGs, record dedup signatures, set next action |
| 11 | `contract_health` | (informational) | Renderer-only summary section |

Every stage module exports the same four symbols (verified by `tests/unit/test_stages_protocol_conformance.py`):

- `INPUT_CLASS` — the typed input dataclass
- `OUTPUT_CLASS` — the typed output dataclass
- `STAGE_KEY` — string matching `PROCESS_STAGE_ORDER`
- `execute = <named_verb>` — the `(ctx, inp) -> out` callable the registry imports

### 1.2 The single trace model

There is one canonical container for everything the optimizer decided. Stop thinking about "stdout" and "logs" and "MLflow tags" as separate sources of truth.

```
                            OptimizationTrace
                            (rca_decision_trace.py:297)
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
              ▼                      ▼                      ▼
       journey_events         decision_records       validation_by_iteration
       (qid lifecycle)        (typed choices)         (contract health)
              │                      │                      │
              └──────────────────────┼──────────────────────┘
                                     ▼
                       Deterministic projections:

           operator_transcript.md   ←  operator_process_transcript.py
           decision_trace_all.json  ←  rca_decision_trace.canonical_decision_json
           replay_fixture.json      ←  lever_loop_replay
           scoreboard.json          ←  scoreboard.compute_scoreboard
           failure_buckets.json     ←  failure_bucketing classifier
```

**Implication for improvement work:** new observability is always a typed field on a dataclass + a renderer / classifier line. Never an ad-hoc print or a side-channel log. If the data isn't on `OptimizationTrace`, the postmortem skill cannot see it.

### 1.3 The self-describing bundle

Every lever loop emits `gso_postmortem_bundle/` to MLflow under the parent anchor run, organized as:

```text
gso_postmortem_bundle/
├── manifest.json              ← iteration_count, stage_keys_in_process_order, missing_pieces
├── operator_transcript.md     ← human-readable, mirrors PROCESS_STAGE_ORDER
├── artifact_index.json        ← marker payload for skill retrieval
├── decision_trace_all.json    ← run-level canonical decisions
├── journey_validation_all.json
├── scoreboard.json
├── failure_buckets.json
├── replay_fixture.json
└── iterations/
    └── iter_NN/
        ├── operator_transcript.md
        ├── decision_trace.json
        ├── journey_validation.json
        └── stages/
            ├── 01_evaluation_state/{input,output,decisions}.json
            ├── 02_rca_evidence/{input,output,decisions}.json
            ├── 03_cluster_formation/...
            ├── ...
            └── 10_learning_next_action/...
```

Every per-stage subdirectory carries three files:

- `input.json` — the stage's typed `Input` dataclass, serialized
- `output.json` — the stage's typed `Output` dataclass, serialized
- `decisions.json` — `DecisionRecord`s emitted via `ctx.decision_emit` during the stage's execution

This is what `stage_io_capture.wrap_with_io_capture` produces per call (see `optimization/stage_io_capture.py:83`). The decorator never raises — if MLflow is unavailable, the stage runs, the harness continues, and the missing artifact appears in `manifest.missing_pieces`.

### 1.4 The RCA-Grounded Decision Invariant

Every `DecisionRecord` (`rca_decision_trace.py:165`) must carry the chain:

```text
evidence_refs → rca_id, root_cause → target_qids → expected_effect
  → (gate dispatch) → applied/skipped outcome → observed_effect → next_action
```

If any link is absent, the record must carry a typed `reason_code` explaining why. This is the contract that makes the optimizer tunable even when LLM/Genie behavior is probabilistic. Replay (`tests/replay/test_lever_loop_replay.py`) and decision-trace cross-projection tests assert this every CI run.

---

## 2. The Standard Troubleshooting Loop

When a real run misbehaves (low accuracy, weird drops, regression), follow this sequence. Skills are designed to be invoked in order; you do not need to memorize the underlying CLIs.

### Step 1 — `gso-postmortem` (entry point, read-only)

`docs/skills/gso-postmortem/SKILL.md` is the single user-facing entry point. Operator gives `(job_id, run_id)`; the skill:

1. Builds an evidence bundle via `python -m genie_space_optimizer.tools.evidence_bundle ...`. Output lives at `docs/runid_analysis/<opt_run_id>/` (gitignored).
2. Hands off to `gso-lever-loop-run-analysis` (the read-only analysis skill) which writes `postmortem.md` + `postmortem.json` under the same directory.
3. Reports a verdict: `READY_TO_MERGE` / `PILOT_NEEDS_RERUN` / `MERGE_GATE_GAP` / `BASELINE_REGRESSION` / `INSUFFICIENT_EVIDENCE`.

**Always start by reading `gso_postmortem_bundle/manifest.json`, never raw stdout.** The manifest is the index; per-stage I/O is the evidence.

### Step 2 — narrow to the stage at fault

For each iteration `iter_NN`, the bundle gives you nine `stages/<NN>_<key>/` subdirectories. The diagnostic question is always:

> "Which stage's `output.json` first diverged from what the next stage needed?"

Two attribution patterns:

1. **Cross-iteration drift.** Compare `iter_(N-1)/stages/<X>/output.json` to `iter_N/stages/<X>/output.json`. Surfaces "stage X's behavior changed between iteration N-1 and N — why?"
2. **Within-iteration handoff.** Compare `iter_N/stages/<X>/output.json` to `iter_N/stages/<X+1>/input.json`. The downstream input may add fields, but every field that flows through must match.

### Step 3 — classic symptom → likely stage map

| Symptom in the postmortem | Likely stage at fault | First file to read |
|---|---|---|
| Hard failures unchanged across all iterations, no proposals | `stages/proposals.py` (`PROPOSAL_GAP`) | `iter_NN/stages/05_proposal_generation/output.json` |
| Proposals generated but all dropped | `stages/gates.py` (`GATE_OR_CAP_GAP`) | `iter_NN/stages/06_safety_gates/output.json` (`dropped[*].reason`) |
| Patches applied but accuracy doesn't move | `stages/application.py` or eval re-run drift | `iter_NN/stages/07_applied_patches/output.json` + `08_post_patch_evaluation/` |
| Patches applied + accuracy moved + still rolled back | `stages/acceptance.py` (control plane policy) | `iter_NN/stages/09_acceptance_decision/decisions.json` |
| Soft signals never become hard / RCA seems empty | `stages/rca_evidence.py` or upstream `stages/evaluation.py` (`EVIDENCE_GAP`) | `iter_NN/stages/02_rca_evidence/output.json` (`per_qid_evidence` empty?) |
| Same proposal repeats across iterations + rolled back | content_fingerprint_dedup or learning gap | `iter_NN/stages/10_learning_next_action/output.json` |
| Cluster surfaces wrong root cause | `stages/clustering.py` + `optimizer.cluster_failures` | `iter_NN/stages/03_cluster_formation/output.json` |
| AG scope too broad / wrong levers | `stages/action_groups.py` strategist | `iter_NN/stages/04_action_group_selection/output.json` |
| **(D-1)** Same AG re-emits with `Proposals (0 total)` across consecutive iters; no `AG_COLLISION_SKIPPED` records | C13 admission predicate inert (flag default-off); **diagnose via** `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` shadow marker | grep `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` in run stdout. Field `suppressed_by_admit_no_action_off=true` ⇒ Cycle 14-V T1 working; promote `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` default-on. |
| **(D-2)** AG rollback `reason_code ∈ {target_fixed_offset_by_regression, rejected_unbounded_collateral}` but no `GSO_PATCH_ISOLATION_DIAGNOSTIC_V1` markers | C14B-T3 orchestrator inert (flag default-off); **diagnose via** `GSO_PATCH_ISOLATION_OBSERVE_V1` shadow marker | grep `GSO_PATCH_ISOLATION_OBSERVE_V1` in run stdout. Field `attribution_status` ∈ {single_patch, no_attribution}; promote `GSO_PATCH_SUBSET_ISOLATION` after corpus measurement validates accuracy. |
| **(D-3)** Same QID rendered two ways in `GSO_FULL_EVAL_V1` payload (e.g. `target_delta_states=[[gs_026, soft_to_hard]]` AND `target_still_hard_qids=gs_026`) | Canonical render parallel-derivation bug | `optimization/control_plane.py:format_full_eval_marker_payload`; closed by Cycle 14-V T3. Regression rail: `GSO_CANONICAL_RENDER_INVARIANT_V1` (silent on clean payloads). **D-3 ext (2026-05-09 #4):** `target_delta_states=[[qid, soft_passing]]` while `target_fixed_qids=[]` AND `target_still_hard_qids=[]` (qid invisible in any bucket field). Closes in Cycle 14-W T1 — render `target_soft_passing_qids` as a first-class field. |
| **(D-4)** `GSO_BUNDLE_ASSEMBLY_FAILED_V1` with `AttributeError: 'list' object has no attribute 'get'` | Bundle assembler missing list normalization | `optimization/run_output_bundle.py:_normalize_stage_capture` (Cycle 14-V T5 unit-tested only — **regressed in production**, anchor #8 F7). Diagnostic: `GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1`. Closing again in **Cycle 14-W T2** with full call-site coverage audit + airline-fixture-replay integration test (Discipline A). |
| **(D-5)** `GSO_RUN_MANIFEST_V2.databricks_job_id` / `lever_loop_task_run_id` blank | Manifest emission site doesn't read environment | `optimization/harness.py:_databricks_ids_from_env()` (Cycle 14-V T6 unit-tested only — **regressed in production**, both anchors #7 + #8 F8). Sentinel `'unknown'` after fix; never blank. Closing again in **Cycle 14-W T3** with `GSO_DATABRICKS_IDS_RESOLVED_V1` tracing marker + Jobs-runtime integration test (Disciplines A + B). |
| **(D-6)** Phase H `iterations/iter_NN/stages/09_acceptance_decision/output.json:outcome=rolled_back` while stdout `GSO_FULL_EVAL_V1.accepted=true` for the same iteration | Phase H acceptance writer parallel-derivation bug (sibling of D-3 in a different writer) | `optimization/run_output_bundle.py` + `optimization/phase_h_anchor.py` Phase H acceptance writer. Closes in **Cycle 14-W T6** by routing the writer through canonical `ControlPlaneAcceptance`. Regression rail: `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1` (silent on clean writes). |
| **(D-7)** `GSO_PHASE_B_END_V1.iter_record_counts` length ≠ `iteration_counter`; or fewer `GSO_ITERATION_SUMMARY_V1` markers than attempted iterations | Iteration summary emitted only on accepted-iteration code path; should emit per attempted iteration | `optimization/harness.py` iteration-loop trailer (Cycle 14-T1 totality fix didn't cover this cadence). Closes in **Cycle 14-W T5**. Regression rail: `GSO_ITERATION_SUMMARY_TOTALITY_V1` (silent when counts agree). |
| **(D-8)** Local replay reports N journey violations; Phase H `journey_validation_all.json` reports 0 (or vice-versa) | Phase H journey validator parallel-derivation bug (sibling of D-3 in a third writer) | `optimization/run_output_bundle.py` + `optimization/phase_h_anchor.py` journey validator writer. Closes in **Cycle 14-W T6** by routing both validators through a shared `validate_journey()` helper. Regression rail: `GSO_PHASE_H_JOURNEY_DRIFT_V1` (silent when counts agree). |

### Step 4 — `failure_buckets` taxonomy

`optimization/failure_bucketing.py:25` defines seven buckets. Every unresolved qid in the final iteration carries a `ClassificationResult` with `bucket` + `evidence_record_ids`:

| Bucket | Meaning | Earliest broken link |
|---|---|---|
| `EVIDENCE_GAP` | Judge / RCA evidence ran out for the qid | `evidence_refs` |
| `RCA_GAP` | RCA chain itself broke | `rca_id` / `root_cause` |
| `TARGETING_GAP` | RCA right but patch didn't target the failing qid | `target_qids` |
| `PROPOSAL_GAP` | No proposal was generated | proposal id |
| `GATE_OR_CAP_GAP` | Proposals existed but were dropped at gate / cap | gate dispatch |
| `APPLY_OR_ROLLBACK_GAP` | Patch right but applier / rollback dropped it | applied / skipped |
| `MODEL_CEILING` | Strategist couldn't produce a canonical fix | next_action |

The "spot-check 3-5 unresolved qids" step in `gso-lever-loop-run-analysis/SKILL.md` validates that the assigned bucket matches the **earliest broken link** in the RCA invariant chain. Mismatch means the bucketing classifier needs work, not the optimizer.

### Step 5 — read the operator transcript

`gso_postmortem_bundle/iterations/iter_NN/operator_transcript.md` mirrors `PROCESS_STAGE_ORDER` exactly (renderer at `operator_process_transcript.py`). Section heading `### N. <Stage Title>` matches subdirectory `<NN>_<stage_key>/`. If the section is empty for a stage that should have records, either:

- The stage was wrapped but the harness didn't reach it (look earlier in the iteration for an exception or early-return).
- The renderer's `_STAGE_DECISION_TYPE_MAP` (`operator_process_transcript.py:35`) doesn't cover the `DecisionType` the stage emits — fix the map.

---

## 3. Where to Make Changes — by Class of Improvement

Different kinds of improvement live in different files. The 9-stage decomposition makes this much cleaner than "edit harness.py somewhere".

### A. RCA evidence is empty / wrong

- **Edit:** `optimization/stages/rca_evidence.py` and the primitives it calls (`optimization/rca.py::_asi_finding_from_metadata`, `_top_n_collapse_metadata_override`, `_safe_rca_kind`).
- **Input data:** `EvaluationResult.per_qid_judge` and `.asi_metadata` are populated at `stages/evaluation.py:243-244` from `evaluation.run_evaluation`'s return dict.
- **Upstream fix:** if the judge / ASI extraction itself is wrong, fix `evaluation.run_evaluation` body in `optimization/evaluation.py`. F1 + F2 will pick it up automatically.

### B. Clusters are too coarse / too fragmented

- **Edit:** `optimization/optimizer.py::cluster_failures` (algorithm) and `optimization/stages/clustering.py::form` (typed entry).
- **Caveat:** algorithm changes propagate cluster IDs downstream → decision sequencing changes → byte-stability test goes red. Plan a `gso-replay-cycle-intake` cycle, don't just bump the budget.

### C. Wrong action group / lever mix

- **Edit:** `optimization/stages/action_groups.py::select` and the strategist primitive it wraps.
- **Surfaces as:** `STRATEGIST_AG_EMITTED` records with empty `target_qids` or wrong lever picks.

### D. Bad proposals / over-broad SQL changes

- **Edit:** `optimization/stages/proposals.py::generate`.
- **Note:** the blast-radius gate (F6) catches the worst cases, but the cleaner fix is preventing over-broad proposals upstream.

### E. Wrong drops at safety gates

Two layers — pick the right one:

- **Algorithm change** (e.g., adjusting blast-radius threshold, changing groundedness rule): edit harness inline emit primitives at:
  - `harness.py:14323` (`lever5_structural_gate_records`)
  - `harness.py:15159 + 15332` (`groundedness_gate_records` AG-level + proposal-level)
  - `harness.py:15830` (`blast_radius_decision_records`)
- **Observability surface change** (or adding a new gate): edit `optimization/stages/gates.py`. The Phase H wire-up calls `stages.gates.filter` once per iteration as additive observability — every sub-handler is a `_run_*_gate(ctx, ...)` returning `tuple[GateDrop, ...]` with **zero** `ctx.decision_emit` calls.

If you're adding a brand-new gate that should drop proposals algorithmically (not just observe), add it as a sub-handler in `gates.py`, append to `GATE_PIPELINE_ORDER`, AND wire it inline at the appropriate harness point. Single-place changes risk byte-stability drift between F6 and the harness.

### F. Patches don't apply / apply but mis-target

- **Edit:** `optimization/stages/application.py::apply` and the underlying applier in `optimization/optimizer.py`.
- **Surfaces as:** `PATCH_SKIPPED` records with reason codes.

### G. Acceptance / rollback policy wrong

- **Edit:** `optimization/stages/acceptance.py::decide` and `optimization/control_plane.py::decide_control_plane_acceptance`.
- **Heads-up on A5 v2.1:** the harness still emits 3 pre-gate `skipped_*` records inline (DOA, no-pre-snapshot, no-applied-patches) via the closure at `harness.py:12298`. F8's per-stage `decisions.json` only owns the 2 post-gate outcomes (rolled_back, accepted). If you change acceptance policy, you're touching `decide_control_plane_acceptance` — not the inline emits. The post-merge `A5 v2.2 fidelity bump` ticket tracks future taxonomy unification.

### H. Learning isn't carrying state across iterations

- **Edit:** `optimization/stages/learning.py::update`.
- **Output:** `LearningUpdate` carries retired AGs, resolved/unresolved QID transitions, and dedup signatures.
- **Common cause of repeat-proposal-loops:** `rolled_back_content_fingerprints` set isn't being threaded into the next iteration's gates input.

### I. Operator transcript missing a section / wrong order

Edit two files in lockstep:

- `optimization/run_output_contract.py:53` — `PROCESS_STAGE_ORDER` adds / reorders the conceptual section.
- `optimization/operator_process_transcript.py` — `render_iteration_transcript` + `_STAGE_DECISION_TYPE_MAP` at `:35` (tells the renderer which `DecisionType` values to surface for each stage).

The integration test `tests/integration/test_phase_h_bundle_populated.py::test_transcript_renders_all_11_process_order_sections` catches drift between the two.

### J. Scoreboard signal misleading

- **Edit:** `optimization/scoreboard.py`.
- **Pattern:** the `*_from_trace` projections are deterministic — same inputs, same outputs. Add a new metric there, then wire it into `compute_scoreboard` at `:159`.

### K. Shipped-but-silent defect (cycle code is right but inert under flag-default-off)

A specific defect class registered in the [Defect Registry](./2026-05-07-contract-spirit-compliance-roadmap.md#defect-registry--single-source-of-truth-for-shipped-but-silent-defects). Symptoms look like a stage bug but the stage code is correct — the underlying behavior flag is default-off pending corpus measurement, so the canonical-trigger code path short-circuits before any observable evidence emits.

**Diagnostic protocol (always do this before deep-diving into stage code):**

1. **Identify the canonical trigger.** Look up the defect ID in the Registry. Each `D-N` row's `Anchor` cites the postmortem evidence and `Regression rail` cites the typed marker that should have emitted.
2. **Grep run stdout for the shadow-mode marker.** Each registered defect has a paired `GSO_<NAME>_OBSERVE_V1` marker (Cycle 14-V T1 + T2 pattern) that emits *even when the underlying flag is off*. If the marker emits with `suppressed_by_<flag>_off=true`, the cycle's logic is correct and you have your evidence to flip the behavior flag's default.
3. **If the shadow marker is silent on the canonical trigger,** that's a NEW defect — register it as `D-N+1` in the Registry and ship a Cycle-14-V-style observability fix.

**Edit-by-defect mapping (current registry as of 2026-05-09 #4):**

- **D-1** (C13 admission inert — **corpus-validated** by anchor #7 F5): `optimization/harness.py:9844-9889` (`_compute_forbidden_ag_set`) reads `forbidden_ag_admits_no_action_enabled()`. Shadow marker `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` (Cycle 14-V T1) fired on 5/5 NO_ACTION reflections. Default-flip to on in **Cycle 14-W T4**.
- **D-2** (C14B-T3 orchestrator inert — **untested**, neither anchor #7 nor #8 hit the canonical trigger): `optimization/harness.py:2000-2100+` (`_maybe_run_patch_isolation_orchestrator`). Shadow marker `GSO_PATCH_ISOLATION_OBSERVE_V1` (Cycle 14-V T2) shipped; awaiting future pilot evidence.
- **D-3** (canonical render contradiction — **partial**): `optimization/control_plane.py:format_full_eval_marker_payload` derives target fields. Cycle 14-V T3 fixed `target_fixed_qids`/`target_still_hard_qids`; Cycle 14-W T1 closes `target_soft_passing_qids` (anchor #7 F2 evidence — `gs_026=soft_passing` invisible in any bucket field).
- **D-4** (bundle assembler list normalization — **regressed in production**, anchor #8 F7): `optimization/run_output_bundle.py:_normalize_stage_capture` exists but isn't called at every `.get()` site. Cycle 14-W T2 audits every call site + ships airline-fixture-replay integration test (Discipline A).
- **D-5** (manifest Databricks IDs blank — **regressed in production**, both anchors #7 + #8 F8): `optimization/harness.py:_databricks_ids_from_env()` exists but production code path returns blank. Cycle 14-W T3 instruments resolution path with `GSO_DATABRICKS_IDS_RESOLVED_V1` tracing marker (Discipline B canonical example) + Jobs-runtime integration test.
- **D-6** (Phase H acceptance writer drift — new, anchor #8 F8): Phase H writer `iterations/iter_NN/stages/09_acceptance_decision/output.json` says `outcome=rolled_back` while stdout says ACCEPTED. Sibling-of-D-3 in a different writer. Cycle 14-W T6 routes through canonical `ControlPlaneAcceptance`. Rail: `GSO_PHASE_H_ACCEPTANCE_DRIFT_V1`.
- **D-7** (iteration summary totality — new, anchor #8 F7): Only 1 of 3 expected `GSO_ITERATION_SUMMARY_V1` emitted; `iter_record_counts=[46,54,48,47]` for 3-iter run. Cycle 14-W T5 emits one summary per attempted iteration. Rail: `GSO_ITERATION_SUMMARY_TOTALITY_V1`.
- **D-8** (replay-vs-Phase-H journey-validator drift — new, anchor #7 F8): Local replay reports 25 violations; Phase H reports 0. Sibling-of-D-3 in a third writer (Phase H journey validator). Cycle 14-W T6 shares `validate_journey()` helper across writers. Rail: `GSO_PHASE_H_JOURNEY_DRIFT_V1`.

**Regressed-defect diagnostic protocol (NEW 2026-05-09 #4 — Discipline A):** if a defect was previously closed but its symptom recurs in a fresh corpus pilot, do NOT reopen the original defect ID. Register a new `D-N+1` and treat the unit-test pass for the original closure as evidence that the unit test was insufficient. The new defect's closing cycle MUST include an end-to-end fixture-replay integration test that exercises the production code path. Cycle 14-W T2 + T3 are the canonical examples: D-4 + D-5 closed in Cycle 14-V with unit tests only; their production-shape regressions are closing in Cycle 14-W with anchor-fixture-replay integration tests.

**Multi-path resolver protocol (NEW 2026-05-09 #4 — Discipline B):** if a defect's closure depends on a function that has multiple internal resolution paths (e.g., `_databricks_ids_from_env` tries env vars → dbutils tags → sentinel), the closing cycle MUST emit a typed `GSO_<RESOLVER>_RESOLVED_V1` marker recording which path fired. This makes "function reached but wrong path" failures visible to corpus measurement. Without the trace, a regression in one resolution path is invisible while the other paths still work.

**Anti-pattern:** do NOT "fix" a shipped-but-silent defect by flipping the underlying behavior flag's default to on without first running a corpus pilot under the shadow marker. The flag is default-off because Cycle N's discipline is "warn-only pilot first, then promote"; flipping defaults without the corpus measurement skips the discipline and risks introducing decision-sequence drift that breaks replay byte-stability. **Exception (corpus-validated):** if the shadow marker has fired on the canonical trigger across the majority of a corpus pilot AND the regression rail stays silent, default-flip is justified — Cycle 14-W T4 is the canonical example (5/5 NO_ACTION reflections traced; rail silent; flip from off to on).

### Verification economics (Tier-1 / Tier-2 / Tier-3 closure)

Lever-loop pilots are expensive and stochastic. Replay-fixture
verification + diagnostic micro-jobs are cheap and deterministic.
Use the cheapest tier that produces sufficient evidence:

- **Tier-1 (`closed-local`)** is sufficient for observability-only
  cycles, pure-helper correctness fixes, and default-flips with prior
  shadow-corpus validation. Run via `uv run pytest`. Evidence: every
  binary success criterion has a replay-fixture integration test
  asserting it against vendored anchors. ~12 of the 20-plan
  roadmap is Tier-1.
- **Tier-2 (`closed-runtime`)** adds a ~30-second diagnostic micro-job
  when a defect depends on a runtime-only fact (env vars, dbutils tags,
  cluster IDs, network-time secrets). Canonical example:
  `notebooks/cycle_14_w_databricks_ids_diagnostic.py`. Submit as
  one-shot Databricks Jobs task; read the typed marker; close.
- **Tier-3 (`closed-corpus`)** runs a full lever-loop pilot on each
  anchor space when the cycle introduces new behaviour fixtures
  cannot validate (e.g., new attribution accounting in C14-C; new
  state-machine transitions in C17-T1). Reserve for behaviour cycles;
  amortise across multiple cycles whenever sequencing allows
  (e.g., C14-W + C14-C ship together; one pilot validates both).

Decision rule: before scheduling a Tier-3 pilot, ask "what would a
Tier-1 or Tier-2 closure miss?" If the answer is "nothing", skip
the pilot.

---

## 4. The Byte-Stability Discipline

This is the discipline that prevents the optimizer from drifting silently.

### 4.1 The merge gate

```bash
cd packages/genie-space-optimizer
uv run pytest tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -v
```

This replays the airline real-run fixture and asserts `len(violations) <= BURNDOWN_BUDGET` (currently `0`).

If your change keeps this green, **no decision sequencing changed** — the change is observability-only, refactor, or pure-output additive. Safe to ship.

If it goes red, you have three legitimate paths:

1. **Bug** — your change accidentally changed behavior. Fix it.
2. **Intentional algorithm change** — the change *should* change decisions. Run a fresh real lever loop, capture a new fixture via the `gso-replay-cycle-intake` skill, and update `docs/2026-05-02-phase-a-burndown-log.md`. Do not bump the budget without a cycle row.
3. **Test fixture is stale** — rare. Refresh via the `gso-replay-cycle-intake` workflow.

### 4.2 Per-stage byte-stability tests

For module-only changes, the per-stage tests are faster feedback:

```bash
uv run pytest tests/replay/test_phase_f4_byte_stable.py \
              tests/replay/test_phase_f5_byte_stable.py \
              tests/replay/test_phase_f6_byte_stable.py \
              tests/replay/test_phase_f7_byte_stable.py \
              tests/replay/test_phase_f8_byte_stable.py \
              tests/replay/test_phase_f9_byte_stable.py -v
```

Use them when iterating on a single stage; the full replay test is the merge gate.

### 4.3 Cross-projection completeness

`tests/replay/test_cross_projection_completeness.py` asserts every required `DecisionType` that had at least one corresponding event appears in the persisted `decision_trace.json`. This catches "added a decision-emit that isn't on the canonical projection list" before it ships.

---

## 5. The Dev-Loop in One Paragraph

```
Real run (job_id, run_id)
   → gso-postmortem skill
   → bundle materialized at docs/runid_analysis/<opt_run_id>/
   → identify failing stage from manifest + per-stage I/O capture
   → identify failing-qid bucket via failure_buckets classifier
   → narrow to one of the 9 stage modules + its primitive in optimizer.py
   → write a unit test that fails (TDD, against the typed Input/Output)
   → make the surgical change in the stage / primitive
   → run the byte-stability replay (must stay green OR plan a cycle)
   → run the full unit + integration suite
   → if intentional algorithm change: gso-replay-cycle-intake to advance
     the burn-down ledger
   → commit + repeat
```

**Cadence:** each commit should be a single logical change with a corresponding test, the byte-stability test green, and a commit message that names the stage / primitive touched.

---

## 6. Patterns and Anti-Patterns

### Patterns to follow

- **Read the typed surface first.** `iter_NN/stages/<NN>_<key>/output.json` is more reliable than stdout. The capture decorator never raises (`stage_io_capture.py:14`), so a stage that "didn't run" is silent — a stage that ran but had a bug has typed I/O for you.
- **Add new fields to typed dataclasses, not to dict-shaped `raw` blobs.** F1's `EvaluationResult.raw` exists as a backward-compat escape hatch (`stages/evaluation.py:62-69`); every Phase F follow-up shrinks it. Don't grow it.
- **Prefer additive observability over algorithm replacement.** F6's wire-up is a textbook example: typed surface populated, harness inline emits unchanged, byte-stability untouched. New gates can land this way before a future cleanup migrates the algorithm.
- **Use `gso-postmortem` even when you "know" what the bug is.** The bundle catches second-order issues you wouldn't have looked for.
- **Lock decisions in the follow-up plan's "Decision Log" section.** Both F2 and F6 plans have one (`docs/2026-05-05-phase-f{2,6}-*-followup-plan.md`). Future maintainers can see why a path was chosen without re-litigating.

### Anti-patterns

- **Editing `harness.py` for stage logic** when there's a stage module. The stage module is the right edit point; harness.py is orchestration only.
- **Adding `print(...)` for debugging.** The renderer (`operator_process_transcript.py`) is the deterministic projection. New observability = new typed field on an existing dataclass + a renderer line, not a print.
- **Bumping `BURNDOWN_BUDGET` without an intake cycle.** `gso-replay-cycle-intake` exists exactly to keep the audit trail honest. Skipping it ratchets noise into the gate.
- **Touching the inline emit closure at `harness.py:12298`** (`_phase_b_emit_ag_outcome_record`). It is intentionally kept inline by A5 v2.1. The post-merge `A5 v2.2 fidelity bump` ticket is where that work lives.
- **Reordering `PROCESS_STAGE_ORDER` without updating the renderer's `_STAGE_DECISION_TYPE_MAP`.** The integration test catches it, but you'll waste a commit.

---

## 7. Three Concrete Near-Term Improvement Vectors

The roadmap's post-merge backlog and the open follow-up plans give a natural prioritization.

### 7.1 Bucket-driven proposal targeting (highest leverage)

**Today:** the strategist (`stages/action_groups.py::select`) picks AGs from clusters; failure buckets are computed *after the fact* for postmortem analysis only.

**Proposal:** reverse-feed. The prior iteration's `FailureBucket` per qid becomes an input to `select`, so an `EVIDENCE_GAP` qid forces an evidence-gathering AG before another patch attempt; a `MODEL_CEILING` qid escalates to operator review instead of consuming proposal budget.

**Why this is small:** pure stage-input addition. Add a `prior_buckets_by_qid: dict[str, FailureBucket]` field on `ActionGroupsInput`, plumb it from the previous iteration's `LearningUpdate`, and let `select` use it as a gating signal. Byte-stable until you wire it; algorithm change at the wire-up step, with a cycle.

### 7.2 A5 v2.2 fidelity bump

**Today:** F8's `decisions.json` carries 2 of 5 acceptance outcomes; the 3 pre-gate `skipped_*` records emit inline from the harness closure at `:12298`.

**Proposal:** extend `AcceptanceInput` with a `pre_resolved_outcomes_by_ag: dict[str, str]` field, populate it in the pre-gate filtering steps, and have F8.decide() iterate that field first emitting the 3 pre-gate records, then proceed to its current post-gate logic.

**Why this is small:** ~2-4 hours; closes the per-stage taxonomy without changing run-level behavior. Documented in the roadmap appendix.

### 7.3 F6 algorithm migration (Path B of the F6 follow-up plan)

**Today:** F6 module is a typed observability surface; the harness inline gate algorithm at `:14323, 15159, 15332, 15830` is authoritative.

**Proposal:** move the inline gate algorithm into F6 sub-handlers as the authoritative implementation, then the inline `*_records` callers become DecisionRecord emit shims around F6 calls.

**Why this is bigger:** algorithm move → fresh variance baseline required → real lever-loop pilot run. High payoff for "single source of truth per gate"; should be planned, not improvised. Reference: `docs/2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md` Path B body.

---

## 8. Reference — Skills, Tools, Tests

### Skills (in `docs/skills/`)

| Skill | Direction | When to invoke |
|---|---|---|
| `gso-postmortem` | Entry point (read-only by default) | Operator says "postmortem", "diagnose", "troubleshoot a run" |
| `gso-lever-loop-run-analysis` | Read-only analysis | Auto-invoked by `gso-postmortem`; can also be called directly when you only want analysis |
| `gso-replay-cycle-intake` | Write-side ops | After a real-run pilot — promote fixture, advance burn-down ledger |

### Tools (in `src/genie_space_optimizer/tools/`)

| Tool | Purpose |
|---|---|
| `evidence_bundle.py` | Build `runid_analysis/<opt_run_id>/` from `(job_id, run_id)`; the `gso-postmortem` skill drives it |
| `mlflow_audit.py` | Discover sibling runs, anchor run, parent bundle from MLflow tags |
| `mlflow_artifact_anchor.py` | Resolve the per-run anchor that per-stage I/O captures attach to |
| `mlflow_backfill.py` | Re-emit decision-trail artifacts to MLflow when a run was missing them (operator-confirmed) |
| `marker_parser.py` | Parse stable stdout markers (`GSO_RUN_MANIFEST_V1`, `GSO_ARTIFACT_INDEX_V1`, etc.) |
| `trace_fetcher.py` | Pull MLflow traces when bundle alone is insufficient |

### Tests that gate the merge

| Test | Asserts |
|---|---|
| `tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget` | Real-run replay produces ≤ `BURNDOWN_BUDGET` violations |
| `tests/replay/test_phase_f{4..9}_byte_stable.py` | Per-stage decision sequence unchanged |
| `tests/replay/test_cross_projection_completeness.py` | Every emitted `DecisionType` reaches `decision_trace.json` |
| `tests/integration/test_phase_h_bundle_populated.py` | 9 executable stages + 11 transcript sections + run-overview block |
| `tests/integration/test_phase_h_skills_retrieval_smoke.py` | Phase H SKILL.md Steps 3-5 paths reachable end-to-end |
| `tests/unit/test_stages_protocol_conformance.py` | Every stage exports `INPUT_CLASS`, `OUTPUT_CLASS`, `STAGE_KEY`, `execute` |
| `tests/unit/test_process_stage_order_matches_stages_registry.py` | `PROCESS_STAGE_ORDER` ⊇ `STAGES.stage_key` |

---

## 9. Quick Reference — Stage-by-Stage Edit Map

When in doubt, this table tells you exactly where to look.

| Concern | Module | Primitive | Inline harness anchor (if any) |
|---|---|---|---|
| Eval row classification | `stages/evaluation.py` | `evaluation.run_evaluation` | `harness.py:10028` (F1 wire-up) |
| Per-qid evidence | `stages/rca_evidence.py` | `rca._asi_finding_from_metadata` | inserted before F3 (Phase H Task 2) |
| Cluster grouping | `stages/clustering.py` | `optimizer.cluster_failures` | F3 wire-up at A1 |
| Strategist | `stages/action_groups.py` | strategist primitive | F4 wire-up at A2 |
| Proposal generation | `stages/proposals.py` | proposal primitive | F5 wire-up at A3 |
| Lever-5 gate | `stages/gates.py::_run_lever5_structural_gate` | inline `lever5_structural_gate_records` | `harness.py:14323` |
| Groundedness gate | `stages/gates.py::_run_rca_groundedness_gate` | inline `groundedness_gate_records` | `harness.py:15159, 15332` |
| Blast-radius gate | `stages/gates.py::_run_blast_radius_gate` | inline `blast_radius_decision_records` | `harness.py:15830` |
| Apply | `stages/application.py` | applier primitive | F7 wire-up at A4 |
| Acceptance | `stages/acceptance.py` | `control_plane.decide_control_plane_acceptance` | `harness.py:16898` (F8 wire-up at A5 v2.1) |
| Pre-gate skip emits | (kept inline) | closure `_phase_b_emit_ag_outcome_record` | `harness.py:12298` (intentionally not migrated; A5 v2.2 backlog) |
| Learning / next action | `stages/learning.py` | learning primitive | F9 wire-up at A6 |
| Transcript section order | `optimization/run_output_contract.py:53` | `PROCESS_STAGE_ORDER` | n/a |
| Transcript record→section map | `optimization/operator_process_transcript.py:35` | `_STAGE_DECISION_TYPE_MAP` | n/a |
| Bundle assembly | `optimization/run_output_bundle.py` | `build_manifest`, `build_artifact_index`, `build_run_summary` | C18 wire-up |
| Per-stage I/O capture | `optimization/stage_io_capture.py:83` | `wrap_with_io_capture` | every stage call site |
| Failure bucket classifier | `optimization/failure_bucketing.py` + `failure_buckets.py` | `SEED_CATALOG`, `classify` | post-iteration |
| Scoreboard projections | `optimization/scoreboard.py` | `*_from_trace` functions | post-iteration |

---

## 10. C15 chunk-flag flip protocol

After Cycle 15 P1-P4 ship, four flags control whether the typed
stage-handler path runs:

| Flag | Default after C15 P1-P4 lands | Default after default-flip PRs land |
|---|---|---|
| `GSO_STAGE_HANDLERS_CHUNK_D` | off | on |
| `GSO_STAGE_HANDLERS_CHUNK_A` | off | on |
| `GSO_STAGE_HANDLERS_CHUNK_B` | off | on |
| `GSO_STAGE_HANDLERS_CHUNK_C` | off | on |

When a flag is **off**, the legacy harness path runs verbatim — no
behaviour change relative to pre-C15. When a flag is **on**, the
harness call sites delegate to `stages.<x>.execute()` and consume
the typed Output.

**Default-flip protocol (per chunk, 1 PR each):**

1. Land the chunk's contract PR (e.g. C15-P1).
2. Verify byte-stability replay both ways (flag off vs flag on) on
   a local replay of the airline + 7Now anchors. Deltas in marker
   ORDER are acceptable; deltas in marker PAYLOAD must be triaged.
3. Run a Tier-3 corpus pilot with the chunk flag on — same protocol
   as the C14-W + C14-C combined pilot.
4. If the pilot is clean, ship a default-flip PR (`default=True`) for
   that chunk.
5. After all four chunks are default-on for one full corpus pilot
   without regressions, schedule a follow-up cycle to remove the
   legacy harness paths (Phase 6 below; not in this plan).

**Rollback protocol:**

If a corpus pilot reveals a regression caused by a chunk flag flip:

1. Set the flag to `0` in the affected runtime (env var override).
2. Open a revert PR for the default-flip commit; merge.
3. Register the regression as D-(N+1) in the Defect Registry.
4. Diagnose against the chunk's boundary fixtures; the regression's
   shape will pinpoint which Input/Output field drifted.

---

If something in this guide is wrong or stale, fix it in the same commit as the underlying change — the guide is a contract document for the next maintainer, not a snapshot. The roadmap (`docs/2026-05-01-burn-down-to-merge-roadmap.md`) covers the strategic narrative and the post-merge backlog; this guide covers the day-to-day loop.
