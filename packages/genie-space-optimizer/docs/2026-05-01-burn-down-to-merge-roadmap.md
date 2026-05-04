# Burn-Down to Modular RCA Optimizer — Roadmap

> **Status:** Working roadmap for the `fix/gso-lossless-contract-replay-gate` feature branch. This is the **high-level program plan** for moving from a large, log-driven `harness.py` to a modular RCA-based optimizer with a unified trace architecture, typed contracts, deterministic replay gates, and operator-facing stdout that explains every important decision.

## Goal

Turn the Lever Loop into a reliable RCA optimization system where every iteration has a lossless, typed record of:

1. What evidence was observed.
2. What root cause was inferred.
3. Which action group, proposals, and patches were chosen.
4. Which gates accepted or rejected them and why.
5. Whether the target qids improved, regressed, or remained unresolved.
6. What the operator should try next.

The end state is a modular codebase where `harness.py` is an orchestration spine, each subsystem has a narrow role and explicit type contract, replay fixtures reproduce the same decisions byte-stably, and stdout renders a standard operator transcript that makes optimizer behavior easy to debug and improve.

## Architecture in one sentence

Nine sequential phases (0 → A → B → C → D → E → F → G → H), with one pre-merge observability insert (E.0) and a pre-rerun bug-fix batch (PR-A through PR-E). Phase A establishes a clean per-iteration journey contract and real replay fixture. Phase B introduces the unified `OptimizationTrace` / `DecisionRecord` contract and standard operator transcript. Phase C hardens the RCA loop itself. Phase D builds scoreboard and failure bucketing as projections of the same trace while starting low-risk extractions. The PR-A through PR-E batch (`2026-05-03-merge-readiness-pre-rerun-plans-index.md`) closes correctness and observability gaps surfaced by the live `407772af-9662-4803-be6b-f00a368c528a` run before the next expensive cycle. Phase E.0 makes MLflow decision artifacts reliable; Phase E flips the hard gate and merges. Phase F decomposes `harness.py` into nine **stage-aligned modules**, one per `RCA Evidence → Cluster → AG → Proposal → Gate → Applied Patch → Eval Result → Learning` step. Phase G freezes per-stage typed `StageInput` / `StageOutput` contracts. Phase H lands the **GSO Run Output Contract** as the final unification: a process-first `operator_transcript.md`, a parent-run `gso_postmortem_bundle/` with per-stage I/O capture, and a one-stop CLI/LLM postmortem package — all generated automatically from the typed stage modules introduced in F+G.

## Unified Architecture Target

The roadmap should converge on one canonical trace model with multiple projections, not separate logging systems:

```text
OptimizationTrace
  |-- Journey events: qid lifecycle projection
  |-- Decision records: optimizer choice/rationale projection
  |-- Journey validation reports: per-iteration contract health
  |-- Scoreboard snapshot: aggregate operator health metrics
  `-- Operator transcript: pretty stdout rendered from the same data
```

The shared identity spine is:

```text
run_id, iteration, question_id, cluster_id, rca_id, ag_id,
proposal_id, patch_id, gate, decision_type, reason_code, outcome
```

The journey ledger answers "where did this qid go?" Decision records answer
"what did the optimizer choose, why, and what happened?" The scoreboard and
stdout transcript must be derived from the same records so they cannot drift.

## RCA-Grounded Decision Invariant

No optimizer decision is valid unless it can be traced through this chain:

```text
evidence -> RCA -> causal target qids -> proposed patch -> gate rationale
  -> applied/skipped outcome -> observed eval result -> next action
```

This is a contract, not just an observability preference. Every AG, proposal,
patch, gate result, rollback/acceptance decision, and unresolved-qid state must
either carry that chain or carry a typed reason explaining which link is absent.

Concretely, every applicable `DecisionRecord` must carry:

- `evidence_refs` — trace IDs, eval-row references, judge/ASI IDs, SQL-diff IDs, or replay fixture references that justify the decision.
- `rca_id` and `root_cause` — the normalized RCA being acted on.
- `target_qids` — the qids the decision claims to help, or a typed broad-scope reason.
- `expected_effect` — the specific behavior the patch/decision expects to change.
- `observed_effect` — what post-eval actually observed.
- `regression_qids` — any out-of-target qids harmed or put into regression debt.
- `reason_code` — a stable enum suitable for replay assertions and dashboards.
- `next_action` — the operator or optimizer action implied by the result.

LLMs may propose, summarize, and reason over competing RCA options. Deterministic
contracts decide whether the proposal is RCA-grounded, targetable, safe to apply,
accepted, rolled back, or escalated. This keeps the optimizer tunable even when
Genie or model behavior remains probabilistic.

## Observability Contract

Standard output is a deterministic projection of `OptimizationTrace`, not a
separate logging path. New observability must add typed trace fields or renderer
sections, not ad hoc print blocks in `harness.py`.

The operator transcript is the human-readable projection of the same machine-
readable records persisted in replay fixtures and MLflow artifacts. If stdout
says "gate dropped P001 because no causal target," the same fact must exist as a
typed `DecisionRecord` with a stable `reason_code`.

Every iteration's transcript must use a fixed schema:

1. Iteration summary.
2. Hard failures and current qid state.
3. RCA cards with evidence.
4. AG decisions and rationale.
5. Proposal survival and gate drops.
6. Applied patches and rollback/acceptance decision.
7. Observed result and regressions.
8. Unresolved qid buckets.
9. Next suggested action.

Replay must assert the operator transcript is byte-stable. A future operator
should be able to diagnose any failed iteration using only the standard
operator transcript plus linked trace JSON, without grepping raw logs or reading
`harness.py`.

The reusable analyzer for this contract is the `gso-lever-loop-run-analysis`
skill under `docs/skills/`. It consumes Databricks Job ID + Run ID, reads
`GSO_*_V1` markers, MLflow tags/artifacts, replay fixtures, and traces, then
writes a structured postmortem under `docs/runid_analysis/`.

The standard process every human transcript and machine artifact must mirror is:

```text
RCA Evidence -> Cluster -> Action Group -> Proposal -> Gate
  -> Applied Patch -> Eval Result -> Learning
```

Phase E.1 turns that process into the formal **GSO Run Output Contract**:

- `operator_transcript.md` renders the human process ledger with short "what happened" and "why this stage exists" descriptions for every stage.
- `OptimizationTrace` and `DecisionRecord` remain the canonical typed evidence for LLM reasoning.
- `GSO_*_V1` stdout markers expose run, iteration, convergence, and artifact-index pointers for Databricks CLI discovery.
- MLflow stores a parent-run `gso_postmortem_bundle/` as the one-stop shop for LLM troubleshooting, while iteration eval runs retain iteration-local artifacts and logged models retain candidate/champion state.
- `evidence_bundle` materializes that MLflow bundle into `docs/runid_analysis/<optimization_run_id>/evidence/`.
- `gso-postmortem` consumes only the evidence bundle unless the manifest declares a missing piece that requires raw stdout or trace fetch fallback.

## At a glance

| # | Phase | Replay-only? | Real-Genie runs | Calendar | Branch state |
|---|---|---|---|---|---|
| 0 | Cross-task state resilience (Repair Run fix) **(✅ complete)** | No | 0–1 | shipped | pre-merge |
| A | Contract burn-down + real-fixture capture **(✅ complete — 2026-05-02; cycle-9 post-close burndown landed 2026-05-03)** | No | 9 (cycles 1-8 + post-close cycle 9) | ~1 day actual (estimate was 3-5 days) | pre-merge |
| B | Unified trace + DecisionRecord + operator transcript **(✅ complete — 10/10 producers shipped via cycle 9 + delta)** | Yes | 0 | shipped | pre-merge |
| C | RCA loop reliability hardening **(✅ complete — RCA loop contract, residuals, target-qid propagation landed)** | Mixed | 0–1 | shipped | pre-merge |
| D | Scoreboard, failure bucketing, and first trace-aware extractions **(✅ complete — 3/3 plans landed)** | Yes | 0 | shipped | pre-merge |
| **D.5** | **Pre-merge polish — alternatives capture (cluster / AG / proposal) (✅ complete)** | Yes | 0 | shipped | pre-merge |
| **PR-A→PR-E** | **Pre-rerun bug-fix batch from live run `407772af` (✅ complete — 6/6 plans landed)** | Mixed | 0 (replay-only during impl) | shipped | pre-merge (next live run gated by E.0) |
| **E.0** | **MLflow artifact integrity audit + persistence fixes** | Mostly | 0 (replay-only) + 1 backfill smoke | ~2–3 days | pre-merge prerequisite for E |
| E | Final integration + contract-gate flip + merge | No | 1 | ~1 day | merge point |
| F | Stage-aligned `harness.py` modularization (9 stage modules) | Yes | 0 | ~10–14 days | post-merge follow-up |
| G | Stage Protocol + registry + RunEvaluationKwargs (G-lite) | Yes | 0 | ~1–2 days | post-merge architecture follow-up |
| **H** | **GSO Run Output Contract — process-first transcript + per-stage MLflow bundle (final unification)** | Yes | 0 | ~5–7 days | post-merge unification |
| | **Pre-merge total** | | **10–11 runs (~20–22 hrs, 9 already spent)** | **~1 week remaining** | |
| | **Post-merge follow-up (F → G → H)** | | 0 | ~2–3.5 weeks | |

## Why this sequencing

Nine reasons it has to be in this order:

1. **Phase 0 unblocks iteration cadence.** Repair Run has to carry enough state for short re-runs; otherwise every burn-down cycle costs a full 2-hour DAG.
2. **Phase A makes replay trustworthy.** The real fixture now validates per iteration, persists `journey_validation`, keeps raw cycle fixtures, and has a CI budget tightened to zero. That gives every later phase a deterministic safety rail.
3. **Phase B must precede more observability.** Scoreboards, failure buckets, and stdout should not each invent their own schema. A canonical `DecisionRecord` first makes every later rendering a projection of one source of truth.
4. **Phase C makes RCA reliability first-class.** The optimizer is only useful if evidence, root cause, causal patch, targeted qids, observed effect, and learned next action form a closed loop. Cycle 8's `target_qids: []` and GT-correction qid loss bugs are symptoms of that contract not being explicit enough.
5. **Phase D can then build operator UX safely.** Scoreboard, bucketing, and initial extractions become consumers of `OptimizationTrace`, not parallel log parsers.
6. **Phase F is stage-aligned, not just byte-stable.** Phases A–E and the PR-A→E batch made every stage-level decision (`EVAL_CLASSIFIED`, `CLUSTER_SELECTED`, `RCA_FORMED`, `STRATEGIST_AG_EMITTED`, `PROPOSAL_GENERATED`, `GATE_DECISION`, `PATCH_APPLIED`, `ACCEPTANCE_DECIDED`, `QID_RESOLUTION`, `AG_RETIRED`) typed and traceable. Phase F finally aligns the *executable code* with the same nine-stage process so the LLM postmortem can map a `decision_type` from stdout straight to one source file. Each extraction is its own commit, gated by byte-stable replay.
7. **Phase G freezes the per-stage typed input/output contracts** so each stage module exposes a `StageInput` and `StageOutput` dataclass that LLMs and humans can reason about in isolation. Strong contracts become both safe and useful once the code has coherent stage-aligned homes.
8. **Phase H lands the GSO Run Output Contract on top of F+G.** The process-first `operator_transcript.md`, the parent-run `gso_postmortem_bundle/` with per-stage `iter_NN/stages/<stage>/input.json + output.json + decisions.json`, and the `GSO_ARTIFACT_INDEX_V1` marker all become deterministic projections of the typed stage I/O. Without F+G, Phase H would have to re-invent stage attribution by parsing `harness.py`; with F+G, attribution is free.
9. **The final unification reads as a tape.** After H lands, an iteration of `_run_lever_loop` reads as a linear sequence of `stages.evaluation.evaluate_post_patch` → `stages.rca_evidence.collect` → `stages.clustering.form` → `stages.action_groups.select` → `stages.proposals.generate` → `stages.gates.filter` → `stages.application.apply` → `stages.evaluation.evaluate_post_patch` → `stages.acceptance.decide` → `stages.learning.update`, each capturing typed I/O. That tape is what the operator transcript renders, what the LLM postmortem reasons over, and what scoreboard/failure bucketing project into operator metrics.

**Current guardrail:** avoid adding new substantial helpers directly to `harness.py`. If a helper is a reusable domain operation or grows beyond roughly 30–50 LOC, put it in the module it will eventually belong to and import it into `harness.py`. New instrumentation must add `DecisionRecord` / `OptimizationTrace` producers or renderer sections, not freeform print/log blocks. After Phase F1 lands, "the module it will eventually belong to" is concretely the corresponding `optimization/stages/<stage>.py` file.

---

## Open Gaps and Future Work — Diagnosability rubric

The end-state target is: **given a stdout/stderr from a Lever Loop job, an operator can identify which module's reasoning was off and fix it**. Phases 0–D get the program ~70–80% there. The remaining 20–30% is tracked here explicitly so it doesn't fall off the radar.

### Diagnosability scorecard (as of 2026-05-04, post-PR-E)

| Property | Grade | Concrete gap | Phase that closes it |
|---|---|---|---|
| **Per-qid RCA log** is typed and complete | A (~95%) | Alternatives are now captured for cluster / AG / proposal selection (Phase D.5 landed). | done |
| **Cluster formation rationale** is first-class | A− (~90%) | `cluster_records` now stamps both chosen cluster and rejected alternatives. | done |
| **Lane-aware journey validation** | A (~95%) | `validate_question_journeys` now splits trunk and per-`proposal_id` lanes (PR-C landed). | done |
| **AG retirement transparency** | A (~95%) | Plateau termination now emits one `AG_RETIRED` `DecisionRecord` per silently-retired AG (PR-B2 landed). | done |
| **RCA top-N intent classification** | A (~95%) | `RANK()` without `LIMIT N` now routes to `TOP_N_CARDINALITY_COLLAPSE` instead of `wrong_join_spec` (PR-D landed). | done |
| **Pre-arbiter saturation acceptance** | A (~95%) | `accepted_pre_arbiter_improvement` branch now fires when post-arbiter is flat but pre-arbiter improved (PR-E landed). | done |
| **Reflection content-fingerprint dedup** | A (~95%) | `_drop_proposals_matching_rolled_back_content_fingerprints` now blocks byte-identical re-proposals (PR-E landed). | done |
| **Modularized code** (defect → one file mapped to one stage) | C+ (~40%) | 6 of 10 stages still live inside `harness.py` (~19,900 LOC) / `optimizer.py` (~15,600 LOC) / `synthesis.py` / `applier.py`. The PR-A→E fixes landed in the right files but the executable code is still not stage-aligned. | **Phase F (9 stage modules)** |
| **Per-stage typed input/output** (LLM can reason about each stage in isolation) | F (~10%) | Stage I/O today is implicit through shared dicts and harness locals. | **Phase G (typed `StageInput` / `StageOutput`)** |
| **Per-stage I/O capture in postmortem bundle** | F (~5%) | The `gso_postmortem_bundle/iterations/iter_NN/` plan exists but cannot be populated without per-stage modules. | **Phase H (built on F + G)** |
| **Stdout-only diagnosability** | B (~70%) | (a) Hard gate not yet flipped; (b) MLflow artifacts (`phase_a/`, `phase_b/`) need anchoring (E.0). | Phase E.0 + E |
| **Stderr-only diagnosability** | F (~10%) | Stderr today is mostly Python tracebacks, not contract reasoning. The transcript lives in stdout + MLflow artifacts. | Out of scope; not needed if stdout + artifacts are reliable. |

### Stage → module localization map

When stdout points at a `decision_type`, today this is where the reasoning lives. Phase F closes the right column by extracting one module per stage in `optimization/stages/`.

| Stage (per `PROCESS_STAGE_ORDER`) | `decision_type` | Producer (`decision_emitters.py`) | Reasoning today | Reasoning after Phase F |
|---|---|---|---|---|
| evaluation_state | `EVAL_CLASSIFIED` | `eval_classification_records:102` | `eval_entry.py` ✅ + `harness.py` + `evaluation.py` | `stages/evaluation.py` (entry + post-eval; F1) |
| rca_evidence | (feeds RCA_FORMED) | (none direct — feeds `rca_formed_records`) | `harness.py` + `rca.py` + `judge_classes.py` | `stages/rca_evidence.py` (F2) |
| cluster_formation | `CLUSTER_SELECTED` + `RCA_FORMED` | `cluster_records:161`, `rca_formed_records:220` | `harness.py` + `optimizer.py:cluster_failures:1865` + `rca.py` | `stages/clustering.py` (F3) |
| action_group_selection | `STRATEGIST_AG_EMITTED` | `strategist_ag_records:284` | `harness.py` + `optimizer.py` + `strategist_constraints.py` | `stages/action_groups.py` (F4) |
| proposal_generation | `PROPOSAL_GENERATED` | `proposal_generated_records:380` | `harness.py` + `synthesis.py` + `optimizer.py:generate_proposals_from_strategy` | `stages/proposals.py` (F5) |
| safety_gates | `GATE_DECISION` (lever-5 / blast-radius / groundedness / DOA) | `lever5_structural_gate_records:855` / `blast_radius_decision_records:776` / `groundedness_gate_records:1220` / `dead_on_arrival_decision_records:911` | `harness.py` + `applier.py` + `proposal_grounding.py` + `iteration_acceptance.py` + `reflection_retry.py` | `stages/gates.py` (F6) |
| applied_patches | `PATCH_APPLIED` / `PATCH_SKIPPED` | `patch_applied_records:466` | `harness.py` + `applier.py` | `stages/application.py` (F7) |
| post_patch_evaluation | (re-uses EVAL stage) | (re-uses `eval_classification_records`) | `harness.py` + `post_eval.py` ✅ | `stages/evaluation.py` (post-eval entry; F1) |
| acceptance_decision | `ACCEPTANCE_DECIDED` + `QID_RESOLUTION` | `ag_outcome_decision_record:592`, `post_eval_resolution_records:677` | `ag_outcome.py` ✅ + `post_eval.py` ✅ + `control_plane.py` + `iteration_acceptance.py` + `acceptance_policy.py` + `harness.py` | `stages/acceptance.py` (F8) |
| learning_next_action | `AG_RETIRED` + terminal records | (terminal/AG_RETIRED records emitted inline in `harness.py:11801-11828`) | `harness.py` + `reflection_retry.py` + `rca_terminal.py` | `stages/learning.py` (F9) |

After Phase F lands, every `decision_type` in the operator transcript maps to exactly one stage module. Phase G adds typed `StageInput` / `StageOutput` per module. Phase H captures both into the parent-run `gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/{input.json,output.json,decisions.json}` so an LLM postmortem can attribute any regression to a single stage with full per-stage I/O.

### Pre-merge gap closures (Phase D.5, PR-A→PR-E batch, and Phase E.0)

Three pre-merge gap-closure batches stand between the post-Phase D codebase and the Phase E merge gate flip:

- **Phase D.5 — Alternatives capture (✅ complete).** Added `alternatives_considered: tuple[AlternativeOption, ...]` to `DecisionRecord` and stamped it on `CLUSTER_SELECTED`, `STRATEGIST_AG_EMITTED`, and `PROPOSAL_GENERATED`. Transformed transcript reasoning from "this stage chose X" to "this stage chose X over {Y, Z} because of {reason_Y, reason_Z}". Plan: [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md).

- **PR-A → PR-E — Pre-rerun bug-fix batch from live run `407772af` (✅ complete).** The 2026-05-03 live Lever Loop run on the airline benchmark surfaced six structural bugs that the existing trace-and-decision-record contract caught but could not fix on its own. The batch landed all of them before the next expensive cycle; the index plan documents what each PR shipped and gives the cross-references. Index: [`2026-05-03-merge-readiness-pre-rerun-plans-index.md`](./2026-05-03-merge-readiness-pre-rerun-plans-index.md).

  | PR | Plan | Fix |
  | -- | ---- | --- |
  | PR-A | [`2026-05-03-pr-a-replay-pasted-fixture-validation-plan.md`](./2026-05-03-pr-a-replay-pasted-fixture-validation-plan.md) | Operator script `replay_runid_fixture` + canonical analysis outputs under `docs/runid_analysis/<opt_run_id>/analysis/`. |
  | PR-B1 | [`2026-05-03-pr-b1-evidence-bundle-notebook-output-fallback-plan.md`](./2026-05-03-pr-b1-evidence-bundle-notebook-output-fallback-plan.md) | `evidence_bundle` falls back to `notebook_output.result` when Databricks Jobs API returns no logs. |
  | PR-B2 | [`2026-05-03-pr-b2-lever-loop-termination-vocab-and-ag-retirement-plan.md`](./2026-05-03-pr-b2-lever-loop-termination-vocab-and-ag-retirement-plan.md) | Convergence-marker `reason` unified with the human-readable termination print; one `AG_RETIRED` `DecisionRecord` per silently-retired AG at plateau. |
  | PR-C | [`2026-05-03-pr-c-lane-aware-journey-validator-and-fixture-persistence-plan.md`](./2026-05-03-pr-c-lane-aware-journey-validator-and-fixture-persistence-plan.md) | `validate_question_journeys` now splits trunk and per-`proposal_id` lanes; multi-proposal iterations no longer trigger spurious `illegal_transition` violations. |
  | PR-D | [`2026-05-03-pr-d-rca-classifier-top-n-cardinality-routing-plan.md`](./2026-05-03-pr-d-rca-classifier-top-n-cardinality-routing-plan.md) | `_safe_rca_kind` routes `RANK()` without `LIMIT N` to `TOP_N_CARDINALITY_COLLAPSE` instead of `wrong_join_spec` when intent + SQL shape align. |
  | PR-E | [`2026-05-03-pr-e-pre-arbiter-secondary-acceptance-and-reflection-dedup-plan.md`](./2026-05-03-pr-e-pre-arbiter-secondary-acceptance-and-reflection-dedup-plan.md) | `decide_control_plane_acceptance` now accepts on `accepted_pre_arbiter_improvement` when post-arbiter is saturated and pre-arbiter improved with no collateral regression; content-fingerprint dedup blocks byte-identical re-proposals across rollback classes. |

- **Phase E.0 — MLflow artifact integrity audit.** Phase A claims to persist `phase_a/journey_validation/iter_<N>.json` and Phase B claims to persist `phase_b/decision_trace/iter_<N>.json` + `phase_b/operator_transcript/iter_<N>.txt`. Spot inspection of `iter_04 / full_eval / pass_1 / run_d6a7faeb` shows only `evaluation_runtime/`, `judge_prompts/`, `model_snapshots/` — the decision-trail artifacts are not visible on the run an operator naturally clicks into. The persistence calls exist (`harness.py:17241`, `17312-17319`) but route to whichever MLflow run was last started by the harness's `end_run` / `start_run` pattern. E.0 audits where artifacts actually land, anchors them to a stable per-iteration parent run, surfaces silent persistence failures, and adds a backfill CLI for completed runs. Plan: [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md).

The **GSO Run Output Contract** (formerly E.1) has been moved to **Phase H** as the post-Phase G unification step. The motivation is unchanged — the live `407772af-9662-4803-be6b-f00a368c528a` run proved humans and LLMs need a process-first transcript and a one-stop MLflow `gso_postmortem_bundle/` — but the contract becomes much smaller and much more powerful once Phases F and G have produced typed per-stage modules whose I/O can be captured automatically. See **Phase H** below. Plan (target file): [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md).

### Future work explicitly on the radar

- **Phase F stage-aligned modularization** — see Phase F below. Index: [`2026-05-04-phase-f-stages-modularization-index.md`](./2026-05-04-phase-f-stages-modularization-index.md).
- **Phase G typed `StageInput` / `StageOutput` contracts** — see Phase G below; concrete because each Phase F module already exposes the right input/output shape.
- **Phase H GSO Run Output Contract unification** — see Phase H below; the `gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/` payload is generated automatically from per-stage I/O capture once F+G land.
- **Production observability dashboard** — currently parked. Phase H's stable bundle layout is the precondition; E.0's anchoring fix is a prerequisite for dashboards to point at the right run.

---

## Phase 0 — Make Repair Run reliable

**Why first:** Without this, Phase A's burn-down becomes a 10-day slog at 2 hours per iteration. With it, ~20 minutes per iteration.

**What ships:** The plan in [`2026-05-01-cross-task-state-resilience-plan.md`](./2026-05-01-cross-task-state-resilience-plan.md):

- New `jobs/_handoff.py` module with typed `HandoffValue` reads (taskValues → Delta fallback → loud failure).
- Three new columns on `genie_opt_runs` (`warehouse_id`, `human_corrections_json`, `max_benchmark_count`).
- `assert_lever_loop_inputs_sane` loud-failure guard that refuses to run the loop with degenerate inputs.
- Wired into `run_lever_loop.py`, `run_finalize.py`, `run_deploy.py`.

13 tasks, all TDD, all unit-tested. No real Genie needed until the verification smoke run at Task 13.

**Exit criterion:** A Repair Run on the `lever_loop` task produces logs containing `baseline_accuracy_source: delta_fallback`, the loop iterates, and the validator fires (in warn-only mode). One smoke run is enough — you don't need a successful optimization, just proof that the loop entered iteration 1 with real state instead of terminating at iteration 0 with `plateau_no_open_failures`.

**Real-Genie runs:** 1 (Phase 0 verification — the same run rolls into Phase A).

---

## Phase A — Contract burn-down + real-fixture capture

**Why second:** The lossless-contract validator is already deployed in **warn-only** mode (per [`2026-05-01-lever-loop-lossless-contract-and-replay-gate-plan.md`](./2026-05-01-lever-loop-lossless-contract-and-replay-gate-plan.md)). Burn-down is the operator activity of running real loops, triaging the violations the warn-only validator surfaces, fixing missing event emits in `harness.py`, and re-running until violations are zero.

Without burn-down, Phase D's scoreboard math would be built on a journey ledger that's still missing events — `causal_patch_survival_pct` would be wrong by construction.

**What ships:**

1. Per-iteration replay validation that mirrors the production harness contract instead of flattening multi-iteration journeys into one qid timeline.
2. Event-emit and replay-engine fixes surfaced by real airline cycles, with the CI burn-down budget tightened to zero.
3. A committed real fixture: `tests/replay/fixtures/airline_real_v1.json`, plus preserved raw cycle fixtures such as `airline_real_v1_cycle7_raw.json` and `airline_real_v1_cycle8_raw.json`.
4. Per-iteration `journey_validation` persisted into the replay fixture, MLflow artifacts (`phase_a/journey_validation/iter_<N>.json`), and MLflow tags.
5. Burn-down logs that record cycle history, violation composition, replay-engine fixes, and the final zero-violation close.

**Why a real-captured fixture, not a hand-synthesized one?** Capturing real loop output is faster (no design work), unbiased (it exercises every event the real loop *actually* emits, not what we *expected* it to emit), and refreshable (when the loop's emit set legitimately changes, re-capture and commit). The hand-synthesized fixture extension that earlier drafts proposed is dropped.

Logic correctness for later trace, transcript, scoreboard, and bucketing work is validated by **pure-function unit tests over synthetic events**. Fixture work is for end-to-end replay byte-stability and real-cycle regression coverage.

**Exit criterion:** Clean burn-down on airline corpus + `airline_real_v1.json` committed with `expected_canonical_journey`, per-iteration validation report persistence, and `test_run_replay_airline_real_v1_within_burndown_budget` enforcing budget `0`.

**Real-Genie runs:** 8 cycles actual. Phase A's burndown closed 2026-05-02; see [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md). A 9th post-close cycle on 2026-05-03 surfaced new bugs (premature plateau termination via the dead-on-arrival path discarding unrelated buffered AGs, blast-radius drops with no escape hatch, contradictory `add_*` patches against `remove_*` counterfactuals, dormant SQL-shape predicates, missing `phase_b_marker` when no decision records were emitted). Those fixes shipped via [`2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md`](./2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md), which also pre-shipped four of Phase B's seven decision-record producers (`blast_radius`, `dead_on_arrival`, `ag_outcome`, `post_eval_resolution`), the strategist-`forbid_tables` constraint store, the operator scoreboard banner, and four new failure-bucket seed patterns. The post-close burndown is treated as the tail of Phase A; the Phase B delta plan below picks up where cycle 9 left off.

---

## Phase B — Unified trace + DecisionRecord + operator transcript

**Why third:** The journey ledger is complete, but it is only the qid lifecycle projection. The optimizer also needs a decision projection: every important choice should have a typed record that explains the choice, input evidence, policy/rationale, causal target, and observed result. Without this phase, scoreboard and stdout would keep being derived from scattered logs.

**What ships:**

- New trace module ownership under `optimization/rca_decision_trace.py` first, with the option to split transcript rendering into `optimization/operator_transcript.py` during Phase F, containing:
  - `OptimizationTrace`
  - `DecisionRecord`
  - `DecisionType`
  - `DecisionOutcome`
  - `ReasonCode`
  - helpers for appending and rendering records deterministically
- Shared identity fields across journey events and decisions:
  - `run_id`
  - `iteration`
  - `question_id`
  - `cluster_id`
  - `rca_id`
  - `ag_id`
  - `proposal_id`
  - `patch_id`
  - `gate`
  - `decision_type`
  - `reason_code`
  - `outcome`
- Required RCA-grounded decision fields where applicable:
  - `evidence_refs`
  - `root_cause`
  - `target_qids`
  - `expected_effect`
  - `observed_effect`
  - `regression_qids`
  - `next_action`
- Decision records for the first end-to-end path:
  - eval row classified
  - cluster selected
  - RCA card/theme formed
  - strategist AG emitted
  - proposal generated
  - gate accepted/dropped
  - patch applied or skipped
  - rollback/acceptance decided
  - qid resolved/unresolved
- Standard operator transcript rendered from `OptimizationTrace`, not from ad hoc logging or scattered harness locals. Minimum transcript sections:
  - iteration summary
  - hard failures and current qid state
  - RCA cards
  - strategist/action-group decisions
  - proposal survival table
  - gate drop reasons
  - patch application and rollback/acceptance decision
  - observed result and regressions
  - unresolved qid buckets
  - next suggested action
- Replay fixture extension: `iterations[N].decision_records`, preserved through `journey_fixture_exporter.py`.
- MLflow artifacts: `phase_b/decision_trace/iter_<N>.json` and `phase_b/operator_transcript/iter_<N>.txt` when an active run exists.

**Validation strategy:**

- Pure unit tests for `DecisionRecord` serialization, required RCA/evidence fields, stable sort order, and renderer snapshots.
- Replay tests assert `airline_real_v1.json` produces byte-stable decision records and a byte-stable operator transcript.
- Cross-projection consistency tests: if a decision says a patch was applied, the journey projection must contain the corresponding applied event; if the journey says dropped at a gate, a gate decision record must explain why.

**Exit criterion:** every replay iteration has a decision trace and operator transcript; transcript is readable without grepping raw logs; journey, decision, RCA, and validation projections agree; every applicable decision is traceable through evidence → RCA → causal target qids → proposed patch → gate rationale → applied/skipped outcome → observed eval result → next action.

**Real-Genie runs:** 0.

**Detailed plans:**
- [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md) — original 9-task contract-first plan; Tasks 1-7 shipped; Tasks 8-9 superseded by the delta plan below.
- [`2026-05-03-phase-b-decision-trace-completion-plan.md`](./2026-05-03-phase-b-decision-trace-completion-plan.md) — **delta plan, ready for implementation.** Closes the remaining gaps after cycle 9: plumbs `rca_id_by_cluster` from real RCA findings, adds the three remaining producers (`RCA_FORMED`, `PROPOSAL_GENERATED`, `PATCH_APPLIED`), widens the validator's `applied`-stage matcher, projects `DecisionType` slices into the nine named transcript sections, and adds a synthetic cross-projection replay test that pins all ten DecisionTypes byte-stably. 10 TDD tasks; ~1–2 days; no real-Genie cycles needed during implementation. After it lands, one real-Genie airline cycle refreshes `airline_real_v1.json` with seeded `expected_canonical_decisions` / `expected_operator_transcript` and unblocks Phase C.

---

## Phase C — RCA loop reliability hardening

**Why fourth:** The optimizer's core job is not to produce events; it is to improve Genie Spaces through an RCA loop. Phase C makes that loop explicit and reliable:

```text
evidence -> root_cause -> causal_patch -> targeted_qids
  -> expected_fix -> observed_result -> learned_next_action
```

Cycle 8 exposed two concrete gaps in this loop: decomposed strategist patches with `target_qids: []`, and GT-correction candidates losing `question_id`. Both are identity/causality failures. They must be fixed before deeper modularization, because extracted modules should inherit a correct RCA contract rather than preserve broken ambiguity.

**What ships:**

- A canonical RCA loop contract in the same trace vocabulary used by Phase B:
  - `EvidenceRecord`
  - `RcaFinding`
  - `CausalPatchIntent`
  - `ExpectedFix`
  - `ObservedEffect`
  - `LearnedNextAction`
- An RCA-groundedness gate: any AG, proposal, or patch without an RCA-backed causal claim is rejected, quarantined, or flagged with a typed `reason_code`.
- Fix for strategist/decomposition patch emission where patches lose `target_qids`; every patch must carry target qids or an explicit reason it is intentionally broad.
- Shared canonical `extract_question_id(row)` helper used by baseline seeding, GT correction, eval row consumers, and replay/exporter code. Trace/request IDs are last-resort fallbacks, never preferred over benchmark qids.
- Decision records for RCA failures:
  - no evidence
  - no RCA
  - RCA but no AG
  - AG but no proposal
  - proposal but no causal target
  - patch dropped by gate
  - patch applied but no observed improvement
- Unit tests from observed Cycle 8 row and patch shapes.

**Validation strategy:**

- Unit tests prove every known qid shape extracts canonical benchmark qids and never prefers `tr-*` over `inputs.question_id`.
- Unit tests prove decomposed AG patches inherit `affected_questions` when patch-level `target_qids` is omitted.
- Replay tests assert every unresolved qid has an RCA loop state and next action.
- One optional real-Genie run if Cycle 8's side-bug fixes need live confirmation before merge.

**Exit criterion:** no `target_qids: []` patches reach gates unless explicitly marked broad with a typed reason; no GT-correction candidate is skipped for missing qid; every AG/proposal/patch has an RCA-backed causal claim or an explicit ungrounded reason; every unresolved qid has a traceable RCA loop state and suggested next action.

**Real-Genie runs:** 0–1.

**Detailed plans:** split into focused plans:
- `2026-05-XX-canonical-qid-extraction-plan.md`
- `2026-05-XX-target-qid-propagation-plan.md`
- `2026-05-XX-rca-loop-contract-plan.md`

---

## Phase D — Scoreboard, failure bucketing, and first trace-aware extractions

**Why fifth:** Once journey events, decision records, and RCA loop states share one trace architecture, operator metrics and unresolved-qid buckets become projections instead of separate logic. Phase D also starts modularization, but only where the new trace contract makes extraction low risk.

**What ships:**

- New module `optimization/scoreboard.py` exposing `ScoreboardSnapshot` and `build_scoreboard(trace)`.
- New module `optimization/failure_bucketing.py` exposing `FailureBucket` and `classify_unresolved_qid(trace, qid)`.
- Scoreboard metrics computed from `OptimizationTrace`, not directly from scattered harness locals:
  - `journey_completeness_pct`
  - `hard_cluster_coverage_pct`
  - `causal_patch_survival_pct`
  - `malformed_proposals_at_cap`
  - `rollback_attribution_complete_pct`
  - `terminal_unactionable_qids`
  - `accuracy_delta`
  - `decision_trace_completeness_pct`
  - `rca_loop_closure_pct`
- Failure buckets with next-action labels:
  - `EVIDENCE_GAP`
  - `RCA_GAP`
  - `PROPOSAL_GAP`
  - `TARGETING_GAP`
  - `GATE_OR_CAP_GAP`
  - `APPLY_OR_ROLLBACK_GAP`
  - `MODEL_CEILING`
- Three initial trace-aware extractions:

| Order | Extraction | New module | Why low-risk |
|---|---|---|---|
| 1 | Eval entry & classification | `optimization/eval_entry.py` | Already pure; emits journey + decision records. |
| 2 | AG outcome wiring | `optimization/ag_outcome.py` | Contract helpers already isolated this; now emits decision records too. |
| 3 | Post-eval transition | `optimization/post_eval.py` | Same pattern; produces qid result and RCA loop observed effect. |

**Validation strategy:** each extraction is its own commit. Replay tests assert canonical journey, decision trace, scoreboard, and transcript snapshots are byte-stable before vs after each extraction.

**Exit criterion:** scoreboard and failure buckets render from `OptimizationTrace`; every unresolved qid has a bucket and next action; first three extractions land without changing journey or decision snapshots.

**Real-Genie runs:** 0.

**Detailed plans:** all three implemented:
- [`2026-05-04-operator-scoreboard-plan.md`](./2026-05-04-operator-scoreboard-plan.md) — implemented.
- [`2026-05-04-failure-bucketing-classifier-plan.md`](./2026-05-04-failure-bucketing-classifier-plan.md) — implemented.
- [`2026-05-04-harness-extractions-phase-1-plan.md`](./2026-05-04-harness-extractions-phase-1-plan.md) — implemented.

---

## Phase D.5 — Pre-merge polish: alternatives capture

**Why insert here:** the transcript today says "the strategist picked AG_X" without saying what AG_Y and AG_Z it rejected, and why. Same for cluster formation and proposal generation. Until alternatives are typed, an operator who sees "wrong AG selected" cannot tell whether the strategist (a) only ever saw one AG (a wiring problem), (b) saw two but rejected the better one for a bad reason (a logic problem), or (c) had the right reasoning but the proposal pipeline downstream was the actual defect. This is the highest-leverage small follow-up: it changes the postmortem question from "which stage misreasoned?" to "which stage rejected option Y for reason Z, and was that reason wrong?".

**What ships:**

- New `AlternativeOption` typed dataclass on `rca_decision_trace.py` carrying `option_id`, `kind` (`cluster` | `ag` | `proposal`), `score` (optional float), `reject_reason` (typed enum), and `reject_detail` (free-form short string).
- New `alternatives_considered: tuple[AlternativeOption, ...]` field on `DecisionRecord`, included in canonical JSON serialization with stable sort order.
- Producer extensions in `decision_emitters.py`:
  - `cluster_records` accepts and stamps cluster alternatives (candidate clusters that were not promoted to hard).
  - `strategist_ag_records` accepts and stamps AG alternatives (AGs the strategist returned but were filtered or buffered).
  - `proposal_generated_records` accepts and stamps proposal alternatives (proposals dropped pre-survival: malformed, target-cap, RCA-ungrounded).
- Caller-side capture at the three sites in `harness.py` so the rejected options reach the producers.
- `render_operator_transcript` surfaces alternatives in sections 3 (RCA cards), 4 (AG decisions), and 5 (proposal survival) when present.
- New cross-projection replay test that pins alternatives ordering byte-stably.

**Validation strategy:**

- Pure unit tests for the dataclass + `AlternativeOption` serialization, the three producer extensions with empty / single / multi-alternative scenarios, and renderer snapshots showing alternatives.
- Replay byte-stability: a synthetic fixture exercising 3 clusters (1 selected / 2 rejected), 4 AGs (2 emitted / 2 filtered), 5 proposals (3 surviving / 2 dropped) produces a stable canonical decision trace.
- No real-Genie cycles required; the existing `airline_real_v1.json` keeps passing because alternatives default to empty.

**Exit criterion:** every `CLUSTER_SELECTED`, `STRATEGIST_AG_EMITTED`, and `PROPOSAL_GENERATED` record either carries alternatives (when the upstream had alternatives to consider) or carries an empty tuple with a reason (e.g. "single candidate"). Transcript sections 3/4/5 render alternatives when present. Replay tests pass byte-stably.

**Real-Genie runs:** 0.

**Detailed plan:** [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md).

---

## Phase E.0 — MLflow artifact integrity audit

**Why insert here:** Phase E's pilot-run validation depends on `phase_a/journey_validation/iter_<N>.json` and `phase_b/decision_trace/iter_<N>.json` + `phase_b/operator_transcript/iter_<N>.txt` artifacts being reliably present on a run an operator can navigate to. Spot inspection of a recent post-D run shows the operator-visible eval child run (`iter_04 / full_eval / pass_1`) has only `evaluation_runtime/`, `judge_prompts/`, `model_snapshots/` — the decision-trail artifacts are absent. The persistence calls exist at `harness.py:17241` and `harness.py:17311-17319` but the harness rotates the active MLflow run via `end_run` / `start_run` between stages (see `harness.py:12557-12562`), so `mlflow.active_run()` at persistence time is whichever stage run was last started, not the parent optimization run. Combined with silent `except Exception → logger.debug` catches, persistence failures and stale-run-anchor problems both disappear without trace.

**What ships:**

- Read-only MLflow audit CLI `gso-mlflow-audit --opt-run-id <id>` that lists all MLflow runs sharing the `genie.optimization_run_id` tag, dumps artifact paths for each, and reports where (if anywhere) `phase_a/`/`phase_b/` artifacts landed.
- Stable per-iteration anchor for decision-trail artifacts: replace `mlflow.active_run()`-based persistence with explicit `MlflowClient().log_text(run_id=<resolved_anchor>, ...)` so artifacts always land on a deterministic run regardless of which stage happened to be active.
- New `GSO_PHASE_A_ARTIFACT_V1` and `GSO_PHASE_B_ARTIFACT_V1` stdout markers carrying `success`, `run_id`, `artifact_path`, and `exception_class` per persistence attempt — silent failures become loud.
- Promotion of the relevant `logger.debug` catches to `logger.warning` in `harness.py:17228, 17256, 17328, 17396, 17419` after the underlying causes are diagnosed.
- Backfill CLI `gso-mlflow-backfill --opt-run-id <id>` that reads the persisted replay fixture and rebuilds + uploads `phase_a/journey_validation/`, `phase_b/decision_trace/`, and `phase_b/operator_transcript/` artifacts to the resolved anchor for already-completed runs.
- Smoke regression test using a `mlflow.set_tracking_uri("file://...")` stub that runs one iteration and asserts the expected artifact paths exist on the expected run.

**Validation strategy:**

- The audit CLI is read-only — running it against existing runs is the diagnostic.
- The anchoring fix is unit-tested with a stubbed MLflow client.
- The smoke test is the regression rail for the anchoring fix.
- Backfill is hand-verified against the screenshot's `iter_04` run before merging.

**Exit criterion:** for every iteration of every Phase E candidate run, `phase_a/journey_validation/iter_<N>.json`, `phase_b/decision_trace/iter_<N>.json`, and `phase_b/operator_transcript/iter_<N>.txt` are present on a single, operator-discoverable MLflow run. Stdout markers confirm successful persistence per artifact. The audit CLI returns zero discrepancies for fresh runs and a documented backfill plan for legacy runs.

**Real-Genie runs:** 0 dedicated. The next Phase E candidate pilot run validates the fix end-to-end.

**Detailed plan:** [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md).

---

## Phase E — Final integration + merge

**What happens:**

1. Run one real Lever Loop on the airline benchmark (~2 hours).
2. Confirm:
   - Zero validator warnings.
   - Decision trace is complete for every iteration.
   - Operator transcript renders with iteration summary, RCA cards, AG decisions, proposal survival, gate reasons, acceptance/rollback, unresolved buckets, and next suggested action.
   - A failed iteration can be diagnosed from the operator transcript plus linked trace JSON without grepping raw logs or reading `harness.py`.
   - Scoreboard renders with sensible numbers.
   - Bucketing labels look right (spot-check 3–5 unresolved qids manually).
   - RCA loop state is present for every unresolved qid.
   - `GSO_ARTIFACT_INDEX_V1` and `dbutils.notebook.exit(...)` identify the parent MLflow run, iteration eval runs, strategy runs, logged model ids, and `gso_postmortem_bundle/` paths.
   - `gso_postmortem_bundle/operator_transcript.md` is readable as a process ledger for humans.
   - `gso_postmortem_bundle/decision_trace_all.json` and per-iteration `decision_trace.json` artifacts are sufficient for LLM postmortem analysis.
   - No accuracy regression vs the variance baseline captured during Phase A burn-down.
3. **Flip `raise_on_violation=True`** in `harness.py` (the journey contract becomes a hard gate on every future run).
4. Add a decision-trace hard-gate check for required decision records on replay. Missing journey emits, missing decision records, missing RCA/evidence fields, and stdout/trace drift should all fail closed.
5. Open the deliberately-broken sanity PR for CI verification: intentionally drop one `_emit_ag_outcome_journey` call or one required decision record in a test branch, watch CI fail with a clear contract violation, then close the PR. This proves the gates are wired correctly and CI catches regressions.
6. Merge the feature branch.

**Exit criterion:** PR merged; journey and decision gates are live; CI fails closed on missing emits or missing required decisions.

**Real-Genie runs:** 1.

---

## Phase F — Stage-aligned `harness.py` modularization (post-merge)

**Why sixth:** Phases A–E and the PR-A→E batch made every stage-level decision typed and traceable, but the **executable code** remains a monolith — `harness.py` is ~19,900 LOC, `optimizer.py` is ~15,600 LOC, and 6 of 10 process stages still live inside one of those two files. The decision-emitter producers in `decision_emitters.py` are already stage-aligned (`eval_classification_records`, `cluster_records`, `rca_formed_records`, `strategist_ag_records`, `proposal_generated_records`, four gate producers, `patch_applied_records`, `ag_outcome_decision_record`, `post_eval_resolution_records`, AG_RETIRED via PR-B2). The `PROCESS_STAGE_ORDER` taxonomy in [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) is already locked. Phase F finally aligns the source code with the same nine-stage process so the LLM postmortem maps a `decision_type` from stdout to exactly one source file — and so Phase H can capture per-stage I/O automatically.

**Why post-merge:** these are behavior-preserving refactors with zero algorithmic change. They do not need to block the merge of the contract gate, the scoreboard, or the bucketing classifier. They land as nine small, individually reviewable, individually reversible PRs on `main`, each gated by byte-stable replay.

**Architecture — `optimization/stages/` package:**

```
src/genie_space_optimizer/optimization/
  stages/
    __init__.py          # StageHandler protocol, StageContext, ProcessStageKey re-exports
    evaluation.py        # F1 — stages 1 + 8: evaluate_baseline + evaluate_post_patch
    rca_evidence.py      # F2 — stage 2: judge / ASI / sql-diff / counterfactual evidence
    clustering.py        # F3 — stage 3: cluster_failures + RCA card formation
    action_groups.py     # F4 — stage 4: strategist invocation + AG selection + lane lock
    proposals.py         # F5 — stage 5: synthesis + cluster-driven synthesis
    gates.py             # F6 — stage 6: lever-5 / blast-radius / groundedness / DOA / dedup
    application.py       # F7 — stage 7: apply + immediate rollback verification
    acceptance.py        # F8 — stage 9: control plane + iteration acceptance + AG outcome
    learning.py          # F9 — stage 10: reflection buffer, do-not-retry, content-fingerprint blocklist, terminal resolution, AG_RETIRED
  harness.py             # ~2k-LOC orchestration spine (down from ~19,900) reading as a linear tape over the 9 stages
```

Each `stages/<stage>.py` module exposes:

- A typed `StageInput` dataclass (frozen in Phase G).
- A typed `StageOutput` dataclass (frozen in Phase G).
- A single `execute(ctx: StageContext, inp: StageInput) -> StageOutput` entry point.
- Ownership of the corresponding `decision_emitters.py` producer(s).
- A per-stage replay fixture asserting byte-stable I/O.

**Order matters (lowest-risk first):**

| Order | Plan | Stage(s) | New module | Decision producer it owns | Phase A/B/C/D/PR-A→E precondition that makes it safe |
|---|---|---|---|---|---|
| F1 | [`2026-05-04-phase-f1-stages-skeleton-and-evaluation-plan.md`](./2026-05-04-phase-f1-stages-skeleton-and-evaluation-plan.md) | `evaluation_state` + `post_patch_evaluation` | `stages/evaluation.py` (+ `stages/__init__.py` skeleton) | `eval_classification_records` | `eval_entry.py` + `post_eval.py` already extracted in Phase D. |
| F2 | [`2026-05-04-phase-f2-rca-evidence-stage-extraction-plan.md`](./2026-05-04-phase-f2-rca-evidence-stage-extraction-plan.md) | `rca_evidence` | `stages/rca_evidence.py` | (feeds `rca_formed_records`) | Phase C RCA loop contract: every failure has typed evidence. |
| F3 | [`2026-05-04-phase-f3-clustering-stage-extraction-plan.md`](./2026-05-04-phase-f3-clustering-stage-extraction-plan.md) | `cluster_formation` | `stages/clustering.py` | `cluster_records`, `rca_formed_records` | Phase D.5 alternatives capture: rejected clusters are typed. |
| F4 | [`2026-05-04-phase-f4-action-groups-stage-extraction-plan.md`](./2026-05-04-phase-f4-action-groups-stage-extraction-plan.md) | `action_group_selection` | `stages/action_groups.py` | `strategist_ag_records` | Phase B AG decisions carry source clusters and rationale; Phase D.5 stamps rejected AGs. |
| F5 | [`2026-05-04-phase-f5-proposals-stage-extraction-plan.md`](./2026-05-04-phase-f5-proposals-stage-extraction-plan.md) | `proposal_generation` | `stages/proposals.py` | `proposal_generated_records` | Phase C target-qid propagation; Phase D.5 stamps rejected proposals. |
| F6 | [`2026-05-04-phase-f6-gates-stage-extraction-plan.md`](./2026-05-04-phase-f6-gates-stage-extraction-plan.md) | `safety_gates` | `stages/gates.py` | `lever5_structural_gate_records`, `blast_radius_decision_records`, `groundedness_gate_records`, `dead_on_arrival_decision_records`, content-fingerprint dedup (PR-E) | All four gate producers + PR-E content-fingerprint dedup landed. |
| F7 | [`2026-05-04-phase-f7-application-stage-extraction-plan.md`](./2026-05-04-phase-f7-application-stage-extraction-plan.md) | `applied_patches` | `stages/application.py` | `patch_applied_records` | Phase B `PATCH_APPLIED` / `PATCH_SKIPPED` records complete. |
| F8 | [`2026-05-04-phase-f8-acceptance-stage-extraction-plan.md`](./2026-05-04-phase-f8-acceptance-stage-extraction-plan.md) | `acceptance_decision` | `stages/acceptance.py` | `ag_outcome_decision_record`, `post_eval_resolution_records` | `ag_outcome.py` + `post_eval.py` already extracted; PR-E pre-arbiter acceptance branch landed. |
| F9 | [`2026-05-04-phase-f9-learning-stage-extraction-plan.md`](./2026-05-04-phase-f9-learning-stage-extraction-plan.md) | `learning_next_action` | `stages/learning.py` | terminal records + `AG_RETIRED` (PR-B2) | PR-B2 typed termination + AG_RETIRED, PR-E content-fingerprint blocklist landed. |

**Validation strategy:** identical to Phase D. Each extraction is its own commit. The replay test (a hard gate post-Phase E) asserts byte-identical journey ledger, decision trace, scoreboard snapshot, and operator transcript before vs after. If anything reorders, CI fails closed and the commit is rolled back.

**Exit criterion:** all nine stage modules land on `main`; replay byte-stable across each; total LoC reduction in `harness.py` ≈ 14,000–16,000 lines (from ~19,900 post-merge to ~3,500–5,500 orchestration spine). The remaining `harness.py` reads as a linear tape over the nine stage modules — the loop body becomes:

```python
for iter_num in range(1, max_iterations + 1):
    eval_result = stages.evaluation.evaluate_post_patch(ctx, state.space)
    evidence    = stages.rca_evidence.collect(ctx, eval_result)
    clusters    = stages.clustering.form(ctx, evidence)
    slate       = stages.action_groups.select(ctx, clusters, state.reflection)
    proposals   = stages.proposals.generate(ctx, slate, state.space_snapshot)
    gated       = stages.gates.filter(ctx, proposals, evidence, state.applied_history)
    applied     = stages.application.apply(ctx, gated, state.space_snapshot)
    post_eval   = stages.evaluation.evaluate_post_patch(ctx, applied.space)
    outcome     = stages.acceptance.decide(ctx, applied, post_eval)
    learning    = stages.learning.update(ctx, outcome, ctx.terminal_status)
    state       = state.advance(applied, post_eval, outcome, learning)
```

**Real-Genie runs:** 0. Replay test is the only gate.

**Detailed plans:** [`2026-05-04-phase-f-stages-modularization-index.md`](./2026-05-04-phase-f-stages-modularization-index.md) sequences F1 through F9, with one plan per stage module.

**Why the stage-aligned shape matters:** when stdout points at `decision_type=GATE_DECISION reason=blast_radius`, the LLM postmortem can navigate to exactly `stages/gates.py` and read its `StageInput`/`StageOutput` rather than chasing through 4 files. The eight existing decision-emitter producers are already stage-aligned; Phase F finishes the alignment by pulling the executable code into the same shape.

---

## Phase G — Stage Protocol + registry + RunEvaluationKwargs (G-lite)

**Why after Phase F:** Phase F gives every stage module a typed
`StageInput` / `StageOutput` and a single named-verb entry point.
Phase G in its **lite** form (the original full-freeze + mypy-strict
scope was ruled out after a cost/benefit review) adds three small
contract surfaces that Phase H's per-stage I/O capture builds on:

1. `@runtime_checkable` on `StageHandler` so conformance can be
   asserted via `isinstance(module, StageHandler)`.
2. A uniform `execute` callable on every stage module (alias of the
   named verb).
3. A `stages/_registry.py` exposing `STAGES: tuple[StageEntry, ...]`
   in canonical 9-stage process order — the iteration target Phase H
   wraps with its capture decorator.
4. A `RunEvaluationKwargs` `TypedDict` closing the F1 weak point
   (`eval_kwargs: dict[str, Any]`).
5. A conformance test pinning every stage module's `STAGE_KEY` and
   Protocol satisfaction.
6. A smoke test pinning F8's `ag_outcome.py` / `post_eval.py`
   deletions.

**Why "lite" instead of full freeze + mypy strict:** the F-plan
replay byte-stability tests already catch behavioral regressions
in stage modules. Adding `frozen=True, slots=True` to every stage
Input/Output dataclass introduces real breakage risk (subclassing,
pickling, mutation sites the audit missed) for marginal contract-
safety gain. Adding `mypy --strict` ratcheting introduces a
permanent maintenance tax (false positives, `# type: ignore`
proliferation) on top of a codebase that is fundamentally
probabilistic (LLM-driven RCA, judges, ASI). Both are deferred
until a real bug motivates them. If a specific stage shows a
mutation bug, freeze that one stage in a focused follow-up plan.

**What ships:** see [`2026-05-04-phase-g-stage-protocol-and-registry-plan.md`](./2026-05-04-phase-g-stage-protocol-and-registry-plan.md).

**Validation strategy:** every existing F-plan replay byte-stability
test continues to pass (G-lite is annotation-only at the harness
call sites). New unit tests cover Protocol conformance, registry
shape and lookup, RunEvaluationKwargs TypedDict shape, and F8
no-resurrection.

**Exit criterion:** the registry is importable from
`stages/__init__.py`; every stage module satisfies the
runtime-checkable `StageHandler` Protocol; the F1 weak point
(`eval_kwargs: dict[str, Any]`) is replaced by `RunEvaluationKwargs`
at all three sites; F8 deletions stay deleted.

**Real-Genie runs:** 0. Replay + unit tests are the only gates.

**Detailed plan:** [`2026-05-04-phase-g-stage-protocol-and-registry-plan.md`](./2026-05-04-phase-g-stage-protocol-and-registry-plan.md).

**Calendar:** ~1-2 days.

**What's out of scope (explicitly deferred):**

- Frozen + slots on stage Input/Output dataclasses.
- `mypy --strict` per-stage ratcheting.
- `LoopContext` and `IterationState` typed dataclasses.
- Per-stage `to_dict()` / `from_dict()` round-trip tests.

If any of these is later motivated by a real bug, file a focused
follow-up plan rather than re-opening the full Phase G scope.

---

## Phase H — GSO Run Output Contract: final unification on top of F + G

**Why last:** the live run `407772af-9662-4803-be6b-f00a368c528a` proved that the loop can already improve a Genie Space, but humans and LLMs still have to stitch together stdout, notebook exit JSON, MLflow eval runs, strategy runs, logged model snapshots, and local evidence bundles manually. Phase H formalizes the **GSO Run Output Contract** as the final unification: a process-first human transcript, a parent-run `gso_postmortem_bundle/` with per-stage I/O capture, a `GSO_ARTIFACT_INDEX_V1` stdout marker for CLI discovery, and a `gso-postmortem` skill that consumes the bundle as the one-stop troubleshooting package. **Phase H exists because Phases F and G make it small.** Without per-stage modules, capturing per-stage I/O would require parsing `harness.py`. With per-stage typed `execute(ctx, inp) -> out` calls, the harness wraps each call with one decorator that dumps `inp` and `out` to `iter_NN/stages/<stage_key>/`. The bundle becomes a deterministic projection of typed stage I/O.

**Why this is the end goal:** every preceding phase converges on a single reader-facing artifact:

```text
RCA Evidence -> Cluster -> Action Group -> Proposal -> Gate
  -> Applied Patch -> Eval Result -> Learning
```

After Phase H lands, this process is rendered to humans (`operator_transcript.md`), to LLMs (typed JSON per stage), to CLI tools (`GSO_ARTIFACT_INDEX_V1` + `dbutils.notebook.exit(...)` pointers), and to evidence bundles (`gso_postmortem_bundle/`). The same data — stage I/O captured in F+G — projects to all four surfaces. There is one source of truth.

**What ships:**

- A formal **GSO Run Output Contract** rooted in the standard loop:

  ```text
  RCA Evidence -> Cluster -> Action Group -> Proposal -> Gate
    -> Applied Patch -> Eval Result -> Learning
  ```

- A process-first `operator_transcript.md` for humans. Each iteration renders the same stage order, with each stage including `What happened` (concrete facts), `Why this stage exists` (educational), and `Input -> Decision -> Output` (the process transition).
- A parent-run `gso_postmortem_bundle/` in MLflow as the one-stop LLM troubleshooting package:

  ```text
  gso_postmortem_bundle/
    manifest.json
    run_summary.json
    artifact_index.json
    operator_transcript.md
    decision_trace_all.json
    journey_validation_all.json
    replay_fixture.json
    scoreboard.json
    failure_buckets.json
    iterations/
      iter_01/
        summary.json
        operator_transcript.md
        decision_trace.json
        journey_validation.json
        rca_ledger.json
        proposal_inventory.json
        patch_survival.json
        stages/                          # NEW — populated automatically from F+G stage I/O
          01_evaluation/
            input.json
            output.json
            decisions.json
          02_rca_evidence/...
          03_clustering/...
          04_action_groups/...
          05_proposals/...
          06_gates/...
          07_application/...
          08_acceptance/...
          09_learning/...
  ```

- Iteration eval runs continue to store iteration-local artifacts and metrics. The parent bundle assembles the right subset so postmortem starts from one place without losing MLflow-native lineage.
- Logged models store candidate/champion state only: config snapshots, applied patches, source iteration run id. They do not become the one-stop troubleshooting store.
- A new `GSO_ARTIFACT_INDEX_V1` stdout marker and pointer-rich `dbutils.notebook.exit(...)` fields so `databricks jobs get-run-output <lever_loop_task_run_id>` can locate the parent bundle and linked iteration artifacts even when stdout is truncated.
- `evidence_bundle` pulls the parent bundle into `docs/runid_analysis/<optimization_run_id>/evidence/gso_postmortem_bundle/` before falling back to legacy phase artifacts or raw notebook output.
- `gso-postmortem` consumes the evidence bundle, not live ad hoc log scraping.
- Per-stage I/O capture decorator wired into the harness's stage call sites — wraps every `stages.<stage>.execute(ctx, inp)` and writes `inp` + `out` + producer-emitted decisions to MLflow under `gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/`.

**Validation strategy:**

- Unit tests for artifact path constants, run-role tags, artifact-index markers, marker parsing, evidence-bundle local layout, parent-bundle assembly, and MLflow audit coverage.
- Snapshot-style tests for the process transcript stage order and stage descriptions.
- A lightweight smoke test proves `GSO_ARTIFACT_INDEX_V1` points from CLI-visible stdout to the parent bundle.
- Replay tests assert the per-stage `iter_NN/stages/<stage_key>/{input.json,output.json,decisions.json}` payloads are byte-stable.
- The Phase H plan is implementation-independent of any other phase: it can be implemented before, during, or after any other roadmap phase, but it becomes substantially smaller after F+G land because per-stage I/O capture replaces ad hoc rendering.

**Exit criterion:** a completed lever-loop task exposes enough CLI-visible pointers to locate the parent MLflow `gso_postmortem_bundle/`; the parent bundle contains a readable human transcript, typed LLM artifacts, and per-stage I/O captures; `evidence_bundle` materializes the bundle locally; and `gso-postmortem` can produce a postmortem without grepping raw task output unless the manifest declares a missing artifact.

**Real-Genie runs:** 0 dedicated. The next post-Phase H pilot validates the full path in the workspace.

**Detailed plan:** [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) — the existing plan remains the implementation blueprint. Two task additions land in Phase H itself: (1) wire the per-stage I/O capture decorator into `harness.py`'s stage call sites, and (2) extend `bundle_artifact_paths(...)` to enumerate `iter_NN/stages/<stage_key>/{input.json,output.json,decisions.json}`.

---

## Real-Genie cost summary

| Phase | Real runs | Wall time |
|---|---|---|
| 0 (verification smoke) | 0–1 actual | ~2 hr |
| A (burn-down, cycles 1–9) | 9 actual | ~18 hr actual |
| B (unified trace + transcript) | 0 | 0 |
| C (RCA loop reliability) | 0–1 actual | ~0–2 hr |
| D (scoreboard, bucketing, first extractions) | 0 | 0 |
| D.5 (alternatives capture) | 0 | 0 |
| PR-A → PR-E (pre-rerun bug-fix batch) | 1 actual (run `407772af`) | ~2 hr actual |
| E.0 (MLflow artifact integrity audit) | 0 (replay-only) | 0 |
| E (final integration) | 1 | ~2 hr |
| F (stage-aligned modularization, replay-only, post-merge) | 0 | 0 |
| G (per-stage typed contracts, replay-only, post-merge) | 0 | 0 |
| H (GSO Run Output Contract unification, replay-only, post-merge) | 0 | 0 |
| **Pre-merge total** | **10–11** | **~22–24 hr (~22 hr already spent)** |
| **Post-merge follow-up (F → G → H)** | **0** | **0** |

Calendar estimate from the current point: ~1 additional week pre-merge (E.0 + E pilot) with journey and decision gates live at the end, then ~2–3 weeks of small post-merge PRs for Phase F (one per stage module), ~1-2 days for Phase G-lite (Stage Protocol + registry + RunEvaluationKwargs), and ~1 week for Phase H to wire per-stage I/O capture and ship the parent-run `gso_postmortem_bundle/`.

---

## What's parked (deliberate)

- **Generic key/value handoff table.** Phase 0 widens `genie_opt_runs` with 3 columns instead — minimal change, matches existing data shape. Revisit only if a future task needs handoff for arbitrary keys not already in Delta.
- **Restart-from-checkpoint inside `_run_lever_loop`.** The harness already resumes via `load_latest_full_iteration`. Phase 0 only fixes notebook-level state handoff, not harness internals.
- **Hand-synthesized fixture extensions.** Replaced by real-fixture capture at the end of Phase A (more honest, less work).
- **Further orchestration-spine decomposition beyond Phase F/G/H.** After Phase F, the remaining `harness.py` should be a ~3,500–5,500-LOC orchestration spine that reads as a linear tape over the nine `stages/<stage>.py` modules. Phase G improves the contracts crossing that spine. Phase H wires per-stage I/O capture into the spine. Splitting the spine itself further is parked unless the spine becomes hard to reason about after H lands.
- **Typed contract redesign before Phase G.** Strong typing is the desired endpoint, but it is deliberately delayed until after the burn-down, merge gate, and byte-stable extractions. Before then, only add narrow types that support the active phase without reshaping module boundaries.
- **Dashboarding beyond stdout/MLflow artifacts.** Phase B standardizes the operator transcript first. Rich dashboards can follow once the trace contract is stable and persisted consistently.
- **Ad hoc diagnostic print blocks in `harness.py`.** New operator-visible diagnostics should be typed trace producers plus centralized transcript renderer sections. Freeform prints are parked unless they are temporary migration shims removed by the same phase.

---

## Concrete next action

**Phases 0 → D, Phase D.5, and the PR-A → PR-E batch are complete.** One pre-merge phase remains before Phase E flips the merge gate:

1. **Phase E.0 — MLflow artifact integrity audit.** Required because Phase E's pilot validation depends on `phase_a/`/`phase_b/` artifacts being present on an operator-discoverable run, and current spot inspection shows they are not. ~9 TDD tasks across audit/anchoring/backfill phases. Plan ready at [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md).

E.0 is replay-only during implementation. The next real-Genie cycle is the **Phase E pilot run**, which validates the PR-A→E fixes in a live run, validates E.0 artifact integrity, gates the `raise_on_violation=True` flip, and produces the merge-baseline fixture for `gso-replay-cycle-intake` to lock.

**After Phase E merges**, the post-merge work runs in three stages: Phase F (9 small stage extractions, replay-gated), Phase G (per-stage typed contracts, replay + mypy gated), and Phase H (GSO Run Output Contract unification on top of F+G). The Phase F index is at [`2026-05-04-phase-f-stages-modularization-index.md`](./2026-05-04-phase-f-stages-modularization-index.md).

---

## Cross-references

| Plan | Status | Phase it serves |
|---|---|---|
| [`2026-05-01-lever-loop-lossless-contract-and-replay-gate-plan.md`](./2026-05-01-lever-loop-lossless-contract-and-replay-gate-plan.md) | Implemented; warn-only | A (burn-down operates on it) |
| [`2026-05-01-cross-task-state-resilience-plan.md`](./2026-05-01-cross-task-state-resilience-plan.md) | Implemented; smoke verification pending | 0 (Repair Run fix) |
| [`2026-05-01-phase-a-contract-burndown-plan.md`](./2026-05-01-phase-a-contract-burndown-plan.md) | Implemented | A (this plan) |
| [`2026-05-01-phase-a-burndown-log.md`](./2026-05-01-phase-a-burndown-log.md) | Captured | A (high-level close summary) |
| [`2026-05-02-run-replay-per-iteration-fix-plan.md`](./2026-05-02-run-replay-per-iteration-fix-plan.md) | Implemented | A (replay-engine fix that drove cycle-7→8 burn-down to 0) |
| [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md) | Captured | A (per-cycle ledger and detail) |
| [`2026-05-02-cycle7-reconstruction-postmortem.md`](./2026-05-02-cycle7-reconstruction-postmortem.md) | Captured | A (cycles 1-7 fixture-shape postmortem) |
| [`2026-05-02-cycle8-side-bugs-high-level-plan.md`](./2026-05-02-cycle8-side-bugs-high-level-plan.md) | Drafted | C (qid extraction and target-qid propagation gaps) |
| [`high level plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md`](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) | Implemented | A (consolidated 16-track Phase A plan) |
| [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md) | Tasks 1-7 shipped; remaining scope subsumed by the cycle-9 close + delta plan | B |
| [`2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md`](./2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md) | Implemented | A (post-close burndown) and partial B/C/D pre-shipping |
| [`2026-05-03-phase-b-decision-trace-completion-plan.md`](./2026-05-03-phase-b-decision-trace-completion-plan.md) | Ready | B (delta — closes Phase B) |
| [`2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`](./2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md) | Implemented | C |
| [`2026-05-04-operator-scoreboard-plan.md`](./2026-05-04-operator-scoreboard-plan.md) | Implemented | D |
| [`2026-05-04-failure-bucketing-classifier-plan.md`](./2026-05-04-failure-bucketing-classifier-plan.md) | Implemented | D |
| [`2026-05-04-harness-extractions-phase-1-plan.md`](./2026-05-04-harness-extractions-phase-1-plan.md) | Implemented | D |
| [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md) | Implemented | D.5 |
| [`2026-05-03-merge-readiness-pre-rerun-plans-index.md`](./2026-05-03-merge-readiness-pre-rerun-plans-index.md) | Implemented (6/6 PRs landed) | PR-A → PR-E |
| [`2026-05-03-pr-a-replay-pasted-fixture-validation-plan.md`](./2026-05-03-pr-a-replay-pasted-fixture-validation-plan.md) | Implemented | PR-A |
| [`2026-05-03-pr-b1-evidence-bundle-notebook-output-fallback-plan.md`](./2026-05-03-pr-b1-evidence-bundle-notebook-output-fallback-plan.md) | Implemented | PR-B1 |
| [`2026-05-03-pr-b2-lever-loop-termination-vocab-and-ag-retirement-plan.md`](./2026-05-03-pr-b2-lever-loop-termination-vocab-and-ag-retirement-plan.md) | Implemented | PR-B2 |
| [`2026-05-03-pr-c-lane-aware-journey-validator-and-fixture-persistence-plan.md`](./2026-05-03-pr-c-lane-aware-journey-validator-and-fixture-persistence-plan.md) | Implemented | PR-C |
| [`2026-05-03-pr-d-rca-classifier-top-n-cardinality-routing-plan.md`](./2026-05-03-pr-d-rca-classifier-top-n-cardinality-routing-plan.md) | Implemented | PR-D |
| [`2026-05-03-pr-e-pre-arbiter-secondary-acceptance-and-reflection-dedup-plan.md`](./2026-05-03-pr-e-pre-arbiter-secondary-acceptance-and-reflection-dedup-plan.md) | Implemented | PR-E |
| [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md) | Ready | E.0 |
| [`2026-05-04-phase-f-stages-modularization-index.md`](./2026-05-04-phase-f-stages-modularization-index.md) | Ready | F (9-plan index) |
| [`2026-05-04-phase-f1-stages-skeleton-and-evaluation-plan.md`](./2026-05-04-phase-f1-stages-skeleton-and-evaluation-plan.md) | Ready | F1 (skeleton + evaluation) |
| [`2026-05-04-phase-f2-rca-evidence-stage-extraction-plan.md`](./2026-05-04-phase-f2-rca-evidence-stage-extraction-plan.md) | Ready | F2 (RCA evidence) |
| [`2026-05-04-phase-f3-clustering-stage-extraction-plan.md`](./2026-05-04-phase-f3-clustering-stage-extraction-plan.md) | Ready | F3 (clustering) |
| [`2026-05-04-phase-f4-action-groups-stage-extraction-plan.md`](./2026-05-04-phase-f4-action-groups-stage-extraction-plan.md) | Ready | F4 (action groups) |
| [`2026-05-04-phase-f5-proposals-stage-extraction-plan.md`](./2026-05-04-phase-f5-proposals-stage-extraction-plan.md) | Ready | F5 (proposals) |
| [`2026-05-04-phase-f6-gates-stage-extraction-plan.md`](./2026-05-04-phase-f6-gates-stage-extraction-plan.md) | Ready | F6 (gates) |
| [`2026-05-04-phase-f7-application-stage-extraction-plan.md`](./2026-05-04-phase-f7-application-stage-extraction-plan.md) | Ready | F7 (application) |
| [`2026-05-04-phase-f8-acceptance-stage-extraction-plan.md`](./2026-05-04-phase-f8-acceptance-stage-extraction-plan.md) | Ready | F8 (acceptance) |
| [`2026-05-04-phase-f9-learning-stage-extraction-plan.md`](./2026-05-04-phase-f9-learning-stage-extraction-plan.md) | Ready | F9 (learning) |
| [`2026-05-04-phase-g-stage-protocol-and-registry-plan.md`](./2026-05-04-phase-g-stage-protocol-and-registry-plan.md) | Implemented (G-lite scope) | G |
| [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) | Ready (final unification) | H |
| [`skills/gso-lever-loop-run-analysis/SKILL.md`](./skills/gso-lever-loop-run-analysis/SKILL.md) | Ready | B/C/D/E run analysis |
| [`skills/gso-replay-cycle-intake/SKILL.md`](./skills/gso-replay-cycle-intake/SKILL.md) | Ready | A burn-down ledger intake |
