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

Eight sequential phases (0 → A → B → C → D → E → F → G), with two pre-merge observability inserts (E.0 and E.1). Phase A establishes a clean per-iteration journey contract and real replay fixture. Phase B introduces the unified `OptimizationTrace` / `DecisionRecord` contract and standard operator transcript. Phase C hardens the RCA loop itself. Phase D builds scoreboard and failure bucketing as projections of the same trace while starting low-risk extractions. Phase E.0 makes MLflow decision artifacts reliable, and Phase E.1 formalizes the GSO Run Output Contract so humans, CLI tools, and LLM postmortem skills all consume the same RCA-grounded process. Phase E flips the hard gate and merges. Phases F/G finish modularization and tighten typed contracts.

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
| 0 | Cross-task state resilience (Repair Run fix) | No | 0–1 | ~2–3 days | pre-merge |
| A | Contract burn-down + real-fixture capture **(✅ complete — 2026-05-02; cycle-9 post-close burndown landed 2026-05-03)** | No | 9 (cycles 1-8 + post-close cycle 9) | ~1 day actual (estimate was 3-5 days) | pre-merge |
| B | Unified trace + DecisionRecord + operator transcript **(✅ complete — 10/10 producers shipped via cycle 9 + delta)** | Yes | 0 | shipped | pre-merge |
| C | RCA loop reliability hardening **(✅ complete — RCA loop contract, residuals, target-qid propagation landed)** | Mixed | 0–1 | shipped | pre-merge |
| D | Scoreboard, failure bucketing, and first trace-aware extractions **(✅ complete — 3/3 plans landed)** | Yes | 0 | shipped | pre-merge |
| **D.5** | **Pre-merge polish — alternatives capture (cluster / AG / proposal)** | Yes | 0 | ~2–3 days | pre-merge |
| **E.0** | **MLflow artifact integrity audit + persistence fixes** | Mostly | 0 (replay-only) + 1 backfill smoke | ~2–3 days | pre-merge prerequisite for E |
| **E.1** | **GSO Run Output Contract - centralized human + LLM observability** | Mostly | 0 (validated by next E pilot) | ~2–3 days | pre-merge prerequisite for E |
| E | Final integration + contract-gate flip + merge | No | 1 | ~1 day | merge point |
| F | Deeper `harness.py` modularization (5 byte-stable extractions) | Yes | 0 | ~5–8 days | post-merge follow-up |
| G | Typed contract hardening for extracted modules | Yes | 0 | ~5–10 days | post-merge architecture follow-up |
| | **Pre-merge total** | | **10–12 runs (~20–24 hrs, 9 already spent)** | **~3–4 weeks** | |
| | **Post-merge follow-up** | | 0 | ~2–3 weeks | |

## Why this sequencing

Seven reasons it has to be in this order:

1. **Phase 0 unblocks iteration cadence.** Repair Run has to carry enough state for short re-runs; otherwise every burn-down cycle costs a full 2-hour DAG.
2. **Phase A makes replay trustworthy.** The real fixture now validates per iteration, persists `journey_validation`, keeps raw cycle fixtures, and has a CI budget tightened to zero. That gives every later phase a deterministic safety rail.
3. **Phase B must precede more observability.** Scoreboards, failure buckets, and stdout should not each invent their own schema. A canonical `DecisionRecord` first makes every later rendering a projection of one source of truth.
4. **Phase C makes RCA reliability first-class.** The optimizer is only useful if evidence, root cause, causal patch, targeted qids, observed effect, and learned next action form a closed loop. Cycle 8's `target_qids: []` and GT-correction qid loss bugs are symptoms of that contract not being explicit enough.
5. **Phase D can then build operator UX safely.** Scoreboard, bucketing, and initial extractions become consumers of `OptimizationTrace`, not parallel log parsers.
6. **Phase F remains byte-stable modularization.** The deeper extractions should still be behavior-preserving PRs, but they should move trace-aware subsystems, not opaque dict mutation blocks.
7. **Phase G tightens APIs after modules exist.** Strong contracts become easier and safer once the code has coherent homes. Phase G should harden the module APIs while preserving persisted Delta and replay compatibility.

**Current guardrail:** avoid adding new substantial helpers directly to `harness.py`. If a helper is a reusable domain operation or grows beyond roughly 30–50 LOC, put it in the module it will eventually belong to and import it into `harness.py`. New instrumentation must add `DecisionRecord` / `OptimizationTrace` producers or renderer sections, not freeform print/log blocks.

---

## Open Gaps and Future Work — Diagnosability rubric

The end-state target is: **given a stdout/stderr from a Lever Loop job, an operator can identify which module's reasoning was off and fix it**. Phases 0–D get the program ~70–80% there. The remaining 20–30% is tracked here explicitly so it doesn't fall off the radar.

### Diagnosability scorecard (as of 2026-05-03)

| Property | Grade | Concrete gap | Phase that closes it |
|---|---|---|---|
| **Per-qid RCA log** is typed and complete | A− (~90%) | Alternatives aren't captured — when the strategist picks AG_X over AG_Y, only AG_X gets a record. Same for clustering and proposal generation. | Phase D.5 |
| **Cluster formation rationale** is first-class | C+ (~50%) | `cluster_records` stamps the chosen cluster but not "why this clustering vs another". | Phase D.5 |
| **Modularized code** (defect → one file) | C+ (~40%) | 5 of 10 stages still live in `harness.py` / `optimizer.py` / `synthesis.py` / `applier.py`. | Phase F |
| **Stdout-only diagnosability** | B (~70%) | (a) Hard gate not yet flipped; (b) MLflow artifacts (`phase_a/`, `phase_b/`) appear missing on operator-visible runs — see Phase E.0. | Phase E.0 + E |
| **Stderr-only diagnosability** | F (~10%) | Stderr today is mostly Python tracebacks, not contract reasoning. The transcript lives in stdout + MLflow artifacts. | Out of scope; not needed if stdout + artifacts are reliable. |

### Stage → module localization map

When stdout points at a `decision_type`, today this is where the reasoning lives. Phase F closes the right column.

| `decision_type` | Producer (`decision_emitters.py`) | Reasoning today | Reasoning after Phase F |
|---|---|---|---|
| `EVAL_CLASSIFIED` | `eval_classification_records:102` | `eval_entry.py` ✅ | same |
| `CLUSTER_SELECTED` | `cluster_records:161` | `harness.py` + `rca.py` | `rca_clustering.py` |
| `RCA_FORMED` | `rca_formed_records:220` | `harness.py` + `rca.py` | `rca_clustering.py` |
| `STRATEGIST_AG_EMITTED` | `strategist_ag_records:284` | `harness.py` + `optimizer.py` | `strategist_invocation.py` |
| `PROPOSAL_GENERATED` | `proposal_generated_records:380` | `harness.py` + `synthesis.py` + `cluster_driven_synthesis.py` | `proposal_pipeline.py` |
| `GATE_DECISION` (Lever 5 / blast radius) | `lever5_structural_gate_records:855` / `blast_radius_decision_records:776` | `harness.py` + `applier.py` | `applier_rollback.py` |
| `PATCH_APPLIED` / `PATCH_SKIPPED` | `patch_applied_records:466` | `harness.py` + `applier.py` | `applier_rollback.py` |
| `ACCEPTANCE_DECIDED` | `ag_outcome_decision_record:592` | `ag_outcome.py` ✅ | same |
| `QID_RESOLUTION` | `post_eval_resolution_records:677` | `post_eval.py` ✅ | same |

Stages with ✅ are already module-precise from a stdout marker. The remaining five collapse to one module each after Phase F.

### Pre-merge gap closures (Phases D.5 and E.0)

Before flipping the merge gate at Phase E, two pre-merge phases close gaps that a Phase E pilot run would otherwise immediately surface:

- **Phase D.5 — Alternatives capture.** Adds `alternatives_considered: tuple[AlternativeOption, ...]` to `DecisionRecord` and stamps it on `CLUSTER_SELECTED`, `STRATEGIST_AG_EMITTED`, and `PROPOSAL_GENERATED`. Transforms transcript reasoning from "this stage chose X" to "this stage chose X over {Y, Z} because of {reason_Y, reason_Z}". Plan: [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md).
- **Phase E.0 — MLflow artifact integrity audit.** Phase A claims to persist `phase_a/journey_validation/iter_<N>.json` and Phase B claims to persist `phase_b/decision_trace/iter_<N>.json` + `phase_b/operator_transcript/iter_<N>.txt`. Spot inspection of `iter_04 / full_eval / pass_1 / run_d6a7faeb` shows only `evaluation_runtime/`, `judge_prompts/`, `model_snapshots/` — the decision-trail artifacts are not visible on the run an operator naturally clicks into. The persistence calls exist (`harness.py:17241`, `17312-17319`) but route to whichever MLflow run was last started by the harness's `end_run` / `start_run` pattern. E.0 audits where artifacts actually land, anchors them to a stable per-iteration parent run, surfaces silent persistence failures, and adds a backfill CLI for completed runs. Plan: [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md).
- **Phase E.1 - GSO Run Output Contract.** The live run `407772af-9662-4803-be6b-f00a368c528a` proved that the loop can improve a space while still leaving humans and LLMs to stitch together stdout, notebook exit JSON, MLflow eval runs, strategy runs, logged model snapshots, and local evidence bundles manually. E.1 formalizes the output shape: a process-first human transcript, an artifact-index marker for CLI discovery, iteration-local MLflow artifacts, and a parent-run `gso_postmortem_bundle/` that `evidence_bundle` and `gso-postmortem` consume as the one-stop troubleshooting package. Plan: [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md).

### Future work explicitly on the radar (post-Phase F)

- **Per-stage module attribution field on `DecisionRecord`.** Once Phase F lands, add a `module: str` field (e.g. `"rca_clustering"`, `"proposal_pipeline"`) so transcript readers don't need the stage→module table above. Doable as a one-task PR after Phase F.
- **Phase G typed contracts** — already on the roadmap; surfaces strong types after module boundaries are real.
- **Production observability dashboard** — currently parked. Phase B's stable artifact structure is a precondition; E.0's anchoring fix is a prerequisite for dashboards to point at the right run.

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

## Phase E.1 - GSO Run Output Contract: centralized human + LLM observability

**Why insert here:** Phase E.0 makes artifact persistence reliable, but it does not by itself define what the run output should look like once those artifacts are reliable. The live run `407772af-9662-4803-be6b-f00a368c528a` showed the gap: the raw task output contained enough information to diagnose the loop, but it was too large, partly truncated, and not organized as the RCA process. LLMs also need a stable way to discover parent MLflow runs, iteration eval runs, strategy runs, logged model artifacts, and local evidence bundles without guessing from raw stdout.

**What ships:**

- A formal **GSO Run Output Contract** rooted in the standard loop:

  ```text
  RCA Evidence -> Cluster -> Action Group -> Proposal -> Gate
    -> Applied Patch -> Eval Result -> Learning
  ```

- A process-first `operator_transcript.md` for humans. Each iteration renders the same stage order, and each stage includes a short explanation of what happened and why the stage exists so a new operator can follow the optimizer end to end.
- A parent-run `gso_postmortem_bundle/` in MLflow as the one-stop LLM troubleshooting package:
  - `manifest.json`
  - `run_summary.json`
  - `artifact_index.json`
  - `operator_transcript.md`
  - `decision_trace_all.json`
  - `journey_validation_all.json`
  - `replay_fixture.json`
  - `scoreboard.json`
  - `failure_buckets.json`
  - `iterations/iter_<N>/*`
- Iteration eval runs continue to store iteration-local artifacts and metrics. The parent bundle assembles the right subset so postmortem starts from one place without losing MLflow-native lineage.
- Logged models store candidate/champion state only: config snapshots, applied patches, and source iteration run id. They do not become the one-stop troubleshooting store.
- A new `GSO_ARTIFACT_INDEX_V1` stdout marker and pointer-rich `dbutils.notebook.exit(...)` fields so `databricks jobs get-run-output <lever_loop_task_run_id>` can locate the parent bundle and linked iteration artifacts even when stdout is truncated.
- `evidence_bundle` pulls the parent bundle into `docs/runid_analysis/<optimization_run_id>/evidence/gso_postmortem_bundle/` before falling back to legacy phase artifacts or raw notebook output.
- `gso-postmortem` consumes the evidence bundle, not live ad hoc log scraping.

**Validation strategy:**

- Unit tests for artifact path constants, run-role tags, artifact-index markers, marker parsing, evidence-bundle local layout, parent-bundle assembly, and MLflow audit coverage.
- Snapshot-style tests for the process transcript stage order and stage descriptions.
- A lightweight smoke test proves `GSO_ARTIFACT_INDEX_V1` points from CLI-visible stdout to the parent bundle.
- No dedicated real-Genie run. The next Phase E pilot validates the full path in the workspace.

**Exit criterion:** a completed lever-loop task exposes enough CLI-visible pointers to locate the parent MLflow `gso_postmortem_bundle/`; the parent bundle contains a readable human transcript and typed LLM artifacts; `evidence_bundle` materializes the bundle locally; and `gso-postmortem` can produce a postmortem without grepping raw task output unless the manifest declares a missing artifact.

**Real-Genie runs:** 0 dedicated. The next Phase E candidate pilot validates the fix end to end.

**Detailed plan:** [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md).

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

## Phase F — Deeper `harness.py` modularization (post-merge)

**Why sixth:** Phase D extracted three lowest-risk subsystems. The five remaining subsystems carry most of `harness.py`'s remaining mass and most of its cross-module coupling. Phase A's journey contract plus Phases B/C's decision trace and RCA loop contracts make their input/output behavior explicit and testable, so each one becomes a small PR gated by byte-stable replay.

**Why post-merge:** these are isolated refactors with zero behavior change. They do not need to block the merge of the contract gate, the scoreboard, or the bucketing classifier. They land as a sequence of small follow-up PRs on `main`, each individually reviewable and reversible.

**What Phase F is not:** Phase F is not the "strongly typed modular harness" endpoint. It is the prerequisite for that endpoint. The extraction PRs must preserve behavior and call shape closely enough for replay to keep journey, decision, scoreboard, and transcript snapshots byte-identical, so they may still carry today's `dict[str, Any]` payloads, mutation-through-shared-state patterns, and wide call signatures. Phase G tightens those contracts after the code has coherent homes.

**Order matters (lowest-risk first, same logic as Phase D):**

| Order | Extraction | New module | Phase A precondition that makes it safe |
|---|---|---|---|
| 1 | RCA & clustering | `optimization/rca_clustering.py` | RCA loop contract: evidence, root cause, cluster, and RCA card are traceable. |
| 2 | Strategist invocation | `optimization/strategist_invocation.py` | AG decisions carry source clusters, affected qids, and rationale. |
| 3 | Proposal pipeline | `optimization/proposal_pipeline.py` | Proposal decisions carry causal targets, lineage, and malformed/gate reasons. |
| 4 | Application / rollback | `optimization/applier_rollback.py` | Gate, apply, rollback, and survival decisions are trace-complete. |
| 5 | Acceptance gating | `optimization/acceptance_gate.py` | Acceptance decisions carry target wins, regressions, rollback trust, and learned next action. |

**Validation strategy:** identical to Phase D. Each extraction is its own commit. The replay test (now a hard gate post-Phase E) asserts byte-identical journey ledger, decision trace, scoreboard snapshot, and operator transcript before vs after. If anything reorders, CI fails closed and the commit is rolled back.

**Exit criterion:** all five extractions land on `main`; replay byte-stable across each; total LoC reduction in `harness.py` of an additional ~6000–8000 lines (from ~12k post-Phase D to ~4–6k). The remaining `harness.py` is the orchestration spine — the loop body, lever ordering, and inter-module wiring — but the extracted modules may still expose legacy-shaped contracts until Phase G.

**Real-Genie runs:** 0. Replay test is the only gate.

**Detailed plan:** to be written as `2026-05-XX-harness-extractions-phase-2-plan.md` once Phase D lands and the replay gate is hard.

**Why the eventual Phase F is realistic, not aspirational:** the [combined Phase A burn-down plan](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) made the journey surface replayable; Phases B/C add the missing decision and RCA contracts. Together, those make the five deep subsystems independently extractable without asking reviewers to trust a giant behavioral refactor.

---

## Phase G — Typed contract hardening (post-extraction architecture)

**Why after Phase F:** After Phase F, the harness behavior is stable, replay-gated, and split across coherent modules. That is the first point where it is safe to change API shape deliberately. Phase G turns byte-stable modules into strongly typed, LLM-legible subsystems with explicit contracts, focused responsibilities, and narrow public surfaces.

**Why not earlier:** Typed contract redesign before Phase A/E mixes two failure modes: missing journey emits and API redesign regressions. Typed contract redesign before Phase F also forces contract choices around code that still lives inside a large orchestration file. Phase G waits until the behavior is stable and the module boundaries are real.

**What ships:**

- A small contract model layer for the lever loop, likely under `optimization/contracts.py` or `optimization/lever_loop_contracts.py`, with frozen dataclasses or Pydantic models for the state that crosses module boundaries.
- Module APIs converted from wide positional signatures and open-ended dictionaries to kwargs-only typed inputs/outputs.
- Explicit typed objects for recurring concepts such as:
  - `LoopContext` — run IDs, space IDs, catalog/schema, warehouse, apply mode, lever set, and feature flags.
  - `IterationState` — iteration number, baseline/candidate accuracy, hard qids, cluster assignments, and terminal state.
  - `OptimizationTrace` — owned container for journey events, decision records, validation reports, and projections.
  - `DecisionRecord` — canonical record for an optimizer choice with evidence refs, RCA, root cause, causal targets, expected effect, observed effect, regression qids, reason code, and next action.
  - `RcaLoopState` — evidence, root cause, causal patch intent, expected fix, observed result, and learned next action.
  - `ProposalBatch` — proposals, parent/child lineage, cap decisions, malformed counts, and gate outcomes.
  - `PatchApplicationResult` — applied patches, rejected patches, rollback metadata, and survival attribution.
  - `AcceptanceDecision` — accepted/rolled-back decision, reason class, regression debt, and accuracy delta.
  - `JourneyLedger` — typed wrapper around journey events plus validation report accessors.
  - `ScoreboardSnapshot` — scoreboard inputs and rendered operator-facing metrics.
  - `OperatorTranscript` — deterministic pretty stdout projection for replay, MLflow artifacting, and human review.
- Per-module type-checking ratcheted in gradually. Start with the extracted modules; do not require strict typing across all of legacy `harness.py` on day one.
- Compatibility adapters only where needed to keep persisted Delta payloads and replay fixtures stable. The public module APIs should become typed; persisted schemas should remain backward-compatible unless a separate migration plan says otherwise.

**Validation strategy:**

- Each module gets its own typed-contract PR after its Phase F extraction has landed.
- Unit tests prove the typed model constructors reject malformed inputs and preserve the current happy-path shape.
- Replay tests continue to assert journey, decision, scoreboard, and transcript behavior does not regress.
- Type checking is opt-in per hardened module first, then expanded. The end state is enforced typing for the extracted modules and a much smaller untyped allowance for the remaining orchestration spine.

**Exit criterion:** the eight extracted modules expose typed public APIs; `harness.py` orchestrates typed contracts rather than building and mutating open-ended dictionaries at every boundary; LLM-assisted edits can target one module plus its contract tests without loading the whole lever-loop harness into context.

**Real-Genie runs:** 0. Replay + unit + type checks are the gates.

**Detailed plan:** to be written as `2026-05-XX-lever-loop-typed-contract-hardening-plan.md` after Phase F's module boundaries are real.

---

## Real-Genie cost summary

| Phase | Real runs | Wall time |
|---|---|---|
| 0 (verification smoke) | 0–1 | ~2 hr |
| A (burn-down, cycles 1–8) | 8 actual | ~16 hr actual |
| B (unified trace + transcript) | 0 | 0 |
| C (RCA loop reliability) | 0–1 | ~0–2 hr |
| D (scoreboard, bucketing, first extractions) | 0 | 0 |
| E (final integration) | 1 | ~2 hr |
| F (deeper modularization, replay-only, post-merge) | 0 | 0 |
| G (typed contract hardening, replay-only, post-merge) | 0 | 0 |
| **Pre-merge total** | **9–11** | **~18–22 hr** |
| **Post-merge follow-up** | **0** | **0** |

Calendar estimate from the current point: ~1–2 additional weeks pre-merge with journey and decision gates live at the end, then ~1 week of small post-merge PRs for Phase F, then ~1–2 weeks of typed-contract hardening for Phase G.

---

## What's parked (deliberate)

- **Generic key/value handoff table.** Phase 0 widens `genie_opt_runs` with 3 columns instead — minimal change, matches existing data shape. Revisit only if a future task needs handoff for arbitrary keys not already in Delta.
- **Restart-from-checkpoint inside `_run_lever_loop`.** The harness already resumes via `load_latest_full_iteration`. Phase 0 only fixes notebook-level state handoff, not harness internals.
- **Hand-synthesized fixture extensions.** Replaced by real-fixture capture at the end of Phase A (more honest, less work).
- **Further orchestration-spine decomposition beyond Phase F/G.** After Phase F, the remaining `harness.py` should be the orchestration spine. Phase G improves the contracts crossing that spine. Splitting the spine itself further is parked unless the spine becomes hard to reason about after typed contracts land.
- **Typed contract redesign before Phase G.** Strong typing is the desired endpoint, but it is deliberately delayed until after the burn-down, merge gate, and byte-stable extractions. Before then, only add narrow types that support the active phase without reshaping module boundaries.
- **Dashboarding beyond stdout/MLflow artifacts.** Phase B standardizes the operator transcript first. Rich dashboards can follow once the trace contract is stable and persisted consistently.
- **Ad hoc diagnostic print blocks in `harness.py`.** New operator-visible diagnostics should be typed trace producers plus centralized transcript renderer sections. Freeform prints are parked unless they are temporary migration shims removed by the same phase.

---

## Concrete next action

**Phases 0 → D are complete.** Two pre-merge phases remain before Phase E flips the merge gate:

1. **Phase D.5 — Alternatives capture.** Highest-leverage small follow-up. ~9 TDD tasks; replay-only; closes the "this stage misreasoned" → "this stage rejected option Y for reason Z" diagnostic gap. Plan ready at [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md).
2. **Phase E.0 — MLflow artifact integrity audit.** Required because Phase E's pilot validation depends on `phase_a/`/`phase_b/` artifacts being present on an operator-discoverable run, and current spot inspection shows they are not. ~9 TDD tasks across audit/anchoring/backfill phases. Plan ready at [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md).

Both phases are replay-only during implementation. The next real-Genie cycle is the **Phase E pilot run**, which validates D.5 transcripts and E.0 artifact integrity in one shot, gates the `raise_on_violation=True` flip, and produces the merge-baseline fixture for `gso-replay-cycle-intake` to lock.

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
| [`2026-05-04-pre-phase-e-alternatives-capture-plan.md`](./2026-05-04-pre-phase-e-alternatives-capture-plan.md) | Ready | D.5 |
| [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md) | Ready | E.0 |
| `2026-05-XX-harness-extractions-phase-2-plan.md` | To be written | F |
| `2026-05-XX-lever-loop-typed-contract-hardening-plan.md` | To be written | G |
| [`skills/gso-lever-loop-run-analysis/SKILL.md`](./skills/gso-lever-loop-run-analysis/SKILL.md) | Ready | B/C/D/E run analysis |
| [`skills/gso-replay-cycle-intake/SKILL.md`](./skills/gso-replay-cycle-intake/SKILL.md) | Ready | A burn-down ledger intake |
