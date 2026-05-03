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

Eight sequential phases (0 → A → B → C → D → E → F → G). Phase A establishes a clean per-iteration journey contract and real replay fixture. Phase B introduces the unified `OptimizationTrace` / `DecisionRecord` contract and standard operator transcript. Phase C hardens the RCA loop itself. Phase D builds scoreboard and failure bucketing as projections of the same trace while starting low-risk extractions. Phase E flips the hard gate and merges. Phases F/G finish modularization and tighten typed contracts.

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

## At a glance

| # | Phase | Replay-only? | Real-Genie runs | Calendar | Branch state |
|---|---|---|---|---|---|
| 0 | Cross-task state resilience (Repair Run fix) | No | 0–1 | ~2–3 days | pre-merge |
| A | Contract burn-down + real-fixture capture **(✅ complete — 2026-05-02)** | No | 8 (cycles 1-8) | ~1 day actual (estimate was 3-5 days) | pre-merge |
| B | Unified trace + DecisionRecord + operator transcript | Yes | 0 | ~3–5 days | pre-merge |
| C | RCA loop reliability hardening | Mixed | 0–1 | ~3–5 days | pre-merge |
| D | Scoreboard, failure bucketing, and first trace-aware extractions | Yes | 0 | ~4–6 days | pre-merge |
| E | Final integration + contract-gate flip + merge | No | 1 | ~1 day | merge point |
| F | Deeper `harness.py` modularization (5 byte-stable extractions) | Yes | 0 | ~5–8 days | post-merge follow-up |
| G | Typed contract hardening for extracted modules | Yes | 0 | ~5–10 days | post-merge architecture follow-up |
| | **Pre-merge total** | | **9–11 runs (~18–22 hrs, 8 already spent)** | **~2–3 weeks** | |
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

**Real-Genie runs:** 8 cycles actual. Phase A is complete as of 2026-05-02; see [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md).

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

**Detailed plan:** [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md) — ready for implementation.

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

**Detailed plans:** to be written as:
- `2026-05-XX-operator-scoreboard-plan.md`
- `2026-05-XX-failure-bucketing-classifier-plan.md`
- `2026-05-XX-harness-extractions-phase-1-plan.md`

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

**Implement Phase B — Unified trace + DecisionRecord + operator transcript.** Phases 0 and A are complete (see [`2026-05-01-phase-a-burndown-log.md`](./2026-05-01-phase-a-burndown-log.md) for the close summary and [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md) for the per-iter detail). The airline corpus's journey-contract validation count is 0; `airline_real_v1.json` is committed with `expected_canonical_journey` (365 events, 38 706 bytes) and gated by `test_run_replay_airline_real_v1_within_burndown_budget` (budget=0). Phase B is replay-only, requires zero real-Genie cycles, and should define the canonical decision-trace schema before scoreboard or bucketing work begins. Detailed plan is ready at [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md).

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
| [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md) | Ready | B |
| `2026-05-XX-rca-loop-contract-plan.md` | To be written | C |
| `2026-05-XX-canonical-qid-extraction-plan.md` | To be written | C |
| `2026-05-XX-target-qid-propagation-plan.md` | To be written | C |
| `2026-05-XX-operator-scoreboard-plan.md` | To be written | D |
| `2026-05-XX-failure-bucketing-classifier-plan.md` | To be written | D |
| `2026-05-XX-harness-extractions-phase-1-plan.md` | To be written | D |
| `2026-05-XX-harness-extractions-phase-2-plan.md` | To be written | F |
| `2026-05-XX-lever-loop-typed-contract-hardening-plan.md` | To be written | G |
| [`skills/gso-lever-loop-run-analysis/SKILL.md`](./skills/gso-lever-loop-run-analysis/SKILL.md) | Ready after implementation | B/C/D+ run analysis |
