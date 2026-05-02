# Burn-Down to Merge — Roadmap

> **Status:** Working roadmap for the `fix/gso-lossless-contract-replay-gate` feature branch. This is the **high-level program plan** that ties together the stabilization path to merge, the byte-stable post-merge modularization path, and the follow-on typed-contract hardening needed to make the lever-loop harness rationally extensible for LLM-assisted iteration. Each phase has (or will have) its own detailed implementation plan; this document is the strategic narrative.

## Goal

Land three pieces of optimizer instrumentation — operator scoreboard, failure bucketing classifier, three `harness.py` extractions — on top of the already-merged lossless contract + replay gate, validated primarily against the deterministic <30s replay test, with the journey contract flipped to a **hard CI gate** at the merge point. Then land the **deeper `harness.py` modularization** (RCA & clustering, strategist invocation, proposal pipeline, application/rollback, acceptance gating) as a sequence of small, byte-stable post-merge PRs. Finally, after the code has safe module boundaries, tighten those boundaries into typed contracts so the remaining harness is an orchestration spine and each subsystem can be understood, tested, and iterated on independently.

## Architecture in one sentence

Eight sequential phases (0 → A → B → C → D → E → F → G). Phases 0 and A are the only pre-merge phases that exercise real Genie. Phases B/C/D and Phase F run entirely against the deterministic replay. Phase E is one final real-Genie validation before merge. Phase F is byte-stable post-merge modularization. Phase G is post-extraction typed-contract hardening: it deliberately changes API shape, but only after Phase F has moved behavior into coherent modules and the hard replay gate can prove behavior did not regress.

## At a glance

| # | Phase | Replay-only? | Real-Genie runs | Calendar | Branch state |
|---|---|---|---|---|---|
| 0 | Cross-task state resilience (Repair Run fix) | No | 0–1 | ~2–3 days | pre-merge |
| A | Contract burn-down + real-fixture capture **(✅ complete — 2026-05-02)** | No | 8 (cycles 1-8) | ~1 day actual (estimate was 3-5 days) | pre-merge |
| B | Operator scoreboard | Yes | 0 | ~3–4 days | pre-merge |
| C | Failure bucketing classifier | Yes | 0 | ~3–4 days | pre-merge |
| D | Three initial `harness.py` extractions | Yes | 0 | ~2–3 days | pre-merge |
| E | Final integration + contract-gate flip + merge | No | 1 | ~1 day | merge point |
| F | Deeper `harness.py` modularization (5 byte-stable extractions) | Yes | 0 | ~5–8 days | post-merge follow-up |
| G | Typed contract hardening for extracted modules | Yes | 0 | ~5–10 days | post-merge architecture follow-up |
| | **Pre-merge total** | | **2–4 runs (~4–8 hrs)** | **~2 weeks** | |
| | **Post-merge follow-up** | | 0 | ~2–3 weeks | |

## Why this sequencing

Six reasons it has to be in this order:

1. **Phase 0 unblocks the iteration cadence for Phase A.** Burn-down is the loop "deploy → run → triage validator warnings → fix emits → re-run." Without Phase 0, every "re-run" is a 2-hour full DAG. With Phase 0 (Repair Run works), it's ~20 minutes.
2. **Phase A produces the real fixture that Phases B/C depend on.** Hand-synthesized fixtures are biased toward what we expect to find, not what the real loop actually emits. Capturing real `_journey_events` after a clean burn-down is the more honest validation surface for byte-stable replay.
3. **Phase D's byte-stability check needs the real fixture in place.** Refactoring against a synthetic fixture that doesn't fully exercise the pipeline lets regressions slip through. Real fixture → real regression detection.
4. **Phase F (deeper modularization) needs Phase A's contract work + Phase D's byte-stable replay gate already on `main`.** The five Phase F extractions (RCA, strategist, proposal pipeline, applier/rollback, acceptance gate) are only safe and tiny because the [combined Phase A burn-down plan](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) makes their input/output contracts explicit and the Phase D gate proves byte stability for any harness extraction. Doing F before E and Phase A would re-introduce the cross-module coupling we just spent Phase A removing.
5. **Phase F is intentionally not the typed-architecture endpoint.** It is a byte-stable lift-and-shift that moves coherent subsystems out of `harness.py` without changing behavior. That gives reviewers tiny, reversible PRs, but it preserves today's dict shapes, positional arguments, and mutation patterns where those exist.
6. **Phase G needs Phase F's module boundaries.** Strong typed contracts should be introduced after behavior has been moved into focused modules. Tightening contracts before the burn-down and extraction gates are green would mix behavioral stabilization with API redesign, making failures harder to attribute.

B and C technically have no inter-dependency — they could be parallel. We sequence them because the scoreboard naturally calls into the bucketing classifier for the `terminal_unactionable_qids` list, so building the scoreboard first makes the bucketing wiring smaller.

**Current guardrail:** during Phase A burn-down, avoid adding new substantial helpers directly to `harness.py`. If a new helper is purely local and small, keep it near the call site. If it is a reusable domain operation or grows beyond roughly 30–50 LOC, put it in the domain module it will eventually belong to (or a small new module) and import it into `harness.py`. This prevents more random helper accretion while preserving the strict Phase A sequencing.

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

Without burn-down, Phase B's scoreboard math is built on a journey ledger that's still missing events — `causal_patch_survival_pct` would be wrong by construction.

**What ships:**

1. Whatever event-emit fixes the burn-down surfaces. These are bug fixes against the existing contract module, not new architecture. Each is a small, focused commit.
2. A short burn-down log: "after N runs on the airline corpus, validator reports 0 violations."
3. **One critical artifact**: at the end of the clean run, dump `_journey_events` to JSON and commit it as `tests/replay/fixtures/airline_real_v1.json`. Run `scripts/record_replay_baseline.py` to compute the canonical ledger.

**Why a real-captured fixture, not a hand-synthesized one?** Capturing real loop output is faster (no design work), unbiased (it exercises every event the real loop *actually* emits, not what we *expected* it to emit), and refreshable (when the loop's emit set legitimately changes, re-capture and commit). The hand-synthesized fixture extension that earlier drafts proposed is dropped.

Logic correctness for B and C is validated by **pure-function unit tests over synthetic events** — fixture work is only for end-to-end replay byte-stability.

**Exit criterion:** Clean burn-down on airline corpus + `airline_real_v1.json` committed and referenced by the existing replay test.

**Real-Genie runs:** 1–3 (depends on how many emit gaps surface).

---

## Phase B — Operator scoreboard

**Why third:** Now that the journey ledger is complete (Phase A) and the replay fixture is real (Phase A), the scoreboard can be computed and validated. Scoreboard is the operator-facing UX layer on top of the contract — it makes "did this run actually do something?" answerable in 5 seconds instead of 5 minutes of grepping.

**What ships:**

- New module `optimization/scoreboard.py` exposing a `Scoreboard` dataclass and `build_scoreboard(...)` pure function.
- Seven leading metrics computed from `JourneyValidationReport` + `PatchSurvivalSnapshot` + the terminal-state classifier:
  - `journey_completeness_pct`
  - `hard_cluster_coverage_pct`
  - `causal_patch_survival_pct`
  - `malformed_proposals_at_cap` (count, must be 0)
  - `rollback_attribution_complete_pct`
  - `terminal_unactionable_qids` (named list)
  - `accuracy_delta` with variance band from baseline doc
- Rendered at end of `_run_lever_loop` alongside the existing journey ledger.
- Persisted to existing eval Delta tables for trend analysis.

**Validation strategy:**

- **Logic correctness:** ~30 unit tests over synthetic event lists — one test per metric edge case. No fixture work needed.
- **Byte-stability:** the existing replay test asserts the scoreboard for `airline_real_v1.json` matches a committed expected snapshot.

**Exit criterion:** Scoreboard renders on every replay run; numeric values match expected snapshot; ~5–7 tasks committed.

**Real-Genie runs:** 0.

**Detailed plan:** to be written as `2026-05-XX-operator-scoreboard-plan.md` once Phase A is complete (the real fixture's exact event set informs the metric edge cases).

---

## Phase C — Failure bucketing classifier

**Why fourth:** Scoreboard tells you `terminal_unactionable_qids: [q017, q032, q041]`. The bucketing classifier tells you *why* each one is unresolved.

| Bucket | Diagnostic question | Operator next action |
|---|---|---|
| `EVIDENCE_GAP` | Did any judge fire on this qid? | Fix the judge / add coverage |
| `PROPOSAL_GAP` | Was a cluster formed but no proposal generated? | Inspect strategist; add cluster-driven synthesis |
| `GATE_OR_CAP_GAP` | Was a proposal generated but capped/gated/firewalled? | Tune the gate; relax the cap |
| `MODEL_CEILING` | Did everything fire correctly and the model still didn't fix it? | Out of optimizer's scope — escalate to human review or different patch type |

This is the diagnostic that turns "the loop terminated unactionable" into a concrete next action.

**What ships:**

- New module `optimization/failure_bucketing.py` exposing a pure `classify_unresolved_qid(events, ag_outcomes) -> FailureBucket` function.
- Four-bucket enum.
- Wired into the scoreboard's `terminal_unactionable_qids` so each qid carries its bucket label.

**Validation strategy:**

- **Logic correctness:** ~5–10 unit tests per bucket over synthetic event lists. No fixture work needed.
- **Byte-stability:** replay test asserts each unresolved qid's bucket assignment in `airline_real_v1.json` matches the expected snapshot.

**Exit criterion:** Every unresolved hard qid has a bucket; ~5–7 tasks committed.

**Real-Genie runs:** 0.

**Detailed plan:** to be written as `2026-05-XX-failure-bucketing-classifier-plan.md`.

---

## Phase D — Three initial `harness.py` extractions

**Why fifth:** `harness.py` is ~14,907 lines and growing. Modularization makes the file holdable in context for future agentic edits. Phase D picks the three lowest-risk, highest-leverage extractions that the contract helpers already factored out. Phase F (post-merge) finishes the job.

**Order matters (lowest-risk first):**

| Order | Extraction | New module | Why low-risk |
|---|---|---|---|
| 1 | Eval entry & classification | `optimization/eval_entry.py` | Already a pure function; lift-and-shift |
| 2 | AG outcome wiring | `optimization/ag_outcome.py` | Contract helpers (`_emit_ag_outcome_journey`, etc.) already isolated this |
| 3 | Post-eval transition | `optimization/post_eval.py` | Same pattern as 2 |

**Validation strategy:** each extraction is its own commit. Replay test asserts the canonical journey ledger is **byte-identical** before vs after each extraction. If even one event reorders, the diff fails the replay test and the commit is rolled back. This is the same byte-stability guarantee that makes Phase A's real fixture so valuable here.

**Exit criterion:** Three extractions land; replay byte-stable across each; total LoC reduction in `harness.py` ~1500–2500 lines.

**Real-Genie runs:** 0.

**Detailed plan:** to be written as `2026-05-XX-harness-extractions-phase-1-plan.md`.

**Phase F (post-merge) finishes byte-stable modularization** with five deeper extractions. Phase G then tightens the extracted module contracts. See Phase F and Phase G below.

---

## Phase E — Final integration + merge

**What happens:**

1. Run one real Lever Loop on the airline benchmark (~2 hours).
2. Confirm:
   - Zero validator warnings.
   - Scoreboard renders with sensible numbers.
   - Bucketing labels look right (spot-check 3–5 unresolved qids manually).
   - No accuracy regression vs the variance baseline captured during Phase A burn-down.
3. **Flip `raise_on_violation=True`** in `harness.py` (the journey contract becomes a hard gate on every future run).
4. Open the deliberately-broken sanity PR for CI verification: intentionally drop one `_emit_ag_outcome_journey` call in a test branch, watch CI fail with a clear contract violation, then close the PR. This proves the gate is wired correctly and CI catches regressions.
5. Merge the feature branch.

**Exit criterion:** PR merged; contract gate is live; CI fails closed on missing emits.

**Real-Genie runs:** 1.

---

## Phase F — Deeper `harness.py` modularization (post-merge)

**Why sixth:** Phase D extracted three lowest-risk subsystems. The five remaining subsystems carry most of `harness.py`'s remaining mass and most of its cross-module coupling. Phase A's lossless contract + the [combined Phase A burn-down plan](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) make their input/output contracts explicit and testable, so each one becomes a tiny PR gated by byte-stable replay — the same gate Phase D uses.

**Why post-merge:** these are isolated refactors with zero behavior change. They do not need to block the merge of the contract gate, the scoreboard, or the bucketing classifier. They land as a sequence of small follow-up PRs on `main`, each individually reviewable and reversible.

**What Phase F is not:** Phase F is not the "strongly typed modular harness" endpoint. It is the prerequisite for that endpoint. The extraction PRs must preserve behavior and call shape closely enough for the replay ledger to stay byte-identical, so they may still carry today's `dict[str, Any]` payloads, mutation-through-shared-state patterns, and wide call signatures. Phase G tightens those contracts after the code has coherent homes.

**Order matters (lowest-risk first, same logic as Phase D):**

| Order | Extraction | New module | Phase A precondition that makes it safe |
|---|---|---|---|
| 1 | RCA & clustering | `optimization/rca_clustering.py` | Track D (stable cluster signatures across iterations) + Track 2 (`_BEHAVIOR_ROOT_CAUSES` complete). |
| 2 | Strategist invocation | `optimization/strategist_invocation.py` | Track D (AG signature stability) + Track 4 (AG decomposition guardrail) + lever-directive drift closed. |
| 3 | Proposal pipeline | `optimization/proposal_pipeline.py` | Track 1 (proposal-to-patch metadata contract) + Track B (split-child propagation). |
| 4 | Application / rollback | `optimization/applier_rollback.py` | Track A (cap conservation) + Track C (patch-family budgeting) + Tracks 3/E (survival truthfulness). |
| 5 | Acceptance gating | `optimization/acceptance_gate.py` | Track F (acceptance predicate reorder) + Tracks 3/E/F/I (audit-log consistency). |

**Validation strategy:** identical to Phase D. Each extraction is its own commit. The replay test (now a hard gate post-Phase E) asserts byte-identical journey ledger before vs after. If anything reorders, CI fails closed and the commit is rolled back.

**Exit criterion:** all five extractions land on `main`; replay byte-stable across each; total LoC reduction in `harness.py` of an additional ~6000–8000 lines (from ~12k post-Phase D to ~4–6k). The remaining `harness.py` is the orchestration spine — the loop body, lever ordering, and inter-module wiring — but the extracted modules may still expose legacy-shaped contracts until Phase G.

**Real-Genie runs:** 0. Replay test is the only gate.

**Detailed plan:** to be written as `2026-05-XX-harness-extractions-phase-2-plan.md` once Phase D lands and the replay gate is hard.

**Why the eventual Phase F is realistic, not aspirational:** the [combined Phase A burn-down plan](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) is specifically scoped to make these five subsystems independently extractable. Each Phase A track maps to one of the Phase F preconditions in the table above. After Phase A, the cross-module coupling that currently makes these five modules risky is gone.

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
  - `ProposalBatch` — proposals, parent/child lineage, cap decisions, malformed counts, and gate outcomes.
  - `PatchApplicationResult` — applied patches, rejected patches, rollback metadata, and survival attribution.
  - `AcceptanceDecision` — accepted/rolled-back decision, reason class, regression debt, and accuracy delta.
  - `JourneyLedger` — typed wrapper around journey events plus validation report accessors.
  - `ScoreboardSnapshot` — scoreboard inputs and rendered operator-facing metrics.
- Per-module type-checking ratcheted in gradually. Start with the extracted modules; do not require strict typing across all of legacy `harness.py` on day one.
- Compatibility adapters only where needed to keep persisted Delta payloads and replay fixtures stable. The public module APIs should become typed; persisted schemas should remain backward-compatible unless a separate migration plan says otherwise.

**Validation strategy:**

- Each module gets its own typed-contract PR after its Phase F extraction has landed.
- Unit tests prove the typed model constructors reject malformed inputs and preserve the current happy-path shape.
- Replay tests continue to assert behavior does not regress.
- Type checking is opt-in per hardened module first, then expanded. The end state is enforced typing for the extracted modules and a much smaller untyped allowance for the remaining orchestration spine.

**Exit criterion:** the eight extracted modules expose typed public APIs; `harness.py` orchestrates typed contracts rather than building and mutating open-ended dictionaries at every boundary; LLM-assisted edits can target one module plus its contract tests without loading the whole lever-loop harness into context.

**Real-Genie runs:** 0. Replay + unit + type checks are the gates.

**Detailed plan:** to be written as `2026-05-XX-lever-loop-typed-contract-hardening-plan.md` after Phase F's module boundaries are real.

---

## Real-Genie cost summary

| Phase | Real runs | Wall time |
|---|---|---|
| 0 (verification smoke) | 0–1 | ~2 hr |
| A (burn-down, 1–3 cycles) | 1–3 | ~2–6 hr |
| B / C / D (replay-only) | 0 | 0 |
| E (final integration) | 1 | ~2 hr |
| F (deeper modularization, replay-only, post-merge) | 0 | 0 |
| G (typed contract hardening, replay-only, post-merge) | 0 | 0 |
| **Pre-merge total** | **2–4** | **~4–10 hr** |
| **Post-merge follow-up** | **0** | **0** |

Calendar estimate: ~2 weeks pre-merge with the contract gate live at the end, then ~1 week of small post-merge PRs for Phase F, then ~1–2 weeks of typed-contract hardening for Phase G.

---

## What's parked (deliberate)

- **Generic key/value handoff table.** Phase 0 widens `genie_opt_runs` with 3 columns instead — minimal change, matches existing data shape. Revisit only if a future task needs handoff for arbitrary keys not already in Delta.
- **Restart-from-checkpoint inside `_run_lever_loop`.** The harness already resumes via `load_latest_full_iteration`. Phase 0 only fixes notebook-level state handoff, not harness internals.
- **Hand-synthesized fixture extensions.** Replaced by real-fixture capture at the end of Phase A (more honest, less work).
- **Further orchestration-spine decomposition beyond Phase F/G.** After Phase F, the remaining `harness.py` should be the orchestration spine. Phase G improves the contracts crossing that spine. Splitting the spine itself further is parked unless the spine becomes hard to reason about after typed contracts land.
- **Typed contract redesign before Phase G.** Strong typing is the desired endpoint, but it is deliberately delayed until after the burn-down, merge gate, and byte-stable extractions. Before then, only add narrow types that support the active phase without reshaping module boundaries.

---

## Concrete next action

**Start Phase B — Operator Scoreboard.** Phases 0 and A are complete (see [`2026-05-01-phase-a-burndown-log.md`](./2026-05-01-phase-a-burndown-log.md) for the close summary and [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md) for the per-iter detail). The airline corpus's journey-contract validation count is 0; `airline_real_v1.json` is committed with `expected_canonical_journey` (365 events, 38 706 bytes) and gated by `test_run_replay_airline_real_v1_within_burndown_budget` (budget=0). Phase B is replay-only, requires zero real-Genie cycles, and can begin immediately. Detailed plan to be drafted as `2026-05-XX-operator-scoreboard-plan.md`.

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
| [`high level plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md`](./high%20level%20plans/2026-05-01-lever-loop-phase-a-burndown-combined-high-level-plan.md) | Implemented | A (consolidated 16-track Phase A plan) |
| `2026-05-XX-operator-scoreboard-plan.md` | To be written | B |
| `2026-05-XX-failure-bucketing-classifier-plan.md` | To be written | C |
| `2026-05-XX-harness-extractions-phase-1-plan.md` | To be written | D |
| `2026-05-XX-harness-extractions-phase-2-plan.md` | To be written | F |
| `2026-05-XX-lever-loop-typed-contract-hardening-plan.md` | To be written | G |
