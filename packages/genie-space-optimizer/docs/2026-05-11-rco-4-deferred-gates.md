# RCO-4 Deferred Gates

> Three of the six conceptual gates named in the closeout doc are deferred out of RCO-4. This document records the rationale and the named blocker that must clear before each extraction can begin. No deferred gate becomes "stuck" — each has a named unblocker.

## Alignment gate

**Why deferred:** No single extractable code site. The "alignment" concept is split across (a) pre-application RCA-groundedness in `stages/gates.py:_run_rca_groundedness_gate` (already pure, observability-only) and (b) the full-eval acceptance logic at `harness.py:_run_gate_checks` line ~13565 (`gate_name="full_eval_acceptance"`), where alignment between proposed patches and the AG's RCA is implicitly checked by post-eval score-delta vs target-qid distribution. The post-eval half is entangled with mlflow run state, `predict_fn`, `scorers`, and the iteration's evaluation transcript.

**Named blocker:** `_run_gate_checks` decomposition. The full-eval-acceptance region needs to be pulled out into its own typed evaluation-acceptance helper (with `predict_fn` / `scorers` as injected dependencies) before alignment can be extracted as a discrete pure gate.

**Unblocker plan:** RCO-4b — `_run_gate_checks` decomposition. Not in scope for RCO-4. If RCO-4b is not scheduled before Milestone B closes, this deferral becomes a closeout-doc gap and surfaces in RCO-9.

**Coverage gap risk:** Low. The current `_run_rca_groundedness_gate` plus I7 (proposal/patch grounding invariant) plus I9 (acceptance render-drift invariant) already cover the alignment failure modes that motivated naming the gate.

## Reflection gate

**Why deferred:** Reflection is not a discrete gate-decision site. It is a control-flow loop across iteration boundaries: an AG with a halted patch in iteration N reflects, retries, and may surface a narrow variant in iteration N+1. The "gate" framing in the closeout doc was aspirational — there is no single function returning `kept / dropped` for reflection.

**Named blocker:** The reflection loop's iteration-boundary state lives in `reflection_buffer` (threaded through the harness lever-loop). Extracting a pure helper requires elevating `reflection_buffer` to a typed dataclass with its own contract — that is closer in spirit to RCO-5 (Acceptance Object Consolidation) than to a Stage-6 gate extraction.

**Unblocker plan:** Either part of RCO-5 (acceptance object consolidation) or a follow-up "reflection contract" plan after RCO-5 lands. Not in scope for RCO-4.

**Coverage gap risk:** Low. The `reflection_retry.py` module's `patch_body_fingerprint` helper is already pure and covered by the existing reflection-retry unit tests. The control-flow correctness is exercised by the journey-validation invariants from Cycle 17.

## Cap gate

**Why deferred:** The cumulative-regression-debt cap is threaded as a parameter (`cumulative_regression_debt`) into `_run_gate_checks` at `harness.py:12767`, where it is consumed alongside slice/P0/full-eval logic. The "accuracy cap" and "patch cap" referenced in `audit_emit` strings are not discrete functions — they are decision points inside the 800-line `_run_gate_checks` body.

**Named blocker:** Same as alignment — `_run_gate_checks` decomposition (RCO-4b).

**Unblocker plan:** RCO-4b. Not in scope for RCO-4.

**Coverage gap risk:** Medium. The cap gate is one of the highest-impact gates in production (it is what stops a runaway acceptance pattern). However, the cap logic itself is well-tested by `optimization/cumulative_regression_debt.py` (per Cycle 14B). The gap is the absence of a typed `CapGateOutcome` — not the absence of correctness coverage.

## What is NOT deferred

- Blast-radius gate orchestration (Task 3)
- Narrow-replacement / Branch C orchestration (Task 4)
- Applyability gate wrapper (Task 5)
- Sequencing test for the production firing order (Task 7)
- Production-shape fixture pairs for the three extracted helpers (Task 8)

## Re-visit trigger

This document gets revisited when any of the following is true:

1. RCO-4b (`_run_gate_checks` decomposition) is drafted. At that point, alignment and cap extraction become directly addressable.
2. RCO-5 (acceptance object consolidation) lands. At that point, reflection-loop extraction becomes addressable.
3. A production-shape regression surfaces inside one of the deferred gates and the postmortem identifies the lack of a pure helper as the proximate cause. (None known as of 2026-05-11.)
