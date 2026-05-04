# Phase F6 (gates) Order Reconciliation Follow-up Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. This is a **decision-then-implementation** plan: Path A is operator-driven (cost-benefit decision), Paths B / C / D are TDD implementation tracks. Pick exactly ONE implementation track after Path A and execute it.

**Goal:** reconcile the gate-pipeline order between F6's module-level `GATE_PIPELINE_ORDER` and the harness's actual inline gate execution order, so F6 can be wired into the harness without changing decision-record drop ordering (which would break `canonical_decision_json` byte-stability).

**Background:** the F+H wire-up audit ([`2026-05-04-phase-f-h-wireup-audit-findings.md`](./2026-05-04-phase-f-h-wireup-audit-findings.md), Section 1 A5) showed two diverging orderings:

| Source | Order |
|---|---|
| `stages/gates.py:33-39` (`GATE_PIPELINE_ORDER`) | `content_fingerprint_dedup → lever5_structural → rca_groundedness → blast_radius → dead_on_arrival` |
| Harness inline gate sites (by line position) | `lever5_structural` (`:14106`) → `rca_groundedness` AG-level (`:14921`) → `rca_groundedness` proposal-level (`:15058`) → `blast_radius` (`:15562`); content_fingerprint_dedup runs separately via `_run_content_fingerprint_dedup_helper`; no clear `dead_on_arrival` gate |

The redrafted Phase A plan defers F6's harness wire-up entirely (per audit Section 2's classification 5: "Algorithm-replacement with config drift — gate-order reconcile required before wire-up"). This follow-up plan picks a permanent disposition.

**Tech Stack:** unchanged — `stages/gates.py` sub-handlers + harness inline gate primitives (`lever5_structural_gate_records`, `groundedness_gate_records`, `blast_radius_decision_records`).

**Status:** post-merge. NOT a blocker for Phase E. Schedule alongside the F2 follow-up after the main F+H wire-up + merge is complete.

---

## Path A: decide which side is canonical

This decision is a one-shot reasoning pass. Output a recommendation that subsequent paths execute.

### Task A1: Confirm the actual harness gate order with file:line precision

**Files:**
- Read: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14000-15600`

- [ ] **Step A1.1: Locate every harness gate emit site**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
grep -nE "lever5_structural_gate_records|groundedness_gate_records|blast_radius_decision_records|_run_content_fingerprint_dedup_helper|dead_on_arrival|DOA" \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

Expected (per audit Section 1 A5): four primary sites at `:14106, :14921, :15058, :15562`. Capture every match's line number — these are the canonical positions.

- [ ] **Step A1.2: Determine whether the harness invokes a content_fingerprint_dedup gate**

```bash
grep -nE "content_fingerprint_dedup|_run_content_fingerprint_dedup_helper" \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

If matches exist, record the line. If absent, content_fingerprint_dedup runs ONLY in F6 module (not in harness), and any F6 wire-up that activates this gate would change harness behavior. This is critical for Path B/C decisions.

- [ ] **Step A1.3: Determine whether `dead_on_arrival` exists as a gate in harness**

```bash
grep -nE "dead_on_arrival|DOA|doa" \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

If matches exist, record the line. If absent, F6's DOA gate is module-only — wire-up implications same as A1.2.

### Task A2: Score the four options

| Option | Approach | Cost | Benefit | Risk |
|---|---|---|---|---|
| **B** Make harness match F6 order | Refactor harness to call gates in `GATE_PIPELINE_ORDER`. Add content_fingerprint_dedup as a leading gate (currently runs separately); add dead_on_arrival as a trailing gate; reorder lever5 / groundedness / blast_radius to match F6 | HIGH (~8-12 hours; harness algorithm change; replay regression near-certain because drop-order changes survival sets) | Single source of truth (F6 module) for gate order; LLM postmortem can navigate F6 → exactly one source file | HIGH — drop-order change → different surviving proposal sets → different decision records → byte-stability fails. Requires fresh airline replay fixture + accuracy parity check |
| **C** Make F6 match harness order | Update `GATE_PIPELINE_ORDER` in F6 module to match harness's actual order (lever5 → groundedness → blast_radius). Remove content_fingerprint_dedup from the pipeline (it's a side-channel in harness). Either (a) remove dead_on_arrival entirely or (b) wire its harness equivalent if one exists | LOW (~2-3 hours; F6 module change only; one F6 unit test update; no harness change) | Module reflects production reality; F6 wire-up becomes a 1:1 swap | LOW — F6 unit tests have to be updated, but no algorithm change → byte-stable replay |
| **D** Keep both — F6 is "ideal" order, harness is "current" order | Document F6's `GATE_PIPELINE_ORDER` as an aspirational target; harness wire-up uses F6's sub-handlers but in harness's current order. Treat F6 as a typed surface, not a gate orchestrator | MEDIUM (~3-5 hours; F6 wire-up plan but with explicit per-gate insertion points instead of a single `filter()` call) | No algorithm change, no byte-stability risk; harness keeps its current order; F6 module's individual sub-handlers (e.g. `_run_content_fingerprint_dedup`, `_run_lever5_structural`) are reusable callable primitives | MEDIUM — F6's `filter()` becomes effectively dead code; `GATE_PIPELINE_ORDER` becomes documentation, not enforcement |
| **E** Defer indefinitely | Leave F6 unwired. Harness inline gates continue. F6 module exists only as observability surface that nothing populates. | NONE | NONE | Compounds technical debt; F6 wire-up never happens |

- [ ] **Step A2.1: Score against priorities**

| Priority | B | C | D | E |
|---|---|---|---|---|
| Time-to-completion | poor (multi-day) | excellent | medium | excellent (no work) |
| Risk of replay regression | HIGH | none | none | none |
| Algorithm change required | YES | NO | NO | NO |
| Long-term clarity | excellent | good | acceptable | poor |
| Engineering effort | poor (high cost, replay rebase) | excellent | medium | poor (free, invites confusion) |
| Reversibility | poor (algorithm change) | good | good | excellent |

- [ ] **Step A2.2: Pick the recommended path**

**Recommended: Path C** (make F6 match harness order). Cheapest path, no algorithm change, no replay risk. The F6 wire-up becomes a 1:1 swap of inline gate calls with stage sub-handler calls in harness's existing order.

**Fallback: Path D** (parallel orderings) only if Step A1.2 shows the content_fingerprint_dedup or dead_on_arrival gates are essential and have no harness equivalent. In that case, F6 module retains the gates as sub-handlers but `filter()`'s composed pipeline is documentation, not enforcement.

**Reject: Path B** (force harness to match F6) — the byte-stability cost is prohibitive without a clear algorithmic improvement. If a future algorithmic improvement justifies a gate-order change, that's a separate Phase X plan with its own variance baseline rebase.

**Reject: Path E** — same anti-pattern as F2 indefinite defer.

- [ ] **Step A2.3: Document the decision**

Append decision + rationale + date to this plan's footer (or to `docs/2026-05-05-phase-f6-decision-log.md`). Then execute the corresponding Path B/C/D below.

---

## Path B: refactor harness to match F6 order (REJECT-DEFAULT)

Skipped unless A2.2 selects it. If selected, plan body:

1. New variance baseline pilot run on `main` HEAD with current gate order (capture `final_accuracy` ± 2pp and the per-iteration drop sets).
2. Refactor harness gates to fire in `GATE_PIPELINE_ORDER` — content_fingerprint_dedup first, then lever5, groundedness, blast_radius, DOA.
3. Add the missing gates to harness (content_fingerprint_dedup as a primary gate, DOA at the end).
4. Run pilot — confirm accuracy parity; if regressed, halt and escalate to algorithm review.
5. Land the refactor with new variance baseline.
6. Then F6 wire-up becomes 1:1.

Estimate: 8-12 hours + 1 pilot run (~2 hours). Detailed task breakdown out of scope for this follow-up plan; if Path B is chosen, write a separate detailed plan at `docs/2026-05-XX-phase-f6-harness-gate-order-refactor-plan.md`.

---

## Path C: align F6 module to harness order (RECOMMENDED)

Implementation track. F6 module changes only; no harness change. Replay byte-stable trivially.

### Task C1: Snapshot harness's current gate order

**Files:**
- Read: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (gate emit sites from Task A1.1)

- [ ] **Step C1.1: Build the canonical ordered list**

From Task A1.1 results, build the order by ascending line number:

| Order | Gate | Harness line | F6 sub-handler |
|---|---|---|---|
| 1 | `lever5_structural` | `:14106` | `_run_lever5_structural` |
| 2 | `rca_groundedness` (AG-level) | `:14921` | `_run_rca_groundedness` |
| 3 | `rca_groundedness` (proposal-level) | `:15058` | `_run_rca_groundedness` (same handler, called twice) |
| 4 | `blast_radius` | `:15562` | `_run_blast_radius` |

Note: Steps 2 and 3 are both `rca_groundedness` — F6's `GATE_PIPELINE_ORDER` lists it once. Path C must decide whether F6 enforces a single fire or two fires. For replay parity, two fires (matching harness) is required.

If Step A1.2 finds content_fingerprint_dedup in harness, add it to the table at its line position. If absent, REMOVE it from F6's `GATE_PIPELINE_ORDER`.

If Step A1.3 finds dead_on_arrival, add it. If absent, REMOVE it.

### Task C2: Update F6 `GATE_PIPELINE_ORDER`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/gates.py:33-39`
- Test: `packages/genie-space-optimizer/tests/unit/test_gates_pipeline_order.py`

- [ ] **Step C2.1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_gates_pipeline_order.py
"""Phase F6 follow-up Path C: GATE_PIPELINE_ORDER matches harness."""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.gates import (
    GATE_PIPELINE_ORDER,
)


def test_gate_pipeline_order_matches_harness() -> None:
    """The pipeline order must match the harness's inline gate firing
    order (verified at harness.py:14106, 14921, 15058, 15562 — see
    docs/2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md).

    If a future commit reverses or reorders these, this test fails and
    the F6 wire-up plan must reconcile before continuing.
    """
    expected = (
        "lever5_structural",
        "rca_groundedness",  # fires twice in harness (AG + proposal level)
        "blast_radius",
    )
    # Path C drops content_fingerprint_dedup and dead_on_arrival from
    # the pipeline because harness does not invoke them as primary gates
    # (see Task A1.2 / A1.3 findings).
    assert GATE_PIPELINE_ORDER == expected
```

If Step A1.2 found content_fingerprint_dedup in harness, prepend it to `expected`. If Step A1.3 found dead_on_arrival, append it. The point of the test is to PIN whatever the actual harness order is at the time of this commit.

- [ ] **Step C2.2: Run the test, expect FAIL**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_gates_pipeline_order.py -v
```

Expected: FAIL — current `GATE_PIPELINE_ORDER` has 5 elements (`content_fingerprint_dedup, lever5_structural, rca_groundedness, blast_radius, dead_on_arrival`).

- [ ] **Step C2.3: Update `GATE_PIPELINE_ORDER` to match harness**

In `stages/gates.py:33-39`:

```python
# Path C update: aligned to harness inline gate order at
# harness.py:14106, 14921, 15058, 15562. content_fingerprint_dedup and
# dead_on_arrival are NOT primary gates in harness; they are dropped
# from the pipeline. The sub-handlers _run_content_fingerprint_dedup
# and (if present) _run_dead_on_arrival remain in this module as
# reusable callables for the harness's side-channel paths and for
# unit tests.
GATE_PIPELINE_ORDER: tuple[str, ...] = (
    "lever5_structural",
    "rca_groundedness",
    "blast_radius",
)
```

If A1.2/A1.3 found content_fingerprint_dedup or dead_on_arrival in harness as primary gates, include them in the corresponding positions instead.

- [ ] **Step C2.4: Update `filter(ctx, inp)` to handle the dual-fire `rca_groundedness`**

`stages/gates.py:filter(...)` iterates `GATE_PIPELINE_ORDER` and dispatches to a sub-handler per gate name. With the dual-fire requirement, `filter()` must call `_run_rca_groundedness(...)` once for AG-level (input shape: `proposals_by_ag` keyed) and once for proposal-level (input shape: flattened proposals with rca evidence).

Two implementation options:

  - **C2.4(a):** Split into two named handlers `_run_rca_groundedness_ag_level` and `_run_rca_groundedness_proposal_level`; update `GATE_PIPELINE_ORDER` to list them separately. Cleaner but introduces a 4-element pipeline.

  - **C2.4(b):** Keep one handler `_run_rca_groundedness`; have `filter()` call it twice in succession when iterating the pipeline reaches `rca_groundedness`. Looser but matches harness's "groundedness fires twice in this iteration" semantics.

Recommend C2.4(a) for clarity. Document the choice in this plan's footer.

- [ ] **Step C2.5: Run the test, expect PASS**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_gates_pipeline_order.py -v
```

Expected: PASS.

- [ ] **Step C2.6: Run the full F6 unit suite**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_stages_gates*.py -q
```

Expected: ALL pass. Pre-existing tests asserting `len(GATE_PIPELINE_ORDER) == 5` fail and must be updated.

- [ ] **Step C2.7: Commit**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/gates.py \
        packages/genie-space-optimizer/tests/unit/test_gates_pipeline_order.py
git commit -m "refactor(f6): align GATE_PIPELINE_ORDER to harness inline gate order (Path C)"
```

### Task C3: Wire F6 into harness gate-by-gate

This task replaces each harness inline gate emit with the corresponding F6 sub-handler call. **One commit per gate** for atomic byte-stability gating. Each commit must pass `tests/replay/test_phase_f_h_wireup_byte_stable.py` independently.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`

- [ ] **Step C3.1: Wire `lever5_structural` at `:14106`**

Locate the `lever5_structural_gate_records(...)` emit at `:14106`. Replace with:

```python
# Phase F6 follow-up Path C: lever5_structural via F6 sub-handler.
# Verified against: stages/gates.py:_run_lever5_structural body.
from genie_space_optimizer.optimization.stages import gates as _gates_stage

_lever5_inp = _gates_stage.GatesInput(
    proposals_by_ag={...},  # populated from existing harness locals at this site
    ags=tuple(action_groups),
    rca_evidence={},  # not consumed by this gate
)
_lever5_drops = _gates_stage._run_lever5_structural(
    _stage_ctx, _lever5_inp.proposals_by_ag, _lever5_inp.ags,
)
# Emit decision records via existing producer to preserve byte-stability.
for drop in _lever5_drops:
    _emit_lever5_drop(drop)  # use the existing producer
```

The exact adapter shape depends on `_run_lever5_structural`'s signature; read `stages/gates.py:80-180` (or the actual line range) before authoring. The plan-snippet here is illustrative; the redraft author must verify against actual API.

Run the byte-stability replay test. Commit only if PASS.

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
git commit -m "refactor(f6): wire lever5_structural gate via F6 sub-handler (Path C, harness:14106)"
```

- [ ] **Step C3.2: Wire `rca_groundedness` at `:14921` (AG-level)**

Same pattern. Replace inline `groundedness_gate_records(...)` AG-level call with `_gates_stage._run_rca_groundedness_ag_level(...)`.

```bash
git commit -m "refactor(f6): wire rca_groundedness AG-level gate via F6 sub-handler (Path C, harness:14921)"
```

- [ ] **Step C3.3: Wire `rca_groundedness` at `:15058` (proposal-level)**

Same pattern. Replace inline `groundedness_gate_records(...)` proposal-level call with `_gates_stage._run_rca_groundedness_proposal_level(...)`.

```bash
git commit -m "refactor(f6): wire rca_groundedness proposal-level gate via F6 sub-handler (Path C, harness:15058)"
```

- [ ] **Step C3.4: Wire `blast_radius` at `:15562`**

Same pattern. Replace inline `blast_radius_decision_records(...)` call with `_gates_stage._run_blast_radius(...)`.

```bash
git commit -m "refactor(f6): wire blast_radius gate via F6 sub-handler (Path C, harness:15562)"
```

### Task C4: Wrap F6 with capture decorator (Phase H Commit B5 equivalent)

Now that F6 has 4 inline call sites consuming its sub-handlers, wrap the **composed `filter()`** at a single call site if practical, OR wrap each sub-handler call individually with `wrap_with_io_capture`.

If the harness orchestration doesn't lend itself to a single `filter()` call (because the gates fire at different code locations), keep the per-gate wraps and emit the composed bundle entry via a synthetic stage call at iteration end. The capture decorator only activates when `ctx.mlflow_anchor_run_id` is non-None, so a synthetic call is cheap.

- [ ] **Step C4.1: Decide composed-call vs. per-gate-wrap**

Read `harness.py:14000-15600` end-to-end and decide:
- If gates 1-4 can be lifted into a single `filter()` call at a single insertion point without changing semantics, do so → single capture wrap.
- Otherwise, keep gates inline and add a synthetic `_gates_stage.filter(...)` call at iteration end with the assembled `GatesInput` and check survived sets match the existing harness drops list. Use this synthetic call as the capture target.

- [ ] **Step C4.2: Implement chosen approach + commit**

```bash
git commit -m "feat(f6): wrap gates stage with capture decorator (Path C, Phase H B5 equivalent)"
```

### Task C5: End-to-end replay smoke

```bash
cd packages/genie-space-optimizer
pytest tests/replay/ tests/integration/ -q
```

Expected: ALL pass. Bundle's `iter_NN/stages/06_safety_gates/` dir is now populated.

---

## Path D: parallel orderings (FALLBACK)

If Path C is blocked because harness gate sites cannot be cleanly mapped to F6 sub-handlers (e.g. content_fingerprint_dedup is essential but lives in `_run_content_fingerprint_dedup_helper` outside the gate pipeline), execute Path D.

### Task D1: Document F6's `GATE_PIPELINE_ORDER` as aspirational

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/gates.py:1-22` (module docstring)

- [ ] **Step D1.1: Update the module docstring**

```python
"""Stage 6: Safety Gates (Phase F6).

PARALLEL OBSERVABILITY MODE (2026-05-05): F6's `GATE_PIPELINE_ORDER` is
ASPIRATIONAL, not enforced. The harness fires gates in its OWN order
at distinct code sites (`harness.py:14106, 14921, 15058, 15562`); F6's
`filter()` is currently called as a synthetic post-hoc observability
pass that re-runs the gate sub-handlers in the F6 order against the
gates' inputs to capture a typed `GateOutcome` for the bundle.

Use F6's individual sub-handlers (`_run_lever5_structural`,
`_run_rca_groundedness`, `_run_blast_radius`, `_run_content_fingerprint_dedup`,
`_run_dead_on_arrival`) as the canonical gate logic; the harness wires
each sub-handler at its respective inline call site (per docs/
2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md Path C
Tasks C3.1-C3.4). The `filter()` orchestrator is the typed surface for
Phase H bundles, not an executable gate pipeline.
"""
```

- [ ] **Step D1.2: Wire F6 sub-handlers per Path C Tasks C3.1-C3.4**

Same as Path C — each gate site uses the F6 sub-handler.

- [ ] **Step D1.3: Add a synthetic `filter()` call at iteration end for bundle capture**

After all per-gate sub-handlers have fired, call `_gates_stage.filter(_stage_ctx, _gates_inp)` with the assembled inputs. Discard the result for production; the call exists ONLY for the capture decorator to write `input.json` + `output.json` to the bundle.

- [ ] **Step D1.4: Commit**

```bash
git commit -m "refactor(f6): wire gate sub-handlers + synthetic filter() for Phase H capture (Path D)"
```

---

## Self-Review

After completing the chosen path:

- [ ] All replay tests pass (`pytest tests/replay/ -q`).
- [ ] If Path C: `tests/integration/test_phase_h_bundle_populated.py` shows F6 dir non-empty.
- [ ] If Path D: F6 module docstring reflects the parallel-observability character.
- [ ] Decision rationale documented at this plan's footer or `docs/2026-05-05-phase-f6-decision-log.md`.
- [ ] Roadmap updated to mark Phase F6 follow-up complete.

---

## Footer: decision log placeholder

(Filled in during execution.)

- Decision date: <YYYY-MM-DD>
- Path chosen: <B / C / D>
- Step A1.1 — confirmed harness gate sites: `<file:line list>`
- Step A1.2 — content_fingerprint_dedup in harness? <yes / no, location>
- Step A1.3 — dead_on_arrival in harness? <yes / no, location>
- Step C2.4 — chose (a) split handlers OR (b) double-fire? <a / b, rationale>
- Implementer: <name>
- Final commit hash: <hash>

## Decision Log

**Date:** 2026-05-05
**Chosen path:** **Path C** (align F6 module to harness order).
**Rationale:** No algorithm change; byte-stability preserved trivially;
F6 wire-up becomes additive observability after the three harness inline
gates. content_fingerprint_dedup and dead_on_arrival remain as F6-only
observability sub-handlers.
**Implementation:** see
`packages/genie-space-optimizer/docs/2026-05-05-phase-h-completion-plan.md`
Task 3.
