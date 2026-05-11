# RCO-4b Phase Roadmap

> RCO-4b decomposes `harness._run_gate_checks` (~1500 lines) into pure helpers. The work is split across five phases (A-E) so each ships a reviewable diff. This doc names every phase, its scope, its blockers, and its expected commit count.

## Why phased

`_run_gate_checks` is the largest single function in the optimizer. A single plan extracting all six gate-stages would be too large to execute reliably. Phasing also lets the parity-test pattern shake out on the simplest stage (`propagation_wait`) before any complex extraction commits.

## Phase A — Foundation (this is the plan you are reading the roadmap for)

**Plan:** `docs/2026-05-12-rco-4b-phase-a-run-gate-checks-decomposition-plan.md`

**Scope:**
- Inventory doc + this roadmap doc.
- Typed input/output dataclasses for all six gate-stages in `optimization/stages/gate_types.py`.
- New module `optimization/stages/eval_gates.py`.
- Extract `propagation_wait` as the canonical example.
- Default-off feature flags for all six stage extractions (Phase A consumes only `GSO_GATE_CHECKS_PROPAGATION_PURE`; the other five flags are reserved for Phases B-E).
- Production-shape parity-test pattern and fixtures for propagation_wait.
- Sequence-guard grep test pinning the production firing order of `gate_name=` sentinels.

**Blocked by:** nothing.

**Estimated commits:** ~12.

**Closes:** propagation_wait extraction; foundation for B-E.

## Phase B — Slice gate

**Plan:** `docs/2026-05-12-rco-4b-phase-b-slice-gate-extraction-plan.md`.

**Status:** in-flight — extraction proceeds in three pure helpers
(should_run / effective_tolerance / post_eval) so the side-effecting
middle step (``run_evaluation`` + ``detect_regressions``) remains in the
harness. See ``2026-05-12-rco-4b-phase-b-slice-gate-extraction-plan.md``.

**Scope:**
- Add `decide_slice_gate_should_run`, `compute_slice_gate_effective_tolerance`, and `decide_slice_gate_post_eval` to `eval_gates.py`.
- Three-step pure helpers because `run_evaluation` runs between the pre-eval and post-eval decisions.
- Wire `GSO_GATE_CHECKS_SLICE_PURE` flag (default-off); legacy inline preserved in `else` branch.
- Parity test + production-shape fixtures.

**Blocked by:** Phase A.

**Estimated commits:** ~10 (1 doc + 3 helpers + harness wiring + 5
fixtures + parity + sequence-guard + roadmap close).

## Phase C — P0 gate

**Plan:** `docs/2026-05-12-rco-4b-phase-c-p0-gate-extraction-plan.md`.

**Status:** in-flight — extraction proceeds in two pure helpers
(should_run / post_eval). No tolerance computation needed (P0 gate
has none). See ``2026-05-12-rco-4b-phase-c-p0-gate-extraction-plan.md``.

**Scope:** same two-step extraction pattern as Phase B (minus the
tolerance helper).

**Blocked by:** Phase A. Independent of Phase B (can land in parallel).

**Estimated commits:** ~7 (1 doc + 2 helpers + harness wiring + 4
fixtures + parity + sequence-guard + roadmap close).

## Phase D — ASI + baseline-drift

**Plan:** `docs/2026-05-12-rco-4b-phase-d-asi-extraction-and-baseline-drift-plan.md`.

**Status:** in-flight — extracts two observability-only stages
(ASI audit forwarder + baseline-drift diagnostic) into pure helpers.
Independent of Phase B (slice gate) and Phase C (P0 gate); safe to
parallelize.

**Scope:**
- Add `forward_asi_extraction_audit(inp: AsiExtractionInput) -> AsiExtractionOutcome`.
- Add `build_baseline_drift_diagnostic(inp: BaselineDriftDiagnosticInput) -> BaselineDriftDiagnosticOutcome`.
- Refine Phase A placeholder ASI shape to audit-forwarder contract.
- Two independent default-off flags (`GSO_GATE_CHECKS_ASI_EXTRACTION_PURE`, `GSO_GATE_CHECKS_BASELINE_DRIFT_PURE`).
- Both are audit-only — they emit a row and return; no rollback path.

**Blocked by:** Phase A. Independent of Phases B/C/E.

**Estimated commits:** ~9 (1 doc + 1 contract refine + 2 helpers +
2 harness wirings + 1 sequence-guard + 1 fixtures + 1 roadmap close).

## Phase E — Full-eval acceptance

**Plan:** `docs/2026-05-12-rco-4b-phase-e-full-eval-acceptance-plan.md`.

**Status:** `closed-local pending corpus`. Three audit-emission sites
extracted into a single pure verdict-consolidation helper. With
Phase E landed, the `_run_gate_checks` decomposition is structurally
complete and **RCO-4b is fully landed**.

**Scope:**
- Add `decide_full_eval_acceptance` to `eval_gates.py` consolidating
  upstream `_strict_decision`, `_t4_verdict`, `_control_plane_decision`,
  and `regressions[]` into a typed verdict outcome carrying all three
  audit-metrics payloads.
- Three audit emissions stay in harness on both branches (verdict /
  rollback / accept).
- Default-off behind `GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE`.

**Blocked by:** Phase A.

**Estimated commits:** ~8 (1 doc + 1 contract refine + 1 helper +
1 harness wiring + 1 sequence-guard + 1 fixtures + 1 parity +
1 roadmap close).

**Phase E completion marks RCO-4b as fully landed.** With all six
gate-stages extracted, the next milestone is the consolidating
trial run that validates RCO-2b + RCO-3 + RCO-4c + RCO-6
simultaneously.

## Phase F (post-RCO-4b) — Unblock RCO-4's deferred gates

**Plan:** `docs/<TBD>-rco-4c-alignment-cap-reflection-extraction-plan.md`.

**Scope:**
- Now that `full_eval_acceptance` is a typed pure helper, alignment-gate extraction becomes addressable.
- Cap-gate extraction follows similarly.
- Reflection-gate extraction depends on RCO-5's acceptance-object consolidation (already landed).

**Blocked by:** Phases A + E.

## Dependency graph

```
A — Foundation
├── B — Slice gate
├── C — P0 gate
├── D — ASI + baseline drift
└── E — Full-eval acceptance
     └── F (RCO-4c) — Alignment / cap / reflection
```

Phases B, C, D can land in any order (or in parallel) after A. Phase E can land after A but the harness wiring is cleanest after B/C/D have already removed surrounding inline blocks.

## Closeout signal

RCO-4b is "closed-local pending corpus" when Phases A-E have all landed and the sequence-guard test passes with every gate flag flippable to true (parity verified). RCO-9's final audit greps for `class .*Input` and `def run_.*_gate` in `eval_gates.py` to confirm the surface matches this roadmap.


## Post-RCO-4b — Trial-Run Gating

After Phase E lands, the recommended next step is a **single
consolidating trial run** that enables all six gate-flags
simultaneously:

- ``GSO_GATE_CHECKS_PROPAGATION_PURE`` (Phase A)
- ``GSO_GATE_CHECKS_SLICE_PURE`` (Phase B)
- ``GSO_GATE_CHECKS_P0_PURE`` (Phase C)
- ``GSO_GATE_CHECKS_ASI_EXTRACTION_PURE`` (Phase D)
- ``GSO_GATE_CHECKS_BASELINE_DRIFT_PURE`` (Phase D)
- ``GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE`` (Phase E)

The trial's captured artifacts (``GSO_CONTRACT_HEALTH_V1`` markers,
audit rows, journey events, bundle JSON) simultaneously unblock:

- **RCO-2b** strict-mode default-flip decision
- **RCO-3** pilot-gated default-flip closeout
- **RCO-4c** deferred-gate unblock (alignment, cap, reflection)
- **RCO-6** replay/production journey producer parity
