# RCO-5 Acceptance Object Consolidation Policy

> Source-of-truth policy for Stage-9 acceptance object types as of 2026-05-11. Cite this from any plan that touches Stage-9 acceptance semantics. The structural guard test at `tests/unit/test_rco5_acceptance_structural_guard.py` enforces the rule.

## The canonical runtime acceptance object

**`optimization.control_plane.ControlPlaneAcceptance`** is the canonical, frozen, runtime decision object for Stage-9 acceptance. It owns:

- The accepted-vs-rejected bit (`accepted: bool`)
- The reason code (`reason_code: str`)
- Baseline / candidate accuracy + delta (`baseline_accuracy`, `candidate_accuracy`, `delta_pp`)
- Target-set qid classification (`target_qids`, `target_fixed_qids`, `target_still_hard_qids`, `target_delta_states`, `target_soft_passing_qids`)
- Out-of-target regression classification (`out_of_target_regressed_qids`, `regression_debt_qids`, `protected_regressed_qids`, `soft_to_hard_regressed_qids`, `passing_to_hard_regressed_qids`, `unknown_to_hard_regressed_qids`, `existing_hard_still_hard_outside_target_qids`)
- Reattribution accounting (`accidentally_improved_qids`, `unresolved_target_debt_qids`)

Every runtime acceptance decision in the harness flows through `ControlPlaneAcceptance`. Every render surface (`format_full_eval_marker_payload`, `acceptance_decided` DecisionRecord, the `FULL EVAL` print block, Phase B / Phase H transcripts) reads from `ControlPlaneAcceptance` and goes through `format_full_eval_marker_payload` to ensure I9 (`check_i9_acceptance_render_byte_equality`) catches drift.

## The two ancillary acceptance-related types

### `optimization.acceptance_policy.GainGateDecision`

Narrow, frozen dataclass returned by `decide_acceptance()`. Represents the **gain-gate sub-decision**: did the candidate beat the baseline by at least `min_gain_pp`? This is the upstream gate; the harness combines its outcome with target-set classification to produce the canonical `ControlPlaneAcceptance`.

`GainGateDecision` is NOT the canonical acceptance object. It is an upstream feeder. The rename from `AcceptanceDecision` to `GainGateDecision` makes the upstream role explicit so future contributors do not confuse it with the canonical type.

### `tools.lever_loop_stdout_parser.ParsedAcceptanceView`

Frozen dataclass produced by the stdout parser. Represents an **observable projection** of a canonical acceptance decision: only the fields visible in the rendered stdout (iteration, ag_id, accepted, reason_code, target_qids, target_fixed_qids, target_still_hard_qids, target_still_hard_qids_source).

`ParsedAcceptanceView` is NOT a runtime decision; it is a view-layer artifact. The rename from `AcceptanceDecision` to `ParsedAcceptanceView` makes the view-only role explicit.

Use `parsed_view_to_control_plane(view) -> ControlPlaneAcceptance` to project a parsed view into a canonical-shaped object with unobservable fields filled by sentinel (empty tuples for tuple-typed fields, zeros for float-typed fields). The projection is one-way; the canonical-shaped result is for downstream tooling that wants to consume a `ControlPlaneAcceptance` regardless of whether the source was a runtime decision or a stdout parse.

## The structural rule

> No type named `AcceptanceDecision` lives in `optimization.acceptance_policy` or `tools.lever_loop_stdout_parser`. The only canonical Stage-9 acceptance type is `ControlPlaneAcceptance` in `optimization.control_plane`.

`tests/unit/test_rco5_acceptance_structural_guard.py` enforces this rule. Drift surfaces as a test failure, not a runtime defect.

## How I9 fits

I9 (`check_i9_acceptance_render_byte_equality` in `optimization/invariants.py:421`) operates on evidence-mapping payloads, not on dataclass instances directly. The rename does not affect I9's logic. I9 remains the runtime guard that catches render drift between `DecisionRecord.metrics` and `GSO_FULL_EVAL_V1` marker payloads. RCO-5's structural guard catches drift at the type-system level; I9 catches drift at the rendered-payload level. Both stay.

## What this policy does NOT cover

- The semantics of `ControlPlaneAcceptance`'s fields. See `optimization/control_plane.py` source and Cycle 14-V T3 / Cycle 14-W T1 / Cycle 14-C T2 / Cycle 16-T3 plan docs.
- The `format_full_eval_marker_payload` rendering rules. See Cycle 14-V T3 plan.
- I9's evidence-shape contract. See `optimization/invariants.py:421-465`.

## When to revisit

This policy gets revisited when:

1. A new acceptance-related type is added anywhere in the codebase. The structural guard test will fail; updating it is the trigger.
2. `ControlPlaneAcceptance`'s field set changes. Update the canonical fields list above.
3. The gain-gate decision logic stops being a separable upstream gate (which would mean folding `GainGateDecision` into `ControlPlaneAcceptance`'s construction path). That is its own plan.
