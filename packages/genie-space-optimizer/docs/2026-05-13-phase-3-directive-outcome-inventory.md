# Phase 3 — Directive Outcome Inventory

**Date:** 2026-05-13
**Status:** Locked
**Plan reference:** `2026-05-13-phase-3-directive-to-proposal-obligation-plan.md`

## What problem is this closing?

The 2314bb2c trial's iter 2-5 AG2 trajectory had non-empty `lever_directives`
(L5 + L6) but produced zero applied patches. The iteration-level P-F
`proposal_failure_decided` record correctly fired
(`failure_mode=proposal_generation_empty`,
`next_action=REQUEST_EVIDENCE_GATHERING`) but the trace had no per-lever
attribution. A reader could not answer "did L5 fail to find a structural
candidate, or did L6 force-LLM decline?" without re-reading raw stdout.

## What exists today

* P-F's `failure_mode` closed vocabulary (5 entries) names **iteration-level
  triggers** for the recovery policy.
* P-F's `ProposalFailureNextAction` closed vocabulary (6 entries) names
  **recovery decisions**.
* `check_proposal_failure_decided_coverage` enforces **iteration-level
  totality** (`applied_total == 0` on a no-applied exit path ⇒ ≥1 P-F record).
* `proposal_generation_empty_marker` fires **per AG** when `all_proposals`
  is empty across every lever — but only as a single binary signal.

None of these tells you per (AG, lever_key) what happened.

## What this plan adds

A new closed vocabulary `DirectiveOutcomeCode`:

| Code | Meaning | Trigger condition |
| --- | --- | --- |
| `proposal_emitted` | ≥1 proposal returned by `generate_proposals_from_strategy` for this lever | `lever_proposals` non-empty |
| `no_structural_candidate` | Generator found no archetype/pattern match | `lever_proposals` empty AND no STRUCTURAL_GATE_DROPPED records AND not force-L6 |
| `force_llm_declined` | L6 force-LLM path returned no candidate | `failure_mode == lever6_force_llm_declined` AND lever_key == 6 |
| `applyability_rejected` | Generator emitted but applyability gate dropped every proposal | `lever_proposals` non-empty AND every entry tagged `applyability_dropped` |
| `collateral_rejected` | Generator emitted but blast-radius gate dropped every proposal | `lever_proposals` non-empty AND every entry tagged `high_collateral_risk_flagged` |
| `lever_not_proposal_generating` | Lever-3 directives: applied as instruction-edits, never enter `generate_proposals_from_strategy` | `lever_key == 3` |

And one invariant `check_directive_outcome_coverage`:

> For each AG with `ag.lever_directives` non-empty, every lever_key in the
> directive dict MUST map to exactly one `DirectiveOutcomeCode` in the
> iteration's `directive_outcomes` ledger.

Default-ON observability with the `GSO_DIRECTIVE_OUTCOME_COVERAGE=0`
rollback escape hatch. Warn-and-degrade: violations emit
`GSO_INVARIANT_VIOLATION_V1` with `invariant_name="directive_outcome_coverage"`
and the loop continues.

## Relationship to other plans

* **P-F (landed):** P-F's iteration-level coverage stays unchanged. Phase 3's
  per-AG-per-directive coverage is finer-grained and additive. Both invariants
  fire independently; neither subsumes the other.
* **Phase 2 (proposed):** Phase 2's recovery dispatcher consumes
  `DirectiveOutcomeCode` to decide per-directive recovery actions. Phase 3
  is a strict prerequisite.
* **Phase 4 (proposed):** Phase 4's gs_026 anchor assertions use Phase 3's
  invariant as the assertion surface ("on gs_026, every directive must have
  an outcome; silent retry forbidden").

## Validation

* Synthesized AG2-shape fixture in `tests/integration/test_directive_outcome_2314bb2c_ag2.py`
  — L5 directive present, L6 directive present, both lever loops return zero
  proposals, ledger MUST record `no_structural_candidate` for L5 and
  `force_llm_declined` for L6, and the coverage invariant MUST pass (every
  directive key mapped).
* Counter-fixture: same AG with the ledger artificially missing an entry —
  the coverage invariant MUST emit a `GSO_INVARIANT_VIOLATION_V1` marker.
