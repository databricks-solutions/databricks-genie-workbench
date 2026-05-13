# Acceptance Gate Redesign — Design Decision Record

**Date:** 2026-05-13
**Status:** Locked
**Supersedes:** Open design questions in `2026-05-12-phase-0-offline-acceptance-policy-replay-results.md:45-49`
**Plan reference:** `2026-05-13-acceptance-gate-redesign-phase-1-plan.md`

## Context

Phase 0 (`2026-05-12-phase-0-offline-acceptance-policy-replay-results.md`) ran the pilot `regression_debt_policy_pilot_default()` against three captured fixtures and reported `pass_criterion_met=true` but an explicit `DEFER` decision: the policy rejects both real-candidate fixtures (ccf1d60d_iter1, 3b050ec5_iter1) on the same `min_target_clusters_fixed=1` gate. The replay surfaced four design questions; this record locks the answers.

## Decisions

### D1. New attribution-drift acceptance tier (separate from partial-harvest)

A new acceptance tier is introduced. It is a *sibling* of the existing partial-harvest tier, not a replacement and not a relaxation. Both tiers use the same `RegressionDebtPolicy` dataclass with different field values.

| Tier | Policy factory | `min_target_clusters_fixed` | When it fires |
| --- | --- | --- | --- |
| Partial-harvest | `regression_debt_policy_pilot_default` | 1 | Candidate fixed >= 1 target AND has bounded debt |
| Attribution-drift | `attribution_drift_policy_pilot_default` (new) | 0 | Candidate fixed 0 targets AND moved aggregate AND has bounded debt |

The two are mutually exclusive in practice: a candidate either fixed at least one target or it did not. The two reason codes (`accepted_with_partial_harvest_debt`, `accepted_with_attribution_drift_and_debt`) preserve that distinction in the trace so postmortems can tell the outcomes apart.

### D2. Aggregate-gain floor for the new tier

The attribution-drift tier uses `min_aggregate_improvement_pp=4.0`, not the pilot's `10.0`. Justification:

- ccf1d60d_iter1 at +4.3pp passes; 3b050ec5_iter1 at +8.1pp passes.
- Stays strictly above the four-tier classifier's `net_win_min_delta_pp=3.0`. A NET_WIN at the same delta has a target fix; this tier does not. Demanding more aggregate evidence to compensate keeps the tier ordering monotone.

### D3. Allowed debt buckets for the new tier

`allowed_debt_buckets = frozenset({DeltaState.SOFT_TO_HARD, DeltaState.LOOKUP_FAILED})`.

- `SOFT_TO_HARD` (a previously-soft-failing QID regressing to hard) is admissible under the same conditions the partial-harvest tier admits it.
- `LOOKUP_FAILED` (a QID with no pre-row, equivalent to `unknown_to_hard`) is admissible because corpus fixtures (ccf1d60d) have this pattern. It is the bucket `unknown_to_hard_regressed_qids` lands in per `evaluate_regression_debt:1840`.
- `REGRESSED_TO_UNKNOWN` (`passing_to_hard`, a previously-known-passing QID regressing) is **excluded**. Admitting a previously-passing regression while not fixing the target is the worst-case tradeoff.

### D4. Trace vocabulary

A new closed-vocabulary reason code `accepted_with_attribution_drift_and_debt` is added. Distinct from:

- `accepted_with_attribution_drift` (existing — zero debt, target unchanged, thresholds met)
- `accepted_with_partial_harvest_debt` (existing — target fixed, bounded debt)
- `accepted_with_regression_debt` (existing — target fixed, soft-to-hard only, no flag gate)

The FULL EVAL banner emits the full reason code so the `_and_debt` suffix surfaces in stdout markers and the operator transcript. No new `AcceptedClass` enum value is needed; the tier maps to existing `NET_WIN_WITH_DEBT`.

## Non-decisions

- `cumulative_debt_max` is shared between tiers (single harness counter). Per-tier accounting is a follow-up if Phase 0.2 evidence shows the shared counter is too coarse.
- The attribution-drift tier's flag (`GSO_ATTRIBUTION_DRIFT_WITH_DEBT`) starts default-OFF. The flag-flip is a separate plan after Phase 0.2 turns green and one lever-loop trial confirms the new path emits the right markers.

## Validation

Phase 0.2 (Task 10 in the implementation plan) re-runs the offline replay under the new policy and must report:

- ccf1d60d_iter1 → `accepted_with_attribution_drift_and_debt`, accepted=true
- 31ecd96f_no_payload → `no_payload`, accepted=null
- 3b050ec5_iter1 → `no_debt_to_harvest`, accepted=true (no debt present; the legacy zero-debt drift path handles this case in production)
- `pass_criterion_met=true`, 3 matches, 0 structured mismatches, 0 unstructured mismatches.
