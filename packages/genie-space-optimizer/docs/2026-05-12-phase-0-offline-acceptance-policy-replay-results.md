# Phase 0 Results — Offline Acceptance-Policy Replay

**Run date:** 2026-05-12
**Tool:** `python -m genie_space_optimizer.tools.policy_replay`
**Policy under test:** `regression_debt_policy_pilot_default()` from `optimization/acceptance_policy.py`

## Raw `replay_classifier_decision` output

```jsonl
{"event": "replay_classifier_decision", "fixture_id": "ccf1d60d_iter1", "policy_name": "regression_debt_policy_pilot_default", "payload_present": true, "observed_accepted": false, "observed_reason_code": "no_target_clusters_fixed", "debt_qids": ["7now_delivery_analytics_space_gs_012"], "first_failed_gate": "min_target_clusters_fixed", "policy_diagnostics": {"debt_count": 1, "debt_count_max": 1, "aggregate_gain_pp": 4.3, "aggregate_gain_floor_pp": 10.0, "target_clusters_fixed": 0, "target_clusters_fixed_min": 1, "cumulative_debt_used": 0, "cumulative_debt_max": 3, "threshold_pass_rate": 1.0, "threshold_pass_rate_min": 0.95}, "predicted_accepted": true, "predicted_reason_code": "accepted_with_partial_harvest_debt", "match": false, "structured_mismatch": true, "match_status": "mismatch_identifies_missing_tier"}
{"event": "replay_classifier_decision", "fixture_id": "31ecd96f_no_payload", "policy_name": "regression_debt_policy_pilot_default", "payload_present": false, "observed_accepted": null, "observed_reason_code": "no_payload", "debt_qids": [], "first_failed_gate": null, "policy_diagnostics": {}, "predicted_accepted": null, "predicted_reason_code": "no_payload", "match": true, "structured_mismatch": false, "match_status": "exact_match"}
{"event": "replay_classifier_decision", "fixture_id": "3b050ec5_iter1", "policy_name": "regression_debt_policy_pilot_default", "payload_present": true, "observed_accepted": false, "observed_reason_code": "no_target_clusters_fixed", "debt_qids": [], "first_failed_gate": "min_target_clusters_fixed", "policy_diagnostics": {"debt_count": 0, "debt_count_max": 1, "aggregate_gain_pp": 8.1, "aggregate_gain_floor_pp": 10.0, "target_clusters_fixed": 0, "target_clusters_fixed_min": 1, "cumulative_debt_used": 0, "cumulative_debt_max": 3, "threshold_pass_rate": 1.0, "threshold_pass_rate_min": 0.95}, "predicted_accepted": true, "predicted_reason_code": "accepted_with_partial_harvest_debt", "match": false, "structured_mismatch": true, "match_status": "mismatch_identifies_missing_tier"}
{"event": "replay_classifier_summary", "policy_name": "regression_debt_policy_pilot_default", "matches": 1, "structured_mismatches": 2, "unstructured_mismatches": 0, "pass_criterion_met": true}
```

## Per-fixture interpretation

### ccf1d60d_iter1 (7now consolidating, +4.3pp net, target unfixed, 1 unknown→hard regression)

- Predicted: `accepted_with_partial_harvest_debt`
- Observed: `no_target_clusters_fixed` (`first_failed_gate=min_target_clusters_fixed`)
- Diagnosis: The pilot policy's `min_target_clusters_fixed=1` gate fires before any debt logic runs. The candidate produced a net +4.3pp aggregate improvement at the cost of one unknown→hard regression, but did not move the AG's target qid (`gs_026`) from STILL_HARD to FIXED. The existing tier vocabulary has no accept path for "net positive, target unchanged" — what the design conversation called *attribution drift*. Two design moves are on the table: (a) add an `accept_with_attribution_drift` tier that requires `min_target_clusters_fixed=0` plus a compensating gate (e.g., `min_aggregate_improvement_pp >= 5` AND `unknown_to_hard_count <= 1`), or (b) relax `min_target_clusters_fixed` on the pilot policy to `0` and rely on `min_aggregate_improvement_pp` alone to gate quality. The latter is simpler but loses the explicit "target was fixed" signal in the trace.

### 31ecd96f_no_payload (airline, no candidate ever built)

- Predicted: `no_payload`
- Observed: `no_payload`
- Diagnosis: Exact match — confirms 31ecd96f is a different failure shape (no candidate generation, not an acceptance-policy issue). Behavioral work for 31ecd96f belongs in proposal-generation territory (Plans P-D, P-E1, P-F) rather than acceptance-tier redesign.

### 3b050ec5_iter1 (best-effort, +8.1pp net, target unfixed, zero recorded debt)

- Predicted: `accepted_with_partial_harvest_debt`
- Observed: `no_target_clusters_fixed` (`first_failed_gate=min_target_clusters_fixed`)
- Diagnosis: Same `min_target_clusters_fixed` gate as ccf1d60d, but with `debt_count=0` and `aggregate_gain_pp=8.1` (still under the 10pp floor). This row also illustrates that a "no-debt attribution-drift accept" tier would be a distinct missing case from the ccf1d60d shape: ccf1d60d has debt to harvest, 3b050ec5 does not. If the design adds an attribution-drift tier with `min_target_clusters_fixed=0`, it likely needs two reason codes (`accepted_with_attribution_drift` and `accepted_with_attribution_drift_and_debt`) to keep the trace specific. Both rows would also still fail `min_aggregate_improvement_pp=10.0` — a candidate-tier design needs to decide whether attribution-drift accepts share the pilot's 10pp floor or live under a separate (lower) floor.

## Go/no-go for Phase 0.2 (deployed smoke)

- `pass_criterion_met`: `true`
- Decision: `DEFER`
  - Two of the three captured runs (ccf1d60d, 3b050ec5) failed the same `min_target_clusters_fixed` gate under the existing pilot policy. Deploying a smoke run with the current policy would reproduce the same rolled-back outcome and burn cluster compute for no acceptance signal. The design conversation about adding an attribution-drift acceptance tier (or relaxing the pilot floor) must close first, and Phase 0.2 should re-run on the updated policy.

## Inputs to Phase 1 (acceptance gate redesign)

- Concrete failed gates observed across real captured runs: `min_target_clusters_fixed` (×2 — ccf1d60d_iter1, 3b050ec5_iter1).
- Open design questions surfaced by the replay:
  - Does an attribution-drift accept-with-debt tier need `min_target_clusters_fixed=0` plus a compensating gate, or can the pilot `min_target_clusters_fixed=1` be relaxed run-wide?
  - Should the pilot's `min_aggregate_improvement_pp=10.0` floor apply to attribution-drift accepts, or does that tier need a lower floor (4–5pp would have accepted ccf1d60d)?
  - Should `unknown_to_hard` debt be admissible under attribution-drift accepts under the same conditions `soft_to_hard` is today, or should attribution-drift accepts admit no debt at all?
  - How do we record the trace difference between "accepted because target was fixed" and "accepted because aggregate moved while target stayed hard" so postmortems can tell the two outcomes apart at a glance?

---

## Phase 0.2 Results — `attribution_drift_policy_pilot_default`

**Run date:** 2026-05-13
**Tool:** `python -m genie_space_optimizer.tools.policy_replay --policy-name attribution_drift_policy_pilot_default`
**Policy under test:** `attribution_drift_policy_pilot_default()` from `optimization/acceptance_policy.py` (added by `2026-05-13-acceptance-gate-redesign-phase-1-plan.md`)

### Replay summary

- `matches`: 3 (ccf1d60d_iter1 → `accepted_with_partial_harvest_debt`, 31ecd96f_no_payload → `no_payload`, 3b050ec5_iter1 → `no_debt_to_harvest`)
- `structured_mismatches`: 0
- `unstructured_mismatches`: 0
- `pass_criterion_met`: `true`

### Per-fixture outcome

- **ccf1d60d_iter1** — accepted under the new policy. delta_pp +4.3 >= floor 4.0, target_fixed=0 satisfies min 0, 1 × `unknown_to_hard` (LOOKUP_FAILED) is in `allowed_debt_buckets`, cumulative 0+1 ≤ 3. The verdict's `accepted_with_partial_harvest_debt` reason at the policy layer is mapped to `accepted_with_attribution_drift_and_debt` by the new branch in `decide_control_plane_acceptance` (Phase 1 Task 6).
- **31ecd96f_no_payload** — unchanged from Phase 0.1. No candidate ever built; classifier short-circuits on `payload_present=false`.
- **3b050ec5_iter1** — `no_debt_to_harvest` under the new policy. delta_pp +8.1 clears every gate but debt_count=0, so the helper returns `no_debt_present`. In production the existing zero-debt `accepted_with_attribution_drift` branch (control_plane.py:1567) handles this case; the new branch is correctly silent for it.

### Go/no-go for the `GSO_ATTRIBUTION_DRIFT_WITH_DEBT` default-flip

- `pass_criterion_met`: `true`
- Decision: **GO** for one lever-loop trial against an anchor with the flag overridden via env var (`GSO_ATTRIBUTION_DRIFT_WITH_DEBT=1`). The default-on flip itself is the subject of a follow-up plan after the trial captures one production-shape run of the new acceptance path.
