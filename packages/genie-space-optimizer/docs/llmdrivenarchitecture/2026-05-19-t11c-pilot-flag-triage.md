# Plan 9 Task 11.C — Pilot-flag telemetry triage (2026-05-19)

Evidence scope: repo-only (fixtures, replay tests, unit tests, committed docs). No new Databricks workspace queries or production pilots were run for this audit.

---

### `attribution_drift_with_debt_enabled`

**Code path:** `config.py:6603` — `_flag_default_on("GSO_ATTRIBUTION_DRIFT_WITH_DEBT")` (default ON after T11.C flip)

**Pilot condition (verbatim):**
> Phase 0.2 (offline replay under the new policy) must return pass_criterion_met=true before this flips.

**Candidate telemetry sources:**
- `tests/replay/test_policy_replay_phase0_predictions.py` — Phase 0.2 parametrized CLI replay (`attribution_drift_policy_pilot_default`, `predictions_attribution_drift.json`)
- `tests/replay/fixtures/policy_replay/predictions_attribution_drift.json` — three pre-registered fixtures (`ccf1d60d_iter1`, `31ecd96f_no_payload`, `3b050ec5_iter1`)
- `genie_space_optimizer.tools.policy_replay` — emits `pass_criterion_met` in `replay_classifier_summary`
- `tests/unit/test_attribution_drift_policy.py`, `tests/unit/test_decide_control_plane_acceptance_attribution_drift_branch.py`
- Marker `GSO_ATTRIBUTION_DRIFT_V1` in `run_analysis_contract.py` (downstream of acceptance branch; not the flip gate)

**Evidence available today:**
- `test_phase0_replay_each_policy_meets_pass_criterion[attribution_drift_policy_pilot_default-predictions_attribution_drift.json-3]` passes locally: `pass_criterion_met=true`, `matches=3`, `unstructured_mismatches=0`
- Phase 0.1 sibling policy (`regression_debt_policy_pilot_default`) also passes in the same test module (sanity that replay harness is healthy)

**Recommendation:** `flip-now`

**Rationale:** The docstring names a single boolean gate (`pass_criterion_met=true`) from Phase 0.2 offline replay. That gate is enforced in CI via `test_policy_replay_phase0_predictions.py` and passes for `attribution_drift_policy_pilot_default` with the expected three exact matches. This satisfies the pilot condition without extrapolating from logic alone.

---

### `l6_narrow_replacement_for_expression_enabled`

**Code path:** `config.py:7279` — `_flag_enabled("GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION")` (default OFF)

**Pilot condition (verbatim):**
> flipped on after the P0 re-pilot confirms the synthesized narrow expressions clear the blast-radius gate AND improve target QID accuracy on the run-809960554692716 fixture.

**Candidate telemetry sources:**
- `tests/replay/fixtures/run_809960554692716_3b050ec5_pre_p0_fix.json`
- `tests/replay/test_p0_narrow_structural_fallback.py`
- `tests/unit/test_narrow_replacement_for_expression.py`, `tests/unit/test_narrow_replacement_diagnostic.py`
- `docs/optimizer-process-design/burn-down-ledger.md` (replay anchor reference)

**Evidence available today:**
- `test_h002_expression_drop_produces_narrow_survivor_when_flag_on` and `test_h002_expression_drop_produces_no_survivor_when_flag_off` both **skip**: fixture does not record a per-iteration HCRF drop with `patch_type=add_sql_snippet_expression`
- Unit tests prove narrow synthesis when flag is explicitly ON with synthetic patches; no committed replay pass tying run-809960554692716 to blast-radius clearance or target-QID accuracy improvement

**Recommendation:** `hold-pending-evidence`

**Rationale:** The pilot condition requires P0 re-pilot on a specific run fixture including blast-radius gate clearance and accuracy delta. The named fixture exists but the replay tests do not execute the contract (skipped). No postmortem in-repo states "P0 re-pilot complete; ready to flip."

---

### `l6_narrow_replacement_branch_c_enabled`

**Code path:** `config.py:7292` — `_flag_enabled("GSO_L6_NARROW_REPLACEMENT_BRANCH_C")` (default OFF)

**Pilot condition (verbatim):**
> Promote to default-on after one corpus pilot confirms Branch C survivors clear the blast-radius gate AND improve target-QID accuracy on the anchor runs.

**Candidate telemetry sources:**
- `tests/unit/test_branch_c_wiring_in_narrow_loop.py`, `tests/unit/test_branch_c_diagnosis.py`, `tests/unit/test_branch_c_l5_synthesis.py`
- Marker `GSO_NARROW_REPLACEMENT_BRANCH_C_SYNTHESIZED_V1` (emitted when flag ON in unit tests)
- `cluster_driven_synthesis.py` — Branch C takes precedence over Branch A when both flags on

**Evidence available today:**
- Unit tests cover flag-on/off wiring and synthetic survivors; no corpus-pilot postmortem or replay fixture asserting anchor-run accuracy improvement with Branch C default-on
- Phase 3.6 anchor tape replay validates other properties but does not exercise narrow Branch C default-on

**Recommendation:** `hold-pending-evidence`

**Rationale:** Pilot condition is explicitly "one corpus pilot" on anchor runs with blast-radius and accuracy gates. No committed corpus-pilot completion record found in repo.

---

### `partial_harvest_with_debt_enabled`

**Code path:** `config.py:6297` — `_flag_enabled("GSO_PARTIAL_HARVEST_WITH_DEBT")` (default OFF)

**Pilot condition (verbatim):**
> Flip to default-on after one corpus pilot validates the pilot-default policy.

**Candidate telemetry sources:**
- `tests/integration/test_partial_harvest_anchor_replay.py` — synthetic anchor rows with flag explicitly `GSO_PARTIAL_HARVEST_WITH_DEBT=1`
- `tests/replay/test_policy_replay_phase0_predictions.py` — Phase 0.1 policy replay (`regression_debt_policy_pilot_default`)
- `tests/unit/test_control_plane_partial_harvest.py`, `tests/unit/test_regression_debt_policy.py`

**Evidence available today:**
- Phase 0.1 offline replay passes (`pass_criterion_met=true` for `regression_debt_policy_pilot_default`) — policy layer only, not the production flag default
- Integration test validates accept-with-debt **when env var is set to 1**; does not certify a completed corpus pilot on airline/7now anchors

**Recommendation:** `hold-pending-evidence`

**Rationale:** Vague "one corpus pilot" condition with no postmortem stating pilot complete. Phase 0.1 replay success is necessary but not the same as the docstring's corpus-pilot gate for flipping the harness flag.

---

### `patch_subset_isolation_live_enabled`

**Code path:** `config.py:6588` — `_flag_enabled("GSO_PATCH_SUBSET_ISOLATION_LIVE")` (default OFF)

**Pilot condition (verbatim):**
> Flips after one corpus pilot validates diagnostic-mode attribution accuracy.

**Candidate telemetry sources:**
- `patch_subset_isolation_enabled()` — default-ON diagnostic sibling (`GSO_PATCH_SUBSET_ISOLATION`)
- Marker `GSO_PATCH_ISOLATION_DIAGNOSTIC_V1` (diagnostic path)
- `tests/unit/test_patch_isolation_observe.py`, `tests/integration/test_cycle_14_v_anchor_shadow_emissions.py`
- `harness.py` — live re-eval gated on `patch_subset_isolation_live_enabled()` and substrate check

**Evidence available today:**
- Diagnostic mode has default-on flip inventory tests (`test_flag_default_on_flip_inventory.py`) and unit coverage for observe/diagnostic markers
- No committed run log or postmortem in repo quantifying diagnostic-mode attribution accuracy across a corpus pilot
- LIVE flag remains default-off in `test_invariants.py`

**Recommendation:** `hold-pending-evidence`

**Rationale:** Pilot requires corpus validation of diagnostic attribution accuracy before behavior-changing live re-eval. Diagnostic path is on; LIVE promotion lacks committed pilot sign-off.

---

### `near_miss_reflection_strict_drop_enabled`

**Code path:** `config.py:7835` — `_flag_enabled("GSO_NEAR_MISS_REFLECTION_STRICT_DROP")` (default OFF)

**Pilot condition (verbatim):**
> operators flip to ON once the near-miss telemetry shows the gate is rejecting only truly repeated shapes.

**Candidate telemetry sources:**
- `near_miss_reflection_enabled()` — default ON (observability path)
- `tests/replay/test_phase_3_replay_predictions.py::test_near_miss_reflection_diagnostic_hold_predicted` (requires `run_replay_for_run_id` helper; may skip)
- `tests/unit/test_phase_3_config_flags.py` — asserts strict_drop default off
- Harness gate at `harness.py` (~21581) using `_nm_strict_drop()`

**Evidence available today:**
- No near-miss / strict_drop mentions in `docs/runid_analysis/` or airline/7now postmortem paths grep-able in repo
- Phase 3 replay near-miss test depends on helper availability; no committed telemetry summary showing false-positive drop rate for strict mode

**Recommendation:** `hold-pending-evidence`

**Rationale:** Operator judgment gate tied to production near-miss telemetry shape quality. Repo lacks committed telemetry demonstrating "only truly repeated shapes" under strict drop.

---

### `rca_card_llm_normalization_enabled`

**Code path:** `config.py:6491` — `_flag_enabled("GSO_RCA_CARD_LLM_NORMALIZATION")` (default OFF)

**Pilot condition (verbatim):**
> Default OFF (deterministic-only) so the initial pilot can verify the deterministic mapper before adding LLM-induced variance.

**Candidate telemetry sources:**
- `tests/unit/test_rca_card_llm_normalization.py`
- `tests/unit/test_config_flags.py::test_rca_card_llm_normalization_disabled_by_default`
- RCA builder flags (`rca_card_builder_enabled` default ON; soft evidence, matchers default ON)

**Evidence available today:**
- Unit tests cover LLM normalization when flag explicitly ON
- Phase 3.6 anchor replay validates typed NSC markers with deterministic RCA path; no committed artifact stating "deterministic mapper pilot verified; enable LLM normalization"
- Docstring pilot is verification of deterministic mapper first — inverse of "flip when pass"; no explicit "flip when X" beyond that sequencing

**Recommendation:** `needs-investigation`

**Rationale:** Pilot condition describes why it starts OFF, not a measurable pass criterion for turning ON. Deterministic RCA path is exercised in tape replay, but no repo evidence anchor says LLM normalization is cleared for default-on. Defer to operator review rather than inferring from deterministic stability alone.

---

### `provisional_synthesis_llm_enabled`

**Code path:** `config.py:7772` — `_flag_enabled("GSO_PROVISIONAL_SYNTHESIS_LLM")` (default OFF)

**Pilot condition (verbatim):**
> Phase 2 Action 2.5 — Provisional archetype LLM synthesis. Default OFF.

**Candidate telemetry sources:**
- `optimization/archetype_learning.py` — early return when flag off
- `tests/unit/test_archetype_learning_tier3_synthesis.py` (explicit `GSO_PROVISIONAL_SYNTHESIS_LLM=1`)
- Parent `archetype_learning_enabled()` default ON

**Evidence available today:**
- No pilot-validation docstring beyond "Default OFF"
- Unit tests only exercise synthesis with env var set to `1`
- Not orphaned: live call site in `archetype_learning.py`

**Recommendation:** `needs-investigation`

**Rationale:** No explicit pilot pass criterion in docstring. Flag is intentionally off pending product decision on provisional LLM synthesis cost/risk. Requires operator intent before default-on; not a deletion candidate.

---

### `stage_handlers_chunk_a_enabled` (+ `b` / `c` / `d`)

**Code path:** `config.py:7464` (A), `7493` (B), `7499` (C), `7505` (D) — `_flag_enabled("GSO_STAGE_HANDLERS_CHUNK_*")` (all default OFF)

**Pilot condition (verbatim):**
> Default-off … flipped on after Phase 2 lands and replay parity is verified. (chunks B/C/D: minimal docstrings; treat as one group)

**Candidate telemetry sources:**
- `tests/unit/test_chunk_flags.py` — all four default false when unset
- `tests/integration/test_chunk_d_flag_on_byte_stability.py` — manual byte-stability procedure for Chunk D
- `docs/architecture/phase-3-6-close.md` — Phase 3.6 anchor tape bar (6 passing tests) does not enable stage-handler chunks
- Chunk A tests in `test_decision_emitters_strategist_context.py` (explicit `GSO_STAGE_HANDLERS_CHUNK_A=1`)

**Evidence available today:**
- Phase 3.6 documents replay parity for harness smoke/NSC markers with chunks **off**
- No automated "replay parity verified" test with all chunk flags default-on
- Chunk B wired in action-group tests only when env explicitly set

**Recommendation:** `hold-pending-evidence`

**Rationale:** Docstring requires Phase 2 land + replay parity before flip. Phase 3.6 closed a related tape-replay bar but explicitly does not constitute chunk-handler default-on promotion. Parity under `GSO_STAGE_HANDLERS_CHUNK_*=1` not evidenced as CI-green for default-on.

---

## Summary

| Flag | Recommendation | Evidence strength |
|---|---|---|
| `attribution_drift_with_debt_enabled` | `flip-now` (flipped in T11.C) | **Strong** — Phase 0.2 `pass_criterion_met=true` in CI replay test |
| `l6_narrow_replacement_for_expression_enabled` | `hold-pending-evidence` | Weak — P0 replay tests skip on fixture gap |
| `l6_narrow_replacement_branch_c_enabled` | `hold-pending-evidence` | Weak — unit-only; no corpus pilot record |
| `partial_harvest_with_debt_enabled` | `hold-pending-evidence` | Medium for policy replay; missing corpus-pilot sign-off |
| `patch_subset_isolation_live_enabled` | `hold-pending-evidence` | Weak — diagnostic on; no LIVE pilot metrics |
| `near_miss_reflection_strict_drop_enabled` | `hold-pending-evidence` | Weak — no committed near-miss telemetry audit |
| `rca_card_llm_normalization_enabled` | `needs-investigation` | N/A — no explicit flip criterion in docstring |
| `provisional_synthesis_llm_enabled` | `needs-investigation` | N/A — intentional default OFF; operator decision |
| `stage_handlers_chunk_a/b/c/d_enabled` | `hold-pending-evidence` | Weak — tape bar does not cover chunk default-on |

## Flips landed this task

**1** flag flipped to default-ON: `attribution_drift_with_debt_enabled` (separate commit `plan9(t11.c.1): …`).

## Revisit in a future audit

- `l6_narrow_replacement_for_expression_enabled` — after `run_809960554692716` fixture records HCRF expression drops or P0 replay tests run green
- `l6_narrow_replacement_branch_c_enabled` and `partial_harvest_with_debt_enabled` — after documented corpus pilot on airline/7now anchors
- `patch_subset_isolation_live_enabled` — after diagnostic attribution accuracy metrics from production runs are exported into repo
- `near_miss_reflection_strict_drop_enabled` — after near-miss shape-repeat false-positive analysis
- `stage_handlers_chunk_*` — after byte-stability / tape replay suite passes with chunks default-on
- `rca_card_llm_normalization_enabled`, `provisional_synthesis_llm_enabled` — operator policy decision with explicit pass criteria

No flags classified `consider-deletion`. No STOP condition triggered (>4 `flip-now`).
