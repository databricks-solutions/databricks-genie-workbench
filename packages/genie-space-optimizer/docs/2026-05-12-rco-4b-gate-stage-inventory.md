# RCO-4b `_run_gate_checks` Stage Inventory

> Source-of-truth mapping of the gate-stages inside `harness._run_gate_checks` as of 2026-05-12. Cite this from every RCO-4b phase plan (B/C/D/E) and from any future plan that touches the function's body. When the function's line ranges shift, update this document in the same commit.

## The eight gate-stages (production firing order)

| Order | gate_name= sentinel | Approximate line range | Side effects | Rollback path |
|---|---|---|---|---|
| 1 | `propagation_wait` (confirmed-fast) | 12915-12924 | `_audit_emit` (decision="confirmed") | none |
| 1' | `propagation_wait` (full-budget) | 12925-12946 | `_audit_emit` (decision="waited_full_budget", reason_code="no_verifiable_snippet" \| "snippet_not_observed") | none |
| 2 | `slice_gate` | 12948-13143 | `mlflow.end_run`; `run_evaluation`; `write_stage`; `write_iteration`; `update_provenance_gate`; `log_gate_feedback_on_traces`; `_audit_emit` (decision="rolled_back" on drop) | early return on drop |
| 3 | `p0_gate` | 13144-13219 | `mlflow.end_run`; `run_evaluation`; `write_iteration`; `_audit_emit` (decision="rolled_back" on failures) | early return on failures |
| 4 | `full_eval_acceptance` (Part 1 — eval run) | 13220-13478 | `mlflow.end_run`; `write_stage`; `_eval_stage.run_evaluation` (full corpus); `write_iteration`; attribution-drift reattribution | none yet |
| 5 | `asi_extraction` (gate_name override via `_asi_audit_1.get("gate_name") or "asi_extraction"`) | 13340-13415 | `_audit_emit` for accept-with-stale-instructions | scoped audit row |
| 6 | `baseline_drift_diagnostic` | 13420-13478 | `_audit_emit` for `suspected_stale_baseline` | none — diagnostic only |
| 7 | `pre_arbiter_regression_guardrail` | 13760-13860 | `_audit_emit` for regression-guardrail rollback | early return |
| 8 | `full_eval_acceptance` (Part 2 — decide) | 14000-14235 | `_audit_emit` for accept/reject/debt-partition branches; constructs the canonical `ControlPlaneAcceptance` | accept OR rollback depending on branch |

The sequence-guard test at `tests/unit/test_rco4b_run_gate_checks_sequence_guard.py` pins this order. Note: that test's regex captures the bare identifier (`_asi_audit_1`) rather than the literal fallback (`asi_extraction`) for stage 5, because the harness emits `gate_name=_asi_audit_1.get("gate_name") or "asi_extraction"`. The semantic gate-name is `asi_extraction`; the wire identifier is `_asi_audit_1`. Both appear in the guard.

## Per-stage I/O contract

### 1. propagation_wait

**Inputs:**
- `apply_log` (used only to inspect `applied[].patch.{new_text,proposed_value,text_instructions}` and `applied[].action.type` and `applied[].patch.enable_entity_matching` to determine whether dictionary changes triggered an extended wait)
- `space_id` (used for `fetch_space_config` polling)
- `ag_id` (printed; included in audit row)
- `w` (`WorkspaceClient`, used by `fetch_space_config`)
- Module-level constants: `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS`, `PROPAGATION_WAIT_SECONDS`

**Decision shape:**
- Whether propagation was confirmed during polling (boolean)
- Elapsed seconds (float)
- `audit_decision: "confirmed" | "waited_full_budget"`
- `reason_code: None | "no_verifiable_snippet" | "snippet_not_observed"` (None when confirmed)

**Side effects (kept in harness):**
- `time.sleep`
- `fetch_space_config`
- `print` of section header
- `_audit_emit`

**Why this is the canonical Phase A extraction:**
- No `run_evaluation` call.
- No rollback path.
- Smallest input set.
- Side effects are all easily injected as dependencies (`fetch_space_config_fn`, `sleep_fn`).

### 2. slice_gate

**Inputs:**
- `benchmarks` (full list — used to compute `_all_qids` and `_full_corpus`)
- `affected_question_ids` (set)
- `prev_failure_qids` (set or None)
- `patches_applied` (count for audit metrics)
- `patched_objects`
- `best_scores`, `best_accuracy`, `noise_floor`
- `ag_id`, `run_id`, `iteration_counter`, `lever_keys`, `prev_model_id`
- Module-level: `ENABLE_LEGACY_SLICE_P0_GATES`, `ENABLE_SLICE_GATE`, `SLICE_GATE_MIN_REDUCTION`, `SLICE_GATE_TOLERANCE`, `SLICE_GATE_TOLERANCE_SMALL_CORPUS`, `SLICE_GATE_SMALL_CORPUS_ROWS`, `DEFAULT_THRESHOLDS`, `MAX_BENCHMARK_COUNT`

**Two-step decision shape (the slice gate is naturally two helpers):**
1. **Pre-eval decision:** should the slice run? If yes, what benchmark subset and tolerance?
2. **Post-eval decision:** given the eval result, do we roll back?

**Side effects (kept in harness):**
- `mlflow.end_run`
- `_ensure_sql_context`
- `run_evaluation`
- `write_stage`, `write_iteration`, `update_provenance_gate`, `log_gate_feedback_on_traces`
- `_audit_emit`
- `print`

**Status:** extracted in Phase B — three pure helpers in
`stages/eval_gates.py` (`decide_slice_gate_should_run`,
`compute_slice_gate_effective_tolerance`,
`decide_slice_gate_post_eval`). Default-off behind
`GSO_GATE_CHECKS_SLICE_PURE`. See
`2026-05-12-rco-4b-phase-b-slice-gate-extraction-plan.md`.

### 3. p0_gate

**Inputs:**
- `benchmarks` (filtered via `filter_benchmarks_by_scope(benchmarks, "p0")`)
- `ag_id`, `run_id`, `iteration_counter`, `lever_keys`, `prev_model_id`
- Module-level: `ENABLE_LEGACY_SLICE_P0_GATES`, `MAX_BENCHMARK_COUNT`

**Decision shape:** same two-step pattern as slice_gate (run / drop).

**Status:** extracted in Phase C — two pure helpers in
`stages/eval_gates.py` (`decide_p0_gate_should_run`,
`decide_p0_gate_post_eval`). Default-off behind
`GSO_GATE_CHECKS_P0_PURE`. See
`2026-05-12-rco-4b-phase-c-p0-gate-extraction-plan.md`.

### 4. full_eval_acceptance (Part 1 — eval run)

**Inputs:**
- `benchmarks` (full corpus)
- `predict_fn`, `scorers`, `metadata_snapshot`, `patches`
- `phase_h_anchor_run_id`
- `accepted_baseline_rows_for_control_plane`
- Several `RunEvaluationKwargs` fields

**Decision shape:** does the eval run succeed? What is the full-eval result payload?

**Side effects:**
- `mlflow.end_run`, `write_stage`, `_eval_stage.run_evaluation`, `write_iteration`, attribution-drift reattribution

**Status:** verdict consolidation extracted in Phase E — one pure
helper in `stages/eval_gates.py` (`decide_full_eval_acceptance`).
Default-off behind `GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE`. Phase
E is the final decomposition phase; with E landed, every gate-stage
inside `_run_gate_checks` has a pure-helper extraction. See
`2026-05-12-rco-4b-phase-e-full-eval-acceptance-plan.md`.

### 5. asi_extraction

**Inputs:**
- `full_result` (from full-eval Part 1)
- `metadata_snapshot`
- `apply_log` (specifically the applied patches' new instruction text)
- Cycle 14-T2 ASI-detection helpers (`_extract_asi`)

**Decision shape:** is this an ASI scenario? What audit row should fire?

**Side effects:** `_audit_emit` only.

**Status:** extracted in Phase D — pure helper in
`stages/eval_gates.py` (`forward_asi_extraction_audit`). Default-off
behind `GSO_GATE_CHECKS_ASI_EXTRACTION_PURE`. See
`2026-05-12-rco-4b-phase-d-asi-extraction-and-baseline-drift-plan.md`.

Note: the sequence-guard regex captures `_rco4b_asi_out` as the
flag-on branch's wire identifier (from `gate_name=_rco4b_asi_out.gate_name`)
and `_asi_audit_1` as the legacy branch's wire identifier (from
`gate_name=_asi_audit_1.get("gate_name") or "asi_extraction"`). Both
resolve to the literal `"asi_extraction"` at runtime; the dual entries
in `_EXPECTED_ORDER` are intentional Phase D state.

### 6. baseline_drift_diagnostic

**Inputs:**
- `prev_iter_pre_accept_baseline` (float or None)
- Current `post_arbiter_accuracy` (computed from full_result)
- Module-level: `BASELINE_DRIFT_DIAGNOSTIC_PP`

**Decision shape:** does the diagnostic trigger? Audit metrics payload.

**Side effects:** `_audit_emit` only.

**Status:** extracted in Phase D — pure helper in
`stages/eval_gates.py` (`build_baseline_drift_diagnostic`). Default-off
behind `GSO_GATE_CHECKS_BASELINE_DRIFT_PURE`. See
`2026-05-12-rco-4b-phase-d-asi-extraction-and-baseline-drift-plan.md`.

### 7. pre_arbiter_regression_guardrail

**Inputs:**
- Full-eval `pre_arbiter` and `post_arbiter` accuracies
- `best_accuracy`
- Module-level guardrail thresholds

**Decision shape:** does the guardrail trip? What rollback reason?

**Side effects:** `_audit_emit`, early return.

**Why deferred to Phase E:** sub-decision of `full_eval_acceptance` Part 2; ships with it.

### 8. full_eval_acceptance (Part 2 — decide)

**Inputs:**
- All Part 1 outputs
- `target_qids` classification from upstream control-plane handoff
- `cumulative_regression_debt`
- Multiple regression-bucket QID lists

**Decision shape:** the canonical `ControlPlaneAcceptance` instance + accept/reject/debt-partition branch tag.

**Side effects:** `_audit_emit` x N depending on branch.

**Why deferred to Phase E:** the canonical-acceptance construction site. Unblocks RCO-4's alignment-gate deferral.

## Cross-stage state

Inside `_run_gate_checks`, the following local names are shared across stage boundaries. The extraction must thread these through helper inputs/outputs explicitly:

- `_decision_rows: list[dict]`, `_decision_order: list[int]` — audit-row accumulator. Stays in the harness; helpers RETURN audit rows that the harness appends via `_audit_emit`.
- `_audit_emit`, `_audit_persist` — closures. Stay in the harness.
- `_run_slice: bool` — set by slice_gate's pre-eval decision; consumed nowhere downstream (purely local to the slice region).
- `slice_result`, `p0_result`, `full_result` — each stage's eval payload. Stage N+1 may read prior payloads (especially Part 2 of full-eval reading Part 1's `full_result`).
- `_blast_kept`, `_blast_dropped`, `_blast_target_qids` — RCO-4 owns these (already extracted via `run_blast_radius_production_gate`).

## Re-visit trigger

This inventory is rewritten when:

1. A new gate-stage is added to `_run_gate_checks` (i.e. a new `gate_name=` sentinel appears). The sequence-guard test at `tests/unit/test_rco4b_run_gate_checks_sequence_guard.py` will fail.
2. A gate-stage's line range shifts by more than ~50 lines (re-grep and update the table).
3. A gate-stage's side-effect surface changes (e.g. a new `write_*` call gets added inside its block).

## Cross-references

- RCO-4 deferred gates: `docs/2026-05-11-rco-4-deferred-gates.md`
- RCO-4 gate inventory (Stage-6 production-firing gates, outside `_run_gate_checks`): `docs/2026-05-11-rco-4-gate-inventory.md`
- Phase-roadmap for RCO-4b: `docs/2026-05-12-rco-4b-phase-roadmap.md`
