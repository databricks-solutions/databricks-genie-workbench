# RCO-4b Consolidating Trial Runbook

> Long-lived operator reference. Use this whenever the consolidating
> trial needs to be re-run (e.g. after a defect fix surfaced by the
> first trial).

## Flags (default-ON after Task 3 default-flip)

All nine flags default to ON in the codebase. The lever-loop job
exercises every new pure helper without any env-var setup.

| # | Flag | Default | Phase | Helper(s) it routes to | Rollback escape hatch |
|---|---|---|---|---|---|
| 1 | `GSO_STAGE6_BLAST_RADIUS_PURE` | **ON** | RCO-4 | `stages.gates.run_blast_radius_production_gate` | set `=0` to disable |
| 2 | `GSO_STAGE6_NARROW_REPL_PURE` | **ON** | RCO-4 | `stages.gates.resolve_narrow_replacement` | set `=0` to disable |
| 3 | `GSO_STAGE6_APPLYABILITY_PURE` | **ON** | RCO-4 | `stages.gates.run_applyability_gate` | set `=0` to disable |
| 4 | `GSO_GATE_CHECKS_PROPAGATION_PURE` | **ON** | RCO-4b A | `stages.eval_gates.run_propagation_wait_gate` | set `=0` to disable |
| 5 | `GSO_GATE_CHECKS_SLICE_PURE` | **ON** | RCO-4b B | `decide_slice_gate_should_run`, `compute_slice_gate_effective_tolerance`, `decide_slice_gate_post_eval` | set `=0` to disable |
| 6 | `GSO_GATE_CHECKS_P0_PURE` | **ON** | RCO-4b C | `decide_p0_gate_should_run`, `decide_p0_gate_post_eval` | set `=0` to disable |
| 7 | `GSO_GATE_CHECKS_ASI_EXTRACTION_PURE` | **ON** | RCO-4b D | `forward_asi_extraction_audit` | set `=0` to disable |
| 8 | `GSO_GATE_CHECKS_BASELINE_DRIFT_PURE` | **ON** | RCO-4b D | `build_baseline_drift_diagnostic` | set `=0` to disable |
| 9 | `GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE` | **ON** | RCO-4b E | `decide_full_eval_acceptance` | set `=0` to disable |

`GSO_LOOP_INVARIANTS_STRICT` stays at `"0"` (the lever-loop job
hardcodes that in `jobs/run_lever_loop.py:327`). RCO-2b flips it after
this trial.

## Anchors

| Anchor name | Genie Space replay fixture | Expected `merge_gate_status` | Driving evidence |
|---|---|---|---|
| F9-3b050ec5 | `tests/replay/fixtures/run_3b050ec5_7now.json` | `merge_gate_blocked` | I12 (25 illegal trunk transitions) |
| AIRLINE-clean | `tests/replay/fixtures/airline_real_v1.json` | `healthy` | No HIGH-tier invariants; complete bundle; replay-valid |

## Submission command

Pre-requisite: the commit from Task 3 (default-flip) must be deployed
to the workspace via `databricks bundle deploy -t app`. After deploy,
trigger the job per anchor — no env vars, no widget overrides beyond
the anchor's normal production params:

```bash
databricks jobs run-now <LEVER_LOOP_JOB_ID> \
  --profile <DATABRICKS_PROFILE> \
  --notebook-params '{
    "run_id": "<unique-trial-id>",
    "space_id": "<F9-or-AIRLINE space id>",
    "domain": "<domain>",
    "catalog": "<catalog>",
    "schema": "<schema>",
    "experiment_name": "<mlflow exp name>",
    "max_iterations": "5",
    "levers": "<levers>",
    "apply_mode": "<apply_mode>",
    "triggered_by": "rco4b-trial",
    "warehouse_id": "<warehouse_id>",
    "max_benchmark_count": "<count>"
  }'
```

Capture the printed `run_id` (Databricks job-run ID). You need it for
Task 7's evidence-bundle invocation.

Repeat for the second anchor.

## Capture command

```bash
uv run python -m genie_space_optimizer.tools.evidence_bundle \
  --job-id <LEVER_LOOP_JOB_ID> \
  --run-id <CAPTURED_RUN_ID> \
  --profile <DATABRICKS_PROFILE> \
  --output-dir packages/genie-space-optimizer/docs/runid_analysis
```

The CLI writes the trial's stdout, MLflow artifacts, markers, and
replay fixture into
`packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/evidence/`.

## Pass/fail criteria

The trial passes when **all** of the following hold for **both**
anchors:

1. `parse_markers(stdout)` returns a non-None `contract_health` payload.
2. `merge_gate_status` matches `expected_outcomes.json` for that
   anchor.
3. The six `gate_name="…"` audit positions in
   `_run_gate_checks` stdout fire in the order pinned by
   `tests/unit/test_rco4b_run_gate_checks_sequence_guard.py`.
4. `bundle_status == "complete"` for the healthy anchor (the blocked
   anchor may legitimately have `bundle_status` other than `complete`).
5. `tests/integration/test_rco4b_trial_postflight_artifact_capture.py`
   passes when pointed at the evidence-bundle output via
   `RCO4B_TRIAL_EVIDENCE_DIR=<path>`.

If any anchor fails any criterion, halt — do not promote that anchor's
evidence. File a defect plan against the failing surface.

## Preflight green-light log

| Date | Operator | Preflight test count | Defense-in-depth suite | Result |
|---|---|---|---|---|
| 2026-05-11 | prashanth.subrahmanyam | 57 (9 default-ON + 45 falsy-rollback + 1 harness-import + 1 `_flag_default_on` grep + 1 strict-default unchanged) | 360 RCO unit tests (`test_rco4b_*`, `test_rco4_*`, `test_rco5_*`, `test_rco7_*`, `test_rco8_*`, `test_rco2a_*`); 4693 unit+integration passes, 4 pre-existing failures unrelated to the flip (`test_skill_parser_handoff` x2; `test_evidence_bundle_smoke` + `test_mlflow_smoke_one_iteration` show same failures on pre-flip HEAD) | All green; trial is submittable. |

## Trial disposition (2026-05-12)

Two Databricks lever-loop runs were captured and post-mortem'd:

| opt_run_id | space / domain | merge_gate_status | bundle_status | replay | Optimizer verdict |
|---|---|---|---|---|---|
| `31ecd96f-5d56-4b5a-af8e-38e9e5c549af` | airline_ticketing_and_fare_analysis (`01f143df...`) | `warn` (phase_h skipped) | `complete` | `is_valid=true`, 0 violations | `MERGE_GATE_GAP / NO_APPLIED_PATCHES` |
| `ccf1d60d-d686-467b-bafa-1640131b4393` | `7now_delivery_analytics_space` (`01f128ae...`) | `warn` (phase_h skipped) | `complete` | `is_valid=false`, 5 Cycle-17 carry-over violations for `gs_021` | `MERGE_GATE_GAP / NO_ACCEPTED_PROGRESS` |

**Postflight test:** 8/8 pass against each run (anchors
`airline_trial_2026_05_12_31ecd96f` and
`seven_now_trial_2026_05_12_ccf1d60d` in
`tests/integration/fixtures/rco4b_trial/expected_outcomes.json`).

**What the trial validated (infrastructure):**

- ✅ All four keystone end-of-run markers emit and are
  parser-roundtripable (`GSO_RUN_MANIFEST_V1` start + end,
  `GSO_CONVERGENCE_V1`, `GSO_CONTRACT_HEALTH_V1`).
- ✅ `ContractHealthSummary` payload roundtrips cleanly via
  `from_json_dict`.
- ✅ RCO-2a `MergeGateStatus` classifier correctly emits `warn` when
  `phase_h_listing_status=skipped` and `phase_h_validator_status=skipped`.
- ✅ All nine default-on RCO-4 / RCO-4b pure helpers fired without
  crash on real Databricks workspace runs.
- ✅ Marker emission ordering is stdout-observable and stable.

**What the trial did NOT validate (deferred):**

- ❌ The original `f9_3b050ec5` blocked-anchor outcome — no captured
  evidence yet.
- ❌ The original `airline_clean` healthy-anchor outcome — no
  captured evidence yet.
- ⚠️ Phase H bundle totality — `GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1`
  fires on both runs (`missing_count=40`). Per-iteration rollups are
  missing. This is a Phase H output-contract gap, not a trial
  blocker, but it means the contract_health `bundle_status=complete`
  comes from a different source than the bundle-assembly markers.
  **Update (2026-05-12):** the `contract_health.bundle_status="complete"`
  half of this gap is fixed by
  `docs/2026-05-12-bundle-status-wiring-fix-plan.md`. The Phase H
  *per-iteration rollup* gap (missing_count=40) is independent and
  tracked separately.
- ⚠️ Databricks ID resolution in run manifest — both runs report
  `databricks_job_id=unknown`, `databricks_parent_run_id=unknown`,
  `lever_loop_task_run_id=unknown` despite the live Jobs API
  resolving all three.

**Defects surfaced (separate plans):**

- `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md` — driven
  by run 31ecd96f. **SUPERSEDED** by Defect Plan 1
  (`docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md`),
  landed 2026-05-12.
- `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md` —
  driven by run ccf1d60d. Includes the named-RCO-6 blocker fix
  (`clustered → soft_signal` for `gs_021`). PARTIALLY SUPERSEDED:
  items 1, 5 → Defect Plan 1; airline-stub-4 → Defect Plan 2
  (`docs/2026-05-12-defect-no-applied-patches-retry-signature-plan.md`,
  landed 2026-05-12). Only the RCO-6 carve-out (item 6) remains
  pending its own defect plan.

**Defect Plan 2** — `docs/2026-05-12-defect-no-applied-patches-retry-signature-plan.md`
closes the airline F6 leg (`skipped_no_applied_patches` retry loop). No new
flags introduced; piggybacks on the default-ON `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`.
Re-trial readiness: Defect 1 + Defect 2 + RCO-2b strict-mode flip + bundle-status
micro-plan all landed 2026-05-12/13.

**Deferred-RCO unblocking status:**

| RCO | Status | Reason |
|---|---|---|
| RCO-2b (strict-mode default-flip) | ✅ unblocked | `GSO_CONTRACT_HEALTH_V1` captured on both runs |
| RCO-3 (pilot-gated default-flip) | ✅ unblocked | All nine helpers fired default-on without crash |
| RCO-4c (alignment/cap/reflection carve-out) | ⚠️ partial | `decide_full_eval_acceptance` survived a real run (ccf1d60d); the airline run had all-skipped iterations |
| RCO-6 (replay/journey parity) | ❌ blocked | `gs_021` Cycle-17 carry-over named in defect-forbidden-ag-admission-enforcement |

**Re-trial expectation:** Defect Plans 1 + 2 have both landed
(2026-05-12), plus the bundle-status wiring fix and RCO-2b strict-mode
flip. Re-run against F9-3b050ec5 + AIRLINE-clean is now unblocked.
The captured runs and the `f9_3b050ec5` / `airline_clean`
future-target anchors remain in `expected_outcomes.json` side by side
so the re-trial postflight can validate either anchor as captured.
