# RCO-4b Consolidating Trial Runbook

> Long-lived operator reference. Use this whenever the consolidating
> trial needs to be re-run (e.g. after a defect fix surfaced by the
> first trial).

## Flags to enable (all default-off; set to `"1"` for trial)

| # | Flag | Phase | Helper(s) it routes to |
|---|---|---|---|
| 1 | `GSO_STAGE6_BLAST_RADIUS_PURE` | RCO-4 | `stages.gates.run_blast_radius_production_gate` |
| 2 | `GSO_STAGE6_NARROW_REPL_PURE` | RCO-4 | `stages.gates.resolve_narrow_replacement` |
| 3 | `GSO_STAGE6_APPLYABILITY_PURE` | RCO-4 | `stages.gates.run_applyability_gate` |
| 4 | `GSO_GATE_CHECKS_PROPAGATION_PURE` | RCO-4b A | `stages.eval_gates.run_propagation_wait_gate` |
| 5 | `GSO_GATE_CHECKS_SLICE_PURE` | RCO-4b B | `decide_slice_gate_should_run`, `compute_slice_gate_effective_tolerance`, `decide_slice_gate_post_eval` |
| 6 | `GSO_GATE_CHECKS_P0_PURE` | RCO-4b C | `decide_p0_gate_should_run`, `decide_p0_gate_post_eval` |
| 7 | `GSO_GATE_CHECKS_ASI_EXTRACTION_PURE` | RCO-4b D | `forward_asi_extraction_audit` |
| 8 | `GSO_GATE_CHECKS_BASELINE_DRIFT_PURE` | RCO-4b D | `build_baseline_drift_diagnostic` |
| 9 | `GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE` | RCO-4b E | `decide_full_eval_acceptance` |

`GSO_LOOP_INVARIANTS_STRICT` stays at `"0"` (the lever-loop job
hardcodes that in `jobs/run_lever_loop.py:327`). RCO-2b flips it after
this trial.

## Anchors

| Anchor name | Genie Space replay fixture | Expected `merge_gate_status` | Driving evidence |
|---|---|---|---|
| F9-3b050ec5 | `tests/replay/fixtures/run_3b050ec5_7now.json` | `merge_gate_blocked` | I12 (25 illegal trunk transitions) |
| AIRLINE-clean | `tests/replay/fixtures/airline_real_v1.json` | `healthy` | No HIGH-tier invariants; complete bundle; replay-valid |

## Submission command

Set the nine env vars on the lever-loop cluster (or as job-level
`spark_env_vars` if the cluster is policy-managed) and trigger the job:

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
| 2026-05-11 | prashanth.subrahmanyam | 36 (20 trial preflight + 14 RCO-4b sequence guard + 2 RCO-4 sequence guard) | 323 unit tests across `test_rco4b_*`, `test_rco4_*`, `test_rco5_*`, `test_rco7_*`, `test_rco8_*`, `test_rco2a_*` | All green; trial is submittable. |
