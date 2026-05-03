# GSO Lever Loop Run Analysis: 1036606061019898 / 526124065145154

## Metadata

| Field | Value |
|---|---|
| Job ID | `1036606061019898` |
| Parent run ID | `526124065145154` |
| Lever-loop task run ID | `852330621004424` |
| Optimization run ID | `407772af-9662-4803-be6b-f00a368c528a` *(recovered from job parameters; markers absent)* |
| MLflow experiment ID | `1304929376038672` |
| Workspace | `https://fevm-prashanth-subrahmanyam.cloud.databricks.com` |
| Profile | `fevm-prashanth` |
| Domain | `airline_ticketing_and_fare_analysis` |
| Space ID | `01f143dfbeec15a3a0e87ced8662f4ed` |
| Triggered by | prashanth.subrahmanyam@databricks.com |
| Bundle path | `packages/genie-space-optimizer/docs/runid_analysis/407772af-9662-4803-be6b-f00a368c528a/` |
| Captured at | 2026-05-03T17:40:10Z |
| Phase requested | E → F gating |
| Bundle exit_status | `incomplete` |

## Executive Summary

**Verdict: `MERGE_GATE_GAP` — NOT READY for Phase E → F advancement.**

The lever loop completed successfully (TERMINATED/SUCCESS) and produced a usable space — pre-arbiter accuracy improved from 79.2% (iter 2) to 83.3% (iter 3 = iter 4, plateau). But the **Phase B / decision-trail contract — the central Phase E → F gating signal — is in a state that fails every relevant exit criterion in the roadmap**:

1. **46 illegal-transition violations** when the cycle-10 fixture is replayed against the canonical replay engine — 19 `proposed -> proposed` and 19 `applied -> applied` self-transitions on `gs_009` (per-proposal emit instead of per-qid emit; 12 `add_join_spec` alternatives + 1 `add_sql_snippet_measure` for `gs_009` in iter 2 cause the double-fire), plus 6 `evaluated -> post_eval` and 2 `clustered -> soft_signal` residuals on hard-cluster qids that re-cluster as soft mid-iteration. Phase E requires zero validator warnings.
2. **Decision-trail artifacts (`phase_b/decision_trace/*.json`, `phase_b/operator_transcript/*.txt`) are missing from every MLflow run** in the experiment, despite the notebook output claiming `phase_b/decision_trace/iter_2.json` was logged. The replay-fixture export (forensic stderr stream) was successful — the persistence-to-MLflow leg is the failing one.
3. **`notebook_output.phase_b.iter_record_counts: [75]` is misleading.** A subsequent replay-fixture inspection (added 2026-05-03 — see "Phase B revisited" below) shows the producers actually emitted **210 decision records across all 4 iterations** (37 + 75 + 39 + 59). The summary block in `notebook_output` is a summary-side defect, not a producer-side defect — the original RC-2 producer-gap hypothesis is refuted.
4. **Stdout `GSO_*_V1` markers absent** because the marker emitters (committed earlier today on `fix/gso-lossless-contract-replay-gate`) have not been deployed to the workspace.
5. **MLflow tagging schema mismatch**: lever_loop iteration full_eval runs are tagged only `genie.run_id=…`, not `genie.optimization_run_id=…`, and no run carries `genie.run_type=lever_loop`. The analysis skill's audit therefore picked the `enrichment_snapshot` run as anchor and did not find the 5 untagged iteration runs.

The pilot run did not blow up — the space is shippable. But the decision-trail invariants the roadmap requires for Phase F (operator-readable trace, replay byte-stability, scoreboard derivation from typed records) cannot be verified from this run's evidence.

## Phase B revisited (2026-05-03 update — replay fixture available)

After this postmortem was first written, the replay fixture from the lever-loop's stderr was recovered (the `===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===`/`===END===` block) and saved to `evidence/replay_fixture.json` (and forensically as `tests/replay/fixtures/airline_real_v1_cycle10_raw.json`). It contains **210 decision records across 4 iterations** — refuting the producer-gap hypothesis (RC-2 below) and refining the picture:

| Iter | decision_records | journey_validation | Notes |
|---|---|---|---|
| 1 | 37 | not populated | only H003 reaches the gate; lever5 structural drop + groundedness drop, no patches applied |
| 2 | 75 | `is_valid=False`, **32 violations** | H001 picks up `add_join_spec` from a sub-cluster strategy; 12 alternative proposals all blast-radius dropped, 1 `add_sql_snippet_measure` on H001 patch_cap-selected and applied; the 32 in-fixture violations are a subset of the 46 the canonical replay engine sees |
| 3 | 39 | not populated | AG_DECOMPOSED_H002 emits a `rewrite_instruction` and `add_sql_snippet_measure`; rolled back |
| 4 | 59 | not populated | AG4 emits 5 column-description proposals + 1 instruction rewrite + 1 measure; all rolled back |

Total: **210 records**. The harness-side `notebook_output.phase_b.decision_records_total: 75` was reporting iter 2 only because the in-loop `phase_b` aggregator collects from the same iteration where `_journey_report` is non-None (only iter 2 in this run). Iters 1, 3, and 4 emit records but are not surfaced in the summary.

The cycle-10 intake (see `cycle10_intake.md`) ran the canonical `run_replay()` against this fixture and measured **46 violations vs `BURNDOWN_BUDGET=0`** — a regression. Cycle aborted, canonical baseline reverted, no commit. Top patterns:

| Pattern | Count | Diagnosis |
|---|---|---|
| `proposed -> proposed` | 19 | Iter 2 emits 12 `add_join_spec` alternative proposals + 1 `add_sql_snippet_measure` for `gs_009` and the corresponding stack of `proposal_generated` decision records — the proposal-stage journey emitter fires once per proposal_id, but the journey state machine expects one transition per qid per iteration. 19 ≈ 12 alts + 1 H001 + iter-3/iter-4 alt-proposal residuals. |
| `applied -> applied` | 19 | Same pattern at the apply stage — `patch_applied` decision records fire per (expanded) patch_id, but the `applied` journey stage is one-shot per qid per iter. The replay engine sees 12-13 self-transitions in a single iteration. |
| `evaluated -> post_eval` | 6 | Residual harness emit gap on `gs_013` and `gs_022` — both re-classified between hard and soft mid-run; same kind of pattern that survived Cycle 8's fix. |
| `clustered -> soft_signal` | 2 | `gs_016` appears in both `clusters` (as H002 hard) and `soft_clusters` (as wrong_column / tvf_parameter_error soft) within iter 3/4 — fixture-level cluster-overlap residual. |

**This is a write-side fix on the proposal/applied emitters** — aggregate per-qid before emitting journey events, or de-dupe at the validator boundary. Both producers (`proposal_generated_records`, `patch_applied_records`) need a `seen_qids_this_iter` guard.

This is a NEW failure pattern that did not appear in cycles 7, 8, or 9 because those runs accepted at most a handful of single-proposal AGs per iter. Cycle 10's iter 2 is the first run where one AG carried 12+ alternative proposals, exposing the per-proposal emit. The producer is a Phase B byte-stability candidate (now seen on real data, was previously only theoretical).

## Evidence Collected

| Evidence | Source | Status |
|---|---|---|
| `evidence/job_run.json` | `databricks jobs get-run 526124065145154` | ✅ |
| `evidence/lever_loop_notebook_output.json` | `databricks jobs get-run-output 852330621004424` | ✅ (captured manually; bundle missed it because the Jobs API returns empty `logs` for notebook tasks) |
| `evidence/markers.json` | parsed from lever_loop stdout | ⚠️ empty (no markers emitted by deployed harness) |
| `evidence/mlflow_audit.{json,md}` | `mlflow_audit.audit_optimization_run()` | ⚠️ found 5 sibling runs (4 strategy + 1 enrichment_snapshot); decision-trail artifacts: 0 |
| `evidence/replay_fixture.json` | stdout markers | ❌ absent (`REPLAY_FIXTURE_NOT_IN_STDOUT`) |
| Per-iteration full_eval metrics | `MlflowClient.search_runs()` extra query | ✅ via direct MLflow query (audit missed them due to tag-schema mismatch) |

### Why the bundle came back partial

| Gap | Cause |
|---|---|
| `OPTIMIZATION_RUN_ID_UNRESOLVED` (resolved manually with `--opt-run-id`) | Workspace harness pre-dates the `GSO_RUN_MANIFEST_V1` emitter committed earlier on this branch |
| `REPLAY_FIXTURE_NOT_IN_STDOUT` | Same — replay-fixture stderr markers are emitted, but the deployed harness's stdout/stderr capture is silently routing or none is reaching `get-run-output` |
| `mlflow_audit anchor = enrichment_snapshot` | Deployed harness does not tag any run with `genie.run_type=lever_loop` |
| `mlflow_artifacts: []` | Audit's anchor fallback (enrichment_snapshot) only has `model_snapshots/iter_-1/*` artifacts, which are filtered out by `_walk_audit_artifacts` (only `phase_a/`, `phase_b/` prefixes are pulled) |
| `stdout/stderr files: empty` | Notebook tasks return empty `logs`/`error` from the Jobs `get-run-output` endpoint; the bundle CLI does not yet fall back to `notebook_output.result` |

## Databricks Job And Task State

| Task | Lifecycle | Result |
|---|---|---|
| `preflight` | TERMINATED | SUCCESS |
| `enrichment` | TERMINATED | SUCCESS |
| `baseline_eval` | TERMINATED | SUCCESS |
| **`lever_loop`** | **TERMINATED** | **SUCCESS** |
| `finalize` | RUNNING (at audit time) | — |
| `deploy` | BLOCKED (waiting on finalize) | — |

Lever loop ran for `4028000` ms (~67 min) across 4 iterations. Started 2026-05-03 17:00 UTC (epoch 1777825191841), ended ~18:07 UTC (epoch 1777829221830).

The parent run was not in a terminal state when the bundle was captured because `finalize` and `deploy` were still pending. This **does not block the postmortem of the lever loop itself** — the lever_loop task is terminal — but means downstream UC registration / champion promotion can't be analyzed yet.

## Phase A Journey And Replay Health

❌ **Cannot evaluate.** Replay fixture absent from stdout (`REPLAY_FIXTURE_NOT_IN_STDOUT`). No `phase_a/journey_validation/iter_*.json` artifacts on any MLflow run.

This is purely a contract-deployment gap. Phase A burn-down is officially complete (`✅ complete — 2026-05-02; cycle-9 post-close burndown landed 2026-05-03` per the roadmap), but the deployed harness on the workspace is older than the burn-down close commit and does not emit the fixture/journey-validation artifacts to MLflow yet.

## Phase B Trace And Transcript Health

| Check | Status | Evidence |
|---|---|---|
| `GSO_PHASE_B_V1` markers per iteration | ❌ none | `markers.json` empty |
| `decision_records` present per iteration | ❌ only iter 2 | `notebook_output.phase_b.iter_record_counts: [75]` |
| `phase_b/decision_trace/iter_<N>.json` artifacts | ❌ none in MLflow | Verified by listing artifacts on all 9 runs |
| `phase_b/operator_transcript/iter_<N>.txt` artifacts | ❌ none in MLflow | Same |
| Decision validation count | ❌ **101** in iter 2 | `notebook_output.phase_b.iter_violation_counts: [101]` |
| `target_qids_missing_count` | ✅ 0 | `notebook_output.phase_b.target_qids_missing_count: 0` |
| `producer_exceptions` | ✅ `{}` | `notebook_output.phase_b.producer_exceptions: {}` |

**`notebook_output.phase_b` summary:**

```json
{
  "contract_version": "v1",
  "decision_records_total": 75,
  "iter_record_counts": [75],
  "iter_violation_counts": [101],
  "no_records_iterations": [],
  "artifact_paths": ["phase_b/decision_trace/iter_2.json"],
  "producer_exceptions": {},
  "target_qids_missing_count": 0,
  "total_violations": 101
}
```

Two layered defects:

1. **Producer gap** — `iter_record_counts: [75]` has only one element while `iteration_counter: 4`. Either the emitter that builds this list is only appending for one iteration, or producers aren't emitting on iters 1, 3, 4. `no_records_iterations: []` is misleading because it's only counting iters that *attempted* to capture but found nothing; iters 1/3/4 never appear.
2. **Persistence gap** — even for iter 2 (75 records), the artifact `phase_b/decision_trace/iter_2.json` claimed in `artifact_paths` does **not exist** on any MLflow run. The harness path that writes Phase B artifacts is silently failing — most likely the same exception-suppressed `try: … except: logger.debug(…)` block that this branch's commit `b78247f` instrumented with `genie.phase_b.partial=true`. That instrumentation has not been deployed.

## RCA-Groundedness Health

❌ **Cannot evaluate.** Without decision-trail artifacts, `evidence_refs`, `rca_id`, `root_cause`, `target_qids`, `expected_effect`, `observed_effect`, and `next_action` are unverifiable for the 75 records that exist.

The fact that `target_qids_missing_count: 0` is one positive signal — the records that exist do appear to carry target_qids. But the 101 decision validation violations indicate that journey events and decision records are not lining up; without the actual records on disk, the violation type breakdown is unknown.

## Optimizer Outcome

From `notebook_output.result` + per-iteration MLflow metrics:

| Metric | Value |
|---|---|
| `iteration_counter` | 4 (of 5 max) |
| `levers_attempted` | 17 |
| `levers_accepted` | `[4, 5, 1, 2, 6]` (5 — one of each lever family) |
| `levers_rolled_back` | `[5, 1, 2, 6, 2, 5, 6, 1]` (8 rollbacks) |
| `_debug_ref_sqls_count` | 24 |
| `_debug_failure_rows_loaded` | 24 |

**Accuracy trajectory** (post-arbiter is the gating metric; pre-arbiter is the trustworthy one for ceiling):

| Stage | run_id | post-arbiter (overall) | pre-arbiter (overall) | pre-arbiter (schema) |
|---|---|---|---|---|
| iter_02 full_eval | `2cfd388f…` | 91.67% | 79.2% | 83.3% |
| iter_03 full_eval | `f8bd67b0…` | 91.67% | 83.3% | 91.7% |
| iter_04 full_eval | `5e3d5b68…` | 91.67% | 83.3% | 87.5% |
| iter_04 finalize / repeat_pass_1 | `c8e9c0a0…` | 83.3% (regression) | — | — |
| iter_04 finalize / held_out | `788be7f5…` | 100.0% | 80.0% | 80.0% |

**Reading**:
- Pre-arbiter improved 4.1 pp from iter 2 → iter 3 (79.2% → 83.3%).
- Iter 4 added no pre-arbiter improvement over iter 3 — plateau.
- Post-arbiter capped at 91.67% across iters 2–4.
- The repeatability re-run on iter 4 returned **83.3%** (vs 91.67% on the original eval). That's an **8.3 pp drop on identical input**, suggesting Genie response non-determinism or arbiter non-determinism on this corpus. Repeatability < 90% is itself a finalize concern.
- Held-out pre-arbiter is 80% — 3.3 pp below the iter-4 in-corpus pre-arbiter, which is within reasonable generalization slack.
- 8 rollbacks against 5 accepts (1.6:1 churn ratio) is high but not catastrophic.

**Convergence**: the loop did not hit `max_iterations=5`; it stopped at iter 4. Without `GSO_CONVERGENCE_V1` markers we don't know the typed reason — most likely `arbiter_objective_complete_from_counts` (the first break point in `_run_lever_loop`) given post-arbiter held at 91.67%, or plateau detection.

## MLflow Runs And Traces

9 runs in experiment `1304929376038672`. The audit found 5 (those tagged `genie.optimization_run_id`); the other 4 iteration full_eval runs were missed because they tag only `genie.run_id`. Direct query result:

| Stage | Iteration | Run ID | Tag has `genie.run_type`? | Tag has `genie.optimization_run_id`? | Has `phase_b/*` artifacts? |
|---|---|---|---|---|---|
| `enrichment_snapshot` | -1 | `1256e82d…` | ❌ | ❌ | ❌ |
| `strategy` | 1 | `314788dc…` | ✅ `strategy` | ✅ | ❌ |
| `full_eval` | 2 | `2cfd388f…` | ❌ | ❌ (only `genie.run_id`) | ❌ |
| `strategy` | 2 | `a7e946ba…` | ✅ `strategy` | ✅ | ❌ |
| `full_eval` | 3 | `f8bd67b0…` | ❌ | ❌ | ❌ |
| `strategy` | 3 | `67794da8…` | ✅ `strategy` | ✅ | ❌ |
| `full_eval` | 4 | `5e3d5b68…` | ❌ | ❌ | ❌ |
| `strategy` | 4 | `a70161f4…` | ✅ `strategy` | ✅ | ❌ |
| `finalize_repeatability` | 4 | `c8e9c0a0…` | ❌ | ❌ | ❌ |
| `finalize_held_out` | 4 | `788be7f5…` (RUNNING) | ❌ | ❌ | ❌ |

Trace fetch was **not invoked** — there are no decision-trace artifacts to triage from, and `manifest.trace_fetch_recommendations` is empty.

## Root Cause Hypothesis

Two independent root causes, both surfaced by this run:

### RC-1: Phase B trace persistence is silently broken in production (PHASE_B_TRACE_GAP)

The `notebook_output.phase_b` block is being computed correctly inside `_run_lever_loop` (it claims 75 records, 101 violations, 1 artifact path). But the actual `mlflow.log_text(canonical_decision_json(...), artifact_file="phase_b/decision_trace/iter_2.json")` call (or whatever the deployed equivalent is) is failing — most likely caught by the broad `except Exception:` around the Phase B block — and **no error is reaching the operator** because the deployed harness has no `genie.phase_b.partial` tag (added only on this branch in commit `b78247f`).

Specific implication: the lever_loop's Phase B exception-suppressed block is hiding a real failure mode. The exact failure cannot be diagnosed without either (a) deploying this branch's `genie.phase_b.partial=true` tagging so future runs surface it, or (b) re-running the iter-2 transcript path in isolation against this run's metadata to reproduce the exception.

### RC-2: Producer gap — only iter 2 captured decision records (PHASE_B_TRACE_GAP / RCA_EVIDENCE_GAP)

`iter_record_counts: [75]` for a 4-iteration run is a producer-side defect. Iters 1, 3, and 4 should have captured records too (or, if no records were emitted, should appear in `no_records_iterations` with a typed reason). Neither is happening. Either:

- The decision-record accumulator is being reset somewhere it shouldn't be, OR
- The producers (RCA-loop, applier, gate) are silently not appending on iters where they should, OR
- The list-building logic that produces `iter_record_counts` is single-iteration-aware in a way that drops iter-1/3/4 contributions.

This is a contract-correctness defect, not a deployment-version mismatch; this branch's marker wiring would not change it.

### Combined verdict

**Phase E → F is `MERGE_GATE_GAP` — not `PILOT_NEEDS_RERUN` and not `BASELINE_REGRESSION`.** The pilot run worked (the loop converged, accuracy was reasonable, no infra failures), but the codebase contract isn't ready: re-running this same workspace against this same harness will reproduce the same gaps until both RC-1 and RC-2 are fixed and deployed.

## Recommended Next Actions

In priority order. None of these requires a rerun yet — fix the codebase first.

1. **Investigate RC-2 first** (no deploy needed). Read `_run_lever_loop` Phase B accumulator code + decision-record producers and find why `iter_record_counts` only has the iter-2 contribution. The fix is likely small. This branch's commit `b78247f` doesn't change the producer flow, so this is a pre-existing defect.
2. **Investigate the 101 violations** in iter 2 (no deploy needed). Run `validate_decisions_against_journey()` against the iter-2 records (recoverable via `notebook_output.phase_b.artifact_paths` *if* the artifact actually exists on a different run/path; otherwise reproduce locally from this corpus). 101 violations across 75 records ≫ 100% violation rate — likely a target_qid extraction or journey-event lifecycle mismatch, not 101 distinct contract bugs.
3. **Deploy `fix/gso-lossless-contract-replay-gate`** to the workspace and rerun the lever_loop pilot. After deployment:
   - `GSO_*_V1` markers will land in stdout, so the bundle won't need `--opt-run-id` override.
   - `genie.phase_b.partial=true` will fire on the next Phase B persistence failure, so RC-1 stops being silent.
   - `genie.databricks.{job_id, parent_run_id, lever_loop_task_run_id}` tags will land on the active MLflow run, so the analysis skill can correlate Databricks Jobs evidence to MLflow without the manual `--opt-run-id` step.
4. **Add `genie.run_type=lever_loop` (or equivalent) tag** somewhere in `_run_lever_loop` so the analysis skill's audit anchors to the lever_loop run rather than `enrichment_snapshot`. Alternatively, change `mlflow_audit.audit_optimization_run` to fall back to `genie.stage` matching when `genie.run_type=lever_loop` is absent.
5. **Extend the analysis skill's audit filter** to accept either `genie.optimization_run_id` OR `genie.run_id` as the linking tag. The deployed harness uses `genie.run_id` on iteration full_eval runs; the audit currently misses those.
6. **Extend `evidence_bundle` to capture `notebook_output.result`** for notebook tasks. Today the bundle only persists `logs`/`error`, both empty for notebook tasks, so most lever_loop runs deployed today will produce empty `lever_loop_stdout.txt` files. The notebook result is structured JSON and is the de-facto "stdout" for these tasks — capture it as `evidence/lever_loop_notebook_output.json` (already done manually for this run).
7. **Decide whether the 91.67% post-arbiter / 83.3% repeatability split is acceptable** as a Phase E pilot baseline. If yes, document it; if no, treat repeatability as its own gate.
8. **Do NOT advance to Phase F** until at least RC-1 and RC-2 are fixed and a rerun produces (a) decision-trail artifacts on MLflow for every iteration, (b) zero validator violations, and (c) a non-empty `iter_record_counts` matching iteration count.

Tooling fixes already applied in this session (uncommitted) that you may want to land:

- `_databricks_cli.py`: positional `RUN_ID` for v0.298.0+ CLI; `str()` coercion for IDs returned as integers from JSON.
- `evidence_bundle.py`: `--opt-run-id` and `--experiment-id` overrides; lifted the audit gate so it runs whenever `optimization_run_id` is set.

## Evidence Appendix

### Job parameters (from `evidence/job_run.json`)

```text
run_id           = 407772af-9662-4803-be6b-f00a368c528a
space_id         = 01f143dfbeec15a3a0e87ced8662f4ed
domain           = airline_ticketing_and_fare_analysis
catalog          = prashanth_subrahmanyam_catalog
schema           = genie_space_optimizer
apply_mode       = genie_config
levers           = [1, 2, 3, 4, 5, 6]
max_iterations   = 5
warehouse_id     = 3b1be27d7a807e80
experiment_name  = /Shared/genie-space-optimizer/01f143dfbeec15a3a0e87ced8662f4ed/airline_ticketing_and_fare_analysis
```

### Lever loop result (from `evidence/lever_loop_notebook_output.json`)

See full file under `evidence/lever_loop_notebook_output.json`. Key block:

```json
{
  "iteration_counter": 4,
  "levers_attempted": [5, 1, 2, 6, 4, 5, 1, 2, 6, 5, 1, 2, 6, 2, 5, 6, 1],
  "levers_accepted": [4, 5, 1, 2, 6],
  "levers_rolled_back": [5, 1, 2, 6, 2, 5, 6, 1],
  "phase_b": {
    "contract_version": "v1",
    "decision_records_total": 75,
    "iter_record_counts": [75],
    "iter_violation_counts": [101],
    "no_records_iterations": [],
    "artifact_paths": ["phase_b/decision_trace/iter_2.json"],
    "producer_exceptions": {},
    "target_qids_missing_count": 0,
    "total_violations": 101
  }
}
```

### MLflow audit anchor (from `evidence/mlflow_audit.json`)

Anchor was misidentified as `1256e82d76604cac8af9abb093ee40c5` (`enrichment_snapshot`) because no run carries `genie.run_type=lever_loop`. Sibling runs found:

```text
strategy iter1   314788dc01154a2eb9c12dc414dec0f1
strategy iter2   a7e946baa57045609b28771ba6202129
strategy iter3   67794da81c72498b9ab07c3906db1648
strategy iter4   a70161f4e7a74a1da6c75fb8faa2790b
enrichment_snap  1256e82d76604cac8af9abb093ee40c5
```

Untagged iteration full_eval runs missed by the audit: `2cfd388f…` (iter 2), `f8bd67b0…` (iter 3), `5e3d5b68…` (iter 4), `c8e9c0a0…` (finalize repeat), `788be7f5…` (finalize held_out).

## Replay analysis outputs

Generated by `scripts.replay_runid_fixture` from the captured PHASE_A
fixture. Outputs live under `analysis/` (gitignored) and are reproducible:

```bash
uv run python -m genie_space_optimizer.scripts.replay_runid_fixture \
  --fixture packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle10_raw.json \
  --opt-run-id 407772af-9662-4803-be6b-f00a368c528a \
  --analysis-root packages/genie-space-optimizer/docs/runid_analysis
```

| File | Contains |
| --- | --- |
| `analysis/journey_validation.json` | `JourneyValidationReport` for every iteration, merged. Read `violations[]` to see which transitions failed. |
| `analysis/canonical_journey.json` | Canonical sorted journey events (`(qid, stage_rank, proposal_id)`). Diff against future runs to detect drift. |
| `analysis/canonical_decisions.json` | Canonical sorted `DecisionRecord`s. Empty until PR-B2 ships the per-iteration producer. |
| `analysis/operator_transcript.md` | Human-readable per-iteration narrative + decision-vs-journey validation errors. |

Pre-PR-C/PR-B2, expect `violations > 0` (illegal_transition spam) and
`canonical_decisions.json == "[]"`. PR-C closes the violation gap; PR-B2
populates the decision records.

**Captured baseline (2026-05-03):** running the script against the
cycle-10 fixture produced `violations=8`, `missing_qids=0`,
`decisions=210` — Phase B producer wiring (PR-B2) has effectively
shipped via the earlier Phase B observability work, so decision_records
are already populated. The remaining 8 violations are the
`illegal_transition` pattern PR-C will close.
