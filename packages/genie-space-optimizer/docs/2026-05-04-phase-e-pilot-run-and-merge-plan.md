# Phase E Pilot Run + Merge Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the single pre-merge real-Genie pilot run, validate the lossless contract end-to-end, flip `raise_on_violation=True`, prove CI fails closed via a deliberately-broken sanity PR, and merge `fix/gso-lossless-contract-replay-gate` to `main`.

**Architecture:** Phase E is the merge gate. It assumes Phases A-D plus PR-A through PR-E plus the Phase F+H wire-up plan have all landed. Phase E does **not** introduce new features — it validates the assembled artifact, hardens two soft gates into hard gates, and lands the merge.

**Tech Stack:** Real Databricks workspace (FE-Vending Machine), MLflow tracking, airline benchmark fixture, journey contract validator, decision-trace replay-side hard gate.

**Depends on:** the F+H harness wire-up plan ([`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md)) reaching Phase C Commit 19's smoke-test green. **Do not start Phase E** until C19 passes locally and the audit doc Section 4-6 action items are resolved.

---

## File Structure

| File | Responsibility | Touched By |
|---|---|---|
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:17760` | `_validate_journeys_at_iteration_end(..., raise_on_violation=False)` call site that becomes the hard gate | Task 6 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py` | Replay-side fail-closed check on missing required decision records | Task 7 |
| `packages/genie-space-optimizer/tests/unit/test_journey_validator_hard_gate.py` | New unit test pinning the `raise_on_violation=True` behavior | Task 6 |
| `packages/genie-space-optimizer/tests/replay/test_decision_trace_hard_gate.py` | New replay test pinning the decision-trace fail-closed | Task 7 |
| `packages/genie-space-optimizer/docs/2026-05-04-phase-e-pilot-run-validation-matrix.md` | Capture sheet for the pilot-run validation results | Task 5 |
| `packages/genie-space-optimizer/docs/2026-05-04-phase-e-sanity-pr-procedure.md` | Step-by-step for the deliberately-broken sanity PR | Task 8 |

---

## Pre-flight checklist

Before scheduling the pilot run. **All items must pass green locally on the merge candidate branch HEAD.**

### Task 0: Verify the merge candidate branch is ready

**Files:**
- Read: `git log --oneline fix/gso-lossless-contract-replay-gate ^main`
- Read: `packages/genie-space-optimizer/docs/2026-05-04-phase-f-h-wireup-audit-findings.md`

- [ ] **Step 0.1: Confirm the F+H harness wire-up plan is fully landed**

Run:

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git log --oneline fix/gso-lossless-contract-replay-gate -- \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stage_io_capture.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_output_bundle.py
```

Expected: at least 17 commits matching `Phase F+H Commit A[1-6]|B[1-8]|1[7-9]` (i.e. the entire F+H plan landed).

If fewer commits, halt — Phase E is blocked.

- [ ] **Step 0.2: Run the full unit + replay test suite**

```bash
cd packages/genie-space-optimizer
pytest -q
```

Expected: ALL pass. Specifically:

- `tests/unit/test_stage_conformance.py` (Protocol conformance for all 9 stages).
- `tests/replay/test_phase_f_h_wireup_byte_stable.py` (T0 byte-stability snapshot).
- `tests/integration/test_phase_h_bundle_smoke.py` (path computations).
- `tests/integration/test_phase_h_bundle_populated.py` (C19 smoke).
- `tests/unit/test_mlflow_*` (E.0 MLflow audit + backfill + anchor).
- `tests/integration/test_mlflow_smoke_one_iteration.py` (E.0 anchor smoke).

If any test fails, halt and remediate before scheduling the pilot.

- [ ] **Step 0.3: Confirm `raise_on_violation=False` is still the current state**

```bash
grep -n "raise_on_violation=False\|raise_on_violation=True" \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

Expected: exactly one match at `:17760` for `raise_on_violation=False`. (Task 6 will flip this.)

- [ ] **Step 0.4: Verify the replay fixture is up to date**

```bash
ls -la packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json
git log --oneline -5 packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json
```

Expected: a recent commit (last 7 days) updating the fixture to reflect Phase F+H wire-up state.

If the fixture is older than the F+H wire-up commits, run `tests/replay/conftest.py` regenerator (or the `lever_loop_replay.run_replay(...)` driver) to refresh; do not start the pilot with a stale fixture.

### Task 1: Workspace + warehouse provisioning

**Files:** none. Operator-driven setup.

- [ ] **Step 1.1: Choose a fresh FE-Vending Machine workspace**

Use the `databricks-fe-vm-workspace-deployment` skill or `vibe vm reserve --workspace-type=standard --hours=4` (or whatever the team's current invocation is). The pilot needs ~2 hours of run time + ~1 hour of validation, so reserve 4 hours.

Record the workspace URL and profile name in the validation matrix doc (Task 5).

- [ ] **Step 1.2: Confirm the airline benchmark Genie space is provisioned**

```bash
databricks --profile <pilot-profile> genie spaces list | grep airline
```

Expected: at least one Genie space matching `airline*`. Record the `space_id` for use in the run config.

If absent, provision via the standard airline benchmark setup (out of scope for this plan; see the team's standard onboarding doc).

- [ ] **Step 1.3: Confirm a SQL warehouse is available**

```bash
databricks --profile <pilot-profile> warehouses list --output json | \
  jq '.[] | select(.state=="RUNNING") | {id, name, state}'
```

Expected: at least one running warehouse. Record its ID for use in the run config.

### Task 2: Lock the run config

**Files:**
- Edit (per-pilot): `packages/genie-space-optimizer/configs/pilot_e.yaml` (create if absent — copy from the airline benchmark template)

- [ ] **Step 2.1: Create or update the pilot run config**

```yaml
# packages/genie-space-optimizer/configs/pilot_e.yaml
run_id: "pilot-e-2026-05-04"
space_id: "<airline-space-id-from-Task-1.2>"
domain: "airline"
catalog: "<pilot-catalog-from-FE-VM>"
schema: "<pilot-schema-from-FE-VM>"
warehouse_id: "<warehouse-id-from-Task-1.3>"
apply_mode: "in_place"
max_iterations: 10
benchmark_path: "/Volumes/<pilot-catalog>/<pilot-schema>/benchmarks/airline_benchmark.json"
```

The exact field set depends on `run_lever_loop.py`'s entrypoint signature; mirror the existing `configs/airline_benchmark.yaml` structure exactly.

- [ ] **Step 2.2: Commit the config to a side branch**

Pilot configs live on a throwaway branch so the merge candidate stays clean:

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git checkout -b pilot-e-config-2026-05-04 fix/gso-lossless-contract-replay-gate
git add packages/genie-space-optimizer/configs/pilot_e.yaml
git commit -m "config(pilot): airline benchmark pilot E run config (throwaway branch)"
```

This branch is for run reproducibility only; do NOT merge it.

### Task 3: Bundle-populated smoke test (final pre-flight)

**Files:**
- Run: `packages/genie-space-optimizer/tests/integration/test_phase_h_bundle_populated.py`

- [ ] **Step 3.1: Run the C19 smoke test on the merge candidate**

```bash
cd packages/genie-space-optimizer
pytest tests/integration/test_phase_h_bundle_populated.py -q -v
```

Expected: PASS. The test stubs MLflow and the capture decorator's `_log_text`, and verifies `bundle_artifact_paths` produces non-empty paths for all 9 stage keys per iteration.

If FAIL, halt — this is the last gate before the expensive real run.

---

## Pilot-run execution

### Task 4: Execute the real-Genie pilot run

**Files:** none. Job-driven execution on the pilot workspace.

- [ ] **Step 4.1: Submit the run via the Databricks Jobs API**

```bash
cd packages/genie-space-optimizer

databricks --profile <pilot-profile> bundle deploy \
  --target dev \
  --var "config_path=configs/pilot_e.yaml"

databricks --profile <pilot-profile> bundle run lever_loop_pilot_e \
  --target dev
```

The exact bundle target name depends on `databricks.yml`; the team's standard pilot job is named `lever_loop` or `lever_loop_pilot`.

- [ ] **Step 4.2: Poll the job until completion (~2 hours)**

```bash
databricks --profile <pilot-profile> jobs get-run <run-id-from-Step-4.1> --output json | \
  jq '{state: .state.life_cycle_state, result: .state.result_state, start_time, end_time}'
```

Repeat every 10 minutes until `state == "TERMINATED"` and `result == "SUCCESS"`.

If `result == "FAILED"`, fetch the run output:

```bash
databricks --profile <pilot-profile> jobs get-run-output <run-id> --output json | jq '.error // .notebook_output'
```

Diagnose the failure. Most likely causes mid-pilot:
- MLflow logging failure (check `_log_text` warnings in driver stdout).
- Genie API throttling (retry with reduced `max_iterations`).
- Apply-time SQL error (the apply pipeline raises `FailedRollbackVerification`; this is a real bug in the pipeline, not in the pilot).

Do NOT proceed to Task 5 until the run completes successfully.

- [ ] **Step 4.3: Capture the run identifiers**

From the job-run output, record:

| Identifier | Source | Use |
|---|---|---|
| `databricks_job_id` | Step 4.1 output | All MLflow audit queries |
| `databricks_parent_run_id` | Step 4.2 output | All MLflow audit queries |
| `lever_loop_task_run_id` | Step 4.2 output → tasks[0].run_id | Stdout retrieval via `databricks jobs get-run-output` |
| `optimization_run_id` | From the GSO_RUN_MANIFEST_V1 marker in stdout | Cross-reference with MLflow tags |
| `mlflow_experiment_id` | From the manifest marker | MLflow UI navigation |
| `parent_bundle_run_id` | From the GSO_ARTIFACT_INDEX_V1 marker | Bundle artifact discovery |

Add all six identifiers to the validation matrix doc (Task 5).

---

## Validation matrix

### Task 5: Walk the 9-row validation matrix

**Files:**
- Create: `packages/genie-space-optimizer/docs/2026-05-04-phase-e-pilot-run-validation-matrix.md`

This task is the operator-driven validation pass. Each row must be GREEN before the merge.

- [ ] **Step 5.1: Create the validation matrix doc with the template**

```markdown
# Phase E Pilot Run Validation Matrix

**Run identifiers:**

- databricks_job_id: <from Task 4.3>
- databricks_parent_run_id: <from Task 4.3>
- lever_loop_task_run_id: <from Task 4.3>
- optimization_run_id: <from Task 4.3>
- mlflow_experiment_id: <from Task 4.3>
- parent_bundle_run_id: <from Task 4.3>

**Iteration count:** <from final stdout marker>
**Final accuracy:** <from scoreboard>

| # | Validation | Method | Expected | Actual | Status |
|---|---|---|---|---|---|
| 1 | Zero validator warnings across all iterations | `mlflow_audit` query | `iter_violation_counts` is all zeros | | |
| 2 | Decision trace complete for every iteration | `mlflow_audit` query | `iter_record_counts` ≥ minimum threshold for every iter | | |
| 3 | Operator transcript renders end-to-end | `databricks fs cp` from MLflow artifact | `gso_postmortem_bundle/operator_transcript.md` non-empty + parses as markdown | | |
| 4 | Failed iteration diagnosable from transcript alone | Pick one rolled-back iteration; read its `iter_NN/operator_transcript.md` | RCA card + AG decision + gate reasons + acceptance reason all present | | |
| 5 | Scoreboard renders with sensible numbers | Read `gso_postmortem_bundle/scoreboard.json` | accuracy_delta_pp matches stdout, no NaN, no negative iteration counts | | |
| 6 | Bucketing labels look right (spot-check 3-5 unresolved qids) | Read `gso_postmortem_bundle/failure_buckets.json` + verify each spot-check qid against eval row | label matches the qid's actual symptom | | |
| 7 | RCA loop state present for every unresolved qid | Cross-reference `failure_buckets.json` with each iteration's `iter_NN/rca_ledger.json` | every unresolved qid appears with non-empty rca_kind | | |
| 8 | Markers identify all run roles | Run `tools/marker_parser.py < <stdout-file>` | GSO_RUN_MANIFEST_V1 + GSO_ITERATION_SUMMARY_V1 (per iter) + GSO_ARTIFACT_INDEX_V1 + GSO_CONVERGENCE_V1 all present | | |
| 9 | No accuracy regression vs Phase A baseline | Compare `gso_postmortem_bundle/run_summary.json:final_accuracy` against the variance baseline captured during Phase A burn-down (recorded at `docs/2026-05-02-phase-a-baseline.md` if present) | within ±2pp of baseline | | |
```

- [ ] **Step 5.2: Run validation #1 — zero validator warnings**

```bash
cd packages/genie-space-optimizer
python -m genie_space_optimizer.tools.mlflow_audit \
  --profile <pilot-profile> \
  --experiment-id <mlflow_experiment_id> \
  --optimization-run-id <optimization_run_id> \
  --output json | jq '{iter_violation_counts}'
```

Expected: `{"iter_violation_counts": [0, 0, 0, 0, ...]}` — all zeros.

If any non-zero value, find the offending iteration via:

```bash
python -m genie_space_optimizer.tools.mlflow_audit \
  --profile <pilot-profile> \
  --optimization-run-id <optimization_run_id> \
  --iteration <N> \
  --output text
```

If this is a journey-validator warning, the fix is upstream (in Phase A-D); halt and do NOT proceed to Task 6 (the `raise_on_violation=True` flip would mask the warning by raising mid-loop).

- [ ] **Step 5.3: Run validation #2 — decision trace completeness**

Same audit tool, but query record counts:

```bash
python -m genie_space_optimizer.tools.mlflow_audit \
  --profile <pilot-profile> \
  --experiment-id <mlflow_experiment_id> \
  --optimization-run-id <optimization_run_id> \
  --output json | jq '{iter_record_counts}'
```

Expected: every iteration emitted at least:

- 1 EVAL_CLASSIFICATION record per hard-failure qid.
- 1 RCA_FORMED record per RCA cluster.
- 1 STRATEGIST_AG_EMITTED record per AG.
- 1 PROPOSAL_GENERATED record per proposal.
- 1 PATCH_APPLIED record per applied patch.
- 1 ACCEPTANCE_DECIDED record per AG (post-eval).
- 1 QID_RESOLUTION record per eval qid.

Minimum threshold: an iteration with one cluster, one AG, one proposal, one patch ≈ 7+ records. Fewer means a producer regressed.

If any iteration's record count is below threshold, run the gso-postmortem skill on that iteration to diagnose.

- [ ] **Step 5.4: Run validations #3-#9 against the bundle artifacts**

For each, fetch the bundle artifact via `databricks fs cp dbfs:/databricks/mlflow-tracking/<experiment-id>/<run-id>/artifacts/<path> ./pilot_e_artifacts/<path>` (or via `MlflowClient().download_artifacts(...)`).

Then validate per the matrix's "Method" column. Record actual values + status (PASS / FAIL / NEEDS-FIX).

- [ ] **Step 5.5: Commit the populated validation matrix**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/docs/2026-05-04-phase-e-pilot-run-validation-matrix.md
git commit -m "docs(phase-e): pilot run validation matrix (run <opt-run-id>)"
```

- [ ] **Step 5.6: Decide PASS / FAIL on the validation matrix**

If all 9 rows are GREEN, proceed to Task 6.

If any row is RED, halt and remediate. Common remediation paths:

| Failed Row | Remediation |
|---|---|
| 1 (validator warnings) | Burn down warnings (return to Phase D) before re-pilot |
| 2 (decision trace gaps) | Check Phase F+H atomic dedup — likely a producer was deleted without the stage call wiring (or vice versa) |
| 3 (transcript empty) | C18 bundle assembly failed — check parent run's stderr for `Phase H bundle assembly failed` warnings |
| 4 (transcript not diagnosable) | Phase H Task 9-11 transcript renderers are missing fields — patch the renderer |
| 5 (scoreboard NaN) | Aggregator math regressed — check `run_output_bundle.build_run_summary` |
| 6 (bucketing wrong) | Bucketing classifier from PR-D regressed — check `failure_bucketing.py` |
| 7 (RCA missing) | RCA evidence path regressed — Phase F2 follow-up plan |
| 8 (markers missing) | Marker emission site regressed — check stdout for missing markers |
| 9 (accuracy regression) | Major — likely an algorithm regression. Run the gso-postmortem skill on the entire run + bisect against the variance baseline |

Re-run the pilot only after remediation lands and Tasks 0-3 pass green again.

---

## Hard-gate flips

### Task 6: Flip `raise_on_violation=True`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:17760`
- Create: `packages/genie-space-optimizer/tests/unit/test_journey_validator_hard_gate.py`

- [ ] **Step 6.1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_journey_validator_hard_gate.py
"""Phase E Task 6: pin raise_on_violation=True at the harness call site.

Verifies the journey-contract validator is wired as a HARD gate. Any
contract violation in production raises ContractViolationError instead
of warning; the lever loop terminates fast.
"""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_iteration_end_validator_call_site_uses_hard_gate() -> None:
    """The single _validate_journeys_at_iteration_end call site in
    harness.py must pass raise_on_violation=True (Phase E Task 6).
    """
    src = inspect.getsource(harness)
    # Allow only the hard-gate form. The soft-gate form was the Phase 2
    # state and is forbidden post-Phase-E.
    assert "raise_on_violation=False" not in src, (
        "Phase E Task 6 regression: raise_on_violation=False found in "
        "harness.py. The journey contract must be a hard gate post-merge."
    )
    assert "raise_on_violation=True" in src, (
        "Phase E Task 6 regression: raise_on_violation=True missing "
        "from harness.py. The journey contract must be a hard gate."
    )
```

- [ ] **Step 6.2: Run the test, expect it to FAIL**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_journey_validator_hard_gate.py -v
```

Expected: FAIL with `assert "raise_on_violation=False" not in src` (currently the soft-gate form lives at `:17760`).

- [ ] **Step 6.3: Flip the call site**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, locate `:17760`:

```python
# BEFORE
_journey_report = _validate_journeys_at_iteration_end(
    events=_journey_events,
    eval_qids=_eval_qids_for_validator,
    iteration=iteration_counter,
    raise_on_violation=False,
)
```

Change to:

```python
# AFTER (Phase E Task 6: hard gate)
_journey_report = _validate_journeys_at_iteration_end(
    events=_journey_events,
    eval_qids=_eval_qids_for_validator,
    iteration=iteration_counter,
    raise_on_violation=True,
)
```

Do NOT remove the surrounding `try / except Exception` block — it's there to make the validator-side bug never break the loop, and the hard gate's `ContractViolationError` is intentionally caught inside it. Re-read `harness.py:17745-17770` to confirm the wrap is still appropriate. The validator must raise on contract violation; the harness catches it and ends the loop with a `ContractViolationError` propagating to the job's exit status.

> **Important:** if the surrounding try/except catches `ContractViolationError` and swallows it, the hard gate is defeated. Verify the except clause re-raises `ContractViolationError` (or narrows the catch to specifically allow validator-internal bugs while propagating contract errors). If it doesn't, this commit must also widen the test in Step 6.1 to assert the re-raise path.

- [ ] **Step 6.4: Run the test, expect it to PASS**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_journey_validator_hard_gate.py -v
```

Expected: PASS.

- [ ] **Step 6.5: Run the full test suite to confirm no regressions**

```bash
cd packages/genie-space-optimizer
pytest -q
```

Expected: ALL pass. If any test fails because it relied on `raise_on_violation=False`, that test was incorrect post-Phase-E and must be updated to the hard-gate form OR converted to an explicit warn-only test of the validator itself (separate from the harness call site).

- [ ] **Step 6.6: Commit**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_journey_validator_hard_gate.py
git commit -m "feat(phase-e): flip journey contract to hard gate (raise_on_violation=True)"
```

### Task 7: Add the decision-trace replay-side hard gate

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py`
- Create: `packages/genie-space-optimizer/tests/replay/test_decision_trace_hard_gate.py`

The replay-side gate fails closed when a fixture is missing required decision records. Without it, a regression that drops a producer would silently pass replay tests as long as byte-stability holds (because byte-stability tests *the captured trace*, not its completeness against a contract).

- [ ] **Step 7.1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/replay/test_decision_trace_hard_gate.py
"""Phase E Task 7: replay-side fail-closed on missing required decision records.

The decision-trace contract (Phase A burn-down) requires every iteration
to emit at least one record per: EVAL_CLASSIFICATION, STRATEGIST_AG_EMITTED,
PROPOSAL_GENERATED, ACCEPTANCE_DECIDED, QID_RESOLUTION. (Plus the
type-conditional ones: PATCH_APPLIED when at least one patch applies,
RCA_FORMED when at least one cluster forms.)

The replay-side gate enforces this on every fixture replay so a regressed
producer fails CI immediately.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization import lever_loop_replay


def _load_airline_fixture() -> dict:
    fixture_path = (
        Path(__file__).parent / "fixtures" / "airline_real_v1.json"
    )
    with fixture_path.open() as f:
        return json.load(f)


def test_replay_fails_closed_on_missing_required_decision_record() -> None:
    """Stripping every PROPOSAL_GENERATED record from the airline fixture
    must cause the replay to fail closed."""
    fixture = _load_airline_fixture()

    for iteration in fixture.get("iterations", []):
        records = iteration.get("decision_records", [])
        iteration["decision_records"] = [
            r for r in records if r.get("decision_type") != "PROPOSAL_GENERATED"
        ]

    with pytest.raises(lever_loop_replay.ContractViolationError) as exc_info:
        lever_loop_replay.run_replay(fixture)

    assert "PROPOSAL_GENERATED" in str(exc_info.value), (
        "Replay-side hard gate must name the missing decision type"
    )


def test_replay_passes_when_required_decision_records_present() -> None:
    """The unmodified airline fixture must pass."""
    fixture = _load_airline_fixture()
    result = lever_loop_replay.run_replay(fixture)
    assert result.validation.is_valid
```

- [ ] **Step 7.2: Run the test, expect it to FAIL**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_decision_trace_hard_gate.py -v
```

Expected:

- `test_replay_passes_when_required_decision_records_present` PASSES (unchanged fixture works).
- `test_replay_fails_closed_on_missing_required_decision_record` FAILS — the replay does NOT raise on the stripped fixture (today's behavior is to warn or silently continue).

- [ ] **Step 7.3: Implement the hard gate**

Open `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py` and locate the iteration-validation loop.

Add a `ContractViolationError` exception (if not present) and the required-records check. The minimum required types per iteration:

```python
# packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py

# Add near module-scope imports if not already present:
class ContractViolationError(Exception):
    """Raised when a fixture replay finds a missing required decision
    record (Phase E Task 7 hard gate)."""


# Required decision types per iteration. PATCH_APPLIED and RCA_FORMED are
# conditionally required (only when at least one patch / cluster exists);
# the rest are unconditional.
_REQUIRED_DECISION_TYPES_PER_ITERATION: tuple[str, ...] = (
    "EVAL_CLASSIFICATION",
    "STRATEGIST_AG_EMITTED",
    "PROPOSAL_GENERATED",
    "ACCEPTANCE_DECIDED",
    "QID_RESOLUTION",
)


def _verify_required_decisions(
    *, iteration_index: int, decision_records: list[dict],
) -> None:
    """Phase E Task 7: fail closed when an iteration's decision_records
    are missing any required decision type."""
    types_present = {
        rec.get("decision_type") for rec in decision_records
    }
    missing = [
        t for t in _REQUIRED_DECISION_TYPES_PER_ITERATION
        if t not in types_present
    ]
    if missing:
        raise ContractViolationError(
            f"Iteration {iteration_index}: missing required decision "
            f"records: {missing}. Replay fails closed (Phase E Task 7)."
        )
```

Then call `_verify_required_decisions(...)` from inside `run_replay(...)`'s per-iteration loop. The exact insertion point is wherever `run_replay` already iterates `fixture["iterations"]` — locate that loop and add the call once per iteration.

- [ ] **Step 7.4: Run the test, expect it to PASS**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_decision_trace_hard_gate.py -v
```

Expected: BOTH tests PASS.

- [ ] **Step 7.5: Run the full replay suite**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/ -q
```

Expected: ALL pass. If `test_phase_f_h_wireup_byte_stable.py` fails because the airline fixture is missing a required record, the regressor is upstream of Phase E — diagnose with the gso-postmortem skill before merge.

- [ ] **Step 7.6: Commit**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py \
        packages/genie-space-optimizer/tests/replay/test_decision_trace_hard_gate.py
git commit -m "feat(phase-e): replay-side fail-closed on missing required decision records"
```

---

## Sanity PR procedure

### Task 8: Open the deliberately-broken sanity PR

**Files:**
- Modify (throwaway branch): `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/decision_emitters.py` (single-line change to break a producer)
- Create: `packages/genie-space-optimizer/docs/2026-05-04-phase-e-sanity-pr-procedure.md`

This task proves the gates are wired correctly and CI catches regressions. The PR is opened, CI fails closed, the PR is closed (NOT merged).

- [ ] **Step 8.1: Create the procedure doc**

```markdown
# Phase E Sanity PR Procedure

**Purpose:** prove the journey contract hard gate (Task 6) and the
decision-trace hard gate (Task 7) catch regressions in CI.

**Procedure:**

1. From the merge candidate HEAD, create a throwaway branch:
   ```
   git checkout -b sanity-pr-e-2026-05-04 fix/gso-lossless-contract-replay-gate
   ```

2. Pick ONE producer to break. Recommended: a single-line skip in
   `_emit_ag_outcome_journey` at `decision_emitters.py:<line>` that
   conditionally returns early for the second AG of any iteration.
   The break is small enough to revert with one keystroke and large
   enough that CI catches it within minutes.

3. Run the unit suite locally — confirm at least one test fails:
   ```
   pytest tests/unit/test_journey_emit*.py -q
   ```

4. Push the branch:
   ```
   git push origin sanity-pr-e-2026-05-04
   ```

5. Open a PR against `fix/gso-lossless-contract-replay-gate` with
   subject prefix "[SANITY-DO-NOT-MERGE]".

6. Watch CI. Expected timeline:
   - Linters pass (~2 min).
   - Unit tests fail at the journey-emit test (~5 min).
   - Replay tests fail at the decision-trace hard gate (~10 min).

7. Capture the failing CI logs in the procedure doc.

8. **Close the PR. Do NOT merge.**

9. Delete the throwaway branch:
   ```
   git push origin --delete sanity-pr-e-2026-05-04
   git branch -D sanity-pr-e-2026-05-04
   ```

**Acceptance:** CI fails closed within 15 minutes with at least one
test failure naming the broken producer. If CI passes (the gate is
NOT catching the regression), HALT — do NOT merge.
```

- [ ] **Step 8.2: Execute the procedure**

Follow steps 1-9 above. Capture timestamps and CI run URLs in the procedure doc.

- [ ] **Step 8.3: Commit the populated procedure doc**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/docs/2026-05-04-phase-e-sanity-pr-procedure.md
git commit -m "docs(phase-e): sanity PR execution log (CI fails closed as expected)"
```

---

## Merge gate flip

### Task 9: Final merge

**Files:** none. PR-driven.

- [ ] **Step 9.1: Open the merge PR**

Push the merge candidate branch and open a PR:

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git push origin fix/gso-lossless-contract-replay-gate

gh pr create \
  --base main \
  --head fix/gso-lossless-contract-replay-gate \
  --title "GSO: lossless contract + replay gate + stage-aligned modularization (Phases A-E + F + H)" \
  --body "$(cat <<EOF
## Summary

This PR ships the GSO Lever Loop's lossless contract — every optimizer
decision is captured as a typed DecisionRecord, every iteration is
journey-validated, every stage exposes a typed I/O surface, and every
real run produces a structured \\\`gso_postmortem_bundle\\\`.

Includes Phases A-D (decision contract + replay byte-stability), Phase
E.0 (MLflow artifact integrity), PR-A through PR-E (pre-rerun gap
closures), Phase F (stage-aligned modularization, both modules and
harness wire-up), Phase G-lite (StageHandler Protocol + STAGES registry
+ RunEvaluationKwargs), Phase H (per-stage I/O capture + bundle
assembly + GSO_ARTIFACT_INDEX_V1), Phase E (hard-gate flip + sanity
PR + merge).

## Test plan

- [x] Pilot run validation matrix complete (see docs/2026-05-04-phase-e-pilot-run-validation-matrix.md)
- [x] raise_on_violation=True flipped (Task 6)
- [x] Decision-trace replay-side hard gate added (Task 7)
- [x] Sanity PR opened, CI failed closed within 15 min, PR closed without merge (Task 8)
- [x] Full unit + replay + integration suites pass on merge candidate
- [x] No accuracy regression vs Phase A baseline

## Validation artifacts

- Pilot run optimization_run_id: <fill in>
- Pilot run parent_bundle_run_id: <fill in>
- Sanity PR CI run URL: <fill in>
EOF
)"
```

- [ ] **Step 9.2: Address review feedback**

If reviewers ask for changes, land each as a separate commit on the merge candidate branch (do not force-push to rewrite history). Re-run the validation matrix's Task 5 abbreviated form (rows 1, 2, 8 — the cheap ones) after every reviewer-requested code change to confirm no regression.

- [ ] **Step 9.3: Merge**

When all reviewers approve and CI is green, merge with **squash + merge** (the team's standard for feature branches).

```bash
gh pr merge <pr-number> --squash --delete-branch
```

- [ ] **Step 9.4: Confirm post-merge state**

```bash
git fetch origin main
git log --oneline main -5
```

Expected: top commit is the squashed merge commit. The branch `fix/gso-lossless-contract-replay-gate` is deleted on origin.

- [ ] **Step 9.5: Tag the release**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git checkout main
git pull origin main
git tag -a gso-lossless-contract-v1 -m "GSO Lossless Contract + Replay Gate (Phases A-E + F + H)"
git push origin gso-lossless-contract-v1
```

- [ ] **Step 9.6: Update the roadmap**

Apply the roadmap update text from Track 1C (separate plan: `2026-05-04-phase-f-h-roadmap-update-draft.md`) to `2026-05-01-burn-down-to-merge-roadmap.md`. Mark Phase E as ✅, Phase F status flipped to ✅ (full wire-up), Phase H Option 1 → ✅, the F+H wire-up line item → ✅, E.0 → ✅.

```bash
git add packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md
git commit -m "docs(roadmap): mark Phase E + F + H + F+H wire-up + E.0 complete"
git push origin main
```

---

# Self-Review Checklist

After completing all tasks, verify:

- [ ] Task 0-3 (pre-flight) passes 100% green before scheduling Task 4.
- [ ] Task 4 (pilot run) completed with `result_state == "SUCCESS"`.
- [ ] Task 5 (validation matrix) all 9 rows GREEN.
- [ ] Task 6 (`raise_on_violation=True` flip) test passes; full suite green.
- [ ] Task 7 (replay hard gate) BOTH tests pass; full replay suite green.
- [ ] Task 8 (sanity PR) CI failed closed; PR closed without merge.
- [ ] Task 9 (merge) PR merged via squash; tag pushed; roadmap updated.

If any item is RED, do NOT proceed past it. Halt and remediate, or escalate.

---

# Dependencies + Out-of-Scope

**Hard dependencies (must land before Task 0):**

1. F+H harness wire-up plan ([`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md)) Phase A + Phase B + Phase C all complete.
2. Audit doc Section 4-6 action items ([`2026-05-04-phase-f-h-wireup-audit-findings.md`](./2026-05-04-phase-f-h-wireup-audit-findings.md)) all resolved.
3. Phase E.0 (MLflow artifact integrity) ✅ already shipped per the roadmap E.0 ✅ annotation.

**Out of scope:**

1. F2 follow-up (rca_evidence sparse-bundle) — runs as a separate post-merge plan ([`2026-05-05-phase-f2-rca-evidence-followup-plan.md`](./2026-05-05-phase-f2-rca-evidence-followup-plan.md)).
2. F6 follow-up (gates order reconciliation) — runs as a separate post-merge plan ([`2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md`](./2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md)).
3. F1 capture decorator wrap — explicit out-of-scope per the F+H plan §B prologue note (line 754). Tracked separately if/when needed.
4. Production-mode UC ASI enrichment parity for F3 (`spark` parameter caveat) — pinned in F3 wire-up commit message; not gating Phase E.
