# Cycle 9 Burndown — Blast-Radius Recovery, Decision-Trace Capture, and CLI-Visible Manifest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **⚠️ Status update — 2026-05-03 (post Phase-B-observability-followup landing):**
>
> Several tasks in this plan were authored before the **Phase B observability follow-up** landed (4 commits, branch `fix/gso-lossless-contract-replay-gate`):
>
> | Commit | Subject |
> |---|---|
> | `68ce5eb` | Phase B observability foundations (Tasks 1-4) |
> | `346ebd6` | wire 5 DecisionRecord producers + Phase B manifest |
> | `bd0c93e` | postmortem follow-up section + manifest in canonical schema |
>
> See `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md` "Postmortem Follow-up" section for the manifest schema and producer module layout. The relevant deltas vs this plan as written:
>
> 1. **All `DecisionRecord` producers now live in `optimization/decision_emitters.py`**, not `rca_decision_trace.py`. New producers (T6 blast-radius, T7 dead-on-arrival) should be added there to keep the producer module cohesive.
> 2. **`DecisionRecord` no longer has a `details` field.** It carries the RCA-grounding contract: `evidence_refs`, `rca_id`, `root_cause`, `target_qids`, `expected_effect`, `observed_effect`, `regression_qids`, `next_action` (plus `metrics` for free-form key/value pairs). New producers must populate these instead of `details`.
> 3. **`POST_EVAL_HOLD_PASS` is rca-exempt** in `validate_decisions_against_journey`. New producers calling existing reason codes should respect the exemption logic.
> 4. **`phase_b_no_records_marker(...)` already ships** in `run_analysis_contract.py` and is wired in the harness Phase B persistence block. T10 below is **superseded** — the marker exists; T10's wiring is already done. Keep T10's `producers_silent` reason hook as a **doc-only** clarification (the harness uses `NoRecordsReason` enum from `decision_emitters.py`).
> 5. **`loop_out["phase_b"]` manifest is already populated** at lever-loop return and survives `run_lever_loop.py:548-563` debug_info filter. T12's notebook-exit manifest builders should **wrap and extend** the existing `phase_b` key, not replace it. New T12 fields (per_iteration_decision_counts, etc.) overlap with `phase_b.iter_record_counts`, etc — reconcile.
> 6. **`PHASE_B_CONTRACT_VERSION = "v1"` MLflow tag** is set at lever-loop start. Bump to `"v2"` if T6/T7 land changes that break the contract.
> 7. **5 producers wired**: EVAL_CLASSIFIED, CLUSTER_SELECTED, STRATEGIST_AG_EMITTED, ACCEPTANCE_DECIDED (5 outcome paths including `skipped_dead_on_arrival`, `skipped_no_applied_patches`, `skipped_pre_ag_snapshot_failed`), QID_RESOLUTION. **Blast-radius dropped patches still produce no record** (T6 is genuinely needed). The dead-on-arrival path **does** produce `ACCEPTANCE_DECIDED(SKIPPED)` records via the closure `_phase_b_emit_ag_outcome_record`; T7's added value is the more specific `PATCH_SKIPPED` granularity per dropped patch (one record per signature, not one per AG).
>
> Per-task status updates are inline below (see "📋 Status").

**Goal:** Make the lever loop survive a blast-radius dead-end without burning all five iterations on the same dropped AG, populate the Phase B decision trace + journey-validation contract on every iteration (so `decision_records` stops being `[]` and `journey_validation` stops being `null` in the replay fixture), and surface that contract in `dbutils.notebook.exit(...)` so CLI-only operators can triage a run from `databricks jobs get-run-output` alone.

**Architecture:**
1. **Loop liveness fix** — convert three unconditional `pending_action_groups = []` clears in `harness.py` (dead-on-arrival, pre-AG-snapshot-failure, deterministic-no-applied-patches) into selective drains that only discard the *failed* AG, not unrelated buffered AGs that target other clusters.
2. **Phase B trace contract completion** — emit typed `DecisionRecord`s on the blast-radius drop, dead-on-arrival, pre-AG-snapshot-failure, and applier-rejection paths so `_current_iter_inputs["decision_records"]` is non-empty for every iteration that touched a gate; tag iterations that *legitimately* have zero decisions so the operator knows the difference between "captured nothing" and "nothing to capture".
3. **Operator surface** — wire `compute_scoreboard()` into the end-of-iteration banner; carry decision counts, journey violation counts, and Phase B artifact paths into the lever-loop and finalize notebook exit JSON so `databricks jobs get-run-output` reveals the same numbers MLflow has.
4. **Catalog & predicate strengthening** — extend `SEED_CATALOG` with the four new patterns this run revealed; add a `proposal_direction_contradicts_counterfactual` predicate that fires when a strategist proposal's `value` directly contradicts the dominant cluster's `counterfactual_fix`.
5. **Replay intake** — extract this run's `===PHASE_A_REPLAY_FIXTURE_JSON===` block as `airline_real_v1_cycle9_raw.json` and gate fixture promotion on `journey_validation` violations remaining at zero.

**Tech Stack:** Python 3.12, pytest, MLflow tracing, Databricks Asset Bundles, Pandas. No new runtime dependencies.

---

## Background

This plan is the burndown response to run `1e855111-b463-4556-9b30-8cd32f78ebcb` (5 iterations, +0.0% net improvement, 0 AGs accepted). Three Phase A tracks shipped before this run — Track I (eval-row trace-id persistence), Track 5 (`sql_shape_quality.py`), Track 6 (`scoreboard.py`), Track 7 (`failure_buckets.py`), as documented in `2026-05-01-phase-a-provenance-and-readiness-plan.md`. They all live in the codebase and are unit-tested. None of them prevented this run from collapsing because the loop never reached the surfaces they protect:

| Symptom (from log) | Why the existing tracks didn't help | Track in this plan |
|---|---|---|
| Iter 1 burned `AG_DECOMPOSED_H001`; H002 / H003 buffered then **silently discarded** by `pending_action_groups = []` (`harness.py:14727`, `:14782`, `:14907`). Iters 2-5 re-ran the same dead AG against the same `()` patch signature. | Track 6 (`scoreboard`) and Track D (plateau truthfulness) only fire when an AG completes a gate decision. The skip paths bypass the gate. | T1, T2, T3 |
| `BLAST-RADIUS GATE` dropped both proposals (`high_collateral_risk_flagged` because `gs_003` is a passing dependent on `tkt_payment`); strategist re-proposed *the same patch shape* in the next iteration. | No "forbid this table for this AG" feedback loop; reflection key keys on `patch_retry_signature`, not on the gate that dropped the patch. | T4, T5 |
| `decision_records: []` and `journey_validation: null` in the emitted fixture even though gate drops, blast-radius drops, and dead-on-arrival decisions all happened. Phase B operator-transcript persistence at `harness.py:16250-16306` only fires when there are records to render — but the only producer wired today is `patch_cap_decision_records` (`harness.py:14668`), which does not run for AGs whose patches are dropped *before* the cap. | Phase B decision-record contract is partially wired: producers exist for the patch-cap path; producers are missing for blast-radius / dead-on-arrival / pre-AG-snapshot paths. | T6, T7 |
| `Track I: trace ID fallback recovered 24/24 rows; cumulative fallback rate = 50.0%` for both the baseline and post-eval scopes — the primary persistence path is still losing trace context every time. | Track I implemented the fallback; root cause of the *primary* loss is not in scope for Phase A. | (out of scope; tracked under Phase B) |
| `[UNBOUND_SQL_PARAMETER] Found the unbound parameter: ticket_number` (and `pnr_locator`, `carrier_code`) on arbiter execution. The benchmark SQL contains `:ticket_number` placeholders without values. | Pre-existing benchmark-author hygiene issue (`GT SQL contains parameterized placeholders … skipping EXPLAIN validation` at preflight). Mentioned for completeness; **not in scope for this plan** — tracked under preflight benchmark hygiene. | (out of scope) |
| `compute_scoreboard()` exists in `scoreboard.py` and is unit-tested but is **not called from `harness.py`** — operators get no end-of-iteration `dominant_signal`. | Track 6 implemented the math; the wire-up was deferred. | T8 |
| `failure_buckets.SEED_CATALOG` has 16 entries; none match `dead_on_arrival_blocks_buffered_drain`, `blast_radius_no_escape_hatch`, `proposal_direction_inversion`, or `union_all_grain_split` (all observed this run). | Track 7 was scoped to the May-01 ESR / 7Now / 23:04 patterns. | T9 |

---

## File Structure

| File | Status | Responsibility | Owning task(s) |
|---|---|---|---|
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` | Modify | Lever loop orchestration. Selective buffered-AG drain, blast-radius DecisionRecord emission, scoreboard wire-up. **Note:** the no-decision diagnostic is already wired (postmortem follow-up); T10 below is doc-only / superseded. | T1, T2, T3, T4, T6, T8 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/decision_emitters.py` | Modify | **Producer module home (per postmortem follow-up).** Add `blast_radius_decision_records` and `dead_on_arrival_decision_records` here, NOT in `rca_decision_trace.py`. New producers must populate the RCA-grounding contract fields (no `details` field). | T6, T7 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py` | (no change) | Type module (`DecisionType`, `DecisionOutcome`, `ReasonCode`, `DecisionRecord`) and cross-checker (`validate_decisions_against_journey`). New producers consume these types but live in `decision_emitters.py`. | — |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/strategist_constraints.py` | Create | Container for cross-iteration AG constraints (e.g., `forbid_tables`). | T5 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_quality.py` | Modify | Add `proposal_direction_contradicts_counterfactual` predicate. | T11 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py` | Modify | Wire the new predicate into the snippet-quality demotion. | T11 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/failure_buckets.py` | Modify | Append four new `SEED_CATALOG` entries. | T9 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py` | Modify | Add `lever_loop_exit_manifest` and `finalize_exit_manifest` JSON shapes. | T12 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py` | Modify | Replace plain `dbutils.notebook.exit(json.dumps(debug_info))` with `lever_loop_exit_manifest(...)`. | T12 |
| `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_finalize.py` | Modify | Replace plain `dbutils.notebook.exit(...)` with `finalize_exit_manifest(...)`. | T12 |
| `packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py` | Create | TDD test for selective drain on three skip paths. | T1, T2, T3 |
| `packages/genie-space-optimizer/tests/unit/test_blast_radius_decision_records.py` | Create | TDD test for blast-radius DecisionRecord emission. | T6 |
| `packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_decision_records.py` | Create | TDD test for dead-on-arrival DecisionRecord emission. | T7 |
| `packages/genie-space-optimizer/tests/unit/test_strategist_forbid_tables.py` | Create | TDD test for `forbid_tables` constraint propagation. | T5 |
| `packages/genie-space-optimizer/tests/unit/test_scoreboard_harness_wiring.py` | Create | TDD test for end-of-iteration scoreboard banner. | T8 |
| `packages/genie-space-optimizer/tests/unit/test_failure_buckets_cycle9.py` | Create | TDD test for new `SEED_CATALOG` entries. | T9 |
| `packages/genie-space-optimizer/tests/unit/test_no_decision_records_marker.py` | Create | TDD test for the no-decisions diagnostic tag. | T10 |
| `packages/genie-space-optimizer/tests/unit/test_proposal_direction_contradicts_counterfactual.py` | Create | TDD test for the new predicate. | T11 |
| `packages/genie-space-optimizer/tests/unit/test_lever_loop_exit_manifest.py` | Create | TDD test for the notebook-exit manifest shape. | T12 |
| `packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle9_raw.json` | Create | Cycle 9 replay intake fixture (extracted from this run's stderr). | T13 |
| `packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py` | Create | Snapshot test: cycle 9 fixture has zero `journey_validation` violations after replay. | T13 |

---

## Cross-Task Conventions

* **Run pytest from the package root:** `cd packages/genie-space-optimizer && pytest -xvs <path>`.
* **Imports in tests follow the codebase convention:** `from genie_space_optimizer.optimization.harness import …`.
* **Decision-record dataclass conventions** are defined in `rca_decision_trace.py:32-77` (`DecisionType`, `DecisionOutcome`, `ReasonCode` enums + `DecisionRecord.from_dict` / `to_dict`).
* **Markers contract** is in `run_analysis_contract.py:60-110` (`iteration_summary_marker`, `phase_b_marker`).
* **Iteration snapshot contract** is `journey_fixture_exporter.py:198-210` (allowed keys: `eval_rows`, `clusters`, `soft_clusters`, `strategist_response`, `ag_outcomes`, `post_eval_passing_qids`, `journey_validation`, `decision_records`).

---

## Task 1: Selective drain — dead-on-arrival path (`harness.py:14727`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14702-14729`
- Create: `packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py`

The dead-on-arrival path fires when `_selected_patch_signature in _dead_on_arrival_patch_signatures` (`harness.py:14702`). Today it unconditionally clears `pending_action_groups = []` (line 14727), which discards every buffered AG even though they target unrelated clusters. The selective drain keeps buffered AGs whose `affected_questions` set is **disjoint** from the failed AG's `affected_questions`.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py`:

```python
"""TDD coverage for selective buffered-AG drain (T1, T2, T3).

The three skip paths in harness.py at 14727, 14782, and 14907 used to
unconditionally clear ``pending_action_groups`` when the *current* AG
failed. This regression test pins the new contract: buffered AGs whose
``affected_questions`` are disjoint from the failed AG survive.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.harness import (
    _drain_buffered_action_groups,
)


def _ag(ag_id: str, qids: list[str]) -> dict:
    return {"id": ag_id, "affected_questions": list(qids)}


def test_dead_on_arrival_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_009"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    assert [a["id"] for a in survivors] == [
        "AG_DECOMPOSED_H002",
        "AG_DECOMPOSED_H003",
    ]
    assert dropped == []


def test_dead_on_arrival_drops_overlapping_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024", "gs_025"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_025"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H003"]
    assert [a["id"] for a in dropped] == ["AG_DECOMPOSED_H002"]


def test_drain_handles_failed_ag_with_no_affected_questions():
    failed = _ag("AG_BROAD", [])
    buffered = [_ag("AG_DECOMPOSED_H002", ["gs_009"])]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    # Empty failed-AG qids → conservatively drop overlapping; here disjoint, keep.
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H002"]
    assert dropped == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py::test_dead_on_arrival_keeps_disjoint_buffered_ags`

Expected: FAIL with `ImportError: cannot import name '_drain_buffered_action_groups' from 'genie_space_optimizer.optimization.harness'`.

- [ ] **Step 3: Add the helper next to existing harness helpers**

Insert the following helper into `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` immediately after the `_dead_on_arrival_patch_signatures` declaration on line 10982-10983 (top-level module function, not nested):

```python
def _drain_buffered_action_groups(
    *,
    failed_ag: dict,
    buffered: list[dict],
    reason: str,
) -> tuple[list[dict], list[dict]]:
    """Selectively drop buffered AGs that share affected_questions
    with the failed AG.

    Unrelated buffered AGs survive: they target other clusters and the
    failure of one AG (blast-radius dead-end, dead-on-arrival, pre-AG
    snapshot failure) tells us nothing about their proposals.

    Returns (survivors, dropped) so the caller can log / emit a
    DecisionRecord for the dropped subset.
    """
    failed_qids = {
        str(q)
        for q in (failed_ag.get("affected_questions") or [])
        if str(q)
    }
    survivors: list[dict] = []
    dropped: list[dict] = []
    for ag in buffered or []:
        ag_qids = {
            str(q)
            for q in (ag.get("affected_questions") or [])
            if str(q)
        }
        if failed_qids and ag_qids & failed_qids:
            dropped.append(ag)
        else:
            survivors.append(ag)
    if dropped:
        logger.warning(
            "Selective drain (%s): dropped %d buffered AG(s) overlapping "
            "with %s; %d survived",
            reason,
            len(dropped),
            failed_ag.get("id", "?"),
            len(survivors),
        )
    return survivors, dropped
```

- [ ] **Step 4: Replace the unconditional clear at line 14727**

Find lines 14727-14728 in `harness.py`:

```python
            pending_action_groups = []
            pending_strategy = None
            continue
```

Replace with:

```python
            _survivors, _dropped_buffered = _drain_buffered_action_groups(
                failed_ag=ag,
                buffered=pending_action_groups,
                reason="dead_on_arrival",
            )
            pending_action_groups = _survivors
            if not pending_action_groups:
                pending_strategy = None
            continue
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py`

Expected: PASS for all three tests.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(harness): selective buffered-AG drain on dead-on-arrival path

Replaces the unconditional pending_action_groups = [] at harness.py:14727
with _drain_buffered_action_groups, which keeps buffered AGs whose
affected_questions are disjoint from the failed AG's. Cycle 9 burndown:
H002 and H003 stop being silently discarded when H001 dead-on-arrives."
```

---

## Task 2: Selective drain — pre-AG-snapshot-failure path (`harness.py:14782`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14773-14784`

The pre-AG snapshot-capture failure path uses the same unconditional clear. Reuse the helper from T1.

- [ ] **Step 1: Add a pre-snapshot test case to `test_buffered_ag_drain.py`**

Append this test to `packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py`:

```python
def test_pre_snapshot_failure_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [_ag("AG_DECOMPOSED_H002", ["gs_009"])]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="pre_ag_snapshot_failed",
    )
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H002"]
    assert dropped == []
```

- [ ] **Step 2: Verify it passes for the helper but the harness path is still unconditional**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py::test_pre_snapshot_failure_keeps_disjoint_buffered_ags`

Expected: PASS (helper already supports any reason string; this confirms the contract is consistent before we change the second call site).

- [ ] **Step 3: Replace the unconditional clear at line 14782**

Find lines 14782-14784 in `harness.py`:

```python
            pending_action_groups = []
            pending_strategy = None
            continue
```

(this block is preceded by `_section(f"[{ag_id}] SKIP APPLY: PRE-AG SNAPSHOT FAILED", "!")`).

Replace with:

```python
            _survivors, _dropped_buffered = _drain_buffered_action_groups(
                failed_ag=ag,
                buffered=pending_action_groups,
                reason="pre_ag_snapshot_failed",
            )
            pending_action_groups = _survivors
            if not pending_action_groups:
                pending_strategy = None
            continue
```

- [ ] **Step 4: Re-run the buffered-drain test suite**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py`

Expected: PASS for all four tests.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(harness): selective buffered-AG drain on pre-AG-snapshot-failure

Replaces the unconditional pending_action_groups = [] at harness.py:14782."
```

---

## Task 3: Selective drain — deterministic-no-applied-patches path (`harness.py:14907`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14890-14908`

This is the path GPT specifically called out: when the applier rejects every selected patch (`all_selected_patches_dropped_by_applier`), the loop stamps the AG signature into `_dead_on_arrival_patch_signatures` AND clears every buffered AG. Selective drain.

- [ ] **Step 1: Add an applier-failure test case to `test_buffered_ag_drain.py`**

Append:

```python
def test_applier_failure_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_009"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="all_selected_patches_dropped_by_applier",
    )
    assert [a["id"] for a in survivors] == [
        "AG_DECOMPOSED_H002",
        "AG_DECOMPOSED_H003",
    ]
    assert dropped == []
```

- [ ] **Step 2: Verify it passes for the helper**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py::test_applier_failure_keeps_disjoint_buffered_ags`

Expected: PASS.

- [ ] **Step 3: Replace the unconditional clear at line 14907**

Find lines 14907-14908 in `harness.py` (inside the `if _apply_skip.reason_code == "no_applied_patches":` branch):

```python
                pending_action_groups = []
                pending_strategy = None
```

Replace with:

```python
                _survivors, _dropped_buffered = _drain_buffered_action_groups(
                    failed_ag=ag,
                    buffered=pending_action_groups,
                    reason="all_selected_patches_dropped_by_applier",
                )
                pending_action_groups = _survivors
                if not pending_action_groups:
                    pending_strategy = None
```

- [ ] **Step 4: Re-run the buffered-drain test suite**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_buffered_ag_drain.py`

Expected: PASS for all five tests.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_buffered_ag_drain.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(harness): selective buffered-AG drain on applier-rejection path

Replaces the unconditional pending_action_groups = [] at harness.py:14907,
which fired in cycle 9 when AG_DECOMPOSED_H001's blast-radius drops left
the applier with zero patches and then discarded H002 and H003."
```

---

## Task 4: Distinguish blast-radius drops from applier rejection in the dead-on-arrival ledger

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14260-14283` (the post-blast-radius dropped-list emission)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14702-14728` (the dead-on-arrival check)

Today the dead-on-arrival ledger keys on `_selected_patch_signature` (a tuple of patch IDs). When blast-radius drops every patch, the signature is `()` — the *empty tuple*. Iters 2-5 of cycle 9 each computed signature `()` independently and immediately matched the cached `()` from iter 1, so the loop never even tried to ground a different proposal. The fix: never cache `()` as a "tried" signature, and emit the blast-radius drop with its own outcome label so the operator can distinguish it from `all_selected_patches_dropped_by_applier`.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_ledger.py`:

```python
"""TDD coverage for the dead-on-arrival ledger contract (T4).

Empty patch signatures (``()``) must never be cached as "already tried".
Otherwise a blast-radius drop in iter 1 short-circuits every subsequent
iteration before the strategist can change tack.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _record_dead_on_arrival_signature,
)


def test_empty_signature_is_not_recorded():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    assert seen == set()


def test_non_empty_signature_is_recorded():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=("P001#1", "P002#1"),
        reason="all_selected_patches_dropped_by_applier",
    )
    assert seen == {("P001#1", "P002#1")}


def test_blast_radius_dropped_does_not_block_future_attempts():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    # Future iter computes the same empty signature; should not match.
    assert () not in seen
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_dead_on_arrival_ledger.py`

Expected: FAIL with `ImportError: cannot import name '_record_dead_on_arrival_signature'`.

- [ ] **Step 3: Add the helper to `harness.py`**

Insert into `harness.py` immediately after `_drain_buffered_action_groups` (added in T1):

```python
def _record_dead_on_arrival_signature(
    *,
    seen: set[tuple[str, ...]],
    signature: tuple[str, ...],
    reason: str,
) -> None:
    """Record a dead-on-arrival patch signature, but only if it is
    informative.

    The empty tuple ``()`` is never recorded: it represents "every
    candidate patch was dropped before the applier saw it" (today only
    blast-radius gate causes this), and the right next step is to ask
    the strategist for a different shape, not to short-circuit.
    """
    if not signature:
        logger.info(
            "Dead-on-arrival ledger skipped empty signature (reason=%s); "
            "strategist will get another attempt.",
            reason,
        )
        return
    seen.add(signature)
```

- [ ] **Step 4: Replace the direct `.add()` at line 14893**

Find lines 14892-14893 in `harness.py` (inside `if _apply_skip.reason_code == "no_applied_patches":`):

```python
                _dead_on_arrival_ag_ids.add(str(ag_id))
                _dead_on_arrival_patch_signatures.add(_selected_patch_signature)
```

Replace with:

```python
                _dead_on_arrival_ag_ids.add(str(ag_id))
                _record_dead_on_arrival_signature(
                    seen=_dead_on_arrival_patch_signatures,
                    signature=_selected_patch_signature,
                    reason=_apply_skip.reason_code,
                )
```

- [ ] **Step 5: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_dead_on_arrival_ledger.py`

Expected: PASS for all three tests.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_ledger.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "fix(harness): never cache empty patch signature as dead-on-arrival

Cycle 9: H001's blast-radius drops produced signature=() in iter 1, then
matched (== ()) in every subsequent iter, blocking strategist retries
with new patch shapes."
```

---

## Task 5: Strategist `forbid_tables` constraint (cross-iteration learning)

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/strategist_constraints.py`
- Create: `packages/genie-space-optimizer/tests/unit/test_strategist_forbid_tables.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14260-14283` (record blast-radius tables into the constraint store)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (read the constraint into `metadata_snapshot["_strategist_constraints"]` before the next strategist call)

When blast-radius drops every patch because the touched table has passing dependents outside the AG's targets, the strategist needs to be told "for AG X, don't propose patches against table Y on the next iteration." This is structured constraint, not a freeform reflection note.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_strategist_forbid_tables.py`:

```python
"""TDD coverage for the strategist forbid-tables constraint (T5)."""
from __future__ import annotations

from genie_space_optimizer.optimization.strategist_constraints import (
    StrategistConstraints,
    record_blast_radius_drop,
)


def test_constraints_default_empty():
    c = StrategistConstraints()
    assert c.forbid_tables_for_ag("AG_X") == set()


def test_record_blast_radius_drop_adds_table():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_DECOMPOSED_H001",
        dropped_patches=[
            {"target": "ucat.dev.tkt_payment", "type": "add_sql_snippet_filter"},
            {"target": "ucat.dev.tkt_payment", "type": "add_sql_snippet_measure"},
        ],
    )
    assert c.forbid_tables_for_ag("AG_DECOMPOSED_H001") == {
        "ucat.dev.tkt_payment"
    }


def test_record_skips_patches_without_target():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"type": "add_instruction"}],
    )
    assert c.forbid_tables_for_ag("AG_X") == set()


def test_constraints_serialize_for_strategist_context():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"target": "ucat.dev.t1"}],
    )
    payload = c.to_strategist_context()
    assert payload == {
        "AG_X": {"forbid_tables": ["ucat.dev.t1"]},
    }
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_strategist_forbid_tables.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.strategist_constraints'`.

- [ ] **Step 3: Implement the module**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/strategist_constraints.py`:

```python
"""Cross-iteration AG constraints for the strategist (T5).

Today the lever loop forgets *why* a previous iteration's proposals
failed when it asks the strategist to try again. This module keeps a
small ledger of structured constraints — initially just
``forbid_tables`` per AG-id — that gets serialized into the strategist
prompt context on the next call.

Phase B will extend this with ``forbid_filters``,
``required_root_cause_families``, and rollback-driven negative
constraints.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class StrategistConstraints:
    """Per-AG constraints that survive across iterations."""

    _forbid_tables: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def forbid_table_for_ag(self, ag_id: str, table: str) -> None:
        ag = str(ag_id or "").strip()
        tbl = str(table or "").strip()
        if not ag or not tbl:
            return
        self._forbid_tables[ag].add(tbl)

    def forbid_tables_for_ag(self, ag_id: str) -> set[str]:
        return set(self._forbid_tables.get(str(ag_id or "").strip(), set()))

    def to_strategist_context(self) -> dict[str, dict[str, list[str]]]:
        """Render constraints as a stable JSON-friendly dict.

        Lists are sorted so the prompt-cache key is stable across runs.
        """
        out: dict[str, dict[str, list[str]]] = {}
        for ag, tables in self._forbid_tables.items():
            if not tables:
                continue
            out[ag] = {"forbid_tables": sorted(tables)}
        return out


def record_blast_radius_drop(
    *,
    constraints: StrategistConstraints,
    ag_id: str,
    dropped_patches: list[dict],
) -> None:
    """Mirror the blast-radius gate's drop list into the constraint store.

    ``dropped_patches`` is the list of dropped-patch dicts the gate
    already builds (each carries ``target`` = fully-qualified table).
    """
    for p in dropped_patches or []:
        target = str(p.get("target") or "").strip()
        if target:
            constraints.forbid_table_for_ag(ag_id, target)
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_strategist_forbid_tables.py`

Expected: PASS for all four tests.

- [ ] **Step 5: Wire into the harness — declare the constraint store and stamp on blast-radius drops**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, immediately after the `_dead_on_arrival_patch_signatures: set[tuple[str, ...]] = set()` declaration on line 10982 (next to existing run-scoped state), add:

```python
    from genie_space_optimizer.optimization.strategist_constraints import (
        StrategistConstraints,
    )
    _strategist_constraints: StrategistConstraints = StrategistConstraints()
```

Then, in the blast-radius gate emission block (after line 14283 `patches = _blast_kept`, but inside the `if _blast_dropped:` arm at 14260-14282), add this just after the `logger.warning(...)` call:

```python
            from genie_space_optimizer.optimization.strategist_constraints import (
                record_blast_radius_drop,
            )
            record_blast_radius_drop(
                constraints=_strategist_constraints,
                ag_id=str(ag_id),
                dropped_patches=[
                    {
                        "target": (
                            (p.get("target") if isinstance(p, dict) else None)
                            or _candidate_target_for_proposal(_blast_dropped)
                        ),
                    }
                    for p in _blast_dropped
                ],
            )
```

Replace the `_candidate_target_for_proposal(_blast_dropped)` shortcut with the canonical extraction the gate already uses — examine `harness.py:14233-14258` for the existing target-extraction pattern and reuse it. (If the dropped-patch dict already carries `target`, the inner `or` short-circuits and that helper is never called.)

- [ ] **Step 6: Pass the constraint into the next strategist call**

Find the strategist context build site (search for `metadata_snapshot["_strategist_constraints"]` if exists, else search for the next `generate_proposals_from_strategy` call). At the top of the strategist call (the `if ag is None:` block around line 12178-12183), add:

```python
                if _strategist_constraints.to_strategist_context():
                    metadata_snapshot["_strategist_constraints"] = (
                        _strategist_constraints.to_strategist_context()
                    )
```

The strategist's prompt-renderer will surface `_strategist_constraints` in a future task; for now, the structured dict on `metadata_snapshot` is observable in the replay fixture and MLflow tags, which is enough to confirm wire-up.

- [ ] **Step 7: Add an integration-style assertion to the test file**

Append to `tests/unit/test_strategist_forbid_tables.py`:

```python
def test_to_strategist_context_omits_empty_ags():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"type": "add_instruction"}],  # no target
    )
    assert c.to_strategist_context() == {}
```

- [ ] **Step 8: Run the full T5 suite**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_strategist_forbid_tables.py`

Expected: PASS for all five tests.

- [ ] **Step 9: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/strategist_constraints.py packages/genie-space-optimizer/tests/unit/test_strategist_forbid_tables.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(strategist): forbid_tables constraint after blast-radius drops

Records the table set blast-radius dropped for each AG, surfaces it in
metadata_snapshot[_strategist_constraints], and is observable in the
replay fixture. Phase B will extend with forbid_filters and pipe it
through the strategist prompt renderer."
```

---

## Task 6: Emit blast-radius `DecisionRecord`s

📋 **Status (2026-05-03):** Still needed. The postmortem follow-up wired
EVAL_CLASSIFIED, CLUSTER_SELECTED, STRATEGIST_AG_EMITTED, ACCEPTANCE_DECIDED,
and QID_RESOLUTION producers, but blast-radius drops still produce no
record. **Two changes from this draft:**
1. **Put the producer in `optimization/decision_emitters.py`**, not
   `rca_decision_trace.py` — that's where the 5 existing producers live.
2. **Use the RCA-grounding contract fields**, not `details`. The current
   `DecisionRecord` schema has no `details` field; instead populate
   `evidence_refs`, `target_qids`, `reason_detail`, and put gate-specific
   bits (e.g. `passing_dependents_outside_target`) inside `metrics`.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/decision_emitters.py` (add producer)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14260-14283` (and the `except ImportError` fallback at ~14320)
- Create: `packages/genie-space-optimizer/tests/unit/test_blast_radius_decision_records.py`

The Phase B operator transcript renders only when `_decision_records` is non-empty. Today the only producer is `patch_cap_decision_records` (`harness.py:14668`), which fires after the cap. Patches dropped by blast-radius never reach the cap, so cycle 9 produced `decision_records: []` for those iterations.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_blast_radius_decision_records.py`:

```python
"""TDD coverage for blast-radius DecisionRecord emission (T6).

Per postmortem follow-up: producer lives in decision_emitters.py and
populates the RCA-grounding contract fields (no `details` field).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.decision_emitters import (
    blast_radius_decision_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def _dropped(proposal_id: str, target: str, outside: list[str]) -> dict:
    return {
        "proposal_id": proposal_id,
        "patch_type": "add_sql_snippet_filter",
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": outside,
        "target": target,
    }


def test_emits_one_record_per_dropped_patch():
    records = blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        dropped=[
            _dropped("P001#1", "ucat.dev.tkt_payment", ["gs_003"]),
            _dropped("P002#1", "ucat.dev.tkt_payment", ["gs_003"]),
        ],
    )
    assert len(records) == 2
    for r in records:
        assert r.decision_type == DecisionType.GATE_DECISION
        assert r.outcome == DecisionOutcome.DROPPED
        # ReasonCode enum has no BLAST_RADIUS_DROPPED yet; the producer
        # uses NO_CAUSAL_TARGET (added in postmortem follow-up Task 1)
        # because that's exactly what blast-radius semantically means:
        # the patch's target qids overlap with passing dependents.
        assert r.reason_code == ReasonCode.NO_CAUSAL_TARGET
        assert r.ag_id == "AG_DECOMPOSED_H001"
        assert r.gate == "blast_radius"
        assert r.rca_id == "rca_h001"
        assert r.root_cause == "missing_filter"
        assert r.evidence_refs == ("ag:AG_DECOMPOSED_H001", "blast_radius_gate")
        assert r.target_qids == ("gs_024",)
        # Gate-specific bits land in metrics; cross-checker doesn't read these.
        assert "passing_dependents_outside_target" in r.metrics
        assert r.metrics["passing_dependents_outside_target"] == ["gs_003"]


def test_returns_empty_for_no_drops():
    assert blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_X",
        rca_id="",
        root_cause="",
        target_qids=[],
        dropped=[],
    ) == []
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_blast_radius_decision_records.py`

Expected: FAIL with `ImportError: cannot import name 'blast_radius_decision_records' from 'genie_space_optimizer.optimization.rca_decision_trace'`.

- [ ] **Step 3: Add the producer to `decision_emitters.py`**

Add the producer next to the 5 existing producers in
`packages/genie-space-optimizer/src/genie_space_optimizer/optimization/decision_emitters.py`.
The producer must populate the RCA-grounding contract fields (this is the
contract the cross-checker validates) and put gate-specific signals
inside `metrics`:

```python
def blast_radius_decision_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    rca_id: str,
    root_cause: str,
    target_qids: Sequence[str],
    dropped: Sequence[Mapping[str, Any]],
) -> list[DecisionRecord]:
    """Emit one ``GATE_DECISION`` / ``DROPPED`` record per blast-radius drop.

    The blast-radius gate runs *before* the patch-cap; without this
    producer, an iteration whose AG was fully dropped by the gate
    contributes zero ``DecisionRecord``s and Phase B's operator
    transcript renders nothing for that iteration.

    ``reason_code=NO_CAUSAL_TARGET`` because that's the precise semantic
    of a blast-radius drop: the patch would change rows for passing
    dependents outside the AG's target qids — i.e. the patch has no
    causally-clean target.
    """
    cleaned_target_qids = tuple(
        str(q) for q in (target_qids or ()) if str(q)
    )
    records: list[DecisionRecord] = []
    for d in dropped or []:
        proposal_id = str(d.get("proposal_id") or "")
        outside = [
            str(q)
            for q in (d.get("passing_dependents_outside_target") or [])
            if str(q)
        ]
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                ag_id=str(ag_id),
                rca_id=str(rca_id or ""),
                root_cause=str(root_cause or ""),
                proposal_id=proposal_id,
                proposal_ids=(proposal_id,) if proposal_id else (),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.DROPPED,
                reason_code=ReasonCode.NO_CAUSAL_TARGET,
                gate="blast_radius",
                reason_detail=str(d.get("reason") or ""),
                evidence_refs=(f"ag:{ag_id}", "blast_radius_gate"),
                target_qids=cleaned_target_qids,
                affected_qids=cleaned_target_qids,
                expected_effect=(
                    f"Patch would address {root_cause or 'failure pattern'} "
                    f"on {len(cleaned_target_qids)} target qid(s)."
                ),
                observed_effect=(
                    f"Dropped: collateral risk on {len(outside)} passing "
                    f"dependent(s) outside target."
                ),
                next_action=(
                    "Add target table to AG forbid_tables and re-strategize"
                ),
                metrics={
                    "patch_type": str(d.get("patch_type") or ""),
                    "passing_dependents_outside_target": outside,
                    "target": str(d.get("target") or ""),
                },
            )
        )
    return records
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_blast_radius_decision_records.py`

Expected: PASS for both tests.

- [ ] **Step 5: Wire into `harness.py` blast-radius emission**

In `harness.py` after the `logger.warning("AG %s blast-radius gate dropped …")` call at line 14276-14282 (the *first* blast-radius emission block; do the same in the `except ImportError` fallback at 14320-14326), add:

```python
            try:
                from genie_space_optimizer.optimization.decision_emitters import (
                    blast_radius_decision_records,
                    is_strict_mode,
                )
                # Recover RCA grounding from the AG's source clusters
                # (via _iter_source_clusters_by_id and _iter_rca_id_by_cluster
                # already initialized at the eval-entry site).
                _br_root_cause = ""
                _br_rca_id = ""
                for _cid in (ag.get("source_cluster_ids") or []):
                    _br_cluster = _iter_source_clusters_by_id.get(str(_cid)) or {}
                    if not _br_root_cause:
                        _br_root_cause = str(_br_cluster.get("root_cause") or "")
                    if not _br_rca_id:
                        _br_rca_id = str(_iter_rca_id_by_cluster.get(str(_cid)) or "")
                    if _br_root_cause and _br_rca_id:
                        break
                _br_target_qids = [
                    str(q) for q in (ag.get("affected_questions") or []) if q
                ]
                _br_records = blast_radius_decision_records(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=str(ag_id),
                    rca_id=_br_rca_id,
                    root_cause=_br_root_cause,
                    target_qids=_br_target_qids,
                    dropped=_blast_dropped,
                )
                _current_iter_inputs.setdefault("decision_records", []).extend(
                    [r.to_dict() for r in _br_records]
                )
            except Exception:
                # Mirror the strict-mode pattern from existing producers
                # so test failures from wiring bugs surface.
                _phase_b_producer_exceptions["blast_radius"] = (
                    _phase_b_producer_exceptions.get("blast_radius", 0) + 1
                )
                logger.debug(
                    "blast-radius DecisionRecord emission failed (non-fatal)",
                    exc_info=True,
                )
                if is_strict_mode():
                    raise
```

Place this block in **both** branches (primary at ~14283 and fallback at ~14327) so neither blast-radius code path drops the records.

Note: ``_iter_source_clusters_by_id``, ``_iter_rca_id_by_cluster``,
``_phase_b_producer_exceptions`` are all already in scope (initialized in
the eval-entry block per the postmortem follow-up).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py packages/genie-space-optimizer/tests/unit/test_blast_radius_decision_records.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(decision-trace): emit DecisionRecord on blast-radius drop

Phase B operator transcript was empty for cycle 9 because the only
producer (patch_cap_decision_records) doesn't fire when blast-radius
drops every patch before the cap. Now decision_records is populated
on every iteration that hits the gate."
```

---

## Task 7: Emit dead-on-arrival `DecisionRecord`s

📋 **Status (2026-05-03):** Partially superseded. The 5 ACCEPTANCE_DECIDED
producers wired by the postmortem follow-up already emit a
`DecisionRecord` (decision_type=ACCEPTANCE_DECIDED, outcome=SKIPPED,
reason_code=NO_APPLIED_PATCHES) at every dead-on-arrival /
no_applied_patches / pre_ag_snapshot_failed site. The `ag_outcomes` map
is what the existing wiring keys off — and Task 4.5 of the postmortem
follow-up already added the missing `skipped_pre_ag_snapshot_failed`
write.

**What's still needed in T7:** A more granular `PATCH_SKIPPED` record
*per dropped patch signature* (not per AG), so the operator can
distinguish "AG dropped because patch P001#1 was a no-op" from "AG
dropped because patch P002#1 hit applier rejection." This is
finer-grained than ACCEPTANCE_DECIDED and provides the per-signature
cohort for Track D's diagnostic-AG signature revalidation.

**Two changes from this draft:**
1. **Put the producer in `decision_emitters.py`** (not `rca_decision_trace.py`).
2. **Use the RCA-grounding contract fields** (no `details`).

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/decision_emitters.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:14702-14729` (dead-on-arrival skip) and `:14890-14908` (applier no-applied-patches) — alongside the existing `_phase_b_emit_ag_outcome_record(...)` calls
- Create: `packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_decision_records.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_decision_records.py`:

```python
"""TDD coverage for dead-on-arrival DecisionRecord emission (T7)."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
    dead_on_arrival_decision_record,
)


def test_record_for_blast_radius_dead_on_arrival():
    rec = dead_on_arrival_decision_record(
        run_id="run_1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    assert rec.decision_type == DecisionType.PATCH_SKIPPED
    assert rec.outcome == DecisionOutcome.SKIPPED
    assert rec.reason_code == ReasonCode.NO_APPLIED_PATCHES
    d = rec.to_dict()
    assert d["details"]["signature"] == []
    assert d["details"]["recovery_reason"] == "all_patches_dropped_by_blast_radius"


def test_record_for_applier_dead_on_arrival():
    rec = dead_on_arrival_decision_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_X",
        signature=("P001#1",),
        reason="all_selected_patches_dropped_by_applier",
    )
    d = rec.to_dict()
    assert d["details"]["signature"] == ["P001#1"]
    assert d["details"]["recovery_reason"] == (
        "all_selected_patches_dropped_by_applier"
    )
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_dead_on_arrival_decision_records.py`

Expected: FAIL with `ImportError: cannot import name 'dead_on_arrival_decision_record'`.

- [ ] **Step 3: Add the producer to `rca_decision_trace.py`**

Below `blast_radius_decision_records` (added in T6), add:

```python
def dead_on_arrival_decision_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    signature: tuple[str, ...],
    reason: str,
) -> DecisionRecord:
    """Emit a single ``PATCH_SKIPPED`` / ``SKIPPED`` record describing a
    dead-on-arrival AG.

    ``reason`` is the canonical recovery reason
    (``all_selected_patches_dropped_by_applier`` or
    ``all_patches_dropped_by_blast_radius``).
    """
    return DecisionRecord(
        run_id=str(run_id),
        iteration=_as_int(iteration),
        ag_id=str(ag_id),
        proposal_id="",
        decision_type=DecisionType.PATCH_SKIPPED,
        outcome=DecisionOutcome.SKIPPED,
        reason_code=ReasonCode.NO_APPLIED_PATCHES,
        details={
            "signature": list(signature or ()),
            "recovery_reason": str(reason or ""),
        },
    )
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_dead_on_arrival_decision_records.py`

Expected: PASS for both tests.

- [ ] **Step 5: Wire into both dead-on-arrival paths in `harness.py`**

In `harness.py`, replace the dead-on-arrival skip block at line 14708-14729 (inside `if _selected_patch_signature in _dead_on_arrival_patch_signatures:`) — add this just before the existing `_drain_buffered_action_groups` call (added in T1):

```python
            try:
                from genie_space_optimizer.optimization.rca_decision_trace import (
                    dead_on_arrival_decision_record,
                )
                _doa_record = dead_on_arrival_decision_record(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=str(ag_id),
                    signature=_selected_patch_signature,
                    reason="dead_on_arrival_retry_blocked",
                )
                _current_iter_inputs.setdefault("decision_records", []).append(
                    _doa_record.to_dict()
                )
            except Exception:
                logger.debug(
                    "dead-on-arrival DecisionRecord emission failed (non-fatal)",
                    exc_info=True,
                )
```

Do the same after `_dead_on_arrival_ag_ids.add(str(ag_id))` at line 14892 (the applier-rejection path), passing `reason="all_selected_patches_dropped_by_applier"`.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py packages/genie-space-optimizer/tests/unit/test_dead_on_arrival_decision_records.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(decision-trace): emit DecisionRecord on dead-on-arrival paths

Cycle 9 iters 2-5 produced decision_records=[] because the dead-on-arrival
short-circuit ran before any cap/applier emitter. Both the retry-blocked
and applier-rejection paths now emit a typed PATCH_SKIPPED record."
```

---

## Task 8: Wire `compute_scoreboard()` into the end-of-iteration banner

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:16319-16350` (the `iteration_summary_marker` emission block)
- Create: `packages/genie-space-optimizer/tests/unit/test_scoreboard_harness_wiring.py`

`scoreboard.compute_scoreboard()` exists and is unit-tested but never runs in a real loop. Wire it in adjacent to the existing `iteration_summary_marker` so the operator gets `dominant_signal` for free.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_scoreboard_harness_wiring.py`:

```python
"""TDD coverage for the end-of-iteration scoreboard banner (T8)."""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _format_scoreboard_banner,
)


def test_scoreboard_banner_renders_dominant_signal():
    snapshot = {
        "iteration": 3,
        "passing_qids": ["q1", "q2"],
        "hard_failure_qids": ["q3"],
        "applied_patch_count": 2,
        "rolled_back_patch_count": 0,
        "trace_id_fallback_count": 0,
        "trace_id_total": 24,
    }
    banner = _format_scoreboard_banner(loop_snapshot=snapshot)
    assert "iteration_3" in banner.lower() or "iteration 3" in banner.lower()
    assert "dominant_signal" in banner.lower()


def test_scoreboard_banner_handles_empty_snapshot():
    banner = _format_scoreboard_banner(loop_snapshot={})
    assert banner.strip() != ""
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_scoreboard_harness_wiring.py`

Expected: FAIL with `ImportError: cannot import name '_format_scoreboard_banner'`.

- [ ] **Step 3: Add the formatter helper to `harness.py`**

Insert at module scope in `harness.py`, near the other `_format_*` helpers (search for `def _kv` or `def _section` for the right neighborhood; place it just below them):

```python
def _format_scoreboard_banner(*, loop_snapshot: dict) -> str:
    """Render an end-of-iteration scoreboard banner.

    Calls ``scoreboard.compute_scoreboard`` against ``loop_snapshot``
    and renders the result via the existing ``_section`` / ``_kv``
    helpers so the format matches every other harness banner.
    """
    try:
        from genie_space_optimizer.optimization.scoreboard import (
            compute_scoreboard,
        )
        result = compute_scoreboard(loop_snapshot or {})
    except Exception:
        return _section("SCOREBOARD UNAVAILABLE", "-") + "\n" + _bar("-")

    lines = [_section("END-OF-ITERATION SCOREBOARD", "=")]
    lines.append(_kv("dominant_signal", result.get("dominant_signal", "?")))
    for k in (
        "journey_completeness_pct",
        "hard_cluster_coverage_pct",
        "causal_patch_survival_pct",
        "trace_id_fallback_rate_pct",
        "accuracy_delta",
    ):
        if k in result:
            lines.append(_kv(k, result[k]))
    lines.append(_bar("="))
    return "\n".join(lines)
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_scoreboard_harness_wiring.py`

Expected: PASS for both tests (the second test exercises the empty-snapshot fallback).

- [ ] **Step 5: Call the banner from the iteration emission block**

In `harness.py` immediately *before* the `print(iteration_summary_marker(...))` call near line 16338, add:

```python
            try:
                _loop_snapshot_for_scoreboard = {
                    "iteration": int(iteration_counter),
                    "passing_qids": list(
                        _current_iter_inputs.get("post_eval_passing_qids") or []
                    ),
                    "hard_failure_qids": [
                        str(c.get("question_ids") or [])
                        for c in (_current_iter_inputs.get("clusters") or [])
                    ],
                    "applied_patch_count": _accepted_count,
                    "rolled_back_patch_count": _rolled_back_count,
                    "trace_id_fallback_count": int(
                        _trace_id_fallback_count_this_iter
                        if "_trace_id_fallback_count_this_iter" in dir()
                        else 0
                    ),
                    "trace_id_total": int(
                        _trace_id_total_this_iter
                        if "_trace_id_total_this_iter" in dir()
                        else 0
                    ),
                }
                print(_format_scoreboard_banner(
                    loop_snapshot=_loop_snapshot_for_scoreboard,
                ))
            except Exception:
                logger.debug("scoreboard banner failed (non-fatal)", exc_info=True)
```

(If `_trace_id_fallback_count_this_iter` / `_trace_id_total_this_iter` aren't defined in the surrounding scope, they're optional — `compute_scoreboard` tolerates zeros and emits a separate signal-class.)

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_scoreboard_harness_wiring.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(harness): wire compute_scoreboard into end-of-iteration banner

Track 6 (scoreboard.py) shipped the math but the banner was never
rendered; cycle 9 ran 5 iterations with no dominant_signal output."
```

---

## Task 9: Extend `failure_buckets.SEED_CATALOG` with cycle 9 patterns

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/failure_buckets.py:233` (append before the closing `]`)
- Create: `packages/genie-space-optimizer/tests/unit/test_failure_buckets_cycle9.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_failure_buckets_cycle9.py`:

```python
"""TDD coverage for cycle 9 SEED_CATALOG additions (T9)."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.failure_buckets import (
    FailureBucket,
    SEED_CATALOG,
    match_pattern_id,
)


CYCLE9_PATTERN_IDS = (
    "dead_on_arrival_blocks_buffered_drain",
    "blast_radius_no_escape_hatch",
    "proposal_direction_inversion",
    "union_all_grain_split",
)


@pytest.mark.parametrize("pid", CYCLE9_PATTERN_IDS)
def test_pattern_present_in_catalog(pid):
    pattern = match_pattern_id(pid)
    assert pattern is not None, f"Missing seed pattern: {pid}"
    assert pattern.source_run.startswith("cycle9") or "cycle9" in pattern.source_run


def test_buckets_assigned():
    expected = {
        "dead_on_arrival_blocks_buffered_drain": FailureBucket.GATE_OR_CAP_GAP,
        "blast_radius_no_escape_hatch": FailureBucket.GATE_OR_CAP_GAP,
        "proposal_direction_inversion": FailureBucket.PROPOSAL_GAP,
        "union_all_grain_split": FailureBucket.MODEL_CEILING,
    }
    for pid, bucket in expected.items():
        p = match_pattern_id(pid)
        assert p is not None
        assert p.bucket is bucket, f"{pid} bucket={p.bucket} expected={bucket}"


def test_catalog_grows_by_four():
    pids = [p.pattern_id for p in SEED_CATALOG]
    for pid in CYCLE9_PATTERN_IDS:
        assert pid in pids, f"{pid} not appended to SEED_CATALOG"
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_failure_buckets_cycle9.py`

Expected: FAIL because none of the four patterns exists.

- [ ] **Step 3: Append the four patterns**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/failure_buckets.py`, immediately before the closing `]` of `SEED_CATALOG` on line 233-234, add:

```python
    BucketingSeedPattern(
        pattern_id="dead_on_arrival_blocks_buffered_drain",
        description=(
            "AG fails dead-on-arrival or applier-rejection and the "
            "lever loop unconditionally clears pending_action_groups, "
            "discarding buffered AGs targeting other clusters"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="buffered_ag_unrelated_drop",
        source_run="cycle9",
        why=(
            "Selective drain not yet wired; one failed AG took down "
            "two unrelated buffered AGs."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="blast_radius_no_escape_hatch",
        description=(
            "Blast-radius gate dropped every candidate patch; the "
            "strategist re-proposed the same shape on the next "
            "iteration with no constraint on the dropped table"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="blast_radius_dead_end",
        source_run="cycle9",
        why=(
            "No forbid_tables feedback loop into the strategist; "
            "loop spent 5 iterations producing the same dropped "
            "patch shape."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="proposal_direction_inversion",
        description=(
            "Strategist proposal value directly contradicts the "
            "dominant cluster counterfactual (e.g. ADD filter X when "
            "the diagnosis says REMOVE filter X)"
        ),
        bucket=FailureBucket.PROPOSAL_GAP,
        sub_bucket="counterfactual_contradiction",
        source_run="cycle9",
        why=(
            "Proposal grounding does not validate that proposal value "
            "agrees with cluster counterfactual_fix direction."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="union_all_grain_split",
        description=(
            "Generated SQL composed two heterogeneous report shapes "
            "via UNION ALL with NULL filler columns instead of one "
            "result set at the question's expected grain"
        ),
        bucket=FailureBucket.MODEL_CEILING,
        sub_bucket="union_all_grain_split",
        source_run="cycle9",
        why=(
            "Strategist cannot synthesize a single canonical grain; "
            "no template asset exists for compound monthly + pattern "
            "breakdowns."
        ),
    ),
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_failure_buckets_cycle9.py`

Expected: PASS for all six tests (3 parametrized + 3 plain).

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/failure_buckets.py packages/genie-space-optimizer/tests/unit/test_failure_buckets_cycle9.py
git commit -m "feat(failure-buckets): add four cycle9 seed patterns

Adds dead_on_arrival_blocks_buffered_drain, blast_radius_no_escape_hatch,
proposal_direction_inversion, and union_all_grain_split — all observed
in run 1e855111-b463-4556-9b30-8cd32f78ebcb."
```

---

## Task 10: No-decision-records diagnostic marker

📋 **Status (2026-05-03): SUPERSEDED.** The postmortem follow-up
already shipped:
- `phase_b_no_records_marker(...)` in `run_analysis_contract.py`
- `NoRecordsReason` enum in `decision_emitters.py` (closed vocabulary:
  `no_clusters`, `no_ags_emitted`, `all_ags_dropped_at_grounding`,
  `patch_cap_did_not_fire`, `producer_exception`, `unknown`)
- `classify_no_records_reason(...)` helper that picks the reason
- Harness wiring at the per-iteration accounting block (right after the
  Phase B persistence stanza) emits the marker + `decision_trace.iter_<N>.no_records_reason`
  MLflow tag when `_decision_records` is empty
- Byte-stable snapshot tests in `test_phase_b_marker_snapshot.py`

The `no_decision_records_marker` from this T10 draft (with `event=no_decision_records`,
`ag_outcomes_count` field) is **not** the same payload as the shipped
`phase_b_no_records_marker` (which carries `producer_exceptions` dict +
`contract_version`). The shipped one is strictly richer.

**Action:** drop T10 from the implementation list. If the analyzer skill
needs `ag_outcomes_count` specifically, extend `phase_b_no_records_marker`
to carry it as a follow-up (single field add).

**Files:** *(none — superseded)*

---

## Task 10 (originally proposed): No-decision-records diagnostic marker

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:16240-16306` (after the Phase B persistence block)
- Create: `packages/genie-space-optimizer/tests/unit/test_no_decision_records_marker.py`

When `_decision_records` is genuinely empty (no AG attempted, baseline-only iteration, etc.), the operator needs to know it's *intentional*, not a capture failure.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_no_decision_records_marker.py`:

```python
"""TDD coverage for the no-decision-records diagnostic (T10)."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    no_decision_records_marker,
)


def test_marker_emits_reason():
    s = no_decision_records_marker(
        optimization_run_id="run_1",
        iteration=2,
        reason="no_ag_attempted",
        ag_outcomes_count=0,
    )
    payload = _parse(s)
    assert payload["event"] == "no_decision_records"
    assert payload["reason"] == "no_ag_attempted"
    assert payload["ag_outcomes_count"] == 0


def test_marker_iteration_int_coerced():
    s = no_decision_records_marker(
        optimization_run_id="run_1",
        iteration="3",
        reason="capture_skipped",
        ag_outcomes_count=1,
    )
    assert _parse(s)["iteration"] == 3


def _parse(line: str) -> dict:
    # Markers are printed as ===NAME===<json>===END===; harness contract.
    # Extract JSON between the first '{' and last '}'.
    start = line.index("{")
    end = line.rindex("}") + 1
    return json.loads(line[start:end])
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_no_decision_records_marker.py`

Expected: FAIL with `ImportError: cannot import name 'no_decision_records_marker'`.

- [ ] **Step 3: Add the marker producer**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py`, immediately after `phase_b_marker` (line ~110), add:

```python
def no_decision_records_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    reason: str,
    ag_outcomes_count: int,
) -> str:
    """Emit a marker stating *why* an iteration produced zero
    DecisionRecords.

    ``reason`` should be one of:
      * ``no_ag_attempted`` — baseline / convergence iteration.
      * ``capture_skipped`` — exception in the Phase B persistence block.
      * ``mlflow_run_absent`` — no active MLflow run; persistence noop.
      * ``producers_silent`` — AGs attempted but no producer fired
        (this is a bug; investigate harness wiring).
    """
    return _emit_marker(
        name="NO_DECISION_RECORDS",
        payload={
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "event": "no_decision_records",
            "reason": str(reason),
            "ag_outcomes_count": int(ag_outcomes_count),
        },
    )
```

(`_emit_marker` is the existing helper used by `iteration_summary_marker` and `phase_b_marker`; reuse it.)

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_no_decision_records_marker.py`

Expected: PASS for both tests.

- [ ] **Step 5: Wire into the Phase B block in `harness.py`**

In `harness.py`, modify the Phase B persistence block at line 16250-16306. After the `if _decision_records:` arm, add an `else:` arm:

```python
            if _decision_records:
                # ... existing block, unchanged ...
            else:
                _ag_outcomes = _current_iter_inputs.get("ag_outcomes") or {}
                if not _ag_outcomes:
                    _no_records_reason = "no_ag_attempted"
                else:
                    _no_records_reason = "producers_silent"
                from genie_space_optimizer.optimization.run_analysis_contract import (
                    no_decision_records_marker,
                )
                print(no_decision_records_marker(
                    optimization_run_id=run_id,
                    iteration=iteration_counter,
                    reason=_no_records_reason,
                    ag_outcomes_count=len(_ag_outcomes),
                ))
```

Also, in the existing `except Exception:` arm at line 16302-16314 of the original block, replace `_mlflow_phase_b_partial.set_tag("genie.phase_b.partial", "true")` with the corresponding marker:

```python
                from genie_space_optimizer.optimization.run_analysis_contract import (
                    no_decision_records_marker,
                )
                print(no_decision_records_marker(
                    optimization_run_id=run_id,
                    iteration=iteration_counter,
                    reason="capture_skipped",
                    ag_outcomes_count=len(
                        _current_iter_inputs.get("ag_outcomes") or {}
                    ),
                ))
                if _mlflow_phase_b_partial.active_run() is not None:
                    _mlflow_phase_b_partial.set_tag(
                        "genie.phase_b.partial", "true"
                    )
```

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py packages/genie-space-optimizer/tests/unit/test_no_decision_records_marker.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(run-analysis): no_decision_records marker with reason

Cycle 9 emitted decision_records=[] for every iteration; today the
operator can't tell whether that's a capture bug or an intentional
empty iteration. The new marker tells them which."
```

---

## Task 11: `proposal_direction_contradicts_counterfactual` predicate

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_quality.py:111` (insert before `prefer_scoped_instruction_over_weak_snippet`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py:600-610` (wire into the existing snippet-quality demotion list)
- Create: `packages/genie-space-optimizer/tests/unit/test_proposal_direction_contradicts_counterfactual.py`

Cycle 9 P002 proposed `add_sql_snippet_filter PAYMENT_CURRENCY_CD = 'USD'` while the dominant cluster's `counterfactual_fix` said `Remove the PAYMENT_CURRENCY_CD = USD filter`. A predicate that pattern-matches `Remove the X filter` against the patch's `value` flags this contradiction.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_proposal_direction_contradicts_counterfactual.py`:

```python
"""TDD coverage for proposal direction contradiction detection (T11)."""
from __future__ import annotations

from genie_space_optimizer.optimization.sql_shape_quality import (
    proposal_direction_contradicts_counterfactual,
)


def test_add_filter_when_counterfactual_says_remove():
    patch = {
        "type": "add_sql_snippet_filter",
        "value": "tkt_payment.PAYMENT_CURRENCY_CD = 'USD'",
        "counterfactual_fix": (
            "Remove the PAYMENT_CURRENCY_CD = USD filter (the question "
            "says total payment amount in USD referring to the display "
            "unit)"
        ),
    }
    assert proposal_direction_contradicts_counterfactual(patch) is True


def test_aligned_proposal_does_not_flag():
    patch = {
        "type": "add_sql_snippet_measure",
        "value": "SUM(tkt_payment.PAYMENT_AMT)",
        "counterfactual_fix": "Use SUM(PAYMENT_AMT) instead of COUNT(*)",
    }
    assert proposal_direction_contradicts_counterfactual(patch) is False


def test_no_counterfactual_returns_false():
    patch = {"type": "add_sql_snippet_filter", "value": "X = 1"}
    assert proposal_direction_contradicts_counterfactual(patch) is False


def test_remove_phrasing_variants_detected():
    for cf in (
        "Remove the X filter",
        "remove the X = USD filter",
        "Drop the X filter",
        "Strip the X = USD filter",
    ):
        patch = {
            "type": "add_sql_snippet_filter",
            "value": "T.X = 'USD'",
            "counterfactual_fix": cf,
        }
        assert proposal_direction_contradicts_counterfactual(patch) is True, cf


def test_only_fires_for_add_snippet_types():
    patch = {
        "type": "add_instruction",
        "value": "Always include PAYMENT_CURRENCY_CD = 'USD'",
        "counterfactual_fix": "Remove the PAYMENT_CURRENCY_CD = USD filter",
    }
    assert proposal_direction_contradicts_counterfactual(patch) is False
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_proposal_direction_contradicts_counterfactual.py`

Expected: FAIL with `ImportError: cannot import name 'proposal_direction_contradicts_counterfactual'`.

- [ ] **Step 3: Implement the predicate**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_quality.py`, immediately before `def prefer_scoped_instruction_over_weak_snippet` (around line 113), add:

```python
import re

_REMOVE_VERB_RE = re.compile(
    r"\b(remove|drop|strip|delete)\b\s+the\s+([A-Z_]+(?:\s*=\s*[A-Za-z0-9'_-]+)?)"
    r"(?:\s+filter)?",
    re.IGNORECASE,
)
_ADD_SNIPPET_TYPES = {
    "add_sql_snippet_filter",
    "add_sql_snippet_measure",
    "add_sql_snippet_dimension",
}


def proposal_direction_contradicts_counterfactual(
    patch: dict[str, Any],
) -> bool:
    """Return True when an ``add_sql_snippet_*`` patch's value matches
    the column/expression the counterfactual_fix says to *remove*.

    Triggers only on ``add_*`` patch types. Instruction patches are
    out of scope (an instruction can legitimately say "always include
    X" even when one historic counterfactual said remove X — context
    matters and the instruction layer is the right place to argue).
    """
    patch_type = str(patch.get("type") or patch.get("patch_type") or "").lower()
    if patch_type not in _ADD_SNIPPET_TYPES:
        return False
    cf = str(patch.get("counterfactual_fix") or "")
    if not cf:
        return False
    value = str(patch.get("value") or "").upper()
    if not value:
        return False
    for match in _REMOVE_VERB_RE.finditer(cf):
        token = match.group(2).upper().strip()
        # Token can be "X" or "X = 'USD'"; check both forms in the value.
        col = token.split("=")[0].strip()
        if col and col in value:
            return True
    return False
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_proposal_direction_contradicts_counterfactual.py`

Expected: PASS for all five tests.

- [ ] **Step 5: Wire into `proposal_grounding.py`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py`, find the existing list of "weak snippet" predicates (search for `is_unrequested_currency_filter` or `is_unrequested_is_not_null_filter`). Add `proposal_direction_contradicts_counterfactual` to that list so the demotion path treats a direction-inverted snippet as weak. Concretely, find the import block that currently reads:

```python
from genie_space_optimizer.optimization.sql_shape_quality import (
    is_rank_when_limit_n_required,
    is_unrequested_currency_filter,
    is_unrequested_is_not_null_filter,
    prefer_scoped_instruction_over_weak_snippet,
)
```

and change it to:

```python
from genie_space_optimizer.optimization.sql_shape_quality import (
    is_rank_when_limit_n_required,
    is_unrequested_currency_filter,
    is_unrequested_is_not_null_filter,
    prefer_scoped_instruction_over_weak_snippet,
    proposal_direction_contradicts_counterfactual,
)
```

Then in the existing predicate-iteration loop (search for `is_unrequested_is_not_null_filter(patch)` to find it), add the new predicate to the OR chain so any single firing predicate marks the patch weak.

- [ ] **Step 6: Run full sql_shape_quality tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_proposal_direction_contradicts_counterfactual.py tests/unit/test_sql_shape_quality.py`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_quality.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py packages/genie-space-optimizer/tests/unit/test_proposal_direction_contradicts_counterfactual.py
git commit -m "feat(sql-shape-quality): proposal_direction_contradicts_counterfactual

Catches the cycle 9 P002 case: strategist proposed add_sql_snippet_filter
PAYMENT_CURRENCY_CD = USD while the cluster counterfactual said Remove
the PAYMENT_CURRENCY_CD = USD filter. Wired into proposal_grounding's
weak-snippet predicate list."
```

---

## Task 12: Notebook-exit analysis manifests

📋 **Status (2026-05-03):** Half done. The postmortem follow-up
already shipped:
- `loop_out["phase_b"]` manifest at lever-loop return with
  `contract_version`, `decision_records_total`, `iter_record_counts`,
  `iter_violation_counts`, `no_records_iterations`, `artifact_paths`,
  `producer_exceptions`, `target_qids_missing_count`, `total_violations`.
- `run_lever_loop.py:548-563` debug_info allowlist with `"phase_b"` so
  the manifest survives the `dbutils.notebook.exit(...)` filter.
- `phase_b_end_marker(...)` emitted at lever-loop terminate.

**What's still needed in T12:**
1. **Refactor to a typed manifest builder** so the harness no longer
   builds the dict inline. Move the dict literal in `harness.py` (right
   before `return loop_out`) into `lever_loop_exit_manifest(...)` /
   `finalize_exit_manifest(...)` builders in `run_analysis_contract.py`.
2. **Reconcile field names with the existing `phase_b` manifest.** The
   T12 draft below proposes `per_iteration_decision_counts`,
   `per_iteration_journey_violations`, `no_decision_record_reasons`,
   `phase_b_decision_artifacts`, `phase_b_transcript_artifacts`. These
   must either:
   - **(a)** live alongside the existing `phase_b` block (additive),
     duplicating data; or
   - **(b)** be folded INTO the existing `phase_b` manifest as
     additional fields. **Recommended: (b)** — the existing manifest
     already carries `iter_record_counts` (≈ `per_iteration_decision_counts`),
     `iter_violation_counts` (≈ `per_iteration_journey_violations`), and
     `artifact_paths` (≈ `phase_b_decision_artifacts`). Add the missing
     `phase_b_transcript_artifacts` (transcripts) and
     `no_records_iterations` already covers the no-decision case (no
     need for `no_decision_record_reasons` array since each iter's
     reason is in the `GSO_PHASE_B_NO_RECORDS_V1` marker).
3. **Add `finalize_exit_manifest(...)`** for the finalize task — this
   was not done by the postmortem follow-up (only lever-loop exit was
   touched).

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py` (add two manifest builders that wrap existing phase_b shape)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (replace inline `phase_b` dict literal with a call to the builder)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py:560-562` (use the builder; allowlist already passes `phase_b` through)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_finalize.py:466-490`
- Create: `packages/genie-space-optimizer/tests/unit/test_lever_loop_exit_manifest.py`

GPT's point: `databricks jobs get-run-output` only returns notebook exit JSON. Surface decision counts, journey violations, and Phase B artifact paths in the exit value.

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_lever_loop_exit_manifest.py`:

```python
"""TDD coverage for the lever-loop and finalize exit manifests (T12)."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    finalize_exit_manifest,
    lever_loop_exit_manifest,
)


def test_lever_loop_manifest_carries_decision_counts():
    payload_str = lever_loop_exit_manifest(
        optimization_run_id="run_1",
        mlflow_experiment_id="123",
        accuracy=0.875,
        iteration_counter=5,
        levers_attempted=[5, 1, 2, 6],
        levers_accepted=[],
        levers_rolled_back=[],
        per_iteration_decision_counts=[0, 0, 0, 0, 0],
        per_iteration_journey_violations=[0, 0, 0, 0, 0],
        no_decision_record_reasons=["producers_silent"] * 5,
        phase_b_decision_artifacts=[],
        phase_b_transcript_artifacts=[],
    )
    payload = json.loads(payload_str)
    assert payload["accuracy"] == 0.875
    assert payload["iteration_counter"] == 5
    assert payload["per_iteration_decision_counts"] == [0, 0, 0, 0, 0]
    assert payload["no_decision_record_reasons"] == ["producers_silent"] * 5


def test_finalize_manifest_carries_status_and_artifacts():
    payload_str = finalize_exit_manifest(
        optimization_run_id="run_1",
        status="MAX_ITERATIONS",
        convergence_reason="max_iterations",
        repeatability_pct=100.0,
        elapsed_seconds=1044.7,
        report_path="/tmp/report.md",
        promoted_to_champion=False,
    )
    payload = json.loads(payload_str)
    assert payload["status"] == "MAX_ITERATIONS"
    assert payload["repeatability_pct"] == 100.0
    assert payload["promoted_to_champion"] is False
    assert payload["report_path"] == "/tmp/report.md"
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_lever_loop_exit_manifest.py`

Expected: FAIL with `ImportError: cannot import name 'lever_loop_exit_manifest'`.

- [ ] **Step 3: Add the manifest builders**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py`, append below `no_decision_records_marker`:

```python
def lever_loop_exit_manifest(
    *,
    optimization_run_id: str,
    mlflow_experiment_id: str,
    accuracy: float,
    iteration_counter: int,
    levers_attempted: list[int],
    levers_accepted: list[int],
    levers_rolled_back: list[int],
    per_iteration_decision_counts: list[int],
    per_iteration_journey_violations: list[int],
    no_decision_record_reasons: list[str],
    phase_b_decision_artifacts: list[str],
    phase_b_transcript_artifacts: list[str],
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    Task 4 (``run_lever_loop``).

    Returned as a JSON string (not dict) so the call site stays a
    single ``dbutils.notebook.exit(lever_loop_exit_manifest(...))``.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "mlflow_experiment_id": str(mlflow_experiment_id),
        "accuracy": float(accuracy),
        "iteration_counter": int(iteration_counter),
        "levers_attempted": list(levers_attempted),
        "levers_accepted": list(levers_accepted),
        "levers_rolled_back": list(levers_rolled_back),
        "per_iteration_decision_counts": list(per_iteration_decision_counts),
        "per_iteration_journey_violations": list(
            per_iteration_journey_violations
        ),
        "no_decision_record_reasons": list(no_decision_record_reasons),
        "phase_b_decision_artifacts": list(phase_b_decision_artifacts),
        "phase_b_transcript_artifacts": list(phase_b_transcript_artifacts),
    }
    import json as _json
    return _json.dumps(payload, default=str)


def finalize_exit_manifest(
    *,
    optimization_run_id: str,
    status: str,
    convergence_reason: str,
    repeatability_pct: float,
    elapsed_seconds: float,
    report_path: str,
    promoted_to_champion: bool,
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    Task 5 (``run_finalize``)."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "status": str(status),
        "convergence_reason": str(convergence_reason),
        "repeatability_pct": float(repeatability_pct),
        "elapsed_seconds": float(elapsed_seconds),
        "report_path": str(report_path),
        "promoted_to_champion": bool(promoted_to_champion),
    }
    import json as _json
    return _json.dumps(payload, default=str)
```

- [ ] **Step 4: Run the tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_lever_loop_exit_manifest.py`

Expected: PASS for both tests.

- [ ] **Step 5: Wire `lever_loop_exit_manifest` into `run_lever_loop.py`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py:560-562`, find:

```python
)
_banner("Task 4 Completed")
dbutils.notebook.exit(json.dumps(debug_info, default=str))
```

Replace with:

```python
)
_banner("Task 4 Completed")
from genie_space_optimizer.optimization.run_analysis_contract import (
    lever_loop_exit_manifest,
)
dbutils.notebook.exit(lever_loop_exit_manifest(
    optimization_run_id=run_id,
    mlflow_experiment_id=os.environ.get("MLFLOW_EXPERIMENT_ID", ""),
    accuracy=float(debug_info.get("accuracy", 0.0)),
    iteration_counter=int(debug_info.get("iteration_counter", 0)),
    levers_attempted=list(
        debug_info.get("debug_info", {}).get("levers_attempted") or []
    ),
    levers_accepted=list(
        debug_info.get("debug_info", {}).get("levers_accepted") or []
    ),
    levers_rolled_back=list(
        debug_info.get("debug_info", {}).get("levers_rolled_back") or []
    ),
    per_iteration_decision_counts=list(
        debug_info.get("per_iteration_decision_counts") or []
    ),
    per_iteration_journey_violations=list(
        debug_info.get("per_iteration_journey_violations") or []
    ),
    no_decision_record_reasons=list(
        debug_info.get("no_decision_record_reasons") or []
    ),
    phase_b_decision_artifacts=list(
        debug_info.get("phase_b_decision_artifacts") or []
    ),
    phase_b_transcript_artifacts=list(
        debug_info.get("phase_b_transcript_artifacts") or []
    ),
))
```

(`debug_info` is the existing dict the harness builds; the four new keys (`per_iteration_decision_counts`, `per_iteration_journey_violations`, `no_decision_record_reasons`, `phase_b_decision_artifacts`) need to be populated by the harness — see Step 6 below. They default to `[]` so the manifest stays well-formed even before harness wiring lands.)

- [ ] **Step 6: Populate `debug_info` in the harness**

In `harness.py`, find where `debug_info` is constructed for the lever-loop return (search for `"levers_attempted"` near the loop exit). Append:

```python
        "per_iteration_decision_counts": [
            len(it.get("decision_records") or [])
            for it in iterations_data
        ],
        "per_iteration_journey_violations": [
            len((it.get("journey_validation") or {}).get("violations", []))
            for it in iterations_data
        ],
        "no_decision_record_reasons": [
            (it.get("no_decision_record_reason") or "")
            for it in iterations_data
        ],
        "phase_b_decision_artifacts": [
            it.get("phase_b_decision_artifact")
            for it in iterations_data
            if it.get("phase_b_decision_artifact")
        ],
        "phase_b_transcript_artifacts": [
            it.get("phase_b_transcript_artifact")
            for it in iterations_data
            if it.get("phase_b_transcript_artifact")
        ],
```

In the Phase B persistence block (T10's `else:` arm), also stamp `_current_iter_inputs["no_decision_record_reason"] = _no_records_reason`. In the `if _decision_records:` arm, stamp `_current_iter_inputs["phase_b_decision_artifact"] = _phase_b_decision_artifact` and `_current_iter_inputs["phase_b_transcript_artifact"] = _phase_b_transcript_artifact`.

- [ ] **Step 7: Wire `finalize_exit_manifest` into `run_finalize.py`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_finalize.py:466`, find:

```python
dbutils.notebook.exit(json.dumps({
    "status": finalize_out["status"],
    "convergence_reason": finalize_out["convergence_reason"],
    ...
}))
```

Replace with:

```python
from genie_space_optimizer.optimization.run_analysis_contract import (
    finalize_exit_manifest,
)
dbutils.notebook.exit(finalize_exit_manifest(
    optimization_run_id=run_id,
    status=str(finalize_out["status"]),
    convergence_reason=str(finalize_out["convergence_reason"]),
    repeatability_pct=float(finalize_out.get("repeatability_pct", 0.0)),
    elapsed_seconds=float(finalize_out.get("elapsed_seconds", 0.0)),
    report_path=str(finalize_out.get("report_path", "")),
    promoted_to_champion=bool(finalize_out.get("promoted_to_champion", False)),
))
```

- [ ] **Step 8: Re-run the manifest tests**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/unit/test_lever_loop_exit_manifest.py`

Expected: PASS for both tests.

- [ ] **Step 9: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_analysis_contract.py packages/genie-space-optimizer/tests/unit/test_lever_loop_exit_manifest.py packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_finalize.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(notebook-exit): structured manifests for lever-loop + finalize

Surfaces decision counts, journey violations, and Phase B artifact paths
in dbutils.notebook.exit so databricks jobs get-run-output reveals the
same numbers MLflow has."
```

---

## Task 13: Cycle 9 replay intake

**Files:**
- Create: `packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle9_raw.json`
- Create: `packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py`

The user pasted a complete `===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN/END===` block from this run. Extract it verbatim, save as the cycle 9 fixture, and gate cycle-9 promotion on zero `journey_validation` violations after replay.

- [ ] **Step 1: Extract the fixture JSON**

The fixture lives between `===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===` and `===PHASE_A_REPLAY_FIXTURE_JSON_END===` in the cycle 9 stderr the user shared. Save the JSON body **verbatim** (no reformatting) to:

```
packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle9_raw.json
```

(Use the existing cycle 8 fixture path naming convention. The JSON body starts at `{"fixture_id":"airline_real_v1_run_1e855111-…"` and ends at the matching closing `]}}`.)

- [ ] **Step 2: Write the snapshot test**

Create `packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py`:

```python
"""Cycle 9 replay intake — gate promotion on zero violations (T13).

The cycle 9 fixture is the verbatim PHASE_A_REPLAY_FIXTURE_JSON block
from run 1e855111-b463-4556-9b30-8cd32f78ebcb. It already shows
``journey_validation: null`` and ``decision_records: []`` because the
producers added in T6/T7/T10 hadn't shipped at capture time.

This test pins what the *next* run must look like once T6/T7/T10 land:
journey_validation populated, decision_records populated for every
iteration whose AG hit a gate, and zero violation counts after replay.
"""
from __future__ import annotations

import json
import pathlib

import pytest

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "airline_real_v1_cycle9_raw.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_loaded_with_five_iterations(fixture):
    assert fixture["fixture_id"].startswith("airline_real_v1_run_")
    assert len(fixture["iterations"]) == 5


@pytest.mark.skip(
    reason=(
        "Cycle 9 raw fixture — pre-T6/T7/T10. journey_validation and "
        "decision_records are not yet populated. Unskip once a refreshed "
        "run captures the post-burndown contract."
    )
)
def test_every_iteration_has_decision_records(fixture):
    for it in fixture["iterations"]:
        assert it["decision_records"], (
            f"iter {it['iteration']}: decision_records empty — "
            f"producers did not fire"
        )


@pytest.mark.skip(
    reason="Same as above; gated on post-burndown re-run."
)
def test_journey_validation_populated_and_no_violations(fixture):
    for it in fixture["iterations"]:
        jv = it["journey_validation"]
        assert jv is not None, f"iter {it['iteration']}: journey_validation null"
        assert jv.get("violations", []) == [], (
            f"iter {it['iteration']}: journey violations: "
            f"{jv['violations']}"
        )
```

- [ ] **Step 3: Run the unblocked sanity check**

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/replay/test_replay_cycle9_zero_violations.py`

Expected: PASS for `test_fixture_loaded_with_five_iterations`; SKIP for the other two (gated on a post-burndown re-run).

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle9_raw.json packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py
git commit -m "test(replay): cycle 9 raw fixture intake

Captures run 1e855111-b463-4556-9b30-8cd32f78ebcb verbatim. The two
post-burndown contract tests are gated on a refresh re-run after T1-T12
land."
```

---

## Task 14: Re-run + verify (process)

This task is not code — it's the verification gate.

- [ ] **Step 1: Deploy the burndown bundle**

```bash
cd packages/genie-space-optimizer
databricks bundle deploy --target dev
```

- [ ] **Step 2: Trigger one optimization run for the airline space**

Use the same job + params that produced run `1e855111-b463-4556-9b30-8cd32f78ebcb`.

- [ ] **Step 3: Inspect the notebook-exit JSON**

```bash
databricks jobs get-run-output <new_run_id>
```

Expected (per T12 manifest): `per_iteration_decision_counts` is non-zero on every iteration that hit a gate; `no_decision_record_reasons` is empty (or `no_ag_attempted` only for baseline iterations); `per_iteration_journey_violations` is all zeros.

- [ ] **Step 4: Extract the new replay fixture**

From the new run's stderr, extract the `===PHASE_A_REPLAY_FIXTURE_JSON===` block and replace `airline_real_v1_cycle9_raw.json` with the new content. Update the fixture id in the JSON body to reflect the new run id.

- [ ] **Step 5: Unskip the post-burndown contract tests**

In `packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py`, remove the two `@pytest.mark.skip(...)` decorators.

Run: `cd packages/genie-space-optimizer && pytest -xvs tests/replay/test_replay_cycle9_zero_violations.py`

Expected: PASS for all three tests.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle9_raw.json packages/genie-space-optimizer/tests/replay/test_replay_cycle9_zero_violations.py
git commit -m "test(replay): cycle 9 burndown verified — fixtures refreshed, contracts unskipped"
```

---

## Self-Review

**1. Spec coverage (2026-05-03 status):**
| Burndown item from analysis + GPT | Task | Status |
|---|---|---|
| Buffered AGs silently discarded on dead-on-arrival | T1 | TODO |
| Buffered AGs silently discarded on pre-AG snapshot failure | T2 | TODO |
| Buffered AGs silently discarded on applier rejection | T3 | TODO |
| Empty signature `()` cached as "tried" → infinite re-run | T4 | TODO |
| No `forbid_tables` feedback to strategist after blast-radius drop | T5 | TODO |
| `decision_records: []` because no blast-radius producer | T6 | TODO (use `decision_emitters.py`, RCA-grounding fields) |
| `decision_records: []` because no dead-on-arrival producer | T7 | Partial — ACCEPTANCE_DECIDED already covers it; T7's added value is per-signature `PATCH_SKIPPED` granularity |
| `compute_scoreboard` not wired into harness | T8 | TODO |
| `SEED_CATALOG` missing four cycle-9 patterns | T9 | TODO |
| Empty `_decision_records` indistinguishable from capture failure | T10 | **DONE (postmortem follow-up)** — `phase_b_no_records_marker` + `NoRecordsReason` enum shipped |
| `proposal_direction_inversion` (P002 contradicts counterfactual) | T11 | TODO |
| Notebook-exit JSON missing decision counts and artifact paths | T12 | Half done — `loop_out["phase_b"]` manifest shipped; T12 still needs typed builders + finalize manifest + reconcile field names with existing `phase_b` shape |
| Cycle 9 fixture intake | T13 | TODO |
| End-to-end re-run verification | T14 | TODO |

Items intentionally **out of scope** (tracked elsewhere):
- `[UNBOUND_SQL_PARAMETER]` errors on `:ticket_number` / `:pnr_locator` / `:carrier_code` — pre-existing benchmark hygiene issue; preflight already warns and skips EXPLAIN. Scoped under preflight benchmark hygiene, not this plan.
- Track I primary trace-id loss (`50.0%` cumulative fallback rate) — Track I implemented the fallback; the primary cause is in the Genie SDK call path. Phase B follow-on.
- Strategist prompt-renderer pickup of `_strategist_constraints` — T5 plumbs the data; rendering belongs to the next strategist plan revision.
- Producer-side metadata stamping for `metric_native_currency` / `question_requested_currency` / `question_requests_exact_top_n` — separate strategist proposal plan; T11 (direction predicate) provides immediate cycle-9 mitigation.

**2. Placeholder scan:** No "TBD" / "implement later" / "fill in details" / "add appropriate error handling" / "Similar to Task N" / "handle edge cases" markers. Every step has either complete code or an exact command with expected output.

**3. Type consistency:**
- `_drain_buffered_action_groups` returns `tuple[list[dict], list[dict]]` and is called identically in T1, T2, T3.
- `_record_dead_on_arrival_signature` takes `seen: set[tuple[str, ...]]` and is called with `_dead_on_arrival_patch_signatures` (declared at `harness.py:10982` as `set[tuple[str, ...]]`).
- `blast_radius_decision_records` returns `list[DecisionRecord]`; `dead_on_arrival_decision_record` returns a single `DecisionRecord`. Both call sites use `.to_dict()` to land on the iteration snapshot, matching the existing patch-cap path at `harness.py:14674`.
- `proposal_direction_contradicts_counterfactual` signature `(patch: dict[str, Any]) -> bool` matches the existing `is_unrequested_currency_filter`, `is_rank_when_limit_n_required`, and `is_unrequested_is_not_null_filter` predicates in `sql_shape_quality.py`.
- `lever_loop_exit_manifest` and `finalize_exit_manifest` return `str` (JSON-encoded), so call sites stay `dbutils.notebook.exit(<manifest>(...))`.
- `StrategistConstraints.to_strategist_context()` returns `dict[str, dict[str, list[str]]]` consistently used in T5 step 5.
- `no_decision_records_marker` matches the signature pattern of `iteration_summary_marker` and `phase_b_marker` in `run_analysis_contract.py:60-110`.

