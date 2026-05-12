# RCO-2b — Contract Health Merge Gate Production Posture Flip Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flip the production posture so that (a) a `MERGE_GATE_BLOCKED` contract-health summary causes the lever-loop task to exit non-zero, and (b) `loop_invariants_strict()` defaults to True in production (the `GSO_LOOP_INVARIANTS_STRICT=0` setdefault override is removed from the lever-loop notebook).

**Architecture:** The harness already emits `GSO_CONTRACT_HEALTH_V1` (RCO-2a). RCO-2b refactors `_emit_contract_health_summary` to also return the built `ContractHealthSummary`, threads it onto `loop_out["contract_health_summary"]`, adds a `MergeGateBlockedError` + `enforce_merge_gate(loop_out)` helper in `optimization/contract_health.py`, and wires that helper into `jobs/run_lever_loop.py` so a blocked status raises (mark task failed, downstream `finalize` / `deploy` skip) while a `warn` / `healthy` status proceeds. Concurrently, the `_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")` line in the notebook is deleted — `loop_invariants_strict()` already returns `_flag_default_on("GSO_LOOP_INVARIANTS_STRICT")` (default True), and the override is what was pinning it off. The RCO-2a structural posture guard is inverted into an RCO-2b "posture-flipped" guard. Captured trial markers from the May-12 consolidating trial are promoted to golden-output fixtures.

**Tech Stack:** Python 3.11, `dataclasses(frozen=True)` for the existing summary type, stdlib `Exception` subclass for the blocked-status error, pytest for the test suite, no new dependencies.

---

## Scope Notes — Read Before Implementing

This plan is the **Phase B** half of the original RCO-2 ("Contract Health + Merge Gate Keystone"). The Phase A foundation (marker, parser, summary, classifier, three wired categories) shipped in `2026-05-12-rco-2a-contract-health-marker-and-summary-plan.md`. RCO-2b consumes the foundation; it adds no new evidence producers, no new invariants, no new markers.

**Named blocker — cleared 2026-05-12.** The deferral entry criterion was *"first trial run that emits the new `GSO_CONTRACT_HEALTH_V1` marker for ≥1 anchor, with the marker payload showing the expected `merge_gate_status` for that anchor's known failure mode."* The May-12 consolidating trial produced two real captures:

- `runid_analysis/31ecd96f-…/` (airline) — `merge_gate_status=warn` driven by `phase_h_listing_status=skipped + phase_h_validator_status=skipped`.
- `runid_analysis/ccf1d60d-…/` (7now) — `merge_gate_status=warn` driven by `phase_h_listing_status=skipped + phase_h_validator_status=skipped`.

Both payloads round-trip cleanly through `marker_parser.parse_markers(...)` and `ContractHealthSummary.from_json_dict(...)`. The trial captured the marker but did NOT capture a `MERGE_GATE_BLOCKED` payload (the F9-3b050ec5 blocked anchor was deferred to a future re-trial). RCO-2b therefore relies on the unit-fixture coverage of `MERGE_GATE_BLOCKED` shipped by RCO-2a (`tests/unit/fixtures/rco2a/blocked_anchor/`) plus the two captured `warn` payloads as live evidence that the marker emission pipeline is real.

**RCO-2b ships:**
- Refactor `_emit_contract_health_summary` to return the built `ContractHealthSummary` (still emits the marker as before).
- Thread the summary onto `loop_out["contract_health_summary"]` as a `dict[str, Any]` (JSON-roundtripped via `to_json_dict()`).
- A new `MergeGateBlockedError` exception in `optimization/contract_health.py`.
- A new pure `enforce_merge_gate(loop_out)` helper that raises `MergeGateBlockedError` iff `loop_out["contract_health_summary"]["merge_gate_status"] == "merge_gate_blocked"`.
- Wire `enforce_merge_gate(loop_out)` into `jobs/run_lever_loop.py` just before the final `dbutils.notebook.exit(...)` (so task values are published first; the raise marks the task failed; downstream `finalize` / `deploy` skip).
- Delete `_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")` from `jobs/run_lever_loop.py`.
- Invert the RCO-2a structural posture guard (`tests/unit/test_rco2a_strict_mode_posture_guard.py`) into a new RCO-2b "posture-flipped" guard (`tests/unit/test_rco2b_strict_mode_posture_flipped.py`).
- Promote both captured trial-anchor payloads to golden-output fixtures under `tests/unit/fixtures/rco2b/trial_airline_31ecd96f/` and `tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/`, with a parametrized parity test (`tests/unit/test_rco2b_trial_anchor_parity.py`).
- Updates to `2026-05-12-rco-2a-contract-health-policy.md` (Status block flips to Phase B landed), `2026-05-12-rco-2b-deferral.md` (Status flips to landed; entry criterion remains documented for history), `2026-05-10-roadmap-closeout.md` (RCO-2b row flips to done).

**RCO-2b explicitly does NOT ship:**
- Any new marker, invariant, or evidence producer.
- Any change to `MergeGateStatus`, `HIGH_TIER_INVARIANT_IDS`, or `build_contract_health_summary` itself. The classifier shipped in RCO-2a is byte-stable.
- A re-trial against F9-3b050ec5 / AIRLINE-clean. Those re-trials are gated on the two defect plans (`2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md` and `2026-05-12-defect-forbidden-ag-admission-enforcement.md`), not on RCO-2b.
- Any change to the `BUNDLE_ASSEMBLY_INCOMPLETE` vs `bundle_status=complete` contradiction observed in both trial captures (`docs/runid_analysis/.../postmortem.md` F8 for both runs). That contradiction is investigated by a separate micro-plan, NOT by RCO-2b. RCO-2b's only assertion about bundle_status is that the classifier returns whatever the existing `_classify_bundle` function returns.

### Strict Prerequisite (read before starting) — added by 2026-05-12-merge-gate-risk-mitigations-plan.md

RCO-2b flips the merge gate from observe-only to job-failing. Before the strict-mode flip is safe to land, all four of the following must be true:

1. **W1 (Bundle Status Wiring Fix) — landed.** `_emit_contract_health_summary` reads bundle-assembly payloads post-Phase-H. Live-code evidence: `harness.py:26636` call site, comment "relocated here from the convergence try/except". Captured in `docs/2026-05-12-bundle-status-wiring-fix-plan.md`.
2. **W2 (Run-End Replay Validation) — landed.** `_run_end_replay_validation` is populated by `lever_loop_replay.run_replay` at run end. Live-code evidence: `harness.py:25958`. Captured in `docs/2026-05-12-run-end-replay-validation-plan.md`.
3. **W3 (`_invariant_violations` Run Accumulator) — landed via Task 2 of `docs/2026-05-12-merge-gate-risk-mitigations-plan.md`.** Without W3, the HIGH-tier merge-gate input is dead: `MergeGateStatus.MERGE_GATE_BLOCKED` cannot fire via the `bool(high)` branch at `contract_health.py:181`. RCO-2b's `enforce_merge_gate` would block on nothing HIGH-tier — under-blocking on partial evidence. **Verification command:** `pytest packages/genie-space-optimizer/tests/integration/test_contract_health_high_tier_violations_populated.py -v` returns PASS (the harness-wiring grep-guard is unskipped; the full end-to-end test gates on a runid_analysis fixture).
4. **Defect 3 corpus validation — landed via Task 3 of `docs/2026-05-12-merge-gate-risk-mitigations-plan.md`.** The corpus replay regression test asserts every in-tree replay fixture is clean of `clustered → soft_signal` under the Defect-3 strict producer. Once W3 is wired and I12 is the canonical HIGH-tier replay-validity invariant, any unvalidated fixture would over-block RCO-2b's canary on legacy producer output. **Verification command:** `pytest packages/genie-space-optimizer/tests/integration/test_replay_corpus_i12_green.py -v` returns PASS for every parametrised case.

If any of (1)–(4) is missing, **do not start** the RCO-2b strict-mode flip. The two defect plans previously listed (`2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md`, `2026-05-12-defect-forbidden-ag-admission-enforcement.md`) remain prerequisites for the canary re-trial against F9-3b050ec5 / AIRLINE-clean; they are not prerequisites for the strict-mode flip itself.

---

## File Structure

**Modified files:**
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py` — add `MergeGateBlockedError` and `enforce_merge_gate(loop_out)` pure helpers.
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` — refactor `_emit_contract_health_summary` to return the summary (still emits the marker), capture the return value at the call site (line 25731), and thread it into `_loop_out_base` at line 26260 as `"contract_health_summary"`.
- `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py` — (a) delete the `_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")` line at 327, (b) add an `enforce_merge_gate(loop_out)` call between task-values publishing and `dbutils.notebook.exit(...)` at line 642.
- `packages/genie-space-optimizer/docs/2026-05-12-rco-2a-contract-health-policy.md` — flip Status block to "Phase B (RCO-2b) landed 2026-05-13".
- `packages/genie-space-optimizer/docs/2026-05-12-rco-2b-deferral.md` — flip Status block to "✅ landed 2026-05-13"; preserve entry criterion + named-blocker history.
- `packages/genie-space-optimizer/docs/2026-05-10-roadmap-closeout.md` — RCO-2b row flips from "deferred" to "✅ landed 2026-05-13"; remove RCO-2b from the deferred-RCO list at the top.

**New files:**
- `packages/genie-space-optimizer/tests/unit/test_rco2b_merge_gate_error_type.py`
- `packages/genie-space-optimizer/tests/unit/test_rco2b_enforce_merge_gate_helper.py`
- `packages/genie-space-optimizer/tests/unit/test_rco2b_emit_returns_summary.py`
- `packages/genie-space-optimizer/tests/unit/test_rco2b_loop_out_carries_contract_health.py`
- `packages/genie-space-optimizer/tests/unit/test_rco2b_strict_mode_posture_flipped.py`
- `packages/genie-space-optimizer/tests/unit/test_rco2b_trial_anchor_parity.py`
- `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/input.json`
- `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/expected_output.json`
- `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/input.json`
- `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/expected_output.json`

**Deleted files:**
- `packages/genie-space-optimizer/tests/unit/test_rco2a_strict_mode_posture_guard.py` — replaced by `test_rco2b_strict_mode_posture_flipped.py`. (The RCO-2a guard's whole purpose was to lock in the unflipped posture; deleting it is correct.)

---

## Task 1: MergeGateBlockedError exception type

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_rco2b_merge_gate_error_type.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_merge_gate_error_type.py`:

```python
"""RCO-2b — MergeGateBlockedError type guard."""
from __future__ import annotations


def test_merge_gate_blocked_error_is_runtime_exception() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateBlockedError,
    )
    assert issubclass(MergeGateBlockedError, Exception)
    err = MergeGateBlockedError(
        merge_gate_status="merge_gate_blocked",
        high_tier_violation_count=3,
        optimization_run_id="run-abc",
    )
    assert str(err) == (
        "merge_gate_status=merge_gate_blocked "
        "high_tier_violations=3 "
        "optimization_run_id=run-abc"
    )
    assert err.merge_gate_status == "merge_gate_blocked"
    assert err.high_tier_violation_count == 3
    assert err.optimization_run_id == "run-abc"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_merge_gate_error_type.py -v`

Expected: FAIL with `ImportError: cannot import name 'MergeGateBlockedError' from 'genie_space_optimizer.optimization.contract_health'`.

- [ ] **Step 3: Add the exception class**

Append to `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py` (after `build_contract_health_summary` at line 211):

```python
class MergeGateBlockedError(Exception):
    """RCO-2b — raised by ``enforce_merge_gate`` when the contract-health
    summary reports ``merge_gate_blocked``.

    Carries the structured fields a postmortem analyzer cares about
    (status, HIGH-tier violation count, optimization run id) so the
    surfaced error message in Databricks job-run logs is self-describing
    without needing to re-parse stdout.
    """

    def __init__(
        self,
        *,
        merge_gate_status: str,
        high_tier_violation_count: int,
        optimization_run_id: str,
    ) -> None:
        self.merge_gate_status = str(merge_gate_status)
        self.high_tier_violation_count = int(high_tier_violation_count)
        self.optimization_run_id = str(optimization_run_id)
        super().__init__(
            f"merge_gate_status={self.merge_gate_status} "
            f"high_tier_violations={self.high_tier_violation_count} "
            f"optimization_run_id={self.optimization_run_id}"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_merge_gate_error_type.py -v`

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py packages/genie-space-optimizer/tests/unit/test_rco2b_merge_gate_error_type.py
git commit -m "feat(rco-2b): add MergeGateBlockedError exception type"
```

---

## Task 2: enforce_merge_gate pure helper

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_rco2b_enforce_merge_gate_helper.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_enforce_merge_gate_helper.py`:

```python
"""RCO-2b — enforce_merge_gate pure-helper behavior."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.contract_health import (
    MergeGateBlockedError,
    enforce_merge_gate,
)


def test_healthy_status_does_not_raise() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "high_tier_violations": [],
            "medium_tier_violations": [],
            "optimization_run_id": "run-healthy",
            "phase_h_listing_status": "ok",
            "phase_h_validator_status": "ok",
            "bundle_status": "complete",
            "replay_is_valid": True,
            "replay_violation_count": 0,
        },
    }
    enforce_merge_gate(loop_out)  # no raise


def test_warn_status_does_not_raise() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "warn",
            "high_tier_violations": [],
            "medium_tier_violations": [
                {"invariant_id": "I3", "title": "stale_evidence"},
            ],
            "optimization_run_id": "run-warn",
            "phase_h_listing_status": "skipped",
            "phase_h_validator_status": "skipped",
            "bundle_status": "complete",
            "replay_is_valid": True,
            "replay_violation_count": 0,
        },
    }
    enforce_merge_gate(loop_out)  # no raise


def test_blocked_status_raises_with_payload() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "merge_gate_blocked",
            "high_tier_violations": [
                {"invariant_id": "I12", "title": "replay_validity_violated"},
                {"invariant_id": "I12", "title": "replay_validity_violated"},
            ],
            "medium_tier_violations": [],
            "optimization_run_id": "run-blocked",
            "phase_h_listing_status": "ok",
            "phase_h_validator_status": "ok",
            "bundle_status": "complete",
            "replay_is_valid": False,
            "replay_violation_count": 25,
        },
    }
    with pytest.raises(MergeGateBlockedError) as excinfo:
        enforce_merge_gate(loop_out)
    err = excinfo.value
    assert err.merge_gate_status == "merge_gate_blocked"
    assert err.high_tier_violation_count == 2
    assert err.optimization_run_id == "run-blocked"


def test_missing_contract_health_does_not_raise() -> None:
    """Defensive: if the harness path that builds the summary failed
    silently (e.g. RCO-2a's try/except swallowed an exception), the
    notebook MUST continue. RCO-2b only blocks on a known-blocked
    payload; absence is treated as ``warn`` upstream and not enforced
    here."""
    enforce_merge_gate({})
    enforce_merge_gate({"contract_health_summary": None})
    enforce_merge_gate({"contract_health_summary": {}})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_enforce_merge_gate_helper.py -v`

Expected: FAIL with `ImportError: cannot import name 'enforce_merge_gate' from 'genie_space_optimizer.optimization.contract_health'`.

- [ ] **Step 3: Add the helper**

Append to `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py` (after `MergeGateBlockedError`):

```python
def enforce_merge_gate(loop_out: Mapping[str, Any]) -> None:
    """RCO-2b — raise ``MergeGateBlockedError`` iff the lever-loop's
    contract-health summary reports ``merge_gate_blocked``.

    Called by ``jobs/run_lever_loop.py`` between task-values publishing
    and ``dbutils.notebook.exit(...)``. Task values are published first
    so postmortem tooling can read the failing run's debug payload;
    the raise marks the Databricks task as failed so downstream
    ``finalize`` / ``deploy`` tasks skip.

    Missing / ``None`` / empty ``contract_health_summary`` is a no-op:
    RCO-2a's emit path is fail-soft (swallows all exceptions). RCO-2b
    only enforces on a known-blocked payload, never on absence — a
    silently-skipped emit must not block the run.
    """
    payload = (loop_out or {}).get("contract_health_summary")
    if not payload:
        return
    if not isinstance(payload, Mapping):
        return
    status = str(payload.get("merge_gate_status") or "")
    if status != MergeGateStatus.MERGE_GATE_BLOCKED.value:
        return
    high_tier = payload.get("high_tier_violations") or ()
    raise MergeGateBlockedError(
        merge_gate_status=status,
        high_tier_violation_count=len(list(high_tier)),
        optimization_run_id=str(payload.get("optimization_run_id") or ""),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_enforce_merge_gate_helper.py -v`

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/contract_health.py packages/genie-space-optimizer/tests/unit/test_rco2b_enforce_merge_gate_helper.py
git commit -m "feat(rco-2b): add enforce_merge_gate pure helper"
```

---

## Task 3: _emit_contract_health_summary returns the built summary

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:914-955`
- Test: `packages/genie-space-optimizer/tests/unit/test_rco2b_emit_returns_summary.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_emit_returns_summary.py`:

```python
"""RCO-2b — _emit_contract_health_summary returns the built summary.

This is the surgical refactor that lets the harness thread the typed
summary into ``loop_out`` (Task 4) instead of forcing the lever-loop
notebook to re-parse stdout.
"""
from __future__ import annotations


def test_emit_returns_contract_health_summary_when_flag_on(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "1")
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
    )
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )

    summary = _emit_contract_health_summary(
        optimization_run_id="run-emit-001",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert isinstance(summary, ContractHealthSummary)
    assert summary.optimization_run_id == "run-emit-001"
    # marker must still be printed to stdout (regression guard against
    # accidentally dropping the side-effect during the refactor)
    captured = capsys.readouterr()
    assert "GSO_CONTRACT_HEALTH_V1 " in captured.out


def test_emit_returns_none_when_flag_off(monkeypatch, capsys) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "0")
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )

    result = _emit_contract_health_summary(
        optimization_run_id="run-emit-off",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert result is None
    captured = capsys.readouterr()
    assert "GSO_CONTRACT_HEALTH_V1 " not in captured.out


def test_emit_returns_none_on_internal_exception(monkeypatch) -> None:
    """The emit path is intentionally fail-soft: any internal exception
    yields ``None``, not a raise. RCO-2b relies on Task 4 treating
    ``None`` as 'no enforcement signal' so a buggy emit cannot crash
    the lever loop."""
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "1")

    import genie_space_optimizer.optimization.contract_health as ch
    monkeypatch.setattr(
        ch,
        "build_contract_health_summary",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("simulated")),
    )

    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )
    result = _emit_contract_health_summary(
        optimization_run_id="run-emit-boom",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_emit_returns_summary.py -v`

Expected: FAIL — the existing `_emit_contract_health_summary` returns `None` unconditionally, so the first test fails on `isinstance(summary, ContractHealthSummary)`.

- [ ] **Step 3: Refactor _emit_contract_health_summary to return the summary**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, replace the function body at lines 914-955 with:

```python
def _emit_contract_health_summary(
    *,
    optimization_run_id: str,
    invariant_violations,
    phase_h_strict_validation,
    bundle_assembly_failed,
    bundle_assembly_incomplete,
    replay_validation,
):
    """RCO-2a — emit ``GSO_CONTRACT_HEALTH_V1`` at end-of-run and
    return the built ``ContractHealthSummary`` for in-process consumers
    (Task 4 threads it onto ``loop_out``).

    Pure I/O wrapper: builds the summary via the pure module, prints
    the marker line, returns the summary. Swallows all exceptions
    silently and returns ``None`` — a bug here must never break the
    end-of-run path. RCO-2b's ``enforce_merge_gate`` treats ``None``
    as 'no enforcement signal'.
    """
    try:
        from genie_space_optimizer.common.config import (
            gso_contract_health_summary_enabled,
        )
        if not gso_contract_health_summary_enabled():
            return None
        from genie_space_optimizer.optimization.contract_health import (
            build_contract_health_summary,
        )
        from genie_space_optimizer.optimization.run_analysis_contract import (
            contract_health_summary_marker,
        )
        summary = build_contract_health_summary(
            optimization_run_id=optimization_run_id,
            invariant_violations=invariant_violations or (),
            phase_h_strict_validation=phase_h_strict_validation,
            bundle_assembly_failed=bundle_assembly_failed or (),
            bundle_assembly_incomplete=bundle_assembly_incomplete,
            replay_validation=replay_validation,
        )
        print(contract_health_summary_marker(summary))
        return summary
    except Exception:
        logger.debug(
            "RCO-2a contract-health summary emission skipped",
            exc_info=True,
        )
        return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_emit_returns_summary.py -v`

Expected: 3 passed.

- [ ] **Step 5: Run the RCO-2a harness emission test to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2a_harness_emits_contract_health_marker.py -v`

Expected: all passing (the RCO-2a tests only assert the marker prints; the new return value is additive).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_rco2b_emit_returns_summary.py
git commit -m "refactor(rco-2b): _emit_contract_health_summary returns ContractHealthSummary"
```

---

## Task 4: Thread contract_health_summary onto loop_out

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:25731-25744, 26260-26302`
- Test: `packages/genie-space-optimizer/tests/unit/test_rco2b_loop_out_carries_contract_health.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_loop_out_carries_contract_health.py`:

```python
"""RCO-2b — loop_out carries the typed contract_health_summary so the
lever-loop notebook can ``enforce_merge_gate(loop_out)`` without
re-parsing stdout.
"""
from __future__ import annotations

import pathlib
import re


def test_harness_assigns_contract_health_summary_local() -> None:
    """Source-level guard: the harness must capture the
    ``_emit_contract_health_summary`` return value into a local named
    ``_contract_health_summary``.

    Without this, Step 3's loop_out_base entry is unreachable.
    """
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert re.search(
        r"_contract_health_summary\s*=\s*_emit_contract_health_summary\(",
        src,
    ), (
        "harness.py must capture _emit_contract_health_summary's "
        "return value into a local named _contract_health_summary"
    )


def test_loop_out_base_carries_contract_health_summary_key() -> None:
    """Source-level guard: ``_loop_out_base`` must declare the
    ``contract_health_summary`` key projected from
    ``_contract_health_summary.to_json_dict() if _contract_health_summary else None``.
    """
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert '"contract_health_summary"' in src, (
        "harness.py must declare a 'contract_health_summary' key on "
        "_loop_out_base"
    )
    assert (
        "_contract_health_summary.to_json_dict()"
        in src
    ), (
        "harness.py must project the typed summary through "
        "to_json_dict() so loop_out carries a plain dict (JSON-safe "
        "for the dbutils.notebook.exit task-values round trip)"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_loop_out_carries_contract_health.py -v`

Expected: FAIL with "harness.py must capture …" — the harness currently calls `_emit_contract_health_summary(...)` without assigning the return value.

- [ ] **Step 3: Capture the summary at the call site**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` at line 25731, change:

```python
        _emit_contract_health_summary(
            optimization_run_id=run_id,
            invariant_violations=locals().get("_invariant_violations") or (),
            phase_h_strict_validation=locals().get(
                "_phase_h_marker_payload"
            ),
            bundle_assembly_failed=tuple(
                locals().get("_bundle_assembly_failed_payloads") or ()
            ),
            bundle_assembly_incomplete=locals().get(
                "_bundle_assembly_incomplete_payloads"
            ),
            replay_validation=locals().get("_run_end_replay_validation"),
        )
```

to:

```python
        # RCO-2b: capture the built summary so loop_out can carry it
        # and the lever-loop notebook can call enforce_merge_gate(...)
        # without re-parsing stdout.
        _contract_health_summary = _emit_contract_health_summary(
            optimization_run_id=run_id,
            invariant_violations=locals().get("_invariant_violations") or (),
            phase_h_strict_validation=locals().get(
                "_phase_h_marker_payload"
            ),
            bundle_assembly_failed=tuple(
                locals().get("_bundle_assembly_failed_payloads") or ()
            ),
            bundle_assembly_incomplete=locals().get(
                "_bundle_assembly_incomplete_payloads"
            ),
            replay_validation=locals().get("_run_end_replay_validation"),
        )
```

- [ ] **Step 4: Initialize the variable defensively above the try block**

The end-of-run emission is inside a `try` block (line 25685) whose `except` (line 25745) swallows everything. If the try raises before reaching `_emit_contract_health_summary`, `_contract_health_summary` is undefined. Add this line immediately before the `try:` at line 25685:

```python
    # RCO-2b: default to None so the variable is defined even if the
    # try block raises before the emission call. enforce_merge_gate
    # treats None as 'no enforcement signal'.
    _contract_health_summary = None
    try:
        # PR-B2: project plateau / divergence break state onto a typed
        # marker reason so the GSO_CONVERGENCE_V1 reader sees the same
        # vocabulary as the LEVER LOOP — TERMINATION print above.
```

(The added line is the comment + assignment above the existing `try:`.)

- [ ] **Step 5: Thread the summary into _loop_out_base**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` at line 26260's `_loop_out_base = {...}` dict literal, add the following key/value pair immediately before the closing `}` at line 26302 (i.e. on the line before `"phase_h_upload_status": _phase_h_upload_status,`):

```python
        # RCO-2b: the typed contract-health summary, projected through
        # to_json_dict() so the task-values JSON round trip is safe.
        # ``enforce_merge_gate`` consumes this in
        # ``jobs/run_lever_loop.py``. ``None`` when the RCO-2a emit
        # path returned None (flag off, exception swallowed, or
        # build_contract_health_summary raised).
        "contract_health_summary": (
            _contract_health_summary.to_json_dict()
            if _contract_health_summary is not None
            else None
        ),
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_loop_out_carries_contract_health.py -v`

Expected: 2 passed.

- [ ] **Step 7: Run the full RCO-2a harness suite to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2a_harness_emits_contract_health_marker.py tests/unit/test_rco2a_harness_wiring_grep_guard.py -v`

Expected: all passing.

- [ ] **Step 8: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_rco2b_loop_out_carries_contract_health.py
git commit -m "feat(rco-2b): thread contract_health_summary onto loop_out"
```

---

## Task 5: Wire enforce_merge_gate into run_lever_loop.py

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py:642`
- Test: `packages/genie-space-optimizer/tests/unit/test_rco2b_run_lever_loop_calls_enforce.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_run_lever_loop_calls_enforce.py`:

```python
"""RCO-2b — run_lever_loop.py wires enforce_merge_gate into the
end-of-task path. Source-level structural guard."""
from __future__ import annotations

import pathlib


JOB_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/jobs/run_lever_loop.py"
)


def test_run_lever_loop_imports_enforce_merge_gate() -> None:
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert (
        "from genie_space_optimizer.optimization.contract_health import"
        in src
    ), (
        "run_lever_loop.py must import from "
        "genie_space_optimizer.optimization.contract_health"
    )
    assert "enforce_merge_gate" in src, (
        "run_lever_loop.py must reference enforce_merge_gate"
    )


def test_enforce_merge_gate_is_called_before_notebook_exit() -> None:
    """The call must precede the final ``dbutils.notebook.exit(...)``
    so task values are published before the raise, but the raise
    actually marks the task failed."""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    enforce_pos = src.find("enforce_merge_gate(loop_out)")
    assert enforce_pos > 0, (
        "run_lever_loop.py must call enforce_merge_gate(loop_out)"
    )
    final_exit_pos = src.rfind(
        "dbutils.notebook.exit(json.dumps(debug_info, default=str))"
    )
    assert final_exit_pos > 0, (
        "run_lever_loop.py must still have a final notebook.exit"
    )
    assert enforce_pos < final_exit_pos, (
        "enforce_merge_gate(loop_out) must be invoked BEFORE the final "
        "dbutils.notebook.exit(...) — otherwise the exit short-circuits "
        "the raise and the task returns success on blocked status"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_run_lever_loop_calls_enforce.py -v`

Expected: FAIL — `enforce_merge_gate` is not yet referenced in `run_lever_loop.py`.

- [ ] **Step 3: Add the import and the call**

In `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py`, find the existing import block near line 50-90 (the imports section) and add an import alongside the other `genie_space_optimizer.optimization.*` imports:

```python
from genie_space_optimizer.optimization.contract_health import (
    enforce_merge_gate,
)
```

Then, in the same file, find the final block at line 635-643:

```python
_log(
    "Task values published",
    accuracy=loop_out["accuracy"],
    model_id=loop_out["model_id"],
    iteration_counter=loop_out["iteration_counter"],
    debug_info=debug_info,
)
_banner("Task 4 Completed")
dbutils.notebook.exit(json.dumps(debug_info, default=str))
```

Change it to:

```python
_log(
    "Task values published",
    accuracy=loop_out["accuracy"],
    model_id=loop_out["model_id"],
    iteration_counter=loop_out["iteration_counter"],
    debug_info=debug_info,
)

# RCO-2b — production posture flip. Task values are published above
# so the failing run's debug payload survives for postmortem tooling;
# enforce_merge_gate raises MergeGateBlockedError if the contract-
# health summary reports merge_gate_blocked, which marks the
# Databricks task failed and causes downstream finalize/deploy to
# skip. Healthy / warn statuses fall through to the notebook.exit.
enforce_merge_gate(loop_out)

_banner("Task 4 Completed")
dbutils.notebook.exit(json.dumps(debug_info, default=str))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_run_lever_loop_calls_enforce.py -v`

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py packages/genie-space-optimizer/tests/unit/test_rco2b_run_lever_loop_calls_enforce.py
git commit -m "feat(rco-2b): wire enforce_merge_gate into lever-loop task exit"
```

---

## Task 6: Remove the GSO_LOOP_INVARIANTS_STRICT setdefault override

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py:323-327`

Note: the structural guard test that asserts this line is GONE lives in Task 7 (the inverted RCO-2b posture guard). Task 6 is the source change; Task 7 is the test that locks it in.

- [ ] **Step 1: Inspect the existing RCO-2a guard test to understand what NOT to break**

Run: `cat packages/genie-space-optimizer/tests/unit/test_rco2a_strict_mode_posture_guard.py`

Note that line 24 asserts the presence of `'setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")'`. Task 7 will replace this guard. For now, run the guard to confirm it currently passes:

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2a_strict_mode_posture_guard.py -v`

Expected: 3 passed.

- [ ] **Step 2: Delete the override in run_lever_loop.py**

In `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py`, find the block at lines 323-327:

```python
# Cycle 11 — production defaults to warn-and-degrade for the loop
# invariant suite (typed INVARIANT_VIOLATION records, no AssertionError
# raise). CI / replay can override by setting
# GSO_LOOP_INVARIANTS_STRICT=1 explicitly.
_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")
```

Replace it with:

```python
# RCO-2b — production posture flipped 2026-05-13.
#
# Cycle 11 originally pinned ``GSO_LOOP_INVARIANTS_STRICT=0`` here to
# default warn-and-degrade. With the contract-health merge gate
# enforced (see ``enforce_merge_gate`` above), strict mode is now the
# production posture: an invariant violation raises in-loop and the
# merge gate blocks at end-of-run.
#
# ``loop_invariants_strict()`` already returns ``_flag_default_on(...)``
# (default True); removing this setdefault is what flips production.
# Emergency rollback: set ``GSO_LOOP_INVARIANTS_STRICT=0`` in the job
# config (the helper still honors a falsy explicit override).
```

(The comment block stays as historical context; the `setdefault(...)` line is deleted.)

- [ ] **Step 3: Confirm the RCO-2a guard now FAILS**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2a_strict_mode_posture_guard.py::test_run_lever_loop_still_pins_invariants_strict_to_off -v`

Expected: FAIL — the assertion `'setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")' in src` no longer holds. This is intentional; Task 7 deletes this guard.

- [ ] **Step 4: Commit (do NOT run the RCO-2a guard suite — Task 7 deletes it)**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py
git commit -m "feat(rco-2b): remove GSO_LOOP_INVARIANTS_STRICT=0 production override"
```

---

## Task 7: Invert the structural posture guard

**Files:**
- Delete: `packages/genie-space-optimizer/tests/unit/test_rco2a_strict_mode_posture_guard.py`
- Create: `packages/genie-space-optimizer/tests/unit/test_rco2b_strict_mode_posture_flipped.py`

- [ ] **Step 1: Write the new guard**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_strict_mode_posture_flipped.py`:

```python
"""RCO-2b — structural guard that the production posture has been
flipped.

Replaces ``test_rco2a_strict_mode_posture_guard.py`` (which locked in
the unflipped posture). This guard asserts the three positive symptoms
of the flip:

  1. ``run_lever_loop.py`` no longer pins
     ``GSO_LOOP_INVARIANTS_STRICT`` to ``"0"`` via ``setdefault``.
  2. ``run_lever_loop.py`` references ``enforce_merge_gate`` and calls
     it before the final ``dbutils.notebook.exit(...)``.
  3. ``loop_invariants_strict()`` still reads via ``_flag_default_on``
     (its accessor shape did not change; only the runtime override
     went away).

When all three hold, RCO-2b's production posture is live.
"""
from __future__ import annotations

import pathlib


JOB_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/jobs/run_lever_loop.py"
)
CONFIG_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/common/config.py"
)


def test_run_lever_loop_no_longer_pins_invariants_strict_to_off() -> None:
    """The ``setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")`` override
    must be removed. Reintroducing it would silently revert RCO-2b."""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert 'setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")' not in src, (
        "RCO-2b removed this override — readding it reverts the "
        "production posture flip. If a rollback is needed, set "
        "GSO_LOOP_INVARIANTS_STRICT=0 in the job widget instead."
    )


def test_run_lever_loop_enforces_merge_gate_before_exit() -> None:
    """The merge-gate enforcement must precede the final notebook
    exit. (Same assertion shape as
    test_rco2b_run_lever_loop_calls_enforce.py; duplicated here as a
    structural posture guard so deleting the wiring trips a guard
    even if the dedicated wiring test is also removed.)"""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert "enforce_merge_gate(loop_out)" in src, (
        "run_lever_loop.py must call enforce_merge_gate(loop_out) — "
        "this is the merge-gate production-posture entry point"
    )
    enforce_pos = src.find("enforce_merge_gate(loop_out)")
    final_exit_pos = src.rfind(
        "dbutils.notebook.exit(json.dumps(debug_info, default=str))"
    )
    assert enforce_pos < final_exit_pos, (
        "enforce_merge_gate(loop_out) must come before the final "
        "dbutils.notebook.exit(...)"
    )


def test_loop_invariants_strict_accessor_unchanged() -> None:
    """The accessor's shape must still read via ``_flag_default_on``.
    RCO-2b removed the runtime override, not the accessor."""
    src = CONFIG_SRC_PATH.read_text(encoding="utf-8")
    assert "def loop_invariants_strict()" in src
    assert '_flag_default_on("GSO_LOOP_INVARIANTS_STRICT")' in src
```

- [ ] **Step 2: Delete the old RCO-2a posture guard**

Run: `git rm packages/genie-space-optimizer/tests/unit/test_rco2a_strict_mode_posture_guard.py`

- [ ] **Step 3: Run the new RCO-2b guard**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_strict_mode_posture_flipped.py -v`

Expected: 3 passed.

- [ ] **Step 4: Confirm the deletion is clean (no stale references)**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/ -k rco2a -v 2>&1 | tail -20`

Expected: all surviving RCO-2a tests pass; no test imports the deleted guard module. If a test file imports from the deleted guard, fix that file (this should not happen — the guard was self-contained).

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_rco2b_strict_mode_posture_flipped.py
git commit -m "test(rco-2b): replace RCO-2a posture guard with flipped guard"
```

---

## Task 8: Promote captured trial markers to byte-stable golden fixtures

**Files:**
- Create: `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/input.json`
- Create: `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/expected_output.json`
- Create: `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/input.json`
- Create: `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/expected_output.json`
- Create: `packages/genie-space-optimizer/tests/unit/test_rco2b_trial_anchor_parity.py`

**Background.** The RCO-2b deferral doc names "promote the trial marker to a fixture" as the first scope item. The May-12 consolidating trial captured two real `GSO_CONTRACT_HEALTH_V1` payloads (both `warn`, driven by `phase_h_listing_status=skipped + phase_h_validator_status=skipped`). The classifier inputs that produced those captured outputs are reconstructible deterministically from the payloads themselves:

- `high_tier_violations=[]` and `medium_tier_violations=[]` → `invariant_violations=[]`.
- `phase_h_listing_status="skipped" + phase_h_validator_status="skipped"` → `phase_h_strict_validation=None` (per `_classify_phase_h(None) == ("skipped", "skipped")`).
- `bundle_status="complete"` → `bundle_assembly_failed=[] + bundle_assembly_incomplete=None`.
- `replay_is_valid=True + replay_violation_count=0` → `replay_validation={"is_valid": true, "violation_count": 0}`.

The fixture asserts `build_contract_health_summary` produces the captured payload byte-for-byte given those reconstructed inputs. This is the byte-stable parity the deferral doc requested.

- [ ] **Step 1: Create the trial_airline_31ecd96f input fixture**

Create `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/input.json`:

```json
{
  "optimization_run_id": "31ecd96f-5d56-4b5a-af8e-38e9e5c549af",
  "invariant_violations": [],
  "phase_h_strict_validation": null,
  "bundle_assembly_failed": [],
  "bundle_assembly_incomplete": null,
  "replay_validation": {
    "is_valid": true,
    "violation_count": 0
  }
}
```

- [ ] **Step 2: Create the trial_airline_31ecd96f expected_output fixture**

Create `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/expected_output.json`:

```json
{
  "optimization_run_id": "31ecd96f-5d56-4b5a-af8e-38e9e5c549af",
  "merge_gate_status": "warn",
  "high_tier_violations": [],
  "medium_tier_violations": [],
  "phase_h_listing_status": "skipped",
  "phase_h_validator_status": "skipped",
  "bundle_status": "complete",
  "replay_is_valid": true,
  "replay_violation_count": 0
}
```

- [ ] **Step 3: Create the trial_seven_now_ccf1d60d input fixture**

Create `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/input.json`:

```json
{
  "optimization_run_id": "ccf1d60d-d686-467b-bafa-1640131b4393",
  "invariant_violations": [],
  "phase_h_strict_validation": null,
  "bundle_assembly_failed": [],
  "bundle_assembly_incomplete": null,
  "replay_validation": {
    "is_valid": true,
    "violation_count": 0
  }
}
```

- [ ] **Step 4: Create the trial_seven_now_ccf1d60d expected_output fixture**

Create `packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/expected_output.json`:

```json
{
  "optimization_run_id": "ccf1d60d-d686-467b-bafa-1640131b4393",
  "merge_gate_status": "warn",
  "high_tier_violations": [],
  "medium_tier_violations": [],
  "phase_h_listing_status": "skipped",
  "phase_h_validator_status": "skipped",
  "bundle_status": "complete",
  "replay_is_valid": true,
  "replay_violation_count": 0
}
```

- [ ] **Step 5: Write the parametrized parity test**

Create `packages/genie-space-optimizer/tests/unit/test_rco2b_trial_anchor_parity.py`:

```python
"""RCO-2b — captured-trial-anchor byte-stable parity.

The May-12 consolidating trial captured two real ``GSO_CONTRACT_HEALTH_V1``
payloads. This test asserts that ``build_contract_health_summary``
reproduces those payloads byte-for-byte from inputs reconstructed from
the captured stdout (see fixture README and the RCO-2b plan for the
reconstruction logic).

If this test fails, either:

  1. The classifier semantics drifted (regression). Investigate before
     accepting the new output as canonical.
  2. The captured payloads were re-captured against a new trial run
     with different inputs. Update both ``input.json`` and
     ``expected_output.json`` for the affected anchor.
"""
from __future__ import annotations

import json
import pathlib

import pytest


FIXTURE_ROOT = pathlib.Path(__file__).parent / "fixtures" / "rco2b"


def _list_trial_anchor_dirs():
    if not FIXTURE_ROOT.exists():
        return []
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


@pytest.mark.parametrize(
    "fixture_dir",
    _list_trial_anchor_dirs(),
    ids=lambda p: p.name,
)
def test_builder_matches_captured_trial_payload(
    fixture_dir: pathlib.Path,
) -> None:
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )
    summary = build_contract_health_summary(
        optimization_run_id=inp["optimization_run_id"],
        invariant_violations=inp["invariant_violations"],
        phase_h_strict_validation=inp.get("phase_h_strict_validation"),
        bundle_assembly_failed=tuple(inp.get("bundle_assembly_failed") or ()),
        bundle_assembly_incomplete=inp.get("bundle_assembly_incomplete"),
        replay_validation=inp.get("replay_validation"),
    )
    assert summary.to_json_dict() == expected
```

- [ ] **Step 6: Run the parity test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_rco2b_trial_anchor_parity.py -v`

Expected: 2 passed (`trial_airline_31ecd96f`, `trial_seven_now_ccf1d60d`).

- [ ] **Step 7: Confirm the actual captured stdout payload matches the fixture (cross-check)**

Run:

```bash
diff <(grep -o 'GSO_CONTRACT_HEALTH_V1 .*' packages/genie-space-optimizer/docs/runid_analysis/31ecd96f-5d56-4b5a-af8e-38e9e5c549af/evidence/lever_loop_latest_export_run_357881600282129_text.txt | head -1 | sed 's/^GSO_CONTRACT_HEALTH_V1 //' | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), sort_keys=True, indent=2))') <(python3 -c 'import json; print(json.dumps(json.load(open("packages/genie-space-optimizer/tests/unit/fixtures/rco2b/trial_airline_31ecd96f/expected_output.json")), sort_keys=True, indent=2))')
```

Expected: no output (the captured stdout payload and the expected_output fixture are JSON-equal).

If they differ: re-derive the input fixture from the captured stdout and regenerate the expected_output fixture by running `build_contract_health_summary(**reconstructed_inputs).to_json_dict()`. Do NOT silently edit `expected_output.json` to match — that defeats the parity guard.

- [ ] **Step 8: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/fixtures/rco2b/ packages/genie-space-optimizer/tests/unit/test_rco2b_trial_anchor_parity.py
git commit -m "test(rco-2b): promote captured trial markers to byte-stable fixtures"
```

---

## Task 9: Update RCO-2a policy + RCO-2b deferral docs

**Files:**
- Modify: `packages/genie-space-optimizer/docs/2026-05-12-rco-2a-contract-health-policy.md` (Status block)
- Modify: `packages/genie-space-optimizer/docs/2026-05-12-rco-2b-deferral.md` (Status block + named-blocker history)

- [ ] **Step 1: Flip the RCO-2a policy Status block**

In `packages/genie-space-optimizer/docs/2026-05-12-rco-2a-contract-health-policy.md`, replace lines 3-9:

```markdown
## Status

**Phase A (RCO-2a):** in-flight — ships marker, parser, summary, and
merge-gate categories. Production posture remains warn-and-degrade.

**Phase B (RCO-2b):** deferred — flips production posture once the first
trial run emits ``GSO_CONTRACT_HEALTH_V1`` for ≥1 anchor.
```

with:

```markdown
## Status

**Phase A (RCO-2a):** ✅ landed (May 12, 2026). Ships marker, parser,
summary, and merge-gate categories.

**Phase B (RCO-2b):** ✅ landed (May 13, 2026). The merge-gate
production posture is now enforced; ``GSO_LOOP_INVARIANTS_STRICT=0``
override is removed from the lever-loop notebook. See
``2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md``.
```

- [ ] **Step 2: Flip the RCO-2a/2b boundary section to reflect the flip**

In the same file, replace the section "## RCO-2a vs RCO-2b Boundary" (lines 36-47) with:

```markdown
## RCO-2a vs RCO-2b Boundary (historical)

RCO-2a wired the merge-gate categories (the classifier returns the
correct enum value for every input) but did NOT enforce them. The
production job returned success on ``MERGE_GATE_BLOCKED`` and
``loop_invariants_strict()`` was pinned to False via
``run_lever_loop.py:_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")``.

RCO-2b (landed 2026-05-13) flipped both:

  - The job exit code on ``MERGE_GATE_BLOCKED`` is now non-zero
    (``enforce_merge_gate(loop_out)`` raises ``MergeGateBlockedError``
    before the final ``dbutils.notebook.exit(...)``).
  - The ``setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")`` override is
    deleted, so ``loop_invariants_strict()`` returns its
    ``_flag_default_on`` default (True) in production.

Emergency rollback: set ``GSO_LOOP_INVARIANTS_STRICT=0`` in the job
widget (the helper still honors a falsy explicit override). The
merge-gate enforcement does not have a flag — to roll it back, revert
the ``enforce_merge_gate(loop_out)`` call in ``run_lever_loop.py``.
```

- [ ] **Step 3: Flip the RCO-2b deferral Status block**

In `packages/genie-space-optimizer/docs/2026-05-12-rco-2b-deferral.md`, replace lines 3-8:

```markdown
## Status

**Deferred.** The structural foundation lands in RCO-2a (see
``2026-05-12-rco-2a-contract-health-marker-and-summary-plan.md``).
RCO-2b ships the production posture flip once the named blocker
clears.
```

with:

```markdown
## Status

**✅ Landed (May 13, 2026).** The named blocker cleared on the May-12
consolidating trial (two captured ``GSO_CONTRACT_HEALTH_V1`` payloads;
see ``runid_analysis/31ecd96f-…`` and ``runid_analysis/ccf1d60d-…``).
The production posture flip shipped in
``2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md``.
The structural foundation landed in RCO-2a (see
``2026-05-12-rco-2a-contract-health-marker-and-summary-plan.md``).
```

- [ ] **Step 4: Append a "Disposition" section to the deferral doc**

In the same file, append a new section at the end (after the "Anchor Inventory for Trial" table):

```markdown
## Disposition (2026-05-13)

The named blocker cleared on the May-12 consolidating trial. Two
captured ``GSO_CONTRACT_HEALTH_V1`` payloads validated the marker
emission pipeline end-to-end:

| Captured anchor                                         | ``merge_gate_status`` | Driving evidence                                          |
|---------------------------------------------------------|------------------------|-----------------------------------------------------------|
| ``31ecd96f-5d56-4b5a-af8e-38e9e5c549af`` (airline)      | ``warn``               | ``phase_h_listing_status=skipped``, ``phase_h_validator_status=skipped`` |
| ``ccf1d60d-d686-467b-bafa-1640131b4393`` (7now)         | ``warn``               | ``phase_h_listing_status=skipped``, ``phase_h_validator_status=skipped`` |

The trial did NOT capture a ``MERGE_GATE_BLOCKED`` payload (the
F9-3b050ec5 blocked anchor was deferred to a future re-trial gated on
the two defect plans, ``2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md``
and ``2026-05-12-defect-forbidden-ag-admission-enforcement.md``). The
RCO-2a ``blocked_anchor`` fixture (``tests/unit/fixtures/rco2a/blocked_anchor/``)
provides the unit-fixture coverage of the blocked path.

Both captured payloads were promoted to byte-stable parity fixtures
under ``tests/unit/fixtures/rco2b/trial_airline_31ecd96f/`` and
``tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/``, asserted by
``tests/unit/test_rco2b_trial_anchor_parity.py``.
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/docs/2026-05-12-rco-2a-contract-health-policy.md packages/genie-space-optimizer/docs/2026-05-12-rco-2b-deferral.md
git commit -m "docs(rco-2b): flip policy + deferral status to landed"
```

---

## Task 10: Update roadmap-closeout to reflect RCO-2b landed

**Files:**
- Modify: `packages/genie-space-optimizer/docs/2026-05-10-roadmap-closeout.md`

- [ ] **Step 1: Find the RCO-2b status block**

Run: `grep -n 'RCO-2b' packages/genie-space-optimizer/docs/2026-05-10-roadmap-closeout.md | head -10`

Note the line numbers of the RCO-2b row(s) in the roadmap status table.

- [ ] **Step 2: Update the RCO-2b row**

In `packages/genie-space-optimizer/docs/2026-05-10-roadmap-closeout.md`, find every row or bullet that names RCO-2b as "deferred" / "in-flight" / "blocked" and replace the status field with "✅ landed (2026-05-13)" and add a pointer to `2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md`.

Example shape (apply to whichever cell or bullet currently carries the deferred status):

```markdown
| RCO-2b | Contract Health Merge Gate Production Posture Flip | ✅ landed (2026-05-13) | See `2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md` |
```

If the doc carries a "deferred backlog" callout that lists RCO-2b separately at the top, remove RCO-2b from that list.

- [ ] **Step 3: Add a brief Disposition pointer if the roadmap doc has a "Disposition" or "Trial-evidence" section**

If `2026-05-10-roadmap-closeout.md` already has a `## Disposition` or `## Trial Evidence` section (e.g. one appended for RCO-4b), add one bullet:

```markdown
- **RCO-2b (2026-05-13):** Production posture flipped. Merge gate
  raises ``MergeGateBlockedError`` on ``merge_gate_blocked``;
  ``GSO_LOOP_INVARIANTS_STRICT=0`` override removed from
  ``run_lever_loop.py``. Two captured trial payloads promoted to
  byte-stable fixtures under ``tests/unit/fixtures/rco2b/``.
```

If no such section exists, skip Step 3.

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/docs/2026-05-10-roadmap-closeout.md
git commit -m "docs(rco-2b): mark RCO-2b landed in roadmap-closeout"
```

---

## Task 11: Run the full RCO-2 test suite and the broader unit suite to confirm no regression

**Files:** (no edits)

- [ ] **Step 1: Run all RCO-2a + RCO-2b tests**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/ -k 'rco2a or rco2b' -v`

Expected: all passing. (RCO-2a count from the production-fixture run: 3 anchors. RCO-2b count: 11 new tests from this plan + 2 trial-anchor parametrizations.)

If any test fails, do NOT proceed. Fix the regression (most likely candidate: a stale assertion in an RCO-2a guard test that conflicted with the structural change). The plan does not introduce any RCO-2a-incompatible change; a failure here is a wiring bug introduced by an earlier task in this plan.

- [ ] **Step 2: Run the broader unit suite**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/ -x --maxfail=5 -q`

Expected: all passing, except the four pre-existing failures listed in the RCO-4b consolidating-trial preflight green-light log (`test_skill_parser_handoff` x2; `test_evidence_bundle_smoke`; `test_mlflow_smoke_one_iteration`). Those are unrelated to RCO-2b and pre-date this plan.

If a new failure appears, investigate before continuing — DO NOT mass-update assertions.

- [ ] **Step 3: Confirm import sanity of the lever-loop notebook**

Run:

```bash
cd packages/genie-space-optimizer && uv run python -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location(
    'run_lever_loop',
    'src/genie_space_optimizer/jobs/run_lever_loop.py',
)
print('module spec loaded OK:', spec is not None)
# We do not import the full module here — it executes notebook cells
# that require a Spark / dbutils context. The spec load is sufficient
# to catch syntax errors and import-time regressions.
"
```

Expected: `module spec loaded OK: True`. (A `SyntaxError` or `ImportError` at this stage indicates Task 5 or Task 6 introduced a typo.)

---

## Self-Review

I checked the spec against the plan:

**1. Spec coverage:**

| RCO-2b deferral scope item | Task |
|---|---|
| 1. Promote the trial marker to a fixture | Task 8 |
| 2. Flip the lever-loop task exit code | Tasks 1–5 (exception, helper, refactor, threading, wiring) |
| 3. Decide ``loop_invariants_strict()`` default | Task 6 (removes the override; default is already True via ``_flag_default_on``) |
| 4. Update the structural guard | Task 7 (inverts RCO-2a guard to RCO-2b "posture-flipped" guard) |
| Policy doc update | Task 9 |
| Deferral doc update | Task 9 |
| Roadmap-closeout update | Task 10 |
| Regression coverage | Task 11 |

All four scope items are covered. No orphan tasks.

**2. Placeholder scan:** No `TBD`, `TODO`, `implement later`, or "add appropriate error handling" patterns. Every step has either code or an exact command + expected output. The diff in Task 5 Step 3 and Task 6 Step 2 shows complete before/after blocks.

**3. Type consistency:**
- `MergeGateBlockedError` (Task 1) uses kwargs `merge_gate_status: str`, `high_tier_violation_count: int`, `optimization_run_id: str` — consumed identically in Task 2's `enforce_merge_gate` raise site.
- `enforce_merge_gate(loop_out: Mapping[str, Any]) -> None` (Task 2) consumes `loop_out["contract_health_summary"]` — same key as produced in Task 4's `_loop_out_base` dict.
- `_emit_contract_health_summary` (Task 3) returns `ContractHealthSummary | None` — captured into `_contract_health_summary` in Task 4 and projected via `.to_json_dict()` (the existing RCO-2a method).
- Test names match assertion strings (e.g. `test_enforce_merge_gate_is_called_before_notebook_exit` in Task 5; `enforce_merge_gate(loop_out)` is the exact string asserted, matching the exact wiring in Task 5 Step 3).
- Fixture directory names in Task 8 (`trial_airline_31ecd96f`, `trial_seven_now_ccf1d60d`) match the parametrized test's auto-discovery in `test_rco2b_trial_anchor_parity.py`.

**4. Cross-task ordering:**
- Task 6 deletes the override line that Task 7 then asserts is gone — correct order.
- Task 7 deletes the RCO-2a posture guard whose `test_run_lever_loop_still_pins_invariants_strict_to_off` Task 6 Step 3 deliberately broke — correct order.
- Task 5 introduces the `enforce_merge_gate` call before Task 6 removes the strict-mode override. Reverse order (remove override first, then wire enforcement) would briefly leave production in a half-flipped state if a deploy happened mid-plan, but since this plan is committed and deployed as a whole, the order chosen is fine and matches TDD discipline (wire the new behavior first, then remove the old guard).

No issues found.

---

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
