"""Trial 31 W31.3 — optimizer-invariant ↔ task-result consistency.

W30.5 surfaced the silent-failure bug: the lever_loop task reported
SUCCESS while the optimizer outcome was ``OPTIMIZER_INVARIANT_VIOLATION``
(guardrail ``OPTIMIZER_INVARIANT_VIOLATION_TASK_SUCCEEDED``), and the
deploy was not even blocked on it.

The fix has two halves, both pinned here:
1. ``compute_deploy_eligibility`` marks an invariant-violation outcome as
   ``optimizer_task_status="failed"`` + ``candidate_deploy_eligible=False``
   with a distinct ``deploy_skip_reason="invariant_violation"`` — WITHOUT
   disturbing the Trial 21 W9 contract that contract-health / no-candidate
   outcomes keep the task SUCCESS.
2. ``lever_loop_task_should_fail`` projects that onto a flag-gated
   raise decision the lever_loop task entrypoint consumes.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.harness import (
    compute_deploy_eligibility,
    deploy_eligibility_from_loop_out,
    lever_loop_task_should_fail,
)

_VIOLATION = "OPTIMIZER_INVARIANT_VIOLATION"


def test_invariant_violation_marks_task_failed_and_blocks_deploy():
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome=_VIOLATION,
    )
    assert v.optimizer_task_status == "failed"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "invariant_violation"


def test_invariant_violation_wins_over_contract_health():
    """An invariant violation is a genuine SM breakage — its task=failed
    signal must dominate the milder contract-health-blocked skip reason."""
    v = compute_deploy_eligibility(
        merge_gate_status="merge_gate_blocked",
        bundle_status="assembly_failed",
        run_outcome=_VIOLATION,
    )
    assert v.optimizer_task_status == "failed"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "invariant_violation"


@pytest.mark.parametrize(
    "outcome",
    [
        "OPTIMIZER_IMPROVED",
        "OPTIMIZER_TRIED_INSUFFICIENT_GAIN",
        "OPTIMIZER_NO_CANDIDATES",
        "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
    ],
)
def test_non_violation_outcomes_keep_task_success(outcome: str):
    """Trial 21 W9 contract preserved: only a genuine invariant violation
    fails the task; everything else stays SUCCESS."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome=outcome,
    )
    assert v.optimizer_task_status == "success"


def test_loop_out_projection_marks_failed():
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "bundle_status": "complete",
        },
        "optimizer_outcome": _VIOLATION,
    }
    v = deploy_eligibility_from_loop_out(loop_out)
    assert v.optimizer_task_status == "failed"
    assert v.candidate_deploy_eligible is False


def test_task_should_fail_true_on_violation_when_enabled():
    loop_out = {"optimizer_outcome": _VIOLATION}
    assert lever_loop_task_should_fail(loop_out, enabled=True) is True


def test_task_should_fail_false_when_flag_disabled():
    """GSO_TRIAL31_FAIL_ON_INVARIANT=0 → legacy always-SUCCESS posture."""
    loop_out = {"optimizer_outcome": _VIOLATION}
    assert lever_loop_task_should_fail(loop_out, enabled=False) is False


def test_task_should_fail_false_on_clean_run():
    loop_out = {"optimizer_outcome": "OPTIMIZER_IMPROVED"}
    assert lever_loop_task_should_fail(loop_out, enabled=True) is False


def test_task_should_fail_fail_soft_on_garbage_loop_out():
    """A malformed loop_out must never block the task (fail-soft)."""
    assert lever_loop_task_should_fail(None, enabled=True) is False
    assert lever_loop_task_should_fail({}, enabled=True) is False
