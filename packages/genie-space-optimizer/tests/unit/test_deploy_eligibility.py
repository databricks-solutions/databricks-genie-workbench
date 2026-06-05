"""Trial 21 W9 unit tests — compute_deploy_eligibility.

Pins the contract: a blocked merge gate or failed bundle assembly
makes the candidate ineligible to deploy without failing the optimizer
task. No-candidate outcomes block with a distinct skip reason so
postmortems can disambiguate "no work to ship" from "contract health
caught a regression".
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.harness import (
    DeployEligibilityVerdict,
    compute_deploy_eligibility,
)


def test_clean_run_is_deployable():
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
    )
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is True
    assert v.deploy_skip_reason == ""


def test_run_a_merge_gate_blocked_blocks_deploy():
    """Run A's exact contract-health inputs: merge_gate_blocked +
    assembly_failed → optimizer task succeeds, deploy is blocked."""
    v = compute_deploy_eligibility(
        merge_gate_status="merge_gate_blocked",
        bundle_status="assembly_failed",
        run_outcome="OPTIMIZER_TRIED_INSUFFICIENT_GAIN",
    )
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


@pytest.mark.parametrize("merge_value", ["merge_gate_blocked", "blocked"])
def test_merge_gate_blocked_alone_is_sufficient_to_block(merge_value: str):
    v = compute_deploy_eligibility(
        merge_gate_status=merge_value,
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
    )
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


@pytest.mark.parametrize("bundle_value", ["assembly_failed", "failed"])
def test_assembly_failed_alone_is_sufficient_to_block(bundle_value: str):
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status=bundle_value,
        run_outcome="OPTIMIZER_IMPROVED",
    )
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


@pytest.mark.parametrize(
    "outcome",
    [
        "OPTIMIZER_NO_CANDIDATES",
        "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
        "OPTIMIZER_STALLED_SAFE_NOOP",
        "OPTIMIZER_SKIPPED_INPUT_GAP",
    ],
)
def test_no_candidate_outcomes_block_with_distinct_skip_reason(
    outcome: str,
):
    """No-candidate outcomes do not deploy, but the skip reason is
    ``"no_candidate"`` (not ``"contract_health_blocked"``) so deploy
    logs can disambiguate."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome=outcome,
    )
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "no_candidate"


def test_optimizer_task_status_is_always_success():
    """The optimizer task NEVER reports failure based on contract
    health or deploy eligibility — the parent Databricks Job task
    stays SUCCESS so the workflow does not page on these signals."""
    for merge, bundle, outcome in [
        ("passed", "assembled", "OPTIMIZER_IMPROVED"),
        ("merge_gate_blocked", "assembled", "OPTIMIZER_IMPROVED"),
        ("passed", "assembly_failed", "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"),
        ("merge_gate_blocked", "assembly_failed", "OPTIMIZER_NO_CANDIDATES"),
    ]:
        v = compute_deploy_eligibility(
            merge_gate_status=merge,
            bundle_status=bundle,
            run_outcome=outcome,
        )
        assert v.optimizer_task_status == "success"
        assert isinstance(v, DeployEligibilityVerdict)


# ──────────────────────────────────────────────────────────────────────
# Trial 23 W10 — Phase H as a contract boundary.
#
# assembly_failed already blocks (Trial 21). W10 adds: a failed Phase H
# upload/render OR a stale scoreboard.json also makes the candidate
# non-deployable, gated by GSO_TRIAL23_PHASE_H_CONTRACT_GATE (default ON;
# =0 restores the legacy Trial 21/22 posture). The optimizer task stays
# SUCCESS — only the deploy gate sees the failure.
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def _w10_on(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "1")
    monkeypatch.setenv("GSO_TRIAL23_PHASE_H_CONTRACT_GATE", "1")


@pytest.fixture
def _w10_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL23_PHASE_H_CONTRACT_GATE", "0")


@pytest.mark.parametrize(
    "upload_status", ["upload_failed", "render_failed", "UPLOAD_FAILED"]
)
def test_w10_failed_phase_h_upload_blocks_deploy(_w10_on, upload_status):
    """A failed Phase H upload/render blocks deploy while the optimizer
    task stays SUCCESS."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
        phase_h_upload_status=upload_status,
    )
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


def test_w10_stale_scoreboard_blocks_deploy(_w10_on):
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
        scoreboard_stale=True,
    )
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


@pytest.mark.parametrize("upload_status", ["uploaded", "skipped_no_anchor", ""])
def test_w10_clean_or_benign_phase_h_upload_still_deploys(
    _w10_on, upload_status
):
    """A successful upload, a no-anchor replay skip, and an empty status
    are all deploy-eligible — W10 only blocks on hard failures."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
        phase_h_upload_status=upload_status,
        scoreboard_stale=False,
    )
    assert v.candidate_deploy_eligible is True
    assert v.deploy_skip_reason == ""


def test_w10_rollback_flag_off_ignores_phase_h_signals(_w10_off):
    """With the W10 flag off, a failed upload + stale scoreboard are
    ignored (legacy Trial 21/22 posture: deploy-eligible)."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
        phase_h_upload_status="upload_failed",
        scoreboard_stale=True,
    )
    assert v.candidate_deploy_eligible is True
    assert v.deploy_skip_reason == ""


def test_w10_master_flag_off_ignores_phase_h_signals(monkeypatch):
    """The Trial 23 master flag forces W10 off regardless of the
    sub-flag."""
    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "0")
    monkeypatch.setenv("GSO_TRIAL23_PHASE_H_CONTRACT_GATE", "1")
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembled",
        run_outcome="OPTIMIZER_IMPROVED",
        phase_h_upload_status="upload_failed",
        scoreboard_stale=True,
    )
    assert v.candidate_deploy_eligible is True


def test_w10_assembly_failed_blocks_regardless_of_flag(_w10_off):
    """assembly_failed is a Trial 21 contract signal — it blocks even
    when the W10 sub-flag is off (legacy behaviour preserved)."""
    v = compute_deploy_eligibility(
        merge_gate_status="passed",
        bundle_status="assembly_failed",
        run_outcome="OPTIMIZER_IMPROVED",
    )
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


def test_w10_loop_out_projection_blocks_on_failed_upload(_w10_on):
    """deploy_eligibility_from_loop_out reads phase_h_upload_status off
    loop_out and blocks deploy; a failed upload implies a stale board."""
    from genie_space_optimizer.optimization.harness import (
        deploy_eligibility_from_loop_out,
    )

    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "bundle_status": "complete",
        },
        "optimizer_outcome": "OPTIMIZER_IMPROVED",
        "phase_h_upload_status": "upload_failed",
    }
    v = deploy_eligibility_from_loop_out(loop_out)
    assert v.optimizer_task_status == "success"
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


def test_w10_loop_out_projection_blocks_on_explicit_stale_flag(_w10_on):
    from genie_space_optimizer.optimization.harness import (
        deploy_eligibility_from_loop_out,
    )

    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "bundle_status": "complete",
        },
        "optimizer_outcome": "OPTIMIZER_IMPROVED",
        "phase_h_upload_status": "uploaded",
        "scoreboard_stale": True,
    }
    v = deploy_eligibility_from_loop_out(loop_out)
    assert v.candidate_deploy_eligible is False
    assert v.deploy_skip_reason == "contract_health_blocked"


def test_w10_loop_out_projection_clean_upload_deploys(_w10_on):
    from genie_space_optimizer.optimization.harness import (
        deploy_eligibility_from_loop_out,
    )

    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "bundle_status": "complete",
        },
        "optimizer_outcome": "OPTIMIZER_IMPROVED",
        "phase_h_upload_status": "uploaded",
    }
    v = deploy_eligibility_from_loop_out(loop_out)
    assert v.candidate_deploy_eligible is True
    assert v.deploy_skip_reason == ""
