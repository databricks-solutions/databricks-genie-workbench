"""Trial 23 Phase 1 postmortem-replay gate.

Phase 1 of Trial 23 ("honest + able to pivot") ships W1 (kept_insufficient
authoritative) and W2 (target-honest acceptance). The plan requires a single
replay gate to hold before Phase 2 begins:

  * d139 terminates ``kept_insufficient`` (NOT ``no_applied_patches``) — W1.
    After Trial 22 fixed bundle delivery, d139's add_example_sql patch now
    applies and survives the full eval, but the target QID stays hard. The
    iteration learned a real fact — the tested repair was insufficient — and
    the terminal taxonomy must say so instead of the false NO_APPLIED_PATCHES.

  * e943's 95.8% shows as ``net_win_non_deployable`` with unresolved target
    debt — W2. The global accuracy rose via attribution drift while the named
    target stayed hard; the candidate must be recorded as evidence but NOT be
    deployable.

These tests replay the production-derived fixtures through the live Phase 1
helpers (``compute_iteration_terminal_reason`` and
``decide_control_plane_acceptance``) with the Trial 23 master flag at its
default-on state.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"
RUN_D139_FIXTURE = FIXTURE_DIR / "run_d139_322426313992436.json"
RUN_E943_FIXTURE = FIXTURE_DIR / "run_e943_231749822620014.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def run_d139() -> dict[str, Any]:
    return _load(RUN_D139_FIXTURE)


@pytest.fixture(scope="module")
def run_e943() -> dict[str, Any]:
    return _load(RUN_E943_FIXTURE)


@pytest.fixture(autouse=True)
def _trial23_flags_default_on(monkeypatch):
    # Phase 1 ships default-on under the master flag. Set it explicitly so the
    # replay gate is deterministic regardless of ambient env.
    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "1")
    yield


# ---------------------------------------------------------------------
# Replay gate #1 — d139 terminates kept_insufficient, not no_applied (W1)
# ---------------------------------------------------------------------


def test_w1_d139_applied_but_insufficient_terminates_kept_insufficient(
    run_d139,
) -> None:
    from genie_space_optimizer.optimization.iteration_terminal import (
        compute_iteration_terminal_reason,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    f = run_d139["trial23_phase1_replay"]
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=int(f["stage3_proposal_count"]),
        compiler_surviving_count=int(f["compiler_surviving_count"]),
        applied_outcome_count=int(f["applied_outcome_count"]),
        kept_insufficient_count=int(f["kept_insufficient_count"]),
    )

    assert verdict.terminal_reason == TerminalReason.KEPT_INSUFFICIENT, (
        "W1 replay gate: d139 applied an add_example_sql patch that "
        "survived eval but did not change behaviour; the iteration "
        "terminal reason MUST be kept_insufficient, not the "
        f"no_applied_patches catch-all. got {verdict.terminal_reason}"
    )
    # Anti-assertions from the fixture.
    assert verdict.terminal_reason != TerminalReason.NO_APPLIED_PATCHES, (
        "W1 replay gate: a patch WAS applied — NO_APPLIED_PATCHES is a lie."
    )
    assert verdict.terminal_reason != TerminalReason.APPLIER_NO_OUTCOMES, (
        "W1 replay gate: the applier produced an outcome."
    )


def test_w1_d139_rollback_restores_no_applied_patches(run_d139) -> None:
    """Flag-off byte-stable contract: with the master flag off, the caller
    passes the ``-1`` sentinel for kept_insufficient_count, so the helper
    falls through to the legacy taxonomy. This pins the rollback guarantee.
    """
    from genie_space_optimizer.optimization.iteration_terminal import (
        compute_iteration_terminal_reason,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    f = run_d139["trial23_phase1_replay"]
    # Rollback: caller withholds the kept_insufficient signal (sentinel -1).
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=int(f["stage3_proposal_count"]),
        compiler_surviving_count=int(f["compiler_surviving_count"]),
        applied_outcome_count=int(f["applied_outcome_count"]),
        kept_insufficient_count=-1,
    )
    # stage3 returned proposals, compiler kept them, applier produced an
    # outcome → not the kept_insufficient branch; legacy taxonomy applies.
    assert verdict.terminal_reason != TerminalReason.KEPT_INSUFFICIENT, (
        "W1 rollback: without the kept_insufficient signal the helper must "
        "NOT synthesize kept_insufficient out of thin air."
    )


# ---------------------------------------------------------------------
# Replay gate #2 — e943 95.8% is net_win_non_deployable (W2)
# ---------------------------------------------------------------------


def test_w2_e943_attribution_drift_is_net_win_non_deployable(run_e943) -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    f = run_e943["trial23_phase1_replay"]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=float(f["baseline_accuracy"]),
        candidate_accuracy=float(f["candidate_accuracy"]),
        target_qids=tuple(f["target_qids"]),
        pre_rows=tuple(f["pre_rows"]),
        post_rows=tuple(f["post_rows"]),
    )
    exp = f["expected_after_trial23_w2"]

    assert decision.accepted is False, (
        "W2 replay gate: e943's 95.8% rose via attribution drift while the "
        "named target stayed hard; the candidate MUST NOT be deployable. "
        f"got accepted={decision.accepted} reason={decision.reason_code}"
    )
    assert decision.reason_code == exp["reason_code"], (
        "W2 replay gate: the demoted reason must be net_win_non_deployable; "
        f"got {decision.reason_code}"
    )
    assert tuple(decision.unresolved_target_debt_qids) == tuple(
        exp["unresolved_target_debt_qids"]
    ), (
        "W2 replay gate: unresolved_target_debt_qids must name the target "
        f"that stayed hard; got {decision.unresolved_target_debt_qids}"
    )
    # The global delta is still recorded as evidence (not discarded).
    assert decision.delta_pp == pytest.approx(
        float(exp["delta_pp_recorded_as_evidence"])
    ), (
        "W2 replay gate: the global delta_pp must still be recorded as "
        f"evidence; got {decision.delta_pp}"
    )


def test_w2_e943_rollback_restores_deployable_accept(
    run_e943, monkeypatch
) -> None:
    """Flag-off byte-stable contract: with the master flag off, the same
    attribution-drift inputs accept as deployable (legacy behaviour).
    """
    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    f = run_e943["trial23_phase1_replay"]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=float(f["baseline_accuracy"]),
        candidate_accuracy=float(f["candidate_accuracy"]),
        target_qids=tuple(f["target_qids"]),
        pre_rows=tuple(f["pre_rows"]),
        post_rows=tuple(f["post_rows"]),
    )
    assert decision.accepted is True, (
        "W2 rollback: with the master flag off the legacy attribution-drift "
        f"accept must hold; got accepted={decision.accepted}"
    )
    assert decision.reason_code != "net_win_non_deployable", (
        "W2 rollback: the W2 demotion must not fire when the flag is off."
    )


# ---------------------------------------------------------------------
# Replay gate #3 — e943 Phase H contract boundary (W10)
#
# e943 (task 339587654249993) raised the build_decision_trace_all
# 'list'.get crash, reported bundle_status=assembly_failed +
# phase_h_upload_status=upload_failed, and shipped a stale scoreboard —
# yet the Databricks task returned SUCCESS and deploy RAN. W10 keeps the
# optimizer task SUCCESS (Trial 21 W9 contract) but the deploy gate must
# mark the candidate non-deployable so a busted/stale bundle never ships.
# ---------------------------------------------------------------------


def test_w10_e943_phase_h_failure_blocks_deploy_but_task_stays_success(
    run_e943,
) -> None:
    from genie_space_optimizer.optimization.harness import (
        deploy_eligibility_from_loop_out,
    )

    f = run_e943["trial23_w10_replay"]
    verdict = deploy_eligibility_from_loop_out(f["loop_out"])
    exp = f["expected_after_trial23_w10"]

    assert verdict.optimizer_task_status == exp["optimizer_task_status"], (
        "W10 replay gate: the optimizer task must stay SUCCESS (effort-"
        "successful) even when Phase H assembly failed; "
        f"got {verdict.optimizer_task_status}"
    )
    assert verdict.candidate_deploy_eligible is exp["candidate_deploy_eligible"], (
        "W10 replay gate: assembly_failed + upload_failed + stale scoreboard "
        "MUST make the candidate non-deployable; e943 shipped anyway. "
        f"got candidate_deploy_eligible={verdict.candidate_deploy_eligible}"
    )
    assert verdict.deploy_skip_reason == exp["deploy_skip_reason"], (
        "W10 replay gate: the deploy skip reason must be "
        f"contract_health_blocked; got {verdict.deploy_skip_reason}"
    )


def test_w10_e943_upload_failure_is_new_coverage_beyond_legacy(
    run_e943, monkeypatch
) -> None:
    """Isolates the NEW W10 coverage on the e943 anchor: with contract
    health otherwise clean (healthy + complete), a failed Phase H upload +
    stale scoreboard blocks deploy ONLY when the W10 flag is on. Flag-off
    restores the legacy Trial 21/22 posture (deploy-eligible). This is the
    gap e943 fell through."""
    from genie_space_optimizer.optimization.harness import (
        deploy_eligibility_from_loop_out,
    )

    iso = run_e943["trial23_w10_replay"]["w10_isolated_upload_failure"]

    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "1")
    monkeypatch.setenv("GSO_TRIAL23_PHASE_H_CONTRACT_GATE", "1")
    on = deploy_eligibility_from_loop_out(iso["loop_out"])
    assert (
        on.candidate_deploy_eligible
        is iso["expected_flag_on"]["candidate_deploy_eligible"]
    ), (
        "W10 flag-on: a clean-contract-health run with upload_failed + stale "
        "scoreboard must still be blocked from deploy."
    )
    assert on.deploy_skip_reason == iso["expected_flag_on"]["deploy_skip_reason"]

    monkeypatch.setenv("GSO_TRIAL23_PHASE_H_CONTRACT_GATE", "0")
    off = deploy_eligibility_from_loop_out(iso["loop_out"])
    assert (
        off.candidate_deploy_eligible
        is iso["expected_flag_off"]["candidate_deploy_eligible"]
    ), (
        "W10 rollback: with the W10 sub-flag off, the legacy gate ignores "
        "upload_failed + stale scoreboard (deploy-eligible)."
    )
    assert off.deploy_skip_reason == iso["expected_flag_off"]["deploy_skip_reason"]
