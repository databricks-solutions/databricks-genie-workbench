"""Verify the gate's pre-rows invariants from harness-shaped inputs.

The pure helper algebra is correct in isolation. The bug observed in the
7now run was at the harness boundary — the gate received post-eval rows as
its baseline. These tests pin the explicit reason codes the harness will
now see when that happens, so a regression is loud."""

from __future__ import annotations


def test_gate_rejects_when_baseline_rows_match_candidate_rows_exactly() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    rows = [
        {
            "id": "q021",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q001",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q009",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=90.9,
        target_qids=("q009", "q021"),
        pre_rows=rows,
        post_rows=rows,
    )
    assert decision.reason_code == "stale_or_candidate_pre_rows"
    assert decision.accepted is False


def test_gate_detects_actual_iteration2_regression_debt_shape() -> None:
    """The actual iter-2 shape is a net score improvement with bounded debt:
    q009/q026 improve while q001 is an out-of-target hard regression. This
    test asserts detection, not final rejection."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {"id": "q009", "feedback/arbiter/value": "ground_truth_correct",
         "feedback/result_correctness/value": "no"},
        {"id": "q021", "feedback/arbiter/value": "ground_truth_correct",
         "feedback/result_correctness/value": "no"},
        {"id": "q026", "feedback/arbiter/value": "ground_truth_correct",
         "feedback/result_correctness/value": "no"},
        {"id": "q001", "feedback/arbiter/value": "both_correct",
         "feedback/result_correctness/value": "yes"},
    ]
    post_rows = [
        {"id": "q009", "feedback/arbiter/value": "both_correct",
         "feedback/result_correctness/value": "yes"},
        {"id": "q021", "feedback/arbiter/value": "ground_truth_correct",
         "feedback/result_correctness/value": "no"},
        {"id": "q026", "feedback/arbiter/value": "genie_correct",
         "feedback/result_correctness/value": "no"},
        {"id": "q001", "feedback/arbiter/value": "ground_truth_correct",
         "feedback/result_correctness/value": "no"},
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=90.9,
        target_qids=("q009", "q021"),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )
    assert "q009" in decision.target_fixed_qids
    assert "q001" in decision.out_of_target_regressed_qids
    assert decision.delta_pp == 4.5


def test_run_gate_checks_requires_explicit_accepted_baseline_rows() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    signature = inspect.signature(harness._run_gate_checks)

    assert "accepted_baseline_rows_for_control_plane" in signature.parameters


def test_run_gate_checks_does_not_read_loop_local_baseline_name() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_gate_checks)

    assert "list(_accepted_baseline_rows_for_control_plane or [])" not in source
    assert "accepted_baseline_rows_for_control_plane or []" in source
