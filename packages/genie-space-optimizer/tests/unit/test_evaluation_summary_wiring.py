"""Pin wiring of the pre/post-arbiter evaluation summary block."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_full_eval_gate_prints_format_evaluation_summary_block() -> None:
    src = inspect.getsource(harness._run_gate_checks)
    assert "format_evaluation_summary_block(" in src, (
        "The full-eval gate must print the dedicated pre/post-arbiter "
        "summary block; defining the helper is not enough."
    )


def test_full_eval_gate_passes_control_plane_target_fixed_qids() -> None:
    src = inspect.getsource(harness._run_gate_checks)
    call_idx = src.index("format_evaluation_summary_block(")
    call_block = src[call_idx: call_idx + 600]
    assert "target_fixed_qids=" in call_block
    assert "_target_fixed_qids_for_guardrail" in src or "_control_plane_decision.target_fixed_qids" in src


def test_legacy_two_line_eval_accuracy_print_is_replaced() -> None:
    src = inspect.getsource(harness._run_gate_checks)
    legacy_block = (
        '_kv("Eval accuracy (post-arbiter)", f"{full_accuracy:.1f}%")'
    )
    assert legacy_block not in src, (
        "Replace the loose two-line print with format_evaluation_summary_block "
        "so stdout has one canonical full-eval summary."
    )
