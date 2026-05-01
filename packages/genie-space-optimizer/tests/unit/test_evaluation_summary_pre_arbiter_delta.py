"""Pin pre-arbiter accuracy + regression flag in the EVALUATION SUMMARY."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    format_evaluation_summary_block,
)


def test_summary_includes_pre_and_post_arbiter_lines() -> None:
    block = format_evaluation_summary_block(
        iteration=1,
        ag_id="AG1",
        baseline_pre_arbiter=66.7,
        candidate_pre_arbiter=58.3,
        baseline_post_arbiter=83.3,
        candidate_post_arbiter=83.3,
        target_fixed_qids=(),
    )
    assert "pre_arbiter" in block.lower()
    assert "66.7" in block and "58.3" in block
    assert "regressed_only_pre_arbiter: yes" in block.lower()


def test_summary_no_regression_when_post_dropped_too() -> None:
    block = format_evaluation_summary_block(
        iteration=1,
        ag_id="AG1",
        baseline_pre_arbiter=80.0,
        candidate_pre_arbiter=70.0,
        baseline_post_arbiter=90.0,
        candidate_post_arbiter=80.0,
        target_fixed_qids=(),
    )
    assert "regressed_only_pre_arbiter: no" in block.lower()


def test_summary_no_flag_when_target_fixed() -> None:
    block = format_evaluation_summary_block(
        iteration=1,
        ag_id="AG1",
        baseline_pre_arbiter=80.0,
        candidate_pre_arbiter=70.0,
        baseline_post_arbiter=90.0,
        candidate_post_arbiter=95.0,
        target_fixed_qids=("gs_017",),
    )
    assert "regressed_only_pre_arbiter: no" in block.lower()
