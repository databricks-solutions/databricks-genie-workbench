"""Track 3/E — apply-time journey emit must differentiate targeted
applications (qid was named in the patch's target_qids) from broad
AG-scope applications (qid was in the AG's affected_questions but not
specifically targeted by this patch). Phase B's
``causal_patch_survival_pct`` metric depends on this distinction.
"""
from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness, question_journey


def test_journey_stage_order_includes_applied_targeted_and_broad_ag_scope() -> None:
    """The new stage names must appear in question_journey._STAGE_ORDER
    so journey rendering sorts them correctly relative to ``applied``.
    """
    stages = question_journey._STAGE_ORDER
    assert "applied_targeted" in stages, (
        "applied_targeted not in journey stage order; can't sort it"
    )
    assert "applied_broad_ag_scope" in stages, (
        "applied_broad_ag_scope not in journey stage order; can't sort it"
    )
    rank = stages.index
    assert rank("applied_targeted") > rank("dropped_at_cap")
    assert rank("applied_broad_ag_scope") > rank("dropped_at_cap")
    assert rank("applied_targeted") < rank("rolled_back")
    assert rank("applied_broad_ag_scope") < rank("rolled_back")


def test_apply_emit_distinguishes_targeted_from_broad_ag_scope() -> None:
    """The harness apply-time emit at ``harness.py:14618-14645`` must
    emit ``applied_targeted`` for qids in the patch's target_qids and
    ``applied_broad_ag_scope`` for qids in the AG's affected_questions
    that are NOT in the patch's target_qids.
    """
    import re

    src = inspect.getsource(harness._run_lever_loop)
    apply_block_start = src.find("# Task 13 — emit ``applied``")
    assert apply_block_start >= 0, (
        "Task 13 apply-emit anchor missing; harness file must still "
        "contain the # Task 13 — emit ``applied`` comment"
    )
    apply_block = src[apply_block_start : apply_block_start + 2500]

    assert "applied_targeted" in apply_block, (
        "harness apply-emit does not emit applied_targeted"
    )
    assert "applied_broad_ag_scope" in apply_block, (
        "harness apply-emit does not emit applied_broad_ag_scope"
    )
    legacy_pattern = re.compile(
        r'_journey_emit\s*\(\s*"applied"\s*,',
        re.MULTILINE,
    )
    assert legacy_pattern.search(apply_block) is None, (
        "harness apply-emit still calls _journey_emit('applied', ...) "
        "directly; replace with applied_targeted / applied_broad_ag_scope"
    )
