"""Unit tests for the subset-first gate question-id selection (GSO v2 Phase 1).

The 3-gate sequencing itself is orchestrated inline in
``harness._run_gate_checks`` against the official Benchmark Eval-Run API;
this module only owns the slice/P0 subset-id selectors tested here.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.eval_gates import (
    select_p0_qids,
    select_slice_qids,
)


# ── subset selection ─────────────────────────────────────────────────────────
def test_select_slice_qids_caps_and_dedupes() -> None:
    qids = select_slice_qids(["a", "b", "b", "c", "d"], max_questions=3)
    assert qids == ["a", "b", "c"]


def test_select_slice_qids_falls_back_to_all_when_no_failing() -> None:
    qids = select_slice_qids([], all_qids=["x", "y", "z"], max_questions=2)
    assert qids == ["x", "y"]


def test_select_p0_qids_caps() -> None:
    assert select_p0_qids(["p1", "p2", "p3"], max_questions=2) == ["p1", "p2"]
