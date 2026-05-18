"""Unit tests for the AST-based branch-point enumerator."""
from __future__ import annotations

import textwrap

import pytest

from genie_space_optimizer.optimization.harness_control_flow_ast import (
    BranchPoint,
    enumerate_branch_points,
)


_FIXTURE = textwrap.dedent('''
    def _run_lever_loop(*args, **kwargs):
        for _iter_num in range(4):
            if _iter_num == 0:
                continue
            try:
                action_groups = []
                ag = action_groups[0]
            except IndexError:
                return None
            collect_blocked_clusters([])
            if not action_groups:
                continue
            _generate_lever6_proposal(ag)
''')


def test_enumerator_finds_continue_statements():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    continues = [p for p in points if p.statement_type == "continue"]
    assert len(continues) == 2


def test_enumerator_finds_return_statement():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    returns = [p for p in points if p.statement_type == "return"]
    assert len(returns) == 1


def test_enumerator_finds_if_headers():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    ifs = [p for p in points if p.statement_type == "if"]
    assert len(ifs) == 2


def test_enumerator_finds_for_loop():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    fors = [p for p in points if p.statement_type == "for"]
    assert len(fors) == 1
    assert fors[0].depth == 0


def test_enumerator_finds_try_and_except():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    kinds = {p.statement_type for p in points}
    assert "try" in kinds
    assert "except_handler" in kinds


def test_enumerator_finds_known_checkpoint_calls():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    checkpoints = [p for p in points if p.statement_type == "checkpoint_call"]
    names = {p.detail for p in checkpoints}
    assert "collect_blocked_clusters" in names
    assert "_generate_lever6_proposal" in names


def test_branch_point_records_line_and_depth():
    points = enumerate_branch_points(_FIXTURE, function_name="_run_lever_loop")
    # The first `continue` (inside `if _iter_num == 0`) is nested at
    # depth 2 (for-loop -> if-block).
    first_continue = next(
        p for p in points if p.statement_type == "continue"
    )
    assert first_continue.lineno >= 4  # first line of fixture body
    assert first_continue.depth >= 2


def test_unknown_function_raises():
    with pytest.raises(LookupError):
        enumerate_branch_points(_FIXTURE, function_name="not_defined")


def test_branch_point_is_dataclass_with_expected_fields():
    bp = BranchPoint(
        lineno=10,
        end_lineno=10,
        statement_type="continue",
        detail=None,
        depth=2,
        parent_construct="if",
        snippet="continue",
    )
    assert bp.lineno == 10
    assert bp.statement_type == "continue"
