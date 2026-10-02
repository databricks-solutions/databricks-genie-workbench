"""MV-D117 (C-8), MV-D124: no production caller builds a multi-hop MvProfiling.

Every join rung is proven in Unity Catalog and creatable (MV-D124), but no
production caller passes join hops, so no proposal can reach a joined rung. This
pin fails the moment a production call site starts passing join hops or
attributes; it is lifted by the item that wires them from the space's join specs.
"""

import ast
from pathlib import Path

import pytest

import genie_space_optimizer

SRC = Path(genie_space_optimizer.__file__).parent
BACKEND = Path(__file__).resolve().parents[4] / "backend"


def _profiling_calls(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name == "MvProfiling":
                yield node


def _calls(root: Path):
    for path in root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in _profiling_calls(tree):
            yield path, node


def _may_supply_hops_or_attributes(node: ast.Call) -> bool:
    """A fourth positional argument is ``hops``; ``*args`` and ``**kw`` (``arg is None``) may be."""
    if len(node.args) > 3 or any(isinstance(arg, ast.Starred) for arg in node.args):
        return True
    return any(kw.arg is None or kw.arg in ("hops", "attributes") for kw in node.keywords)


def test_no_production_caller_supplies_join_hops_or_attributes():
    offenders = [
        f"{path.relative_to(root.parent)}:{node.lineno}"
        for root in (SRC, BACKEND)
        for path, node in _calls(root)
        if _may_supply_hops_or_attributes(node)
    ]
    assert offenders == []


def test_the_pin_sees_the_known_call_sites():
    sites = {path.name for root in (SRC, BACKEND) for path, _ in _calls(root)}
    assert "mv_advisor.py" in sites


@pytest.mark.parametrize(
    "call",
    [
        "MvProfiling(a, b, c, hops)",
        "MvProfiling(*args)",
        "MvProfiling(a, *rest)",
        "MvProfiling(**kw)",
        "MvProfiling(source_table=a, hops=h)",
        "MvProfiling(source_table=a, attributes=x)",
    ],
)
def test_the_pin_flags_hops_supplied_by_position_or_unpacking(call):
    """MV-D122: the fourth positional field is ``hops``, and ``**kw`` can carry it."""
    (node,) = _profiling_calls(ast.parse(call))
    assert _may_supply_hops_or_attributes(node)


@pytest.mark.parametrize(
    "call", ["MvProfiling(a, b, c)", "MvProfiling(source_table=a, uniqueness=u)"],
)
def test_the_pin_passes_a_single_hop_call(call):
    (node,) = _profiling_calls(ast.parse(call))
    assert not _may_supply_hops_or_attributes(node)
