"""MV-D117 (C-8): no production caller builds a multi-hop MvProfiling.

The nested and subquery-source rungs are not proven in Unity Catalog. They stay
reachable only from tests until a live proof lands; this pin fails the moment a
production call site starts passing join hops or attributes.
"""

import ast
from pathlib import Path

import genie_space_optimizer

SRC = Path(genie_space_optimizer.__file__).parent
BACKEND = Path(__file__).resolve().parents[4] / "backend"


def _calls(root: Path):
    for path in root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
                if name == "MvProfiling":
                    yield path, node


def test_no_production_caller_supplies_join_hops_or_attributes():
    offenders = [
        f"{path.relative_to(root.parent)}:{node.lineno}"
        for root in (SRC, BACKEND)
        for path, node in _calls(root)
        if {kw.arg for kw in node.keywords} & {"hops", "attributes"}
    ]
    assert offenders == []


def test_the_pin_sees_the_known_call_sites():
    sites = {path.name for root in (SRC, BACKEND) for path, _ in _calls(root)}
    assert "mv_advisor.py" in sites
