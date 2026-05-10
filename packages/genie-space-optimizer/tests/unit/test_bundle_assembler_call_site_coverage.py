"""Cycle 14-W T2 — every `.get()` call site on stage-capture-typed
values in ``run_output_bundle.py`` must route through
``_normalize_stage_capture``. AST-walk based guardrail against
future drift.
"""

from __future__ import annotations

import ast
from pathlib import Path

_RUN_OUTPUT_BUNDLE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "run_output_bundle.py"
)


def test_no_unguarded_get_on_stage_capture_values() -> None:
    """Every ``xxx.get(...)`` where ``xxx`` plausibly holds a
    stage-capture value (heuristic: variable name contains
    ``stage`` or ``capture``) must be wrapped in
    ``_normalize_stage_capture(...)``."""
    tree = ast.parse(_RUN_OUTPUT_BUNDLE.read_text())
    violations: list[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and isinstance(node.func.value, ast.Name)
            ):
                name = node.func.value.id
                if "stage" in name.lower() or "capture" in name.lower():
                    violations.append(
                        f"line {node.lineno}: {name}.get(...)"
                    )
            self.generic_visit(node)

    Visitor().visit(tree)
    assert not violations, (
        "stage-capture values must route through "
        "_normalize_stage_capture: " + "; ".join(violations)
    )


def test_normalize_stage_capture_emits_marker_with_stage_key(capsys) -> None:
    """When called with ``stage_key=...`` and a list-of-dict input,
    the helper emits a ``GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1``
    marker recording the lossy collapse."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        _normalize_stage_capture,
    )

    out = _normalize_stage_capture(
        [{"k": "a"}, {"k": "b"}],
        stage_key="09_acceptance_decision",
        iteration=2,
    )
    assert out == {"k": "a"}
    captured = capsys.readouterr().out
    assert "GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1" in captured
    assert "09_acceptance_decision" in captured


def test_normalize_stage_capture_silent_without_stage_key(capsys) -> None:
    """Backward-compat: existing C14-V T5 call sites (no stage_key)
    do not emit the lossy-collapse marker."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        _normalize_stage_capture,
    )

    out = _normalize_stage_capture([{"k": "a"}, {"k": "b"}])
    assert out == {"k": "a"}
    captured = capsys.readouterr().out
    assert "GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1" not in captured
