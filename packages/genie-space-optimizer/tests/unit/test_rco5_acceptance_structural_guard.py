"""RCO-5 Task 7 — structural guard for acceptance object consolidation.

Enforces the rule from
``docs/2026-05-11-rco-5-acceptance-consolidation-policy.md``:

  No class named ``AcceptanceDecision`` lives in
  ``optimization.acceptance_policy`` or
  ``tools.lever_loop_stdout_parser``. The only canonical Stage-9
  acceptance type is ``ControlPlaneAcceptance``.

Drift surfaces as a test failure here, not as a runtime defect later.
"""

from __future__ import annotations

import importlib
import inspect


def _classes_in_module(module_path: str) -> set[str]:
    mod = importlib.import_module(module_path)
    return {
        name
        for name, obj in inspect.getmembers(mod, inspect.isclass)
        if obj.__module__ == module_path
    }


def test_acceptance_policy_has_no_acceptance_decision_class() -> None:
    classes = _classes_in_module(
        "genie_space_optimizer.optimization.acceptance_policy"
    )
    assert "AcceptanceDecision" not in classes, (
        "RCO-5 structural guard: ``AcceptanceDecision`` reappeared in "
        "``optimization.acceptance_policy``. Per the consolidation "
        "policy, the gain-gate decision type is ``GainGateDecision``. "
        "Update the rename or update the policy doc deliberately."
    )


def test_acceptance_policy_exports_gain_gate_decision() -> None:
    classes = _classes_in_module(
        "genie_space_optimizer.optimization.acceptance_policy"
    )
    assert "GainGateDecision" in classes, (
        "RCO-5 structural guard: ``GainGateDecision`` missing from "
        "``optimization.acceptance_policy``."
    )


def test_lever_loop_stdout_parser_has_no_acceptance_decision_class() -> None:
    classes = _classes_in_module(
        "genie_space_optimizer.tools.lever_loop_stdout_parser"
    )
    assert "AcceptanceDecision" not in classes, (
        "RCO-5 structural guard: ``AcceptanceDecision`` reappeared in "
        "``tools.lever_loop_stdout_parser``. Per the consolidation "
        "policy, the parser projection type is ``ParsedAcceptanceView``."
    )


def test_lever_loop_stdout_parser_exports_parsed_acceptance_view() -> None:
    classes = _classes_in_module(
        "genie_space_optimizer.tools.lever_loop_stdout_parser"
    )
    assert "ParsedAcceptanceView" in classes, (
        "RCO-5 structural guard: ``ParsedAcceptanceView`` missing from "
        "``tools.lever_loop_stdout_parser``."
    )


def test_parsed_view_to_control_plane_is_importable() -> None:
    from genie_space_optimizer.tools.lever_loop_stdout_parser import (  # noqa: F401
        parsed_view_to_control_plane,
    )


def test_control_plane_acceptance_is_canonical() -> None:
    """``ControlPlaneAcceptance`` is the only Stage-9 acceptance
    *decision* class in ``optimization.control_plane``.

    Plan P-C adds ``AcceptanceDecisionRendering`` — a pure view
    derived from ``ControlPlaneAcceptance``, NOT a competing
    decision type. The structural guard allows that one specific
    sibling because it has a distinct role (rendering) and
    explicitly does not duplicate the decision semantics.
    """
    classes = _classes_in_module(
        "genie_space_optimizer.optimization.control_plane"
    )
    acceptance_classes = {c for c in classes if "Acceptance" in c}
    allowed = {"ControlPlaneAcceptance", "AcceptanceDecisionRendering"}
    assert acceptance_classes == allowed, (
        "RCO-5 structural guard: ``optimization.control_plane`` should "
        f"contain exactly {sorted(allowed)} Acceptance classes. "
        f"Found: {acceptance_classes}"
    )


def test_no_acceptance_decision_class_anywhere() -> None:
    """Belt-and-suspenders: search the project for any other class
    literally named ``AcceptanceDecision`` (i.e. ``class
    AcceptanceDecision`` followed by ``(``, ``:``, or whitespace).
    Production-code only — tests and docs are allowed to mention
    the old name in transitional comments.

    Plan P-C — ``AcceptanceDecisionRendering`` is excluded because
    it is a pure render-view, not a decision class (see
    ``test_control_plane_acceptance_is_canonical``).
    """
    import pathlib
    import re

    src_root = (
        pathlib.Path(__file__).resolve().parents[2]
        / "src"
        / "genie_space_optimizer"
    )
    pattern = re.compile(r"^class AcceptanceDecision[\s\(:]", re.MULTILINE)
    offenders: list[str] = []
    for py_file in src_root.rglob("*.py"):
        text = py_file.read_text()
        if pattern.search(text):
            offenders.append(str(py_file.relative_to(src_root)))
    assert offenders == [], (
        "RCO-5 structural guard: ``class AcceptanceDecision`` found in "
        f"production source: {offenders}. Per the consolidation policy, "
        "this name is retired; use ``GainGateDecision`` or "
        "``ParsedAcceptanceView`` depending on role, or "
        "``ControlPlaneAcceptance`` for the canonical type."
    )
