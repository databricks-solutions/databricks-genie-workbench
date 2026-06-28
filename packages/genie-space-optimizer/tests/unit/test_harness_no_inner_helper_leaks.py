"""Audit lint — close the entire "inner-helper variable name leaked
to ``_run_lever_loop``" bug family in one shot.

Cycle 11's typed ``PRODUCER_EXCEPTION`` decision records named three
sibling bugs, each with the same shape: a name assigned only inside
a sibling helper function (e.g., ``_run_gate_checks``) is read inside
``_run_lever_loop`` with no local assignment and no closure
relationship, so Python compiles the read as a free-variable lookup
that fails LEGB at runtime.

| Bug | Name | Helper that owns the assignment | Surfaced in run |
|-----|------|--------------------------------|-----------------|
| A   | ``full_pre_arbiter_accuracy``     | ``_run_gate_checks`` | ``90000000000003`` |
| B   | ``full_accuracy``                 | ``_run_gate_checks`` | ``90000000000004`` |
| C   | ``_baseline_rows_for_control_plane`` | ``_run_gate_checks`` | ``900000000000002`` |

Each one cost a half-cycle of investigation (postmortem read,
fixture capture, helper authoring, regression assertion). This lint
catches the family structurally at compile time so future siblings
fail CI rather than waiting on a typed PRODUCER_EXCEPTION record
from production.

The lint walks ``_run_lever_loop``'s AST and asserts every
``Name(ctx=Load)`` in the function's TOP scope (excluding nested
``FunctionDef`` / ``Lambda`` / comprehension scopes which are
independent and may legitimately close over ``_run_lever_loop``'s
locals) resolves to one of:

  (a) a parameter of ``_run_lever_loop``,
  (b) a name assigned in ``_run_lever_loop``'s top scope,
  (c) a module-level binding (top-level ``def`` / ``class`` /
      ``import`` / assignment),
  (d) a Python builtin.

Anything else is, by definition, a leak.
"""

from __future__ import annotations

import ast
import builtins
from pathlib import Path

HARNESS_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def _find_function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            assert isinstance(node, ast.FunctionDef), (
                f"{name} is async; lint expects sync"
            )
            return node
    raise AssertionError(f"{name} not found at module level")


def _collect_target_names(node: ast.AST, out: set[str]) -> None:
    """Recurse into assignment-target structures (``Tuple`` / ``List`` /
    ``Starred``) and collect every ``Name.id`` found."""
    if isinstance(node, ast.Name):
        out.add(node.id)
    elif isinstance(node, (ast.Tuple, ast.List)):
        for elt in node.elts:
            _collect_target_names(elt, out)
    elif isinstance(node, ast.Starred):
        _collect_target_names(node.value, out)


def _module_level_names(tree: ast.Module) -> set[str]:
    """Names introduced at the module's top level."""
    out: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out.add(node.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                out.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                out.add(alias.asname or alias.name)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                _collect_target_names(tgt, out)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out.add(node.target.id)
        elif isinstance(node, (ast.If, ast.Try)):
            # Top-level conditional imports (TYPE_CHECKING blocks,
            # try/except imports). Walk one level deeper to capture
            # any bindings introduced. Conservative — anything
            # assigned in those branches counts as module-level.
            for child in ast.walk(node):
                if isinstance(child, ast.Import):
                    for alias in child.names:
                        out.add(alias.asname or alias.name.split(".")[0])
                elif isinstance(child, ast.ImportFrom):
                    for alias in child.names:
                        out.add(alias.asname or alias.name)
                elif isinstance(child, ast.Assign):
                    for tgt in child.targets:
                        _collect_target_names(tgt, out)
                elif isinstance(child, ast.AnnAssign) and isinstance(
                    child.target, ast.Name
                ):
                    out.add(child.target.id)
    return out


class _ScopeWalker(ast.NodeVisitor):
    """Collect names BOUND in a function's top scope and names READ
    in that same top scope. Does NOT descend into nested
    ``FunctionDef`` / ``AsyncFunctionDef`` / ``Lambda`` / ``ClassDef``
    or comprehension scopes (they own their own bindings; their
    reads of the outer function's locals are valid closures and
    not the lint's concern).
    """

    def __init__(self) -> None:
        self.bound: set[str] = set()
        self.loads: list[ast.Name] = []

    # ---- nested scopes: bind the *name*, do not descend
    def visit_FunctionDef(self, node):
        self.bound.add(node.name)

    def visit_AsyncFunctionDef(self, node):
        self.bound.add(node.name)

    def visit_ClassDef(self, node):
        self.bound.add(node.name)

    def visit_Lambda(self, node):
        return

    def visit_ListComp(self, node):
        return

    def visit_SetComp(self, node):
        return

    def visit_DictComp(self, node):
        return

    def visit_GeneratorExp(self, node):
        return

    # ---- name reads / writes in this scope
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self.loads.append(node)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.bound.add(node.id)

    def visit_arg(self, node):
        self.bound.add(node.arg)

    # ---- statement-level binding sites
    def visit_For(self, node):
        _collect_target_names(node.target, self.bound)
        self.generic_visit(node)

    def visit_AsyncFor(self, node):
        _collect_target_names(node.target, self.bound)
        self.generic_visit(node)

    def visit_With(self, node):
        for item in node.items:
            if item.optional_vars is not None:
                _collect_target_names(item.optional_vars, self.bound)
        self.generic_visit(node)

    def visit_AsyncWith(self, node):
        for item in node.items:
            if item.optional_vars is not None:
                _collect_target_names(item.optional_vars, self.bound)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node):
        if node.name is not None:
            self.bound.add(node.name)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.bound.add(alias.asname or alias.name.split(".")[0])

    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.bound.add(alias.asname or alias.name)

    def visit_Global(self, node):
        for n in node.names:
            self.bound.add(n)

    def visit_Nonlocal(self, node):
        for n in node.names:
            self.bound.add(n)


def _audit_function(tree: ast.Module, func_name: str) -> list[str]:
    """Return a sorted list of free-variable names that the named
    function reads but cannot resolve to (params | locals |
    module-level | builtins)."""
    func = _find_function(tree, func_name)

    walker = _ScopeWalker()
    # Seed walker.bound with the function's parameters BEFORE
    # walking the body. Python's compile-time scoping treats every
    # parameter as a local of the function for the entire body, so
    # any read of the parameter name must succeed regardless of
    # whether the read appears textually before its first
    # assignment-style use.
    for arg in (
        list(func.args.posonlyargs)
        + list(func.args.args)
        + list(func.args.kwonlyargs)
    ):
        walker.bound.add(arg.arg)
    if func.args.vararg is not None:
        walker.bound.add(func.args.vararg.arg)
    if func.args.kwarg is not None:
        walker.bound.add(func.args.kwarg.arg)

    for stmt in func.body:
        walker.visit(stmt)

    module_level = _module_level_names(tree)
    builtins_set = set(dir(builtins))
    permitted = walker.bound | module_level | builtins_set

    return sorted({n.id for n in walker.loads if n.id not in permitted})


# Known leaks left in place at the time of the Bug C commit. Each
# of these is read inside ``_run_lever_loop`` from the inner scope
# of a sibling helper (``_run_gate_checks`` and friends) BUT is
# guarded at runtime by an ``if "name" in locals()`` or
# ``if "name" in dir()`` check whose True branch is dead — Python's
# compile-time scoping makes the name a free variable, and a free
# variable name is never a key of ``locals()`` for that frame, so
# the guard always falls through to the fallback.
#
# Removing the dead True branches is a behaviour-neutral cleanup
# (the fallback was always executed anyway) but is out of scope for
# this commit. Tracked in a Cycle 13 follow-up TODO. The allow-list
# exists so the lint flags any NEW leak the moment it's added,
# while not rejecting the present commit on the historical surface.
#
# DO NOT add to this set. The only path forward is to remove an
# entry by either:
#   (a) deleting the dead True branch and its guard (cleanup), or
#   (b) sourcing the value via ``gate_result`` / explicit kwargs
#       and adding a real local-scope assignment in
#       ``_run_lever_loop`` (the helper-or-rename pattern from
#       Bug A, B, C, D, E).
_KNOWN_DEFENDED_DEAD_CODE_LEAKS: frozenset[str] = frozenset({
    "_candidate_clusters_for_decision_trace",
    "_raw_proposals_for_ag",
    "_rca_evidence_bundle",
    "_rolled_back_content_fingerprints",
    "strategist_returned_ags",
})


def test_run_lever_loop_has_no_inner_helper_variable_leaks() -> None:
    """Audit lint — every name read inside ``_run_lever_loop``'s top
    scope must resolve to a parameter, a local, a module-level
    binding, a Python builtin, or be on the explicit
    ``_KNOWN_DEFENDED_DEAD_CODE_LEAKS`` allow-list.

    Closes the bug family of A (``full_pre_arbiter_accuracy``),
    B (``full_accuracy``), C (``_baseline_rows_for_control_plane``),
    D (``MIN_POST_ARBITER_GAIN_PP``), and E (``_audit_emit``) in
    one structural assertion. The allow-list captures the historical
    defended-dead-code surface that pre-dates this lint; removing
    entries from it is a Cycle 13 cleanup.

    If this test fails with a name NOT in the allow-list, the named
    leak is guaranteed to be a new sibling. Fix it in the same
    commit that introduced it, using the helper-or-rename pattern
    from the prior commits.
    """
    tree = ast.parse(HARNESS_PATH.read_text())
    leaks = _audit_function(tree, "_run_lever_loop")
    new_leaks = sorted(set(leaks) - _KNOWN_DEFENDED_DEAD_CODE_LEAKS)
    assert not new_leaks, (
        "NEW inner-helper variable name leaked to _run_lever_loop's "
        "outer scope. The name(s) below are read in _run_lever_loop "
        "but are not a parameter, not assigned at the function's "
        "top scope, not a module-level binding, not a Python "
        "builtin, and not on the ``_KNOWN_DEFENDED_DEAD_CODE_LEAKS`` "
        "allow-list. This is the same bug family as A "
        "(full_pre_arbiter_accuracy), B (full_accuracy), C "
        "(_baseline_rows_for_control_plane), D "
        "(MIN_POST_ARBITER_GAIN_PP), E (_audit_emit). Fix in the "
        "same commit using the helper-or-rename pattern.\n"
        f"New leaks: {new_leaks}"
    )

    # Defence-in-depth: the allow-list itself must shrink over time,
    # never grow. Detect drift if the historical defended-dead-code
    # surface is fixed (an entry is removed from the source) without
    # the allow-list being updated.
    stale_allow_entries = sorted(_KNOWN_DEFENDED_DEAD_CODE_LEAKS - set(leaks))
    assert not stale_allow_entries, (
        "Allow-list entries no longer correspond to actual leaks. "
        "These names are on ``_KNOWN_DEFENDED_DEAD_CODE_LEAKS`` but "
        "do not appear in the current audit. Remove them from the "
        f"allow-list: {stale_allow_entries}"
    )
