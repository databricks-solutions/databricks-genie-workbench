#!/usr/bin/env python3
"""Lint rule for the unified example-SQL isolation invariants.

Phase 5.R5b of the unify-example-sql-onto-benchmark-engine plan. Walks
the AST of ``optimization/evaluation.py`` and asserts that neither
``generate_example_sqls`` nor ``generate_validated_sql_examples`` has
grown a parameter named in the benchmark-text forbidden set.

This is the machine-checkable form of isolation invariant #1
documented in ``docs/example-sql-isolation.md``. The function
signatures ARE the contract — once Python's keyword-only signature
enforces ``leakage_oracle`` as a required argument, the only remaining
regression is someone adding a ``benchmarks=`` parameter to the
generator. This script catches exactly that.

Exit codes
----------
0 : all invariants hold.
1 : one or more violations found. Stderr contains the diagnostic;
    stdout is empty so CI logs stay focused on the error.
2 : lint could not run (file missing, unparseable). Treated as a hard
    fail in CI because silent lint success is a security hole.

Run
---
Directly:
    python scripts/lint_example_sql_isolation.py

Wire into pre-commit hook at repo level to run on every commit that
touches ``packages/genie-space-optimizer/src/genie_space_optimizer/**``.
"""
from __future__ import annotations

import ast
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
PKG_ROOT = HERE.parent
EVALUATION_PY = (
    PKG_ROOT / "src" / "genie_space_optimizer" / "optimization" / "evaluation.py"
)

# Functions whose signatures MUST NOT accept benchmark-text inputs.
GUARDED_FUNCTIONS: frozenset[str] = frozenset({
    "generate_example_sqls",
    "generate_validated_sql_examples",
})

# Parameter names that carry benchmark text. If any guarded function
# ever grows a parameter with one of these names, the isolation
# invariant is violated and this script fails.
FORBIDDEN_PARAMS: frozenset[str] = frozenset({
    "benchmarks",
    "benchmark_list",
    "existing_benchmarks",
    "benchmark_questions",
    "benchmark_sqls",
    "expected_sqls",
    "eval_questions",
    "benchmark_corpus",
})

# Parameters that MUST be present (contract-level requirements). The
# required-keyword enforcement happens at call time via Python's own
# TypeError, but a linter mention here catches the "was removed" case
# where someone silently drops the kwarg entirely.
REQUIRED_PARAMS: dict[str, frozenset[str]] = {
    "generate_example_sqls": frozenset({"leakage_oracle"}),
}


def _collect_param_names(fn: ast.FunctionDef) -> list[str]:
    """Return every parameter name declared on ``fn``, including
    positional-only, positional-or-keyword, keyword-only, and
    var-positional/keyword (if named)."""
    names: list[str] = []
    for p in fn.args.posonlyargs:
        names.append(p.arg)
    for p in fn.args.args:
        names.append(p.arg)
    for p in fn.args.kwonlyargs:
        names.append(p.arg)
    if fn.args.vararg:
        names.append(fn.args.vararg.arg)
    if fn.args.kwarg:
        names.append(fn.args.kwarg.arg)
    return names


def _find_function(tree: ast.AST, name: str) -> ast.FunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def lint_file(source_path: pathlib.Path) -> list[str]:
    """Return a list of human-readable violation messages. Empty list
    means no violations."""
    if not source_path.exists():
        raise FileNotFoundError(
            f"isolation lint target not found: {source_path}"
        )
    try:
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(source_path))
    except SyntaxError as exc:
        raise RuntimeError(
            f"cannot parse {source_path} for isolation lint: {exc}"
        ) from exc

    violations: list[str] = []

    for fn_name in GUARDED_FUNCTIONS:
        fn = _find_function(tree, fn_name)
        if fn is None:
            violations.append(
                f"[isolation-lint] guarded function '{fn_name}' was "
                f"removed or renamed in {source_path.name}. Either "
                f"rename the guard in {pathlib.Path(__file__).name} or "
                f"restore the function."
            )
            continue
        params = set(_collect_param_names(fn))
        offending = params & FORBIDDEN_PARAMS
        if offending:
            violations.append(
                f"[isolation-lint] {fn_name} accepts forbidden "
                f"parameter(s) {sorted(offending)}. The example-SQL "
                f"generator MUST NOT receive benchmark text. See "
                f"docs/example-sql-isolation.md (invariant #1)."
            )
        required = REQUIRED_PARAMS.get(fn_name, frozenset())
        missing = required - params
        if missing:
            violations.append(
                f"[isolation-lint] {fn_name} is missing required "
                f"parameter(s) {sorted(missing)}. These are the "
                f"machine-checkable form of the isolation contract."
            )
    return violations


def main() -> int:
    try:
        violations = lint_file(EVALUATION_PY)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if violations:
        for v in violations:
            print(v, file=sys.stderr)
        print(
            f"\nFAIL: {len(violations)} isolation-lint violation(s) "
            f"in {EVALUATION_PY}.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
