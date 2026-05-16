"""Golden test — every active LLM callsite whose linked registry name is
NOT in TYPED_OUTPUT_DEFERRED_ALLOWLIST must pass response_model=<Output>.

Plan: 2026-05-17-active-callsite-typed-output-wiring.md Task 11
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from tests.unit.optimization.test_prompt_registry_inventory import (
    TYPED_OUTPUT_DEFERRED_ALLOWLIST,
)

SRC = pathlib.Path(__file__).resolve().parents[3] / "src" / "genie_space_optimizer"


def _enclosing_func(tree: ast.Module, target: ast.AST) -> ast.FunctionDef | None:
    best: ast.FunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if (
                node.lineno <= target.lineno
                and (node.end_lineno or node.lineno) >= (target.end_lineno or target.lineno)
            ):
                if best is None or node.lineno > best.lineno:
                    best = node
    return best


def _link_calls_in_func(func: ast.FunctionDef) -> list[str]:
    names: list[str] = []
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        callee_name = (
            callee.attr if isinstance(callee, ast.Attribute)
            else callee.id if isinstance(callee, ast.Name)
            else None
        )
        if callee_name != "_link_prompt_to_trace":
            continue
        if not node.args:
            continue
        arg0 = node.args[0]
        if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
            names.append(arg0.value)
    return names


def _call_has_response_model(call: ast.Call) -> bool:
    return any(kw.arg == "response_model" for kw in (call.keywords or []))


_LLM_WRAPPERS: frozenset[str] = frozenset({
    "_traced_llm_call",
    "_call_llm_openai",
})


def _iter_llm_callsites():
    for py_file in SRC.rglob("*.py"):
        rel = py_file.relative_to(SRC.parent)
        if "tests" in py_file.parts:
            continue
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            callee = node.func
            callee_name = (
                callee.attr if isinstance(callee, ast.Attribute)
                else callee.id if isinstance(callee, ast.Name)
                else None
            )
            if callee_name not in _LLM_WRAPPERS:
                continue
            func = _enclosing_func(tree, node)
            if func is None:
                continue
            link_names = _link_calls_in_func(func)
            registry_name = link_names[0] if link_names else None
            yield (
                str(rel),
                node.lineno,
                registry_name,
                _call_has_response_model(node),
            )


def _func_has_traced_call_with_response_model(
    py_file: pathlib.Path, registry_name: str,
) -> bool:
    """Check if any function in py_file linked to registry_name has at
    least one _traced_llm_call with response_model=. Used to recognize
    the opt-in fallback pattern: a function may have BOTH a
    _traced_llm_call with response_model= (primary, gated by
    `response_model is not None`) and a fallback _call_llm_openai
    (legacy, gated by `response_model is None`). The latter is
    acceptable because the primary path enforces typed I/O."""
    try:
        tree = ast.parse(py_file.read_text())
    except SyntaxError:
        return False
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if registry_name not in _link_calls_in_func(func):
            continue
        for node in ast.walk(func):
            if not isinstance(node, ast.Call):
                continue
            callee = node.func
            callee_name = (
                callee.attr if isinstance(callee, ast.Attribute)
                else callee.id if isinstance(callee, ast.Name)
                else None
            )
            if callee_name == "_traced_llm_call" and _call_has_response_model(node):
                return True
    return False


def test_every_non_deferred_llm_callsite_wires_response_model():
    """Every active LLM callsite under a non-deferred registry name must
    either pass response_model= directly OR be a fallback in a function
    where the primary _traced_llm_call already passes response_model=.
    """
    violations: list[str] = []
    for filename, lineno, registry_name, has_rm in _iter_llm_callsites():
        if registry_name is None:
            continue
        if registry_name in TYPED_OUTPUT_DEFERRED_ALLOWLIST:
            continue
        if has_rm:
            continue
        # Opt-in fallback pattern: accept _call_llm_openai if the
        # enclosing function also has a _traced_llm_call with
        # response_model= (the primary path).
        py_file = SRC.parent / filename
        if _func_has_traced_call_with_response_model(py_file, registry_name):
            continue
        violations.append(
            f"{filename}:{lineno} — registry name "
            f"'{registry_name}' is NOT in TYPED_OUTPUT_DEFERRED_ALLOWLIST "
            f"but the callsite does not pass response_model=. "
            f"Wire response_model=<Output> to the call or add the "
            f"registry name to TYPED_OUTPUT_DEFERRED_ALLOWLIST with a "
            f"documented rationale."
        )
    assert not violations, (
        "Active LLM callsites missing response_model= wiring:\n"
        + "\n".join(violations)
    )


def test_no_call_llm_openai_under_non_deferred_registry_name():
    """Phase B migration completeness check — but tolerate the opt-in
    fallback pattern (see _func_has_traced_call_with_response_model)."""
    leaks: list[str] = []
    for filename, lineno, registry_name, _has_rm in _iter_llm_callsites():
        if registry_name is None or registry_name in TYPED_OUTPUT_DEFERRED_ALLOWLIST:
            continue
        src_path = SRC.parent / filename
        line = src_path.read_text().splitlines()[lineno - 1]
        if "_call_llm_openai(" not in line:
            continue
        if _func_has_traced_call_with_response_model(src_path, registry_name):
            continue
        leaks.append(
            f"{filename}:{lineno} — registry name '{registry_name}' "
            f"is not deferred but the callsite uses _call_llm_openai. "
            f"Migrate to _traced_llm_call so it can accept "
            f"response_model=."
        )
    assert not leaks, "Non-deferred callsites still on _call_llm_openai:\n" + "\n".join(leaks)
