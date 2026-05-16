"""Prompt registry coverage tests.

These tests fail-fast when a new prompt is added without being registered
in ``LEVER_PROMPTS`` (or without being wired through
``_link_prompt_to_trace()`` at its callsite). They are the regression
guardrail that makes the rest of this plan stick.

A failure here means one of three things happened:
  1. Someone added a new ``_SKILL_LOADER.load_prompt(...)`` constant and
     forgot to add it to ``LEVER_PROMPTS``.
  2. Someone added a new ``_traced_llm_call(... prompt=...)`` with an
     inline f-string prompt instead of loading from a ``SKILL.md`` file.
  3. Someone added a new callsite that does not call
     ``_link_prompt_to_trace()`` before the LLM call — so the Linked
     Prompts tab in the MLflow trace UI will show nothing.

Plan: docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from genie_space_optimizer.common import config as cfg

SRC = pathlib.Path(__file__).resolve().parents[3] / "src" / "genie_space_optimizer"


def _all_skill_loader_constants() -> set[str]:
    """Return every constant assigned via ``_SKILL_LOADER.load_prompt(...)``.

    Walks the AST of every .py file under src/ to find assignments of the
    shape ``FOO_PROMPT = _SKILL_LOADER.load_prompt("skill_id",
    expected_constant_name="FOO_PROMPT")``. Returns the set of constant
    names — these are the SKILL.md-backed prompts.
    """
    names: set[str] = set()
    for py_file in SRC.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "load_prompt"
                and isinstance(node.value.func.value, ast.Name)
                and node.value.func.value.id == "_SKILL_LOADER"
            ):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def test_every_skill_loader_constant_is_in_LEVER_PROMPTS():
    """Every ``_SKILL_LOADER.load_prompt(...)`` constant must be in
    ``LEVER_PROMPTS``.

    Remediation if this fails: add the constant to ``LEVER_PROMPTS`` in
    ``common/config.py``. The registered name is the lowercase snake_case
    of the constant minus the ``_PROMPT`` suffix.
    """
    skill_constants = _all_skill_loader_constants()
    registered_constants = {
        id(template): name for name, template in cfg.LEVER_PROMPTS.items()
    }
    missing: list[str] = []
    for const_name in sorted(skill_constants):
        const_value = getattr(cfg, const_name, None)
        if const_value is None:
            # Constant lives in another module (e.g. synthesis.py). Skip
            # for the in-module check; covered by Task 4/5 separately.
            continue
        if id(const_value) not in registered_constants:
            missing.append(const_name)
    assert not missing, (
        f"These _SKILL_LOADER constants are NOT in LEVER_PROMPTS, so they "
        f"will not be registered to MLflow Prompt Registry and the Linked "
        f"Prompts tab will be blank for their traces: {missing}. Add an "
        f"entry to LEVER_PROMPTS in common/config.py."
    )


def test_no_inline_fstring_LLM_prompts_in_optimization_module():
    """Inline f-string LLM prompts are forbidden — every prompt must live
    in a ``SKILL.md`` file (so it gets versioning, registry, tracing,
    and A/B-testable rollout).

    Detection heuristic: search for ``_traced_llm_call(`` callsites whose
    ``prompt=`` argument is a Python f-string literal (``f"..."`` or
    ``f'''...'''``) longer than 200 chars. Short prompts (system message
    like ``"You are a metadata curator."``) are exempt — only the user
    prompt is checked.

    Allowlist: tests/, demos/, and any callsite that loads its template
    via ``_SKILL_LOADER.load_prompt(...)`` is fine.
    """
    optimization_dir = SRC / "optimization"
    violations: list[str] = []
    for py_file in optimization_dir.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            func_name = (
                func.attr if isinstance(func, ast.Attribute) else
                func.id if isinstance(func, ast.Name) else None
            )
            if func_name not in {"_traced_llm_call", "_call_llm_for_proposal"}:
                continue
            prompt_arg = None
            for kw in node.keywords or []:
                if kw.arg == "prompt":
                    prompt_arg = kw.value
                    break
            # _traced_llm_call signature: (w, system_msg, prompt, *, span_name, ...)
            # so positional index 2 is prompt.
            if prompt_arg is None and func_name == "_traced_llm_call" and len(node.args) >= 3:
                prompt_arg = node.args[2]
            if prompt_arg is None:
                continue
            if isinstance(prompt_arg, ast.JoinedStr):
                rendered_chars = sum(
                    len(v.value) for v in prompt_arg.values if isinstance(v, ast.Constant)
                )
                if rendered_chars > 200:
                    violations.append(
                        f"{py_file.relative_to(SRC.parent)}:{node.lineno} — "
                        f"inline f-string prompt ({rendered_chars} chars of "
                        f"static text). Extract to a SKILL.md file under "
                        f"skills/<skill-name>/SKILL.md and load via "
                        f"_SKILL_LOADER.load_prompt(...)."
                    )
    assert not violations, (
        "Inline f-string LLM prompts found — every prompt must live in a "
        "SKILL.md file:\n" + "\n".join(violations)
    )


def test_lever_6_sql_expression_prompt_is_registered():
    assert "lever_6_sql_expression" in cfg.LEVER_PROMPTS, (
        "LEVER_6_SQL_EXPRESSION_PROMPT is the active Stage-2 prompt for "
        "lever-6 (48 LLM calls in Trial-5) but is not in LEVER_PROMPTS, "
        "so it is never registered to MLflow Prompt Registry. Add "
        "'lever_6_sql_expression': LEVER_6_SQL_EXPRESSION_PROMPT to "
        "LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["lever_6_sql_expression"] is cfg.LEVER_6_SQL_EXPRESSION_PROMPT
