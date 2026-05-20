"""Plan 9 Task 11.B — docstring truthfulness gate.

For each function in config.py that uses _flag_default_on, asserts
that its docstring does NOT claim "Default OFF" / "Default-OFF" /
"Default off" / "default off". Catches future regressions of the
cycle-9-flip-without-docstring-update pattern.

The 9 STALE_OFF_BUT_ON functions identified in the 2026-05-19 audit
are explicitly enumerated as the "must pass" set; new functions
added later must also satisfy the rule.
"""
from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

from genie_space_optimizer.common import config as config_module


def _config_source_path() -> Path:
    return Path(inspect.getfile(config_module))


def _functions_using_flag_default_on() -> dict[str, str]:
    """Return {function_name: docstring} for every function in
    config.py whose body calls _flag_default_on(...)."""
    source = _config_source_path().read_text()
    tree = ast.parse(source)
    result: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        uses_default_on = False
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                func = child.func
                name = (
                    func.attr if isinstance(func, ast.Attribute)
                    else (func.id if isinstance(func, ast.Name) else "")
                )
                if name == "_flag_default_on":
                    uses_default_on = True
                    break
        if uses_default_on:
            result[node.name] = ast.get_docstring(node) or ""
    return result


_STALE_PATTERN = re.compile(
    r"\bdefault[\s\-]*off\b", flags=re.IGNORECASE,
)


def test_no_flag_default_on_function_claims_default_off():
    """Every function using _flag_default_on must NOT have a
    docstring that claims default-OFF. Mismatches were a
    significant source of operator confusion."""
    offenders: list[str] = []
    for name, docstring in _functions_using_flag_default_on().items():
        if _STALE_PATTERN.search(docstring):
            offenders.append(name)
    assert offenders == [], (
        f"{len(offenders)} function(s) use _flag_default_on but "
        f"claim default-OFF in docstrings: {sorted(offenders)}. "
        "Each is default-ON; rewrite the docstring to say "
        "'Default ON. Disable with GSO_X=0.'"
    )


def test_critique_gate_docstring_does_not_claim_plan_8_removal():
    """Plan 8 Task 12 was deferred; the accessor is still called
    from harness.py + candidate_critique.py. The docstring must
    not claim the flag 'is removed in Plan 8 Task 12'."""
    docstring = (
        config_module.critique_gate_enforcing_enabled.__doc__ or ""
    )
    assert "removed in Plan 8 Task 12" not in docstring, (
        "critique_gate_enforcing_enabled docstring still claims "
        "Plan 8 T12 removal; T12 was deferred. Update docstring."
    )


def test_sql_shape_overlap_docstring_does_not_claim_unset_off():
    """Code is os.environ.get('X', '1') — unset returns True. The
    legacy docstring sentence 'Anything else (including unset, ...)
    keeps the flag off' contradicts this."""
    docstring = (
        config_module.sql_shape_overlap_gate_enabled.__doc__ or ""
    )
    assert "including unset" not in docstring, (
        "sql_shape_overlap_gate_enabled docstring contradicts code: "
        "claims 'including unset ... keeps the flag off' but code "
        "uses os.environ.get('X', '1'). Remove 'including unset'."
    )


def test_rich_synthesis_primary_docstring_does_not_claim_unset_off():
    """Same contradiction as test above, applied to the sibling
    function."""
    docstring = (
        config_module
        .rich_synthesis_primary_for_sql_shape_enabled.__doc__
        or ""
    )
    assert "including unset" not in docstring, (
        "rich_synthesis_primary_for_sql_shape_enabled docstring "
        "contradicts code: claims 'including unset ... keeps the "
        "flag off' but code uses os.environ.get('X', '1'). Remove "
        "'including unset'."
    )
