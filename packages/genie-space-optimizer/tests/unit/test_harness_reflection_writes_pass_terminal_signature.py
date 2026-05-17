"""Phase 2 (2026-05-16) — structural guard that every non-accepted
``reflection_buffer.append(_build_reflection_entry(...))`` call in
``harness.py`` passes a ``terminal_signature=`` kwarg.

The two ``accepted=True`` writes do NOT need to carry a signature —
``TerminalSignature`` is the non-accepted retirement key (spec
Section 3.2: "For accepted / rolled-back outcomes, use AcceptanceTier
... The two vocabularies are disjoint and complementary.").

Source-inspection style mirrors Phase 1's structural guards.
"""
from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness


# Locate every ``_build_reflection_entry(...)`` call AND walk
# character-by-character to extract the balanced-paren arg list.
# This handles both ``reflection_buffer.append(_build_reflection_entry(
# ...))`` and the two-line ``reflection = _build_reflection_entry(...)``
# form, and copes with arbitrarily deep nesting in the args.
_HELPER_NAME = "_build_reflection_entry("


def _extract_build_calls(source: str) -> list[tuple[int, str]]:
    """Return a list of (start_line_number, raw_arg_text) for every
    ``_build_reflection_entry(...)`` call site in source. Line numbers
    are 1-based.
    """
    calls: list[tuple[int, str]] = []
    pos = 0
    while True:
        idx = source.find(_HELPER_NAME, pos)
        if idx == -1:
            break
        # Skip the function definition line.
        line_start = source.rfind("\n", 0, idx) + 1
        line_prefix = source[line_start:idx]
        if "def " in line_prefix:
            pos = idx + len(_HELPER_NAME)
            continue
        # Walk the args, paren-balanced.
        arg_start = idx + len(_HELPER_NAME)
        depth = 1
        j = arg_start
        while j < len(source) and depth > 0:
            ch = source[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            j += 1
        if depth != 0:
            pos = idx + len(_HELPER_NAME)
            continue
        args = source[arg_start : j - 1]
        line_no = source.count("\n", 0, idx) + 1
        calls.append((line_no, args))
        pos = j
    return calls


def test_audit_finds_at_least_ten_helper_call_sites():
    """Sanity: locate all the expected ``_build_reflection_entry(...)``
    call sites in ``harness.py``. Phase 2 plan A.3 lists 10."""
    source = inspect.getsource(harness)
    calls = _extract_build_calls(source)
    assert len(calls) >= 10, (
        f"Expected >=10 _build_reflection_entry(...) call sites; "
        f"found {len(calls)}. The regex may need tightening if the "
        f"harness was refactored."
    )


def test_every_non_accepted_write_passes_terminal_signature():
    """For every reflection-builder call where ``accepted=False``
    appears in the arg list, ``terminal_signature=`` must also appear
    in the same arg list."""
    source = inspect.getsource(harness)
    calls = _extract_build_calls(source)

    missing: list[tuple[int, str]] = []
    for line_no, args in calls:
        if not re.search(r"\baccepted\s*=\s*False\b", args):
            continue
        if not re.search(r"\bterminal_signature\s*=", args):
            snippet = args.replace("\n", " ").strip()
            if len(snippet) > 120:
                snippet = snippet[:120] + "…"
            missing.append((line_no, snippet))

    assert not missing, (
        "The following non-accepted _build_reflection_entry(...) call "
        "sites do NOT pass terminal_signature=:\n"
        + "\n".join(
            f"  harness.py:{ln}: {snippet}"
            for ln, snippet in missing
        )
    )


def test_accepted_writes_do_not_need_terminal_signature():
    """Defensive: the two accepted=True writes are allowed to omit
    ``terminal_signature=`` — pin that at least 2 such writes exist."""
    source = inspect.getsource(harness)
    calls = _extract_build_calls(source)
    accepted_writes = [
        (ln, args) for ln, args in calls
        if re.search(r"\baccepted\s*=\s*True\b", args)
    ]
    assert len(accepted_writes) >= 2, (
        f"Expected at least 2 accepted=True reflection writes, "
        f"found {len(accepted_writes)}."
    )
