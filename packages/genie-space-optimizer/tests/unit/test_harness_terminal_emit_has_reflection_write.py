"""Phase 2 (2026-05-16) — structural guard that every
``_iter_terminal_emitted = True`` site in ``harness.py`` has a
``reflection_buffer.append(_build_reflection_entry(`` call within
±60 lines AFTER it.

Today several terminal-emit paths set the flag and ``continue``
without writing a reflection entry. The forbidden-set / candidate-
ledger contracts depend on every non-accepted iteration leaving a
reflection-buffer trace; sites that skip the write make their AG
re-eligible in iter N+1, which is the iter-2..iter-N retry-storm
failure mode this plan closes.

Whitelist mechanism: a small literal list of allowed
``_iter_terminal_emitted = True`` sites that legitimately don't need
a reflection write (e.g., pre-AG-selection terminals that have no AG
to retire). Today the whitelist starts empty.
"""
from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness


# Sites that do NOT require a +60-forward reflection write.
# Each entry has a comment justifying the exemption. Two classes:
#   (a) Pre-AG-selection terminals — no AG to retire.
#   (b) Per-AG terminals whose reflection_buffer.append happens
#       earlier in the SAME conditional block (the harness pattern is
#       reflection-first, then the typed marker emit ~60-300 lines
#       later in the cleanup tail).
#
# Line numbers refresh per harness growth. After Plan 8 (T1, T3, T4,
# T8, T9) added ~600 lines to harness.py, every entry shifted. The
# semantic categories (pre-AG vs per-AG-with-earlier-write) are
# unchanged; refresh the indices when the structural guard fails
# only after confirming via the nearest ``_iter_terminal_reason``
# assignment that the new index emits the same reason as the entry
# it replaces.
_REFLECTION_WRITE_WHITELIST: frozenset[int] = frozenset({
    # (a) Pre-AG-selection — no AG in scope.
    19204,  # blast_radius_rejected, reserved-recovery early-terminate
    20686,  # no_structural_candidate, no_actionable_clusters
    22394,  # no_action_group_emitted, strategy_zero_ags
    23102,  # WU-3 (2026-05-18) early_preflight_<reason>, slate-level
            # apply_admission_trace SKIP_AG decision before per-AG
            # processing begins — no AG to retire yet.
    # (b) Per-AG, reflection write precedes the typed marker emit.
    22707,  # ag_collision_with_forbidden_set
    24812,  # proposal_generation_empty
    26420,  # no_rca_ground
    29347,  # no_applied_patches DOA
    29647,  # Phase 0.3 Task 10 — applier_failed short-circuit
            # (Genie API rejected the PATCH payload as
            # SCHEMA_FAILURE/INFRA_FAILURE). Terminates as
            # ``unknown`` because no closed-vocab structural reason
            # applies; the exception path emits before any
            # reflection write reaches this AG.
    # (b) Full-eval path: gate result handling, reflection write
    # happens later in the rollback or accept paths.
    29839,  # full_eval / accepted, write in gate-result branch
})


_TERMINAL_EMIT_RE = re.compile(
    r"^\s*_iter_terminal_emitted\s*=\s*True\b",
    re.MULTILINE,
)

_REFLECTION_WRITE_RE = re.compile(
    r"reflection_buffer\.append\(\s*_build_reflection_entry\("
)
_TWO_LINE_FORM_RE = re.compile(
    r"=\s*_build_reflection_entry\("
)

LOOK_AHEAD_LINES = 60


def _line_no_at(source: str, char_offset: int) -> int:
    return source.count("\n", 0, char_offset) + 1


def _line_offsets(source: str) -> list[int]:
    offsets = [0]
    for i, ch in enumerate(source):
        if ch == "\n":
            offsets.append(i + 1)
    return offsets


def test_every_terminal_emit_has_nearby_reflection_write():
    source = inspect.getsource(harness)
    offsets = _line_offsets(source)

    emit_lines: list[int] = []
    for m in _TERMINAL_EMIT_RE.finditer(source):
        emit_lines.append(_line_no_at(source, m.start()))

    assert emit_lines, (
        "Expected at least one ``_iter_terminal_emitted = True`` "
        "site in harness.py; found none. Has the variable been renamed?"
    )

    missing: list[int] = []
    for emit_line in emit_lines:
        if emit_line in _REFLECTION_WRITE_WHITELIST:
            continue
        if emit_line - 1 >= len(offsets):
            continue
        start_char = offsets[emit_line - 1]
        end_line_idx = min(
            emit_line - 1 + LOOK_AHEAD_LINES,
            len(offsets) - 1,
        )
        end_char = offsets[end_line_idx]
        window = source[start_char:end_char]
        if (
            not _REFLECTION_WRITE_RE.search(window)
            and not _TWO_LINE_FORM_RE.search(window)
        ):
            missing.append(emit_line)

    assert not missing, (
        "The following ``_iter_terminal_emitted = True`` sites do NOT "
        f"have a reflection_buffer.append(_build_reflection_entry( "
        f"call (or two-line form) within {LOOK_AHEAD_LINES} lines "
        "AFTER them. Each one must either (a) write a reflection entry "
        "with a populated terminal_signature, or (b) be added to the "
        "whitelist with a comment justifying the exemption.\n"
        "Missing sites: "
        + ", ".join(f"harness.py:{ln}" for ln in missing)
    )


def test_terminal_reason_paths_listed_in_spec_each_have_an_emit_site():
    """The six terminal paths called out in the Phase 2 spec must each
    appear as a ``_iter_terminal_reason = "<value>"`` assignment (or
    ``= TerminalReason.<NAME>.value``) somewhere in harness.py.

    If a TerminalReason has zero producer sites, no reflection entry
    can carry that signature, and downstream retirement misses the
    cluster.
    """
    source = inspect.getsource(harness)
    # Map spec-name → set of acceptable producer-side string aliases.
    # The harness sometimes uses a legacy alias (e.g.,
    # ``full_eval_regression`` for ``content_regression_rollback``);
    # accept either form. ``structural_gate_dropped_instruction_only``
    # has no producer-site today — Task 7 inserts the producer
    # alongside the missing reflection write, so this is informational
    # only and intentionally not required.
    required: dict[str, set[str]] = {
        "proposal_generation_empty": {"proposal_generation_empty"},
        "no_structural_candidate": {"no_structural_candidate"},
        "blast_radius_rejected": {"blast_radius_rejected"},
        "no_applied_patches": {"no_applied_patches"},
        "content_regression_rollback": {
            "content_regression_rollback",
            "full_eval_regression",
        },
    }
    missing: list[str] = []
    for spec_name, aliases in required.items():
        for alias in aliases:
            if re.search(
                r"_iter_terminal_reason\s*=\s*['\"]"
                + re.escape(alias) + r"['\"]",
                source,
            ):
                break
            enum_name = alias.upper()
            if re.search(
                r"_iter_terminal_reason\s*=\s*TerminalReason\."
                + re.escape(enum_name),
                source,
            ):
                break
        else:
            missing.append(spec_name)
    assert not missing, (
        "These TerminalReason values have ZERO producer assignments "
        "in harness.py — no reflection entry can ever carry the "
        "matching signature:\n  " + "\n  ".join(missing)
    )
