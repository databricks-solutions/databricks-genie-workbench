"""Phase 2.4 — auto narrow-replacement on collateral drop.

When a broad L6 (or example-SQL) patch is dropped for
``high_collateral_risk_flagged``, automatically invoke
:func:`build_narrow_l6_replacement` / :func:`build_l5_example_sql_replacement`
with ``protected_dependents`` set to the outside-target QIDs.

Pure helper: takes the synthesis callables as parameters so the
harness can inject them without import circularity.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from genie_space_optimizer.optimization.terminal_reason import TerminalReason


COLLATERAL_DROP_REASON: str = "high_collateral_risk_flagged"

# Plan C1 (2026-05-16) — production L6 patch types. These three values
# match `harness._LEVER6_PATCH_TYPES` (the only producer of L6
# patches), `optimizer._RCA_SQL_SNIPPET_PATCH_TYPES`, and
# `teaching_kit.SQL_SNIPPET_PATCH_TYPES`. The pre-Plan-C version of
# this set contained placeholder strings (``"sql_snippet"``,
# ``"l6_sql"``, etc.) that never appeared in real proposals, which
# meant the narrow-replacement helper was a dead code path for every
# broad-L6 collateral drop in production. A regression pin lives at
# ``tests/unit/test_auto_narrow_replacement.py::
# test_legacy_placeholder_patch_types_do_not_dispatch_to_l6``.
_BROAD_L6_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_expression",
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
})

_EXAMPLE_SQL_TYPES: frozenset[str] = frozenset({
    "example_sql_per_question", "example_sql", "per_question_example_sql",
})


@dataclass(frozen=True, slots=True)
class NarrowReplacementResult:
    attempted: bool
    replacement_patch: Mapping[str, Any] | None
    terminal_reason: str = ""


def try_narrow_replacement(
    *,
    dropped_patches: Sequence[Mapping[str, Any]],
    outside_target_qids: tuple[str, ...],
    cluster: Mapping[str, Any] | None,
    rca_card: Mapping[str, Any] | None,
    synthesis_callable_l6: Callable[..., Mapping[str, Any] | None],
    synthesis_callable_l5: Callable[..., Mapping[str, Any] | None],
) -> NarrowReplacementResult:
    """Try to synthesize a narrow replacement for the first collateral-
    dropped patch found.

    Returns:
      * ``attempted=False, replacement_patch=None`` — no collateral
        drop in the list
      * ``attempted=True, replacement_patch=<dict>`` — synthesis
        produced a scoped patch; caller substitutes it for the
        original
      * ``attempted=True, replacement_patch=None,
        terminal_reason="blast_radius_rejected"`` — synthesis
        returned nothing; caller should emit
        TerminalReason.BLAST_RADIUS_REJECTED and retire the signature
    """
    for patch in (dropped_patches or ()):
        if str(patch.get("drop_reason") or "") != COLLATERAL_DROP_REASON:
            continue
        patch_type = str(patch.get("patch_type") or "")

        replacement: Mapping[str, Any] | None = None
        if patch_type in _BROAD_L6_TYPES:
            replacement = synthesis_callable_l6(
                cluster=cluster,
                rca_card=rca_card,
                protected_dependents=tuple(outside_target_qids),
                original_dropped_patch=patch,
            )
        elif patch_type in _EXAMPLE_SQL_TYPES:
            replacement = synthesis_callable_l5(
                cluster=cluster,
                rca_card=rca_card,
                protected_dependents=tuple(outside_target_qids),
                original_dropped_patch=patch,
            )
        else:
            # Other patch_types don't currently have a narrow-
            # replacement helper; treat as no-attempt.
            continue

        if replacement is not None:
            return NarrowReplacementResult(
                attempted=True, replacement_patch=replacement,
            )
        return NarrowReplacementResult(
            attempted=True,
            replacement_patch=None,
            terminal_reason=TerminalReason.BLAST_RADIUS_REJECTED.value,
        )

    return NarrowReplacementResult(attempted=False, replacement_patch=None)
