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

_BROAD_L6_TYPES: frozenset[str] = frozenset({
    "sql_snippet", "l6_sql", "broad_l6_sql", "general_sql_expression",
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
