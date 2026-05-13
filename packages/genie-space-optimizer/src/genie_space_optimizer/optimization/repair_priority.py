"""Phase 2 Action 2.1 — Priority order + propagation-root-cause conditioning.

The five-step priority order applies to every cluster repair plan:

    semantic_clarification
    → scoped_instruction
    → repair_kit
    → non_verbatim_example_pattern
    → narrow_l6_snippet

Each ``RepairArchetype`` declares its ``default_priority_step``. The
Phase 1 Action 1.3 propagation root cause can up-promote the step:

* ``instruction_insufficient_force`` — instruction-only repairs are
  insufficient. Up-promote ``repair_kit`` to ``narrow_l6_snippet`` so
  the kit assembly requires an L6 companion.
* ``instruction_not_scoped_to_qid`` — scoping was wrong. Up-promote
  ``scoped_instruction`` (and earlier steps) to ``repair_kit`` so the
  scope decision lands as a kit member rather than a free-floating
  instruction.
* ``propagation_lag`` — repair shape unchanged; ``plan_repair`` will
  separately insert a propagation-verification hook before eval.
* ``eval_cache_stale`` — infrastructure issue; repair shape unchanged.
* ``unknown`` — Action 1.3 is unfilled; preserve the archetype's
  default step.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.repair_archetypes import RepairArchetype


PRIORITY_ORDER: tuple[str, ...] = (
    "semantic_clarification",
    "scoped_instruction",
    "repair_kit",
    "non_verbatim_example_pattern",
    "narrow_l6_snippet",
)


_VALID_PROPAGATION_VALUES: frozenset[str] = frozenset({
    "propagation_lag",
    "instruction_not_scoped_to_qid",
    "instruction_insufficient_force",
    "eval_cache_stale",
    "unknown",
})


def select_priority_step(
    *,
    archetype: RepairArchetype,
    propagation_root_cause: str,
) -> str:
    """Return the priority step for this archetype under the given
    propagation root cause.

    Raises ``ValueError`` if ``propagation_root_cause`` is not one of
    the five allowed values. Callers MUST funnel env reads through
    :func:`common.config.propagation_root_cause` which guarantees a
    valid value.
    """
    if propagation_root_cause not in _VALID_PROPAGATION_VALUES:
        raise ValueError(
            f"unknown propagation_root_cause: {propagation_root_cause!r}; "
            f"expected one of {sorted(_VALID_PROPAGATION_VALUES)}"
        )

    base = archetype.default_priority_step

    if propagation_root_cause == "instruction_insufficient_force":
        # The plural_top_n_collapse archetype's default is repair_kit;
        # we up-promote to narrow_l6_snippet so the kit requires an L6
        # companion.
        if archetype.name == "plural_top_n_collapse":
            return "narrow_l6_snippet"
        # Other archetypes whose default is at or below repair_kit also
        # up-promote to narrow_l6_snippet.
        if PRIORITY_ORDER.index(base) <= PRIORITY_ORDER.index("repair_kit"):
            return "narrow_l6_snippet"
        return base

    if propagation_root_cause == "instruction_not_scoped_to_qid":
        # Anything below repair_kit gets promoted to repair_kit so the
        # scope decision lands as a kit member.
        if PRIORITY_ORDER.index(base) < PRIORITY_ORDER.index("repair_kit"):
            return "repair_kit"
        return base

    # propagation_lag, eval_cache_stale, unknown: keep default.
    return base
