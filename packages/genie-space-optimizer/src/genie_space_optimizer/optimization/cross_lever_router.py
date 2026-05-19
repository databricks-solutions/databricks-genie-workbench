"""Plan 5 — cross-lever router for RepairProposals.

Roadmap.md:337-343 — the Plan-5 LLM may emit a ``patch_type`` that maps
to a per-lever generator different from the L5b in-lane default
(``ADD_EXAMPLE_SQL``). The router:

  * looks up the patch_type → per-lever generator callable in a
    frozen dispatch table.
  * runs the compatible-shape check: patch_type MUST be in
    ``SUPPORTED_OVERRIDE_TARGETS``. Unsupported patch_types → return
    None → caller falls back to ``intent_from_archetype``.
  * returns the generator + (when applicable) a ``CrossLeverOverrideEvent``
    carrying provenance for postmortem visibility.

Plan-5 ships with TWO entries: ADD_EXAMPLE_SQL (in-lane L5b) and
ADD_SQL_SNIPPET_EXPRESSION (cross-lever to L6). Adding new entries
(L4 join discovery, L1 column description, …) requires:
  - adding the PatchType to ``SUPPORTED_OVERRIDE_TARGETS``.
  - adding a generator callable to ``cross_lever_dispatch_table()``.
  - extending ``_LEVER_NAME_BY_PATCH_TYPE``.
  - bumping the SKILL.md ``<context_inputs>.available_patch_types`` list.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


_IN_LANE_LEVER = "lever-5b-example-sql"
_IN_LANE_PATCH_TYPE = PatchType.ADD_EXAMPLE_SQL


_LEVER_NAME_BY_PATCH_TYPE: dict[PatchType, str] = {
    PatchType.ADD_EXAMPLE_SQL: _IN_LANE_LEVER,
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "lever-6-sql-expression",
    PatchType.ADD_SQL_SNIPPET_FILTER: "lever-6-sql-expression",
    PatchType.ADD_SQL_SNIPPET_MEASURE: "lever-6-sql-expression",
    PatchType.ADD_INSTRUCTION: "lever-5a-instructions",
    PatchType.UPDATE_INSTRUCTION: "lever-5a-instructions",
    PatchType.ADD_JOIN_SPEC: "lever-4-join-spec",
    PatchType.ADD_COLUMN_DESCRIPTION: "lever-1-table-column-description",
}


SUPPORTED_OVERRIDE_TARGETS: frozenset[PatchType] = frozenset({
    PatchType.ADD_EXAMPLE_SQL,
    PatchType.ADD_SQL_SNIPPET_EXPRESSION,
})


@dataclass(frozen=True, slots=True)
class CrossLeverOverrideEvent:
    """Provenance record for postmortem visibility.

    ``None`` when the proposal stays in-lane (no override happened).
    Non-None when the LLM emitted a different patch_type than L5b's
    in-lane default — the harness's decision-record stream logs this
    so operators can see how often the LLM crosses lever boundaries.
    """

    intent_id: str
    from_lever: str
    from_patch_type: PatchType
    to_lever: str
    to_patch_type: PatchType

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "from_lever": self.from_lever,
            "from_patch_type": self.from_patch_type.value,
            "to_lever": self.to_lever,
            "to_patch_type": self.to_patch_type.value,
        }


def _l5b_generator(proposal: RepairProposal) -> dict[str, Any]:
    """L5b generator — returns the existing 4-key shape that
    canonicalize_stage_2_proposal expects for ``add_example_sql``."""
    return proposal.to_proposal_dict()


def _l6_generator(proposal: RepairProposal) -> dict[str, Any]:
    """L6 generator — returns the 3-key shape for
    ``add_sql_snippet_expression`` / ``_filter`` / ``_measure``."""
    return proposal.to_proposal_dict()


def cross_lever_dispatch_table() -> dict[PatchType, Callable[[RepairProposal], dict[str, Any]]]:
    """Return the dispatch table.

    Built as a function (not module-level constant) so tests can
    monkey-patch individual generators without touching module state.
    """
    return {
        PatchType.ADD_EXAMPLE_SQL: _l5b_generator,
        PatchType.ADD_SQL_SNIPPET_EXPRESSION: _l6_generator,
    }


def route_to_per_lever_generator(
    proposal: RepairProposal,
) -> tuple[Callable[[RepairProposal], dict[str, Any]],
           CrossLeverOverrideEvent | None] | None:
    """Look up the per-lever generator for a Plan-5 RepairProposal.

    Returns:
      (generator, None) — in-lane case (patch_type is ADD_EXAMPLE_SQL).
      (generator, CrossLeverOverrideEvent) — cross-lever override.
      None — patch_type not in SUPPORTED_OVERRIDE_TARGETS. Caller
             falls back to intent_from_archetype against the
             deterministically picked Archetype.
    """
    if proposal.patch_type not in SUPPORTED_OVERRIDE_TARGETS:
        return None

    table = cross_lever_dispatch_table()
    generator = table.get(proposal.patch_type)
    if generator is None:
        return None

    if proposal.patch_type == _IN_LANE_PATCH_TYPE:
        return (generator, None)

    event = CrossLeverOverrideEvent(
        intent_id=proposal.intent_id,
        from_lever=_IN_LANE_LEVER,
        from_patch_type=_IN_LANE_PATCH_TYPE,
        to_lever=_LEVER_NAME_BY_PATCH_TYPE[proposal.patch_type],
        to_patch_type=proposal.patch_type,
    )
    return (generator, event)
