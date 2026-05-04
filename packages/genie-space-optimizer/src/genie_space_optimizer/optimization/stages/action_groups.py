"""Stage 4: Action Group selection (Phase F4).

Wraps the existing decision_emitters.strategist_ag_records producer in
a typed ActionGroupsInput / ActionGroupSlate surface so F5 (proposals)
can read the slate from a stage-aligned dataclass.

F4 is observability-only: per the plan's Reality Check appendix, the
strategist invocation block in harness.py is a non-contiguous sequence
of inline operations (~300-500 LOC), not a function. Lifting it
inside a single F4 gate is high-risk for byte-stability. F4 stands up
the typed surface and STRATEGIST_AG_EMITTED emission entry; the LLM
invocation, constraint filtering, and buffered-AG draining stay in
harness for now and are deferred to a follow-up plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.decision_emitters import (
    strategist_ag_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
)


STAGE_KEY: str = "action_group_selection"


@dataclass
class ActionGroupsInput:
    """Input to stages.action_groups.select.

    ``action_groups`` is the slate of AGs the strategist returned (after
    filtering and buffered-AG drain — F4 doesn't re-do that work).
    ``source_clusters_by_id`` maps cluster id to cluster dict so each
    AG's root_cause can be recovered. ``rca_id_by_cluster`` maps cluster
    id to its RCA id. ``ag_alternatives_by_id`` carries Phase D.5
    rejected-alternatives stamping.
    """

    action_groups: tuple[Mapping[str, Any], ...]
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    ag_alternatives_by_id: Mapping[str, Sequence[AlternativeOption]] = field(
        default_factory=dict
    )


@dataclass
class ActionGroupSlate:
    """Output of stages.action_groups.select.

    ``ags`` is the selected AG tuple (same content as input but normalized
    to a tuple). ``rejected_ag_alternatives`` records AGs the strategist
    proposed but the constraint/buffer pipeline filtered out, for Phase
    D.5 alternatives capture.
    """

    ags: tuple[Mapping[str, Any], ...]
    rejected_ag_alternatives: tuple[Mapping[str, Any], ...] = ()


def select(ctx, inp: ActionGroupsInput) -> ActionGroupSlate:
    """Stage 4 entry. Emits STRATEGIST_AG_EMITTED records and returns a
    typed slate. F4 is observability-only — does NOT invoke the
    strategist LLM, drain buffered AGs, or apply constraints. Harness
    still owns those steps and feeds the result into ``inp.action_groups``
    when the harness wire-up lands in a follow-up plan.
    """
    records = strategist_ag_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        action_groups=inp.action_groups,
        source_clusters_by_id=inp.source_clusters_by_id,
        rca_id_by_cluster=inp.rca_id_by_cluster,
        ag_alternatives_by_id=inp.ag_alternatives_by_id,
    )
    for record in records:
        ctx.decision_emit(record)

    return ActionGroupSlate(
        ags=tuple(inp.action_groups),
        rejected_ag_alternatives=(),
    )


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = select
