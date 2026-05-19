"""Plan 8 Task 5 — proposal_id → intent_id reverse map for Plan 7.

Plan 6's ``CritiqueOutcome.verdict_by_proposal_id`` is keyed by
proposal_id (the natural key for the LLM call). Plan 7's iteration
helper consumes verdicts keyed by intent_id (the natural key for
RepairIntent rollback bookkeeping). This module owns the single
pure join helper that bridges the two.

Two input shapes are supported so the same helper serves both the
typed-slate consumers (unit tests, future stages that consume a
``ProposalSlate``) and the harness wire-in path (which has only
the legacy ``list[dict]`` per AG in scope — see Plan 8 v2 scope
banner #3):

* ``verdict_by_intent_id(outcome, slate)`` — slate is a typed
  ``ProposalSlate`` (reads ``.proposals_by_ag``).
* ``verdict_by_intent_id_from_proposals_by_ag(outcome, mapping)`` —
  mapping is ``{ag_id: Iterable[dict]}`` with each dict carrying an
  ``intent_id`` stamp (from ``stamp_repair_intent_on_proposal``).

Unstamped proposals (no ``intent_id`` field — legacy / fallback
path that didn't call ``stamp_repair_intent_on_proposal``) are
silently skipped from the output map. Plan 8 Task 7 fixes the
stamp gap at the source so this skip becomes a no-op in production.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Mapping

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.candidate_critique_typed import (
        CritiqueVerdict,
    )
    from genie_space_optimizer.optimization.stages.candidate_critique import (
        CritiqueOutcome,
    )
    from genie_space_optimizer.optimization.stages.proposals import (
        ProposalSlate,
    )


def verdict_by_intent_id_from_proposals_by_ag(
    critique_outcome: "CritiqueOutcome",
    proposals_by_ag: Mapping[str, Iterable[Mapping[str, Any]]],
) -> dict[str, "CritiqueVerdict"]:
    """Return ``{intent_id: CritiqueVerdict}`` from a legacy mapping.

    Iterates every proposal in every AG; for each proposal that has
    both an ``intent_id`` stamp and a matching verdict in
    ``critique_outcome.verdict_by_proposal_id``, emits a
    ``{intent_id: verdict}`` entry. Two proposals sharing an
    intent_id (cross-AG dedup) collapse to the last verdict seen —
    Plan 7's hypothesizer reads one verdict per intent.
    """
    by_pid = critique_outcome.verdict_by_proposal_id or {}
    out: dict[str, "CritiqueVerdict"] = {}
    for _ag_id, props in (proposals_by_ag or {}).items():
        for p in (props or ()):
            pid = str(p.get("proposal_id") or "")
            iid = str(p.get("intent_id") or "")
            if not pid or not iid:
                continue
            verdict = by_pid.get(pid)
            if verdict is not None:
                out[iid] = verdict
    return out


def verdict_by_intent_id(
    critique_outcome: "CritiqueOutcome",
    proposal_slate: "ProposalSlate",
) -> dict[str, "CritiqueVerdict"]:
    """Typed-slate convenience wrapper.

    Delegates to ``verdict_by_intent_id_from_proposals_by_ag`` using
    ``proposal_slate.proposals_by_ag`` as the source. Use this when
    you have a typed ``ProposalSlate`` in scope (unit tests, future
    stages); use the underscore-suffixed helper directly when you
    only have the legacy mapping (Plan 8 Task 9 harness path).
    """
    return verdict_by_intent_id_from_proposals_by_ag(
        critique_outcome,
        (proposal_slate.proposals_by_ag or {}),
    )
