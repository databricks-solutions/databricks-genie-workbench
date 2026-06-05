"""Phase 2 P2.3 — atomic bundle apply contract.

A bundle is a set of proposals sharing a non-empty ``bundle_id``. Each
proposal is a single Genie-Space patch (per ``RepairProposal``). The
**atomic apply** contract states:

    Either EVERY patch in the bundle lands successfully, or NONE do.

The legacy per-step orchestration in :mod:`bundle_orchestration`
applies steps incrementally and terminates early when a step regresses
behavior. That is a *behavior-driven* early termination — fine for
single-QID slicing — but at the survivor-selection layer we treat
each bundle as a single unit. If half of a 3-patch bundle landed and
the other half were dropped at the applier, the bundle has installed
INCOHERENT structural state (e.g. a snippet expecting a column the
description patch did not land). Atomic apply rolls those partial
patches back so the surviving Genie Space state is consistent.

This module is pure orchestration:

  * :func:`partition_apply_outcomes_by_bundle` groups per-proposal
    apply outcomes by bundle_id so callers can ask "did the whole
    bundle land or only part of it?".
  * :func:`bundle_apply_status` returns one of the closed labels
    ``"all_applied"`` / ``"none_applied"`` / ``"partial"`` for a
    given bundle.
  * :func:`bundle_partial_apply_signature` mints the typed
    forbidden_signature appended when a bundle terminates with
    :class:`~genie_space_optimizer.optimization.terminal_reason.TerminalReason.BUNDLE_PARTIAL_APPLY`.
  * :func:`select_survivor_bundle` is the survivor-selection helper
    that picks the highest-scoring fully-applied bundle when the
    iteration produced more than one viable bundle candidate.

Single-proposal patches (``bundle_id == ""``) are NEVER routed
through the atomic-apply gate — they are accepted/rejected at the
applier_gate as today. Callers should pre-filter via
``partition_apply_outcomes_by_bundle`` and pass only the bundled
slice through this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


@dataclass(frozen=True, slots=True)
class BundleApplyOutcome:
    """One bundle's per-step apply results.

    ``applied_intent_ids`` and ``failed_intent_ids`` are disjoint and
    together cover every step in the bundle. ``status`` is one of:

      * ``"all_applied"``    — all bundle steps landed
      * ``"none_applied"``   — zero bundle steps landed
      * ``"partial"``        — at least one but not all landed

    Callers terminate with ``TerminalReason.BUNDLE_PARTIAL_APPLY`` for
    ``"partial"``; surviving bundles with ``"all_applied"`` flow to
    the per-iteration survivor-selection step.
    """

    bundle_id: str
    applied_intent_ids: tuple[str, ...]
    failed_intent_ids: tuple[str, ...]
    status: str


def partition_apply_outcomes_by_bundle(
    proposals: Sequence[RepairProposal],
    applied_intent_ids: Iterable[str],
) -> tuple[dict[str, BundleApplyOutcome], tuple[str, ...]]:
    """Group per-proposal apply outcomes by ``bundle_id``.

    Returns ``(outcomes_by_bundle, singleton_intent_ids)`` where:

      * ``outcomes_by_bundle`` maps bundle_id → :class:`BundleApplyOutcome`
        for every bundle (non-empty bundle_id) present in
        ``proposals``.
      * ``singleton_intent_ids`` is the tuple of intent_ids whose
        proposals had ``bundle_id == ""`` — these are routed through
        the legacy per-patch applier_gate path.

    Both partitions taken together always cover every proposal in the
    input. ``applied_intent_ids`` is the set of intent_ids that
    landed at apply time (the order is irrelevant; only membership
    matters here).
    """
    applied_set = {str(i) for i in applied_intent_ids}

    grouped: dict[str, list[RepairProposal]] = {}
    singletons: list[str] = []
    for proposal in proposals:
        bundle_id = (proposal.bundle_id or "").strip()
        if not bundle_id:
            singletons.append(proposal.intent_id)
            continue
        grouped.setdefault(bundle_id, []).append(proposal)

    outcomes: dict[str, BundleApplyOutcome] = {}
    for bundle_id, members in grouped.items():
        applied: list[str] = []
        failed: list[str] = []
        for m in members:
            if m.intent_id in applied_set:
                applied.append(m.intent_id)
            else:
                failed.append(m.intent_id)
        if not failed:
            status = "all_applied"
        elif not applied:
            status = "none_applied"
        else:
            status = "partial"
        outcomes[bundle_id] = BundleApplyOutcome(
            bundle_id=bundle_id,
            applied_intent_ids=tuple(applied),
            failed_intent_ids=tuple(failed),
            status=status,
        )

    return outcomes, tuple(singletons)


def bundle_apply_status(outcome: BundleApplyOutcome) -> str:
    """Return the closed status label for a single bundle outcome.

    Trivial accessor — exists so callers do not depend on the field
    name of :class:`BundleApplyOutcome` and to give the typed status
    a stable function-name to grep for in postmortems."""
    return outcome.status


def bundle_partial_apply_signature(outcome: BundleApplyOutcome) -> str:
    """Phase 2 P2.3 — typed forbidden_signature for a partially
    applied bundle.

    Shape: ``"bundle_partial_apply:bundle=<ID>:applied=<N>:failed=<M>"``

    Surfaced to the NEXT iteration's strategist via
    ``ctx.forbidden_signatures`` so the LLM sees that the prior
    iteration's kit was incoherent at apply time and re-plans rather
    than re-issuing the same bundle composition.
    """
    return (
        f"bundle_partial_apply:bundle={outcome.bundle_id}"
        f":applied={len(outcome.applied_intent_ids)}"
        f":failed={len(outcome.failed_intent_ids)}"
    )


def select_survivor_bundle(
    outcomes_by_bundle: Mapping[str, BundleApplyOutcome],
    scores_by_bundle: Mapping[str, float],
) -> tuple[str, BundleApplyOutcome] | tuple[None, None]:
    """Phase 2 P2.3 — pick the highest-scoring fully-applied bundle.

    Only bundles with status ``"all_applied"`` are eligible — a
    partial bundle has already been rolled back to ``"none_applied"``
    by the atomic-apply gate, and bundles with no patches applied
    cannot contribute structural change.

    ``scores_by_bundle`` maps bundle_id to the post-apply behavioural
    score (the same metric the per-QID acceptance gate consults).
    Ties are broken by lexicographic bundle_id order so the
    selection is deterministic.

    Returns ``(bundle_id, outcome)`` for the winner, or
    ``(None, None)`` when no eligible bundle exists.
    """
    eligible = [
        (bid, outcome)
        for bid, outcome in outcomes_by_bundle.items()
        if outcome.status == "all_applied"
    ]
    if not eligible:
        return (None, None)
    # Sort descending by score; ties broken by ascending bundle_id.
    eligible.sort(key=lambda kv: (-scores_by_bundle.get(kv[0], 0.0), kv[0]))
    return eligible[0]
