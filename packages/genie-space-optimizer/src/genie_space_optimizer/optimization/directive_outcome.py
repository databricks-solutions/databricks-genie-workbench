"""Phase 3 — directive-to-proposal outcome attribution.

Pure module: maps observable per-(ag, lever) state to a closed-vocabulary
``DirectiveOutcomeCode``. No I/O. No side effects. No harness imports.

The closed vocabulary plus a per-iteration ledger gives a postmortem reader
per-(ag_id, lever_key) attribution for every directive an AG carried, closing
the silent-AG-budget-burn gap from 2314bb2c iter 2-5 where AG2's L5/L6
directives produced zero proposals with no per-lever signal in the trace.

Closed vocabulary:

* ``proposal_emitted`` — generator returned >= 1 proposal for this lever.
* ``no_structural_candidate`` — generator returned 0 proposals and no
  structural-gate drop records were stashed (no archetype match).
* ``force_llm_declined`` — L6 force-LLM path returned no candidate.
* ``applyability_rejected`` — generator returned >= 1 proposal but every
  entry was dropped by the applyability gate.
* ``collateral_rejected`` — generator returned >= 1 proposal but every entry
  was dropped by the blast-radius / high_collateral gate.
* ``lever_not_proposal_generating`` — lever 3 directives are applied as
  instruction-edits, never enter ``generate_proposals_from_strategy``.

Evidence anchor: runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class DirectiveOutcomeCode(str, Enum):
    """Closed vocabulary of per-(ag_id, lever_key) outcomes."""

    PROPOSAL_EMITTED = "proposal_emitted"
    NO_STRUCTURAL_CANDIDATE = "no_structural_candidate"
    # Phase 6.5 (2026-05-17) — distinguish "synthesis ran and returned
    # no candidate" from "synthesis was never invoked". Pre-Phase-6
    # the zero-proposal default branch conflated the two, which masked
    # the L5-gate failure mode (Run B H002).
    SYNTHESIS_ATTEMPTED_EMPTY = "synthesis_attempted_empty"
    FORCE_LLM_DECLINED = "force_llm_declined"
    APPLYABILITY_REJECTED = "applyability_rejected"
    COLLATERAL_REJECTED = "collateral_rejected"
    LEVER_NOT_PROPOSAL_GENERATING = "lever_not_proposal_generating"


@dataclass(frozen=True)
class LeverProposalSnapshot:
    """Typed input to ``classify_lever_proposal_outcome``.

    Every field is observable inside the per-AG proposal-generation loop
    in ``harness.py``. Frozen so the classifier cannot mutate state.
    """

    lever_key: int
    proposals_emitted_count: int
    structural_gate_drop_count: int
    applyability_drop_count: int
    collateral_drop_count: int
    force_llm_declined: bool
    attempted_synthesis: bool = False
    """Phase 6.5 (2026-05-17) — True iff
    ``run_cluster_driven_synthesis_for_single_cluster`` was invoked
    for at least one source cluster of this AG. Distinguishes
    ``SYNTHESIS_ATTEMPTED_EMPTY`` (synthesis ran and produced no
    candidate) from ``NO_STRUCTURAL_CANDIDATE`` (synthesis never
    ran). Default ``False`` preserves pre-Phase-6 behavior for
    every existing call site that omits the field."""


@dataclass(frozen=True)
class AgDirectiveLedger:
    """Per-AG record of every directive outcome for one iteration.

    ``outcomes_by_lever`` is a closed mapping: every lever_key in
    ``directives_present`` MUST appear as a key. The coverage invariant
    enforces this property on the iter_inputs blob.
    """

    ag_id: str
    iteration: int
    directives_present: tuple[int, ...]
    outcomes_by_lever: dict[int, DirectiveOutcomeCode] = field(
        default_factory=dict
    )

    def to_marker_payload(self) -> dict:
        """JSON-serialisable shape for the stdout marker."""
        return {
            "ag_id": self.ag_id,
            "iteration": int(self.iteration),
            "directives_present": list(self.directives_present),
            "outcomes_by_lever": {
                str(k): v.value for k, v in self.outcomes_by_lever.items()
            },
        }


_PROPOSAL_GENERATING_LEVERS: frozenset[int] = frozenset({5, 6})


def classify_lever_proposal_outcome(
    snapshot: LeverProposalSnapshot,
) -> DirectiveOutcomeCode:
    """Map a ``LeverProposalSnapshot`` to one ``DirectiveOutcomeCode``.

    Branch order (mutually exclusive):

    1. Lever 3 (or any non-proposal-generating lever) → ``LEVER_NOT_PROPOSAL_GENERATING``.
    2. Force-LLM declined (only meaningful for lever 6) → ``FORCE_LLM_DECLINED``.
    3. ``proposals_emitted_count > 0`` AND every proposal dropped by applyability
       → ``APPLYABILITY_REJECTED``.
    4. ``proposals_emitted_count > 0`` AND every proposal dropped by collateral
       → ``COLLATERAL_REJECTED``.
    5. ``proposals_emitted_count > 0`` (some survived) → ``PROPOSAL_EMITTED``.
    6. ``proposals_emitted_count == 0`` (default) → ``NO_STRUCTURAL_CANDIDATE``.

    The classifier never returns ``None`` and never raises on valid input
    — every snapshot is mapped to exactly one outcome.
    """
    if snapshot.lever_key not in _PROPOSAL_GENERATING_LEVERS:
        return DirectiveOutcomeCode.LEVER_NOT_PROPOSAL_GENERATING

    if snapshot.lever_key == 6 and snapshot.force_llm_declined:
        return DirectiveOutcomeCode.FORCE_LLM_DECLINED

    emitted = int(snapshot.proposals_emitted_count)
    if emitted > 0:
        # Survivor count = emitted minus drops. Drops are best-effort upper
        # bounds (a single proposal may be dropped by multiple gates and
        # logged in both buckets); the comparison below tolerates that by
        # using strict equality only when the drop count matches the emitted
        # count exactly.
        applyability = int(snapshot.applyability_drop_count)
        collateral = int(snapshot.collateral_drop_count)

        if applyability >= emitted:
            return DirectiveOutcomeCode.APPLYABILITY_REJECTED
        if collateral >= emitted:
            return DirectiveOutcomeCode.COLLATERAL_REJECTED
        return DirectiveOutcomeCode.PROPOSAL_EMITTED

    # Phase 6.5 (2026-05-17) — disambiguate "never attempted" from
    # "attempted and empty".
    if bool(snapshot.attempted_synthesis):
        return DirectiveOutcomeCode.SYNTHESIS_ATTEMPTED_EMPTY
    return DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


# ---------------------------------------------------------------------------
# Phase 3 follow-up (2026-05-13) — precise attribution reconciliation.
# ---------------------------------------------------------------------------
#
# The Phase 3 plan's Task 8 wiring used conservative-zero values for
# ``force_llm_declined`` / ``applyability_drop_count`` / ``collateral_drop_count``
# because the named ``optimizer._*`` accumulators do not exist in the
# codebase. That made the classifier emit ``NO_STRUCTURAL_CANDIDATE`` for
# every zero-proposal L6 lever — including the AG2 case where the L6
# force-LLM declined (the actual 2314bb2c failure shape).
#
# This helper closes the L6-decline gap by reading the canonical signal
# the harness ALREADY emits — a ``DecisionRecord`` with
# ``reason_code=LEVER6_FORCE_LLM_DECLINED`` in ``iter_inputs["decision_records"]``
# — and upgrading a ``NO_STRUCTURAL_CANDIDATE`` outcome to ``FORCE_LLM_DECLINED``
# when the (ag_id, iteration) matches.
#
# Per-cap-loop applyability/collateral attribution is a separate, larger
# follow-up because the cap loop pools proposals across levers and per-drop
# records do not carry an ``originating_lever_key`` tag.


_L6_FORCE_LLM_DECLINED_REASON: str = "lever6_force_llm_declined"


def reconcile_outcome_from_records(
    *,
    classifier_outcome: DirectiveOutcomeCode,
    lever_key: int,
    ag_id: str,
    iteration: int,
    decision_records,
) -> DirectiveOutcomeCode:
    """Refine a classifier outcome using observable decision_records.

    Branch order (each branch mutually exclusive with the next):

    1. ``classifier_outcome != NO_STRUCTURAL_CANDIDATE`` → pass through.
       Only the zero-proposal default-fallback is refinable; once the
       classifier has positive evidence (proposals emitted, force-LLM
       known, drops counted), the classifier output stands.
    2. ``lever_key == 6`` AND ``decision_records`` contains an entry with
       ``reason_code == "lever6_force_llm_declined"`` whose ``ag_id`` and
       ``iteration`` match → ``FORCE_LLM_DECLINED``.
    3. Otherwise → pass through unchanged.

    Pure, never raises. ``decision_records`` may be ``None`` or contain
    non-Mapping garbage entries; both shapes are handled defensively.

    Evidence anchor:
    docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md
    docs/2026-05-13-phase-3-directive-to-proposal-obligation-plan.md
    """
    if classifier_outcome != DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE:
        return classifier_outcome
    if int(lever_key) != 6:
        return classifier_outcome
    if not decision_records:
        return classifier_outcome

    target_ag_id = str(ag_id)
    target_iteration = int(iteration)

    for record in decision_records:
        if not isinstance(record, dict):
            continue
        reason = str(record.get("reason_code") or "")
        if reason != _L6_FORCE_LLM_DECLINED_REASON:
            continue
        if str(record.get("ag_id") or "") != target_ag_id:
            continue
        # ``iteration`` may be int (in-memory) or str (post JSON round-trip).
        rec_iter_raw = record.get("iteration")
        try:
            rec_iter = int(rec_iter_raw)
        except (TypeError, ValueError):
            continue
        if rec_iter != target_iteration:
            continue
        return DirectiveOutcomeCode.FORCE_LLM_DECLINED

    return classifier_outcome
